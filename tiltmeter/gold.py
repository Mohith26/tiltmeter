"""Silver -> gold, one month at a time.

Player features (loss streaks, sessions, rating continuity, the SCD2 tier
version in force) depend on a player's history, so month M reads a small
carried-state snapshot written by month M-1 instead of rescanning all history.
That makes each month an incremental, idempotent unit: rebuilding month M
replaces exactly its own partitions plus its state snapshot. If that snapshot
changes, the DAG knows month M+1 must be restated too; if it does not, the
chain stops.

Outputs per month:
  gold/fact_game, gold/fact_player_game, gold/tier_events (SCD2 change events),
  gold/_state_player, gold/_state_perf (carried state, cumulative)
"""
from __future__ import annotations

from .config import SESSION_GAP_MINUTES, Config, prev_month, tier_sql
from .io import connect, data_fingerprint, write_partition

SK = "(md5_number({}) >> 65)::BIGINT"  # stable 63-bit surrogate key


def _state_sql(cfg: Config, month: str, table: str, empty_cols: str) -> str:
    p = cfg.part("gold", table, prev_month(month))
    if p.exists():
        return f"SELECT * FROM read_parquet('{p}', hive_partitioning = false)"
    return f"SELECT {empty_cols} WHERE false"


def build_gold(cfg: Config, month: str) -> dict:
    con = connect()
    silver = cfg.part("silver", "games", month)
    con.execute(f"CREATE TEMP TABLE g AS SELECT * FROM read_parquet('{silver}', hive_partitioning = false)")
    con.execute("CREATE TEMP TABLE sp AS " + _state_sql(
        cfg, month, "_state_player",
        "NULL::VARCHAR player_id, NULL::TIMESTAMP last_ts, NULL::INT loss_streak, NULL::INT win_streak, "
        "NULL::INT session_num, NULL::INT game_in_session"))
    con.execute("CREATE TEMP TABLE sf AS " + _state_sql(
        cfg, month, "_state_perf",
        "NULL::VARCHAR player_id, NULL::VARCHAR perf, NULL::INT last_post_rating, NULL::VARCHAR tier, "
        "NULL::TIMESTAMP version_from, NULL::INT games_played"))

    # one row per (game, player)
    con.execute("""
    CREATE TEMP TABLE pg AS
    SELECT game_id, month, start_ts, speed AS perf, white_id AS player_id, white_name AS player_name,
           white_title AS title, black_id AS opp_id, 'white' AS color, white_elo AS pre_rating,
           black_elo AS opp_rating, white_rating_diff AS rating_delta, white_score AS score FROM g
    UNION ALL
    SELECT game_id, month, start_ts, speed, black_id, black_name, black_title, white_id, 'black',
           black_elo, white_elo, black_rating_diff, 1 - white_score FROM g
    """)

    # player-level features: sessions and result streaks (across all speeds)
    con.execute(f"""
    CREATE TEMP TABLE f1 AS
    WITH a AS (
      SELECT pg.*, sp.last_ts AS c_last_ts, coalesce(sp.loss_streak, 0) AS c_loss, coalesce(sp.win_streak, 0) AS c_win,
             coalesce(sp.session_num, 0) AS c_sess, coalesce(sp.game_in_session, 0) AS c_gis,
             coalesce(lag(start_ts) OVER w, sp.last_ts) AS prev_ts,
             (score <> 0)::INT AS non_loss, (score <> 1)::INT AS non_win
      FROM pg LEFT JOIN sp USING (player_id)
      WINDOW w AS (PARTITION BY player_id ORDER BY start_ts, game_id)
    ), b AS (
      SELECT *, epoch(start_ts - prev_ts) / 60.0 AS minutes_since_prev,
             (prev_ts IS NULL OR epoch(start_ts - prev_ts) / 60.0 > {SESSION_GAP_MINUTES})::INT AS new_sess,
             sum(non_loss) OVER w AS nl_cnt, sum(non_win) OVER w AS nw_cnt
      FROM a WINDOW w AS (PARTITION BY player_id ORDER BY start_ts, game_id ROWS UNBOUNDED PRECEDING)
    ), c AS (
      SELECT *, c_sess + sum(new_sess) OVER w AS session_num,
             row_number() OVER (PARTITION BY player_id, nl_cnt ORDER BY start_ts, game_id) AS rn_l,
             row_number() OVER (PARTITION BY player_id, nw_cnt ORDER BY start_ts, game_id) AS rn_w
      FROM b WINDOW w AS (PARTITION BY player_id ORDER BY start_ts, game_id ROWS UNBOUNDED PRECEDING)
    ), d AS (
      SELECT *,
        CASE WHEN score = 0 THEN rn_l - (nl_cnt > 0)::INT + CASE WHEN nl_cnt = 0 THEN c_loss ELSE 0 END ELSE 0 END AS loss_incl,
        CASE WHEN score = 1 THEN rn_w - (nw_cnt > 0)::INT + CASE WHEN nw_cnt = 0 THEN c_win ELSE 0 END ELSE 0 END AS win_incl,
        row_number() OVER (PARTITION BY player_id, session_num ORDER BY start_ts, game_id)
          + CASE WHEN session_num = c_sess THEN c_gis ELSE 0 END AS game_in_session
      FROM c
    )
    SELECT *, coalesce(lag(loss_incl) OVER w, c_loss) AS prior_loss_streak,
              coalesce(lag(win_incl) OVER w, c_win) AS prior_win_streak,
              (c_last_ts IS NOT NULL AND start_ts < c_last_ts) AS out_of_order
    FROM d WINDOW w AS (PARTITION BY player_id ORDER BY start_ts, game_id)
    """)

    # perf-level features: rating continuity and the SCD2 tier version
    tier = tier_sql("pre_rating")
    con.execute(f"""
    CREATE TEMP TABLE f2 AS
    WITH a AS (
      SELECT f1.*, sf.last_post_rating AS c_post, sf.tier AS c_tier, sf.version_from AS c_vfrom,
             {tier} AS tier,
             coalesce(sf.games_played, 0) + row_number() OVER w - 1 AS perf_games_before,
             lag(game_id) OVER w AS prev_game,
             lag(pre_rating + rating_delta) OVER w AS lag_post
      FROM f1 LEFT JOIN sf USING (player_id, perf)
      WINDOW w AS (PARTITION BY player_id, perf ORDER BY start_ts, game_id)
    ), b AS (
      SELECT *, CASE WHEN prev_game IS NULL THEN c_post ELSE lag_post END AS expected_pre_rating,
             CASE WHEN prev_game IS NULL THEN c_tier ELSE lag(tier) OVER w END AS prev_tier
      FROM a WINDOW w AS (PARTITION BY player_id, perf ORDER BY start_ts, game_id)
    ), c AS (
      SELECT *, (prev_tier IS NULL OR tier <> prev_tier) AS tier_changed FROM b
    )
    SELECT *,
      CASE WHEN expected_pre_rating IS NULL THEN NULL ELSE pre_rating <> expected_pre_rating END AS rating_continuity_break,
      coalesce(last_value(CASE WHEN tier_changed THEN start_ts END IGNORE NULLS) OVER w, c_vfrom) AS version_from
    FROM c WINDOW w AS (PARTITION BY player_id, perf ORDER BY start_ts, game_id ROWS UNBOUNDED PRECEDING)
    """)
    sk = SK.format("player_id || '|' || perf || '|' || strftime(version_from, '%Y-%m-%dT%H:%M:%S')")
    con.execute(f"CREATE TEMP TABLE f AS SELECT *, {sk} AS player_sk FROM f2")

    out = {}
    out["fact_player_game"] = write_partition(con, """
      SELECT game_id, month, CAST(strftime(start_ts, '%Y%m%d') AS INTEGER) AS date_key, start_ts,
             player_sk, player_id, opp_id, color, perf, tier, pre_rating, opp_rating,
             pre_rating - opp_rating AS rating_gap, rating_delta, score,
             1.0 / (1.0 + pow(10.0, (opp_rating - pre_rating) / 400.0)) AS expected_score,
             minutes_since_prev, session_num::INT AS session_num, game_in_session::INT AS game_in_session,
             prior_loss_streak::INT AS prior_loss_streak, prior_win_streak::INT AS prior_win_streak,
             perf_games_before::INT AS perf_games_before, rating_continuity_break, out_of_order
      FROM f ORDER BY game_id, color""", cfg.part("gold", "fact_player_game", month))

    ok = SK.format("coalesce(eco,'?') || '|' || coalesce(opening,'?')")
    out["fact_game"] = write_partition(con, f"""
      SELECT g.game_id, g.month, CAST(strftime(g.start_ts, '%Y%m%d') AS INTEGER) AS date_key, g.start_ts,
             g.speed, g.is_tournament, g.tournament_id, {ok} AS opening_key,
             w.player_sk AS white_sk, b.player_sk AS black_sk, g.white_id, g.black_id,
             g.white_elo, g.black_elo, g.white_rating_diff, g.black_rating_diff,
             g.result, g.white_score, w.expected_score AS white_expected_score,
             g.termination, g.num_moves, g.time_control, g.base_seconds, g.increment_seconds,
             g.eco, g.opening, g.opening_family, g.dq_flags, g.source_file
      FROM g
      JOIN (SELECT game_id, player_sk, 1.0 / (1.0 + pow(10.0, (opp_rating - pre_rating) / 400.0)) AS expected_score
            FROM f WHERE color = 'white') w USING (game_id)
      JOIN (SELECT game_id, player_sk FROM f WHERE color = 'black') b USING (game_id)
      ORDER BY g.game_id""", cfg.part("gold", "fact_game", month))

    out["tier_events"] = write_partition(con, """
      SELECT player_sk, player_id, any_value(player_name) AS player_name, perf, tier,
             version_from AS valid_from, max(title) AS title
      FROM f WHERE tier_changed
      GROUP BY ALL ORDER BY player_id, perf, valid_from""", cfg.part("gold", "tier_events", month))

    con.execute("""CREATE TEMP TABLE last_p AS
      SELECT * FROM f QUALIFY row_number() OVER (PARTITION BY player_id ORDER BY start_ts DESC, game_id DESC) = 1""")
    out["_state_player"] = write_partition(con, """
      SELECT * FROM (
        SELECT * FROM sp WHERE player_id NOT IN (SELECT player_id FROM last_p)
        UNION ALL
        SELECT player_id, start_ts AS last_ts, loss_incl::INT AS loss_streak, win_incl::INT AS win_streak,
               session_num::INT AS session_num, game_in_session::INT AS game_in_session FROM last_p
      ) ORDER BY player_id""", cfg.part("gold", "_state_player", month))
    out["_state_perf"] = write_partition(con, """
      SELECT * FROM (
        SELECT * FROM sf WHERE (player_id, perf) NOT IN (SELECT (player_id, perf) FROM f)
        UNION ALL
        SELECT player_id, perf, (pre_rating + rating_delta)::INT AS last_post_rating, tier, version_from,
               (perf_games_before + 1)::INT AS games_played FROM f
        QUALIFY row_number() OVER (PARTITION BY player_id, perf ORDER BY start_ts DESC, game_id DESC) = 1
      ) ORDER BY player_id, perf""", cfg.part("gold", "_state_perf", month))

    # downstream months only need restating if the carried state changed
    out["state_fingerprint"] = (
        data_fingerprint(con, cfg.part("gold", "_state_player", month)) + "/"
        + data_fingerprint(con, cfg.part("gold", "_state_perf", month)))
    con.close()
    return out
