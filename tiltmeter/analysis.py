"""The player-experience question, answered from the gold tables.

Q1 (tilt): after consecutive losses in the same session, do players score below
what the rating gap predicts? Outcome = score - expected_score (Elo logistic on
the pre-game ratings), so the rating gap is controlled for game by game.
The comparison group is the same loss streak where the player took a break
(> SESSION_GAP_MINUTES) before the next game: rating mechanics (a streak means
the rating has fallen, possibly below true skill) affect both groups the same
way, while tilt should only show up when the next game follows immediately.

Q2 (churn): how does the chance that a player ends their session depend on the
result streak they just had?

Q3 (matchmaking calibration): does the Elo expected score match observed score?

Uncertainty: 95% percentile intervals from a player-clustered Poisson
bootstrap (every game of a resampled player moves together), B=500.
"""
from __future__ import annotations

import duckdb
import numpy as np

from .config import SESSION_GAP_MINUTES, Config

B = 500
SEED = 7
MIN_PRIOR_GAMES = 20  # skip provisional ratings: a player's first 20 games in a speed


def _cluster_boot(con, sql: str, groups: list[str]):
    """sql must return (player_id, grp, n, s). Returns point estimate s/n per group
    and a (B x G) matrix of bootstrap replicates."""
    rows = con.execute(sql).fetchall()
    players = {p: i for i, p in enumerate(sorted({r[0] for r in rows}))}
    gi = {g: i for i, g in enumerate(groups)}
    N = np.zeros((len(players), len(groups)))
    S = np.zeros_like(N)
    for p, g, n, s in rows:
        if g in gi:
            N[players[p], gi[g]] += n
            S[players[p], gi[g]] += s
    rng = np.random.default_rng(SEED)
    W = rng.poisson(1.0, size=(B, len(players))).astype(float)
    reps = (W @ S) / np.maximum(W @ N, 1e-9)
    point = S.sum(0) / np.maximum(N.sum(0), 1e-9)
    return point, reps, N.sum(0).astype(int), len(players)


def _ci(x):
    lo, hi = np.percentile(x, [2.5, 97.5])
    return [round(float(lo), 5), round(float(hi), 5)]


def run_analysis(cfg: Config) -> dict:
    con = duckdb.connect(str(cfg.warehouse), read_only=True)
    gap = SESSION_GAP_MINUTES
    established = f"perf_games_before >= {MIN_PRIOR_GAMES} AND minutes_since_prev IS NOT NULL"
    con.execute(f"""
    CREATE TEMP TABLE pg AS
    SELECT player_id, score, expected_score, score - expected_score AS resid,
           least(prior_loss_streak, 5) AS k_loss, least(prior_win_streak, 5) AS k_win,
           (minutes_since_prev IS NOT NULL AND minutes_since_prev <= {gap}) AS same_session,
           CASE WHEN score = 0 THEN least(prior_loss_streak + 1, 5) ELSE 0 END AS loss_streak_incl,
           CASE WHEN score = 1 THEN least(prior_win_streak + 1, 5) ELSE 0 END AS win_streak_incl,
           lead(start_ts) OVER w AS next_ts, start_ts,
           (SELECT max(start_ts) FROM fact_player_game) AS data_end
    FROM fact_player_game
    WHERE perf <> 'Correspondence' AND {established}
    WINDOW w AS (PARTITION BY player_id ORDER BY start_ts, game_id)
    """)
    n_rows = con.execute("SELECT count(*) FROM pg").fetchone()[0]

    # ---- Q1 tilt ----------------------------------------------------------------
    ks = [0, 1, 2, 3, 4, 5]
    groups = [f"in:{k}" for k in ks] + [f"brk:{k}" for k in ks]
    point, reps, ns, n_players = _cluster_boot(con, """
        SELECT player_id, (CASE WHEN same_session THEN 'in:' ELSE 'brk:' END) || k_loss, count(*), sum(resid)
        FROM pg WHERE k_win = 0 OR k_loss = 0 GROUP BY ALL""", groups)
    tilt = {"by_prior_loss_streak": []}
    for j, k in enumerate(ks):
        i_in, i_br = groups.index(f"in:{k}"), groups.index(f"brk:{k}")
        tilt["by_prior_loss_streak"].append({
            "prior_losses": f"{k}+" if k == 5 else k,
            "same_session": {"games": int(ns[i_in]), "mean_resid": round(float(point[i_in]), 5), "ci95": _ci(reps[:, i_in])},
            "after_break": {"games": int(ns[i_br]), "mean_resid": round(float(point[i_br]), 5), "ci95": _ci(reps[:, i_br])},
            "tilt_effect_same_minus_break": round(float(point[i_in] - point[i_br]), 5),
            "tilt_effect_ci95": _ci(reps[:, i_in] - reps[:, i_br]),
        })
    # headline: 3+ straight losses, same session vs baseline (same session, previous game not a loss)
    _, reps3, ns3, _ = _cluster_boot(con, """
        SELECT player_id, CASE WHEN k_loss >= 3 THEN 'streak3' WHEN k_loss = 0 THEN 'base' END, count(*), sum(resid)
        FROM pg WHERE same_session GROUP BY ALL""", ["streak3", "base"])
    pt3 = con.execute("""SELECT avg(resid) FILTER (WHERE k_loss >= 3), avg(resid) FILTER (WHERE k_loss = 0)
                         FROM pg WHERE same_session""").fetchone()
    tilt["headline"] = {
        "definition": "same-session games after >=3 consecutive losses vs same-session games whose previous game was not a loss; outcome = score - Elo expected score",
        "streak3_games": int(ns3[0]), "baseline_games": int(ns3[1]),
        "streak3_mean_resid": round(pt3[0], 5), "baseline_mean_resid": round(pt3[1], 5),
        "difference": round(pt3[0] - pt3[1], 5),
        "difference_ci95": _ci(reps3[:, 0] - reps3[:, 1]),
        "difference_pct_points": round(100 * (pt3[0] - pt3[1]), 2),
    }

    # ---- Q2 session ending ------------------------------------------------------
    qgroups = ["win", "draw"] + [f"loss{k}" for k in range(1, 6)]
    qpoint, qreps, qns, _ = _cluster_boot(con, f"""
        SELECT player_id,
               CASE WHEN score = 1 THEN 'win' WHEN score = 0.5 THEN 'draw' ELSE 'loss' || loss_streak_incl END,
               count(*),
               sum((next_ts IS NULL OR epoch(next_ts - start_ts) / 60 > {gap})::INT)
        FROM pg WHERE start_ts < data_end - INTERVAL 1 DAY GROUP BY ALL""", qgroups)
    quit_ = {g: {"games": int(qns[i]), "p_session_ends": round(float(qpoint[i]), 4), "ci95": _ci(qreps[:, i])}
             for i, g in enumerate(qgroups)}
    quit_["loss3_vs_win_ratio"] = round(float(qpoint[qgroups.index("loss3")] / qpoint[0]), 3)
    quit_["loss3_minus_win_pct_points"] = round(100 * float(qpoint[qgroups.index("loss3")] - qpoint[0]), 2)
    quit_["loss3_minus_win_ci95"] = _ci(100 * (qreps[:, qgroups.index("loss3")] - qreps[:, 0]))

    # ---- Q3 matchmaking calibration ---------------------------------------------
    calib = con.execute("""
        SELECT bucket, count(*) AS games, round(avg(expected_score), 4) AS expected, round(avg(score), 4) AS observed
        FROM (SELECT *, least(floor(expected_score * 10), 9)::INT AS bucket FROM fact_player_game
              WHERE perf <> 'Correspondence' AND color = 'white')
        GROUP BY 1 ORDER BY 1""").fetchall()
    tot = sum(r[1] for r in calib)
    ece = sum(r[1] * abs(r[2] - r[3]) for r in calib) / tot
    brier = con.execute("""
        SELECT avg(pow(score - expected_score, 2)), avg(pow(score - 0.5, 2)),
               avg((abs(rating_gap) > 200)::INT)
        FROM fact_player_game WHERE perf <> 'Correspondence' AND color = 'white'""").fetchone()
    con.close()
    return {
        "sample": {"player_games": n_rows, "players": n_players, "session_gap_minutes": gap,
                   "bootstrap_reps": B,
                   "excludes": f"Correspondence games; each player's first {MIN_PRIOR_GAMES} games in a speed (provisional rating)"},
        "tilt": tilt,
        "session_end": quit_,
        "calibration": {
            "deciles": [{"expected_bucket": f"{r[0]/10:.1f}-{(r[0]+1)/10:.1f}", "games": r[1],
                         "mean_expected": r[2], "mean_observed": r[3]} for r in calib],
            "expected_calibration_error": round(ece, 4),
            "brier_elo": round(brier[0], 4), "brier_coinflip_baseline": round(brier[1], 4),
            "share_games_rating_gap_over_200": round(brier[2], 4),
        },
    }
