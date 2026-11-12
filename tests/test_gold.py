import duckdb
import pytest

from conftest import MONTHS
from tiltmeter.dag import Runner


@pytest.fixture
def built(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    return cfg


def wh(cfg):
    return duckdb.connect(str(cfg.warehouse), read_only=True)


def test_streaks_and_sessions_carry_across_months(built):
    rows = wh(built).execute("""SELECT game_id, prior_loss_streak, prior_win_streak, session_num, game_in_session
        FROM fact_player_game WHERE player_id = 'alice' ORDER BY start_ts""").fetchall()
    assert rows == [
        ("aaaaaaa1", 0, 0, 1, 1), ("aaaaaaa2", 1, 0, 1, 2), ("aaaaaaa3", 2, 0, 1, 3),
        ("bbbbbbb1", 3, 0, 2, 1),  # streak carried over the month boundary, new session
        ("bbbbbbb2", 4, 0, 2, 2), ("bbbbbbb3", 0, 1, 2, 3),
        ("ccccccc1", 0, 2, 3, 1),
    ]


def test_rating_continuity_break_detected_exactly_once(built):
    con = wh(built)
    assert con.execute("SELECT game_id, player_id FROM fact_player_game WHERE rating_continuity_break").fetchall() == [("bbbbbbb3", "alice")]
    # the first game of each (player, speed) has nothing to compare against
    assert con.execute("SELECT count(*) FROM fact_player_game WHERE player_id='erin' AND rating_continuity_break IS NULL").fetchone()[0] == 2


def test_scd2_tier_versions_and_point_in_time_keys(built):
    con = wh(built)
    versions = con.execute("""SELECT tier, version, is_current, valid_to IS NULL FROM dim_player
        WHERE player_id='alice' AND perf='Blitz' ORDER BY valid_from""").fetchall()
    assert versions == [("Silver", 1, False, False), ("Bronze", 2, False, False), ("Silver", 3, True, True)]
    # each fact row points at the version in force when the game started
    tiers = con.execute("""SELECT f.game_id, d.tier FROM fact_player_game f JOIN dim_player d USING (player_sk)
        WHERE f.player_id='alice' ORDER BY f.start_ts""").fetchall()
    assert [t for _, t in tiers] == ["Silver"] * 5 + ["Bronze", "Silver"]


def test_star_schema_shape(built):
    con = wh(built)
    assert con.execute("SELECT count(*) FROM fact_game").fetchone()[0] == 10
    assert con.execute("SELECT count(*) FROM fact_player_game").fetchone()[0] == 20
    assert con.execute("SELECT count(*) FROM dim_opening").fetchone()[0] == 1
    lo, hi, n = con.execute("SELECT min(date), max(date), count(*) FROM dim_date").fetchone()
    assert str(lo) == "2012-12-31" and str(hi) == "2013-03-02" and n == 62
    e = con.execute("SELECT expected_score FROM fact_player_game WHERE game_id='aaaaaaa1' AND color='white'").fetchone()[0]
    assert e == pytest.approx(0.5)
    e2 = con.execute("SELECT expected_score FROM fact_player_game WHERE game_id='ccccccc2' AND color='white'").fetchone()[0]
    assert e2 == pytest.approx(1 / (1 + 10 ** ((1488 - 1602) / 400)))


def test_quality_checks_pass_and_report_anomalies(built):
    import json
    dq = json.loads((built.lake / "dq" / "dq_results.json").read_text())
    by = {c["check"]: c for c in dq["checks"]}
    assert dq["n_fail"] == 0
    assert by["rating_continuity_break"]["offending_rows"] == 1
    assert by["decisive_result_zero_moves"]["offending_rows"] == 1
    assert by["late_arriving_games"]["offending_rows"] == 1
    assert by["quarantined_rows"]["offending_rows"] == 3
    assert dq["quarantine_by_rule"]["q_duplicate_game_id"] == 1


def test_quality_fails_on_broken_referential_integrity(built):
    from tiltmeter.quality import run_checks
    con = duckdb.connect(str(built.warehouse))
    con.execute("DELETE FROM dim_player WHERE player_id = 'bob'")
    con.close()
    with pytest.raises(RuntimeError, match="ri_player_sk"):
        run_checks(built, MONTHS)
