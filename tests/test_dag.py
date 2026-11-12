import duckdb
import pytest

from conftest import MONTHS, fixture_months, game, write_month
from tiltmeter import dag as dagmod
from tiltmeter.dag import Runner


def snapshot(cfg):
    con = duckdb.connect()
    out = {}
    for layer, t in [("silver", "games"), ("gold", "fact_game"), ("gold", "fact_player_game"), ("gold", "tier_events")]:
        for m in MONTHS:
            out[(t, m)] = con.execute(f"SELECT * FROM read_parquet('{cfg.part(layer, t, m)}', hive_partitioning=false) ORDER BY ALL").fetchall()
    return out


def test_noop_rerun_skips_every_task(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    r = Runner(cfg, executor="thread").run(MONTHS)
    assert r["ran"] == [] and len(r["skipped"]) == 4 * len(MONTHS) + 2


def test_forced_rerun_is_row_identical(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    before = snapshot(cfg)
    r = Runner(cfg, executor="thread").run(MONTHS, force=True)
    assert len(r["ran"]) == 4 * len(MONTHS) + 2
    assert snapshot(cfg) == before


def test_corrected_last_month_restates_only_that_month(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    before = snapshot(cfg)
    games = fixture_months()["2013-03"][:1]  # March re-published with one game removed
    write_month(cfg, "2013-03", games)
    r = Runner(cfg, executor="thread").run(MONTHS)
    assert sorted(r["ran"]) == sorted(["extract[2013-03]", "bronze[2013-03]", "silver[2013-03]", "gold[2013-03]", "publish", "quality"])
    after = snapshot(cfg)
    assert all(after[k] == before[k] for k in before if k[1] != "2013-03")
    assert len(after[("fact_game", "2013-03")]) == 1


def test_cascade_stops_when_carried_state_is_unchanged(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    feb = fixture_months()["2013-02"]
    feb[0] = feb[0].replace("French Defense: Normal Variation", "French Defense")  # cosmetic correction
    write_month(cfg, "2013-02", feb)
    r = Runner(cfg, executor="thread").run(MONTHS)
    assert "gold[2013-02]" in r["ran"] and "gold[2013-03]" not in r["ran"]


def test_cascade_continues_when_carried_state_changes(cfg):
    Runner(cfg, executor="thread").run(MONTHS)
    feb = fixture_months()["2013-02"]
    feb[2] = feb[2].replace('"1-0"', '"0-1"').replace(" 1-0\n", " 0-1\n")  # result corrected
    write_month(cfg, "2013-02", feb)
    r = Runner(cfg, executor="thread").run(MONTHS)
    assert "gold[2013-02]" in r["ran"] and "gold[2013-03]" in r["ran"]


def test_retries_transient_failure(cfg, monkeypatch):
    calls = {"n": 0}
    real = dagmod.build_silver

    def flaky(c, m):
        calls["n"] += 1
        if calls["n"] == 1:
            raise IOError("transient")
        return real(c, m)

    monkeypatch.setattr(dagmod, "build_silver", flaky)
    r = Runner(cfg, executor="thread", workers=1, backoff=0.01).run(["2013-01"])
    assert len(r["errors"]) == 1 and "silver[2013-01]" in r["ran"]


def test_permanent_failure_raises_after_retries(cfg, monkeypatch):
    def broken(c, m):
        raise ValueError("bad")
    monkeypatch.setattr(dagmod, "build_gold", broken)
    with pytest.raises(RuntimeError, match="gold\\[2013-01\\] failed after 3 attempts"):
        Runner(cfg, executor="thread", workers=1, backoff=0.01, retries=2).run(["2013-01"])


def test_backfill_extends_existing_months(cfg):
    Runner(cfg, executor="thread").run(MONTHS[:2])
    r = Runner(cfg, executor="thread").run(MONTHS)
    assert "gold[2013-03]" in r["ran"] and "gold[2013-01]" not in r["ran"]
