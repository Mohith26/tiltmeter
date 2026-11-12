"""Reproduce every number in results/results.json from the raw Lichess dumps.

    python scripts/download.py          # ~327 MB, 2013-01 .. 2013-10
    python scripts/reproduce.py         # ~2-3 minutes on a laptop

Stages: cold pipeline run -> no-op incremental re-run -> forced full re-run
diffed row-by-row against a snapshot (idempotency) -> simulated corrected
source file for one month (restatement scope) -> restore -> data quality ->
query benchmark -> player-experience analysis.
"""
from __future__ import annotations

import io
import json
import os
import platform
import shutil
import sys
import time
from pathlib import Path

import duckdb
import zstandard

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("TILT_ROOT", str(ROOT))

from tiltmeter.analysis import run_analysis  # noqa: E402
from tiltmeter.bench import run_bench  # noqa: E402
from tiltmeter.config import DEFAULT_MONTHS, Config  # noqa: E402
from tiltmeter.dag import Runner  # noqa: E402

TABLES = [("bronze", "games"), ("silver", "games"), ("silver", "quarantine"),
          ("gold", "fact_game"), ("gold", "fact_player_game"), ("gold", "tier_events"),
          ("gold", "_state_player"), ("gold", "_state_perf")]
CORRECTED_MONTH = "2013-08"


def diff_rows(snap: Path, cfg: Config) -> dict:
    """Rows present in exactly one of (snapshot, current), per table and month, via EXCEPT ALL both ways."""
    con = duckdb.connect()
    out = {}
    for layer, t in TABLES:
        per_month = {}
        for m in DEFAULT_MONTHS:
            a = snap / layer / t / f"month={m}" / "data.parquet"
            b = cfg.part(layer, t, m)
            q = f"""SELECT (SELECT count(*) FROM (SELECT * FROM read_parquet('{a}', hive_partitioning=false)
                                                  EXCEPT ALL SELECT * FROM read_parquet('{b}', hive_partitioning=false)))
                         + (SELECT count(*) FROM (SELECT * FROM read_parquet('{b}', hive_partitioning=false)
                                                  EXCEPT ALL SELECT * FROM read_parquet('{a}', hive_partitioning=false)))"""
            per_month[m] = con.execute(q).fetchone()[0]
        out[f"{layer}.{t}"] = per_month
    con.close()
    return out


def write_corrected(src: Path, dst: Path, drop_every: int = 100) -> int:
    """Simulate a re-published month: drop every Nth game (as if removed by moderation)."""
    dropped, game_idx, keep = 0, -1, True
    reader = io.TextIOWrapper(zstandard.ZstdDecompressor().stream_reader(open(src, "rb")), encoding="utf-8")
    with open(dst, "wb") as fh, zstandard.ZstdCompressor(level=3).stream_writer(fh) as w:
        for line in reader:
            if line.startswith("[Event "):
                game_idx += 1
                keep = game_idx % drop_every != 0
                dropped += 0 if keep else 1
            if keep:
                w.write(line.encode("utf-8"))
    return dropped


def counts(cfg: Config) -> dict:
    con = duckdb.connect(str(cfg.warehouse), read_only=True)
    g = lambda layer, t: f"read_parquet('{cfg.table_glob(layer, t)}', hive_partitioning=false)"
    r = {
        "bronze_rows": con.execute(f"SELECT count(*) FROM {g('bronze', 'games')}").fetchone()[0],
        "silver_rows": con.execute(f"SELECT count(*) FROM {g('silver', 'games')}").fetchone()[0],
        "quarantine_rows": con.execute(f"SELECT count(*) FROM {g('silver', 'quarantine')}").fetchone()[0],
        "fact_game_rows": con.execute("SELECT count(*) FROM fact_game").fetchone()[0],
        "fact_player_game_rows": con.execute("SELECT count(*) FROM fact_player_game").fetchone()[0],
        "dim_player_rows": con.execute("SELECT count(*) FROM dim_player").fetchone()[0],
        "dim_player_distinct_players": con.execute("SELECT count(DISTINCT player_id) FROM dim_player").fetchone()[0],
        "dim_opening_rows": con.execute("SELECT count(*) FROM dim_opening").fetchone()[0],
        "dim_date_rows": con.execute("SELECT count(*) FROM dim_date").fetchone()[0],
        "raw_bytes": sum(cfg.raw_file(m).stat().st_size for m in DEFAULT_MONTHS),
    }
    con.close()
    return r


def main():
    cfg = Config(ROOT)
    months = DEFAULT_MONTHS
    res: dict = {"source": "Lichess open database, standard rated games, monthly dumps (CC0)",
                 "months": months,
                 "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__,
                                 "machine": f"{platform.system()} {platform.machine()}", "cpus": os.cpu_count()}}

    # 1. cold run
    if cfg.lake.exists():
        shutil.rmtree(cfg.lake)
    r1 = Runner(cfg).run(months)
    res["pipeline"] = {"cold_run_seconds": r1["wall_seconds"], "tasks_run": len(r1["ran"]),
                       "executor": r1["executor"], "note": "excludes download; extract task only verifies and hashes the raw file",
                       **counts(cfg)}
    res["pipeline"]["games_per_second"] = round(res["pipeline"]["bronze_rows"] / r1["wall_seconds"])
    res["pipeline"]["task_seconds"] = {t: v["seconds"] for t, v in Runner(cfg).state.items()}

    # 2. no-op incremental re-run
    r2 = Runner(cfg).run(months)
    snap = ROOT / "lake_snapshot"
    if snap.exists():
        shutil.rmtree(snap)
    for layer in ("bronze", "silver", "gold"):
        shutil.copytree(cfg.lake / layer, snap / layer)

    # 3. forced full rebuild, diffed row by row
    r3 = Runner(cfg).run(months, force=True)
    d3 = diff_rows(snap, cfg)
    res["idempotency"] = {
        "noop_rerun_tasks_run": len(r2["ran"]) - (1 if "quality" in r2["ran"] else 0),
        "noop_rerun_tasks_skipped": len(r2["skipped"]), "noop_rerun_seconds": r2["wall_seconds"],
        "forced_rebuild_seconds": r3["wall_seconds"], "forced_rebuild_tasks_run": len(r3["ran"]),
        "forced_rebuild_row_diff_total": int(sum(sum(v.values()) for v in d3.values())),
        "tables_compared": len(d3), "partitions_compared": int(sum(len(v) for v in d3.values())),
    }

    # 4. corrected source file for one month
    raw = cfg.raw_file(CORRECTED_MONTH)
    orig = raw.with_suffix(".orig")
    shutil.move(raw, orig)
    try:
        dropped = write_corrected(orig, raw)
        r4 = Runner(cfg).run(months)
        d4 = diff_rows(snap, cfg)
    finally:
        shutil.move(orig, raw)
    r5 = Runner(cfg).run(months)
    d5 = diff_rows(snap, cfg)
    changed_months = sorted({m for v in d4.values() for m, n in v.items() if n})
    res["restatement"] = {
        "simulated": True,
        "scenario": f"{CORRECTED_MONTH} dump re-published with every 100th game removed ({dropped} games)",
        "games_removed": dropped,
        "tasks_run": [t for t in r4["ran"]],
        "tasks_skipped": len(r4["skipped"]),
        "months_with_changed_rows": changed_months,
        "months_untouched_row_diff": int(sum(n for v in d4.values() for m, n in v.items() if m not in changed_months)),
        "changed_rows_by_table": {k: int(sum(v.values())) for k, v in d4.items()},
        "fact_game_rows_changed_by_month": {m: int(n) for m, n in d4["gold.fact_game"].items() if n},
        "fact_player_game_rows_changed_by_month": {m: int(n) for m, n in d4["gold.fact_player_game"].items() if n},
        "gold_months_restated": [t[5:12] for t in r4["ran"] if t.startswith("gold[")],
        "restore_tasks_run": len(r5["ran"]),
        "restore_row_diff_total": int(sum(sum(v.values()) for v in d5.values())),
    }
    shutil.rmtree(snap)

    # 5. data quality (from the restored, final state)
    res["quality"] = json.loads((cfg.lake / "dq" / "dq_results.json").read_text())
    q = {c["check"]: c["offending_rows"] for c in res["quality"]["checks"]}
    res["quality"]["continuity_break_rate"] = round(q["rating_continuity_break"] / res["quality"]["continuity_checkable_rows"], 4)

    # 6. benchmark, 7. analysis
    res["benchmark"] = run_bench(cfg, months)
    res["analysis"] = run_analysis(cfg)

    out = ROOT / "results" / "results.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(res, indent=2, default=str) + "\n")
    print(json.dumps({k: res[k] for k in ("pipeline", "idempotency", "restatement")}, indent=1, default=str))


if __name__ == "__main__":
    main()
