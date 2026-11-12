"""Query benchmark: the same analyst question answered three ways.

Question: for Blitz games played in June 2013 (UTC calendar month), how many
games, what white score, and what mean rating per opening family (top 10)?

  raw_scan_all       stream-decompress and parse every raw PGN dump (no layout knowledge)
  raw_scan_2_files   parse only the 2013-06 and 2013-07 dumps (needs to know that the dump
                     boundary is Paris midnight, so late-June games sit in the July file)
  parquet_flat       DuckDB over one unpartitioned Parquet file of the same silver rows
  gold_partitioned   DuckDB over gold fact_game with month-partition pruning + dim_opening
"""
from __future__ import annotations

import statistics
import time
from collections import defaultdict

import duckdb

from .config import Config
from .pgn import iter_games

LO, HI = "2013-06-01 00:00:00", "2013-07-01 00:00:00"


def _raw(cfg: Config, months: list[str]):
    agg = defaultdict(lambda: [0, 0.0, 0])
    for m in months:
        for g in iter_games(str(cfg.raw_file(m))):
            if not (g["Event"] or "").startswith("Rated Blitz"):
                continue
            ts = (g["UTCDate"] or "").replace(".", "-") + " " + (g["UTCTime"] or "")
            if not (LO <= ts < HI):
                continue
            try:
                we = int(g["WhiteElo"])
                int(g["BlackElo"])
            except (TypeError, ValueError):
                continue  # same rule silver applies (missing ratings are quarantined)
            sc = {"1-0": 1.0, "0-1": 0.0, "1/2-1/2": 0.5}.get(g["Result"])
            if sc is None:
                continue
            fam = (g["Opening"] or "").split(":")[0]
            a = agg[fam]
            a[0] += 1
            a[1] += sc
            a[2] += we
    top = sorted(agg.items(), key=lambda kv: (-kv[1][0], kv[0]))[:10]
    return [(k, v[0], round(v[1] / v[0], 4)) for k, v in top]


SQL_FLAT = f"""
SELECT opening_family, count(*) n, round(avg(white_score), 4) ws
FROM read_parquet('{{p}}') WHERE speed = 'Blitz' AND start_ts >= '{LO}' AND start_ts < '{HI}'
GROUP BY 1 ORDER BY n DESC, 1 LIMIT 10"""

SQL_GOLD = f"""
SELECT o.opening_family, count(*) n, round(avg(f.white_score), 4) ws
FROM fact_game f JOIN dim_opening o USING (opening_key)
WHERE f.month IN ('2013-06', '2013-07') AND f.speed = 'Blitz'
  AND f.start_ts >= '{LO}' AND f.start_ts < '{HI}'
GROUP BY 1 ORDER BY n DESC, 1 LIMIT 10"""


def _timed(fn, reps):
    times, out = [], None
    for _ in range(reps):
        t = time.perf_counter()
        out = fn()
        times.append(time.perf_counter() - t)
    return statistics.median(times), out


def run_bench(cfg: Config, months: list[str], reps: int = 5) -> dict:
    flat = cfg.lake / "bench" / "silver_flat.parquet"
    flat.parent.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect()
    c.execute(f"""COPY (SELECT * FROM read_parquet('{cfg.table_glob('silver', 'games')}', hive_partitioning=false)
                ORDER BY game_id) TO '{flat}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    c.close()

    t_all, r_all = _timed(lambda: _raw(cfg, months), 1)
    t_two, r_two = _timed(lambda: _raw(cfg, ["2013-06", "2013-07"]), 3)
    con = duckdb.connect()
    t_flat, r_flat = _timed(lambda: con.execute(SQL_FLAT.format(p=flat)).fetchall(), reps)
    con.close()
    wh = duckdb.connect(str(cfg.warehouse), read_only=True)
    t_gold, r_gold = _timed(lambda: wh.execute(SQL_GOLD).fetchall(), reps)
    wh.close()
    norm = lambda rows: [(a, int(b), float(c)) for a, b, c in rows]
    assert norm(r_all) == norm(r_two) == norm(r_flat) == norm(r_gold), "all methods must return identical answers"
    return {
        "question": "Blitz games in June 2013 (UTC): count and white score by opening family, top 10",
        "answers_identical_across_methods": True,
        "top_family": r_gold[0][0], "top_family_games": int(r_gold[0][1]),
        "seconds": {"raw_scan_all_files": round(t_all, 4), "raw_scan_2_files": round(t_two, 4),
                    "parquet_flat_unpartitioned": round(t_flat, 4), "gold_partitioned": round(t_gold, 4)},
        "speedup_gold_vs_raw_all": round(t_all / t_gold, 1),
        "speedup_gold_vs_raw_2_files": round(t_two / t_gold, 1),
        "speedup_gold_vs_flat_parquet": round(t_flat / t_gold, 2),
        "repetitions": {"raw_scan_all_files": 1, "raw_scan_2_files": 3, "parquet": reps},
    }
