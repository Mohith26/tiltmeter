"""Publish the gold star schema into a DuckDB warehouse.

Facts stay as month-partitioned Parquet and are exposed as views (so queries
get partition pruning). Small dimensions are materialised as tables:
  dim_player   SCD Type 2: one row per (player, perf) tier stint
  dim_opening  one row per (ECO, opening name)
  dim_date     one row per calendar day covered by the facts
"""
from __future__ import annotations

import os

from .config import Config
from .io import connect


def publish(cfg: Config) -> dict:
    tmp = cfg.warehouse.with_suffix(".duckdb.tmp")
    if tmp.exists():
        tmp.unlink()
    con = connect()
    con.execute(f"ATTACH '{tmp}' AS wh")
    for t in ("fact_game", "fact_player_game"):
        con.execute(f"""CREATE VIEW wh.{t} AS SELECT * FROM
            read_parquet('{cfg.table_glob('gold', t)}', hive_partitioning = true)""")
    con.execute(f"""
    CREATE TABLE wh.dim_player AS
    SELECT player_sk, player_id, player_name, perf, tier, title,
           valid_from,
           lead(valid_from) OVER w AS valid_to,
           lead(valid_from) OVER w IS NULL AS is_current,
           row_number() OVER w AS version
    FROM read_parquet('{cfg.table_glob('gold', 'tier_events')}', hive_partitioning = false)
    WINDOW w AS (PARTITION BY player_id, perf ORDER BY valid_from)
    ORDER BY player_id, perf, valid_from""")
    con.execute(f"""
    CREATE TABLE wh.dim_opening AS
    SELECT opening_key, eco, opening, opening_family, left(eco, 1) AS eco_volume
    FROM read_parquet('{cfg.table_glob('gold', 'fact_game')}', hive_partitioning = false)
    GROUP BY ALL ORDER BY eco, opening""")
    con.execute("""
    CREATE TABLE wh.dim_date AS
    WITH r AS (SELECT min(start_ts)::DATE lo, max(start_ts)::DATE hi FROM wh.fact_game)
    SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key, d::DATE AS date,
           year(d) AS year, month(d) AS month, strftime(d, '%Y-%m') AS year_month,
           dayname(d) AS day_name, isodow(d) AS iso_dow, isodow(d) >= 6 AS is_weekend
    FROM r, range(r.lo, r.hi + INTERVAL 1 DAY, INTERVAL 1 DAY) t(d) ORDER BY 1""")
    counts = {t: con.execute(f"SELECT count(*) FROM wh.{t}").fetchone()[0]
              for t in ("fact_game", "fact_player_game", "dim_player", "dim_opening", "dim_date")}
    con.execute("DETACH wh")
    con.close()
    os.replace(tmp, cfg.warehouse)
    return counts
