"""Bronze -> silver: type, conform, dedupe, and enforce the data contract.

Rows that fail a `quarantine` rule are routed to silver.quarantine with the list
of failed rule ids (nothing is silently dropped). Rows that fail a `warn` rule
stay in silver with the rule ids recorded in `dq_flags`."""
from __future__ import annotations

import json
from pathlib import Path

from .config import Config
from .io import connect, write_partition

CONTRACT = json.loads((Path(__file__).resolve().parent.parent / "contracts" / "silver_games.json").read_text())

TYPED_SQL = """
WITH b AS (SELECT * FROM read_parquet(?, hive_partitioning = false)),
typed AS (
  SELECT
    regexp_extract(site, '[^/]+$') AS game_id,
    month,
    CASE WHEN event LIKE 'Rated % game' OR event LIKE 'Rated % tournament%'
         THEN split_part(event, ' ', 2) END AS speed,
    event LIKE '%tournament%' AS is_tournament,
    nullif(regexp_extract(event, 'tournament/([A-Za-z0-9]+)', 1), '') AS tournament_id,
    lower(white) AS white_id, lower(black) AS black_id,
    white AS white_name, black AS black_name,
    try_strptime(utcdate || ' ' || utctime, '%Y.%m.%d %H:%M:%S') AS start_ts,
    try_cast(whiteelo AS INTEGER) AS white_elo,
    try_cast(blackelo AS INTEGER) AS black_elo,
    try_cast(whiteratingdiff AS INTEGER) AS white_rating_diff,
    try_cast(blackratingdiff AS INTEGER) AS black_rating_diff,
    nullif(whitetitle, '') AS white_title, nullif(blacktitle, '') AS black_title,
    eco, opening,
    split_part(opening, ':', 1) AS opening_family,
    timecontrol AS time_control,
    try_cast(split_part(timecontrol, '+', 1) AS INTEGER) AS base_seconds,
    try_cast(split_part(timecontrol, '+', 2) AS INTEGER) AS increment_seconds,
    termination, result,
    CAST(CASE result WHEN '1-0' THEN 1.0 WHEN '0-1' THEN 0.0 WHEN '1/2-1/2' THEN 0.5 END AS DOUBLE) AS white_score,
    num_moves, source_file, source_sha256, record_seq
  FROM b
)
SELECT *, row_number() OVER (PARTITION BY game_id ORDER BY record_seq) AS dup_rank FROM typed
"""


def _rules(severity: str) -> list[dict]:
    return [r for r in CONTRACT["rules"] if r["severity"] == severity]


def _failed_list_sql(rules: list[dict]) -> str:
    items = ", ".join(f"CASE WHEN coalesce(({r['pass_if']}), false) THEN NULL ELSE '{r['id']}' END" for r in rules)
    return f"list_filter([{items}], lambda x: x IS NOT NULL)"


def build_silver(cfg: Config, month: str) -> dict:
    con = connect()
    con.execute(f"CREATE TEMP TABLE t AS {TYPED_SQL}", [str(cfg.part("bronze", "games", month))])
    q = _failed_list_sql(_rules("quarantine"))
    w = _failed_list_sql(_rules("warn"))
    con.execute(f"CREATE TEMP TABLE c AS SELECT *, {q} AS q_failed, {w} AS dq_flags FROM t")
    n_silver = write_partition(
        con,
        "SELECT * EXCLUDE (dup_rank, q_failed) FROM c WHERE len(q_failed) = 0 ORDER BY game_id",
        cfg.part("silver", "games", month),
    )
    n_quar = write_partition(
        con,
        "SELECT q_failed AS failed_rules, * EXCLUDE (q_failed, dq_flags) FROM c "
        "WHERE len(q_failed) > 0 ORDER BY record_seq",
        cfg.part("silver", "quarantine", month),
    )
    n_in = con.execute("SELECT count(*) FROM t").fetchone()[0]
    con.close()
    assert n_in == n_silver + n_quar, "silver must conserve rows: in = silver + quarantine"
    return {"rows_in": n_in, "silver_rows": n_silver, "quarantined": n_quar}
