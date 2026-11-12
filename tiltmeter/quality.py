"""Data quality checks over the published warehouse.

Hard invariants (uniqueness, referential integrity, conservation, SCD2 shape)
FAIL the run. Anomaly checks (continuity, timeliness, accuracy, completeness)
WARN with the real count so the anomaly is visible instead of hidden."""
from __future__ import annotations

import json

import duckdb

from .config import Config

CHECKS = [
    # (id, dimension, severity, SQL returning the number of offending rows)
    ("pk_fact_game", "uniqueness", "fail",
     "SELECT count(*) - count(DISTINCT game_id) FROM fact_game"),
    ("pk_fact_player_game", "uniqueness", "fail",
     "SELECT count(*) - count(DISTINCT (game_id, color)) FROM fact_player_game"),
    ("pk_dim_player", "uniqueness", "fail",
     "SELECT count(*) - count(DISTINCT player_sk) FROM dim_player"),
    ("ri_player_sk", "referential_integrity", "fail",
     "SELECT count(*) FROM fact_player_game f ANTI JOIN dim_player d USING (player_sk)"),
    ("ri_game_player_sks", "referential_integrity", "fail",
     "SELECT count(*) FROM fact_game f WHERE white_sk NOT IN (SELECT player_sk FROM dim_player) "
     "OR black_sk NOT IN (SELECT player_sk FROM dim_player)"),
    ("ri_opening_key", "referential_integrity", "fail",
     "SELECT count(*) FROM fact_game f ANTI JOIN dim_opening d USING (opening_key)"),
    ("ri_date_key", "referential_integrity", "fail",
     "SELECT count(*) FROM fact_game f ANTI JOIN dim_date d USING (date_key)"),
    ("two_rows_per_game", "referential_integrity", "fail",
     "SELECT count(*) FROM (SELECT game_id FROM fact_player_game GROUP BY 1 HAVING count(*) <> 2) "
     "UNION ALL SELECT 0 LIMIT 1"),
    ("scd2_one_current_version", "consistency", "fail",
     "SELECT count(*) FROM (SELECT player_id, perf FROM dim_player GROUP BY ALL HAVING sum(is_current::INT) <> 1)"),
    ("scd2_point_in_time", "consistency", "fail",
     "SELECT count(*) FROM fact_player_game f JOIN dim_player d USING (player_sk) "
     "WHERE f.start_ts < d.valid_from OR (d.valid_to IS NOT NULL AND f.start_ts >= d.valid_to)"),
    ("row_conservation", "completeness", "fail",
     "SELECT abs((SELECT count(*) FROM bronze) - (SELECT count(*) FROM silver) - (SELECT count(*) FROM quarantine))"),
    ("quarantined_rows", "validity", "warn", "SELECT count(*) FROM quarantine"),
    ("missing_rating_diff", "completeness", "warn",
     "SELECT count(*) FROM fact_game WHERE white_rating_diff IS NULL OR black_rating_diff IS NULL"),
    ("missing_month_partition", "completeness", "fail",
     "SELECT count(*) FROM expected_months e ANTI JOIN (SELECT DISTINCT month FROM fact_game) f USING (month)"),
    ("days_without_games", "completeness", "warn",
     "SELECT count(*) FROM dim_date d ANTI JOIN (SELECT DISTINCT date_key FROM fact_game) f USING (date_key)"),
    ("rating_continuity_break", "consistency", "warn",
     "SELECT count(*) FROM fact_player_game WHERE rating_continuity_break"),
    ("rating_change_sign_mismatch", "accuracy", "warn",
     "SELECT count(*) FROM fact_player_game WHERE (score = 1 AND rating_delta < 0) OR (score = 0 AND rating_delta > 0)"),
    ("decisive_result_zero_moves", "validity", "warn",
     "SELECT count(*) FROM fact_game WHERE num_moves = 0 AND result <> '1/2-1/2'"),
    ("late_arriving_games", "timeliness", "warn",
     "SELECT count(*) FROM fact_game WHERE strftime(start_ts, '%Y-%m') <> month"),
    ("out_of_order_vs_carried_state", "timeliness", "warn",
     "SELECT count(*) FROM fact_player_game WHERE out_of_order"),
]


def run_checks(cfg: Config, months: list[str]) -> dict:
    con = duckdb.connect(str(cfg.warehouse), read_only=True)
    con.execute(f"CREATE TEMP VIEW bronze AS SELECT * FROM read_parquet('{cfg.table_glob('bronze', 'games')}', hive_partitioning=false)")
    con.execute(f"CREATE TEMP VIEW silver AS SELECT * FROM read_parquet('{cfg.table_glob('silver', 'games')}', hive_partitioning=false)")
    con.execute(f"CREATE TEMP VIEW quarantine AS SELECT * FROM read_parquet('{cfg.table_glob('silver', 'quarantine')}', hive_partitioning=false)")
    con.execute("CREATE TEMP TABLE expected_months(month VARCHAR)")
    con.executemany("INSERT INTO expected_months VALUES (?)", [[m] for m in months])
    results = []
    for cid, dim, sev, sql in CHECKS:
        n = int(con.execute(sql).fetchone()[0])
        status = "pass" if n == 0 else ("FAIL" if sev == "fail" else "warn")
        results.append({"check": cid, "dimension": dim, "severity": sev, "offending_rows": n, "status": status})
    extra = {
        "continuity_checkable_rows": con.execute(
            "SELECT count(*) FROM fact_player_game WHERE rating_continuity_break IS NOT NULL").fetchone()[0],
        "quarantine_by_rule": dict(con.execute(
            "SELECT r, count(*) FROM (SELECT unnest(failed_rules) r FROM quarantine) GROUP BY 1 ORDER BY 1").fetchall()),
        "warn_flags_by_rule": dict(con.execute(
            "SELECT r, count(*) FROM (SELECT unnest(dq_flags) r FROM silver) GROUP BY 1 ORDER BY 1").fetchall()),
        "max_late_arrival_hours": con.execute(
            "SELECT round(max(epoch(date_trunc('month', strptime(month, '%Y-%m')) - start_ts)) / 3600, 2) "
            "FROM fact_game WHERE strftime(start_ts, '%Y-%m') <> month").fetchone()[0],
    }
    con.close()
    summary = {
        "checks": results,
        "n_checks": len(results),
        "n_hard_checks": sum(r["severity"] == "fail" for r in results),
        "n_fail": sum(r["status"] == "FAIL" for r in results),
        "n_warn": sum(r["status"] == "warn" for r in results),
        **extra,
    }
    out = cfg.lake / "dq" / "dq_results.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, default=str))
    if summary["n_fail"]:
        raise RuntimeError(f"{summary['n_fail']} hard data-quality checks failed: "
                           + ", ".join(r["check"] for r in results if r["status"] == "FAIL"))
    return summary
