# Tiltmeter spec

## Problem
Game data teams turn raw match logs into clean, documented, queryable tables that
analysts and game teams can trust, then use them to answer player-experience
questions. I wanted to do that end to end on real competitive-game data: the
Lichess open database (CC0), 2.3M rated games from Jan to Oct 2013.

The player question: **is "tilt" real?** After a run of losses, do players
perform worse than their rating predicts, or do they just stop playing?

## Why it matters to a game data team
Loss streaks are the classic churn moment. If tilt hurts play, matchmaking could
soften the next game; if it only drives quitting, the lever is the session-end
experience instead. Either answer is only as good as the pipeline under it, so
the pipeline (incremental loads, contracts, quality checks, a documented star
schema) is half the project.

## Design
- **Raw**: monthly `.pgn.zst` dumps, never decompressed to disk (streamed).
- **Bronze**: one Parquet partition per dump month, every tag as text, plus
  lineage (file, sha256, record position).
- **Silver**: typed and conformed (lowercased player ids, parsed time control,
  numeric score), deduplicated, and checked against a JSON data contract.
  Hard-rule failures go to a quarantine table with the failing rule ids; soft
  rules are recorded as `dq_flags`.
- **Gold**: star schema. `fact_game`, `fact_player_game` (streaks, sessions,
  rating continuity, expected score), `dim_player` (SCD Type 2 over a
  Riot-style ladder tier per speed), `dim_opening`, `dim_date`. Facts stay as
  month-partitioned Parquet behind DuckDB views; dims are DuckDB tables.
- **Incremental and idempotent**: each gold month reads a small carried-state
  snapshot from the previous month, so a month is a self-contained unit.
  Tasks fingerprint their inputs. Unchanged inputs are skipped; a corrected
  month re-runs its own partitions, and later months re-run only if the
  carried player state actually changed.
- **Orchestration**: a small DAG runner (dependencies, worker pool, retries
  with backoff, backfill). Airflow was not installed because of its footprint;
  the graph is one task per layer per month and maps directly onto an Airflow DAG.

## Metrics and baselines
1. **Pipeline correctness under re-runs**: rows processed and cold runtime;
   no-op re-run (tasks skipped); forced full rebuild diffed row by row against
   a snapshot with `EXCEPT ALL` both ways (target 0 rows). Restatement scope
   for a simulated corrected month: which partitions change, and the row diff
   on untouched months (target 0). Baseline: a full rebuild.
2. **Quality**: number of checks, real anomalies caught with counts.
   Baseline: what a plain load would have passed through silently.
3. **Query speed**: one analyst question answered by (a) parsing raw dumps,
   (b) DuckDB over a single unpartitioned Parquet file, (c) the partitioned
   gold star schema. All three must return identical answers.
4. **Finding**: mean (score minus Elo expected score) after 3+ same-session
   losses vs. same-session games after a non-loss, with a player-clustered
   bootstrap 95% CI; the same streak after a break as a control for rating
   lag; and session-end probability by result streak.

Nothing is simulated except one clearly labelled restatement scenario (a month
re-published with every 100th game removed) used to test restatement scope.
