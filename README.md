# Tiltmeter

I built a production-style data pipeline over real competitive-game data (2.3M rated
games from the Lichess open database) and used the tables it produces to answer a
player-experience question: **is tilt real?** When a player loses several games in a
row, do they start playing below their rating, or do they just leave?

Short answer from the data: losing streaks do not make players play worse than the
rating gap predicts (they slightly outperform it, because their rating has dropped),
but they do make players quit. After a third straight loss, a session ends 25.4% of
the time versus 20.4% after a win.

## What it does

```
raw .pgn.zst (10 monthly dumps, 343 MB, streamed, never decompressed to disk)
  -> bronze   Parquet per month, every tag as text + lineage (file, sha256, position)
  -> silver   typed, conformed, deduplicated; JSON data contract; failures -> quarantine
  -> gold     star schema in DuckDB
                fact_game, fact_player_game (streaks, sessions, expected score, continuity)
                dim_player (SCD Type 2 ladder tier per speed), dim_opening, dim_date
  -> quality  20 checks (uniqueness, referential integrity, completeness, consistency,
              accuracy, timeliness)
  -> analysis tilt, session-ending, matchmaking calibration, bootstrap CIs
```

- **Incremental and idempotent by month.** Each task fingerprints its inputs and is
  skipped when nothing changed. Gold month M reads a small carried-state snapshot
  (streaks, sessions, last rating, current tier) written by month M-1, so a month is a
  self-contained unit. If a raw file is corrected, only that month's partitions are
  rewritten, and later months re-run only if the carried player state actually changed.
  Partition writes are atomic (temp file + rename).
- **Data contract** (`contracts/silver_games.json`): quarantine rules (bad key, unfinished
  result, missing or impossible ratings, self-play, duplicate id) and warn rules (missing
  rating change, decisive result with zero moves, late-arriving record, abnormal
  termination, unknown opening). Nothing is dropped silently: bronze rows = silver +
  quarantine is itself a check.
- **Orchestration**: a small DAG runner (`tiltmeter/dag.py`) with dependencies, a worker
  pool, retries with exponential backoff, and backfill. I did not install Airflow
  because of its size; the graph is one task per layer per month with gold chained month
  to month, so it maps directly onto an Airflow DAG.
- **Documentation**: every gold table and column is in [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md).

## Run it

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/download.py        # 10 dumps, 2013-01..2013-10, about 343 MB
python -m tiltmeter run           # whole pipeline; re-run it and every task is skipped
python scripts/reproduce.py       # every number below -> results/results.json (about 3.5 min)
python -m pytest -q tests         # 21 tests on small fixture months, no download needed
```

## Results

All numbers come from one run of `scripts/reproduce.py` (Apple Silicon laptop, Python 3.13,
DuckDB 2.0, thread pool executor) and are in [results/results.json](results/results.json).

**Pipeline**

| metric | value |
|---|---|
| games parsed (bronze rows) | 2,321,149 |
| games in gold `fact_game` | 2,317,796 |
| player-game rows in `fact_player_game` | 4,635,592 |
| `dim_player` SCD2 versions / distinct players | 535,696 / 53,209 |
| cold run, all 42 tasks (download excluded) | 37.9 s |
| no-op re-run | 42 of 42 tasks skipped, 0.83 s |
| forced full rebuild vs snapshot (`EXCEPT ALL` both ways, 8 tables x 10 months) | **0 rows differ** |

**Restatement (simulated correction):** I re-published the 2013-08 dump with every 100th
game removed (3,256 games). The runner re-ran extract/bronze/silver for August and gold
for August, September and October (their carried player state changed). The 7 earlier
months had a row diff of 0. Restoring the original file and re-running returned all 80
partitions to a row diff of 0. Most of the changed rows in later months come from
cumulative counters (`session_num`, `perf_games_before`) shifting, which is the expected
cost of carrying history forward.

**Quality: real anomalies in the real data**

| check | rows | what it means |
|---|---|---|
| hard checks (keys, RI, SCD2, conservation, partitions) | 0 failures of 12 | |
| quarantined: missing ratings | 3,353 | games with `?` ratings; cannot be scored |
| rating continuity breaks | 227,220 of 4,540,952 checkable (5.0%) | a player's rating moved between two consecutive same-speed games with no game in the dump to explain it |
| rating change sign mismatch | 414 | a winner lost rating points or a loser gained |
| decisive result with zero moves | 815 | |
| late-arriving games | 4,635 | game date is in the previous month; max 2.0 h early. The dumps are cut at Paris midnight (UTC+1 in winter, UTC+2 in summer), not UTC midnight |

**Query speed** (Blitz games in June 2013: count and white score by opening family, top 10;
all four methods return identical answers)

| method | median time |
|---|---|
| parse every raw dump | 26.8 s (1 run) |
| parse only the two relevant raw dumps | 5.95 s |
| DuckDB over one unpartitioned Parquet file | 7.0 ms |
| DuckDB over partitioned gold star schema | **5.1 ms** (1,163x vs. two-dump raw scan, 1.36x vs. flat Parquet) |

Most of the gain comes from columnar storage. Partition pruning adds about 1.4x at this
size; it would matter more at full Lichess scale.

**Finding: tilt is a retention problem, not a performance problem**

Sample: 3,885,134 player-games from 14,072 players, excluding correspondence and each
player's first 20 games in a speed (provisional ratings). Outcome = actual score minus the
Elo expected score from the two pre-game ratings, so the rating gap is controlled game by
game. 95% intervals come from a player-clustered bootstrap (500 reps).

| | value | 95% CI |
|---|---|---|
| score vs. expectation, same-session game after a non-loss | +0.17 pts | 0.09 to 0.24 |
| score vs. expectation, same-session game after 3+ straight losses | +1.16 pts | |
| difference (streak minus baseline) | **+0.99 pts** | 0.80 to 1.16 |
| session ends after a win | 20.4% | 19.9 to 20.9 |
| session ends after a 3rd straight loss | 25.4% | 24.6 to 26.0 |
| difference | **+4.95 pts (1.24x)** | 4.41 to 5.45 |

(pts = percentage points of expected score, or of probability for session ends.)

If tilt hurt play, the streak group would score *below* expectation. It scores slightly
above. The likely reason is rating lag: a losing streak pulls a player's rating down
faster than their skill changes, so the next opponent is a little easier than the rating
says. As a control I compared the same streak length when the player took a break of
more than 45 minutes before the next game. Tilt should only show up without the break.
For 1 to 4 prior losses the difference between "kept playing" and "took a break" is
within noise (for example 3 losses: -0.23 pts, CI -0.68 to +0.20), so there is no
detectable immediate-play penalty there. The one place a tilt-like signal appears is 5 or
more straight losses: players who kept going scored 0.80 pts lower than players who took
a break first (CI -1.26 to -0.32), although both groups still scored above expectation.
What changes clearly is behaviour: the chance of ending the session rises with each
consecutive loss and plateaus at about 25%. (A side effect worth knowing: the first game
after a break, with no loss streak, scores 0.54 pts worse than a game played mid-session,
CI 0.39 to 0.70, a warm-up effect.)

Matchmaking calibration (side result): the Elo formula is overconfident on this data.
Heavy favourites (expected 0.9 to 1.0) scored 0.900 against 0.938 expected, and heavy
underdogs scored 0.117 against 0.062 expected. Expected calibration error is 0.033;
Brier score 0.207 against 0.242 for a coin flip. 28% of games had a rating gap over 200.

## Limits

- This is 2013 Lichess chess, not a team game. Chess has no teammates, no champion select
  and a different rating system history, so the finding is a method demonstration, not a
  claim about League of Legends.
- The tilt analysis is observational. Players choose when to keep playing, so the players
  who continue after losses may be the less tilt-prone ones (survivorship), and the 45-minute
  session gap is a choice I made, not something Lichess records.
- "Expected score" uses the standard Elo logistic curve on the recorded ratings. Lichess was
  not necessarily using that exact model in 2013, which is one reason the calibration is off.
- Rating continuity breaks are counted, not explained. Plausible causes are rated games that
  are not in the standard dump or rating adjustments; I did not verify which.
- The restatement scenario uses a modified copy of a real dump (simulated correction). All
  other numbers come from the unmodified public files.
- Runs used a thread pool because my sandbox blocks process pools; the runner uses
  processes where it can.
- Gold views in `warehouse.duckdb` point at absolute Parquet paths, so the warehouse file
  is not portable without re-publishing.

Data: Lichess open database (https://database.lichess.org), CC0.
