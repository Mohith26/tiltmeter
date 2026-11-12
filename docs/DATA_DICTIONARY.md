# Data dictionary (gold layer)

All gold tables live in `lake/warehouse.duckdb`. Facts are views over
`lake/gold/<table>/month=YYYY-MM/data.parquet` (Hive partitioning on `month`,
which is the **source dump month**, not the game date: Lichess cuts monthly
dumps at Paris midnight, so games from the last 1 to 2 UTC hours of a month
land in the next month's partition).

## fact_game
Grain: one row per rated game that passed the silver contract. PK `game_id`.

| column | type | meaning |
|---|---|---|
| game_id | VARCHAR | Lichess game id (8 chars), primary key |
| month | VARCHAR | partition: source dump month YYYY-MM |
| date_key | INTEGER | FK to dim_date (YYYYMMDD of UTC start) |
| start_ts | TIMESTAMP | game start, UTC |
| speed | VARCHAR | Bullet, Blitz, Classical, Correspondence (Lichess rates each separately) |
| is_tournament | BOOLEAN | game was played in an arena tournament |
| tournament_id | VARCHAR | tournament id when is_tournament |
| opening_key | BIGINT | FK to dim_opening |
| white_sk, black_sk | BIGINT | FK to dim_player: the SCD2 version in force at start_ts |
| white_id, black_id | VARCHAR | lowercased usernames (natural keys) |
| white_elo, black_elo | INTEGER | pre-game ratings in this speed |
| white_rating_diff, black_rating_diff | INTEGER | rating change caused by this game |
| result | VARCHAR | 1-0, 0-1, 1/2-1/2 |
| white_score | DOUBLE | 1, 0.5, 0 |
| white_expected_score | DOUBLE | 1 / (1 + 10^((black_elo - white_elo)/400)) |
| termination | VARCHAR | Normal, Time forfeit, Abandoned, Rules infraction |
| num_moves | INTEGER | full-move number of the last move (0 = no moves played) |
| time_control, base_seconds, increment_seconds | | raw and parsed time control |
| eco, opening, opening_family | VARCHAR | denormalised for convenience; canonical in dim_opening |
| dq_flags | VARCHAR[] | soft contract rules this row failed (see contracts/silver_games.json) |
| source_file | VARCHAR | raw dump the row came from |

## fact_player_game
Grain: one row per player per game (two per game). PK (`game_id`, `color`).

| column | type | meaning |
|---|---|---|
| game_id, month, date_key, start_ts | | as in fact_game |
| player_sk | BIGINT | FK to dim_player, version in force at start_ts |
| player_id, opp_id | VARCHAR | this player and the opponent |
| color | VARCHAR | white or black |
| perf | VARCHAR | speed; ratings and tiers are tracked per perf |
| tier | VARCHAR | ladder tier implied by pre_rating (Iron < 1200 <= Bronze < 1400 <= Silver < 1600 <= Gold < 1800 <= Platinum < 2000 <= Diamond < 2200 <= Master) |
| pre_rating, opp_rating | INTEGER | pre-game ratings |
| rating_gap | INTEGER | pre_rating - opp_rating |
| rating_delta | INTEGER | this player's rating change from the game |
| score | DOUBLE | 1 win, 0.5 draw, 0 loss |
| expected_score | DOUBLE | Elo logistic expectation from the rating gap |
| minutes_since_prev | DOUBLE | minutes since this player's previous game start (any speed); NULL for first game |
| session_num | INTEGER | player's session counter; a new session starts after a gap > 45 minutes |
| game_in_session | INTEGER | 1-based position within the session |
| prior_loss_streak | INTEGER | consecutive losses immediately before this game (any speed; draws reset) |
| prior_win_streak | INTEGER | consecutive wins immediately before this game |
| perf_games_before | INTEGER | games this player had already played in this perf (provisional-rating filter) |
| rating_continuity_break | BOOLEAN | pre_rating differs from previous game's pre_rating + rating_delta in the same perf; NULL when not checkable |
| out_of_order | BOOLEAN | game starts before the carried state's last game (late arrival across a partition boundary) |

## dim_player (SCD Type 2)
Grain: one row per (player, perf, tier stint). PK `player_sk` = stable hash of
(player_id, perf, valid_from). A new version opens when the player's tier in
that perf changes.

| column | type | meaning |
|---|---|---|
| player_sk | BIGINT | surrogate key |
| player_id | VARCHAR | natural key (lowercased username) |
| player_name | VARCHAR | display name as seen in the source |
| perf | VARCHAR | speed the tier applies to |
| tier | VARCHAR | ladder tier for this stint |
| title | VARCHAR | FIDE/Lichess title if seen (rare in 2013) |
| valid_from | TIMESTAMP | start of the first game in this tier |
| valid_to | TIMESTAMP | start of the first game in the next tier (NULL if current) |
| is_current | BOOLEAN | latest version for (player_id, perf) |
| version | INTEGER | 1-based version number |

## dim_opening
Grain: one row per (ECO code, opening name). PK `opening_key`.
Columns: `opening_key`, `eco`, `opening`, `opening_family` (text before the first colon), `eco_volume` (A to E).

## dim_date
Grain: one row per calendar day between the first and last game. PK `date_key` (YYYYMMDD).
Columns: `date_key`, `date`, `year`, `month`, `year_month`, `day_name`, `iso_dow`, `is_weekend`.

## Quarantine and quality
- `lake/silver/quarantine/month=*/data.parquet`: rejected rows with `failed_rules` (rule ids from the contract).
- `lake/dq/dq_results.json`: every check, its dimension, severity and offending row count.
