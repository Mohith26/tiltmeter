import duckdb

from conftest import game, write_month
from tiltmeter.bronze import build_bronze
from tiltmeter.pgn import iter_games
from tiltmeter.silver import build_silver


def q(sql):
    return duckdb.connect().execute(sql)


def test_parser_reads_tags_and_moves(cfg):
    games = list(iter_games(str(cfg.raw_file("2013-01"))))
    assert len(games) == 8
    g = games[0]
    assert g["White"] == "Alice" and g["Result"] == "0-1" and g["WhiteElo"] == "1500"
    assert g["num_moves"] == 2 and g["record_seq"] == 0
    assert games[6]["num_moves"] == 0  # result with no moves
    assert games[4]["WhiteRatingDiff"] is None  # tag absent -> None


def test_parser_handles_game_without_blank_line(tmp_path):
    p = tmp_path / "x.pgn"
    p.write_text('[Event "Rated Blitz game"]\n[Site "https://lichess.org/abcdefgh"]\n\n1. e4 1-0\n'
                 '[Event "Rated Bullet game"]\n[Site "https://lichess.org/hgfedcba"]\n\n1. d4 0-1\n')
    gs = list(iter_games(str(p)))
    assert [g["Site"][-8:] for g in gs] == ["abcdefgh", "hgfedcba"]


def test_bronze_keeps_everything_with_lineage(cfg):
    out = build_bronze(cfg, "2013-01")
    assert out["rows"] == 8
    assert q(f"SELECT count(*) FROM read_parquet('{cfg.part('bronze', 'games', '2013-01')}')").fetchone()[0] == 8
    assert q(f"SELECT count(DISTINCT source_sha256) FROM read_parquet('{cfg.part('bronze', 'games', '2013-01')}')").fetchone()[0] == 1


def test_silver_quarantines_with_rule_ids_and_conserves_rows(cfg):
    build_bronze(cfg, "2013-01")
    out = build_silver(cfg, "2013-01")
    assert out == {"rows_in": 8, "silver_rows": 5, "quarantined": 3}
    rules = dict(q(f"""SELECT game_id, list_sort(failed_rules)
        FROM read_parquet('{cfg.part('silver', 'quarantine', '2013-01')}')""").fetchall())
    assert rules["aaaaaaa1"] == ["q_duplicate_game_id"]
    assert rules["aaaaaaa4"] == ["q_rating_range", "q_ratings_present"]
    assert rules["aaaaaaa5"] == ["q_self_play"]


def test_silver_types_and_warn_flags(cfg):
    build_bronze(cfg, "2013-01")
    build_silver(cfg, "2013-01")
    p = cfg.part("silver", "games", "2013-01")
    flags = dict(q(f"SELECT game_id, dq_flags FROM read_parquet('{p}')").fetchall())
    assert flags["aaaaaaa6"] == ["w_no_moves"]
    assert flags["aaaaaaa7"] == ["w_late_arrival"]
    assert flags["aaaaaaa2"] == []
    row = q(f"SELECT white_id, white_score, base_seconds, increment_seconds, typeof(start_ts) FROM read_parquet('{p}') WHERE game_id='aaaaaaa2'").fetchone()
    assert row == ("bob", 1.0, 300, 0, "TIMESTAMP")


def test_unfinished_result_is_quarantined(tmp_path):
    from tiltmeter.config import Config
    c = Config(tmp_path)
    write_month(c, "2013-01", [game("zzzzzzz1", "A", "B", "*", "2013-01-01 00:00:00", "1500", "1500", "0", "0")])
    build_bronze(c, "2013-01")
    assert build_silver(c, "2013-01")["quarantined"] == 1
