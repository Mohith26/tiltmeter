import os
import sys
from pathlib import Path

import pytest
import zstandard

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tiltmeter.config import Config  # noqa: E402


def game(gid, white, black, result, ts, welo, belo, wdiff, bdiff, event="Rated Blitz game",
         eco="C00", opening="French Defense: Normal Variation", tc="300+0", term="Normal",
         moves="1. e4 e6 2. d4 d5"):
    d, t = ts.split(" ")
    tags = [("Event", event), ("Site", f"https://lichess.org/{gid}"), ("White", white), ("Black", black),
            ("Result", result), ("UTCDate", d.replace("-", ".")), ("UTCTime", t),
            ("WhiteElo", welo), ("BlackElo", belo)]
    if wdiff is not None:
        tags += [("WhiteRatingDiff", wdiff), ("BlackRatingDiff", bdiff)]
    tags += [("ECO", eco), ("Opening", opening), ("TimeControl", tc), ("Termination", term)]
    head = "\n".join(f'[{k} "{v}"]' for k, v in tags)
    return f"{head}\n\n{moves} {result}\n\n"


def write_month(cfg: Config, month: str, games: list[str]):
    cfg.raw.mkdir(parents=True, exist_ok=True)
    data = "".join(games).encode()
    cfg.raw_file(month).write_bytes(zstandard.ZstdCompressor().compress(data))


def fixture_months():
    """Three tiny months with every anomaly the contracts look for."""
    jan = [
        # alice loses three straight to bob in one session (streak + session logic)
        game("aaaaaaa1", "Alice", "Bob", "0-1", "2013-01-05 10:00:00", "1500", "1500", "-10", "+10"),
        game("aaaaaaa2", "Bob", "Alice", "1-0", "2013-01-05 10:10:00", "1510", "1490", "+9", "-9"),
        game("aaaaaaa3", "Alice", "Bob", "0-1", "2013-01-05 10:20:00", "1481", "1519", "-8", "+8"),
        # duplicate of a1 (same id) -> quarantined
        game("aaaaaaa1", "Alice", "Bob", "0-1", "2013-01-05 10:00:00", "1500", "1500", "-10", "+10"),
        # missing rating -> quarantined
        game("aaaaaaa4", "Carol", "Bob", "1-0", "2013-01-06 09:00:00", "?", "1527", None, None),
        # self play -> quarantined
        game("aaaaaaa5", "Carol", "carol", "1-0", "2013-01-06 09:05:00", "1500", "1500", "+1", "-1"),
        # zero moves decisive -> warn
        game("aaaaaaa6", "Carol", "Dave", "1-0", "2013-01-07 09:00:00", "1500", "1500", "+10", "-10", moves=""),
        # late arrival from December -> warn
        game("aaaaaaa7", "Dave", "Erin", "1/2-1/2", "2012-12-31 23:30:00", "1498", "1600", "+2", "-2"),
    ]
    feb = [
        # alice's 4th straight loss, next month: streak must carry over (prior_loss_streak = 3)
        game("bbbbbbb1", "Alice", "Bob", "0-1", "2013-02-01 08:00:00", "1473", "1527", "-7", "+7"),
        # alice wins and crosses Silver->Bronze? no: 1466 is Silver; then a rating continuity break
        game("bbbbbbb2", "Alice", "Carol", "1-0", "2013-02-01 08:10:00", "1466", "1510", "+12", "-12"),
        game("bbbbbbb3", "Alice", "Carol", "1-0", "2013-02-01 08:20:00", "1390", "1498", "+15", "-15"),
    ]
    mar = [
        game("ccccccc1", "Alice", "Dave", "1-0", "2013-03-02 12:00:00", "1405", "1490", "+12", "-12"),
        game("ccccccc2", "Erin", "Dave", "0-1", "2013-03-02 12:30:00", "1602", "1488", "-14", "+14",
             event="Rated Classical game", tc="600+5"),
    ]
    return {"2013-01": jan, "2013-02": feb, "2013-03": mar}


MONTHS = ["2013-01", "2013-02", "2013-03"]


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("TILT_ROOT", str(tmp_path))
    c = Config(tmp_path)
    for m, games in fixture_months().items():
        write_month(c, m, games)
    return c
