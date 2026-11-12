"""Streaming PGN header parser.

Reads a .pgn.zst file without ever materialising the decompressed text on disk.
Only the tag pairs are kept, plus a cheap move count, because the pipeline is
about games and players, not move-level analysis.
"""
from __future__ import annotations

import io
import re
from typing import Iterator

import zstandard

TAGS = [
    "Event", "Site", "White", "Black", "Result", "UTCDate", "UTCTime",
    "WhiteElo", "BlackElo", "WhiteRatingDiff", "BlackRatingDiff",
    "WhiteTitle", "BlackTitle", "ECO", "Opening", "TimeControl", "Termination",
]
_TAG_RE = re.compile(r'^\[(\w+) "(.*)"\]\s*$')
_MOVE_NO_RE = re.compile(r"(\d+)\.")


def _open_text(path: str) -> io.TextIOBase:
    if path.endswith(".zst"):
        fh = open(path, "rb")
        reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(fh, read_size=1 << 20)
        return io.TextIOWrapper(reader, encoding="utf-8", errors="replace", newline="\n")
    return open(path, "r", encoding="utf-8", errors="replace")


def _num_moves(moves: str) -> int:
    """Full-move number of the last move in a movetext line (0 if no moves)."""
    last = None
    for m in _MOVE_NO_RE.finditer(moves):
        last = m
    return int(last.group(1)) if last else 0


def iter_games(path: str) -> Iterator[dict]:
    """Yield one dict per game: known tags (missing -> None), num_moves, record_seq."""
    seq = 0
    tags: dict = {}
    moves_parts: list[str] = []
    in_moves = False
    with _open_text(path) as fh:
        for line in fh:
            if line.startswith("["):
                if in_moves:  # a new game began right after movetext
                    yield _emit(tags, moves_parts, seq)
                    seq += 1
                    tags, moves_parts, in_moves = {}, [], False
                m = _TAG_RE.match(line)
                if m:
                    tags[m.group(1)] = m.group(2)
            elif line.strip() == "":
                continue
            else:
                in_moves = True
                moves_parts.append(line.strip())
        if tags:
            yield _emit(tags, moves_parts, seq)


def _emit(tags: dict, moves_parts: list[str], seq: int) -> dict:
    row = {t: tags.get(t) for t in TAGS}
    row["num_moves"] = _num_moves(" ".join(moves_parts))
    row["record_seq"] = seq
    return row
