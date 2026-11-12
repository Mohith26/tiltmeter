"""Raw -> bronze: parse a month's PGN dump into Parquet, all fields kept as text.

Bronze is a faithful, replayable copy of the source: no typing, no filtering,
no dedup. Lineage columns (source file, its sha256, record position) let any
downstream row be traced back to the exact game in the raw dump."""
from __future__ import annotations

import os

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .io import sha256_file
from .pgn import TAGS, iter_games

SCHEMA = pa.schema(
    [(t.lower(), pa.string()) for t in TAGS]
    + [("num_moves", pa.int32()), ("record_seq", pa.int64()),
       ("source_file", pa.string()), ("source_sha256", pa.string()), ("month", pa.string())]
)


def build_bronze(cfg: Config, month: str) -> dict:
    src = cfg.raw_file(month)
    digest = sha256_file(src)
    cols: dict[str, list] = {f.name: [] for f in SCHEMA}
    for g in iter_games(str(src)):
        for t in TAGS:
            cols[t.lower()].append(g[t])
        cols["num_moves"].append(g["num_moves"])
        cols["record_seq"].append(g["record_seq"])
    n = len(cols["record_seq"])
    cols["source_file"] = [src.name] * n
    cols["source_sha256"] = [digest] * n
    cols["month"] = [month] * n
    table = pa.Table.from_pydict(cols, schema=SCHEMA)
    out = cfg.part("bronze", "games", month)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd", row_group_size=122880)
    os.replace(tmp, out)
    return {"rows": n, "source_sha256": digest}
