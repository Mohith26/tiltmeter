"""Small helpers: atomic partition writes and content fingerprints."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

import duckdb


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def connect(threads: int | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=true")
    return con


def write_partition(con: duckdb.DuckDBPyConnection, select_sql: str, out: Path, params=None) -> int:
    """Write a query result to `out` atomically: write a temp file, then rename.
    A crash mid-write never leaves a half-written partition behind, and a re-run
    simply replaces the partition (delete-and-replace semantics)."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    if tmp.exists():
        tmp.unlink()
    con.execute(
        f"COPY ({select_sql}) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)",
        params or [],
    )
    os.replace(tmp, out)
    return con.execute(f"SELECT count(*) FROM read_parquet('{out}', hive_partitioning = false)").fetchone()[0]


def data_fingerprint(con: duckdb.DuckDBPyConnection, path: Path) -> str:
    """Order-independent fingerprint of a parquet file's rows (row count + xor of row hashes)."""
    n, h = con.execute(
        f"SELECT count(*), coalesce(bit_xor(hash(t)), 0) FROM read_parquet('{path}', hive_partitioning = false) t"
    ).fetchone()
    return f"{n}:{h}"
