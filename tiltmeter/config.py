"""Paths and run configuration. Everything is relative to a lake root so tests
can point the whole pipeline at a temp directory."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

SOURCE_URL = "https://database.lichess.org/standard/lichess_db_standard_rated_{month}.pgn.zst"
DEFAULT_MONTHS = [f"2013-{m:02d}" for m in range(1, 11)]

# A gap longer than this between two game starts ends a player's session.
SESSION_GAP_MINUTES = 45

# Riot-style ladder tiers on top of the Lichess rating. dim_player tracks these
# as a Type 2 slowly changing attribute.
TIERS = [  # (lower bound inclusive, name)
    (0, "Iron"), (1200, "Bronze"), (1400, "Silver"), (1600, "Gold"),
    (1800, "Platinum"), (2000, "Diamond"), (2200, "Master"),
]


@dataclass
class Config:
    root: Path = field(default_factory=lambda: Path(os.environ.get("TILT_ROOT", ".")).resolve())

    @property
    def raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def lake(self) -> Path:
        return self.root / "lake"

    @property
    def warehouse(self) -> Path:
        return self.lake / "warehouse.duckdb"

    def part(self, layer: str, table: str, month: str) -> Path:
        return self.lake / layer / table / f"month={month}" / "data.parquet"

    def table_glob(self, layer: str, table: str) -> str:
        return str(self.lake / layer / table / "month=*" / "data.parquet")

    def raw_file(self, month: str) -> Path:
        return self.raw / f"lichess_db_standard_rated_{month}.pgn.zst"


def prev_month(month: str) -> str:
    y, m = map(int, month.split("-"))
    y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return f"{y}-{m:02d}"


def tier_sql(col: str) -> str:
    """SQL CASE expression mapping a rating column to a tier name."""
    parts = [f"WHEN {col} >= {lo} THEN '{name}'" for lo, name in reversed(TIERS)]
    return "CASE " + " ".join(parts) + " END"
