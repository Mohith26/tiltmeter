"""CLI: python -m tiltmeter run --months 2013-01:2013-10 [--force] [--workers 4]"""
from __future__ import annotations

import argparse
import json

from .config import DEFAULT_MONTHS, Config
from .dag import Runner


def month_range(spec: str) -> list[str]:
    if ":" not in spec:
        return spec.split(",")
    a, b = spec.split(":")
    (y, m), (y2, m2) = map(int, a.split("-")), map(int, b.split("-"))
    out = []
    while (y, m) <= (y2, m2):
        out.append(f"{y}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def main():
    ap = argparse.ArgumentParser(prog="tiltmeter")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run (or backfill) the pipeline for a month range")
    r.add_argument("--months", default=f"{DEFAULT_MONTHS[0]}:{DEFAULT_MONTHS[-1]}")
    r.add_argument("--force", action="store_true", help="ignore fingerprints and rebuild everything")
    r.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    res = Runner(Config(), workers=a.workers).run(month_range(a.months), force=a.force)
    print(json.dumps({k: v for k, v in res.items() if k != "skipped"} | {"skipped": len(res["skipped"])}, indent=1))


if __name__ == "__main__":
    main()
