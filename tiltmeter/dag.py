"""A small DAG runner: dependencies, parallel workers, retries with backoff,
content-fingerprint skipping (incremental runs), and backfill.

I did not use Airflow here because a full Airflow install is several hundred MB
of dependencies plus a scheduler and metadata DB. The task graph maps 1:1 onto
an Airflow DAG (one task per layer per month, gold chained month to month).

Each task records the fingerprint of its inputs. On a re-run a task is skipped
when its input fingerprint is unchanged and its outputs exist, so:
  * re-running the same month is a no-op (or, with --force, rewrites identical rows)
  * a corrected raw file re-runs only that month's bronze/silver/gold, and later
    gold months re-run only if the carried player state actually changed.
"""
from __future__ import annotations

import hashlib
import json
import time
import urllib.request
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .bronze import build_bronze
from .config import SOURCE_URL, Config, prev_month
from .gold import build_gold
from .io import connect, data_fingerprint, sha256_file
from .publish import publish
from .quality import run_checks
from .silver import build_silver

PKG = Path(__file__).resolve().parent


def _code_hash(*files: str) -> str:
    h = hashlib.sha256()
    for f in files:
        p = PKG / f if not f.startswith("..") else (PKG / f).resolve()
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


def extract(cfg: Config, month: str) -> dict:
    dst = cfg.raw_file(month)
    if not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(".part")
        urllib.request.urlretrieve(SOURCE_URL.format(month=month), tmp)
        tmp.rename(dst)
    return {"bytes": dst.stat().st_size, "sha256": sha256_file(dst)}


@dataclass
class Task:
    id: str
    fn: Callable
    args: tuple
    deps: list[str] = field(default_factory=list)
    outputs: list[Path] = field(default_factory=list)


def _run_task(fn, cfg, args):
    t0 = time.perf_counter()
    out = fn(cfg, *args)
    return out, time.perf_counter() - t0


class Runner:
    def __init__(self, cfg: Config, workers: int = 4, retries: int = 2, backoff: float = 0.5,
                 executor: str = "auto"):
        self.cfg, self.workers, self.retries, self.backoff = cfg, workers, retries, backoff
        self.executor_pref = executor
        self.state_path = cfg.lake / "_state" / "tasks.json"
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.log: list[dict] = []

    # ---- graph -------------------------------------------------------------
    def build_graph(self, months: list[str], with_quality: bool = True) -> list[Task]:
        c, tasks = self.cfg, []
        for m in months:
            tasks.append(Task(f"extract[{m}]", extract, (m,), [], [c.raw_file(m)]))
            tasks.append(Task(f"bronze[{m}]", build_bronze, (m,), [f"extract[{m}]"], [c.part("bronze", "games", m)]))
            tasks.append(Task(f"silver[{m}]", build_silver, (m,), [f"bronze[{m}]"],
                              [c.part("silver", "games", m), c.part("silver", "quarantine", m)]))
            deps = [f"silver[{m}]"] + ([f"gold[{prev_month(m)}]"] if prev_month(m) in months else [])
            tasks.append(Task(f"gold[{m}]", build_gold, (m,), deps,
                              [c.part("gold", t, m) for t in ("fact_game", "fact_player_game", "tier_events",
                                                              "_state_player", "_state_perf")]))
        tasks.append(Task("publish", publish, (), [f"gold[{m}]" for m in months], [c.warehouse]))
        if with_quality:
            tasks.append(Task("quality", run_checks, (months,), ["publish"], []))
        return tasks

    # ---- fingerprints --------------------------------------------------------
    def _input_fp(self, t: Task) -> str:
        c = self.cfg
        kind, _, rest = t.id.partition("[")
        m = rest.rstrip("]")
        if kind == "extract":  # identity of the raw file actually on disk
            f = c.raw_file(m)
            parts = [m, sha256_file(f) if f.exists() else "missing"]
        elif kind == "bronze":
            parts = [self.state[f"extract[{m}]"]["output"]["sha256"], _code_hash("bronze.py", "pgn.py")]
        elif kind == "silver":
            parts = [data_fingerprint(connect(), c.part("bronze", "games", m)),
                     _code_hash("silver.py", "../contracts/silver_games.json")]
        elif kind == "gold":
            prev = self.state.get(f"gold[{prev_month(m)}]", {}).get("output", {}).get("state_fingerprint", "none")
            parts = [data_fingerprint(connect(), c.part("silver", "games", m)), prev, _code_hash("gold.py", "config.py")]
        else:
            parts = [json.dumps({k: v.get("input_fp") for k, v in sorted(self.state.items())
                                 if k.startswith("gold[")}), _code_hash("publish.py", "quality.py")]
        return hashlib.sha256("|".join(parts).encode()).hexdigest()[:20]

    # ---- execution -----------------------------------------------------------
    def run(self, months: list[str], force: bool = False, with_quality: bool = True) -> dict:
        tasks = {t.id: t for t in self.build_graph(months, with_quality)}
        done: set[str] = set()
        ran: list[str] = []
        attempts: dict[str, int] = {}
        t_start = time.perf_counter()
        pool_ctx = None
        if self.executor_pref in ("auto", "process"):
            try:
                pool_ctx, self.executor = ProcessPoolExecutor(max_workers=self.workers), "process"
            except (OSError, PermissionError):  # some sandboxes forbid POSIX semaphores
                pool_ctx = None
        if pool_ctx is None:
            pool_ctx, self.executor = ThreadPoolExecutor(max_workers=self.workers), "thread"
        with pool_ctx as pool:
            running = {}
            while len(done) < len(tasks):
                for tid, t in tasks.items():
                    if tid in done or tid in running or not all(d in done for d in t.deps):
                        continue
                    fp = self._input_fp(t)
                    prev = self.state.get(tid, {})
                    if (not force and prev.get("input_fp") == fp and prev.get("status") == "success"
                            and all(p.exists() for p in t.outputs)):
                        done.add(tid)
                        self.log.append({"task": tid, "status": "skipped"})
                        continue
                    attempts[tid] = attempts.get(tid, 0) + 1
                    running[tid] = (pool.submit(_run_task, t.fn, self.cfg, t.args), fp)
                if not running:
                    continue
                finished = [tid for tid, (f, _) in running.items() if f.done()]
                if not finished:
                    time.sleep(0.02)
                    continue
                for tid in finished:
                    fut, fp = running.pop(tid)
                    try:
                        out, secs = fut.result()
                    except Exception as e:  # retry with exponential backoff
                        self.log.append({"task": tid, "status": "error", "attempt": attempts[tid], "error": repr(e)[:300]})
                        if attempts[tid] > self.retries:
                            self._save()
                            raise RuntimeError(f"task {tid} failed after {attempts[tid]} attempts: {e!r}") from e
                        time.sleep(self.backoff * 2 ** (attempts[tid] - 1))
                        continue
                    self.state[tid] = {"input_fp": fp, "status": "success", "output": out, "seconds": round(secs, 3)}
                    self.log.append({"task": tid, "status": "success", "attempt": attempts[tid], "seconds": round(secs, 3)})
                    done.add(tid)
                    ran.append(tid)
                    self._save()
        return {"ran": ran, "skipped": [l["task"] for l in self.log if l["status"] == "skipped"],
                "errors": [l for l in self.log if l["status"] == "error"],
                "wall_seconds": round(time.perf_counter() - t_start, 3), "executor": self.executor}

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, indent=1, default=str))
        tmp.replace(self.state_path)
