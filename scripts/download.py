"""Download the raw Lichess dumps used by the pipeline (about 327 MB total)."""
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from tiltmeter.config import DEFAULT_MONTHS, SOURCE_URL, Config  # noqa: E402

cfg = Config(ROOT)
cfg.raw.mkdir(parents=True, exist_ok=True)
for m in DEFAULT_MONTHS:
    dst = cfg.raw_file(m)
    if dst.exists():
        print("have", dst.name)
        continue
    print("get ", dst.name)
    urllib.request.urlretrieve(SOURCE_URL.format(month=m), dst.with_suffix(".part"))
    dst.with_suffix(".part").rename(dst)
