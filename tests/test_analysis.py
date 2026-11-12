import duckdb
import numpy as np

from tiltmeter.analysis import _ci, _cluster_boot


def test_cluster_bootstrap_point_and_interval():
    con = duckdb.connect()
    # 200 players, group a mean 0.1, group b mean 0.0, per-player noise
    rng = np.random.default_rng(0)
    rows = []
    for p in range(200):
        off = rng.normal(0, 0.05)
        rows.append((f"p{p}", "a", 10, 10 * (0.1 + off)))
        rows.append((f"p{p}", "b", 10, 10 * (0.0 + off)))
    con.execute("CREATE TABLE t(p VARCHAR, g VARCHAR, n INT, s DOUBLE)")
    con.executemany("INSERT INTO t VALUES (?,?,?,?)", rows)
    point, reps, ns, n_players = _cluster_boot(con, "SELECT * FROM t", ["a", "b"])
    assert n_players == 200 and list(ns) == [2000, 2000]
    lo, hi = _ci(reps[:, 0])
    assert lo < point[0] < hi
    # paired difference removes the shared player effect: tight interval around 0.1
    dlo, dhi = _ci(reps[:, 0] - reps[:, 1])
    assert abs(dlo - 0.1) < 1e-6 and abs(dhi - 0.1) < 1e-6
