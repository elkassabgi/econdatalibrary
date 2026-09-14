"""Bounded-memory merge for giant flows — the updater-daily kills of 2026-09-13 and 2026-09-14.

WHAT BROKE. updater-daily 34780466566 and 34841580535 were destroyed inside eurostat's flows
26-50 ("The runner has received a shutdown signal"; the first run's last two [mem] samples read
used=15737MB avail=252MB and used=15732MB avail=257MB). The flow is demo_r_mweek3:
merge.merge_and_write materialises its whole stored parquet (83,287,439 rows) to merge a
1,656,986-row tail. Reproduced on the desktop under Python 3.11 / pyarrow 25.0.1: 21,057 MB
peak RSS uncapped, ArrowMemoryError under a 14,000 MB and a 16,000 MB cap. Nothing was saved,
so every run re-selected the same flows and died at the same one.

WHAT THESE TESTS PIN — every one of them fails on the pre-fix code:
  1. eurostat's production wiring (eurostat.update -> _giant.run_giant) merges a large stored
     flow with BOUNDED memory: a child process, peak RSS measured. The in-memory merge of the
     same inputs grows far past the bound.
  2. merge_and_write_bounded writes exactly what merge_and_write writes — rows, order, keep-last
     winner, changed-key report and return values — on randomized inputs with duplicates in both
     inputs, an UNSORTED stored file, null keys/dates/values, NaN, revisions, new keys and key
     runs that straddle stream chunks, for date32 and string obs_date.
  3. Its refusals leave the published object untouched and leave no temp or spill files.
  4. Under AQUEDUCT_BACKEND=r2 it reads and publishes THROUGH blob, never the local path (R36).
  5. run_giant's defaults stay on merge_and_write, so its other callers are unchanged.
  6. A sweep that is killed mid-loop keeps its checkpointed per-flow state (eurostat opts in).
  7. eurostat's row ceiling also applies to a PLAIN (non-gzip) body.
Hermetic: tmp dirs, the local backend (a fake R2 client for 4), no network.
"""
import datetime as dt
import io
import json
import math
import os
import random
import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, merge  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import _giant, eurostat  # noqa: E402

# Thresholds that send EVERY input down the external-sort path, however small.
FORCE_BOUNDED = dict(in_memory_max_bytes=-1, in_memory_max_new_rows=-1)
EPOCH = dt.date(1970, 1, 1)


@pytest.fixture(autouse=True)
def _hermetic(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("AQUEDUCT_BOUNDED_MERGE_MEMORY", raising=False)
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "_spill"))


def _tbl(rows, date_kind="date32"):
    """rows: (series_key, day-number or None, value). date_kind: date32 or string obs_date."""
    if date_kind == "date32":
        dates = pa.array([r[1] for r in rows], pa.int32()).cast(pa.date32())
    else:
        dates = pa.array([None if r[1] is None else f"2024-{r[1] % 12 + 1:02d}-{r[1] % 28 + 1:02d}"
                          for r in rows], pa.string())
    return pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                     "obs_date": dates,
                     "value": pa.array([r[2] for r in rows], pa.float64())})


def _rows(path):
    t = pq.read_table(path)
    out = []
    for r in t.to_pylist():
        v = r["value"]
        out.append((r["series_key"], r["obs_date"],
                    "NaN" if isinstance(v, float) and math.isnan(v) else v))
    return out


def _random_case(seed):
    rng = random.Random(seed)
    keys = [f"freq=M:geo=G{i:02d}" for i in range(7)] + [None]
    days = list(range(19000, 19012)) + [None]

    def val():
        r = rng.random()
        if r < 0.08:
            return None
        if r < 0.14:
            return float("nan")
        return float(rng.randrange(5))          # few values -> real idempotent re-fetches

    existing = [(rng.choice(keys), rng.choice(days), val()) for _ in range(rng.randrange(0, 150))]
    new = []
    for _ in range(rng.randrange(0, 80)):
        if existing and rng.random() < 0.5:
            k, d, v = rng.choice(existing)
            new.append((k, d, v if rng.random() < 0.5 else val()))
        else:
            new.append((rng.choice(keys), rng.choice(days), val()))
    if new and rng.random() < 0.5:
        new.append(new[0])                      # a duplicate INSIDE the new rows
    return existing, new


# ---- 2. equivalence -------------------------------------------------------------------------

@pytest.mark.parametrize("date_kind", ["date32", "string"])
@pytest.mark.parametrize("seed", range(30))
def test_bounded_merge_writes_exactly_what_the_in_memory_merge_writes(tmp_path, seed, date_kind):
    existing, new = _random_case(seed)
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    if existing:
        for p in (a, b):
            # written in raw random order with tiny row groups: an UNSORTED stored file whose
            # duplicate runs straddle both row groups and (batch_rows=3) stream chunks
            pq.write_table(_tbl(existing, date_kind), p, row_group_size=5)
    new_t = _tbl(new, date_kind)
    try:
        want = merge.merge_and_write(str(a), new_t, report_changed_keys=True)
    except DefinitiveError as e:
        with pytest.raises(DefinitiveError, match=str(e).split(" ")[0]):
            merge.merge_and_write_bounded(str(b), new_t, report_changed_keys=True, batch_rows=3,
                                          **FORCE_BOUNDED)
        return
    got = merge.merge_and_write_bounded(str(b), new_t, report_changed_keys=True, batch_rows=3,
                                        **FORCE_BOUNDED)
    assert got == want
    assert _rows(b) == _rows(a)


def test_a_long_duplicate_run_across_many_chunks_keeps_the_last_row(tmp_path):
    existing = [("A", 19000, float(i)) for i in range(20)] + [("B", 19000, 7.0)]
    new = [("A", 19000, 5.0), ("C", 19001, 1.0)]
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    for p in (a, b):
        pq.write_table(_tbl(existing), p, row_group_size=4)
    want = merge.merge_and_write(str(a), _tbl(new), min_ratio=0.0, report_changed_keys=True)
    got = merge.merge_and_write_bounded(str(b), _tbl(new), min_ratio=0.0,
                                        report_changed_keys=True, batch_rows=3, **FORCE_BOUNDED)
    assert got == want
    assert _rows(b) == _rows(a) == [("A", EPOCH + dt.timedelta(19000), 5.0),
                                    ("B", EPOCH + dt.timedelta(19000), 7.0),
                                    ("C", EPOCH + dt.timedelta(19001), 1.0)]


def test_small_inputs_are_handed_to_merge_and_write_unchanged(tmp_path, monkeypatch):
    p = tmp_path / "s.parquet"
    pq.write_table(_tbl([("A", 19000, 1.0)]), p)
    seen = []
    real = merge.merge_and_write
    monkeypatch.setattr(merge, "merge_and_write", lambda *a, **k: seen.append(k) or real(*a, **k))
    merge.merge_and_write_bounded(str(p), _tbl([("A", 19001, 2.0)]))
    assert len(seen) == 1 and seen[0]["mode"] == "merge"


# ---- 3. refusals ----------------------------------------------------------------------------

def test_refusals_leave_the_published_object_untouched_and_nothing_behind(tmp_path):
    shrink = tmp_path / "shrink.parquet"
    pq.write_table(_tbl([("A", 19000, 1.0)] * 50 + [("B", 19000, 2.0)]), shrink)
    before = shrink.read_bytes()
    with pytest.raises(DefinitiveError, match="refusing shrink"):
        merge.merge_and_write_bounded(str(shrink), _tbl([("C", 19001, 3.0)]), **FORCE_BOUNDED)
    assert shrink.read_bytes() == before

    drop = tmp_path / "drop.parquet"
    pq.write_table(_tbl([("A", 19000, 1.0)]).append_column("flag", pa.array(["e"])), drop)
    before = drop.read_bytes()
    with pytest.raises(DefinitiveError, match="missing column"):
        merge.merge_and_write_bounded(str(drop), _tbl([("A", 19001, 3.0)]), **FORCE_BOUNDED)
    assert drop.read_bytes() == before

    empty = tmp_path / "empty.parquet"
    with pytest.raises(DefinitiveError, match="0 rows"):
        merge.merge_and_write_bounded(str(empty), _tbl([]), **FORCE_BOUNDED)
    assert not empty.exists()

    with pytest.raises(ValueError, match="report_changed_keys refused"):
        merge.merge_and_write_bounded(str(tmp_path / "cap.parquet"), _tbl([("A", 19000, 1.0)] * 3),
                                      report_changed_keys=True, changed_keys_cap=2,
                                      **FORCE_BOUNDED)

    left = sorted(os.listdir(tmp_path))
    assert left == ["_spill", "drop.parquet", "shrink.parquet"], left
    assert os.listdir(tmp_path / "_spill") == []


# ---- 4. R2 routing --------------------------------------------------------------------------

class _FakeR2:
    """Just the R2Blob surface the bounded merge touches, over a dict."""
    bucket = "econ-data"

    def __init__(self):
        self.objects, self.puts, self.downloads = {}, [], []
        self.client = self

    def size(self, key):
        return len(self.objects[key]) if key in self.objects else None

    def exists(self, key):
        return key in self.objects

    def get(self, key):
        return self.objects.get(key)

    def download_file(self, bucket, key, dst):
        from botocore.exceptions import ClientError
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        with open(dst, "wb") as fh:
            fh.write(self.objects[key])
        self.downloads.append(key)

    def put_atomic(self, key, data):
        self.objects[key] = bytes(data)
        self.puts.append(key)

    def put_file(self, key, path):
        with open(path, "rb") as fh:
            self.objects[key] = fh.read()
        self.puts.append(key)


def test_under_r2_the_stored_object_is_read_and_published_through_blob(tmp_path, monkeypatch):
    existing = [("A", 19000 + i, float(i)) for i in range(40)]
    new = [("A", 19039, 99.0), ("B", 19000, 1.0)]
    ref = tmp_path / "ref.parquet"
    pq.write_table(_tbl(existing), ref)
    want = merge.merge_and_write(str(ref), _tbl(new), report_changed_keys=True)

    fake = _FakeR2()
    buf = io.BytesIO()
    pq.write_table(_tbl(existing), buf)
    key = "clean_full/eurostat/ZZ.parquet"
    fake.objects[key] = buf.getvalue()
    monkeypatch.setattr(blob, "_r2_routed", lambda: fake)
    store = tmp_path / "data" / "clean_full" / "eurostat"
    out = store / "ZZ.parquet"                  # absent locally: only R2 holds the stored rows
    got = merge.merge_and_write_bounded(str(out), _tbl(new), report_changed_keys=True,
                                        **FORCE_BOUNDED)
    assert got == want
    assert fake.downloads == [key] and fake.puts == [key]
    published = tmp_path / "published.parquet"
    published.write_bytes(fake.objects[key])
    assert _rows(published) == _rows(ref)
    assert [f for f in os.listdir(store) if f != "ZZ.parquet"] == []   # no .src/.tmp residue


# ---- 5. run_giant defaults ------------------------------------------------------------------

class _Unit:
    def __init__(self, d):
        self.out_paths = [d]


def _giant_run(tmp_path, monkeypatch, **kw):
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)
    cat = {"aa": {"vintage": "v1", "filename": "AA.parquet"},
           "bb": {"vintage": "v1", "filename": "BB.parquet"}}
    return _giant.run_giant(_Unit(str(tmp_path)), source="zzgiant", fetch_catalog=lambda: cat,
                            fetch_flow=lambda fid, meta, since, s: (_tbl([(fid, 19000, 1.0)]), "ok"),
                            csv_accept="text/csv", rate=0, timeout=1, **kw)


def test_run_giant_uses_the_bounded_merge_only_when_asked(tmp_path, monkeypatch):
    real = merge.merge_and_write_bounded
    calls = []

    def boom(*_a, **_k):
        raise AssertionError("bounded merge used without opt-in")

    monkeypatch.setattr(merge, "merge_and_write_bounded", boom)
    assert _giant_run(tmp_path / "plain", monkeypatch).status == "ok"
    assert _giant_run(tmp_path / "plain_rep", monkeypatch, report_changed_flows=True).status == "ok"

    monkeypatch.setattr(merge, "merge_and_write_bounded",
                        lambda *a, **k: calls.append(1) or real(*a, **k))
    assert _giant_run(tmp_path / "bounded", monkeypatch, bounded_merge=True,
                      report_changed_flows=True).status == "ok"
    assert len(calls) == 2


# ---- 6. checkpoint --------------------------------------------------------------------------

class _Killed(BaseException):
    """Stands in for what ends a sweep without passing through `except Exception`: the runner's
    kill, the orchestrator's SIGALRM UnitTimeout escaping, a job timeout."""


def test_a_sweep_killed_mid_loop_keeps_its_checkpointed_progress(tmp_path, monkeypatch):
    monkeypatch.setattr(_giant.time, "sleep", lambda *_a, **_k: None)
    flows = [f"zz{i:02d}" for i in range(25)]
    cat = {f: {"vintage": "v1", "filename": f.upper() + ".parquet"} for f in flows}

    def fetch_flow(fid, meta, since, session):
        if fid == "zz22":
            raise _Killed("runner shutdown signal")
        return _tbl([("k", 19000, 1.0)]), "ok"

    monkeypatch.setattr(eurostat, "fetch_catalog", lambda: cat)
    monkeypatch.setattr(eurostat, "_require_rekeyed", lambda: None)
    monkeypatch.setattr(eurostat, "fetch_flow", fetch_flow)
    with pytest.raises(_Killed):
        eurostat.update(_Unit(str(tmp_path)), None)
    state = json.loads((tmp_path / "_giant_state.json").read_text(encoding="utf-8"))
    done = {f for f, s in state.items() if s.get("status") == "ok"}
    assert set(flows[:20]) <= done and "zz22" not in done


# ---- 7. plain-body ceiling ------------------------------------------------------------------

def test_a_plain_csv_body_over_the_row_ceiling_is_deferred_not_parsed(monkeypatch):
    body = ("DATAFLOW,LAST UPDATE,freq,geo,TIME_PERIOD,OBS_VALUE\n"
            + "".join(f"ESTAT:ZZ(1.0),01/01/26 11:00:00,A,G{i},2024,{i}\n" for i in range(5))
            ).encode()
    monkeypatch.setattr(eurostat, "MAX_FLOW_ROWS", 4)
    with pytest.raises(eurostat.TooBigForRunner):
        eurostat._parse_csv(body)
    monkeypatch.setattr(eurostat, "MAX_FLOW_ROWS", 5)
    keys, _dates, _vals = eurostat._parse_csv(body)
    assert len(keys) == 5                       # at the ceiling it still parses


# ---- 1. memory through the production wiring ------------------------------------------------

N_STORED = 6_000_000
# Measured 2026-09-14 on this fixture (Windows, Python 3.11.9, pyarrow 25.0.1, duckdb 1.5.5):
# pre-fix merge_and_write grew peak RSS by 1,550 MB; the bounded merge (DuckDB limit 256MB) by
# 491 MB. The bound sits between them with ~500 MB of margin on each side.
GROWTH_BOUND_MB = 1000

_CHILD = textwrap.dedent(r'''
    import json, os, sys, threading, time
    root, src_dir = sys.argv[1], sys.argv[2]
    sys.path.insert(0, root)
    import duckdb, pyarrow as pa, pyarrow.compute  # noqa: F401  (imports belong to the baseline)
    from updater.strategies.fetchers import _giant, eurostat

    try:
        import resource
        scale = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
        def peak_mb():
            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / scale
    except ImportError:
        import psutil
        _p, _box = psutil.Process(), [0]
        def _loop():
            while True:
                _box[0] = max(_box[0], _p.memory_info().rss)
                time.sleep(0.02)
        threading.Thread(target=_loop, daemon=True).start()
        time.sleep(0.1)
        def peak_mb():
            return _box[0] / 1048576.0

    tail = pa.table({
        "series_key": pa.array([f"freq=W:age=Y_LT5:sex=F:unit=NR:geo=REG{k:07d}" for k in range(2000)]),
        "obs_date": pa.array([19000 + 50 * 7] * 1000 + [19000] * 1000, pa.int32()).cast(pa.date32()),
        "value": pa.array([-1.0] * 2000, pa.float64())})
    eurostat.fetch_catalog = lambda: {"zz_big": {"vintage": "v1", "filename": "ZZ_BIG.parquet"}}
    eurostat._require_rekeyed = lambda: None
    eurostat.fetch_flow = lambda fid, meta, since, session: (tail, "ok")
    _giant.time.sleep = lambda *_a, **_k: None

    class U:
        out_paths = [src_dir]

    before = peak_mb()
    res = eurostat.update(U(), None)
    after = peak_mb()
    print(json.dumps({"growth_mb": after - before, "before_mb": before, "status": res.status,
                      "obs": res.obs}))
''')


def _have_rss_instrument():
    try:
        import resource  # noqa: F401
        return True
    except ImportError:
        try:
            import psutil  # noqa: F401
            return True
        except ImportError:
            return False


@pytest.mark.skipif(not _have_rss_instrument(), reason="no RSS instrument (resource or psutil)")
def test_eurostat_merges_a_large_stored_flow_with_bounded_memory(tmp_path):
    import numpy as np
    src = tmp_path / "eurostat"
    src.mkdir()
    i = np.arange(N_STORED)
    key_pool = pa.array([f"freq=W:age=Y_LT5:sex=F:unit=NR:geo=REG{k:07d}"
                         for k in range(N_STORED // 50)])
    stored = pa.table({
        "series_key": key_pool.take(pa.array(i // 50)),
        "obs_date": pa.array((19000 + (i % 50) * 7).astype(np.int32)).cast(pa.date32()),
        "value": pa.array(np.random.default_rng(0).random(N_STORED))})
    path = src / "ZZ_BIG.parquet"
    pq.write_table(stored, path)
    del stored, key_pool, i
    floor = getattr(merge, "BOUNDED_IN_MEMORY_MAX_BYTES", 0)
    assert os.path.getsize(path) > floor, "fixture too small to reach the bounded path"

    env = dict(os.environ, AQUEDUCT_BACKEND="local", AQUEDUCT_BOUNDED_MERGE_MEMORY="256MB",
               ECONDL_DUCKDB_TMP=str(tmp_path / "_spill"), PYTHONIOENCODING="utf-8")
    proc = subprocess.run([sys.executable, "-c", _CHILD, ROOT, str(src)], env=env,
                          capture_output=True, text=True, encoding="utf-8", errors="replace",
                          timeout=900)
    assert proc.returncode == 0, proc.stderr[-3000:]
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    print(f"child peak RSS: {out['before_mb']:,.0f} MB before update(), "
          f"+{out['growth_mb']:,.0f} MB during it (bound {GROWTH_BOUND_MB} MB)")
    assert out["status"] == "ok" and out["obs"] == N_STORED + 1000, out
    assert out["growth_mb"] < GROWTH_BOUND_MB, (
        f"merging a 2,000-row tail into a {N_STORED:,}-row stored flow grew peak RSS by "
        f"{out['growth_mb']:,.0f} MB (bound {GROWTH_BOUND_MB} MB): the merge is materialising "
        f"the stored file again — the updater-daily 34780466566 / 34841580535 kill")
