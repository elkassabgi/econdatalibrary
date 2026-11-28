"""The post-T0 statcan lane (jobs/statcan_selfhost_lane.py, T0 item B3) - design
F:/work/econ_finish/b3_statcan_post_t0/DESIGN.md, the coordinator's answers of 2026-11-28 in its section 5.

These tests fake only the network. The store is a real directory (the self-hosted backend's store primitives are
local files), the merge is the real merge.merge_and_write_bounded, the serve is real DuckDB, the CSV store is a REAL
tools/selfhost/blobstore.BlobStore in a tmp folder, and both locks are the real ones (the catalogue writer lock
taken by REAL second processes). T0 is simulated ONLY by writing the file at cutover.FLAG_PATH - never by patching
is_cut_over (R1973). R2 is made to fail if anything asks for it.
"""
from __future__ import annotations

import ast
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import textwrap
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import jobs.ingest_statcan as ing  # noqa: E402
import jobs.statcan_selfhost_lane as NL  # noqa: E402
import statcan_lost_cubes as LC  # noqa: E402 - tools/selfhost, the S13-prep census (code review round 2, G3)
from blobstore import BlobStore  # noqa: E402
from core import catalog_path, cutover, r2_util  # noqa: E402
from updater import blob, writer_lock  # noqa: E402
from updater import config as updater_config  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402

L = NL.L
PID = 24100058
REL = "2026-09-12T08:30"
HDR = ["REF_DATE", "GEO", "DGUID", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR",
       "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"]
LANE_BLOB = "9926d1bd9296067e47756501cb53636ce50f3256"      # jobs/statcan_lane.py at main 4a5bf7259 = E:'s HEAD
ADEN = "series/statcan%3A24100058%23Aden.csv"
WINDSOR_NEW = "series/statcan%3A24100058%23Windsor%20-%20other%20locations.csv"


def _row(ref, geo, vec, coord, val):
    return [ref, geo, "", "Vehicles", "1", "units", "0", vec, coord, val, "", "", "", "0"]


def _zip(path, rows, pid=PID):
    buf = io.StringIO()
    buf.write(",".join(f'"{h}"' for h in HDR) + "\n")
    for r in rows:
        buf.write(",".join(f'"{c}"' for c in r) + "\n")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{pid}.csv", buf.getvalue())
        z.writestr(f"{pid}_MetaData.csv", "meta\n")


def _stored(path, rows):
    t = pa.table({
        "series_key": [r[0] for r in rows],
        "obs_date": pa.array([dt.date.fromisoformat(r[1]) for r in rows], pa.date32()),
        "value": [r[2] for r in rows], "geo": [r[3] for r in rows],
        "uom": ["Vehicles"] * len(rows), "coordinate": [r[4] for r in rows],
        "status": [""] * len(rows)}, schema=ing.SCHEMA)
    pq.write_table(t, path)


NEW = [_row("2026-01", "Windsor - other locations", "v1", "1.1", "1"),
       _row("2026-02", "Windsor - other locations", "v1", "1.1", "2.5"),
       _row("2026-03", "Windsor - other locations", "v1", "1.1", "3"),
       _row("2026-01", "Windsor - other locations", "v2", "1.2", "5"),
       _row("2026-01", "Aden", "v3", "2.1", "7")]
WANT_ROWS = [("v1", "2026-01-01", 1.0, "Windsor - other locations"),
             ("v1", "2026-02-01", 2.5, "Windsor - other locations"),
             ("v1", "2026-03-01", 3.0, "Windsor - other locations"),
             ("v2", "2026-01-01", 5.0, "Windsor - other locations"),
             ("v3", "2026-01-01", 7.0, "Aden")]


# --------------------------------------------------------------------------- #
# the world: a store, a catalogue, a blob store, the two locks - all in tmp
# --------------------------------------------------------------------------- #
class _NetworkCalled(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """NO TEST REACHES STATCAN: every attempt is recorded and fails the test at teardown (the lane suite's guard)."""
    import requests
    calls = []

    def _refuse(url=None, *a, **k):
        calls.append(str(url))
        raise _NetworkCalled(f"a test reached the network: {url}")
    for name in ("get", "post", "head"):
        monkeypatch.setattr(requests, name, _refuse)
    yield calls
    assert not calls, f"tests must not reach the network; attempted: {calls}"


@pytest.fixture(autouse=True)
def _fresh_module_state():
    NL._STORE.clear()
    NL._blocked_logged.update(why=None, at=None)
    NL._wait_logged.update(at=None, why=None)
    NL._session.update(released_mono=None)
    NL._run_flags.update(lock_blocked=False)
    NL._lost_logged.update(at=None)
    yield
    NL._STORE.clear()
    NL._session.update(released_mono=None)
    NL._run_flags.update(lock_blocked=False)
    if blob._process_session is not None:
        blob._process_session.__exit__(None, None, None)
        blob._process_session = None


# Every child process these tests start carries this argument, so a test that died early cannot leave one behind
# (R2236: a stopped job's process tree is listed and stopped, never assumed gone).
CHILD_MARK = "b3-selfhost-lane-test-child"


@pytest.fixture(autouse=True)
def _reap_children():
    yield
    import psutil
    for p in psutil.process_iter(["pid", "cmdline"]):
        try:
            if CHILD_MARK in " ".join(p.info.get("cmdline") or []):
                p.kill()
                p.wait(timeout=10)
        except (psutil.Error, OSError):
            pass


class _NoR2:
    def __init__(self, *a, **k):
        raise AssertionError("R2 used by the post-T0 lane")


@pytest.fixture
def world(tmp_path, monkeypatch):
    """The statcan store, split map and catalogue of the lane suite (tests/test_statcan_lane.py), wired into the
    PRIVATE lane instance this module runs. Flag absent: before T0."""
    # setenv first so monkeypatch RECORDS the variable's original state: the lane's pin sets it, and undo must
    # remove it again (tests/conftest.py _no_process_leak fails a test that leaves it set)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "unset-by-the-test")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    monkeypatch.setattr(updater_config, "BACKEND", updater_config.BACKEND)
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "_spill"))
    d = tmp_path / "data" / "clean_full" / "statcan"
    d.mkdir(parents=True)
    monkeypatch.setattr(sc, "OUT_DIR", str(d))
    monkeypatch.setattr(sc, "TMP_DIR", str(d / "_incr_tmp"))
    for name in ("STATE", "PROGRESS", "DEBT"):
        monkeypatch.setattr(L, name, str(d / f"_lane_{name.lower()}.json"))
    monkeypatch.setattr(L, "LOCAL_PROGRESS", str(tmp_path / "logs" / "statcan_lane.progress.json"))
    monkeypatch.setattr(L, "SPLIT_MAP", str(d / "_split_map.json"))
    monkeypatch.setattr(L, "CATALOGUE", str(tmp_path / "catalog.db"))
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(NL, "LOCAL_HEAVY_LOCK", str(tmp_path / "logs" / "local_heavy.lock"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", ROOT)
    monkeypatch.setattr(updater_config, "ROOT", ROOT)
    monkeypatch.setattr(updater_config, "DATA_ROOT", os.path.join(ROOT, "data", "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 client asked for"))
    monkeypatch.setattr(blob, "R2Blob", _NoR2)
    _stored(d / f"{PID}.parquet", [("v1", "2026-01-01", 1.0, "Windsor", "1.1"),
                                   ("v1", "2026-02-01", 2.0, "Windsor", "1.1"),
                                   ("v2", "2026-01-01", 5.0, "Windsor", "1.2"),
                                   ("v3", "2026-01-01", 6.0, "Aden", "2.1")])
    smap = {str(PID): {"dim": "geo", "parts": 2, "rows": 4}, "33333333": {"dim": "uom", "parts": 4, "rows": 900}}
    (d / "_split_map.json").write_text(json.dumps(smap))
    con = sqlite3.connect(tmp_path / "catalog.db")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO series VALUES (?)", [(f"statcan:{PID}#Windsor",), (f"statcan:{PID}#Aden",)])
    con.commit()
    con.close()
    BlobStore(str(tmp_path / "blobs"), create=True)
    return d


@pytest.fixture
def t0(world, tmp_path):
    """The same world, cut over: the flag file exists."""
    (tmp_path / "CUTOVER").write_text("")
    return world


def _net(monkeypatch, rows=NEW, counts=(5, 3), on_download=None):
    monkeypatch.setattr(sc, "_cube_counts", lambda pid: counts)
    monkeypatch.setattr(sc, "_get", lambda ep, **k: {"status": "SUCCESS", "object": "https://example.invalid/t.zip"})

    def _download(url, dest, tries=5, meta=None):
        if on_download is not None:
            on_download()
        _zip(dest, rows)
        return os.path.getsize(dest)
    monkeypatch.setattr(sc, "_download", _download)


class _Clock:
    def __init__(self):
        self.t = dt.datetime(2026, 12, 1, 12, 0, tzinfo=dt.timezone.utc)

    def __call__(self):
        return self.t


def _run_kw(clock=None, releases=None, calls=None):
    def enum(since):
        if calls is not None:
            calls.append("enum")
        return dict(releases or {PID: REL})
    kw = {"enumerate_releases": enum, "ends_fn": lambda: ({}, "not read in tests")}
    if clock is not None:
        kw["now_fn"] = clock
    return kw


def _state(d):
    return json.loads((d / "_lane_state.json").read_text())


def _progress(d):
    return json.loads((d / "_lane_progress.json").read_text())


def _served(tmp_path):
    bs = BlobStore(str(tmp_path / "blobs"))
    out = {}
    for k in bs.list("series/"):
        meta = bs.head(k)
        raw = open(meta["path"], "rb").read()
        out[k] = (gzip.decompress(raw) if meta.get("content_encoding") == "gzip" else raw).decode("utf-8")
    return out


def _rows(path):
    t = pq.read_table(path).sort_by([("series_key", "ascending"), ("obs_date", "ascending")])
    return [(r["series_key"], r["obs_date"].isoformat(), r["value"], r["geo"]) for r in t.to_pylist()]


class _Stop(BaseException):
    pass


def _fake_loop(n, on_sleep=None):
    """sleep() and monotonic() for main(): each sleep advances a fake clock by the lane's IDLE_SLEEP_S; the n-th
    sleep stops the loop."""
    st = {"t": 0.0, "n": 0}

    def sleep(s):
        st["n"] += 1
        st["t"] += s
        if on_sleep is not None:
            on_sleep(st["n"])
        if st["n"] >= n:
            raise _Stop()
    return sleep, (lambda: st["t"])


# A second process holding the catalogue writer lock (the heartbeat suite's _HOLDER pattern).
_HOLDER = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[1])
    from core import catalog_path
    fh = open(sys.argv[2], "a+b")
    catalog_path._lock(fh)
    print("held", flush=True)
    sys.stdin.readline()
""")
# A second process that TRIES the lock once and says whether it got it.
_PROBE = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[1])
    from core import catalog_path
    fh = open(sys.argv[2], "a+b")
    try:
        catalog_path._lock(fh)
    except OSError:
        print("refused", flush=True)
    else:
        catalog_path._unlock(fh)
        print("took", flush=True)
""")


def _probe() -> str:
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    r = subprocess.run([sys.executable, "-c", _PROBE, ROOT, catalog_path.LOCK_PATH, CHILD_MARK],
                       capture_output=True, text=True, timeout=60)
    return r.stdout.strip()


class _Holder:
    def __enter__(self):
        os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
        self.p = subprocess.Popen([sys.executable, "-c", _HOLDER, ROOT, catalog_path.LOCK_PATH, CHILD_MARK],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert self.p.stdout.readline().strip() == "held"
        return self

    def __exit__(self, *a):
        try:
            self.p.stdin.write("\n")
            self.p.stdin.flush()
            self.p.wait(timeout=30)
        finally:
            if self.p.poll() is None:
                self.p.kill()
                self.p.wait(timeout=30)


# --------------------------------------------------------------------------- #
# T1, T2 - before T0 the lane refuses, resident, and writes nothing
# --------------------------------------------------------------------------- #
def _nothing_written(tmp_path, d):
    assert not (tmp_path / "logs").exists(), "no lock file, no local progress before T0"
    assert not (tmp_path / "lock").exists(), "no catalogue lock folder before T0"
    for name in ("_lane_state.json", "_lane_progress.json", "_lane_debt.json"):
        assert not (d / name).exists(), name
    assert _served(tmp_path) == {}


def test_T1_before_t0_once_refuses_at_once_and_writes_nothing(world, tmp_path, monkeypatch):
    called = []
    monkeypatch.setattr(NL, "pin_backend", lambda *a, **k: called.append("pin"))
    monkeypatch.setattr(writer_lock, "acquire", lambda *a, **k: called.append("lock") or False)
    with pytest.raises(SystemExit, match="REFUSING before T0") as e:
        NL.main(["--once"], sleep=lambda s: pytest.fail("--once must not sleep"))
    assert "CUTOVER" in str(e.value)
    assert called == [] and "AQUEDUCT_BACKEND" not in os.environ
    _nothing_written(tmp_path, world)
    assert not os.path.exists(os.path.join(ROOT, "logs", f"{NL.GUARD_JOB}.DONE")), "never a self-written .DONE"


def test_T2_before_t0_the_resident_lane_logs_hourly_writes_nothing_and_starts_when_the_flag_appears(
        world, tmp_path, monkeypatch, capsys):
    ran = []
    monkeypatch.setattr(NL, "iteration", lambda **k: ran.append(cutover.is_cut_over()) or 0)
    sleep, mono = _fake_loop(30)                         # 30 x 300 s = 2.5 h
    with pytest.raises(_Stop):
        NL.main([], sleep=sleep, monotonic=mono)
    out = capsys.readouterr().out
    assert out.count("REFUSING before T0") == 3, out     # at 0 s, 3,600 s and 7,200 s
    assert ran == []
    _nothing_written(tmp_path, world)

    def appear(n):
        if n == 3:
            (tmp_path / "CUTOVER").write_text("")
    sleep, mono = _fake_loop(5, on_sleep=appear)
    with pytest.raises(_Stop):
        NL.main([], sleep=sleep, monotonic=mono)
    assert ran == [True, True], "it works from the first iteration after the flag, and only after it"
    assert "the cutover flag is present - starting" in capsys.readouterr().out


def test_T2b_an_unreadable_flag_is_cut_over_the_one_rule(world, tmp_path, monkeypatch):
    """core.cutover fails closed: a flag that cannot be read is T0 (M3: an os.path.exists test would say 'before
    T0' here). The lane asks core.cutover, never its own test."""
    real_stat = os.stat

    def stat(p, *a, **k):
        if str(p) == cutover.FLAG_PATH:
            raise PermissionError(13, "denied")
        return real_stat(p, *a, **k)
    monkeypatch.setattr(os, "stat", stat)
    ran = []
    monkeypatch.setattr(NL, "iteration", lambda **k: ran.append(1) or 0)
    assert NL.main(["--once"]) == 0 and ran == [1]


# --------------------------------------------------------------------------- #
# T3 - after T0, end to end: local store, self-hosted CSVs, no R2
# --------------------------------------------------------------------------- #
def test_T3_after_t0_a_released_cube_is_merged_locally_and_served_into_the_blob_store(t0, tmp_path, monkeypatch):
    _net(monkeypatch)
    assert NL.main(["--once"], run_kw=_run_kw()) == 0
    assert _rows(t0 / f"{PID}.parquet") == WANT_ROWS
    assert _served(tmp_path) == {ADEN: "series_id,obs_date,value\nv3,2026-01-01,7.0\n"}, "only catalogued ids"
    c = _state(t0)["cubes"][str(PID)]
    assert c["merged"] == REL and c["served"] == REL and "writer_lock_wait_since" not in c
    prog = _progress(t0)
    assert prog["lane"] == NL.MODULE_ID and prog["state"] == "idle" and prog["writer_lock_waits"] == 0
    assert json.loads((tmp_path / "logs" / "statcan_lane.progress.json").read_text())["lane"] == NL.MODULE_ID
    assert not os.path.exists(writer_lock.lock_path("statcan_writer")), "the region lock is let go"
    assert catalog_path._held is None and blob._process_session is None, "no lock kept for the process"
    assert _probe() == "took", "another process can take the catalogue writer lock after the iteration"
    assert os.environ.get("AQUEDUCT_BACKEND") == "selfhost"


# --------------------------------------------------------------------------- #
# T4 - the backend pin
# --------------------------------------------------------------------------- #
def test_T4_the_backend_is_pinned_to_selfhost_and_any_other_value_writes_nothing(t0, tmp_path, monkeypatch, capsys):
    class _Cfg:
        BACKEND = "local"
    env, cfg = {}, _Cfg()
    assert NL.pin_backend(env, cfg) is None and env["AQUEDUCT_BACKEND"] == "selfhost" and cfg.BACKEND == "selfhost"
    assert NL.pin_backend({"AQUEDUCT_BACKEND": "SelfHost "}, cfg) is None
    for bad in ("r2", "local"):
        assert "runs only with selfhost" in NL.pin_backend({"AQUEDUCT_BACKEND": bad}, _Cfg())
        monkeypatch.setenv("AQUEDUCT_BACKEND", bad)
        _net(monkeypatch)
        assert NL.main(["--once"], run_kw=_run_kw()) == 2
        assert "BLOCKED: AQUEDUCT_BACKEND" in capsys.readouterr().out
        assert not (t0 / "_lane_state.json").exists() and not (t0 / "_lane_progress.json").exists()
        assert not (tmp_path / "logs").exists() and _served(tmp_path) == {}


# --------------------------------------------------------------------------- #
# T5, T6, T7, T8 - the catalogue writer lock
# --------------------------------------------------------------------------- #
def test_T5_a_held_writer_lock_defers_the_serve_without_a_failure_and_merges_go_on(t0, tmp_path, monkeypatch, capsys):
    _net(monkeypatch)
    clock, calls = _Clock(), []
    with _Holder():
        assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 0
        c = _state(t0)["cubes"][str(PID)]
        assert c["merged"] == REL and c.get("served") is None, "merged, not served"
        assert _rows(t0 / f"{PID}.parquet") == WANT_ROWS
        assert _served(tmp_path) == {}
        assert not c.get("transient_fails") and not c.get("fails") and c.get("last_error") is None, c
        assert c["retry_after"] == L._iso(NL._grid_retry(clock.t)) == "2026-12-01T13:00:00Z", "the grid point"
        assert c["writer_lock_wait_since"] == L._iso(clock.t)
        assert _progress(t0)["writer_lock_waits"] == 1
        assert "serve WAITS for the catalogue writer lock" in capsys.readouterr().out
        clock.t += dt.timedelta(minutes=5)               # inside the wait: nothing is due, nothing enumerates
        assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 0
        assert calls == ["enum"], "a cube waiting for the lock does not turn an iteration into an enumeration"
    clock.t += dt.timedelta(minutes=NL.LOCK_WAIT_RETRY_MIN)
    assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 0
    c = _state(t0)["cubes"][str(PID)]
    assert c["served"] == REL and "writer_lock_wait_since" not in c and c.get("retry_after") is None
    assert _served(tmp_path) == {ADEN: "series_id,obs_date,value\nv3,2026-01-01,7.0\n"}


def test_T6_the_writer_lock_is_held_during_the_serve_only(t0, tmp_path, monkeypatch):
    seen = {}
    _net(monkeypatch, on_download=lambda: seen.setdefault("merge", _probe()))
    from updater import derive
    real = derive._put_with_retry

    def put(store, key, body, plain=False):
        seen.setdefault("put", _probe())
        return real(store, key, body)
    monkeypatch.setattr(derive, "_put_with_retry", put)
    assert NL.main(["--once"], run_kw=_run_kw()) == 0
    assert seen == {"merge": "took", "put": "refused"}, seen
    assert _probe() == "took"
    assert _state(t0)["cubes"][str(PID)]["served"] == REL


def test_T7_a_csv_put_outside_the_serve_session_is_refused_and_takes_no_lock(t0, tmp_path):
    put = NL.make_put(NL._blob_store())
    with pytest.raises(RuntimeError, match="REFUSED"):
        put(ADEN, b"series_id,obs_date,value\nv3,2026-01-01,7.0\n")
    assert _served(tmp_path) == {}
    assert catalog_path._held is None and blob._process_session is None
    assert _probe() == "took"


def test_T8_a_lock_already_held_by_this_process_ends_the_process(t0, monkeypatch):
    monkeypatch.setattr(catalog_path, "_held", object())
    with pytest.raises(SystemExit, match="already held by this process"):
        NL._serve_owed_under_writer_lock(str(PID), {}, {}, {}, None, None, L._now())
    with pytest.raises(SystemExit, match="already held by this process"):
        NL.iteration()


# --------------------------------------------------------------------------- #
# T9 - the region lock
# --------------------------------------------------------------------------- #
_OTHER_WRITER = ("import sys, time; sys.path.insert(0, sys.argv[1]); from updater import writer_lock as w; "
                 "w.LOCK_DIR = sys.argv[2]; w.hold_or_refuse('statcan_writer', 'a derive'); "
                 "print('writing', flush=True); time.sleep(60)")


def test_T9_another_statcan_writer_keeps_the_lane_out(t0, tmp_path, monkeypatch, capsys):
    _net(monkeypatch)
    (tmp_path / "logs").mkdir()
    p = subprocess.Popen([sys.executable, "-c", _OTHER_WRITER, ROOT, str(tmp_path / "logs"), CHILD_MARK],
                         stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "writing"
        assert NL.main(["--once"], run_kw=_run_kw()) == 0
    finally:
        p.kill()
        p.wait()
    assert "another writer holds" in capsys.readouterr().out
    assert not (t0 / "_lane_state.json").exists() and _served(tmp_path) == {}
    assert NL.main(["--once"], run_kw=_run_kw()) == 0, "a dead holder is stale: the next iteration works"
    assert _state(t0)["cubes"][str(PID)]["served"] == REL


# --------------------------------------------------------------------------- #
# T10 - kill and resume, in real processes
# --------------------------------------------------------------------------- #
_DRIVER = textwrap.dedent('''
    """A test driver: one post-T0 lane iteration in the tmp world of the calling test, optionally killed hard."""
    import json, os, shutil, sys
    cfg = json.load(open(sys.argv[1]))
    sys.path.insert(0, cfg["root"])
    from core import catalog_path, cutover, r2_util
    from updater import blob, writer_lock, merge, derive
    from updater import config as ucfg
    from updater.strategies.fetchers import statcan as sc
    cutover.FLAG_PATH = cfg["flag"]
    catalog_path.LOCK_PATH, catalog_path.BUILD_PATH, catalog_path.LIVE_STORE_ROOT = cfg["lock"], cfg["db"], cfg["root"]
    blob.SELFHOST_BLOB_ROOT = cfg["blobs"]
    ucfg.ROOT, ucfg.DATA_ROOT = cfg["root"], os.path.join(cfg["root"], "data", "clean_full")
    r2_util.client = lambda *a, **k: (_ for _ in ()).throw(AssertionError("R2"))
    writer_lock.LOCK_DIR = cfg["logs"]
    sc.OUT_DIR, sc.TMP_DIR = cfg["store"], os.path.join(cfg["store"], "_incr_tmp")
    import jobs.statcan_selfhost_lane as NL
    L = NL.L
    for k in ("STATE", "PROGRESS", "DEBT"):
        setattr(L, k, os.path.join(cfg["store"], "_lane_%s.json" % k.lower()))
    L.LOCAL_PROGRESS, L.SPLIT_MAP, L.CATALOGUE = cfg["local_progress"], cfg["split_map"], cfg["db"]
    sc._cube_counts = lambda pid: (5, 3)
    sc._get = lambda ep, **k: {"status": "SUCCESS", "object": "https://example.invalid/t.zip"}

    def _download(url, dest, tries=5, meta=None):
        shutil.copyfile(cfg["zip"], dest)
        return os.path.getsize(dest)
    sc._download = _download
    kill = cfg.get("kill")
    if kill == "after_publish":
        real = merge.merge_and_write_bounded
        def mw(*a, **k):
            r = real(*a, **k)
            os._exit(9)                      # the store is published, the lane state is not recorded
        merge.merge_and_write_bounded = mw
    elif kill == "after_first_put":
        real_put = derive._put_with_retry
        def pw(store, key, body, plain=False):
            ok = real_put(store, key, body)
            os._exit(9)                      # one part written, the cube not recorded as served
        derive._put_with_retry = pw
    rc = NL.main(["--once"], run_kw={"enumerate_releases": lambda since: {int(cfg["pid"]): cfg["rel"]},
                                     "ends_fn": lambda: ({}, "not read in tests")})
    print("rc", rc, flush=True)
''')


@pytest.mark.parametrize("kill", ["after_publish", "after_first_put"])
def test_T10_a_hard_kill_leaves_both_locks_free_and_the_relaunch_serves_everything(world, tmp_path, kill):
    t0 = world
    con = sqlite3.connect(tmp_path / "catalog.db")              # two catalogued parts, so a serve can be cut in half
    con.execute("INSERT INTO series VALUES (?)", (f"statcan:{PID}#Windsor - other locations",))
    con.commit()                                                # (before the flag: after T0 the build needs the lock)
    con.close()
    (tmp_path / "CUTOVER").write_text("")
    _zip(tmp_path / "release.zip", NEW)
    cfg = {"root": ROOT, "flag": cutover.FLAG_PATH, "lock": catalog_path.LOCK_PATH, "db": str(tmp_path / "catalog.db"),
           "blobs": str(tmp_path / "blobs"), "logs": str(tmp_path / "logs"), "store": str(t0),
           "local_progress": str(tmp_path / "logs" / "statcan_lane.progress.json"),
           "split_map": str(t0 / "_split_map.json"), "zip": str(tmp_path / "release.zip"), "pid": PID, "rel": REL}
    drv = tmp_path / "driver.py"
    drv.write_text(_DRIVER, encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "AQUEDUCT_BACKEND"}
    env.update(PYTHONDONTWRITEBYTECODE="1", ECONDL_DUCKDB_TMP=str(tmp_path / "_spill"))

    def launch(kill_at):
        c = dict(cfg, kill=kill_at)
        (tmp_path / "cfg.json").write_text(json.dumps(c))
        return subprocess.run([sys.executable, str(drv), str(tmp_path / "cfg.json"), CHILD_MARK], cwd=ROOT, env=env,
                              capture_output=True, text=True, timeout=600)
    r = launch(kill)
    assert r.returncode == 9, (r.returncode, r.stdout[-2000:], r.stderr[-2000:])
    st = _state(t0)["cubes"][str(PID)]
    assert st.get("served") is None, st
    if kill == "after_publish":
        assert st.get("merge_in_flight") == REL and st.get("merged") is None, st
    assert writer_lock.owner("statcan_writer") is None, "a dead lane's region lock is stale"
    assert _probe() == "took", "the OS freed the dead lane's catalogue writer lock"
    r = launch(None)
    assert r.returncode == 0 and "rc 0" in r.stdout, (r.stdout[-2000:], r.stderr[-2000:])
    st = _state(t0)["cubes"][str(PID)]
    assert st["merged"] == REL and st["served"] == REL and "merge_in_flight" not in st, st
    assert _rows(t0 / f"{PID}.parquet") == WANT_ROWS
    assert _served(tmp_path) == {
        ADEN: "series_id,obs_date,value\nv3,2026-01-01,7.0\n",
        WINDSOR_NEW: "series_id,obs_date,value\nv1,2026-01-01,1.0\nv1,2026-02-01,2.5\nv1,2026-03-01,3.0\n"
                     "v2,2026-01-01,5.0\n"}


# --------------------------------------------------------------------------- #
# T11 - BLOCKED: resident, red, and run() is never called
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("case", ["empty_store", "no_blob_store", "bad_split_map", "empty_split_map"])
def test_T11_a_lane_that_cannot_work_beats_blocked_and_books_nothing(t0, tmp_path, monkeypatch, case, capsys):
    calls = []
    _net(monkeypatch)
    if case == "empty_store":
        os.remove(t0 / f"{PID}.parquet")
        want = "lists 0 cubes"
    elif case == "no_blob_store":
        monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "no_blobs"))
        want = "cannot be opened"
    elif case == "bad_split_map":
        (t0 / "_split_map.json").write_text("{not json")
        want = "split map"
    else:
        (t0 / "_split_map.json").write_text("{}")
        want = "no split decisions"
    sleep, mono = _fake_loop(3)
    with pytest.raises(_Stop):
        NL.main([], sleep=sleep, monotonic=mono, run_kw=_run_kw(calls=calls))
    assert calls == [], "run() was not called: no released cube is booked as held by nobody"
    prog = _progress(t0)
    assert prog["state"] == "blocked" and want in prog["blocked"] and prog["lane"] == NL.MODULE_ID, prog
    assert not _state(t0)["cubes"] if (t0 / "_lane_state.json").exists() else True
    assert capsys.readouterr().out.count("BLOCKED:") == 1, "one line per reason, then hourly"
    with pytest.raises(DefinitiveError, match="the lane is BLOCKED"):
        sc.verdict({"cubes": {}}, prog, dt.datetime.now(dt.timezone.utc))
    import guard_heartbeat as gh
    beat = json.loads((tmp_path / "logs" / "statcan_lane.progress.json").read_text())
    assert "BLOCKED" in gh._lane_problem(beat, dt.datetime.now(dt.timezone.utc))
    assert _probe() == "took" and not os.path.exists(writer_lock.lock_path("statcan_writer"))


def test_T11b_a_refusal_inside_run_is_blocked_too_and_the_process_stays(t0, monkeypatch):
    monkeypatch.setattr(L, "run", lambda **k: (_ for _ in ()).throw(SystemExit("REFUSING: planted")))
    assert NL.iteration() == 2
    assert _progress(t0)["state"] == "blocked" and "planted" in _progress(t0)["blocked"]


def test_T11c_another_checkout_writes_nothing(t0, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(updater_config, "ROOT", str(tmp_path / "a_worktree"))
    _net(monkeypatch)
    assert NL.main(["--once"], run_kw=_run_kw()) == 2
    assert "BLOCKED" in capsys.readouterr().out
    assert not (t0 / "_lane_progress.json").exists() and not (tmp_path / "logs").exists()


# --------------------------------------------------------------------------- #
# T13 - guard_heartbeat: before T0 unchanged, after T0 the post-T0 lane
# --------------------------------------------------------------------------- #
def _beat(lane=None, beat_h_ago=0.1, state="idle"):
    b = {"state": state, "current": None, "counters": {},
         "beat_utc": (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=beat_h_ago)).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "owed": {"merge": 0, "serve": 0, "quarantined": [], "serve_refused": [], "failing": [], "new_cubes": 0,
                  "oldest_new_cube": None, "oldest_owed_release": None}}
    if lane:
        b["lane"] = lane
    return b


def test_T13_the_heartbeat_judges_the_pre_t0_lane_before_t0_and_the_post_t0_lane_after(world, tmp_path, monkeypatch):
    import guard_heartbeat as gh
    now = dt.datetime.now(dt.timezone.utc)
    monkeypatch.setattr(gh, "_lane_expected", lambda: True)
    # BEFORE T0: as it was
    assert gh._lane_problem(_beat(), now) is None
    assert "jobs/statcan_lane.py has never run there" in gh._lane_problem(None, now)
    assert "DEAD" in gh._lane_problem(_beat(beat_h_ago=10), now)
    assert gh._tracked() == gh.TRACKED and "statcan_lane.py" in gh.TRACKED
    # AFTER T0
    (tmp_path / "CUTOVER").write_text("")
    assert gh._tracked() == gh.TRACKED_POST_T0 and "statcan_selfhost_lane.py" in gh.TRACKED_POST_T0
    assert "statcan_lane.py" not in gh.TRACKED_POST_T0 and len(gh.TRACKED_POST_T0) == len(gh.TRACKED)
    assert gh._lane_post_t0() == NL.MODULE_ID, "read from the real registry"
    assert gh._lane_problem(_beat(lane=NL.MODULE_ID), now) is None
    assert "never run there since T0" in gh._lane_problem(None, now)
    assert "pre-T0 lane's last beat" in gh._lane_problem(_beat(), now), "an unmarked beat is the old lane's"
    assert "pre-T0 lane's last beat" in gh._lane_problem(_beat(lane="jobs/other.py"), now)
    assert "DEAD" in gh._lane_problem(_beat(lane=NL.MODULE_ID, beat_h_ago=10), now)
    monkeypatch.setattr(gh, "_lane_post_t0", lambda: None)
    assert "names no post-T0 statcan lane" in gh._lane_problem(_beat(lane=NL.MODULE_ID), now)


def test_T13b_publish_carries_the_lane_verdict_none_before_t0_the_verdict_after(world, tmp_path, monkeypatch):
    import guard_heartbeat as gh
    monkeypatch.setattr(gh, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(gh, "_emptiness_verdict", lambda: {"ran": True, "fetch_without_write": 0, "unreadable": 0})
    monkeypatch.setattr(gh, "_sec_edgar_problem", lambda rec, now: None)
    monkeypatch.setattr(gh, "_launcher_watchdog_beat", lambda now=None: {})   # never the machine's real folder
    monkeypatch.setattr(gh, "_lane_expected", lambda: True)
    monkeypatch.setattr(gh, "LANE_PROGRESS_LOCAL", str(tmp_path / "logs" / "statcan_lane.progress.json"))
    sent = {}

    class _C:
        def put_object(self, Bucket, Key, Body, **kw):
            sent["body"] = json.loads(Body)
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: _C())
    assert gh.publish() == 0                                   # BEFORE T0: R2, and no verdict to judge
    assert sent["body"]["statcan_lane_problem"] is None and "statcan_lane.py" in sent["body"]["tracked"]
    (tmp_path / "CUTOVER").write_text("")                      # AFTER T0: the self-hosted store
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: pytest.fail("R2 after T0"))
    assert gh.publish() == 0
    body = json.loads(blob.SelfhostBlob().get(gh.KEY))
    assert "never run there since T0" in body["statcan_lane_problem"]
    assert "statcan_selfhost_lane.py" in body["tracked"] and "statcan_lane.py" not in body["tracked"]
    (tmp_path / "logs").mkdir(exist_ok=True)
    (tmp_path / "logs" / "statcan_lane.progress.json").write_text(json.dumps(_beat(lane=NL.MODULE_ID)))
    assert gh.publish() == 0
    assert json.loads(blob.SelfhostBlob().get(gh.KEY))["statcan_lane_problem"] is None

    def boom(beat, now):
        raise KeyError("planted")
    monkeypatch.setattr(gh, "_lane_problem", boom)
    assert gh.publish() == 0, "the beat goes out whatever the verdict does"
    assert "could not judge" in json.loads(blob.SelfhostBlob().get(gh.KEY))["statcan_lane_problem"]


# --------------------------------------------------------------------------- #
# T14 - the off-machine reader
# --------------------------------------------------------------------------- #
def test_T14_check_url_fails_a_red_lane_and_t0_ready_requires_the_field(monkeypatch, capsys):
    import urllib.request
    import guard_heartbeat as gh
    fresh = dt.datetime.now(dt.timezone.utc).isoformat()

    def answer(**extra):
        body = {"utc": fresh, "table_ok": True, "jobs_alive": 4, "jobs_tracked": 4, "emptiness_ran": True,
                "fetch_without_write": 0, "sec_edgar_local_ok": True, **extra}
        monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=60: io.BytesIO(json.dumps(body).encode()))
    url = "https://e.example/v1/guard-heartbeat"
    answer(statcan_lane_ok=False)
    assert gh.check_url(url, 45) == 1 and "STATCAN LANE NOT HEALTHY" in capsys.readouterr().out
    answer(statcan_lane_ok=True)
    assert gh.check_url(url, 45) == 0
    assert gh.check_url(url, 45, require_sec_edgar=True, require_statcan=True) == 0
    for old in ({"statcan_lane_ok": None}, {}):
        answer(**old)
        assert gh.check_url(url, 45) == 0, "selfhost-watch: a note, the workflow is unchanged"
        assert "UNMONITORED" in capsys.readouterr().out
        assert gh.check_url(url, 45, require_statcan=True) == 1, "t0_ready: red"
    import t0_ready as T
    src = open(T.__file__, encoding="utf-8").read()
    assert "functools.partial(guard_heartbeat.check_url, require_sec_edgar=True, require_statcan=True)" in src


# --------------------------------------------------------------------------- #
# T16 is in tests/test_selfhost_t0_ready.py; T17 - the registry field
# --------------------------------------------------------------------------- #
def test_T17_the_registry_names_the_post_t0_lane_and_validates_it():
    from updater import config, registry
    reg = registry.load()
    assert registry.validate(reg, expected_count=config.EXPECTED_SOURCE_COUNT) == []
    e = {x["source_id"]: x for x in reg["sources"]}["statcan"]
    assert e["served_by"] == "lane" and e["lane_post_t0"] == NL.MODULE_ID
    base = {"source_id": "x", "strategy": e["strategy"], "cadence": "weekly"}
    cases = {
        "lane_post_t0 needs served_by: lane": dict(base, lane_post_t0=NL.MODULE_ID),
        "must be a 'jobs/<name>.py' path": dict(base, served_by="lane", lane_post_t0="tools/x.py"),
        "the PRE-T0 lane": dict(base, served_by="lane", lane_post_t0="jobs/statcan_lane.py"),
    }
    for want, entry in cases.items():
        probs = registry.validate({"sources": [entry]})
        assert any(want in p for p in probs), (want, probs)
    assert registry.validate({"sources": [dict(base, served_by="lane", lane_post_t0=NL.MODULE_ID)]}) == []
    # SHAPE ONLY (design review C9): one problem in validate refuses EVERY source (R347), so a missing job file is
    # not validate's to judge - t0_ready's statcan-lane check refuses it (tests/test_selfhost_t0_ready.py), and here
    assert registry.validate({"sources": [dict(base, served_by="lane", lane_post_t0="jobs/nowhere_lane.py")]}) == []
    assert os.path.isfile(os.path.join(ROOT, *e["lane_post_t0"].split("/")))


# --------------------------------------------------------------------------- #
# T18 - the guard copies (G2: the pre-T0 copy unchanged, a separate post-T0 copy)
# --------------------------------------------------------------------------- #
MACHINE = os.path.join(ROOT, "tools", "machine")
PRE_COPY = os.path.join(MACHINE, "RELAUNCH_GUARD.ps1.workstation-copy")
POST_COPY = os.path.join(MACHINE, "RELAUNCH_GUARD.ps1.post-t0-copy")
_BLOCK_START = re.compile(r"^  # statcan LANE")


def _without_statcan_block(path):
    lines = open(path, encoding="utf-8-sig").read().splitlines()
    i = next(n for n, l in enumerate(lines) if _BLOCK_START.match(l))
    j = next(n for n in range(i, len(lines)) if lines[n].startswith("  @{ n = 'statcan"))
    return lines[:i] + lines[j + 1:], lines[i:j + 1]


def _jobs(path):
    import t0_ready as T
    code = T._guard_code(open(path, encoding="utf-8-sig").read())
    return re.findall(r"@\{ n = '([^']+)';\s*match = '([^']+)';\s*args = @\(([^)]*)\)", code)


def test_T18_the_post_t0_copy_differs_from_the_pre_t0_copy_in_the_statcan_entry_only():
    pre_rest, pre_block = _without_statcan_block(PRE_COPY)
    post_rest, post_block = _without_statcan_block(POST_COPY)
    assert pre_rest == post_rest, "the post-T0 guard must carry every other change of the machine's copy"
    assert pre_block != post_block
    for p in (PRE_COPY, POST_COPY):
        assert open(p, "rb").read(3) == b"\xef\xbb\xbf", f"{p}: PowerShell 5.1 needs the BOM"
    import t0_ready as T
    pre, post = T._guard_code(open(PRE_COPY, encoding="utf-8-sig").read()), \
        T._guard_code(open(POST_COPY, encoding="utf-8-sig").read())
    assert T._guard_launches(pre, "jobs/statcan_lane.py") and not T._guard_launches(pre, NL.MODULE_ID), \
        "the versioned workstation copy still restores the PRE-T0 machine"
    assert T._guard_launches(post, NL.MODULE_ID) and not T._guard_launches(post, "jobs/statcan_lane.py")
    assert ("statcan_selfhost", "statcan_selfhost_lane.py", "'-u', 'jobs/statcan_selfhost_lane.py'") in _jobs(POST_COPY)
    assert NL.GUARD_JOB == "statcan_selfhost"


@pytest.mark.parametrize("path", [PRE_COPY, POST_COPY])
def test_T18b_no_job_name_or_match_can_be_confused_with_another(path):
    """The guard matches '*<match>*' over command lines, globs guard_<n>_*.log for the stall check and reads
    <n>.PAUSED / <n>.DONE: no entry's match may hit another entry's script, and no name may be another's prefix."""
    jobs = _jobs(path)
    assert len(jobs) >= 4, jobs
    for i, (n1, m1, a1) in enumerate(jobs):
        for j, (n2, m2, a2) in enumerate(jobs):
            if i == j:
                continue
            assert m1 not in a2, (m1, a2)
            assert not n2.startswith(n1 + "_"), (n1, n2)


def test_T18d_check_guard_copies_compares_the_live_guard_with_the_copy_for_the_side_of_t0(world, tmp_path):
    """Design review C3: before T0 the live guard must match the workstation copy, after T0 the post-T0 copy."""
    import check_guard_copies as cgc
    guard = lambda ps: [c for c, live in ps if os.path.basename(live) == "RELAUNCH_GUARD.ps1"]   # noqa: E731
    assert guard(cgc.pairs()) == [PRE_COPY]
    (tmp_path / "CUTOVER").write_text("")
    assert guard(cgc.pairs()) == [POST_COPY]
    assert [p for p in cgc.pairs() if os.path.basename(p[1]) != "RELAUNCH_GUARD.ps1"] == \
        [p for p in cgc.PAIRS if os.path.basename(p[1]) != "RELAUNCH_GUARD.ps1"], "nothing else moves"


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell parses the guard")
def test_T18c_the_post_t0_copy_parses_in_windows_powershell():
    cmd = ("$t=$null;$e=$null;[void][System.Management.Automation.Language.Parser]::ParseFile("
           f"'{POST_COPY}',[ref]$t,[ref]$e);$e.Count")
    r = subprocess.run(["powershell", "-NoProfile", "-Command", cmd], capture_output=True, text=True, timeout=120)
    assert r.stdout.strip() == "0", (r.stdout, r.stderr)


# --------------------------------------------------------------------------- #
# T20, T21 - the two seams, and the pre-T0 lane untouched
# --------------------------------------------------------------------------- #
def test_T20_run_calls_both_patched_names_exactly_once_and_only_the_private_instance_is_patched():
    src = open(NL.LANE_PATH, encoding="utf-8").read()
    tree = ast.parse(src)
    run = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run")
    names = [c.func.id for c in ast.walk(run) if isinstance(c, ast.Call) and isinstance(c.func, ast.Name)]
    assert names.count("serve_owed_cube") == 1, "run() must serve through the patched name (seam 1)"
    assert names.count("Progress") == 1, "run() must build its beat through the patched name (seam 2)"
    top = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    assert {"serve_owed_cube", "Progress", "run", "serve_cube", "merge_cube", "load_state"} <= top
    assert L.serve_owed_cube is NL._serve_owed_under_writer_lock
    assert issubclass(L.Progress, NL._ORIG["Progress"]) and L.Progress is not NL._ORIG["Progress"]
    import jobs.statcan_lane as lane
    assert lane.serve_owed_cube is not NL._serve_owed_under_writer_lock and lane is not L
    assert lane.Progress.__name__ == "Progress" and lane.POST_T0_READY is False
    assert L.__name__ == NL.PRIVATE_NAME and NL.POST_T0_READY is True


def test_T21_the_pre_t0_lane_is_byte_identical_to_the_one_running_on_e():
    data = open(NL.LANE_PATH, "rb").read().replace(b"\r\n", b"\n")
    blob_sha = hashlib.sha1(b"blob %d\x00" % len(data) + data).hexdigest()     # noqa: S324 - git's blob id
    assert blob_sha == LANE_BLOB, (
        "jobs/statcan_lane.py changed. B3 runs it unchanged through a private instance (decision a), so before you "
        "move this pin RE-CHECK: (1) run() still calls serve_owed_cube( and Progress( once each (the two seams, T20); "
        "(2) every CSV it writes still goes through the `put` it is given (make_put's lock guard); (3) it gained no "
        "store writer the post-T0 lane does not lock (T22's ban list); (4) it is still pre-T0 only. Then update "
        "LANE_BLOB in the same reviewed PR.")


def test_T22_the_pre_t0_lane_writes_the_served_store_only_through_the_put_it_is_given():
    """The pin above does not say WHY the seams are enough. This does: the lane's source has no store writer of its
    own besides `_store_put` (which the post-T0 lane never calls - it passes its own put), no lock session and no
    backup/delete - so every served write of the private instance goes through make_put's guard (design review
    C12.1). A new writer in the lane fails here, not in production."""
    tree = ast.parse(open(NL.LANE_PATH, encoding="utf-8").read())
    banned = {"put_atomic", "put_file", "put_gzip_file", "put_if", "copy", "delete", "write_session",
              "write_session_for_process", "writer_lock", "backup_store_object", "delete_store_object"}
    hits = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for c in ast.walk(fn):
            if not isinstance(c, ast.Call):
                continue
            name = c.func.attr if isinstance(c.func, ast.Attribute) else getattr(c.func, "id", None)
            if name in banned:
                hits.append((fn.name, name))
            if name == "csv_store" and fn.name != "_store_put":
                hits.append((fn.name, name))
    assert hits == [], hits
    src = open(os.path.join(ROOT, "jobs", "statcan_selfhost_lane.py"), encoding="utf-8").read()
    assert "_store_put(" not in src, "the post-T0 lane must never use the pre-T0 lane's R2 put"
    # and the post-T0 path never imports the pre-T0 lane by its package name (module identity, R1294)
    mods = {n.module for n in ast.walk(ast.parse(src)) if isinstance(n, ast.ImportFrom)} | \
        {a.name for n in ast.walk(ast.parse(src)) if isinstance(n, ast.Import) for a in n.names}
    assert not any(m and "statcan_lane" in m for m in mods), mods


# --------------------------------------------------------------------------- #
# design review C1 - a lane starved of the lock reads red past 54 h
# --------------------------------------------------------------------------- #
def test_C1_the_oldest_lock_wait_over_serve_owed_cubes_is_published_and_red_past_54h(world, monkeypatch):
    now = dt.datetime.now(dt.timezone.utc)
    z = lambda h: (now - dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")      # noqa: E731
    st = L.load_state()
    rel = (now - dt.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M")     # inside the 14-day SLA: only the wait judges
    st["cubes"] = {
        "1": {"release": rel, "merged": rel, "served": None, "writer_lock_wait_since": z(55)},   # serve-owed
        "2": {"release": rel, "merged": None, "writer_lock_wait_since": z(80)},                 # merge-owed only
        "3": {"release": rel, "merged": rel, "served": rel, "writer_lock_wait_since": z(90)},   # served
        "4": {"release": rel, "merged": rel, "quarantined": True, "writer_lock_wait_since": z(99)},
    }
    p = L.Progress(st)
    p.beat("idle", current=None)
    prog = json.loads(open(L.PROGRESS).read())
    assert prog["writer_lock_wait"]["since"] == z(55) and prog["writer_lock_wait"]["cubes"] == 1, prog
    assert sc.LOCK_WAIT_RED_HOURS == 54.0
    with pytest.raises(DefinitiveError, match="waited for the catalogue writer lock"):
        sc.verdict({"cubes": {}}, prog, now)
    st["cubes"]["1"]["writer_lock_wait_since"] = z(30)
    p.beat("idle", current=None)
    prog = json.loads(open(L.PROGRESS).read())
    assert sc.verdict({"cubes": {}}, prog, now)[0] in ("ok", "partial"), "inside 54 h: not red"
    st["cubes"]["1"]["served"] = rel                                   # the wait ended: nothing is counted
    p.beat("idle", current=None)
    prog = json.loads(open(L.PROGRESS).read())
    assert "writer_lock_wait" not in prog and prog["writer_lock_waits"] == 0


# --------------------------------------------------------------------------- #
# design review C2 - a 48 h hold enumerates at the idle rate, not 6 times an hour
# --------------------------------------------------------------------------- #
def test_C2_a_48h_hold_of_the_writer_lock_enumerates_at_most_hourly(t0, tmp_path, monkeypatch):
    assert NL.LOCK_WAIT_RETRY_MIN >= L.ENUM_MIN_INTERVAL_MIN
    _net(monkeypatch)
    clock, calls = _Clock(), []
    start = clock.t
    with _Holder():
        while clock.t < start + dt.timedelta(hours=48):
            assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 0
            clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
    c = _state(t0)["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None and not c.get("transient_fails")
    assert c["writer_lock_wait_since"] == L._iso(start)
    assert len(calls) <= 49, f"{len(calls)} enumerations in 48 h - the idle rate is one an hour (R2027, R2250)"
    assert _served(tmp_path) == {}


# CODE REVIEW F1: the 1-cube test above cannot see per-cube phases. Real merges take wall time, so cubes whose serves
# first wait minutes apart kept their own minute of every hour, and each due iteration enumerates: the reviewer
# measured 95 cube-list GETs in 48 h with 2 cubes and 564 with 12 (review_code/probes_run2.log, probes_run3.log).
# Every shape below runs the REAL run() for 48 h against a REAL second process holding the lock, with the merge faked
# to take the given minutes of the simulated clock.
_C2_SHAPES = {
    "one_cube_control": [(str(PID), REL, 0)],
    "two_cubes_merged_20_min_apart": [(str(PID), REL, 0), ("11111111", "2026-09-13T08:30", 20)],
    "a_release_day_of_12_cubes_5_min_each": [(str(30000000 + i), f"2026-09-{12 + i // 10:02d}T08:30", 5)
                                             for i in range(12)],
    # a merge longer than the grid step between two serve attempts of ONE iteration: a grid anchored at
    # last_enum_utc splits here into two phases for the rest of the hold
    "a_90_min_merge_between_two_cubes": [(str(PID), REL, 0), ("11111111", "2026-09-13T08:30", 90)],
}


def _timed_kw(clock, releases, log):
    def enum(since):
        log.append(("list", clock.t))
        return dict(releases)

    def ends():
        log.append(("ends", clock.t))
        return {}, "not read in tests"
    return {"enumerate_releases": enum, "ends_fn": ends, "now_fn": clock}


@pytest.mark.parametrize("shape", sorted(_C2_SHAPES))
def test_C2_a_48h_hold_over_many_cubes_stays_on_one_grid(t0, tmp_path, monkeypatch, shape, capsys):
    cubes = _C2_SHAPES[shape]
    for pid, _rel, _m in cubes:
        if pid != str(PID):
            _stored(t0 / f"{pid}.parquet", [("w1", "2026-01-01", 1.0, "Canada", "1.1")])
    minutes = {pid: m for pid, _rel, m in cubes}
    clock, log = _Clock(), []
    start = clock.t

    def fake_merge(pid, c, st, progress, now, attempt=None):
        clock.t += dt.timedelta(minutes=minutes[pid])      # a download + merge takes wall time
        c["merged"] = c["release"]
        c.pop("merge_in_flight", None)
        return "merged"
    monkeypatch.setattr(L, "merge_cube", fake_merge)
    releases = {int(pid): rel for pid, rel, _m in cubes}
    with _Holder():
        while clock.t < start + dt.timedelta(hours=48):
            assert NL.main(["--once"], run_kw=_timed_kw(clock, releases, log)) == 0
            clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
    lists = [t for k, t in log if k == "list"]
    worst = max(sum(1 for _k, u in log if t <= u < t + dt.timedelta(minutes=60)) for _k, t in log)
    st = _state(t0)["cubes"]
    retries = {st[pid].get("retry_after") for pid, _rel, _m in cubes}
    with capsys.disabled():
        print(f"\nMEASURED C2 {shape}: {len(lists)} cube-list GETs and {len(log) - len(lists)} end-date GETs in 48 h, "
              f"at most {worst} StatCan GETs in any 60 minutes; retry_after {sorted(retries)}")
    assert all(st[pid]["merged"] == rel and st[pid].get("served") is None for pid, rel, _m in cubes)
    assert len(retries) == 1, f"every waiting cube is due at ONE grid point: {sorted(retries)}"
    assert len(lists) <= 49, f"{len(lists)} enumerations in 48 h - the idle rate is one an hour (R2027, F1)"
    assert worst <= 3, f"{worst} StatCan GETs inside one hour (R2028's bound was 3)"
    assert _served(tmp_path) == {}


def test_C2_the_grid_is_the_top_of_the_utc_hour_strictly_after_now():
    z = lambda s: dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)   # noqa: E731
    assert NL.LOCK_WAIT_RETRY_MIN == 60.0
    assert NL._grid_retry(z("2026-12-01T12:00:00Z")) == z("2026-12-01T13:00:00Z")
    assert NL._grid_retry(z("2026-12-01T12:59:59Z")) == z("2026-12-01T13:00:00Z")
    assert NL._grid_retry(z("2026-12-01T23:20:00Z")) == z("2026-12-02T00:00:00Z")


# --------------------------------------------------------------------------- #
# design review C4 - only "held elsewhere" waits; a lock that cannot be judged is BLOCKED
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("case", ["oserror_on_the_lock_file", "refused_recovery"])
def test_C4_a_lock_that_cannot_be_judged_is_blocked_red_and_the_process_stays(t0, tmp_path, monkeypatch, case):
    _net(monkeypatch)
    if case == "oserror_on_the_lock_file":
        (tmp_path / "a_file").write_text("x")                      # the lock's folder is a FILE: makedirs/open raise
        monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "a_file" / "writer.lock"))
        want = "could not be taken"
    else:
        def refuse():
            raise RuntimeError("the catalogue at X still has a live rollback journal after recovery: another "
                               "process is mid-write WITHOUT the writer lock")
        monkeypatch.setattr(catalog_path, "_recover_hot_journal", refuse)
        want = "WITHOUT the writer lock"
    assert NL.main(["--once"], run_kw=_run_kw()) == 2, "BLOCKED, and main returned - no crash"
    prog = _progress(t0)
    assert prog["state"] == "blocked" and want in prog["blocked"], prog
    with pytest.raises(DefinitiveError, match="BLOCKED"):
        sc.verdict({"cubes": {}}, prog, dt.datetime.now(dt.timezone.utc))
    c = _state(t0)["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None and "writer_lock_wait_since" not in c, "never a wait"
    assert catalog_path._held is None and _served(tmp_path) == {}
    assert _state(t0)["blocked_hold"]["kind"] == "lock" and prog["blocked_hold"]["kind"] == "lock"


def test_C4_F2_a_blocked_lock_holds_run_to_the_grid_reads_red_on_every_beat_and_clears(t0, tmp_path, monkeypatch):
    """CODE REVIEW F2: the BLOCKED path set no retry, so the cube stayed due and every 5-minute iteration enumerated
    (24 GETs in 2 h, review_code/probes_run2.log P2). Now the hold is in the lane state, run() is not called before its
    grid point, every beat carries it (red, idle beats included), and it clears when the lock can be judged again."""
    _net(monkeypatch)
    good = catalog_path.LOCK_PATH
    (tmp_path / "a_file").write_text("x")
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "a_file" / "writer.lock"))
    seen = []
    real_beat = L.Progress.beat

    def spy(self, *a, **k):
        r = real_beat(self, *a, **k)
        prog = _progress(t0)
        try:
            sc.verdict({"cubes": {}}, prog, dt.datetime.now(dt.timezone.utc))
            seen.append((prog.get("state"), "not red", "blocked_hold" in prog))
        except DefinitiveError as e:
            seen.append((prog.get("state"), str(e), "blocked_hold" in prog))
        return r
    monkeypatch.setattr(L.Progress, "beat", spy)
    clock, log, states = _Clock(), [], []
    start = clock.t
    while clock.t < start + dt.timedelta(hours=2):
        states.append(NL.main(["--once"], run_kw=_timed_kw(clock, {PID: REL}, log)))
        clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
    lists = [t for k, t in log if k == "list"]
    print(f"MEASURED F2: {len(lists)} cube-list GETs in 2 h while BLOCKED, {len(seen)} beats, rc {sorted(set(states))}")
    assert states == [2] * len(states), "BLOCKED after every iteration"
    assert len(lists) <= 3, f"{len(lists)} getAllCubesListLite enumerations in 2 h while BLOCKED"
    first = next(i for i, s in enumerate(seen) if s[2])
    after = seen[first:]
    assert after and all("BLOCKED" in msg for _s, msg, _h in after), after
    assert any(s != "blocked" for s, _m, _h in after), "an idle/working beat came between - and it read red too"
    h = _state(t0)["blocked_hold"]
    assert h["since"] == L._iso(start) and h["retry_after"] == L._iso(start + dt.timedelta(hours=2)), h
    # the lock can be judged again: at the next grid point the cube is served and the hold is over
    monkeypatch.setattr(catalog_path, "LOCK_PATH", good)
    clock.t = start + dt.timedelta(hours=2)
    assert NL.main(["--once"], run_kw=_timed_kw(clock, {PID: REL}, log)) == 0
    st = _state(t0)
    assert "blocked_hold" not in st and st["cubes"][str(PID)]["served"] == REL
    assert "blocked_hold" not in _progress(t0) and _progress(t0)["state"] != "blocked"


@pytest.mark.parametrize("cause", ["lock_file_oserror", "refused_recovery"])
def test_C4_F2_G2_a_lock_hold_whose_grid_point_comes_in_a_run_that_yields(t0, tmp_path, monkeypatch, cause):
    """A lock hold whose grid point came in a run that YIELDED (a waiter's marker), so the lock itself was not tried.
    CODE REVIEW ROUND 2, G2: the hold stood - red, with its old reason - for as long as the lane yielded (a 48 h pass)
    and run() ran once an hour. Now a hold caused by the lock FILE (an OSError) is judged again by opening the file:
    it opens, so the hold is over and the lane works on while it yields. A hold the file cannot judge (a refused
    hot-journal recovery, judged only by taking the lock) stands, moves to the NEXT grid point (a past one would call
    run() every iteration) and says it was not judged again because the lane yields."""
    _net(monkeypatch)
    good = catalog_path.LOCK_PATH
    real_recover = catalog_path._recover_hot_journal
    if cause == "lock_file_oserror":
        (tmp_path / "a_file").write_text("x")                      # the lock's folder is a FILE: makedirs raises
        monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "a_file" / "writer.lock"))
    else:
        def refuse():
            raise RuntimeError("the catalogue at X could not be opened for recovery (planted)")
        monkeypatch.setattr(catalog_path, "_recover_hot_journal", refuse)
    clock = _Clock()
    assert NL.main(["--once"], run_kw=_run_kw(clock)) == 2
    h = _state(t0)["blocked_hold"]
    assert h["retry_after"] == "2026-12-01T13:00:00Z" and bool(h.get("oserror")) is (cause == "lock_file_oserror"), h
    monkeypatch.setattr(catalog_path, "LOCK_PATH", good)          # the lock file is fine again (both cases)
    os.makedirs(os.path.dirname(good), exist_ok=True)
    open(good + ".want", "w").write("a waiter")
    clock.t = dt.datetime(2026, 12, 1, 13, 0, tzinfo=dt.timezone.utc)
    rc = NL.main(["--once"], run_kw=_run_kw(clock))
    st, prog = _state(t0), _progress(t0)
    c = st["cubes"][str(PID)]
    assert c.get("served") is None and c["retry_after"] == "2026-12-01T14:00:00Z", c
    assert "a waiter's marker" in NL._wait_logged["why"], "the run yielded; the lock itself was not tried"
    if cause == "lock_file_oserror":
        assert rc == 0 and "blocked_hold" not in st and "blocked_hold" not in prog, st.get("blocked_hold")
        assert prog["state"] != "blocked", prog
    else:
        h = st["blocked_hold"]
        assert rc == 2 and h["retry_after"] == "2026-12-01T14:00:00Z" and h["since"] == "2026-12-01T12:00:00Z", h
        assert h["yielding"]["since"] == "2026-12-01T13:00:00Z" and "a waiter's marker" in h["yielding"]["to"], h
        assert prog["state"] == "blocked" and "not judged again since 2026-12-01T13:00:00Z" in prog["blocked"], prog
        monkeypatch.setattr(catalog_path, "_recover_hot_journal", real_recover)
    os.remove(good + ".want")
    clock.t = dt.datetime(2026, 12, 1, 14, 0, tzinfo=dt.timezone.utc)
    assert NL.main(["--once"], run_kw=_run_kw(clock)) == 0
    assert "blocked_hold" not in _state(t0) and _state(t0)["cubes"][str(PID)]["served"] == REL


def test_G2_P6_one_lock_file_fault_then_a_desktop_pass_does_not_read_blocked_for_the_whole_pass(t0, tmp_path,
                                                                                               monkeypatch, capsys):
    """CODE REVIEW ROUND 2, G2 (the reviewer's probe P6, review_code/VERDICT_round2.md): the lock FILE fails once and is
    fine again, then a desktop pass runs. The lane yields at every grid point and never tries the lock, so it read
    BLOCKED with the old reason for the whole pass (measured: rc 2 for 6 h, the hold still there) and called run() -
    its merges - once an hour. Now the first grid-point run that yields judges the file, the hold is over, and from
    then on every iteration runs while the serve waits for the pass."""
    import psutil
    _net(monkeypatch)
    good = catalog_path.LOCK_PATH
    (tmp_path / "a_file").write_text("x")
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "a_file" / "writer.lock"))
    clock, calls = _Clock(), []
    assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 2
    monkeypatch.setattr(catalog_path, "LOCK_PATH", good)                 # the lock can be taken again
    me = psutil.Process(os.getpid()).create_time()
    lh = tmp_path / "logs" / "local_heavy.lock"
    lh.parent.mkdir(exist_ok=True)
    lh.write_text(f"{os.getpid()},{int((me + 62_135_596_800) * 10_000_000)}")   # a LIVE desktop pass
    start, rcs = clock.t, []
    while clock.t < start + dt.timedelta(hours=6):
        clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
        rcs.append((L._iso(clock.t), NL.main(["--once"], run_kw=_run_kw(clock, calls=calls))))
    st, prog = _state(t0), _progress(t0)
    with capsys.disabled():
        print(f"\nMEASURED G2 P6: 6 h of a pass after one lock-file fault: rc {sorted({rc for _t, rc in rcs})}, "
              f"{len(calls)} enumerations, hold {st.get('blocked_hold')}, beat state {prog.get('state')}")
    assert "blocked_hold" not in st and "blocked_hold" not in prog, st.get("blocked_hold")
    assert all(rc == 2 for t, rc in rcs if t < "2026-12-01T13:00:00Z"), "held to its grid point, as before"
    assert all(rc == 0 for t, rc in rcs if t >= "2026-12-01T13:00:00Z"), rcs
    c = st["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None and c.get("writer_lock_wait_since"), c
    assert "a desktop pass is running" in str((prog.get("writer_lock_wait") or {}).get("holder")), prog
    assert len(calls) <= 8, f"{len(calls)} enumerations in 6 h"


def test_C4_F2_a_refusal_inside_run_is_held_to_the_grid_too(t0, monkeypatch):
    calls = []

    def refusing_run(**k):
        k["enumerate_releases"](L.ENUM_ALL)
        raise SystemExit("REFUSING: planted")
    monkeypatch.setattr(L, "run", refusing_run)
    clock = _Clock()
    for _ in range(6):                                          # 30 minutes
        assert NL.iteration(run_kw=dict(_run_kw(clock, calls=calls))) == 2
        clock.t += dt.timedelta(minutes=5)
    assert calls == ["enum"], "run() is called once, then held to the grid point"
    h = _state(t0)["blocked_hold"]
    assert h["kind"] == "refusal" and "planted" in h["why"] and h["retry_after"] == "2026-12-01T13:00:00Z"
    with pytest.raises(DefinitiveError, match="BLOCKED"):
        sc.verdict({"cubes": {}}, _progress(t0), dt.datetime.now(dt.timezone.utc))
    monkeypatch.setattr(L, "run", lambda **k: {"enumerated": True, "worked": 0})
    clock.t = dt.datetime(2026, 12, 1, 13, 0, tzinfo=dt.timezone.utc)
    assert NL.iteration(run_kw=dict(_run_kw(clock, calls=calls))) == 0
    assert "blocked_hold" not in _state(t0), "a run that returned ends a refusal hold"


# --------------------------------------------------------------------------- #
# design review C5 - the yield rule
# --------------------------------------------------------------------------- #
def _seam(monkeypatch, served):
    monkeypatch.setitem(NL._ORIG, "serve_owed_cube", lambda *a: served.append(catalog_path._held is not None) or "served")


def _p():
    return L.Progress(L.load_state())


def test_C5_a_fresh_want_marker_or_a_live_pass_gets_no_new_session(t0, tmp_path, monkeypatch):
    served = []
    _seam(monkeypatch, served)
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    want = catalog_path.LOCK_PATH + ".want"
    open(want, "w").write("pid 1 2026-12-01T00:00:00Z")
    c = {"merged": REL}
    assert NL._serve_owed_under_writer_lock("1", c, {}, {}, None, _p(), L._now()) == "lock_wait"
    assert served == [] and "writer_lock_wait_since" in c and "yielding" in NL._wait_logged["why"]
    import time as _time
    old = _time.time() - (NL.WANT_FRESH_MIN + 5) * 60
    os.utime(want, (old, old))
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "served"
    assert served == [True], "a stale marker does not hold the lane off; the serve ran inside the lock"
    os.remove(want)
    import psutil
    me = psutil.Process(os.getpid()).create_time()
    lh = tmp_path / "local_heavy.lock"
    monkeypatch.setattr(NL, "LOCAL_HEAVY_LOCK", str(lh))
    lh.write_text(f"{os.getpid()},{int((me + 62_135_596_800) * 10_000_000)}")
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "lock_wait"
    assert "a desktop pass is running" in NL._wait_logged["why"]
    lh.write_text("not a pid")
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "lock_wait"
    lh.write_text(f"{os.getpid()},{int((me + 62_135_596_800 - 3600) * 10_000_000)}")   # a recycled pid
    monkeypatch.setattr(NL.time, "sleep", lambda s: None)
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "served"


def test_C5_two_sessions_are_at_least_LOCK_GAP_S_apart(t0, monkeypatch):
    served, slept = [], []
    _seam(monkeypatch, served)
    monkeypatch.setattr(NL.time, "sleep", lambda s: slept.append(s))
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "served"
    assert slept == [], "the first session waits for nothing"
    assert NL._serve_owed_under_writer_lock("2", {"merged": REL}, {}, {}, None, _p(), L._now()) == "served"
    assert len(slept) == 1 and NL.LOCK_GAP_S - 5 < slept[0] <= NL.LOCK_GAP_S, slept
    assert NL.LOCK_GAP_S >= 60


# --------------------------------------------------------------------------- #
# design review C6 - the lane's own records and the held cubes, checked before run()
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("case", ["torn_state", "torn_debt", "held_cube_lost", "absent_cube_is_fine"])
def test_C6_a_torn_record_or_a_lost_held_cube_is_blocked_before_run(t0, monkeypatch, case):
    calls = []
    _net(monkeypatch)
    if case == "torn_state":
        (t0 / "_lane_state.json").write_text('{"cubes": {')
        want = "lane state"
    elif case == "torn_debt":
        (t0 / "_lane_debt.json").write_text('{"cubes"')
        want = "catalogue debt"
    else:
        st = {"version": 1, "floor": L.FLOOR, "cubes": {
            str(PID): {"release": REL, "merged": REL, "served": REL},
            "99999999": {"release": REL, "merged": REL, "served": REL,
                         "not_held": case == "absent_cube_is_fine"}}}
        (t0 / "_lane_state.json").write_text(json.dumps(st))
        want = "1 cube(s) the lane state records as held have no parquet"
    rc = NL.main(["--once"], run_kw=_run_kw(calls=calls))
    if case == "absent_cube_is_fine":
        assert rc == 0 and calls == ["enum"]
        return
    assert rc == 2 and calls == [], "run() was not called"
    prog = _progress(t0)
    assert prog["state"] == "blocked" and want in prog["blocked"], prog


def test_C6_F3_a_catalogued_cube_lost_from_the_store_and_booked_not_held_is_blocked(world, tmp_path, monkeypatch):
    """CODE REVIEW F3 (the reviewer's probe P3): a catalogued cube the lane state has never seen (no release since the
    floor) loses its parquet; its next release is booked not_held and settled by run(). From the next iteration on
    the preflight reads it as LOST (a catalogued cube is never new) - BLOCKED, and run() is not called again."""
    lost = "22222222"
    con = sqlite3.connect(tmp_path / "catalog.db")
    con.execute("INSERT INTO series VALUES (?)", (f"statcan:{lost}",))      # catalogued (served before) ...
    con.commit()
    con.close()                                                              # ... but no parquet in the store
    (tmp_path / "CUTOVER").write_text("")                                    # T0, after the catalogue write
    t0 = world
    _net(monkeypatch)
    rels, calls = {PID: REL, int(lost): "2026-10-01T08:30"}, []
    assert NL.main(["--once"], run_kw=_run_kw(releases=rels, calls=calls)) == 0
    c = _state(t0)["cubes"][lost]
    assert c.get("not_held") and c["served"] == "2026-10-01T08:30", "run() booked it settled (the pre-T0 code)"
    assert NL.main(["--once"], run_kw=_run_kw(releases=rels, calls=calls)) == 2
    prog = _progress(t0)
    assert prog["state"] == "blocked" and "NOT HELD have catalogued ids and no parquet" in prog["blocked"], prog
    assert lost in prog["blocked"] and calls == ["enum"], "the second iteration asked StatCan nothing"
    assert _state(t0)["lost_checked"] == {lost: {"release": "2026-10-01T08:30", "catalogued": True}}
    # ROUND 2, G1: the answer is KEPT - a catalogue that cannot be read now does not lift the block
    real = NL._catalogued

    def busy(pids):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(NL, "_catalogued", busy)
    assert NL.main(["--once"], run_kw=_run_kw(releases=rels, calls=calls)) == 2 and calls == ["enum"]
    prog = _progress(t0)
    assert "NOT HELD have catalogued ids and no parquet" in prog["blocked"], prog
    assert prog["lost_check_fail"]["pending"] == 0 and "database is locked" in prog["lost_check_fail"]["last_error"]
    monkeypatch.setattr(NL, "_catalogued", real)
    # a not-held cube with NO catalogued id is a new cube, and one whose parquet is back is held again: both fine
    _stored(t0 / f"{lost}.parquet", [("w1", "2026-01-01", 1.0, "Canada", "1.1")])
    why, _put, names = NL.store_problems()
    assert why is None and f"{lost}.parquet" in names
    st = L.load_state()
    assert NL.lost_cube_check(st, names, L._now())[0] is None
    assert "lost_checked" not in st and "lost_check_fail" not in st, "held again: the kept answer is dropped"
    st["cubes"]["77777777"] = {"release": REL, "merged": REL, "served": REL, "not_held": True}
    assert NL.lost_cube_check(st, names, L._now()) == (None, True)
    assert st["lost_checked"] == {"77777777": {"release": REL, "catalogued": False}}


def test_F4_the_lock_wait_red_comes_before_the_draining_return():
    """CODE REVIEW F4: an owed release past 14 days with fresh progress returns DRAINING (amber); the lock-wait red
    must be read before that return, or a lane starved of the lock that keeps merging reads amber for ever."""
    now = dt.datetime.now(dt.timezone.utc)
    z = lambda h: (now - dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")      # noqa: E731
    prog = {"beat_utc": z(0.1), "state": "idle", "current": None, "last_progress_utc": z(1),
            "owed": {"merge": 3, "serve": 2, "new_cubes": 0,
                     "oldest_owed_release": (now - dt.timedelta(days=20)).strftime("%Y-%m-%dT%H:%M")}}
    assert "DRAINING" in sc.verdict({"cubes": {}}, prog, now)[1], "the control: without the wait it drains"
    prog["writer_lock_wait"] = {"since": z(60), "cubes": 2, "holder": "held elsewhere"}
    with pytest.raises(DefinitiveError, match="waited for the catalogue writer lock"):
        sc.verdict({"cubes": {}}, prog, now)


def test_F7_a_waiter_that_comes_during_the_gap_gets_the_lock_first(t0, monkeypatch):
    """CODE REVIEW F7: the yield rule is asked again AFTER the gap sleep - the gap is when a waiter comes."""
    served = []
    _seam(monkeypatch, served)
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    want = catalog_path.LOCK_PATH + ".want"
    monkeypatch.setattr(NL.time, "sleep", lambda s: open(want, "w").write("a waiter, during the gap"))
    assert NL._serve_owed_under_writer_lock("1", {"merged": REL}, {}, {}, None, _p(), L._now()) == "served"
    c = {"merged": REL}
    assert NL._serve_owed_under_writer_lock("2", c, {}, {}, None, _p(), L._now()) == "lock_wait"
    assert served == [True] and "writer_lock_wait_since" in c and "a waiter's marker" in NL._wait_logged["why"]
    assert catalog_path._held is None and _probe() == "took"


# --------------------------------------------------------------------------- #
# code review round 2, G1 - the lost-cube check must not stop the merges
# --------------------------------------------------------------------------- #
# A second process holding the CATALOGUE itself - SQLite EXCLUSIVE, rollback-journal mode as the live one (ruling v2) -
# until stdin gets a line (the reviewer's review_code/excl_holder.py). It writes no row.
_EXCL = textwrap.dedent("""
    import sqlite3, sys
    con = sqlite3.connect(sys.argv[1], isolation_level=None)
    con.execute("PRAGMA journal_mode=DELETE")
    con.execute("BEGIN EXCLUSIVE")
    print("held", flush=True)
    sys.stdin.readline()
    con.execute("ROLLBACK")
    con.close()
""")


class _CatalogueHolder(_Holder):
    def __init__(self, path):
        self.path = str(path)

    def __enter__(self):
        self.p = subprocess.Popen([sys.executable, "-c", _EXCL, self.path, CHILD_MARK],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert self.p.stdout.readline().strip() == "held"
        return self


def _a_new_cube_in_the_debt(t0):
    """An ordinary new cube: booked not held (no parquet, no catalogued id) - a lost-cube check candidate."""
    st = {"version": 1, "floor": L.FLOOR, "cubes": {
        "77777777": {"release": REL, "merged": REL, "served": REL, "not_held": True}},
        "new_cubes": {"77777777": REL}}
    (t0 / "_lane_state.json").write_text(json.dumps(st))


def test_G1_P7_a_writer_holding_the_catalogue_does_not_stop_the_merges(t0, tmp_path, monkeypatch, capsys):
    """CODE REVIEW ROUND 2, G1 (the reviewer's probe P7): the lost-cube check read the catalogue outside the writer
    lock, every iteration with a not-held cube (every new cube in the catalogue debt), and a read that failed BLOCKED
    the lane: a writer holding the catalogue EXCLUSIVE stalled the iteration 309 s and nothing was merged. Here the
    writer is a real post-T0 one: it holds the writer lock AND the catalogue. Now the read gives up after
    LOST_CHECK_BUSY_S and is SKIPPED (kept as lost_check_fail, on the beat), the cube is merged and its serve waits for
    the lock; with the writer gone, the next iteration reads the booking once and the failure is over."""
    import time as _t
    _net(monkeypatch)
    _a_new_cube_in_the_debt(t0)
    calls = []
    with _Holder(), _CatalogueHolder(tmp_path / "catalog.db"):
        t = _t.monotonic()
        rc = NL.main(["--once"], run_kw=_run_kw(calls=calls))
        took = _t.monotonic() - t
    s, prog = _state(t0), _progress(t0)
    with capsys.disabled():
        print(f"\nMEASURED G1 P7 (writer lock + catalogue held): rc={rc} took={took:.0f} s enumerations={len(calls)} "
              f"state={prog.get('state')} lost_check_fail={s.get('lost_check_fail')}")
    assert rc == 0 and calls == ["enum"], "a busy catalogue does not stop the merges (they never read it)"
    assert took < 4 * NL.LOST_CHECK_BUSY_S + 60, f"{took:.0f} s - the check waits LOST_CHECK_BUSY_S, not 180 s"
    c = s["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None and c.get("writer_lock_wait_since"), c
    f = s["lost_check_fail"]
    assert f["pending"] == 1 and f["count"] == 1 and "OperationalError" in f["last_error"], f
    assert prog["lost_check_fail"] == f and prog["state"] != "blocked", prog
    assert "lost_checked" not in s, "nothing was answered"
    # the writer is gone: the booking is read now, its answer kept, and the failure is over
    assert NL.main(["--once"], run_kw=_run_kw(calls=calls)) == 0
    s = _state(t0)
    assert "lost_check_fail" not in s and "lost_check_fail" not in _progress(t0)
    assert s["lost_checked"] == {"77777777": {"release": REL, "catalogued": False}}


def test_G1_P7_a_catalogue_held_WITHOUT_the_writer_lock_still_merges_then_the_serve_raises_the_legacy_alarm(
        t0, tmp_path, monkeypatch):
    """The reviewer's P7 exactly: a second process holds the catalogue EXCLUSIVE but NOT the writer lock. After T0
    every catalogue writer holds that lock, so this is core.catalog_path's legacy-opener alarm (_recover_hot_journal:
    "another process is writing it WITHOUT the writer lock"). The preflight no longer blocks - the cube is merged -
    and the serve then takes the lock, its recovery cannot open the catalogue, and the lane is BLOCKED with THAT
    reason (unchanged, by design). So the probe's `rc == 0` cannot hold in this shape; the merge is what G1 asks."""
    _net(monkeypatch)
    _a_new_cube_in_the_debt(t0)
    calls = []
    with _CatalogueHolder(tmp_path / "catalog.db"):
        rc = NL.main(["--once"], run_kw=_run_kw(calls=calls))
    s, prog = _state(t0), _progress(t0)
    assert calls == ["enum"] and s["cubes"][str(PID)]["merged"] == REL, "merged: the preflight did not block"
    assert s["lost_check_fail"]["pending"] == 1, s.get("lost_check_fail")
    assert rc == 2 and prog["state"] == "blocked" and "could not be opened for recovery" in prog["blocked"], prog
    assert s["blocked_hold"]["kind"] == "lock" and not s["blocked_hold"].get("oserror"), s["blocked_hold"]


def test_G1_a_not_held_booking_is_read_once_and_a_failed_read_is_skipped_while_the_merges_go_on(t0, monkeypatch):
    """ROUND 2, G1: ONE catalogue read per not-held booking (kept in `lost_checked`), not one per iteration; a new
    booking of the same cube (StatCan released it again) is read again; a read that fails is skipped - every
    iteration works, a new release is merged and served meanwhile - and kept as lost_check_fail on every beat."""
    _net(monkeypatch)
    monkeypatch.setattr(NL.time, "sleep", lambda s: None)        # the 60 s gap between two serves of this test
    _a_new_cube_in_the_debt(t0)
    reads = []
    real = NL._catalogued
    monkeypatch.setattr(NL, "_catalogued", lambda pids: reads.append(list(pids)) or real(pids))
    clock, calls = _Clock(), []
    for _ in range(3):
        assert NL.main(["--once"], run_kw=_run_kw(clock, calls=calls)) == 0
        clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
    assert reads == [["77777777"]], "ONE read for the booking, not one per iteration"
    assert _state(t0)["lost_checked"] == {"77777777": {"release": REL, "catalogued": False}}
    # StatCan releases the cube again: run() books the new release not held, and the next check reads it again
    rels = {PID: REL, 77777777: "2026-10-01T08:30"}
    clock.t += dt.timedelta(hours=1)
    assert NL.main(["--once"], run_kw=_run_kw(clock, releases=rels, calls=calls)) == 0
    assert _state(t0)["cubes"]["77777777"]["release"] == "2026-10-01T08:30"
    assert NL.main(["--once"], run_kw=_run_kw(clock, releases=rels, calls=calls)) == 0
    assert reads == [["77777777"], ["77777777"]], "a NEW not-held booking is read again"
    # a booking whose read FAILS: skipped, the lane works on, the failure is kept in the state and on the beat
    s = _state(t0)
    s["cubes"]["88888888"] = {"release": REL, "merged": REL, "served": REL, "not_held": True}
    (t0 / "_lane_state.json").write_text(json.dumps(s))

    def busy(pids):
        reads.append(list(pids))
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(NL, "_catalogued", busy)
    rels2 = {PID: "2026-10-05T08:30", 77777777: "2026-10-01T08:30"}
    start, rcs = clock.t, []
    while clock.t < start + dt.timedelta(hours=2):
        rcs.append(NL.main(["--once"], run_kw=_run_kw(clock, releases=rels2, calls=calls)))
        clock.t += dt.timedelta(seconds=L.IDLE_SLEEP_S)
    s, prog = _state(t0), _progress(t0)
    assert rcs == [0] * len(rcs), "never BLOCKED by a check that cannot run"
    c = s["cubes"][str(PID)]
    assert c["merged"] == "2026-10-05T08:30" == c["served"], "a new release was merged AND served meanwhile"
    f = s["lost_check_fail"]
    assert f["since"] == L._iso(start) and f["count"] == len(rcs) and f["pending"] == 1, f
    assert prog["lost_check_fail"] == f, "the failure is on the beat"
    assert reads[2:] == [["88888888"]] * len(rcs), "only the unanswered booking is tried, once per iteration"


def test_G1_the_failed_check_is_on_every_beat_and_red_only_past_LOST_CHECK_RED_HOURS(world):
    now = dt.datetime.now(dt.timezone.utc)
    z = lambda h: (now - dt.timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ")      # noqa: E731
    assert sc.LOST_CHECK_RED_HOURS == 6.0
    st = L.load_state()
    st["lost_check_fail"] = {"since": z(2), "count": 24, "pending": 1,
                             "last_error": "OperationalError: database is locked"}
    p = L.Progress(st)
    p.beat("idle", current=None)
    prog = json.loads(open(L.PROGRESS).read())
    assert prog["lost_check_fail"] == st["lost_check_fail"] and prog["lane"] == NL.MODULE_ID, prog
    assert sc.verdict({"cubes": {}}, prog, now)[0] in ("ok", "partial"), "inside 6 h: not red (the merges go on)"
    st["lost_check_fail"]["since"] = z(7)
    p.beat("idle", current=None)
    prog = json.loads(open(L.PROGRESS).read())
    with pytest.raises(DefinitiveError, match="cannot CHECK its not-held cubes against the catalogue"):
        sc.verdict({"cubes": {}}, prog, now)
    st.pop("lost_check_fail")
    p.beat("idle", current=None)
    assert "lost_check_fail" not in json.loads(open(L.PROGRESS).read()), "the next good read ends it on the beat"


# --------------------------------------------------------------------------- #
# code review round 2, G3 - the S13-prep census of lost not-held cubes, on copies
# --------------------------------------------------------------------------- #
def _census_copies(tmp_path, catalogued, store, cubes):
    d = tmp_path / "copies"
    (d / "store").mkdir(parents=True)
    for name in store:
        (d / "store" / name).write_bytes(b"")                  # names only: the census opens no store file
    (d / "state.json").write_text(json.dumps({"version": 1, "cubes": cubes}))
    con = sqlite3.connect(d / "cat.sqlite")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO series VALUES (?)", [(s,) for s in catalogued])
    con.commit()
    con.close()
    return d


def _census_args(d, store=None):
    return ["--state", str(d / "state.json"), "--catalogue", str(d / "cat.sqlite")] + (
        store or ["--store-dir", str(d / "store")])


_CENSUS_CUBES = {
    "11111111": {"release": REL, "merged": REL, "served": REL, "not_held": True},    # LOST: catalogued, no parquet
    "22222222": {"release": REL, "merged": REL, "served": REL, "not_held": True},    # new: no catalogued id
    "33333333": {"release": REL, "merged": REL, "served": REL, "not_held": True},    # its parquet is there
    str(PID): {"release": REL, "merged": REL, "served": REL}}                         # held
# 11111111 has two ids; "statcan:2222222" (7 digits) and "statcan:222222220" (9) sort outside 22222222's range
_CENSUS_IDS = ["statcan:11111111#a", "statcan:11111111#b", "statcan:33333333", "statcan:2222222",
               "statcan:222222220"]


def test_G3_the_census_counts_the_lost_not_held_cubes_on_copies_as_the_lane_would(world, tmp_path, monkeypatch,
                                                                                    capsys):
    d = _census_copies(tmp_path, _CENSUS_IDS, [f"{PID}.parquet", "33333333.parquet", "_split_map.json"],
                       _CENSUS_CUBES)
    before = (d / "cat.sqlite").read_bytes()
    assert LC.main(_census_args(d)) == 1
    out = capsys.readouterr().out
    assert "booked not held with no parquet: 2" in out and out.count("  LOST ") == 1, out
    assert "LOST 11111111: booked not held at release 2026-09-12T08:30, 2 catalogued id(s), no parquet" in out, out
    assert "recorded HELD with no parquet: 0" in out and "HELD-LOST" not in out, out
    assert "RESULT: 1 LOST not-held cube(s) and 0 held cube(s) with no parquet" in out, out
    assert (d / "cat.sqlite").read_bytes() == before and not (d / "cat.sqlite-journal").exists(), "read-only"
    # THE LANE'S OWN RULE on the same copies (before T0 connect_path opens any path): the same candidates, the same
    # lost cube
    st = json.loads((d / "state.json").read_text())
    names = LC.store_names(str(d / "store"))
    assert LC.not_held_without_parquet(st, names) == NL.not_held_without_parquet(st, names)
    monkeypatch.setattr(L, "CATALOGUE", str(d / "cat.sqlite"))
    why, _changed = NL.lost_cube_check(st, names, L._now())
    assert why and "1 cube(s) the lane state books as NOT HELD" in why and "11111111" in why, why
    assert {p for p, a in st["lost_checked"].items() if a["catalogued"]} == {"11111111"}
    # the same through a listing file (CRLF lines), and the negative control: its parquet restored -> 0 lost
    (d / "listing.txt").write_text("\r\n".join([f"{PID}.parquet", "33333333.parquet", "11111111.parquet"]) + "\r\n")
    assert LC.main(_census_args(d, ["--store-listing", str(d / "listing.txt")])) == 0
    assert "RESULT: 0 lost cubes" in capsys.readouterr().out
    # the lane's OTHER first-iteration block, counted beside it: a cube recorded HELD whose parquet is gone
    st["cubes"]["44444444"] = {"release": REL, "merged": REL, "served": REL}
    (d / "state.json").write_text(json.dumps(st))
    names = LC.store_names(listing=str(d / "listing.txt"))
    assert LC.held_without_parquet(st, names) == NL.held_without_parquet(st, names) == ["44444444"]
    assert LC.main(_census_args(d, ["--store-listing", str(d / "listing.txt")])) == 1
    out = capsys.readouterr().out
    assert "HELD-LOST 44444444: merged 2026-09-12T08:30" in out and "0 LOST not-held cube(s) and 1 held" in out, out


def test_G3_the_census_refuses_live_places_and_inputs_it_cannot_judge(tmp_path, monkeypatch, capsys):
    d = _census_copies(tmp_path, _CENSUS_IDS, [f"{PID}.parquet"], _CENSUS_CUBES)
    monkeypatch.setattr(LC, "REFUSED_ROOTS", (str(d),))
    assert LC.main(_census_args(d)) == 2 and "REFUSED" in capsys.readouterr().out
    monkeypatch.setattr(LC, "REFUSED_ROOTS", (str(tmp_path / "elsewhere"),))
    assert LC.main(_census_args(d)) == 1, "the control: the same copies outside a refused place are read"
    capsys.readouterr()
    assert LC.refused(str(d / "sub" / "f"), roots=(str(d),)) and not LC.refused(str(d) + "x", roots=(str(d),))
    (d / "state.json").rename(d / "state.moved")
    assert LC.main(_census_args(d)) == 2 and "MISSING" in capsys.readouterr().out
    (d / "state.json").write_text('{"cubes": {')
    assert LC.main(_census_args(d)) == 2 and "UNREADABLE" in capsys.readouterr().out
    (d / "state.json").write_text(json.dumps({"version": 1, "cubes": {}}))
    assert LC.main(_census_args(d)) == 2 and "books no cubes" in capsys.readouterr().out
    (d / "state.json").write_text(json.dumps({"version": 1, "cubes": _CENSUS_CUBES}))
    (d / "store" / f"{PID}.parquet").unlink()
    assert LC.main(_census_args(d)) == 2 and "lists 0 parquets" in capsys.readouterr().out


# every spelling of a live place a mistaken argument can take: case, either slash, mixed, `..`, the \\?\ prefix
_LIVE_SPELLINGS = [
    "E:\\research\\econfindatalibrary\\data\\catalog.db",
    "e:/Research/EconFinDataLibrary/DATA/state.db",
    "E:\\research/econfindatalibrary\\data",
    "E:\\research\\econfindatalibrary\\tools\\..\\data\\catalog.db",
    "E:\\econ_live",
    "E:/ECON_LIVE/state/writer.lock",
    "\\\\?\\E:\\econ_live\\state\\writer.lock",
]


def _touch_spies(monkeypatch) -> list:
    """(call, path) for every call that can stat, open or resolve a file - B24's R2-L6 spy, wider. Each spy passes
    the call on, so a path that passes the check is still really resolved."""
    import builtins
    import ntpath
    seen = []
    targets = [(os, "stat"), (os, "lstat"), (os, "open"), (os, "listdir"), (os, "scandir"), (os.path, "exists"),
               (os.path, "isfile"), (os.path, "isdir"), (os.path, "realpath"), (os.path, "samefile"),
               (builtins, "open"), (io, "open"), (sqlite3, "connect")]
    if hasattr(ntpath, "_getfinalpathname"):                 # the Windows call that opens the handle
        targets.append((ntpath, "_getfinalpathname"))
    for mod, name in targets:
        real = getattr(mod, name)

        def spy(*a, _real=real, _name=name, **k):
            p = a[0] if a else k.get("path", k.get("file", k.get("database")))
            seen.append((_name, os.fspath(p) if isinstance(p, (str, bytes, os.PathLike)) else repr(p)))
            return _real(*a, **k)
        monkeypatch.setattr(mod, name, spy)
    return seen


def _e_paths(seen) -> list:
    """The recorded calls on ANY E: path (folded by hand here, not by the code under test)."""
    def e(s):
        s = s.decode() if isinstance(s, bytes) else s
        s = s.replace("/", "\\").lower()
        for pre in ("\\\\?\\", "\\\\.\\"):
            s = s[len(pre):] if s.startswith(pre) else s
        return s.startswith("e:")
    return [c for c in seen if e(c[1])]


def test_G3_note_a_live_path_is_refused_on_its_text_and_never_stat_ed_opened_or_resolved(tmp_path, monkeypatch,
                                                                                          capsys):
    """Code re-review round 3, the G3 note (LOW): refused() called os.path.realpath on the path BEFORE deciding - on
    Windows that opens a handle (_getfinalpathname) - so a live path given by mistake was touched by the guard that
    refuses it. Now the text decides first; only a path that passes is resolved."""
    d = _census_copies(tmp_path, _CENSUS_IDS, [f"{PID}.parquet"], _CENSUS_CUBES)
    seen = _touch_spies(monkeypatch)
    for p in _LIVE_SPELLINGS:
        assert LC.refused(p), p
    assert seen == [], seen                                  # nothing at all was stat-ed, opened or resolved
    # through the tool's entry point: each input in turn given as a live path, the others real copies
    good = {"--state": str(d / "state.json"), "--catalogue": str(d / "cat.sqlite"), "--store-dir": str(d / "store")}
    for flag in good:
        for live in (_LIVE_SPELLINGS[0], _LIVE_SPELLINGS[5]):
            args = [x for f, v in good.items() for x in (f, live if f == flag else v)]
            assert LC.main(args) == 2, (flag, live)
            assert "REFUSED" in capsys.readouterr().out
    assert _e_paths(seen) == [], _e_paths(seen)
    # the controls. The spies record: a copy that passes the text test IS resolved, and the census reads it
    assert LC.refused(str(d / "state.json")) is None
    assert ("realpath", str(d / "state.json")) in seen, seen
    assert LC.main(_census_args(d)) == 1 and "LOST 11111111" in capsys.readouterr().out
    # ... and a link into a refused place is still refused, by the resolve step
    live, link = tmp_path / "live", tmp_path / "link"
    live.mkdir()
    try:
        os.symlink(live, link, target_is_directory=True)
    except (OSError, NotImplementedError):
        if os.name != "nt":
            raise
        import _winapi
        _winapi.CreateJunction(str(live), str(link))         # no symlink privilege: a junction, as on the desktop
    assert LC.refused(str(live / "catalog.db"), roots=(str(live),)), "the text test"
    why = LC.refused(str(link / "catalog.db"), roots=(str(live),))
    assert why and "resolves to" in why, why
    assert LC.refused(str(tmp_path / "livex" / "catalog.db"), roots=(str(live),)) is None, "a near-miss name"


def test_G3_the_census_pins_its_live_places_and_id_scheme_to_the_code_it_mirrors():
    """REFUSED_ROOTS = core.catalog_path's live data folder and the folder of its lock's folder (read from the SOURCE:
    tests/conftest.py points LOCK_PATH elsewhere for every test), and unit_id = tools/derive_statcan_tables.py's."""
    import ntpath
    src = open(os.path.join(ROOT, "core", "catalog_path.py"), encoding="utf-8").read()
    consts = {t.id: n.value.value for n in ast.parse(src).body if isinstance(n, ast.Assign)
              and isinstance(n.value, ast.Constant) for t in n.targets if isinstance(t, ast.Name)}
    assert LC.REFUSED_ROOTS == (ntpath.join(consts["LIVE_STORE_ROOT"], "data"),
                                ntpath.dirname(ntpath.dirname(consts["LOCK_PATH"]))), consts
    assert LC.unit_id("24100058") == L._tool().unit_id("24100058") == "statcan:24100058"


# --------------------------------------------------------------------------- #
# design review C7 - the statcan region's writers after T0, by behaviour
# --------------------------------------------------------------------------- #
def test_C7_repull_file_refuses_statcan_after_t0_and_deletes_nothing(t0, monkeypatch, capsys):
    import importlib.util
    spec = importlib.util.spec_from_file_location("_repull_for_b3", os.path.join(ROOT, "tools", "repull_file.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    monkeypatch.setattr(m, "runs_in_flight", lambda: pytest.fail("refused before any other check"))
    monkeypatch.setattr(sys, "argv", ["repull_file.py", "statcan", f"{PID}.parquet", "--apply", "--force-unsafe"])
    assert m.main() == 2
    assert "REFUSING - after T0 statcan's store belongs to its lane" in capsys.readouterr().out
    assert (t0 / f"{PID}.parquet").exists()


def test_C7_F6_before_t0_repull_file_statcan_keeps_its_old_path(world, monkeypatch, capsys):
    """CODE REVIEW F6: the refusal is AFTER T0 only - before it, statcan reaches the in-flight check as it always did."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("_repull_for_b3_pre", os.path.join(ROOT, "tools", "repull_file.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    class _Reached(Exception):
        pass

    def reached():
        raise _Reached()
    monkeypatch.setattr(m, "runs_in_flight", reached)
    monkeypatch.setattr(sys, "argv", ["repull_file.py", "statcan", f"{PID}.parquet"])
    assert not cutover.is_cut_over()
    with pytest.raises(_Reached):
        m.main()
    assert "REFUSING" not in capsys.readouterr().out
    assert (world / f"{PID}.parquet").exists()


# Every non-test file that can WRITE a store path and can reach statcan's region (it names statcan, or it takes a
# source as an argument), with what keeps it off the post-T0 lane's region. A NEW writer fails this test until it is
# classified here (R1282 rule (a): an inventory by behaviour, not by a hand-kept list of names).
#   lane       = a statcan lane itself
#   lock       = takes logs/statcan_writer.lock before it writes statcan (checked in the source)
#   refuses    = refuses after T0 (checked: its source asks core.cutover), or refuses statcan after T0
#   elsewhere  = writes another region only; the reason is the evidence
#   reserved   = an owner-reserved whole-source operation; after T0 a statcan run trips the lane's held-cube check
STATCAN_REGION_WRITERS = {
    "jobs/statcan_lane.py": ("lane", "the pre-T0 lane; refuses after T0"),
    "jobs/statcan_selfhost_lane.py": ("lane", "the post-T0 lane"),
    "core/derive_csv.py": ("lock", "hold_or_refuse('statcan_writer') for statcan"),
    "jobs/ingest_statcan.py": ("lock", "the bulk ingester takes the region lock"),
    "tools/derive_csv_bulk.py": ("lock", "hold_or_refuse('statcan_writer') for statcan"),
    "tools/derive_statcan_tables.py": ("lock", "hold_or_refuse('statcan_writer')"),
    "tools/statcan_new_cubes.py": ("lock", "takes the region lock; refuses after T0 (B15)"),
    "tools/statcan_lift_quarantine.py": ("refuses", "refuses after T0 (B15: no post-T0 quarantine lift)"),
    "tools/repull_file.py": ("refuses", "refuses statcan after T0 (C7)"),
    "tools/mirror_sync.py": ("refuses", "refuses after the flag (S5 only)"),
    "tools/audit_statcan_upstream.py": ("refuses", "a reader; its R2 path refuses after T0"),
    "tools/catalog_statcan_tables.py": ("refuses", "writes the catalogue, never the store; refuses its cloud steps after T0"),
    "tools/series_census.py": ("refuses", "writes _aqueduct/stats.json, never statcan's region"),
    "tools/refresh_served_facts.py": ("refuses", "writes site facts, never statcan's region"),
    "core/apply_stage0b.py": ("refuses", "a catalogue stage, never statcan's store"),
    "updater/orchestrate.py": ("refuses", "served_by: lane keeps its CSV phase and retry drain off statcan"),
    "catalog/gen_site.py": ("elsewhere", "writes catalog/site pages"),
    "tools/guard_heartbeat.py": ("elsewhere", "writes the beat key and its own status"),
    "updater/blob.py": ("elsewhere", "the chokepoint itself"),
    "updater/merge.py": ("elsewhere", "the chokepoint itself"),
    "updater/writer_lock.py": ("elsewhere", "writes lock files only"),
    "updater/strategies/fetchers/statcan.py": ("elsewhere", "the reporter writes nothing to the store"),
    "updater/strategies/fetchers/_giant.py": ("elsewhere", "giant flows of their own sources"),
    "updater/strategies/fetchers/abs.py": ("elsewhere", "abs's store"),
    "updater/strategies/fetchers/census.py": ("elsewhere", "census's store"),
    "updater/strategies/fetchers/istat.py": ("elsewhere", "istat's store"),
    "updater/strategies/fetchers/noaa.py": ("elsewhere", "noaa's store"),
    "tools/delist_source_rows.py": ("elsewhere", "catalogue and D1 rows only; touches no stored object (its header)"),
    "tools/retire_source.py": ("reserved", "owner-reserved archive-first retirement"),
}
_WRITES = re.compile(r"\b(merge_and_write|merge_and_write_bounded|write_table_atomic|commit_local_file|"
                     r"write_bytes_atomic|publish_file|delete_store_object|backup_store_object|put_atomic|put_file|"
                     r"put_gzip_file)\(|os\.replace\(|os\.remove\(|pq\.write_table\(|ParquetWriter\(|shutil\.copy|"
                     r"licence_targets")
_REACH = re.compile(r"\bstatcan\b|add_argument\(\"source\"")


def test_C7_every_writer_that_can_reach_the_statcan_region_is_classified():
    found = set()
    out = subprocess.run(["git", "ls-files", "*.py"], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    for rel in out.split():
        if rel.startswith("tests/"):
            continue
        try:
            src = open(os.path.join(ROOT, rel), encoding="utf-8", errors="replace").read()
        except OSError:
            continue
        if _REACH.search(src) and _WRITES.search(src):
            found.add(rel)
    assert found == set(STATCAN_REGION_WRITERS), (
        f"NEW: {sorted(found - set(STATCAN_REGION_WRITERS))}  GONE: {sorted(set(STATCAN_REGION_WRITERS) - found)}")
    for rel, (kind, _why) in STATCAN_REGION_WRITERS.items():
        src = open(os.path.join(ROOT, rel), encoding="utf-8", errors="replace").read()
        if kind == "lock":
            assert "statcan_writer" in src and re.search(r"(hold_or_refuse|hold|acquire)\(", src), rel
        elif kind == "refuses":
            assert re.search(r"is_cut_over\(\)|refuse_if_cut_over|_served_by_lane", src), rel
