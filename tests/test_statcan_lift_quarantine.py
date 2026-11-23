"""tools/statcan_lift_quarantine.py - lift the statcan lane's quarantine on the 44 NAMED cubes (classes A and B of the
2026-11-22 diagnosis, ledger R2025/R2026), and nothing else; every guard refuses before anything is written; the
receipt after the lift can FAIL. Design: F:/work/econ_finish/statcan_lift_pr/DESIGN.md (review R2037).

ISOLATION (design review F7). tools/pretest.py runs this suite in the production checkout. The autouse fixture
`iso` points every path the tool can write - the store dir, the lane state/progress, the local progress copy, the
writer-lock dir, the receipts dir, the code root, the lane module file - at a temporary tree, replaces the process
scan, and fails the test at teardown if the real lock file, the real receipts dir or the real local progress copy
changed, or if the real process table was read. The bucket is a fake client object; no test reaches the network."""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import io
import json
import os
import re
import sys

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")
pytest.importorskip("psutil")
from botocore.exceptions import ClientError  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.statcan_lane as lane  # noqa: E402
from core import cutover  # noqa: E402
from updater import blob, config, writer_lock  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402
# the lane's own test world and fake www150 (the end-to-end tests); _no_network is autouse here too
from tests.test_statcan_lane import _Bucket, _no_network, _stored, world  # noqa: E402,F401
from tests.test_statcan_strike_rule import Clock, StatCan, _t, clock, statcan  # noqa: E402,F401


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, *rel.split("/")))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


tool = _load("_statcan_lift_quarantine_under_test", "tools/statcan_lift_quarantine.py")
REAL_LANE_PROCESSES = tool.lane_processes
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 23, 8, 0, 0, tzinfo=UTC)
LANE_PID = 48712
TABLE2 = os.path.join(ROOT, "tests", "fixtures", "statcan_lift", "table2_pid_class_release.tsv")
KEY = "clean_full/statcan/_lane_state.json"
PKEY = "clean_full/statcan/_lane_progress.json"
OUTSIDE = "99999999"           # a 50th quarantined cube, outside LIFT: no apply may lift it


def _z(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ep(t):
    return t.timestamp()


class FakeR2:
    """The bucket behind updater.blob.R2Blob: get_object / put_object, what read_bytes and put_atomic use."""

    def __init__(self):
        self.objects, self.puts = {}, []
        self.put_error = None           # raised by put_object; `store_then_raise` stores first
        self.store_then_raise = False
        self.get_error = None
        self.after_put_get_error = None

    def get_object(self, Bucket, Key, **kw):
        assert Bucket == blob.R2_BUCKET, Bucket
        if self.get_error is not None:
            raise self.get_error
        if self.puts and self.after_put_get_error is not None:
            raise self.after_put_get_error
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                              "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        assert Bucket == blob.R2_BUCKET, Bucket
        self.puts.append(Key)
        if self.put_error is not None:
            if self.store_then_raise:
                self.objects[Key] = bytes(Body)
            raise self.put_error
        self.objects[Key] = bytes(Body)


def _real_receipts():
    """The real checkout's receipts dir (exists, names) - only this tool writes there; the lane never does."""
    p = os.path.join(ROOT, "logs", "statcan_lift_quarantine")
    return os.path.isdir(p), sorted(os.listdir(p)) if os.path.isdir(p) else []


def _under_real(path) -> bool:
    real = os.path.normcase(os.path.realpath(ROOT))
    got = os.path.normcase(os.path.realpath(str(path)))
    return got == real or got.startswith(real + os.sep)


@pytest.fixture(autouse=True)
def iso(tmp_path, monkeypatch):
    # SENTINELS ON THIS PROCESS'S WRITES, NOT ON THE REAL FILES' TIMES (verification review V1): the live lane
    # rewrites logs/statcan_lane.progress.json and creates logs/statcan_writer.lock in the production checkout while
    # tools/pretest.py runs this suite there, so a time compare would blame the test for the lane's own writes.
    real_acquire, real_write, real_local = writer_lock.acquire, blob.write_bytes_atomic, tool._write_local

    def acquire(name, what=""):
        assert not _under_real(writer_lock.lock_path(name)), f"a test took the REAL lock {writer_lock.lock_path(name)}"
        return real_acquire(name, what=what)

    def write_bytes_atomic(path, data):
        assert not _under_real(path), f"a test wrote the REAL store path {path}"
        return real_write(path, data)

    def write_local(path, data):
        assert not _under_real(path), f"a test wrote the REAL mirror {path}"
        return real_local(path, data)
    monkeypatch.setattr(writer_lock, "acquire", acquire)
    monkeypatch.setattr(blob, "write_bytes_atomic", write_bytes_atomic)
    monkeypatch.setattr(tool, "_write_local", write_local)
    receipts = _real_receipts()
    code_t = NOW - dt.timedelta(minutes=60)
    files = {}
    for rel, attr in ((("jobs", "statcan_lane.py"), lane), (("updater", "strategies", "fetchers", "statcan.py"), sc)):
        p = tmp_path.joinpath(*rel)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("# stand-in for the code's last write\n")
        os.utime(p, (_ep(code_t), _ep(code_t)))
        monkeypatch.setattr(attr, "__file__", str(p))
        files[rel[-1]] = p
    d = tmp_path / "data" / "clean_full" / "statcan"
    monkeypatch.setattr(tool, "ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "LIVE_ROOT", str(tmp_path))
    monkeypatch.setattr(tool, "RECEIPTS", str(tmp_path / "logs" / "statcan_lift_quarantine"))
    monkeypatch.setattr(sc, "OUT_DIR", str(d))
    monkeypatch.setattr(lane, "STATE", str(d / "_lane_state.json"))
    monkeypatch.setattr(lane, "PROGRESS", str(d / "_lane_progress.json"))
    monkeypatch.setattr(lane, "DEBT", str(d / "_catalogue_debt.json"))
    monkeypatch.setattr(lane, "LOCAL_PROGRESS", str(tmp_path / "logs" / "statcan_lane.progress.json"))
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_programdata" / "CUTOVER"))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")              # recorded, so pin_backend's write is undone
    monkeypatch.setattr(config, "BACKEND", config.BACKEND)
    monkeypatch.setattr(tool, "_now", lambda: NOW)
    import psutil

    def _real_table(*a, **k):
        raise AssertionError("a test read the REAL process table")
    monkeypatch.setattr(psutil, "process_iter", _real_table)
    procs = {"found": [{"pid": LANE_PID, "cmdline": [r"C:\py\python.exe", "-u", "jobs/statcan_lane.py"],
                        "cwd": str(tmp_path), "create_time": _ep(NOW - dt.timedelta(minutes=30)) + 0.37}],
             "unreadable": []}
    monkeypatch.setattr(tool, "lane_processes", lambda: (procs["found"], procs["unreadable"]))
    r2 = FakeR2()
    monkeypatch.setattr(blob.R2Blob, "client", property(lambda self: r2))
    yield {"tmp": tmp_path, "d": d, "r2": r2, "procs": procs, "files": files, "code_t": code_t}
    for name, path in (("lane.STATE", lane.STATE), ("lane.PROGRESS", lane.PROGRESS), ("sc.OUT_DIR", sc.OUT_DIR),
                       ("lane.LOCAL_PROGRESS", lane.LOCAL_PROGRESS), ("writer_lock.LOCK_DIR", writer_lock.LOCK_DIR),
                       ("tool.RECEIPTS", tool.RECEIPTS)):
        assert not _under_real(path), f"{name} points at the real checkout: {path}"
    assert _real_receipts() == receipts, "a test wrote the real receipts dir"


# --------------------------------------------------------------------------- #
# a production-shaped lane state (the shape measured on the 2026-11-22 snapshot)
# --------------------------------------------------------------------------- #
def _cube(pid):
    cls, rel = tool.LIFT[pid]
    err = (tool._SIG_A.format(pid=pid) if cls == "A" else
           tool._SIG_B.format(pid=pid) + "1,090,852 rows parsed vs nbDatapointsCube 1,093,402; rows skipped by the "
                                          "parser {}")
    return {"bytes": 60103, "est_min": 0.0, "fail_release": rel, "fails": 3, "last_error": err,
            "max_obs": "2026-08-01", "merge_in_flight": rel, "merged": "2026-10-15T12:30", "quarantined": True,
            "release": rel, "retry_after": None, "rows": 12117, "served": "2026-10-15T12:30", "transient_fails": 0}


def _doc(edit=None):
    cubes = {p: _cube(p) for p in tool.LIFT}
    for p, k in tool.EXCLUDED.items():
        cubes[p] = {"fail_release": "2026-11-05T13:30", "fails": 3, "quarantined": True, "release": "2026-11-05T13:30",
                    "merge_in_flight": "2026-11-05T13:30",
                    "last_error": f"whole-table fetch refused: statcan {p}: incomplete against StatCan's own counts - "}
    cubes[OUTSIDE] = dict(_cube("10100111"), last_error=tool._SIG_A.format(pid=OUTSIDE))
    cubes["17100061"] = {"bytes": 6216, "fails": 0, "last_error": None, "merged": "2026-11-18T18:50",
                         "release": "2026-11-18T18:50", "retry_after": None, "served": "2026-11-18T18:50",
                         "transient_fails": 0}
    st = {"cubes": cubes, "floor": "2026-07-29", "last_enum_utc": _z(NOW - dt.timedelta(minutes=20)),
          "last_progress_utc": _z(NOW - dt.timedelta(hours=3)), "new_cubes": {}, "parsed_rows": {}, "version": 1,
          "withdrawn_upstream": {}}
    if edit:
        edit(st)
    return st


def _progress(**kw):
    p = {"counters": {}, "current": None, "beat_utc": _z(NOW - dt.timedelta(minutes=2)), "owed": {},
         "pid": LANE_PID, "started_utc": _z(NOW - dt.timedelta(minutes=2)), "state": "idle"}
    p.update(kw)
    return p


def _seed(iso, edit=None, progress=None, raw=None):
    """The document of record on the fake R2 AND its local mirror (as the lane's save_state leaves them)."""
    b = raw if raw is not None else json.dumps(_doc(edit), sort_keys=True).encode("utf-8")
    iso["d"].mkdir(parents=True, exist_ok=True)
    iso["r2"].objects[KEY] = b
    (iso["d"] / "_lane_state.json").write_bytes(b)
    pb = json.dumps(progress if progress is not None else _progress(), sort_keys=True).encode("utf-8")
    iso["r2"].objects[PKEY] = pb
    return b


def _files(root):
    return sorted((str(p.relative_to(root)), p.read_bytes()) for p in root.rglob("*") if p.is_file())


def _run(*argv):
    return tool.main(list(argv))


def _plan(capsys, *argv):
    rc = _run(*argv)
    out = capsys.readouterr().out
    m = re.search(r"READY: apply with --expect-before-sha ([0-9a-f]{64}) --expect-after-sha ([0-9a-f]{64})", out)
    return rc, out, (m.group(1), m.group(2)) if m else (None, None)


def _ns(**kw):
    base = {"pids": None, "from_table2": TABLE2, "apply": True, "expect_before_sha": None, "expect_after_sha": None,
            "check": None, "state_file": None, "progress_file": None}
    base.update(kw)
    return argparse.Namespace(**base)


def _unchanged(iso, before_objects, before_files):
    assert iso["r2"].objects == before_objects, "R2 changed"
    assert not iso["r2"].puts, f"something was PUT: {iso['r2'].puts}"
    assert _files(iso["tmp"]) == before_files, "a local file changed"
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME)), "the lock was left behind"


# --------------------------------------------------------------------------- #
# the pinned cubes
# --------------------------------------------------------------------------- #
def test_the_pinned_cubes_are_the_diagnosis_classes_a_and_b_and_nothing_else():
    assert sum(1 for c, _r in tool.LIFT.values() if c == "A") == 35
    assert sum(1 for c, _r in tool.LIFT.values() if c == "B") == 9
    assert sorted(tool.EXCLUDED.items()) == [("10100145", "C"), ("18100212", "C"), ("18100275", "C"),
                                             ("36100487", "D"), ("36100488", "D")]
    assert not set(tool.LIFT) & set(tool.EXCLUDED)
    assert not set(tool.LIFT) & (lane.HISTORY_FLOOR_PIDS | lane.RECONCILE_PIDS)
    ab, cd = tool.read_table2(TABLE2)            # the fixture is the diagnosis table's pid/class2/release columns
    assert ab == tool.LIFT and cd == tool.EXCLUDED and tool.table2_problems(ab, cd) == []
    assert tool.LIFT_FIELDS == ("quarantined", "fails", "fail_release", "first_strike_utc", "retry_after")


def _table(tmp_path, edit):
    lines = open(TABLE2, encoding="utf-8").read().splitlines()
    lines = edit(lines)
    p = tmp_path / "table2.tsv"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


@pytest.mark.parametrize("name,edit,want", [
    ("extra A row", lambda l: l + ["12345678\tACCESS_REFUSED\t2026-11-18T13:30"], "C1 12345678"),
    ("missing A row", lambda l: [x for x in l if not x.startswith("14100004")], "only pinned ['14100004']"),
    ("changed release", lambda l: [x.replace("2026-10-30T12:30", "2026-10-31T12:30") for x in l],
     "table2 row 36100434"),
    ("C moved to B", lambda l: [x.replace("10100145\tPERSISTENT_OFFSET", "10100145\tSTALE_COPY") for x in l],
     "C2 10100145"),
    ("missing D row", lambda l: [x for x in l if not x.startswith("36100488")], "table2 classes C+D"),
    ("D row read as C", lambda l: [x.replace("36100488\tSERIES_DROPPED", "36100488\tPERSISTENT_OFFSET") for x in l],
     "table2 classes C+D"),
    ("duplicate row", lambda l: l + [l[1]], "appears twice"),
    ("unknown class", lambda l: [x.replace("SERIES_DROPPED", "OTHER") for x in l], "unknown class"),
    ("no release column", lambda l: [x.rsplit("\t", 1)[0] for x in l], "needs the columns"),
])
def test_a_table2_that_does_not_reproduce_the_pinned_list_refuses(iso, capsys, name, edit, want):
    _seed(iso)
    table = _table(iso["tmp"], edit)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc, out, shas = _plan(capsys, "--from-table2", table)
    assert rc == 1 and "NOT READY" in out and want in out, (name, out[-2000:])
    _unchanged(iso, objs, files)
    rc = _run("--from-table2", table, "--apply", "--expect-before-sha", "a" * 64, "--expect-after-sha", "b" * 64)
    assert rc == 1
    _unchanged(iso, objs, files)


@pytest.mark.parametrize("argv", [[], ["--pids", ""], ["--pids", "10100111,10100111"],
                                  ["--pids", "10100111", "--from-table2", TABLE2],
                                  ["--pids", "10100111", "--apply", "--expect-before-sha", "a" * 64],
                                  ["--pids", "10100111", "--apply", "--expect-after-sha", "a" * 64],
                                  ["--check", "x.json", "--pids", "10100111"], ["--no-such-flag"]])
def test_usage_errors_exit_2_and_touch_nothing(iso, capsys, argv):
    _seed(iso)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    assert _run(*argv) == 2
    _unchanged(iso, objs, files)


# --------------------------------------------------------------------------- #
# plan
# --------------------------------------------------------------------------- #
def test_plan_prints_every_cubes_before_and_after_and_the_sha_pair_and_writes_nothing(iso, capsys):
    raw = _seed(iso)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc, out, (before, after) = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0, out[-3000:]
    assert before == tool.sha256(raw)
    want_after, _ch = tool.lift(json.loads(raw), sorted(tool.LIFT))
    assert after == tool.sha256(tool.after_bytes(want_after))
    for p in tool.LIFT:
        assert re.search(rf"^  {p} [AB] release .*: \{{.*\"quarantined\": true.*\}} -> \{{\"fails\": 0\}}$", out, re.M), p
    assert "table2" in out and "classes A+B 44 rows, C+D 5 rows" in out
    assert "summarise() quarantined 50 -> 6" in out          # 44 lifted; the 5 C/D and the 50th stay
    assert "44 of 44 named cube(s) due" in out
    assert "(plan takes no lock)" in out and "REFUSE" not in out
    _unchanged(iso, objs, files)


def test_an_offline_plan_reads_the_copies_and_never_the_store(iso, capsys, monkeypatch):
    raw = _seed(iso)
    sf, pf = iso["tmp"] / "copy_state.json", iso["tmp"] / "copy_progress.json"
    sf.write_bytes(raw)
    pf.write_bytes(iso["r2"].objects[PKEY])
    iso["r2"].get_error = AssertionError("the store was read")
    monkeypatch.setattr(tool, "LIVE_ROOT", str(iso["tmp"] / "elsewhere"))   # a plan from a clone: R3 reported
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2, "--state-file", str(sf), "--progress-file", str(pf))
    assert rc == 1 and "REFUSE R3" in out and f"BEFORE sha256 {tool.sha256(raw)}" in out
    assert "REFUSE C" not in out and "REFUSE R5" not in out and "REFUSE R6" not in out
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2, "--state-file", str(sf))      # no progress copy: not read
    assert rc == 1 and "R6 the lane progress record is absent" in out


# --------------------------------------------------------------------------- #
# apply
# --------------------------------------------------------------------------- #
def _apply_after_plan(capsys, *sel):
    rc, out, (b, a) = _plan(capsys, *sel)
    assert rc == 0, out[-3000:]
    rc = _run(*sel, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    return rc, capsys.readouterr().out, b, a


def test_apply_lifts_exactly_the_planned_fields_and_leaves_a_backup_and_a_receipt(iso, capsys):
    raw = _seed(iso, edit=lambda st: st["cubes"]["34100162"].update(retry_after="2026-11-30T00:00:00Z"))
    rc, out, b, a = _apply_after_plan(capsys, "--from-table2", TABLE2)
    assert rc == 0, out[-3000:]
    new = iso["r2"].objects[KEY]
    assert tool.sha256(new) == a and (iso["d"] / "_lane_state.json").read_bytes() == new
    st0, st1 = json.loads(raw), json.loads(new)
    for p, c in st1["cubes"].items():
        if p in tool.LIFT:
            want = {k: v for k, v in st0["cubes"][p].items() if k not in tool.LIFT_FIELDS}
            want["fails"] = 0
            assert c == want, p                       # merge_in_flight, last_error, merged ... unchanged
        else:
            assert c == st0["cubes"][p], p            # the C/D cubes, the 50th quarantined cube, the merged one
    assert st1["cubes"][OUTSIDE]["quarantined"] is True
    assert {k: v for k, v in st1.items() if k != "cubes"} == {k: v for k, v in st0.items() if k != "cubes"}
    assert iso["r2"].puts == [KEY]
    rec_path = re.search(r"receipt (\S+_receipt\.json)", out).group(1)
    rec = json.load(open(rec_path, encoding="utf-8"))
    assert rec["status"] == "LIFTED" and all(rec["checks"].values()) and len(rec["checks"]) == 5
    assert rec["before_sha256"] == b and rec["after_sha256"] == a and set(rec["cubes"]) == set(tool.LIFT)
    assert rec["cubes"]["34100162"]["before"]["retry_after"] == "2026-11-30T00:00:00Z"   # the planted retry_after
    assert open(rec["backup"], "rb").read() == raw and rec["utc_lift"] == _z(NOW)
    assert rec["tool"] == "tools/statcan_lift_quarantine.py"         # never os.path.relpath (two drives raise)
    assert rec["tool_sha256"] == tool.sha256(open(tool.__file__, "rb").read())
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


def test_the_bytes_put_are_the_plans_and_what_the_lanes_own_save_state_writes(iso, capsys, monkeypatch):
    _seed(iso)
    rc, out, b, a = _apply_after_plan(capsys, "--pids", "10100111,36100434")
    assert rc == 0, out[-3000:]
    put = iso["r2"].objects[KEY]
    seen = {}
    monkeypatch.setattr(blob, "write_bytes_atomic", lambda path, data: seen.setdefault("b", data))
    lane.save_state(json.loads(put))
    assert seen["b"] == put and tool.sha256(put) == a
    st = json.loads(put)
    assert st["cubes"]["10100111"]["fails"] == 0 and "quarantined" not in st["cubes"]["10100111"]
    assert st["cubes"]["14100004"]["quarantined"] is True            # not named: not lifted


def _pids(*p):
    return ("--pids", ",".join(p))


@pytest.mark.parametrize("sel,want", [
    (_pids(OUTSIDE), f"C1 {OUTSIDE} is not one of the 44"),
    (_pids("10100145"), "C2 10100145 is class C"),
    (_pids("36100487"), "C2 36100487 is class D"),
    (_pids("10100111", OUTSIDE), f"C1 {OUTSIDE}"),            # a good and a bad pid: the whole run refuses
])
def test_a_cube_outside_the_44_refuses_the_whole_run(iso, capsys, sel, want):
    _seed(iso)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc, out, _s = _plan(capsys, *sel)
    assert rc == 1 and want in out
    rc = _run(*sel, "--apply", "--expect-before-sha", "a" * 64, "--expect-after-sha", "b" * 64)
    out = capsys.readouterr().out
    assert rc == 1 and want in out and "Nothing was written" in out
    _unchanged(iso, objs, files)


def _cube_edit(pid, **kw):
    def edit(st):
        c = st["cubes"][pid]
        for k, v in kw.items():
            if v is _DEL:
                c.pop(k, None)
            else:
                c[k] = v
    return edit


_DEL = object()


@pytest.mark.parametrize("edit,want", [
    (lambda st: st["cubes"].pop("10100111"), "C3 10100111"),
    (_cube_edit("10100111", quarantined=_DEL), "C4 10100111"),                    # negative control
    (_cube_edit("10100111", quarantined=False), "C4 10100111"),
    (_cube_edit("10100111", release="2026-12-01T13:30"), "C5 10100111"),         # re-released
    (_cube_edit("10100111", fail_release="2026-10-15T12:30"), "C5 10100111"),
    (_cube_edit("10100111", fails=2), "C6 10100111"),
    (_cube_edit("10100111", fails=4), "C6 10100111"),
    (_cube_edit("10100111", first_strike_utc="2026-11-23T01:00:00Z"), "C7 10100111"),
    (_cube_edit("10100111", refused_http=403), "C8 10100111"),
    (_cube_edit("10100111", history_hold={"release": "2026-11-18T13:30"}), "C9 10100111"),
    (_cube_edit("10100111", merge_in_flight="2026-10-15T12:30"), "C10 10100111"),
    (_cube_edit("10100111", merged="2026-11-18T13:30"), "C11 10100111"),
    (_cube_edit("10100111", last_error=tool._SIG_B.format(pid="10100111")), "C12 10100111"),    # the other class
    (_cube_edit("10100111", last_error=tool._SIG_A.format(pid="10100141")), "C12 10100111"),    # another cube's zip
    (_cube_edit("10100111", last_error=tool._SIG_A.format(pid="10100111") + "0"), "C12 10100111"),  # HTTP 4030
    (_cube_edit("16100047", last_error=tool._SIG_A.format(pid="16100047")), "C12 16100047"),
    (_cube_edit("16100047", last_error=tool._SIG_B.format(pid="18100001")), "C12 16100047"),
    (_cube_edit("10100111", transient_fails=6), "C13 10100111"),
    (lambda st: st.update(enum_fail={"since": "2026-11-23T07:00:00Z", "count": 2}), "R7 the lane state carries enum_fail"),
    (lambda st: st.update(breaker={"status": 403, "until": "2026-11-23T08:10:00Z"}), "R7 the lane state carries a circuit"),
])
def test_every_cube_and_document_refusal_writes_nothing(iso, capsys, edit, want):
    _seed(iso, edit=edit)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and want in out, out[-2500:]
    raw = iso["r2"].objects[KEY]
    after, _c = tool.lift(json.loads(raw), sorted(p for p in tool.LIFT if p in json.loads(raw)["cubes"]))
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", tool.sha256(raw),
              "--expect-after-sha", tool.sha256(tool.after_bytes(after)))
    out = capsys.readouterr().out
    assert rc == 1 and want in out and "Nothing was written" in out
    _unchanged(iso, objs, files)


@pytest.mark.parametrize("edit", [
    _cube_edit("10100111", merge_in_flight=_DEL),               # absent passes
    _cube_edit("10100111", transient_fails=5),                  # under TRANSIENT_RED_AFTER passes
    _cube_edit("36100400", merged=_DEL, served=_DEL),           # a pre-lane copy (never merged by the lane) passes
])
def test_must_not_refuse(iso, capsys, edit):
    _seed(iso, edit=edit)
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0, out[-2500:]


@pytest.mark.parametrize("raw,want", [
    (b"{not json", "R8 the lane state does not parse"),
    (json.dumps({"version": 1}, sort_keys=True).encode(), "R8 the lane state has no `cubes` dict"),
    (json.dumps({"cubes": [1, 2]}, sort_keys=True).encode(), "R8 the lane state has no `cubes` dict"),
    (json.dumps(_doc(), sort_keys=True, indent=1).encode(), "R8 the lane state is not in the lane's own"),
])
def test_a_document_the_lane_did_not_write_refuses(iso, capsys, raw, want):
    _seed(iso, raw=raw)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and want in out
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", tool.sha256(raw), "--expect-after-sha", "0" * 64)
    assert rc == 1
    _unchanged(iso, objs, files)


def test_a_document_changed_between_plan_and_apply_refuses(iso, capsys):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0
    st = json.loads(iso["r2"].objects[KEY])                       # the lane saved again in between
    st["last_progress_utc"] = _z(NOW - dt.timedelta(minutes=1))
    iso["r2"].objects[KEY] = json.dumps(st, sort_keys=True).encode()
    (iso["d"] / "_lane_state.json").write_bytes(iso["r2"].objects[KEY])
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "R9 the document is" in out and "it changed since the plan" in out
    _unchanged(iso, objs, files)


def test_an_after_sha_that_is_not_the_lifts_refuses(iso, capsys):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--pids", "10100111,10100141")
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc = _run("--pids", "10100111", "--apply", "--expect-before-sha", b, "--expect-after-sha", a)   # other selection
    out = capsys.readouterr().out
    assert rc == 1 and "R9 the lift gives" in out
    _unchanged(iso, objs, files)


def test_apply_refuses_with_an_offline_copy(iso, capsys):
    raw = _seed(iso)
    sf = iso["tmp"] / "copy.json"
    sf.write_bytes(raw)
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc = _run("--from-table2", TABLE2, "--apply", "--state-file", str(sf), "--expect-before-sha", "a" * 64,
              "--expect-after-sha", "b" * 64)
    assert rc == 1 and "REFUSE R10" in capsys.readouterr().out
    _unchanged(iso, objs, files)
    rc = _run("--from-table2", TABLE2, "--apply", "--progress-file", str(sf), "--expect-before-sha", "a" * 64,
              "--expect-after-sha", "b" * 64)                    # a progress copy alone refuses too
    assert rc == 1 and "REFUSE R10" in capsys.readouterr().out
    _unchanged(iso, objs, files)


# --- R3: the live checkout only, by exact path ------------------------------------------------------------------
@pytest.mark.parametrize("which", ["root", "lane_file", "out_dir", "state", "progress", "lock_dir", "receipts",
                                   "nested_worktree"])
def test_apply_from_anything_but_the_live_checkout_refuses(iso, capsys, monkeypatch, which):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0
    other = iso["tmp"] / ".claude" / "worktrees" / "x"
    if which == "root":
        monkeypatch.setattr(tool, "ROOT", str(other))
    elif which == "lane_file":
        monkeypatch.setattr(lane, "__file__", str(other / "jobs" / "statcan_lane.py"))
    elif which == "out_dir":
        monkeypatch.setattr(sc, "OUT_DIR", str(other / "data" / "clean_full" / "statcan"))
    elif which == "state":
        monkeypatch.setattr(lane, "STATE", str(other / "data" / "clean_full" / "statcan" / "_lane_state.json"))
    elif which == "progress":
        monkeypatch.setattr(lane, "PROGRESS", str(other / "data" / "clean_full" / "statcan" / "_lane_progress.json"))
    elif which == "lock_dir":
        monkeypatch.setattr(writer_lock, "LOCK_DIR", str(other / "logs"))
    elif which == "receipts":
        monkeypatch.setattr(tool, "RECEIPTS", str(other / "logs" / "statcan_lift_quarantine"))
    else:                                           # a worktree INSIDE the live checkout: under it, not equal
        monkeypatch.setattr(tool, "ROOT", str(other))
        monkeypatch.setattr(writer_lock, "LOCK_DIR", str(other / "logs"))
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "REFUSE R3" in out and "--apply writes only the live checkout" in out
    assert iso["r2"].objects == objs and not iso["r2"].puts and _files(iso["tmp"]) == files


def test_the_live_paths_pass_r3_also_when_spelled_differently(iso, monkeypatch):
    t = iso["tmp"]
    monkeypatch.setattr(tool, "LIVE_ROOT", str(t / "data" / ".."))          # realpath resolves it
    assert tool.live_problems() == []


# --- R4: the lock --------------------------------------------------------------------------------------------------
def test_apply_refuses_while_another_writer_holds_the_lock(iso, capsys, monkeypatch):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--from-table2", TABLE2)
    monkeypatch.setattr(writer_lock, "acquire", lambda *x, **k: False)
    monkeypatch.setattr(writer_lock, "owner", lambda name: {"pid": LANE_PID, "what": "jobs/statcan_lane.py"})
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "REFUSE R4" in out and "jobs/statcan_lane.py" in out
    assert iso["r2"].objects == objs and not iso["r2"].puts and _files(iso["tmp"]) == files


def test_an_unjudgeable_lock_file_refuses_and_is_left_alone(iso, capsys):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--from-table2", TABLE2)
    lock = writer_lock.lock_path(lane.LOCK_NAME)
    os.makedirs(os.path.dirname(lock), exist_ok=True)
    open(lock, "w").write("garbage")
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "REFUSE R4" in out and "unreadable lock file" in out
    assert open(lock).read() == "garbage" and not iso["r2"].puts


def test_the_lock_is_released_on_every_refusal_under_it(iso, capsys):
    _seed(iso, edit=_cube_edit("10100111", fails=2))
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", "a" * 64, "--expect-after-sha", "b" * 64)
    assert rc == 1 and not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


# --- R5 / R6: the lane runs the new code and finished an iteration that enumerated ------------------------------
def _proc(iso, minutes_before_now=30.0, pid=LANE_PID, cmd=None, cwd=None):
    return {"pid": pid, "cmdline": cmd or [r"C:\py\python.exe", "-u", "jobs/statcan_lane.py"],
            "cwd": str(cwd or iso["tmp"]), "create_time": _ep(NOW - dt.timedelta(minutes=minutes_before_now)) + 0.37}


@pytest.mark.parametrize("case,want", [
    ("none", "R5 no live lane process"),
    ("older than the code", "before the last write of the strike-rule code"),
    ("only statcan.py newer", "before the last write of the strike-rule code"),
    ("an old and a new one", "before the last write of the strike-rule code"),
    ("another checkout", "does not run this checkout's lane module"),
    ("unreadable", "R5 a python process cannot be judged"),
    ("-c form", "does not run this checkout's lane module"),
])
def test_r5_refuses_unless_every_lane_process_runs_the_new_code(iso, capsys, case, want):
    _seed(iso)
    pr = iso["procs"]
    if case == "none":
        pr["found"] = []
    elif case == "older than the code":
        pr["found"] = [_proc(iso, minutes_before_now=90)]
    elif case == "only statcan.py newer":
        t = NOW - dt.timedelta(minutes=10)                            # statcan.py written after the process start
        os.utime(iso["files"]["statcan.py"], (_ep(t), _ep(t)))
    elif case == "an old and a new one":
        pr["found"] = [_proc(iso), _proc(iso, minutes_before_now=600, pid=777)]
    elif case == "another checkout":
        pr["found"] = [_proc(iso, cwd=iso["tmp"] / "other_clone")]
    elif case == "unreadable":
        pr["unreadable"] = ["pid 901 (python.exe): AccessDenied: denied"]
    else:
        pr["found"] = [_proc(iso, cmd=["python", "-c", "import jobs.statcan_lane"])]
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and want in out, out[-2000:]


@pytest.mark.parametrize("cmd", [["python", "-u", "jobs/statcan_lane.py"], ["python", "-m", "jobs.statcan_lane"],
                                 ["python", "-X", "utf8", "-u", "ABS"]])
def test_r5_passes_the_launch_forms_of_the_live_lane(iso, capsys, cmd):
    _seed(iso)
    cmd = [str(iso["files"]["statcan_lane.py"]) if c == "ABS" else c for c in cmd]
    iso["procs"]["found"] = [_proc(iso, cmd=cmd)]
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0, out[-2000:]


@pytest.mark.parametrize("progress,edit,want", [
    (_progress(pid=999), None, "R6 the progress record's pid 999 is not a live lane process"),
    (_progress(beat_utc=_z(NOW - dt.timedelta(minutes=31))), None, "R6 no beat from pid"),
    (_progress(beat_utc=_z(NOW - dt.timedelta(minutes=16))), None, "R6 the last beat"),
    (_progress(beat_utc=_z(NOW + dt.timedelta(minutes=5))), None, "R6 the last beat"),
    (_progress(state="working"), None, "R6 the lane is not idle"),
    (_progress(state="enumeration_failing"), None, "R6 the lane is not idle"),
    (_progress(current={"pid": "24100058", "phase": "merge"}), None, "R6 the lane is not idle"),
    (_progress(), lambda st: st.update(last_enum_utc=_z(NOW - dt.timedelta(minutes=45))), "R6 no enumeration since"),
    (_progress(), lambda st: st.pop("last_enum_utc"), "R6 no enumeration since"),
])
def test_r6_refuses_unless_the_new_process_finished_an_iteration_that_enumerated(iso, capsys, progress, edit, want):
    _seed(iso, edit=edit, progress=progress)
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and want in out, out[-2000:]


def test_r6_passes_a_beat_14_min_old_and_an_enumeration_in_the_processs_first_second(iso, capsys):
    start = NOW - dt.timedelta(minutes=30)                     # create_time is start + 0.37 s; stamps are whole seconds
    _seed(iso, edit=lambda st: st.update(last_enum_utc=_z(start)),
          progress=_progress(beat_utc=_z(NOW - dt.timedelta(minutes=14))))
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0, out[-2000:]


def test_r6_refuses_an_absent_progress_record(iso, capsys):
    _seed(iso)
    del iso["r2"].objects[PKEY]
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and "R6 the lane progress record is absent" in out


# --- the process scan itself (a fake psutil table) ---------------------------------------------------------------
class _P:
    def __init__(self, pid, name, cmd=None, cwd="E:\\x", ct=1.0, deny=None):
        self.pid, self._n, self._c, self._w, self._t, self._deny = pid, name, cmd or [], cwd, ct, deny

    def _maybe(self, what):
        import psutil
        if self._deny == what:
            raise psutil.AccessDenied(self.pid)

    def name(self):
        self._maybe("name")
        return self._n

    def cmdline(self):
        self._maybe("cmdline")
        return self._c

    def cwd(self):
        self._maybe("cwd")
        return self._w

    def create_time(self):
        self._maybe("create_time")
        return self._t


def test_the_scan_finds_every_statcan_lane_form_and_fails_closed_on_what_it_cannot_read(monkeypatch):
    import psutil
    table = [_P(1, "python.exe", ["python", "-u", "jobs/statcan_lane.py"]),
             _P(2, "python.exe", ["python", "-m", "jobs.statcan_lane"]),
             _P(3, "python.exe", ["python", "-m", "pytest", "tests/test_statcan_lane.py"]),
             _P(4, "python.exe", ["python", "tools/other.py"]),
             _P(5, "notepad.exe", ["notepad", "statcan_lane.py"]),
             _P(6, "python.exe", deny="cmdline"), _P(7, "pythonw.exe", deny="cwd"), _P(8, "py.exe", deny="create_time"),
             _P(9, "x", deny="name")]
    monkeypatch.setattr(psutil, "process_iter", lambda *a, **k: iter(table))
    found, unreadable = REAL_LANE_PROCESSES()
    assert [p["pid"] for p in found] == [1, 2, 3]
    assert [re.match(r"pid (\d+)", u).group(1) for u in unreadable] == ["6", "7", "8", "9"]


# --- R1 / R2 -------------------------------------------------------------------------------------------------------
def test_refused_after_t0(iso, capsys):
    _seed(iso)
    flag = iso["tmp"] / "no_programdata" / "CUTOVER"
    flag.parent.mkdir()
    flag.write_text("")
    objs, files = dict(iso["r2"].objects), _files(iso["tmp"])
    with pytest.raises(cutover.CutoverRefused):
        _run("--from-table2", TABLE2)
    with pytest.raises(cutover.CutoverRefused):
        _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", "a" * 64, "--expect-after-sha", "b" * 64)
    _unchanged(iso, objs, files)


def test_refused_on_another_backend(iso, capsys, monkeypatch):
    _seed(iso)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    with pytest.raises(SystemExit, match="AQUEDUCT_BACKEND must be r2"):
        _run("--from-table2", TABLE2)


# --- the write's failure paths -------------------------------------------------------------------------------------
def _go(iso, capsys, **ns):
    _seed(iso)
    rc, out, (b, a) = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 0
    return b, a


def test_a_failed_put_leaves_the_before_document_on_r2_and_puts_the_mirror_back(iso, capsys):
    b, a = _go(iso, capsys)
    raw = iso["r2"].objects[KEY]
    iso["r2"].put_error = RuntimeError("PUT refused")
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "R2 still holds the before document" in out and "(verified)" in out
    assert iso["r2"].objects[KEY] == raw and (iso["d"] / "_lane_state.json").read_bytes() == raw
    rec = json.load(open(re.search(r"Receipt (\S+_receipt\.json)", out).group(1), encoding="utf-8"))
    assert rec["status"] == "FAILED_NOTHING_CHANGED"
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


def test_a_mirror_replace_that_keeps_failing_leaves_r2_untouched(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    raw = iso["r2"].objects[KEY]
    real_replace = os.replace

    def deny(src, dst):
        if str(dst).endswith("_lane_state.json"):
            raise PermissionError("held open")
        return real_replace(src, dst)
    monkeypatch.setattr(blob.os, "replace", deny)
    monkeypatch.setattr(tool.os, "replace", deny)
    monkeypatch.setattr(blob.time, "sleep", lambda s: None)
    monkeypatch.setattr(tool.time, "sleep", lambda s: None)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and not iso["r2"].puts and iso["r2"].objects[KEY] == raw
    assert (iso["d"] / "_lane_state.json").read_bytes() == raw


def test_a_put_that_raises_after_storing_is_verified_and_counts(iso, capsys):
    b, a = _go(iso, capsys)
    iso["r2"].put_error, iso["r2"].store_then_raise = RuntimeError("timeout after the write"), True
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 0 and "R2 holds the after document although the write raised" in out and "LIFTED" in out
    assert tool.sha256(iso["r2"].objects[KEY]) == a


def test_r2_holding_a_third_document_after_a_failed_put_is_loud(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    r2 = iso["r2"]

    def put(Bucket, Key, Body, **kw):
        r2.puts.append(Key)
        r2.objects[Key] = b'{"cubes": {}}'
        raise RuntimeError("someone else wrote")
    monkeypatch.setattr(r2, "put_object", put)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "R2 holds a THIRD document" in out


def test_r2_that_cannot_be_read_back_after_a_failed_put_is_unknown(iso, capsys):
    b, a = _go(iso, capsys)
    iso["r2"].put_error = RuntimeError("PUT refused")
    iso["r2"].after_put_get_error = RuntimeError("no route")
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "UNKNOWN: R2 cannot be read back" in out


def test_a_backup_that_does_not_read_back_refuses_before_the_write(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    real = tool._read_file
    monkeypatch.setattr(tool, "_read_file", lambda p: b"torn" if "_before_" in str(p) else real(p))
    raw = iso["r2"].objects[KEY]
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "does not read back as written" in out and not iso["r2"].puts and iso["r2"].objects[KEY] == raw


def test_the_write_happens_while_this_process_holds_the_lanes_lock(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    real, seen = blob.write_bytes_atomic, []

    def write(path, data):
        seen.append((writer_lock.owner(lane.LOCK_NAME) or {}).get("pid"))
        return real(path, data)
    monkeypatch.setattr(blob, "write_bytes_atomic", write)
    assert _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a) == 0
    assert seen == [os.getpid()]
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


def test_a_read_back_that_is_not_the_after_document_is_not_verified(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    r2 = iso["r2"]
    real_put = r2.put_object

    def put(Bucket, Key, Body, **kw):
        real_put(Bucket, Key, Body, **kw)
        r2.objects[Key] = bytes(Body).replace(b'"fails": 0', b'"fails": 9', 1)      # the store holds other bytes
    monkeypatch.setattr(r2, "put_object", put)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "check r2_read_back_is_after: FAIL" in out and "WRITTEN BUT NOT VERIFIED" in out


def test_a_lift_that_changes_anything_but_the_planned_fields_is_refused_by_the_structural_diff(iso, capsys,
                                                                                               monkeypatch):
    _seed(iso)
    real = tool.lift

    def wider(st, pids):
        after, ch = real(st, pids)
        after["cubes"]["17100061"]["fails"] = 1                     # one field of an unnamed cube
        return after, ch
    monkeypatch.setattr(tool, "lift", wider)
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and "the structural diff" in out and "is not the planned lift" in out


def test_the_lock_hold_is_bounded_before_the_write(iso, capsys):
    b, a = _go(iso, capsys)
    ticks = iter([0.0] + [tool.LOCK_HOLD_MAX_S + 1.0] * 10)
    rc = tool.apply(_ns(expect_before_sha=b, expect_after_sha=a), sorted(tool.LIFT), clock=lambda: next(ticks))
    out = capsys.readouterr().out
    assert rc == 1 and "the write is not started" in out and not iso["r2"].puts
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


# --------------------------------------------------------------------------- #
# the receipt after the lift: --check can FAIL
# --------------------------------------------------------------------------- #
LIFTED = NOW
REL_A = tool._A_RELEASE


def _h(h):
    return _z(LIFTED + dt.timedelta(hours=h))


@pytest.mark.parametrize("cube,now_h,want", [
    (None, 1, ("MISSING", "fail")),
    ({"release": REL_A, "merged": REL_A, "served": REL_A, "not_held": True}, 1, ("NOT_HELD", "fail")),   # before MERGED
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 1, "retry_after": _h(1),
      "last_error": "the ingest record (x.done) says it is held; the store does not have it - a store fault (R753)"},
     1, ("NOT_HELD", "fail")),
    ({"release": REL_A, "quarantined": True, "fails": 3, "fail_release": REL_A}, 13, ("OLD_RULE", "fail")),
    ({"release": REL_A, "quarantined": True, "fails": 3, "first_strike_utc": _h(-1), "retry_after": _h(11)}, 13,
     ("OLD_RULE", "fail")),                                                  # a stamp from before the lift
    ({"release": REL_A, "quarantined": True, "fails": 3, "first_strike_utc": _h(0.5), "retry_after": _h(6)}, 13,
     ("OLD_RULE", "fail")),                                                  # retry_after - first < 12 h
    ({"release": REL_A, "quarantined": True, "fails": 3, "first_strike_utc": _h(0.5), "retry_after": _h(12.5)}, 12,
     ("OLD_RULE", "fail")),                                                  # now - first < 12 h
    ({"release": REL_A, "quarantined": True, "fails": 3, "first_strike_utc": _h(0.5), "retry_after": _h(12.5)}, 13,
     ("QUARANTINED_NEW_RULE", "pass")),
    ({"release": REL_A, "merged": REL_A, "served": REL_A, "fails": 0}, 1, ("MERGED", "pass")),
    ({"release": REL_A, "merged": REL_A, "served": "2026-10-15T12:30"}, 1, ("MERGED_NOT_SERVED", "wait")),
    ({"release": "2026-12-01T13:30", "merged": "2026-10-15T12:30", "fails": 0}, 1, ("RE_RELEASED", "pass")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 1, "first_strike_utc": _h(0.5), "retry_after": _h(1.5)},
     2, ("HELD_STRIKE", "pass")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 1, "retry_after": None}, 2, ("OLD_RULE", "fail")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 1, "first_strike_utc": _h(0.5), "retry_after": _h(0.6)},
     2, ("OLD_RULE", "fail")),                                               # a strike without the 1 h spacing
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 1, "refused_http": 403}, 1,
     ("HELD_TRANSIENT", "pass")),                                            # freed by a breaker close (review V2)
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 2, "retry_after": _h(1),
      "last_error": "whole-table fetch failed: statcan zip ... HTTP 403"}, 1, ("HELD_TRANSIENT", "pass")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 6, "retry_after": _h(5)}, 5,
     ("HELD_TRANSIENT_RED", "wait")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 1, "retry_after": _h(1),
      "last_error": "merge failed on this machine: OutOfMemory"}, 1, ("HELD_TRANSIENT_RED", "wait")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 0, "publisher": {"end": "x"},
      "bytes": 1}, 1, ("UNTOUCHED", "wait")),
    ({"release": REL_A, "merged": "2026-10-15T12:30", "fails": 0, "transient_fails": 0, "retry_after": _h(3)}, 1,
     ("UNCLASSIFIED", "fail")),
])
def test_classify_judges_the_fixed_outcome_fields_in_order(cube, now_h, want):
    assert tool.classify(cube, REL_A, LIFTED, LIFTED + dt.timedelta(hours=now_h), 0) == want


def _receipt(iso, cubes, status="LIFTED", tf=0):
    p = iso["tmp"] / "receipt.json"
    p.write_text(json.dumps({"utc_lift": _z(LIFTED), "status": status, "cubes": {
        pid: {"class": tool.LIFT[pid][0], "release": tool.LIFT[pid][1], "after_transient_fails": tf}
        for pid in cubes}}))
    return str(p)


def _check_at(iso, monkeypatch, capsys, hours, edit):
    st = _doc()
    for p in ("10100111", "16100047"):
        c = st["cubes"][p]
        for k in ("quarantined", "fail_release", "retry_after"):
            c.pop(k, None)
        c["fails"] = 0
    edit(st)
    _seed(iso, raw=json.dumps(st, sort_keys=True).encode())
    monkeypatch.setattr(tool, "_now", lambda: LIFTED + dt.timedelta(hours=hours))
    rc = _run("--check", _receipt(iso, ["10100111", "16100047"]))
    return rc, capsys.readouterr().out


def _merge(st, p):
    st["cubes"][p].update(merged=st["cubes"][p]["release"], served=st["cubes"][p]["release"])


def test_check_passes_merged_and_new_rule_holds(iso, monkeypatch, capsys):
    def edit(st):
        _merge(st, "10100111")
        st["cubes"]["16100047"].update(fails=1, first_strike_utc=_h(0.1), retry_after=_h(1.1))
    rc, out = _check_at(iso, monkeypatch, capsys, 2, edit)
    assert rc == 0 and "PASS" in out and "HELD_STRIKE 1" in out and "MERGED 1" in out


def test_check_is_pending_while_a_cube_is_untouched_and_fails_at_24_h(iso, monkeypatch, capsys):
    def edit(st):
        _merge(st, "10100111")
        st["breaker"] = {"status": 403, "until": _h(25)}
    rc, out = _check_at(iso, monkeypatch, capsys, 23.9, edit)
    assert rc == 3 and "PENDING" in out and "UNTOUCHED" in out and "['breaker']" in out
    rc, out = _check_at(iso, monkeypatch, capsys, 24, edit)
    assert rc == 1 and "FAIL" in out and "16100047" in out


def test_check_fails_at_once_on_an_old_rule_quarantine(iso, monkeypatch, capsys):
    def edit(st):
        _merge(st, "10100111")
        st["cubes"]["16100047"].update(quarantined=True, fails=3, fail_release=st["cubes"]["16100047"]["release"])
    rc, out = _check_at(iso, monkeypatch, capsys, 1, edit)
    assert rc == 1 and "OLD_RULE" in out


def test_check_refuses_a_file_that_is_not_a_receipt(iso, capsys, tmp_path):
    _seed(iso)
    p = tmp_path / "r.json"
    p.write_text(json.dumps({"cubes": {}}))
    assert _run("--check", str(p)) == 1


# --------------------------------------------------------------------------- #
# END TO END: the lift, then the lane's own run() over the fake www150 (tests/test_statcan_strike_rule.py)
# --------------------------------------------------------------------------- #
def test_end_to_end_a_lifted_class_a_cube_merges_and_a_lifted_class_b_cube_is_held_by_the_new_rule(
        iso, world, clock, statcan, capsys, monkeypatch):
    """world (tests/test_statcan_lane.py) is the lane's local store; the backend is local, so the tool reads and writes
    the same file the lane does. 10100111 (A): its zip answers. 16100047 (B): its zip stays one period short of
    getCubeMetadata for ever (a real count-gate refusal). 99999999: quarantined, not named - stays quarantined."""
    monkeypatch.setattr(lane, "pin_backend", lambda *a, **k: None)
    monkeypatch.setattr(tool, "_now", clock)
    t0 = clock.t = _t("2026-11-23T08:00Z")
    statcan.add("10100111", REL_A)
    statcan.add("16100047", "2026-11-13T13:30", rebuilt=_t("2027-01-01T00:00Z"))       # never rebuilt
    statcan.add(OUTSIDE, REL_A)
    rows = [("v1", "2026-08-01", 0.0, "Canada", "1.1"), ("v2", "2026-08-01", 1.0, "Canada", "1.2"),
            ("v1", "2026-09-01", 1.0, "Canada", "1.1"), ("v2", "2026-09-01", 2.0, "Canada", "1.2")]
    cubes = {}
    for p in ("10100111", "16100047", OUTSIDE):
        _stored(world / f"{p}.parquet", rows)
        c = _cube(p if p in tool.LIFT else "10100111")
        c.update(merged="2026-10-15T12:30", served="2026-10-15T12:30", rows=4, bytes=None, est_min=None)
        if p == OUTSIDE:
            c["last_error"] = tool._SIG_A.format(pid=OUTSIDE)
        cubes[p] = c
    st = {"version": 1, "floor": lane.FLOOR, "cubes": cubes, "parsed_rows": {}, "new_cubes": {},
          "last_enum_utc": _z(t0 - dt.timedelta(hours=2))}
    (world / "_lane_state.json").write_text(json.dumps(st, sort_keys=True))
    # the restarted lane: a process created after the code's last write; its first iteration enumerates and is idle
    iso["procs"]["found"] = [{"pid": os.getpid(), "cmdline": ["python", "-u", "jobs/statcan_lane.py"],
                              "cwd": str(iso["tmp"]), "create_time": _ep(t0 - dt.timedelta(minutes=5)) + 0.4}]
    r = lane.run(put=_Bucket(), now_fn=clock)
    assert r["enumerated"] and r["worked"] == 0, r                 # every cube still quarantined: idle
    clock.step(2)
    rc, out, (b, a) = _plan(capsys, "--pids", "10100111,16100047")
    assert rc == 0, out[-3000:]
    rc = _run("--pids", "10100111,16100047", "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 0, out[-3000:]
    lifted_at = clock.t
    receipt = re.search(r"receipt (\S+_receipt\.json)", out).group(1)
    first = {}
    while clock.t < lifted_at + dt.timedelta(hours=13):
        clock.step()
        lane.run(put=_Bucket(), now_fn=clock)
        for t, k, p, s in statcan.calls:
            if k == "zip" and p in ("10100111", "16100047") and t >= lifted_at:
                first.setdefault(p, t)
        cs = json.loads((world / "_lane_state.json").read_text())["cubes"]
        if clock.t == lifted_at + dt.timedelta(minutes=10):
            assert cs["10100111"]["merged"] == REL_A and cs["10100111"]["served"] == REL_A
            assert cs["16100047"]["fails"] == 1 and cs["16100047"]["first_strike_utc"]
        assert cs[OUTSIDE]["quarantined"] is True                 # never named: never lifted
        assert not (cs["16100047"].get("quarantined") and clock.t < first["16100047"] + dt.timedelta(hours=12))
    with capsys.disabled():                                       # the measured line reaches the log (review F12)
        print(f"\nMEASURED: lift {_z(lifted_at)} -> first zip GET 10100111 {_z(first['10100111'])}, "
              f"16100047 {_z(first['16100047'])} ({(first['10100111'] - lifted_at).total_seconds() / 60:.0f} min, "
              f"the next 5-min iteration); 16100047 quarantined again {cs['16100047'].get('quarantined')} with "
              f"first strike {cs['16100047'].get('first_strike_utc')}")
    assert first["10100111"] - lifted_at <= dt.timedelta(minutes=5)
    assert cs["16100047"]["quarantined"] is True                  # the count gate refused for ever: held at 12 h
    zips = sorted({t for t, k, p, s in statcan.calls if k == "zip" and p == "16100047" and t >= lifted_at})
    assert zips == [first["16100047"], first["16100047"] + dt.timedelta(hours=1),
                    first["16100047"] + dt.timedelta(hours=12)], zips
    rc = _run("--check", receipt)
    out = capsys.readouterr().out
    assert rc == 0 and "MERGED 1" in out and "QUARANTINED_NEW_RULE 1" in out, out
    # NEGATIVE CONTROL: the same receipt over a hand-made OLD-rule re-quarantine (no first_strike_utc) must FAIL
    st = json.loads((world / "_lane_state.json").read_text())
    st["cubes"]["16100047"].pop("first_strike_utc")
    (world / "_lane_state.json").write_text(json.dumps(st, sort_keys=True))
    rc = _run("--check", receipt)
    assert rc == 1 and "OLD_RULE" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# verification review round 1 (V2-V7): the states the lane's OWN writers leave, the receipt on every exit, the
# receipt's status, the scan's spellings, and the regex alternatives - each pinned by the lane's own text
# --------------------------------------------------------------------------- #
def _lifted_cube():
    c = _cube("10100111")
    for k in ("quarantined", "fail_release", "retry_after"):
        c.pop(k)
    c["fails"] = 0
    return c


def test_classify_the_states_the_lanes_own_writers_leave():
    t = LIFTED + dt.timedelta(minutes=5)
    c = _lifted_cube()
    lane._backoff(c, "whole-table fetch failed: statcan zip x HTTP 403", t)
    assert tool.classify(c, REL_A, LIFTED, t, 0) == ("HELD_TRANSIENT", "pass")
    c["refused_http"] = 403                                   # a breaker close pops its retry_after (review V2, P1)
    st = {"cubes": {"10100111": c}, "breaker": {"status": 403}}
    lane._breaker_close(st, "10100141")
    assert "retry_after" not in c and tool.classify(c, REL_A, LIFTED, t, 0) == ("HELD_TRANSIENT", "pass")
    c = _lifted_cube()
    lane._deterministic(c, "10100111", "whole-table fetch refused: x", t)
    assert tool.classify(c, REL_A, LIFTED, t + dt.timedelta(minutes=1), 0) == ("HELD_STRIKE", "pass")
    lane._deterministic(c, "10100111", "whole-table fetch refused: x", t + dt.timedelta(hours=1))
    lane._deterministic(c, "10100111", "whole-table fetch refused: x", t + dt.timedelta(hours=12))
    assert c["quarantined"] is True
    assert tool.classify(c, REL_A, LIFTED, t + dt.timedelta(hours=12, minutes=1), 0) == ("QUARANTINED_NEW_RULE", "pass")
    c = _lifted_cube()                                       # a trip puts the pre-attempt snapshot back, then backs off
    snap = {k: c[k] for k in lane._RESTORE_FIELDS if k in c}
    lane._deterministic(c, "10100111", "whole-table fetch refused: statcan zip x HTTP 404", t)
    st = {"cubes": {"10100111": c}}
    lane._trip_breaker(st, 404, [("10100111", snap, "definitive", False)], t)
    assert tool.classify(c, REL_A, LIFTED, t, 0) == ("HELD_TRANSIENT", "pass")


@pytest.mark.parametrize("text,src", [
    ("the ingest record (10100111.done) says it is held; the store does not have it - a store fault (R753), not a new cube",
     "says it is held; the store does not \""),                   # the lane's f-string is split over two lines
    ("stored cube unreadable: ArrowInvalid: Parquet magic bytes not found", "stored cube unreadable"),
    ("the store could not be asked: ConnectionError: x", "the store could not be asked"),
])
def test_every_store_fault_text_the_lane_writes_is_not_held(text, src):
    assert src in lane.__loader__.get_source(lane.__name__)        # the lane still writes this text
    c = dict(_lifted_cube(), transient_fails=1, retry_after=_h(1), last_error=text)
    assert tool.classify(c, REL_A, LIFTED, LIFTED + dt.timedelta(minutes=10), 0) == ("NOT_HELD", "fail")


@pytest.mark.parametrize("text,src", [
    ("merge failed on this machine: OutOfMemoryException: x", "merge failed on this machine"),
    ("key check failed: ArrowInvalid: x", "key check failed"),
    ("unexpected OSError in the whole-table fetch: disk full", "unexpected {type(e).__name__} in the whole-table"),
    ("unexpected KeyError: 'x'", "_backoff(c, f\"unexpected {type(e).__name__}: {e}\""),
    ("history-floor gate could not read the first valued dates: x", "history-floor gate could not read"),
])
def test_every_this_machine_transient_text_is_held_red_not_a_pass(text, src):
    assert src in lane.__loader__.get_source(lane.__name__)
    c = dict(_lifted_cube(), transient_fails=1, retry_after=_h(1), last_error=text)
    assert tool.classify(c, REL_A, LIFTED, LIFTED + dt.timedelta(minutes=10), 0) == ("HELD_TRANSIENT_RED", "wait")


@pytest.mark.parametrize("status", ["FAILED_NOTHING_CHANGED", "WRITING", "READBACK_FAILED", "UNKNOWN",
                                    "OTHER_DOCUMENT", "WRITTEN_NOT_VERIFIED", None])
def test_check_judges_only_a_verified_lift(iso, monkeypatch, capsys, status):
    _seed(iso)                                                   # every cube still quarantined: nothing lifted
    rc = _run("--check", _receipt(iso, ["10100111", "16100047"], status=status))
    out = capsys.readouterr().out
    assert rc == 1 and "this receipt records no verified lift" in out and "OLD_RULE" not in out


def test_check_counts_transients_against_the_receipts_own_after_value(iso, monkeypatch, capsys):
    def edit(st):
        _merge(st, "10100111")
        st["cubes"]["16100047"]["transient_fails"] = 2           # lifted with 2, unchanged since: UNTOUCHED
    rc, out = _check_at(iso, monkeypatch, capsys, 1, edit)
    assert rc == 0 and "HELD_TRANSIENT 1" in out                # receipt says 0: 2 > 0 reads as a new transient...
    st = _doc()
    for p in ("10100111", "16100047"):
        c = st["cubes"][p]
        for k in ("quarantined", "fail_release", "retry_after"):
            c.pop(k, None)
        c["fails"] = 0
    _merge(st, "10100111")
    st["cubes"]["16100047"]["transient_fails"] = 2
    _seed(iso, raw=json.dumps(st, sort_keys=True).encode())
    monkeypatch.setattr(tool, "_now", lambda: LIFTED + dt.timedelta(hours=1))
    rc = _run("--check", _receipt(iso, ["10100111", "16100047"], tf=2))
    out = capsys.readouterr().out
    assert rc == 3 and "UNTOUCHED" in out                        # ...but with the receipt's 2 it is untouched


def test_apply_records_each_cubes_transient_count_in_the_receipt(iso, capsys):
    _seed(iso, edit=_cube_edit("10100111", transient_fails=2))
    rc, out, b, a = _apply_after_plan(capsys, "--pids", "10100111")
    assert rc == 0
    rec = json.load(open(re.search(r"receipt (\S+_receipt\.json)", out).group(1), encoding="utf-8"))
    assert rec["cubes"]["10100111"]["after_transient_fails"] == 2 and rec["status"] == "LIFTED"
    assert re.search(r"^  10100111 A .* transient_fails 2 ", out, re.M)    # the plan shows it (review C11)


def test_a_good_put_and_a_read_back_that_keeps_failing_leaves_a_readback_failed_receipt(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    iso["r2"].after_put_get_error = RuntimeError("read timeout")
    monkeypatch.setattr(tool.time, "sleep", lambda s: None)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "READ-BACK FAILED 3 times" in out and "NOT VERIFIED" in out
    assert tool.sha256(iso["r2"].objects[KEY]) == a              # the write landed; nobody claims it did
    rec = json.load(open(re.search(r"Receipt (\S+_receipt\.json)", out).group(1), encoding="utf-8"))
    assert rec["status"] == "READBACK_FAILED" and rec["after_sha256"] == a and os.path.exists(rec["backup"])
    assert not os.path.exists(writer_lock.lock_path(lane.LOCK_NAME))


def test_a_read_back_that_fails_once_is_retried(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    r2, n = iso["r2"], {"gets": 0}
    real_get = r2.get_object

    def get(Bucket, Key, **kw):
        if r2.puts and Key == KEY:
            n["gets"] += 1
            if n["gets"] == 1:
                raise RuntimeError("one blip")
        return real_get(Bucket, Key, **kw)
    monkeypatch.setattr(r2, "get_object", get)
    monkeypatch.setattr(tool.time, "sleep", lambda s: None)
    assert _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a) == 0


def test_an_intent_receipt_exists_while_the_write_runs(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    real, seen = blob.write_bytes_atomic, {}

    def write(path, data):
        rs = [f for f in os.listdir(tool.RECEIPTS) if f.endswith("_receipt.json")]
        seen["status"] = json.load(open(os.path.join(tool.RECEIPTS, rs[0]), encoding="utf-8"))["status"]
        return real(path, data)
    monkeypatch.setattr(blob, "write_bytes_atomic", write)
    assert _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a) == 0
    assert seen["status"] == "WRITING"


def test_a_mirror_that_does_not_read_back_as_the_after_document_is_not_verified(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    raw = iso["r2"].objects[KEY]
    monkeypatch.setattr(tool, "mirror_bytes", lambda: raw if iso["r2"].puts else None)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "check mirror_is_after: FAIL" in out
    rec = json.load(open(re.search(r"receipt (\S+_receipt\.json)", out).group(1), encoding="utf-8"))
    assert rec["status"] == "WRITTEN_NOT_VERIFIED" and rec["checks"]["mirror_is_after"] is False


def test_a_mirror_read_that_raises_after_the_write_is_not_verified_and_leaves_a_receipt(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)

    def boom():
        if iso["r2"].puts:
            raise PermissionError("held open")
        return None
    monkeypatch.setattr(tool, "mirror_bytes", boom)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "the mirror cannot be read back: PermissionError" in out and "WRITTEN BUT NOT VERIFIED" in out


def test_a_mirror_that_cannot_be_put_back_after_a_failed_put_says_so(iso, capsys, monkeypatch):
    b, a = _go(iso, capsys)
    raw = iso["r2"].objects[KEY]
    iso["r2"].put_error = RuntimeError("PUT refused")

    def deny(path, data):
        raise PermissionError("held open")
    monkeypatch.setattr(tool, "_write_local", deny)
    rc = _run("--from-table2", TABLE2, "--apply", "--expect-before-sha", b, "--expect-after-sha", a)
    out = capsys.readouterr().out
    assert rc == 1 and "the mirror could not be put back: PermissionError" in out and "(NOT VERIFIED)" in out
    assert iso["r2"].objects[KEY] == raw


def test_the_plan_prints_a_held_lock_and_the_bounds_are_pinned(iso, capsys, monkeypatch):
    _seed(iso)
    monkeypatch.setattr(writer_lock, "owner", lambda name: {"pid": LANE_PID, "what": "jobs/statcan_lane.py"})
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert "R4 writer lock" in out and 'HELD by {"pid": 48712, "what": "jobs/statcan_lane.py"}' in out
    assert (tool.LOCK_HOLD_MAX_S, tool.BEAT_MAX_AGE_MIN, tool.CHECK_DEADLINE_H, tool.READBACK_TRIES) == (240.0, 15.0,
                                                                                                         24.0, 3)


@pytest.mark.parametrize("spent,want", [(239.0, 0), (241.0, 1)])
def test_the_lock_hold_bound_is_240_s(iso, capsys, spent, want):
    b, a = _go(iso, capsys)
    ticks = iter([0.0] + [spent] * 10)
    rc = tool.apply(_ns(expect_before_sha=b, expect_after_sha=a), sorted(tool.LIFT), clock=lambda: next(ticks))
    assert rc == want


def test_the_scan_is_case_blind_and_sees_every_interpreter_name(monkeypatch):
    import psutil
    table = [_P(1, "python.exe", ["python", "-u", r"JOBS\STATCAN_LANE.PY"]),
             _P(2, "python3.14t.exe", ["python3.14t", "-u", "jobs/statcan_lane.py"]),
             _P(3, "PYTHONW.EXE", ["pythonw", "-m", "jobs.statcan_lane"]),
             _P(4, "pyw.exe", ["pyw", "jobs/statcan_lane.py"]),
             _P(5, "pycharm64.exe", ["pycharm64", "statcan_lane.py"])]
    monkeypatch.setattr(psutil, "process_iter", lambda *a, **k: iter(table))
    found, unreadable = REAL_LANE_PROCESSES()
    assert [p["pid"] for p in found] == [1, 2, 3, 4] and unreadable == []


def test_an_old_lane_spelled_in_upper_case_beside_a_new_one_refuses(iso, capsys, monkeypatch):
    import psutil
    _seed(iso)
    new = _proc(iso)
    table = [_P(LANE_PID, "python.exe", new["cmdline"], new["cwd"], new["create_time"]),
             _P(777, "python.exe", ["python", "-u", r"JOBS\STATCAN_LANE.PY"], str(iso["tmp"]),
                _ep(NOW - dt.timedelta(days=2)))]
    monkeypatch.setattr(psutil, "process_iter", lambda *a, **k: iter(table))
    monkeypatch.setattr(tool, "lane_processes", REAL_LANE_PROCESSES)
    rc, out, _s = _plan(capsys, "--from-table2", TABLE2)
    assert rc == 1 and "R5 pid 777" in out and "before the last write of the strike-rule code" in out
