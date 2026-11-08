"""tools/clear_fed_board_z1_retries.py clears ONLY queue rows whose served CSV is proven current, and only
through the guarded pull -> clear -> push lifecycle.

Pinned (offline: a fake HEAD-only R2, a fixture Z1.parquet, the state store in tmp_path, pull/push recorded):
  (a) report-only never pulls, clears or pushes; the verdicts are equal / differ / missing / no_rows by bytes;
  (b) --apply clears exactly the EQUAL ids and pushes once; the others stay queued with attempts and
      enqueued_utc unchanged; another source's rows are untouched;
  (c) a Z1 ETag that moves after the pull: nothing cleared, nothing pushed (exit 5);
  (d) a writer guard that refuses before the pull: no pull (exit 3); before the push: nothing cleared, no push;
  (e) a copy that is not R2's object: refused before any served object is judged (exit 2);
  (f) ids outside fed_board:Z1:, and a cutover flag, are refused; a failed push is exit 6;
  (g) classify never calls a gzip object without csvmd5 'equal' unless this machine's gzip matches, and a
      multipart ETag is 'unknown'.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from tools import clear_fed_board_z1_retries as T  # noqa: E402
from tools.derive_csv_bulk import csv_key  # noqa: E402
from updater import config  # noqa: E402
from updater.state import StateStore  # noqa: E402

A, B, C, D, E = (T.PREFIX + k for k in ("A1.Q", "B1.Q", "C1.Q", "D1.Q", "E1.Q"))   # E1.Q has no store rows


class _Err(Exception):
    def __init__(self, code):
        self.response = {"ResponseMetadata": {"HTTPStatusCode": code}, "Error": {"Code": str(code)}}


class FakeS3:
    def __init__(self, copy):
        with open(copy, "rb") as fh:
            raw = fh.read()
        self.z1 = {"ETag": '"%s"' % hashlib.md5(raw).hexdigest(), "ContentLength": len(raw),
                   "LastModified": dt.datetime(2026, 9, 23, tzinfo=dt.timezone.utc)}
        self.served = {}
        self.calls = []

    def head_object(self, Bucket, Key):
        self.calls.append(Key)
        if Key == T.Z1_KEY:
            return dict(self.z1)
        if Key not in self.served:
            raise _Err(404)
        return dict(self.served[Key])


@pytest.fixture
def rig(tmp_path, monkeypatch):
    for k in ("GITHUB_ACTIONS", "GITHUB_RUN_ID", "AQUEDUCT_BACKEND", "AQUEDUCT_RUN_LOCATION", "ECONDL_DATA"):
        monkeypatch.delenv(k, raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover"))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state" / "state.db"))
    copy = tmp_path / "root" / "fed_board" / "Z1.parquet"
    os.makedirs(copy.parent)
    keys = ["A1.Q", "B1.Q", "C1.Q", "D1.Q"]
    pq.write_table(pa.table({
        "dataset": ["Z1"] * 8, "series_key": [k for k in keys for _ in (0, 1)],
        "obs_date": pa.array([dt.date(2025, 12, 31), dt.date(2026, 6, 30)] * 4, type=pa.date32()),
        "value": [1.0, 2.0, 3.0, 4.0, 5.5, 6.25, 7.0, 8.0], "obs_status": pa.array([None] * 8, type=pa.string())}),
        str(copy))
    s3 = FakeS3(str(copy))
    bodies = T.derive_local(str(copy), [A, B, C, D])
    from core.r2_util import gzip_bytes
    md5 = lambda b: hashlib.md5(b).hexdigest()                          # noqa: E731
    s3.served[csv_key("series", "fed_board", A.split(":", 1)[1])] = {
        "ETag": '"x"', "Metadata": {"csvmd5": md5(bodies[A])}, "ContentEncoding": "gzip"}
    s3.served[csv_key("series", "fed_board", B.split(":", 1)[1])] = {
        "ETag": '"x"', "Metadata": {"csvmd5": "0" * 32}, "ContentEncoding": "gzip"}
    s3.served[csv_key("series", "fed_board", D.split(":", 1)[1])] = {
        "ETag": '"%s"' % md5(gzip_bytes(bodies[D])), "Metadata": {}, "ContentEncoding": "gzip"}
    st = StateStore()
    st.enqueue_csv_retry("fed_board", [A, B, C, D, E], "derive budget spent - deferred, not failed")
    st.enqueue_csv_retry("ilostat", ["ilostat:X"], "planted")
    q0 = {r["series_id"]: (r["attempts"], r["enqueued_utc"]) for r in st.csv_retries()}
    st.db.close()
    ids = tmp_path / "ids.txt"
    ids.write_text("\n".join([A, B, C, D, E]) + "\n", encoding="utf-8")

    class R:
        pass
    r = R()
    r.copy, r.s3, r.ids, r.q0, r.tmp, r.runs = str(copy), s3, str(ids), q0, tmp_path, []
    r.guard = [True, True]

    def runner(*args):
        r.runs.append(args[0])
        return 0
    r.runner = runner

    def guards(window_min=None):
        return r.guard.pop(0) if r.guard else True
    monkeypatch.setattr(T, "guards_ok", guards)

    def go(*extra, out="ev.tsv"):
        return T.main(["--copy", r.copy, "--ids", r.ids, "--out", str(tmp_path / out), "--spot", "4", *extra],
                      s3=r.s3, state_runner=r.runner)
    r.go = go

    def queue():
        s = StateStore()
        try:
            return {x["series_id"]: (x["attempts"], x["enqueued_utc"]) for x in s.csv_retries()}
        finally:
            s.db.close()
    r.queue = queue
    return r


def _verdicts(path):
    with open(path, encoding="utf-8") as fh:
        return {ln.split("\t")[0]: ln.split("\t")[1] for ln in fh.read().splitlines()[1:]}


def test_report_only_touches_no_state(rig):
    assert rig.go() == T.EXIT_STAND
    assert rig.runs == [], "report-only must not pull or push"
    assert rig.queue() == rig.q0
    assert _verdicts(rig.tmp / "ev.tsv") == {A: "equal", B: "differ", C: "missing", D: "equal", E: "no_rows"}


def test_apply_clears_only_the_equal_ids(rig):
    assert rig.go("--apply") == T.EXIT_STAND
    assert rig.runs == ["--pull-state", "--push-state"]
    q = rig.queue()
    assert A not in q and D not in q
    for s in (B, C, E, "ilostat:X"):
        assert q[s] == rig.q0[s], f"{s}: attempts and enqueued_utc must be unchanged"


def test_an_etag_that_moves_after_the_pull_clears_nothing(rig):
    def runner(*args):
        rig.runs.append(args[0])
        if args[0] == "--pull-state":
            rig.s3.z1["ETag"] = '"' + "9" * 32 + '"'
        return 0
    rig.runner = runner
    assert rig.go("--apply") == T.EXIT_MOVED
    assert rig.runs == ["--pull-state"] and rig.queue() == rig.q0


def test_a_guard_before_the_pull_refuses_without_pulling(rig):
    rig.guard = [False]
    assert rig.go("--apply") == T.EXIT_GUARD
    assert rig.runs == [] and rig.queue() == rig.q0


def test_a_guard_before_the_push_clears_nothing(rig):
    rig.guard = [True, False]
    assert rig.go("--apply") == T.EXIT_GUARD
    assert rig.runs == ["--pull-state"] and rig.queue() == rig.q0


def test_a_copy_that_is_not_r2s_object_is_refused_first(rig):
    rig.s3.z1["ETag"] = '"' + "1" * 32 + '"'
    assert rig.go("--apply") == T.EXIT_REFUSED
    assert rig.s3.calls == [T.Z1_KEY], "no served object may be judged against an unproven copy"
    assert rig.runs == [] and rig.queue() == rig.q0


def test_ids_outside_the_z1_prefix_are_refused(rig):
    with open(rig.ids, "a", encoding="utf-8") as fh:
        fh.write("fed_board:H15:RIFSPFF_N.B\n")
    assert rig.go() == T.EXIT_REFUSED


def test_after_t0_it_refuses(rig, monkeypatch):
    from core import cutover
    flag = rig.tmp / "CUTOVER"
    flag.write_text("x")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    assert rig.go("--apply") == T.EXIT_REFUSED and rig.runs == []


def test_a_failed_push_is_its_own_exit(rig):
    def runner(*args):
        rig.runs.append(args[0])
        return 2 if args[0] == "--push-state" else 0
    rig.runner = runner
    assert rig.go("--apply") == T.EXIT_PUSH


def test_classify_is_conservative():
    from core.r2_util import gzip_bytes
    body = b"series_id,obs_date,value\nZ1:A1.Q,2026-06-30,2.0\n"
    m = hashlib.md5(body).hexdigest()
    g = hashlib.md5(gzip_bytes(body)).hexdigest()
    assert T.classify({"status": "200", "csvmd5": m}, body) == "equal"
    assert T.classify({"status": "200", "csvmd5": "0" * 32}, body) == "differ"
    assert T.classify({"status": "200", "etag": g, "content_encoding": "gzip"}, body) == "equal"
    assert T.classify({"status": "200", "etag": "f" * 32, "content_encoding": "gzip"}, body) == "unknown"
    assert T.classify({"status": "200", "etag": m + "-2"}, body) == "unknown"
    assert T.classify({"status": "200", "etag": m}, body) == "equal"
    assert T.classify({"status": "200", "etag": "f" * 32}, body) == "differ"
    assert T.classify({"status": "404"}, body) == "missing"
    assert T.classify({"status": "err:X"}, body) == "error"
    assert T.classify({"status": "200", "csvmd5": m}, None) == "no_rows"


def test_the_client_is_head_only():
    c = T._HeadOnlyClient(object())
    for op in ("get_object", "put_object", "delete_object", "list_objects_v2"):
        with pytest.raises(RuntimeError):
            getattr(c, op)
