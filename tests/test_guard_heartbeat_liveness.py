"""A name match is not liveness (R457, reintroduced as R799, half-fixed as R802).

`_alive_jobs` joined EVERY python.exe command line into one string and asked `if t in cmds`, so
any process merely MENTIONING a tracked name counted, and a process counted from its first
second: `ingest_istat_sliced.py` matched ~2 s after the guard relaunched it, inside the nine
seconds it lived before its designed preflight exit, and CI read `jobs_alive=3/3` while istat had
crawled nothing.

The FIRST attempt at this fix was itself failed by review (R802) for three reasons, all pinned
below because each is a live regression risk:

  * it returned EVERY .py token, so a tracked name passed as an ARGUMENT still forged a beat -
    and the test that claimed to cover it passed only because its fixture was
    `ingest_cbs_nl.py-is-slow`, which does not end in `.py`. The fixture had been shaped to the
    rule. It is now a bare `.py` argument, which is the case that actually failed;
  * `text=True` decoded PowerShell's stdout with the ANSI code page, so one unrelated process
    with a non-ASCII path raised UnicodeDecodeError, the bare except swallowed it, and the beat
    reported nothing running while two crawlers were up - a blind instrument reading as a clean
    one;
  * it dropped processes younger than 60 s out of `jobs_alive`. Measured over logs/_guard.log,
    cbs_nl is legitimately relaunched 372 times at a 68.2-minute median, so roughly one beat in
    six would have called a healthy crawler dead. The age is now REPORTED, never judged.

The last test is the DISCRIMINATION CONTROL: it runs the OLD rule over the same fixtures and
asserts it gets them wrong.
"""
from __future__ import annotations
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import tools.guard_heartbeat as gh                            # noqa: E402

PY = r"C:\Users\x\AppData\Local\Programs\Python\Python314\python.exe"
Q = '"' + PY + '"'

REAL = {"ProcessId": 111, "AgeS": 7200, "CommandLine": Q + " jobs/ingest_cbs_nl.py"}
# R802 finding 4: the name as an ARGUMENT of a different script. The previous fixture ended in
# "-is-slow" and so could never have caught the hole it was written for.
ARG = {"ProcessId": 222, "AgeS": 7200,
       "CommandLine": Q + " tools/audit_x.py --script ingest_cbs_nl.py"}
LONGER = {"ProcessId": 333, "AgeS": 7200, "CommandLine": Q + " jobs/my_ingest_cbs_nl.py"}
YOUNG = {"ProcessId": 444, "AgeS": 9,
         "CommandLine": Q + ' "E:\\research\\x\\jobs\\ingest_istat_sliced.py"'}


class _Ran:
    def __init__(self, rows, rc=0, raw=None):
        self.returncode = rc
        self.stdout = raw if raw is not None else json.dumps(
            rows if len(rows) != 1 else rows[0]).encode("utf-8")
        self.stderr = b""


def _table(monkeypatch, rows, rc=0, raw=None):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _Ran(rows, rc, raw))


# ------------------------------------------------------------------ the script rule, in isolation
def test_the_script_is_the_first_py_token():
    assert gh._script_of(Q + " jobs/ingest_cbs_nl.py") == "ingest_cbs_nl.py"
    assert gh._script_of(Q + ' "E:\\r\\jobs\\ingest_cbs_nl.py"') == "ingest_cbs_nl.py"


def test_the_name_as_an_ARGUMENT_is_NOT_the_job_running():
    """R802 finding 4. These are the command lines a session runs while investigating a sick
    crawler, so the old rule lied hardest exactly when someone was looking at the beat."""
    assert gh._script_of(Q + " tools/audit_x.py --script ingest_cbs_nl.py") == "audit_x.py"
    assert gh._script_of(Q + " tests/t.py ingest_cbs_nl.py") == "t.py"


def test_a_dash_m_or_dash_c_invocation_never_yields_a_script():
    assert gh._script_of(Q + " -m py_compile jobs/ingest_cbs_nl.py") is None
    assert gh._script_of(Q + " -m flake8 ingest_cbs_nl.py") is None
    assert gh._script_of(Q + ' -c "print(1) ingest_cbs_nl.py"') is None


def test_a_longer_name_containing_the_tracked_one_is_not_that_job():
    assert gh._script_of(Q + " jobs/my_ingest_cbs_nl.py") == "my_ingest_cbs_nl.py"
    assert gh._script_of(Q + " jobs/ingest_cbs_nl.pyz") is None


def test_an_unparseable_command_line_reports_NOT_RUNNING_rather_than_guessing():
    """R802: the previous version fell back to `cmdline.split()`, which restored the very
    substring bug it replaced. This function only feeds the heartbeat - the guard's own relaunch
    decision is in RELAUNCH_GUARD.ps1 - so failing closed mis-reports and never mis-relaunches."""
    assert gh._script_of(Q + ' -c "print(1)  jobs/ingest_cbs_nl.py') is None
    assert gh._script_of("") is None


# ------------------------------------------------------------------ the matcher over a table
def test_a_real_launch_is_detected(monkeypatch):
    _table(monkeypatch, [REAL])
    d = gh._alive_jobs_detail()
    assert [x["name"] for x in d] == ["ingest_cbs_nl.py"]
    assert d[0]["pid"] == 111 and d[0]["age_s"] == 7200


def test_an_argument_or_a_longer_name_does_not_count_as_the_job(monkeypatch):
    _table(monkeypatch, [ARG, LONGER])
    assert gh._alive_jobs_detail() == []


def test_a_single_row_object_is_handled(monkeypatch):
    """ConvertTo-Json emits a bare object, not a list, when there is exactly one process."""
    _table(monkeypatch, [REAL])
    assert len(gh._alive_jobs_detail()) == 1


def test_no_python_processes_reads_as_empty_not_as_unknown(monkeypatch):
    _table(monkeypatch, [], raw=b"")
    assert gh._alive_jobs_detail() == []


def test_UNDECODABLE_output_is_UNKNOWN_not_empty(monkeypatch):
    """R802 finding 3, the critical one. PowerShell's stdout is OEM-encoded; `text=True` decoded
    it with the ANSI code page, so ONE unrelated process with a non-ASCII path raised
    UnicodeDecodeError, the bare except swallowed it, and the beat said nothing was running
    while both crawlers were up. Now the bytes are decoded with errors='replace' and the row
    survives."""
    rows = [dict(REAL)]
    raw = json.dumps(rows[0]).encode("utf-8").replace(
        b"jobs/", "jobs/\udce9".encode("utf-8", "surrogatepass"))
    _table(monkeypatch, rows, raw=raw)
    got = gh._alive_jobs_detail()
    assert got is not None, "an undecodable byte must not silently empty the table"


def test_a_FAILED_process_query_is_UNKNOWN_not_empty(monkeypatch):
    _table(monkeypatch, [REAL], rc=1)
    assert gh._alive_jobs_detail() is None


def test_a_raising_process_query_is_UNKNOWN_not_empty(monkeypatch):
    def boom(*a, **k):
        raise OSError("no powershell")
    monkeypatch.setattr(subprocess, "run", boom)
    assert gh._alive_jobs_detail() is None


def test_a_negative_age_is_recorded_as_unknown_not_as_a_negative_number(monkeypatch):
    """R802 finding 2: a skewed clock makes `[int]` of a TimeSpan return e.g. -3600, which used
    to be published and printed as '(-3600s old)'."""
    _table(monkeypatch, [dict(REAL, AgeS=-3600)])
    assert gh._alive_jobs_detail()[0]["age_s"] is None


# ------------------------------------------------------------------ what the beat carries
def _publish_body(monkeypatch, detail):
    captured = {}

    class _C:
        def put_object(self, **kw):
            captured["body"] = json.loads(kw["Body"].decode("utf-8"))

    monkeypatch.setattr(gh, "_alive_jobs_detail", lambda: detail)
    monkeypatch.setattr(gh, "_emptiness_verdict", lambda: {"ran": True, "fetch_without_write": 0})
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: _C())
    gh.publish()
    return captured["body"]


def test_a_young_process_STAYS_alive_and_its_age_is_published(monkeypatch):
    """R802 finding 1: an earlier version dropped young processes out of `jobs_alive` on a 60 s
    floor. cbs_nl is legitimately relaunched every ~68 min and publish runs ~2 s after a tick, so
    that called a healthy crawler dead about one beat in six. Report the age; judge nothing."""
    body = _publish_body(monkeypatch, [
        {"name": "ingest_cbs_nl.py", "pid": 111, "age_s": 7200},
        {"name": "ingest_istat_sliced.py", "pid": 444, "age_s": 9},
    ])
    assert body["jobs_alive"] == ["ingest_cbs_nl.py", "ingest_istat_sliced.py"]
    assert body["table_ok"] is True
    assert [d["age_s"] for d in body["jobs_detail"]] == [7200, 9]


def test_an_unreadable_table_is_published_as_UNKNOWN(monkeypatch):
    body = _publish_body(monkeypatch, None)
    assert body["table_ok"] is False and body["jobs_alive"] == []


def _check_out(capsys, **over):
    import datetime as dt
    import unittest.mock as m

    class _B:
        def __init__(self, r): self._r = r
        def read(self): return self._r

    payload = {
        "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "host": "H", "tracked": list(gh.TRACKED),
        "jobs_alive": list(gh.TRACKED),
        "jobs_detail": [{"name": "ingest_cbs_nl.py", "pid": 1, "age_s": 7200},
                        {"name": "ingest_gus_dbw.py", "pid": 2, "age_s": 33204},
                        {"name": "ingest_istat_sliced.py", "pid": 3, "age_s": 2}],
        "table_ok": True, "relaunch_window_s": 330,
        "emptiness": {"ran": True, "fetch_without_write": 0},
    }
    payload.update(over)

    class _C:
        def get_object(self, Bucket=None, Key=None):        # noqa: N803
            return {"Body": _B(json.dumps(payload).encode())}

    with m.patch.object(gh.r2_util, "client", lambda write=False: _C()):
        rc = gh.check(45.0)
    return rc, capsys.readouterr().out


def test_the_check_NAMES_the_job_that_was_just_restarted(capsys):
    """The end-to-end answer to R799: in the exact scenario that produced `3/3`, a reader now
    sees istat's age and is told to read its log."""
    rc, out = _check_out(capsys)
    assert rc == 0
    assert "RESTARTED within the last guard tick" in out
    assert "ingest_istat_sliced.py (2s old)" in out, out
    assert "ingest_istat_sliced.py (pid 3, 2s)" in out, out


def test_the_ages_are_printed_UNDER_the_line_they_annotate(capsys):
    _rc, out = _check_out(capsys)
    assert out.index("guard heartbeat OK") < out.index("RESTARTED within"), out


def test_an_unreadable_table_does_not_read_as_finished_or_dead(capsys):
    rc, out = _check_out(capsys, table_ok=False, jobs_alive=[], jobs_detail=[])
    assert rc == 0
    assert "UNKNOWN this tick, not empty" in out
    assert "finished or dead" not in out, out


def test_an_older_beat_without_the_new_fields_still_reads_cleanly(capsys):
    """A beat published before this change has no jobs_detail/table_ok. It must not crash the
    gate or be reported as unreadable."""
    rc, out = _check_out(capsys, jobs_detail=None, table_ok=None)
    assert rc == 0 and "guard heartbeat OK" in out


# ------------------------------------------------------------------ DISCRIMINATION CONTROL
def test_the_OLD_rule_gets_these_very_fixtures_WRONG():
    """The rule being replaced. If this stops failing the fixtures, the tests above have stopped
    measuring the change - which is exactly how R457 came back as R799."""
    def old_rule(rows):
        cmds = "".join(r["CommandLine"] for r in rows)
        return [t for t in gh.TRACKED if t in cmds]

    assert old_rule([ARG]) == ["ingest_cbs_nl.py"]
    assert old_rule([LONGER]) == ["ingest_cbs_nl.py"]
    assert old_rule([YOUNG]) == ["ingest_istat_sliced.py"]
    # the new rule rejects the two forgeries outright
    assert gh._script_of(ARG["CommandLine"]) not in gh.TRACKED
    assert gh._script_of(LONGER["CommandLine"]) not in gh.TRACKED
    # and still finds the real one
    assert gh._script_of(REAL["CommandLine"]) == "ingest_cbs_nl.py"
