"""A name match is not liveness (R457, reintroduced as R799 in the publisher that watches guard).

`_alive_jobs` joined EVERY python.exe command line into one string and asked `if t in cmds`. Two
consequences, both measured on the running system on 2026-09-06:

  * any process merely MENTIONING a tracked name counted - an audit script, an editor, a grep;
  * a process counted from its first second, so `ingest_istat_sliced.py` was matched ~2 s after
    the guard relaunched it, INSIDE the nine seconds it lived before its designed preflight exit
    (esploradati times out at TCP:443; sdmx.istat.it 302-redirects to itself). The beat CI reads
    published `jobs_alive=3/3` while istat had crawled nothing.

The last test is the DISCRIMINATION CONTROL: it runs the OLD rule over the same fixtures and
asserts it says the wrong thing. Without it these tests could pass for reasons unrelated to the
change - which is exactly how R457 came back.
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

# One row per process, in the shape `ConvertTo-Json` produces.
REAL = {"ProcessId": 111, "AgeS": 7200,
        "CommandLine": '"' + PY + '" jobs/ingest_cbs_nl.py'}
MENTION = {"ProcessId": 222, "AgeS": 7200,
           "CommandLine": '"' + PY + '" tools/audit_x.py --note ingest_cbs_nl.py-is-slow'}
LONGER = {"ProcessId": 333, "AgeS": 7200,
          "CommandLine": '"' + PY + '" jobs/my_ingest_cbs_nl.py'}
FLAPPING = {"ProcessId": 444, "AgeS": 9,
            "CommandLine": '"' + PY + '" "E:\\research\\x\\jobs\\ingest_istat_sliced.py"'}


class _Ran:
    def __init__(self, rows):
        self.stdout = json.dumps(rows if len(rows) != 1 else rows[0])
        self.stderr = ""


def _table(monkeypatch, rows):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _Ran(rows))


# ------------------------------------------------------------------ the token rule, in isolation
def test_a_script_path_yields_its_basename():
    assert gh._script_tokens('"' + PY + '" jobs/ingest_cbs_nl.py') == {"ingest_cbs_nl.py"}
    assert "ingest_cbs_nl.py" in gh._script_tokens(
        '"' + PY + '" "E:\\research\\econfindatalibrary\\jobs\\ingest_cbs_nl.py"')


def test_a_longer_name_containing_the_tracked_one_is_NOT_that_job():
    assert "ingest_cbs_nl.py" not in gh._script_tokens(PY + " jobs/my_ingest_cbs_nl.py")
    assert "ingest_cbs_nl.py" not in gh._script_tokens(PY + " jobs/ingest_cbs_nl.pyz")


def test_a_non_py_token_cannot_masquerade_as_the_script():
    assert gh._script_tokens(PY + " tools/a.py --source ingest_cbs_nl") == {"a.py"}


def test_an_unbalanced_quote_degrades_and_never_raises():
    assert "ingest_cbs_nl.py" in gh._script_tokens('"' + PY + ' jobs/ingest_cbs_nl.py')


# ------------------------------------------------------------------ the matcher over a table
def test_a_real_launch_is_detected(monkeypatch):
    _table(monkeypatch, [REAL])
    d = gh._alive_jobs_detail()
    assert [x["name"] for x in d] == ["ingest_cbs_nl.py"]
    assert d[0]["pid"] == 111 and d[0]["age_s"] == 7200


def test_a_MENTION_of_the_name_is_not_the_job_running(monkeypatch):
    _table(monkeypatch, [MENTION, LONGER])
    assert gh._alive_jobs_detail() == []


def test_an_unreadable_process_table_reports_NOTHING_alive_not_everything(monkeypatch):
    def boom(*a, **k):
        raise OSError("no powershell")
    monkeypatch.setattr(subprocess, "run", boom)
    assert gh._alive_jobs_detail() == []


def test_a_single_row_object_is_handled(monkeypatch):
    """ConvertTo-Json emits a bare object, not a list, when there is exactly one process."""
    _table(monkeypatch, [REAL])
    assert len(gh._alive_jobs_detail()) == 1


# ------------------------------------------------------------------ flapping vs working
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


def test_a_nine_second_relaunch_is_NOT_counted_as_a_working_crawler(monkeypatch):
    body = _publish_body(monkeypatch, [
        {"name": "ingest_cbs_nl.py", "pid": 111, "age_s": 7200},
        {"name": "ingest_istat_sliced.py", "pid": 444, "age_s": 9},
    ])
    assert body["jobs_alive"] == ["ingest_cbs_nl.py"], body["jobs_alive"]
    assert [f["name"] for f in body["jobs_flapping"]] == ["ingest_istat_sliced.py"]
    assert body["flap_floor_s"] == gh.FLAP_FLOOR_S


def test_a_long_running_crawler_counts_as_alive(monkeypatch):
    body = _publish_body(monkeypatch, [{"name": "ingest_cbs_nl.py", "pid": 1, "age_s": 60}])
    assert body["jobs_alive"] == ["ingest_cbs_nl.py"] and body["jobs_flapping"] == []


def test_an_unknown_age_is_treated_as_flapping_not_as_healthy(monkeypatch):
    """`age_s: None` means we could not tell. The safe reading is 'not proven working'."""
    body = _publish_body(monkeypatch, [{"name": "ingest_cbs_nl.py", "pid": 1, "age_s": None}])
    assert body["jobs_alive"] == []
    assert [f["name"] for f in body["jobs_flapping"]] == ["ingest_cbs_nl.py"]


def test_the_check_SAYS_flapping_rather_than_averaging_it_away(capsys):
    import datetime as dt

    class _B:
        def __init__(self, r): self._r = r
        def read(self): return self._r

    class _C:
        def get_object(self, Bucket=None, Key=None):        # noqa: N803
            return {"Body": _B(json.dumps({
                "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "host": "H", "tracked": list(gh.TRACKED),
                "jobs_alive": ["ingest_cbs_nl.py", "ingest_gus_dbw.py"],
                "jobs_flapping": [{"name": "ingest_istat_sliced.py", "age_s": 9}],
                "flap_floor_s": 60,
                "emptiness": {"ran": True, "fetch_without_write": 0},
            }).encode())}

    import unittest.mock as m
    with m.patch.object(gh.r2_util, "client", lambda write=False: _C()):
        rc = gh.check(45.0)
    out = capsys.readouterr().out
    assert rc == 0
    assert "FLAPPING" in out and "ingest_istat_sliced.py (9s old)" in out, out
    assert "jobs_alive=2/3" in out, out


# ------------------------------------------------------------------ DISCRIMINATION CONTROL
def test_the_OLD_rule_gets_these_very_fixtures_WRONG():
    """The rule being replaced: every command line joined, then a bare substring test. If this
    ever stops failing the fixtures, the tests above have stopped measuring the change."""
    def old_rule(rows):
        cmds = "".join(r["CommandLine"] for r in rows)
        return [t for t in gh.TRACKED if t in cmds]

    # a mere mention, and a longer filename, both counted as the job running
    assert old_rule([MENTION]) == ["ingest_cbs_nl.py"]
    assert old_rule([LONGER]) == ["ingest_cbs_nl.py"]
    # and a nine-second relaunch was indistinguishable from a working crawler
    assert old_rule([FLAPPING]) == ["ingest_istat_sliced.py"]
    # while the new rule rejects the first two outright
    assert gh._script_tokens(MENTION["CommandLine"]).isdisjoint(gh.TRACKED)
    assert gh._script_tokens(LONGER["CommandLine"]).isdisjoint(gh.TRACKED)
