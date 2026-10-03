"""The daily workstation run of sec_edgar after T0 (review AR-210).

The guard loop starts tools/selfhost/run_sec_edgar_local.ps1 -IfDue on every tick (Scheduled Tasks are blocked by
policy here: tools/machine/README.md); the runner calls `refresh_sec_edgar.py --local-only --apply`.

Before T0 it must do nothing: the CI job sec-edgar-daily writes R2 and D1 then, and a second writer would race it.
After T0 it must be the daily run - the one run allowed to move the scan mark - so every repair or test flag is
refused, and the flag is read ONCE. tools/guard_heartbeat.py reads the runner's status file and judges it."""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cutover  # noqa: E402
from tools import guard_heartbeat as GH  # noqa: E402
from tools import refresh_sec_edgar as R  # noqa: E402
from tools.selfhost import t0_ready as T  # noqa: E402


def _yesterday_utc():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat(timespec="seconds")


def _run(monkeypatch, cut, argv, last=None):
    seen = {}
    if callable(cut):
        monkeypatch.setattr(cutover, "is_cut_over", cut)
    else:
        monkeypatch.setattr(cutover, "is_cut_over", lambda: cut)
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py", *argv])
    monkeypatch.setattr(R, "_last_success_utc", lambda: last)
    monkeypatch.setattr(R, "_load_retry", lambda: {})
    monkeypatch.setattr(R, "ticker_map", lambda: {})

    def fake_filers(days, today=None):
        seen["days"] = days
        return {320193}, ["x"], []
    monkeypatch.setattr(R, "filers_since", fake_filers)

    def fake_local(a, todo, t2c, advance=False, stamp_at=None):
        seen["local"] = (todo, advance)
        return 0
    monkeypatch.setattr(R, "_refresh_local", fake_local)
    import core.r2_util as r2_util                     # noqa: PLC0415

    def no_r2(*a, **k):
        raise AssertionError("the R2 path was reached")
    monkeypatch.setattr(r2_util, "client", no_r2)
    return seen


# ---- refresh_sec_edgar.py --local-only -----------------------------------------------------------------------------
def test_before_t0_the_daily_run_does_nothing_and_exits_0(monkeypatch, capsys):
    seen = _run(monkeypatch, False, ["--local-only", "--apply"])
    assert R.main() == 0
    assert seen == {}, "before T0 it scans nothing and writes nothing"
    assert "not cut over" in capsys.readouterr().out


def test_after_t0_it_is_the_daily_run(monkeypatch):
    seen = _run(monkeypatch, True, ["--local-only", "--apply"], last=_yesterday_utc())
    assert R.main() == 0
    assert seen["local"] == ([320193], True), "the daily run reaches the local writer and may move the mark"
    assert seen["days"] == 1 + R.WATERMARK_OVERLAP_DAYS, "the window reaches back to the mark"


def test_the_flag_is_read_once_so_a_later_not_cut_over_never_reaches_r2(monkeypatch):
    """AR-210 finding 1: the gate read True (the flag, or a passing stat error that fail-closed reads as cut over),
    a later read said False, and the run took the pre-T0 branch and asked for an R2 client."""
    answers = iter([True])
    seen = _run(monkeypatch, lambda: next(answers, False), ["--local-only", "--apply"], last=_yesterday_utc())
    assert R.main() == 0
    assert seen["local"] == ([320193], True), "it stayed on the local path"


def test_after_t0_with_no_mark_yet_the_daily_run_refuses(monkeypatch):
    """The first local run is a manual one with --days wide enough to reach the last CI scan (T0 step 6)."""
    seen = _run(monkeypatch, True, ["--local-only", "--apply"], last=None)
    with pytest.raises(cutover.CutoverRefused):
        R.main()
    assert "local" not in seen


@pytest.mark.parametrize("flag", [["--ciks", "320193"], ["--limit", "5"], ["--days", "30"], ["--d1"], ["--audit"],
                                  ["--respan", "AAPL"], ["--force"]])
@pytest.mark.parametrize("cut", [False, True])
def test_a_repair_or_test_flag_is_refused_before_and_after_t0(monkeypatch, capsys, flag, cut):
    seen = _run(monkeypatch, cut, ["--local-only", "--apply", *flag])
    assert R.main() == 2
    assert seen == {}
    assert flag[0] in capsys.readouterr().out


# ---- the runner itself (run for real, against a fake refresher) ----------------------------------------------------
PS = shutil.which("powershell.exe")
needs_ps = pytest.mark.skipif(PS is None, reason="Windows PowerShell runs the runner on the workstation only")
FAKE = '''import json, os, sys
open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "calls.jsonl"), "a").write(
    json.dumps(sys.argv[1:]) + "\\n")
print("fake refresher", sys.argv[1:]); print("to stderr: \\u00c1", file=sys.stderr)
sys.exit(int(os.environ.get("FAKE_RC", "0")))
'''


@pytest.fixture
def box(tmp_path):
    """A copy of the runner in a tree with a fake tools/refresh_sec_edgar.py, a path with a space in it."""
    root = tmp_path / "live checkout"
    (root / "tools" / "selfhost").mkdir(parents=True)
    shutil.copy(os.path.join(ROOT, "tools", "selfhost", "run_sec_edgar_local.ps1"), root / "tools" / "selfhost")
    (root / "tools" / "refresh_sec_edgar.py").write_text(FAKE, encoding="utf-8")
    return root


def _runner(root, now, flag=True, rc=0, if_due=True):
    flag_path = root.parent / "CUTOVER"
    if flag:
        flag_path.write_text("")
    elif flag_path.exists():
        flag_path.unlink()
    cmd = [PS, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(root / "tools" / "selfhost" /
           "run_sec_edgar_local.ps1"), "-Python", sys.executable, "-FlagPath", str(flag_path), "-NowUtc", now]
    if if_due:
        cmd.insert(cmd.index("-Python"), "-IfDue")
    r = subprocess.run(cmd, cwd=os.environ.get("SystemRoot", "C:\\Windows"), capture_output=True, text=True,
                       env={**os.environ, "FAKE_RC": str(rc)}, timeout=120)
    calls = root / "calls.jsonl"
    n = len(calls.read_text(encoding="utf-8").splitlines()) if calls.exists() else 0
    status = root / "logs" / "sec_edgar_local.last.json"
    return r.returncode, n, (json.loads(status.read_text(encoding="ascii")) if status.exists() else None)


@needs_ps
def test_before_t0_the_guard_tick_starts_nothing_and_writes_nothing(box):
    assert _runner(box, "2026-10-05T09:00:00Z", flag=False) == (0, 0, None)
    assert not (box / "logs").exists()


@needs_ps
def test_a_due_run_calls_the_daily_run_once_and_records_it(box):
    rc, n, st = _runner(box, "2026-10-05T09:00:00Z")
    assert (rc, n) == (0, 1)
    args = json.loads((box / "calls.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert args == ["--local-only", "--apply"]
    assert st == {"started": "2026-10-05T09:00:00Z", "ended": "2026-10-05T09:00:00Z", "rc": 0,
                  "pid": st["pid"], "last_ok_started": "2026-10-05T09:00:00Z"}
    logs = sorted((box / "logs").glob("sec_edgar_local_*.log"))
    assert len(logs) == 1 and "fake refresher" in logs[0].read_text(encoding="utf-8")
    assert "\u00c1" in logs[0].read_text(encoding="utf-8"), "stderr is kept, as UTF-8"
    assert not list((box / "logs").glob("*.stderr")) and not (box / "logs" / "sec_edgar_local.lock").exists()
    assert _runner(box, "2026-10-05T15:00:00Z")[:2] == (0, 1), "not due again the same UTC day"
    assert _runner(box, "2026-10-06T08:00:00Z")[:2] == (0, 2), "due the next day at 08:00 UTC"


@needs_ps
def test_before_0800_utc_it_is_not_due(box):
    assert _runner(box, "2026-10-05T07:59:00Z")[:2] == (0, 0)


@needs_ps
def test_a_failed_run_returns_its_code_keeps_the_last_ok_and_retries_after_2h(box):
    _runner(box, "2026-10-04T09:00:00Z")
    rc, n, st = _runner(box, "2026-10-05T09:00:00Z", rc=3)
    assert (rc, n, st["rc"], st["last_ok_started"]) == (3, 2, 3, "2026-10-04T09:00:00Z")
    assert _runner(box, "2026-10-05T10:30:00Z")[:2] == (0, 2), "no retry inside 2 h"
    rc, n, st = _runner(box, "2026-10-05T11:01:00Z")
    assert (rc, n, st["rc"], st["last_ok_started"]) == (0, 3, 0, "2026-10-05T11:01:00Z")


@needs_ps
def test_a_lock_held_by_a_live_process_stops_a_second_run(box):
    (box / "logs").mkdir()
    (box / "logs" / "sec_edgar_local.lock").write_text(str(os.getpid()), encoding="ascii")
    assert _runner(box, "2026-10-05T09:00:00Z")[:2] == (0, 0)
    assert _runner(box, "2026-10-05T09:00:00Z", if_due=False)[:2] == (3, 0), "a manual run is refused loudly"
    (box / "logs" / "sec_edgar_local.lock").write_text("999999", encoding="ascii")          # a dead pid: stale
    assert _runner(box, "2026-10-05T09:00:00Z")[:2] == (0, 1)


@needs_ps
def test_a_missing_interpreter_is_a_failure_with_a_log_line(box):
    flag_path = box.parent / "CUTOVER"
    flag_path.write_text("")
    r = subprocess.run([PS, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
                        str(box / "tools" / "selfhost" / "run_sec_edgar_local.ps1"), "-IfDue",
                        "-Python", str(box / "no-such-python.exe"), "-FlagPath", str(flag_path),
                        "-NowUtc", "2026-10-05T09:00:00Z"], capture_output=True, text=True, timeout=120)
    st = json.loads((box / "logs" / "sec_edgar_local.last.json").read_text(encoding="ascii"))
    assert r.returncode == 98 and st["rc"] == 98 and st["last_ok_started"] is None
    assert "runner error" in next((box / "logs").glob("sec_edgar_local_*.log")).read_text(encoding="utf-8")


def test_the_runner_is_ascii_and_pins_the_interpreter():
    ps1 = open(os.path.join(ROOT, "tools", "selfhost", "run_sec_edgar_local.ps1"), encoding="utf-8").read()
    assert ps1.isascii(), "Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI (run_local_heavy.ps1 header)"
    assert "[string] $Python   = 'C:\\Users\\aelkassabgi\\AppData\\Local\\Programs\\Python\\Python314\\python.exe'" in ps1
    assert "[string] $FlagPath = 'C:\\ProgramData\\econ\\CUTOVER'" in ps1
    src = open(os.path.join(ROOT, "core", "cutover.py"), encoding="utf-8").read()
    assert 'FLAG_PATH = r"C:\\ProgramData\\econ\\CUTOVER"' in src, "the runner's default is the module's flag"


# ---- the guard starts it, and t0_ready checks that the live guard does ---------------------------------------------
GUARD_COPY = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy")


def test_the_tracked_guard_copy_starts_the_runner_with_if_due():
    ok, why = T.sec_edgar_runner(guard_path=GUARD_COPY)
    assert ok, why


def test_t0_ready_fails_a_guard_without_the_call_or_with_it_only_in_a_comment(tmp_path):
    g = tmp_path / "RELAUNCH_GUARD.ps1"
    g.write_text("# Start-Process ... run_sec_edgar_local.ps1 '-IfDue'\n$x = 1\n", encoding="utf-8")
    assert T.sec_edgar_runner(guard_path=str(g))[0] is False
    g.write_text("Start-Process 'powershell.exe' -ArgumentList @('-File','run_sec_edgar_local.ps1')\n", encoding="utf-8")
    assert T.sec_edgar_runner(guard_path=str(g))[0] is False, "without -IfDue it would run every 5 minutes"
    assert T.sec_edgar_runner(guard_path=str(tmp_path / "absent.ps1"))[0] is False
    assert ("sec-edgar-runner", T.sec_edgar_runner) in T.CHECKS


def test_the_owed_list_no_longer_carries_the_task_item():
    assert not [o for o in T.SEC_EDGAR_OWED if "refresh task" in o]


# ---- the heartbeat judges the status file after T0 -----------------------------------------------------------------
NOW = dt.datetime(2026, 10, 6, 12, 0, tzinfo=dt.timezone.utc)


@pytest.mark.parametrize("rec,bad", [
    (None, "no status file"),
    ({"unreadable": "JSONDecodeError: x"}, "unreadable"),
    ({"started": "2026-10-06T07:59:00Z", "ended": None, "rc": None, "pid": 7, "last_ok_started": "2026-10-05T08:00:00Z"},
     "never ended"),
    ({"started": "2026-10-06T09:00:00Z", "ended": "2026-10-06T09:02:00Z", "rc": 1, "pid": 7,
      "last_ok_started": "2026-10-05T08:00:00Z"}, "exited 1"),
    ({"started": "2026-10-05T08:00:00Z", "ended": "2026-10-05T08:02:00Z", "rc": 0, "pid": 7,
      "last_ok_started": "2026-10-05T08:00:00Z"}, "no successful daily run"),
    ({"started": "2026-10-06T08:00:00Z", "ended": "2026-10-06T08:02:00Z", "rc": 0, "pid": 7,
      "last_ok_started": "2026-10-06T08:00:00Z"}, None),
    ({"started": "2026-10-06T11:30:00Z", "ended": None, "rc": None, "pid": 7,
      "last_ok_started": "2026-10-06T08:00:00Z"}, None),
])
def test_after_t0_the_heartbeat_names_what_is_wrong(monkeypatch, rec, bad):
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    got = GH._sec_edgar_problem(rec, NOW)
    assert (got is None) if bad is None else (got is not None and bad in got), got


def test_before_t0_the_heartbeat_judges_nothing(monkeypatch):
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH._sec_edgar_problem(None, NOW) is None


def test_the_beat_carries_the_record_and_the_verdict(monkeypatch, tmp_path):
    st = tmp_path / "sec_edgar_local.last.json"
    st.write_text(json.dumps({"started": "2026-10-06T09:00:00Z", "ended": "2026-10-06T09:01:00Z", "rc": 2, "pid": 1,
                              "last_ok_started": None}), encoding="ascii")
    monkeypatch.setattr(GH, "SEC_EDGAR_STATUS_LOCAL", str(st))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    rec = GH._sec_edgar_beat()
    assert rec["rc"] == 2 and "exited 2" in GH._sec_edgar_problem(rec, NOW)
    src = open(os.path.join(ROOT, "tools", "guard_heartbeat.py"), encoding="utf-8").read()
    assert 'body["sec_edgar_local_problem"] = _sec_edgar_problem(sec, dt.datetime.now(dt.timezone.utc))' in src


def test_the_off_machine_check_fails_on_a_false_verdict(monkeypatch, capsys):
    import io                                                  # noqa: PLC0415
    import urllib.request                                      # noqa: PLC0415
    fresh = dt.datetime.now(dt.timezone.utc).isoformat()

    def answer(body):
        class Resp(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=60: Resp(json.dumps(body).encode()))
    answer({"utc": fresh, "table_ok": True, "jobs_alive": 1, "jobs_tracked": 1, "sec_edgar_local_ok": False})
    assert GH.check_url("https://e.example/v1/guard-heartbeat", 45) == 1
    assert "SEC_EDGAR LOCAL RUN NOT HEALTHY" in capsys.readouterr().out
    answer({"utc": fresh, "table_ok": True, "jobs_alive": 1, "jobs_tracked": 1, "sec_edgar_local_ok": True})
    assert GH.check_url("https://e.example/v1/guard-heartbeat", 45) == 0
