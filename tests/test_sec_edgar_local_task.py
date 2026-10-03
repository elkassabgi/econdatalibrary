"""The scheduled workstation run of sec_edgar (tools/selfhost/run_sec_edgar_local.ps1 calls
`refresh_sec_edgar.py --local-only --apply`).

Before T0 it must do nothing: the CI job sec-edgar-daily writes R2 and D1 then, and a second writer would race it.
After T0 it must be the daily run - the one run allowed to move the scan mark - so every repair or test flag is
refused. The task may therefore be registered before T0 and start working at the flag."""
from __future__ import annotations

import datetime as dt
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cutover  # noqa: E402
from tools import refresh_sec_edgar as R  # noqa: E402


def _yesterday_utc():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat(timespec="seconds")


def _run(monkeypatch, cut, argv, last=None):
    seen = {}
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

    def no_r2():
        raise AssertionError("the R2 path was reached")
    monkeypatch.setattr(r2_util, "client", no_r2)
    return seen


def test_before_t0_the_scheduled_run_does_nothing_and_exits_0(monkeypatch, capsys):
    seen = _run(monkeypatch, False, ["--local-only", "--apply"])
    assert R.main() == 0
    assert seen == {}, "before T0 it scans nothing and writes nothing"
    assert "not cut over" in capsys.readouterr().out


def test_after_t0_the_scheduled_run_is_the_daily_run(monkeypatch):
    seen = _run(monkeypatch, True, ["--local-only", "--apply"], last=_yesterday_utc())
    assert R.main() == 0
    assert seen["local"] == ([320193], True), "the daily run reaches the local writer and may move the mark"
    assert seen["days"] == 1 + R.WATERMARK_OVERLAP_DAYS, "the window reaches back to the mark"


def test_after_t0_with_no_mark_yet_the_scheduled_run_refuses(monkeypatch):
    """The first local run is a manual one with --days wide enough to reach the last CI scan (T0 step 6); until
    it has moved the mark the scheduled run refuses loudly instead of guessing a window."""
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


def test_the_runner_script_calls_the_local_only_daily_run_with_the_pinned_python():
    ps1 = open(os.path.join(ROOT, "tools", "selfhost", "run_sec_edgar_local.ps1"), encoding="utf-8").read()
    assert ps1.isascii(), "Windows PowerShell 5.1 reads a BOM-less .ps1 as ANSI (run_local_heavy.ps1 header)"
    calls = [ln for ln in ps1.splitlines() if "-ArgumentList" in ln]
    assert len(calls) == 1, calls
    assert "@('-B', '-u', 'tools\\refresh_sec_edgar.py', '--local-only', '--apply')" in calls[0]
    assert not re.search(r"--(ciks|limit|days|force|d1|audit|respan)\b", calls[0])
    assert "Python314\\python.exe" in ps1


def test_the_registration_script_is_ascii_has_a_whatif_and_runs_the_runner():
    ps1 = open(os.path.join(ROOT, "tools", "selfhost", "register_sec_edgar_task.ps1"), encoding="utf-8").read()
    assert ps1.isascii()
    assert "[switch] $WhatIf" in ps1 and "[switch] $Unregister" in ps1
    assert "run_sec_edgar_local.ps1" in ps1
    assert "-MultipleInstances IgnoreNew" in ps1 and "-StartWhenAvailable" in ps1
    assert "Register-ScheduledTask" in ps1
