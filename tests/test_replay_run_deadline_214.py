"""The starvation replay's pass timing against PR #214 (review of the replay: item 4).

Since #214 run_local_heavy.ps1 derives the updater's budget AND its hard stop after pull-state, from the cloud
writer's block INSTANT read before the pull (tools/cadence_decision.ps1 Get-PostPullBudget):
    run deadline = readAt + untilBlock - 5 - 19 - 1 - 10,  budget = floor(deadline - launch),  hard stop = deadline + 10
and it asks the same rule before the pull with a 3-min pull, and skips the updater after the pull below 20 min.
run_once then fixes _RUN_DEADLINE_TS = launch + budget and its start gate compares the CLOCK with it
(orchestrate.start_gate_refuses). The replay used the rule before #214: budget = untilBlock - 25 counted from the
updater launch (so the pull moved the deadline later), hard stop = budget + 10, no pre-pull check, no skip.

What is checked here:
  - post_pull_budget (the replay's port) equals the REAL PowerShell function on a grid (where PowerShell exists;
    CI runs on Linux and skips it, said so), and its constants equal the .ps1 parameter defaults (everywhere);
  - pencil cases of the formula and of replayed passes: the budget, the kill at the block-derived hard stop, the
    pre-pull refusal and the post-pull skip (nothing pushed, nothing stamped);
  - the replay's gate goes through orchestrate.start_gate_refuses (shared, not copied).
"""
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import measure_ci_lag as m  # noqa: E402
from updater import orchestrate  # noqa: E402

POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")
PS1 = os.path.join(ROOT, "tools", "cadence_decision.ps1")
READ_AT = dt.datetime(2026, 10, 31, 1, 57, tzinfo=dt.timezone.utc)
UNTIL = [45, 57, 58, 60, 65, 90, 120, 200, 300, 2000, 4000]
PULL = [0.0, 2.0, 2.5, 3.0, 7.3, 36.1]
CURRENT = [None, 50]                       # None = the ps1's clamp to untilBlock - 25 (capped at 2880); 50 = lower


def _cur(ub, c):
    return min(m.PS1_BUDGET_CAP, ub - m.PS1_MARGIN_MIN) if c is None else c


def _grid():
    return [(ub, p, _cur(ub, c)) for ub in UNTIL for p in PULL for c in CURRENT]


@pytest.mark.skipif(POWERSHELL is None, reason="no PowerShell on this host (CI is Linux): the formula check is not run")
def test_post_pull_budget_equals_the_powershell_function_on_a_grid(tmp_path):
    cases = tmp_path / "cases.txt"
    cases.write_text("\n".join(f"{ub},{p},{c}" for ub, p, c in _grid()), encoding="ascii")
    script = tmp_path / "run.ps1"
    script.write_text(
        ". '" + PS1.replace("'", "''") + "'\n"
        "$read = [DateTime]::Parse('" + READ_AT.strftime("%Y-%m-%dT%H:%M:%SZ") + "', "
        "[Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AdjustToUniversal)\n"
        "foreach ($ln in Get-Content '" + str(cases).replace("'", "''") + "') {\n"
        "  $f = $ln.Split(',')\n"
        "  $o = Get-PostPullBudget -UntilReadAtUtc $read -UntilBlockMin ([double]$f[0]) "
        "-NowUtc $read.AddMinutes([double]$f[1]) -CurrentBudgetMin ([double]$f[2])\n"
        "  Write-Output ('' + $ln + '|' + $o.Run + '|' + $o.BudgetMin + '|' + $o.RemainingMin + '|' + "
        "$o.HardStopUtc.ToString('yyyy-MM-ddTHH:mm:ss.fff'))\n"
        "}\n", encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)],
                       capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, (r.stdout + r.stderr)[-2000:]
    got = [ln.split("|") for ln in r.stdout.splitlines() if "|" in ln]
    assert len(got) == len(_grid()), r.stdout[-2000:]
    bad = []
    for (key, run, budget, remaining, stop), (ub, p, c) in zip(got, _grid()):
        py = m.post_pull_budget(READ_AT, ub, READ_AT + dt.timedelta(minutes=p), c)
        want = (str(py["run"]), str(py["budget"]), str(py["remaining"]),
                py["hard_stop"].strftime("%Y-%m-%dT%H:%M:%S.") + f"{py['hard_stop'].microsecond // 1000:03d}")
        if (run, budget, remaining, stop) != want:
            bad.append((key, (run, budget, remaining, stop), want))
    assert not bad, f"{len(bad)} of {len(got)} cases differ (case, powershell, replay): {bad[:5]}"
    # the grid has both answers and both hard-stop forms, or it proves nothing
    runs = {m.post_pull_budget(READ_AT, ub, READ_AT + dt.timedelta(minutes=p), c)["run"] for ub, p, c in _grid()}
    assert runs == {True, False}


def test_the_ports_constants_are_the_ps1_parameter_defaults():
    body = open(PS1, encoding="utf-8").read()
    fn = body[body.index("function Get-PostPullBudget"):body.index("function Test-CadenceShouldStamp")]
    defaults = {k: float(v) for k, v in re.findall(r"\[double\]\s+\$(\w+)\s*=\s*([0-9.]+)", fn)}
    assert defaults == {"GranularityMin": m.PS1_G2_GRANULARITY_MIN, "PushAllowanceMin": m.PS1_G2_PUSH_ALLOWANCE_MIN,
                        "KillLagMin": m.PS1_G2_KILL_LAG_MIN, "OverrunMin": m.PS1_G2_OVERRUN_MIN,
                        "MinBudgetMin": m.PS1_G2_MIN_BUDGET_MIN}, defaults
    runner = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8", errors="replace").read()
    pre = runner[runner.index("$pre = Get-PostPullBudget"):runner.index("if (-not $pre.Run)")]
    assert f"UtcNow.AddMinutes({m.PS1_G2_PRE_PULL_MIN})" in pre and "-CurrentBudgetMin" in pre
    assert "$marginMin = " + str(m.PS1_MARGIN_MIN) in runner


@pytest.mark.parametrize("ub, pull, cur, run, budget, stop_after_read", [
    (65, 36.1, 40, False, -7, 40.0),          # the 2026-10-31 pass: floor(65 - 35 - 36.1) = -7 -> skipped
    (200, 36.1, 175, True, 128, 175.0),       # #214's own example: budget 128, hard stop t + 175
    (200, 2.0, 175, True, 163, 175.0),        # an ordinary pull: 200 - 35 - 2
    (200, 2.0, 50, True, 50, 62.0),           # a lower caller budget keeps its own stop: 2 + 50 + 10
    (58, 3.0, 33, True, 20, 33.0),            # the smallest window that runs after a 3-min pull
    (57, 3.0, 32, False, 19, 32.0),
])
def test_post_pull_budget_pencil(ub, pull, cur, run, budget, stop_after_read):
    o = m.post_pull_budget(READ_AT, ub, READ_AT + dt.timedelta(minutes=pull), cur)
    assert (o["run"], o["budget"]) == (run, budget)
    assert m._mins(READ_AT, o["hard_stop"]) == pytest.approx(stop_after_read)


# ------------------------------------------------------------------ replayed gate passes

def _gate(monkeypatch, ub, units, pull=2.0, rows=None):
    """Replay._gate_attempt at T with --until-block answering `ub`, the cloud neutralised; a real unit book."""
    t = m._parse("2026-10-01T12:00:00Z")
    b = m.UnitBook({s: "monthly" for s in units}, {}, rows or [], dict(units), {})
    r = object.__new__(m.Replay)
    r.cfg = m.ReplayConfig(policy="today", pull_min=pull, push_min=7.0)
    r.book, r.passes, r.skipped = b, [], []
    r.busy_until, r.last_success, r.last_noprog = t, t - dt.timedelta(days=3), None
    r._inflight = lambda: False
    r._cron_pending = lambda x: False
    monkeypatch.setattr(m, "until_block_fast", lambda x: ub)
    return t, r, r._gate_attempt(t, 72.0)


def test_a_gate_pass_kills_at_the_block_derived_hard_stop(monkeypatch):
    # ub 200 at 12:00, pull 2.5: deadline 12:00 + 165 = 14:45, updater 12:02:30 -> budget floor(162.5) = 162; the
    # hard stop is the block-derived 14:55 = 172.5 min after the launch, NOT budget + 10 = 172 (the floor's half
    # minute stays with the unit). A 300-min unit (reserve min(90, 3 x 100) = 90) is admitted and killed there.
    # Before #214: budget 175, killed at 185.
    rows = [("2026-09-01T00:00:00+00:00", "g", "_all", "ok", 6000.0)]
    t, r, verdict = _gate(monkeypatch, 200, {"g": 300.0}, rows=rows, pull=2.5)
    p = r.passes[0]
    assert verdict == "pass" and p["budget"] == 162.0 and p["hard_stop"] == "g"
    assert p["used_min"] == pytest.approx(172.5)
    assert p["end"] == t + dt.timedelta(minutes=2.5 + 172.5 + 7)


def test_a_unit_is_refused_against_the_deadline_not_the_old_budget(monkeypatch):
    # ub 120, pull 2: budget floor(120 - 35 - 2) = 83. A unit with a 90-min reserve is refused (it was admitted on
    # the pre-#214 budget of 95); a 10-min unit runs.
    rows = [("2026-09-01T00:00:00+00:00", "g", "_all", "ok", 6000.0),
            ("2026-09-01T00:00:00+00:00", "s", "_all", "ok", 300.0)]
    _t, r, _v = _gate(monkeypatch, 120, {"g": 100.0, "s": 5.0}, rows=rows)
    p = r.passes[0]
    assert p["budget"] == 83.0 and [x[0] for x in p["ran"]] == ["s"] and p["budget_skipped"] == [("g", 90.0)]


def test_the_pre_pull_check_refuses_without_a_pull(monkeypatch):
    # untilCron 32 (ub 57) passes the old 20-min check, but floor(57 - 35 - 3) = 19 < 20 -> refused before the pull
    _t, r, verdict = _gate(monkeypatch, 57, {"u": 10.0})
    assert verdict == "short" and not r.passes and not r.skipped
    _t, r, verdict = _gate(monkeypatch, 58, {"u": 10.0})                   # control: 20 -> a pass
    assert verdict == "pass" and r.passes[0]["budget"] == 21.0              # 58 - 35 - 2 after the real pull


def test_a_slow_pull_skips_the_updater_and_pushes_nothing(monkeypatch):
    # ub 60: the pre-pull check says floor(60 - 35 - 3) = 22 -> pull. The pull takes 10: floor(60 - 35 - 10) = 15 < 20
    # -> SKIPPING UPDATER AFTER PULL-STATE: no updater, no push, no stamp; busy until the pull ends
    t, r, verdict = _gate(monkeypatch, 60, {"u": 10.0}, pull=10.0)
    assert verdict == "pass" and not r.passes and len(r.skipped) == 1
    assert r.busy_until == t + dt.timedelta(minutes=10) and r.last_noprog is None
    assert r.last_success == t - dt.timedelta(days=3) and r.book.attempts == {}


def test_the_replay_gate_is_the_production_comparison(monkeypatch):
    """Shared, not copied: a replaced orchestrate.start_gate_refuses decides the replay's admissions."""
    t = m._parse("2026-10-01T00:00:00Z")
    rows = [("2026-09-01T00:00:00+00:00", "a", "_all", "ok", 300.0)]
    b = m.UnitBook({"a": "monthly"}, {}, rows, {"a": 5.0}, {})
    seen = []

    def refuse(now_ts, need, deadline, single=False):
        seen.append((now_ts, need, deadline))
        return True
    monkeypatch.setattr(orchestrate, "start_gate_refuses", refuse)
    res = b.run_pass(t, 100.0)
    assert res["ran"] == [] and res["budget_skipped"] == [("a", 15.0)]          # min(90, max(3 x 5, 10))
    assert seen == [(t.timestamp(), 15.0, (t + dt.timedelta(minutes=100)).timestamp())]


def test_start_gate_refuses_is_run_onces_comparison():
    assert orchestrate.start_gate_refuses(1000.0, 10.0, 1600.0) is False          # 1000 + 600 == 1600: admitted
    assert orchestrate.start_gate_refuses(1000.1, 10.0, 1600.0) is True
    assert orchestrate.start_gate_refuses(1000.1, 10.0, 1600.0, single_source=True) is False
    assert orchestrate.start_gate_refuses(1e12, 10.0, None) is False                # no ceiling
    import inspect
    src = inspect.getsource(orchestrate.run_once)
    assert "start_gate_refuses(_now, _need_min, run_deadline, bool(sources and len(sources) == 1))" in src


def test_json_metric_counts_the_post_pull_skips(monkeypatch):
    t, r, _v = _gate(monkeypatch, 60, {"u": 10.0}, pull=10.0)
    r.cfg, r.start, r.end, r.leases, r.done, r.evicted = r.cfg, t, t + dt.timedelta(days=1), [], [], []
    met = m.replay_metrics(r, 10.0, [])
    assert met["post_pull_skips"] == 1 and met["passes"] == 0
    json.dumps({k: v for k, v in met.items() if k != "_replay"}, default=str)
