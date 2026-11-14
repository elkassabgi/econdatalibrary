"""The desktop cadence floor: run_local_heavy.ps1's $MinHours, as production passes it, is the value the replay models.

$MinHours is a scheduler of its own (ledger R1037): the hours since the last stamped pass ENDED before the next one may
start. It moved from 20 to 14 on 2026-11-11 on the strength of `tools/measure_ci_lag.py replay61` (the PR that moved
it carries the table). Three copies of the number exist and must agree, or the replay measures a schedule production
does not run and tools/derive_csv_bulk.py's clear refuses (or allows) on the wrong clock:

  * tools/run_local_heavy.ps1   `[int] $MinHours = N`  - the default; the reboot guard passes only -IfDue, so the
                                                          default IS what production runs
  * tools/measure_ci_lag.py     PS1_MIN_HOURS          - the replay's "today" (ReplayConfig().min_hours)
  * tools/derive_csv_bulk.py    LOCAL_MIN_HOURS        - the durable clear's "is a pass due" check

Each pin has a negative control, and the last test runs the floor through the replay itself.
"""
import datetime as dt
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT))

import measure_ci_lag as m  # noqa: E402
from tools import derive_csv_bulk as dcb  # noqa: E402

RUNNER = ROOT / "tools" / "run_local_heavy.ps1"
GUARD = ROOT / "tools" / "machine" / "RELAUNCH_GUARD.ps1.workstation-copy"
DAILY, HEAVY = "updater-daily.yml", "updater-heavy.yml"


def _ps1_min_hours(text: str) -> int:
    """The $MinHours DEFAULT in the runner's param block. Exactly one declaration, or this raises."""
    found = re.findall(r"^\s*\[int\]\s*\$MinHours\s*=\s*(\d+)\s*,?\s*$", text, flags=re.M)
    assert len(found) == 1, f"expected one `[int] $MinHours = N` declaration, found {found}"
    return int(found[0])


def _guard_overrides(text: str) -> list:
    """Every run_local_heavy.ps1 launch in the guard that passes -MinHours (empty = production uses the default)."""
    return [ln.strip() for ln in text.splitlines()
            if "-ArgumentList" in ln and "$heavy" in ln and re.search(r"-MinHours", ln, flags=re.I)]


def test_the_three_copies_agree():
    ps1 = _ps1_min_hours(RUNNER.read_text(encoding="utf-8", errors="replace"))
    assert ps1 == m.PS1_MIN_HOURS == m.ReplayConfig().min_hours == dcb.LOCAL_MIN_HOURS, (
        f"$MinHours {ps1}, measure_ci_lag.PS1_MIN_HOURS {m.PS1_MIN_HOURS}, ReplayConfig().min_hours "
        f"{m.ReplayConfig().min_hours}, derive_csv_bulk.LOCAL_MIN_HOURS {dcb.LOCAL_MIN_HOURS}: move them together, "
        f"after re-running `measure_ci_lag.py replay61`")


def test_negative_control_a_moved_default_is_seen():
    text = RUNNER.read_text(encoding="utf-8", errors="replace")
    other = m.PS1_MIN_HOURS + 6
    mutated = re.sub(r"(\[int\]\s*\$MinHours\s*=\s*)\d+", lambda x: x.group(1) + str(other), text, count=1)
    assert mutated != text
    assert _ps1_min_hours(mutated) == other != m.PS1_MIN_HOURS


def test_production_passes_the_default():
    """The reboot guard starts the runner with -IfDue and nothing else that touches the floor, and no other launcher
    in tools/ passes -MinHours to it; so the ps1 default is the value production runs."""
    guard = GUARD.read_text(encoding="utf-8", errors="replace")
    launch = [ln for ln in guard.splitlines() if "-ArgumentList" in ln and "'-IfDue'" in ln]
    assert launch and "$heavy" in guard, "cannot find the guard's run_local_heavy.ps1 launch"
    assert _guard_overrides(guard) == [], _guard_overrides(guard)
    callers = []
    for p in (ROOT / "tools").rglob("*"):
        if p.is_file() and p.suffix.lower() in (".ps1", ".cmd", ".bat", ".py", ".yml") or p.name.endswith(
                "workstation-copy"):
            if p == RUNNER:
                continue
            for ln in p.read_text(encoding="utf-8", errors="replace").splitlines():
                if "run_local_heavy" in ln and re.search(r"-MinHours\b", ln):
                    callers.append(f"{p.relative_to(ROOT)}: {ln.strip()}")
    assert callers == [], callers


def test_negative_control_a_guard_override_is_seen():
    guard = GUARD.read_text(encoding="utf-8", errors="replace")
    mutated = guard.replace("'-IfDue')", "'-IfDue','-MinHours','20')", 1)
    assert mutated != guard
    assert len(_guard_overrides(mutated)) == 1


# --- the floor through the replay ----------------------------------------------------------------------------------
# One planted week on the schedule IN FORCE (daily 06:00Z; heavy 03:00Z and 12:00Z), every cron with its run: heavy
# 03:00 for 60 min, daily 06:00 for 120 min, heavy 12:00 for 380 min (ends 18:20Z - the measured 11-07 / 11-08 ends
# were 18:19Z and 18:31Z). The last stamped pass ended 02:30Z. With the production floor the next pass starts the
# moment the 12:00Z run ends; with the old 20 h it waits until 22:30Z, the 250 minutes this change recovers.

def _T(s):
    return dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)


_ids = iter(range(1, 10 ** 6))


def _run(wf, created, exec_min):
    c = _T(created)
    return m.CloudRun(wf, next(_ids), "schedule", c, "completed", "success", float(exec_min), 0.5, float(exec_min),
                      True, 1, c, c + dt.timedelta(minutes=exec_min), True)


def _week():
    rs = []
    for d in range(2, 9):
        day = f"2026-11-{d:02d}"
        rs += [_run(HEAVY, f"{day}T03:00", 60), _run(DAILY, f"{day}T06:00", 120), _run(HEAVY, f"{day}T12:00", 380)]
    return rs


def _first_pass(cfg):
    book = m.UnitBook({"u": "daily"}, {}, [], {"u": 30.0}, {}, (), ())
    tmpl = {DAILY: _run(DAILY, "2026-11-01T06:00", 120), HEAVY: _run(HEAVY, "2026-11-01T03:00", 60)}
    rp = m.Replay(cfg, _week(), book, _T("2026-11-07T02:40"), _T("2026-11-08T02:00"), {DAILY: 8.0, HEAVY: 2.0}, tmpl,
                  _T("2026-11-07T02:30"), None).run()
    assert rp.passes, "no desktop pass at all in the planted day"
    return rp.passes[0]["start"]


def test_the_production_floor_starts_when_the_noon_heavy_run_ends():
    prod = _ps1_min_hours(RUNNER.read_text(encoding="utf-8", errors="replace"))
    assert _first_pass(m.ReplayConfig(policy="today", min_hours=prod, backoff_h=8.0)) == _T("2026-11-07T18:20")
    assert _first_pass(m.ReplayConfig(policy="today", backoff_h=8.0)) == _T("2026-11-07T18:20")   # the default


def test_negative_control_the_old_floor_waits_until_2230():
    assert _first_pass(m.ReplayConfig(policy="today", min_hours=20, backoff_h=8.0)) == _T("2026-11-07T22:30")


# --- WHERE the floor and the warning threshold are used (review AR-469 F2 / R2-F4, the R1726 class) ----------------
# A pinned default proves nothing if the comparison that uses it reads a literal. These read EVERY comparison on
# SinceLastHours in the runner - with or without the script: scope, which at script scope are the same variable
# (AR-469 R2-F4) - and require exactly the two that read their variables.

_CMP = re.compile(r"\$(?:script:)?SinceLastHours\s+-(lt|le|gt|ge)\s+(\([^()]*\)|[^()\s{]+)")


def _comparisons(text: str) -> list:
    return [(op, rhs) for op, rhs in _CMP.findall(text)]


def _runner():
    return RUNNER.read_text(encoding="utf-8-sig", errors="replace")


def test_the_ifdue_floor_and_the_warning_read_their_variables():
    cmp = _comparisons(_runner())
    assert sorted(cmp) == [("ge", "$StarvationWarnHours"), ("lt", "$MinHours")], cmp


def test_negative_control_a_hard_coded_floor_is_seen():                    # mutant M4
    text = _runner()
    bad = text.replace("$script:SinceLastHours -lt $MinHours", "$script:SinceLastHours -lt 20", 1)
    assert bad != text
    assert ("lt", "20") in _comparisons(bad) and ("lt", "$MinHours") not in _comparisons(bad)


def test_negative_control_a_hard_coded_warning_is_seen():                  # mutant M5
    text = _runner()
    bad = text.replace("$script:SinceLastHours -ge $StarvationWarnHours", "$script:SinceLastHours -ge (2 * 20)", 1)
    assert bad != text
    assert ("ge", "(2 * 20)") in _comparisons(bad) and ("ge", "$StarvationWarnHours") not in _comparisons(bad)


def test_negative_control_an_unscoped_second_warning_rule_is_seen():       # AR-469 round 2 mutant M6
    text = _runner()
    old = "$script:SinceLastHours -ge $StarvationWarnHours"
    bad = text.replace(old, old + " -or $SinceLastHours -ge (2 * $MinHours)", 1)
    assert bad != text
    assert ("ge", "(2 * $MinHours)") in _comparisons(bad)
    assert sorted(_comparisons(bad)) != [("ge", "$StarvationWarnHours"), ("lt", "$MinHours")]


# The warning threshold, from the cron table IN FORCE only (AR-469 R2-F5). Wrapper logs 2026-11-05..11-11: hours since
# the last stamp when a pass started were 20.0-20.4 and once 41.9 (after the 11-09 pass did not stamp); stamped pass to
# stamped pass 23.9-24.1 and once 47.8. The older, longer gaps (to 73.5 h, and 120 h) were gate starvation on the
# previous table - what this warning exists to report - so they are not counted as healthy. The warning must sit above
# every healthy tick on this table and fire before updater/health.py's route_silence (ROUTE_SILENCE_DAYS).
LONGEST_HEALTHY_SINCE_AT_START_H = 41.9
LONGEST_HEALTHY_STAMP_GAP_H = 47.8


def _warn_hours() -> int:
    found = re.findall(r"^\s*\[int\]\s*\$StarvationWarnHours\s*=\s*(\d+)\s*,?\s*$", _runner(), flags=re.M)
    assert len(found) == 1, found
    return int(found[0])


def _threshold_problems(w: int) -> list:
    from updater import health
    out = []
    if not w >= LONGEST_HEALTHY_STAMP_GAP_H > LONGEST_HEALTHY_SINCE_AT_START_H:
        out.append(f"{w} h fires on a healthy tick of the table in force")
    if not w < health.ROUTE_SILENCE_DAYS * 24:
        out.append(f"{w} h fires after route_silence ({health.ROUTE_SILENCE_DAYS * 24:g} h)")
    if not w > 2 * m.PS1_MIN_HOURS:
        out.append(f"{w} h is not above the old 2 x $MinHours rule")
    return out


def test_the_starvation_warning_sits_above_healthy_ticks_and_before_route_silence():
    assert _threshold_problems(_warn_hours()) == []


def test_negative_control_the_threshold_pin_rejects_84_and_28():
    assert _threshold_problems(84) and _threshold_problems(28)


def test_no_pause_flag_in_the_runner():
    """AR-469 round 2 R2-F1: a pause flag that nothing expires silently retires the route; it was removed."""
    assert "local_heavy.pause" not in _runner()


# --- the starvation warning FIRES (AR-469 round 3, N1) --------------------------------------------------------------
# The pins above check the comparison's text. This runs the real runner under PowerShell in a sandbox repo: a stamp
# 49 h old (so -IfDue is due and past $StarvationWarnHours), a fake lister and a fake core.cutover (not cut over), and
# a fake tools/ci_writer_gate.py that refuses (exit 3). The tick must print the WARNING and then ABORT with exit 2.
# The negative control is the same tick at 47 h: it aborts the same way, with no WARNING.
import shutil  # noqa: E402
import subprocess  # noqa: E402

import pytest  # noqa: E402

POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")


def _gate_refused_tick(tmp_path, hours_since_stamp: float):
    tools = tmp_path / "tools"
    tools.mkdir()
    for n in ("run_local_heavy.ps1", "cadence_decision.ps1", "pass_priority.ps1"):
        shutil.copy2(ROOT / "tools" / n, tools / n)
    (tools / "_list_local_sources.py").write_text("print('fake_source')\n", encoding="ascii")
    (tools / "ci_writer_gate.py").write_text(
        "import sys\nprint('BLOCK: fake gate, a cloud writer is running')\nsys.exit(3)\n", encoding="ascii")
    core = tmp_path / "core"
    core.mkdir()
    (core / "__init__.py").write_text("", encoding="ascii")
    (core / "cutover.py").write_text("def is_cut_over():\n    return False\n", encoding="ascii")
    logs = tmp_path / "logs"
    logs.mkdir()
    stamp = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours_since_stamp)
    (logs / "local_heavy.last_success").write_text(stamp.strftime("%Y-%m-%dT%H:%M:%S.0000000Z") + "\n",
                                                   encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                        str(tools / "run_local_heavy.ps1"), "-IfDue"], capture_output=True, text=True,
                       timeout=180, cwd=str(tmp_path))
    return r


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell (the workstation runs this script under it)")
def test_the_starvation_warning_fires_on_a_refused_tick_past_the_threshold(tmp_path):
    r = _gate_refused_tick(tmp_path, 49.0)
    out = r.stdout
    assert r.returncode == 2, (r.returncode, out[-800:], r.stderr[-800:])
    assert "BLOCK: fake gate" in out, out[-800:]                      # the fake gate was really asked
    assert re.search(r"WARNING: no stamped pass for 49 h", out), out[-800:]
    assert "ABORT: a cloud state writer is running" in out, out[-800:]
    assert "The gate has refused every attempt" not in out             # AR-469 N3: the old wording is gone


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_negative_control_the_same_refused_tick_at_47_h_does_not_warn(tmp_path):
    r = _gate_refused_tick(tmp_path, 47.0)
    out = r.stdout
    assert r.returncode == 2, (r.returncode, out[-800:], r.stderr[-800:])
    assert "BLOCK: fake gate" in out and "ABORT: a cloud state writer is running" in out, out[-800:]
    assert "WARNING: no stamped pass" not in out, out[-800:]
