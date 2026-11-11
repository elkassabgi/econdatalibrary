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


# --- WHERE the floor and the warning threshold are used (review AR-469 F2, the R1726 class) -------------------------
# A pinned default proves nothing if the comparison that uses it reads a literal. These read the two comparisons on
# $script:SinceLastHours in the runner and require each to read its variable.

_CMP = re.compile(r"\$script:SinceLastHours\s+-(lt|le|gt|ge)\s+(\([^()]*\)|[^()\s{]+)")


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


# The warning threshold, from data (AR-469 F3): stamped pass to stamped pass in the wrapper logs, 2026-09-01..11-11,
# 43 passes - median 30.1 h, longest NORMAL gap 73.5 h (09-26), and one outage of 120.0 h (10-31..11-05). The warning
# must sit above every normal gap and below the outage.
LONGEST_NORMAL_GAP_H = 73.5
OUTAGE_GAP_H = 120.0


def test_the_starvation_warning_sits_between_normal_gaps_and_an_outage():
    found = re.findall(r"^\s*\[int\]\s*\$StarvationWarnHours\s*=\s*(\d+)\s*,?\s*$", _runner(), flags=re.M)
    assert len(found) == 1, found
    w = int(found[0])
    assert LONGEST_NORMAL_GAP_H < w < OUTAGE_GAP_H, f"$StarvationWarnHours {w}"
    assert w > 2 * m.PS1_MIN_HOURS, "the old 2 x $MinHours rule fired below normal gaps"


# --- the pause flag (AR-469 F1): logs\local_heavy.pause holds every -IfDue tick --------------------------------------
import shutil  # noqa: E402
import subprocess  # noqa: E402

import pytest  # noqa: E402

POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")


def _sandbox(tmp_path, pause: bool):
    """A copy of the runner and the two files it dot-sources, in a repo of its own with an empty logs dir and NO
    stamp (so a pass is due) and no lister (so a tick that is not held stops at the lister's FATAL, touching
    nothing)."""
    tools = tmp_path / "tools"
    tools.mkdir()
    for n in ("run_local_heavy.ps1", "cadence_decision.ps1", "pass_priority.ps1"):
        shutil.copy2(ROOT / "tools" / n, tools / n)
    logs = tmp_path / "logs"
    logs.mkdir()
    if pause:
        (logs / "local_heavy.pause").write_text("Fed Z1 heal\n", encoding="ascii")
    r = subprocess.run([POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                        str(tools / "run_local_heavy.ps1"), "-IfDue"], capture_output=True, text=True,
                       timeout=180, cwd=str(tmp_path))
    return r, sorted(p.name for p in logs.iterdir())


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell (the workstation runs this script under it)")
def test_the_pause_flag_holds_a_due_tick(tmp_path):
    r, files = _sandbox(tmp_path, pause=True)
    assert r.returncode == 0, (r.returncode, r.stdout[-500:], r.stderr[-500:])
    assert "PAUSED:" in r.stdout and "local_heavy.pause" in r.stdout, r.stdout
    assert "local heavy updater starting" not in r.stdout, r.stdout
    assert files == ["local_heavy.pause"], f"a paused tick wrote {files}"      # no pass log, no lock


@pytest.mark.skipif(POWERSHELL is None, reason="needs PowerShell")
def test_negative_control_without_the_flag_the_same_tick_starts(tmp_path):
    r, files = _sandbox(tmp_path, pause=False)
    assert "PAUSED:" not in r.stdout
    assert "local heavy updater starting" in r.stdout, (r.stdout[-800:], r.stderr[-800:])
    assert any(f.startswith("local_heavy_") and f.endswith(".log") for f in files), files


def test_the_pause_is_checked_before_the_lock_and_the_stamp():
    text = _runner()
    i_if = text.index("if ($IfDue) {")
    i_pause = text.index("if (Test-Path $pauseFile)", i_if)
    i_lock = text.index("$lockFile", i_if)
    i_stamp = text.index("$script:SinceLastHours -lt $MinHours", i_if)
    assert i_if < i_pause < i_lock < i_stamp
    assert "$pauseFile = Join-Path $logDir 'local_heavy.pause'" in text
