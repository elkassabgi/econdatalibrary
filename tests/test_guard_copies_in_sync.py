"""The reboot-survival scripts must match their versioned copies.

`tools/check_guard_copies.py` has existed, worked, and correctly reported DRIFTED for weeks,
and NOTHING RAN IT. On 2026-09-02 it found `RELAUNCH_GUARD.ps1` frozen 155 lines behind: the
versioned copy was missing `$STALL_HOURS`, the entire `.PAUSED` maintenance-sentinel block and
the whole two-tick CPU+I/O stall-kill path. `tools/machine/README.md` names that copy as the
restore source - "Copy each file back to its live location" - so a machine rebuild would have
restored a guard with no stall detector, reintroducing R457, where `istat_sliced` sat 15.5
hours as a dead orphan the guard read as alive.

It also still carried three finished `$longJobs` entries with no sentinels beside them, which
is how a resurrected `derive_noaa` spent $22.50/month paging 3,139 ListObjects per pass for
three weeks. `logs/` is gitignored, so the `.DONE` files that stop those jobs do not survive a
rebuild; the versioned copy is where that hazard actually lives.

A tool that reports a fault nobody reads is the same as no tool. This is the reading.

SCOPE. The live scripts are gitignored (`.gitignore:47`, `RELAUNCH_*.ps1`), so they do not
exist on a CI runner and the checker says so itself rather than failing. This test therefore
guards the WORKSTATION, which is the only machine where drift can be introduced, and skips
where the question is meaningless.
"""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKER = os.path.join(ROOT, "tools", "check_guard_copies.py")


def _live_files_present() -> bool:
    """True only when at least one live script exists - i.e. we are on the workstation."""
    sys.path.insert(0, ROOT)
    import importlib.util
    spec = importlib.util.spec_from_file_location("_cgc", CHECKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return any(os.path.exists(live) for _copy, live in mod.PAIRS)


def test_the_checker_exists_at_all():
    """Pinned separately: if the tool is renamed or deleted, this file must fail loudly rather
    than skip forever and look green."""
    assert os.path.exists(CHECKER), CHECKER


@pytest.mark.skipif(not os.path.exists(CHECKER), reason="checker missing")
def test_versioned_copies_match_the_live_scripts():
    if not _live_files_present():
        pytest.skip("no live guard scripts - not the workstation, nothing to compare")

    r = subprocess.run([sys.executable, CHECKER], cwd=ROOT,
                       capture_output=True, text=True, timeout=120)
    assert r.returncode == 0, (
        "the versioned copies have drifted from the live scripts. Restoring a drifted copy "
        "onto a rebuilt machine is how the stall detector and the .PAUSED sentinel get "
        "silently removed.\n\n" + (r.stdout or "") + (r.stderr or ""))


@pytest.mark.skipif(not os.path.exists(CHECKER), reason="checker missing")
def test_no_long_job_is_armed_in_the_versioned_copy():
    """The versioned copy is the restore source, and `logs/` is gitignored.

    A `$longJobs` entry in the copy is armed with NO `.DONE` sentinel beside it on any rebuilt
    machine, which is the exact state that produced the derive_noaa leak. Retired entries are
    kept as COMMENTS on purpose - the argv is the recipe for the next campaign - so this looks
    only at live, uncommented ones.
    """
    copy = os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy")
    if not os.path.exists(copy):
        pytest.skip("no versioned copy of RELAUNCH_GUARD.ps1")

    armed = []
    with open(copy, encoding="utf-8", errors="replace") as fh:
        for n, line in enumerate(fh, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if "@{ n = '" in stripped and "jobArgs" not in stripped:
                # crawler entries live in a different array; only flag ones inside $longJobs
                armed.append((n, stripped[:70]))

    # The crawler table also uses `@{ n = '...'`, so this is a coarse net; the assertion below
    # names what it found rather than asserting a bare count, so a legitimate future long job
    # produces a readable failure instead of a mystery.
    long_job_names = [x for x in armed
                      if any(k in x[1] for k in ("derive_", "rekey_", "backfill_"))]
    assert not long_job_names, (
        "uncommented long-job entries in the versioned copy, armed with no sentinel on a "
        "rebuilt machine: " + "; ".join(f"line {n}: {t}" for n, t in long_job_names))
