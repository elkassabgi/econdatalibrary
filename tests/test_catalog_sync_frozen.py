"""Every catalogue-sync call site must stay behind CATALOG_SYNC_ENABLED.

WHY THIS IS A TEST AND NOT A NOTE. The sync was frozen in `updater-daily.yml` on 2026-08-31
(R542) and the leak was recorded as closed. `updater-heavy.yml` runs the SAME
`core/sync_catalog_d1.py` and was never gated, so half the class kept running. The bill is the
evidence: two successful updater-heavy runs on 2026-08-31 (15:38 and 20:25) against **D1
11,412,906 writes (~$11.41) and 2,805,188,474 reads (~$2.81)** that day, versus 678,127 writes
and 373,588,605 reads the day before. Roughly $14 of D1 in one day, and Ahmed found it on his
invoice rather than from anything we built (R557).

`series_fts` is `fts5(series_id UNINDEXED)`, so every id-scoped statement is a full scan of
~23.8M rows; re-sending a 13.5M-series catalogue is how that becomes billions of reads.

A prose rule did not hold this class together — a second call site appeared and nobody noticed.
This does, mechanically, for any call site added later.
"""
import os
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOWS = os.path.join(ROOT, ".github", "workflows")
GATE = "CATALOG_SYNC_ENABLED"
SYNC = "sync_catalog_d1"


def _workflow_files():
    if not os.path.isdir(WORKFLOWS):
        return []
    return [os.path.join(WORKFLOWS, f) for f in sorted(os.listdir(WORKFLOWS))
            if f.endswith((".yml", ".yaml"))]


def _sync_steps():
    """(workflow, job, step name, the step's `if`) for every step that runs the sync."""
    found = []
    for path in _workflow_files():
        with open(path, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        for job_name, job in (doc or {}).get("jobs", {}).items():
            for step in (job or {}).get("steps", []) or []:
                run = str(step.get("run") or "")
                if SYNC in run:
                    found.append((os.path.basename(path), job_name,
                                  step.get("name", "<unnamed>"), str(step.get("if") or "")))
    return found


def test_the_sync_is_actually_referenced_somewhere():
    """Guard the guard: if the script is renamed, this suite must not silently pass by
    finding nothing to check."""
    steps = _sync_steps()
    assert steps, (f"no workflow step runs {SYNC!r} — either it was renamed (update this "
                   f"test) or the call sites moved somewhere this test cannot see them")


@pytest.mark.parametrize("wf,job,name,cond", _sync_steps() or [("", "", "", "")])
def test_every_catalog_sync_call_site_is_gated(wf, job, name, cond):
    if not wf:
        pytest.skip("covered by test_the_sync_is_actually_referenced_somewhere")
    assert GATE in cond, (
        f"{wf} job {job!r} step {name!r} runs {SYNC} with `if: {cond or '(none)'}` — it is "
        f"NOT gated on {GATE}. An ungated catalogue sync re-sends the whole catalogue to D1; "
        f"on 2026-08-31 that cost ~$14 of D1 in one day (R557).")


def test_the_gate_is_off_by_default_in_every_call_site():
    """The gate must compare against '1', so an unset repo variable means OFF. A truthy check
    such as `vars.CATALOG_SYNC_ENABLED != ''` would re-enable on any value at all."""
    for wf, job, name, cond in _sync_steps():
        assert f"{GATE} == '1'" in cond.replace('"', "'"), (
            f"{wf} job {job!r} step {name!r} gates on {GATE} but not with `== '1'`: "
            f"{cond!r}. Unset must mean OFF.")
