"""A file that shrinks the work queue has to justify every line it removes.

`updater/discontinued.yaml` takes sources OUT of the queue that
`tools/audit_schedule_coverage.py` prints — the queue the loop works down, source by source. That
is exactly the shape of thing that becomes a rug: anything hard to build a fetcher for gets
quietly declared dead and the coverage number improves without a single series getting fresher.

So the entries are held to the standard the ledger keeps demanding of me. R381: a gap is not a
defect until you check whether it was a decision — and the inverse, a decision is not a fact until
someone measured it. R386: a claim in a comment (`_imf_direct.py` said "IMF retired IFS") is a
citation, not evidence; I only believed it after asking api.imf.org and counting 222 published
dataflows with 0 matching.

These tests enforce: every entry carries what was PROBED, what it FOUND, and WHEN; a source
cannot be declared dead and scheduled at the same time; and the audit keeps counting these series
as not-auto-updating, because they are not.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

PATH = os.path.join(ROOT, "updater", "discontinued.yaml")
REQUIRED = ("source_id", "publisher", "measured", "probe", "finding", "decision")


def _entries():
    if not os.path.exists(PATH):
        return []
    return (yaml.safe_load(open(PATH, encoding="utf-8")) or {}).get("sources") or []


def test_every_entry_carries_its_measurement():
    for e in _entries():
        missing = [k for k in REQUIRED if not str(e.get(k) or "").strip()]
        assert not missing, (
            f"{e.get('source_id')!r} is missing {missing}. An entry here removes a source from "
            f"the work queue; without the probe that was run and what it returned, that is a "
            f"verdict nobody can re-check.")
        assert isinstance(e["measured"], (dt.date, dt.datetime)), (
            f"{e['source_id']}: `measured` must be a real date, got {e['measured']!r}")
        assert len(str(e["finding"]).split()) >= 12, (
            f"{e['source_id']}: `finding` must state what was actually measured, with numbers — "
            f"'discontinued' restates the claim instead of supporting it")


def test_a_source_cannot_be_dead_and_scheduled_at_once():
    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))
    live = {s["source_id"] for s in reg["sources"] if s.get("live")}
    clash = sorted({e["source_id"] for e in _entries()} & live)
    assert not clash, (
        f"{clash} are declared publisher-discontinued AND live:true. One of the two is wrong: "
        f"either the publisher still ships it, or the orchestrator is burning runs on a dataset "
        f"that cannot move.")


def test_the_probe_names_a_real_endpoint_not_a_recollection():
    """'a previous session found' is how a citation gets laundered into evidence."""
    bad = [e["source_id"] for e in _entries()
           if not any(t in str(e["probe"]).lower() for t in ("http://", "https://", "get ", "curl"))]
    assert not bad, (
        f"{bad}: `probe` must name what was actually requested — a URL or command someone else "
        f"can re-run today. Prose about what was concluded earlier is not a measurement.")


def test_archival_series_are_still_counted_as_not_auto_updating():
    """The split must move sources between HEADINGS, never out of the totals. If archival series
    silently left the denominator, the coverage percentage would rise while nothing got fresher —
    the precise dishonesty this whole file risks introducing."""
    src = open(os.path.join(ROOT, "tools", "audit_schedule_coverage.py"), encoding="utf-8").read()
    assert 'print(f"NOT scheduled           {len(gap) + len(arch):>6,}   "' in src, (
        "the NOT-scheduled line no longer adds the archival sources back in")
    assert "{gap_series + arch_series:>12,} series" in src


def test_a_missing_file_does_not_shrink_the_queue():
    from tools.audit_schedule_coverage import discontinued
    assert isinstance(discontinued(), dict)
    src = open(os.path.join(ROOT, "tools", "audit_schedule_coverage.py"), encoding="utf-8").read()
    assert "if not os.path.exists(p):\n        return {}" in src, (
        "an absent discontinued.yaml must yield {} — never a default that hides work")
