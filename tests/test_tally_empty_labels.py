"""Empty sub-unit labels are COLLECTED and are NOT rendered into Result.error.

Both halves matter, and each records a defect that actually shipped.

COLLECTED, because five Tally methods take a `label` and only three recorded it. `empty_unit`
and `added_unit` dropped theirs, so authors at nine modules - bea, census, defillama, hagstofa,
stat_estonia, unsdg, wid, _imf_direct (imported by 105 fetchers) and _who_gho - had been passing
names into a void. `added_unit(0, label)` increments the same `empty` counter as `empty_unit`,
so if only one of the two recorded, `empty_ids` would be a silent subset of what `empty` counts.

NOT RENDERED, because the first version of this change did render them, and an adversarial review
showed why that is wrong (R680):

  - it could not fire for the source it was built for: `finalize` returns at `if tally.transient`
    first, and every recorded idb run is `partial / 10-11 of 40 sub-unit(s) transient-failed`;
    with no transient, 40 empties raise DefinitiveError at the `empty == attempted` guard instead
  - on the SUCCESS path `orchestrate.py` writes `Result.error` to `unit_state.last_error` and
    `runs.note` with no `_clip_err`, so the string is unbounded exactly where it is not clipped
  - `tools/gen_runbook.py` wraps it and takes `[:12]` lines, cutting at ~1,152 characters with no
    ellipsis - a truncation already visible in `docs/runbook/idb.md`

So the collection is the feature and the silence is deliberate. A future caller that wants the
names can read `tally.empty_ids`; nothing may put them in the note without solving the clipping.
"""
from __future__ import annotations

from updater.strategies.fetchers._common import Tally, finalize


def test_empty_unit_records_its_label() -> None:
    t = Tally()
    t.empty_unit("alpha/0001: no rows")
    t.empty_unit()                                    # unlabelled callers must still work
    assert t.empty == 2
    assert t.empty_ids == ["alpha/0001: no rows"]


def test_added_unit_zero_records_its_label_too() -> None:
    """It increments the same counter, so it must feed the same list or empty_ids is a subset."""
    t = Tally()
    t.added_unit(0, "beta/0002: nothing new")
    t.added_unit(5, "gamma/0003: five rows")
    t.empty_unit("delta/0004: no rows")
    assert t.empty == 2, "added_unit(0) counts as empty"
    assert t.added == 5
    assert t.empty_ids == ["beta/0002: nothing new", "delta/0004: no rows"]
    assert len(t.empty_ids) == t.empty, "every counted empty with a label must be collected"


def test_labels_are_not_rendered_into_the_note() -> None:
    """The note must not grow a list on a quiet tick — see the module docstring."""
    t = Tally()
    for i in range(3):
        t.empty_unit(f"quiet/{i:04d}: no rows")
    res = finalize(t, 1_000, None, source="unittest")
    assert res.error == "no new rows", res.error
    for probe in ("quiet/0000", "empty sub-units"):
        assert probe not in (res.error or ""), f"{probe!r} leaked into Result.error"


def test_an_idb_shaped_run_reports_its_transients_not_its_empties() -> None:
    """The shape every recorded idb run actually has: empties plus merge-guard transients.

    `finalize` returns at the transient branch, which is correct — a transient IS the actionable
    thing. This test exists so nobody re-adds an empties clause believing it will appear here.
    """
    t = Tally()
    for i in range(29):
        t.empty_unit(f"stuck/{i:02d}: no rows")
    for i in range(11):
        t.transient_unit(f"merge/{i:02d}: refused")
    res = finalize(t, 1_000, None, source="idb")
    assert res.status == "partial"
    assert "transient-failed" in res.error
    assert "stuck/00" not in res.error
