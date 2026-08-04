"""Regression gate: every stat_slovenia structural break names its table AND its kind.

WHAT WENT WRONG. On 2026-08-04 the run reported, in full:

    stat_slovenia: 1/85 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body
    (schema/structural break); existing data kept

and that is the entire record. One table out of 85 broke and nothing anywhere says WHICH.
hagstofa, on the same tick, reported its two by name:

    hagstofa: 2/1074 sub-unit(s) ... [1_natturufar/2_vedurfar/UMH11120.px,
                                      1_natturufar/2_vedurfar/UMH11165.px]

which is what let those two be probed against the live API in minutes. The mechanism was always
there — `Tally.structural_unit(label)` appends to `structural_ids`, and the note renders them —
stat_slovenia's three call sites just passed nothing.

WHY THE KIND, NOT JUST THE ID. The three sites are three DIFFERENT failures with three different
fixes, and the old message collapsed them into one sentence:

    metadata envelope gone        -> the PxWeb /table endpoint stopped returning `variables`
    time axis parses to no dates  -> the axis exists but no code on it is a date (the 05L1027S
                                     class, R331/R334 — this is the one that fabricates years
                                     when it falls through to another dimension)
    non-empty body parsed 0 rows  -> the value array is real but the shape changed under us

Naming only the table would still leave the reader to re-derive which of the three it was.

CLASSIFICATION IS DELIBERATELY UNCHANGED. All three stay STRUCTURAL. This is purely about saying
which table and which break, so the next reader has something to act on (cf.
test_cso_failure_naming.py, same defect class in cso's transient path).
"""
from __future__ import annotations
import io
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers._common import Tally      # noqa: E402

MODULE = os.path.join(ROOT, "updater", "strategies", "fetchers", "stat_slovenia.py")

# The three labels the fetcher emits, as substrings. Kept here so a rename has to be deliberate.
KINDS = ["metadata envelope gone",
         "time axis parses to no dates",
         "non-empty body parsed 0 rows"]


def _ids(t: Tally):
    return list(getattr(t, "structural_ids", []) or [])


def _code_lines(path: str) -> str:
    """Source with whole-line `#` comments removed.

    A bare `tally.structural_unit()` written inside a comment would otherwise fail the gate
    below, and — worse — a real one could be masked by counting comment text. R329 was exactly
    this: a pattern counted in text that was not the code being checked.
    """
    out = []
    for ln in io.open(path, encoding="utf-8").read().splitlines():
        if ln.lstrip().startswith("#"):
            continue
        out.append(ln)
    return "\n".join(out)


def test_tally_keeps_the_structural_label_it_is_given():
    """The mechanism the fix rides on. If this stops holding, naming silently reverts."""
    t = Tally()
    t.structural_unit("05L1027S: time axis parses to no dates")
    t.structural_unit("05W2010S: non-empty body parsed 0 rows")
    assert t.structural == 2
    assert _ids(t) == ["05L1027S: time axis parses to no dates",
                       "05W2010S: non-empty body parsed 0 rows"]


def test_an_unnamed_structural_break_is_indistinguishable():
    """Negative control — the old behaviour. Three causes collapse to one indistinguishable
    record, which is why '1/85' could not be acted on."""
    t = Tally()
    t.structural_unit()
    t.structural_unit()
    assert t.structural == 2
    assert not [i for i in _ids(t) if i], (
        "unnamed breaks now carry ids — this control is meaningless, check Tally")


def test_no_unnamed_structural_call_remains_in_the_fetcher():
    """Grep the real module. A Tally-only test passes even if the fetcher reverts, because the
    fetcher is the thing that forgets to pass the label."""
    bare = _code_lines(MODULE).count("tally.structural_unit()")
    assert bare == 0, (
        f"{bare} unnamed tally.structural_unit() call(s) left in stat_slovenia.py — each one is "
        f"a break nobody can act on")


def test_all_three_break_kinds_are_present_and_distinct():
    """The point is discrimination. If two sites ever share a label the note stops answering
    'which break', which is most of what it is for."""
    src = _code_lines(MODULE)
    for kind in KINDS:
        assert kind in src, f"break kind {kind!r} is gone from stat_slovenia.py"
    assert len(set(KINDS)) == len(KINDS)


def test_every_structural_call_site_passes_a_label_containing_the_table_id():
    """Each call must interpolate the table id, not just a constant string. `tid_clean` is the
    id that also forms the store prefix (SI:<tid_clean>), so it is the one that lets a reader go
    straight from the note to the stored rows."""
    src = _code_lines(MODULE)
    calls = re.findall(r"tally\.structural_unit\(([^)]*)\)", src)
    assert calls, "no structural_unit call sites found — did the fetcher change shape?"
    for c in calls:
        assert "tid_clean" in c, (
            f"structural_unit({c}) does not name the table; a bare kind is not actionable")
