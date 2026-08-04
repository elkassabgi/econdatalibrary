"""A publisher's `time: true` says WHICH axis is time — not that we can read it.

THREE cases pull in different directions and step 1 of `resolve_time_dim` must satisfy all three.

1. MIS-FLAGGED AXIS (the defect). SURS marks `time: true` on the AGE dimension of stat_slovenia's
   05L1027S, "Deaths by DEATHS (COMPLETED YEAR / YEAR OF BIRTH) - TOTAL and SEX":

       '1000' -> 'Deaths - TOTAL'     '000' -> 'Age 0'
       '001'  -> 'Age 0, year of birth 2025'

   '1000' parses to year 1000 and three served rows were dated to it.

2. POSITIONAL TIME CODES (must keep working). Some tables index time as '0','1','2' and carry the
   period only in `category.label` — Hagstofa SJA01101 has codes ['0','1',...] against valueTexts
   ['2010','2011',...]. The parsers fall back to labels, and that fallback is what fixed
   hagstofa's 26 false "structural breaks". A codes-ONLY sanity check makes those axes look
   unreadable and kills it: an earlier attempt at this guard failed 7 tests across
   ssb/hagstofa/statfin/dst for exactly that reason.

3. UNPARSEABLE BUT CORRECT AXIS (must not fall through). scb's `Tid` held '2011-2012' and
   '2025V01', which no grammar read until R331. Falling through to the value scan handed the
   table to `Region`, whose municipality codes 0114..2584 became years 114..2026 across 87,358
   rows. The publisher was right about which axis was time; we were wrong about how to read it.

So the rule is: judge the flagged axis on CODES **or LABELS**; if neither yields a sane date,
return None — never a different dimension. And it applies ONLY when the caller supplies labels,
so the 23 existing call sites keep their exact behaviour until each is migrated deliberately.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core.pxweb import resolve_time_dim  # noqa: E402


def _annual(s):
    """The permissive 4-digit-year shape every source ships — the reason a bare code list can
    become a calendar."""
    s = (s or "").strip()
    return dt.date(int(s), 12, 31) if len(s) == 4 and s.isdigit() else None


# 05L1027S as SURS actually publishes it.
AGE_IDS = ["UMRLI", "SPOL"]
AGE_CODES = [["1000"] + [f"{i:03d}" for i in range(304)], ["0", "1", "2"]]
AGE_LABELS = [["Deaths - TOTAL"] + [f"Age {i}" for i in range(304)],
              ["Sex - TOTAL", "Men", "Women"]]


def test_misflagged_age_axis_is_rejected_when_labels_are_supplied():
    idx = resolve_time_dim(AGE_IDS, AGE_CODES, meta_time_code="UMRLI",
                           parse_fn=_annual, dim_labels=AGE_LABELS)
    assert idx is None, (
        f"resolved {AGE_IDS[idx]!r} as time. '1000' is labelled 'Deaths - TOTAL'; neither its "
        f"codes nor its labels are dates."
    )


def test_same_cube_keeps_legacy_behaviour_when_labels_are_NOT_supplied():
    """The compatibility guarantee, asserted rather than assumed.

    23 call sites pass no labels. They must behave exactly as before this change, so the
    migration can proceed one source at a time instead of as a flag day.
    """
    idx = resolve_time_dim(AGE_IDS, AGE_CODES, meta_time_code="UMRLI", parse_fn=_annual)
    assert idx == 0, "a caller without dim_labels must get the old unconditional behaviour"


def test_positional_codes_with_dates_in_LABELS_are_still_accepted():
    """Case 2 — the one that broke the first attempt at this guard."""
    ids = ["Manudur", "Ar"]
    codes = [["0", "1", "2"], [str(i) for i in range(15)]]          # positional, parse to nothing
    labels = [["Jan", "Feb", "Mar"], [str(y) for y in range(2010, 2025)]]   # periods live here
    idx = resolve_time_dim(ids, codes, meta_time_code="Ar", parse_fn=_annual, dim_labels=labels)
    assert idx is not None and ids[idx] == "Ar", (
        "a positional time axis whose periods are in the LABELS was rejected — that is exactly "
        "the fallback that fixed hagstofa's 26 false structural breaks."
    )


def test_unreadable_flagged_axis_returns_None_not_another_dimension():
    """Case 3 — scb pre-R331. Neither codes nor labels are readable; the answer is None."""
    ids = ["Region", "Tid"]
    codes = [[f"{i:04d}" for i in range(114, 2585)],       # municipality codes, ALL 4-digit
             ["2011-2012", "2011-2013", "2025V01"]]
    labels = [[f"Region {i}" for i in range(114, 2585)],
              ["2011-2012", "2011-2013", "2025V01"]]
    idx = resolve_time_dim(ids, codes, meta_time_code="Tid", parse_fn=_annual, dim_labels=labels)
    assert idx is None, (
        f"resolved {ids[idx]!r}. Falling through to Region is how 0114..2584 became years "
        f"114..2026 across 87,358 rows."
    )


def test_a_readable_flagged_axis_still_wins_over_a_numeric_lookalike():
    """The positive control — the guard must not have disabled step 1 (R318)."""
    ids = ["Region", "Tid"]
    codes = [[f"{i:04d}" for i in range(114, 2585)], ["2023", "2024", "2025"]]
    labels = [[f"Region {i}" for i in range(114, 2585)], ["2023", "2024", "2025"]]
    idx = resolve_time_dim(ids, codes, meta_time_code="Tid", parse_fn=_annual, dim_labels=labels)
    assert idx is not None and ids[idx] == "Tid"


def test_a_few_sentinels_among_real_years_do_not_disqualify_an_axis():
    """Hagstofa's `Ar` carries climate normals (3000..3005) beside 1949..2024; statfin carries a
    9999. The rule is "at least one sane date", not "every code parses"."""
    ids = ["Ar", "Station"]
    codes = [[str(y) for y in range(1949, 2025)] + ["3001", "9999"], ["0", "1"]]
    labels = [[""] * 78, ["", ""]]
    idx = resolve_time_dim(ids, codes, meta_time_code="Ar", parse_fn=_annual, dim_labels=labels)
    assert idx is not None and ids[idx] == "Ar", (
        "an axis with a couple of sentinels among 76 real years was rejected; that would empty "
        "every hagstofa weather table."
    )
