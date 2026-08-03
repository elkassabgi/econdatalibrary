"""Regression gate: a numeric CLASSIFICATION dimension must never be taken as cso's time axis.

WHAT WENT WRONG. `is_time_dim` answers True when >=60% of a FIVE-VALUE sample matches
`^\\d{4}[MQHSAW]?\\d*$`, and CSO classification dimensions are full of numeric sentinel codes
(3001, 9998, 9999 for "not stated" / "all"). Selection was first-match-wins over the dimension
list, so a classification axis that merely appeared BEFORE the real TLIST axis was taken as
time and its codes were parsed as YEARS.

Measured on the live store 2026-08-03: 434,408 of cso's 48,960,271 rows (0.887%) across 11
files carry an obs_date beyond the year 2100 — 272,445 of them in 10_Census_2016.parquet, dated
9998-12-31. The keys show it plainly:

    CSO:B0726:...C02750V03319A=3001:...      -> 3001-12-31
    CSO:VSA10:TLIST(A1)=2019:STATISTIC=...   -> 2452-12-31

The second is the tell: TLIST(A1)=2019 sits in the KEY, which is where a dimension goes when it
was NOT chosen as the time axis. The real year was present and lost to an earlier numeric one.

These are not legitimate forward-dated projections. The health module documents real ones (ABS
to 2046, UN WPP to 2101) and they are data; a 2016 census table reporting the year 9998 is a
parse artifact, and it is SERVED — it reaches anyone who downloads that series and it poisons
the store's frontier.

WHAT IS PINNED HERE: an explicitly named time axis wins over any heuristic match, wherever it
sits in the dimension order; the heuristic still works for tables that name nothing; and the
absurd date never appears.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _mod():
    path = os.path.join(ROOT, "jobs", "ingest_cso_ireland.py")
    spec = importlib.util.spec_from_file_location("_cso_t", path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _cube(dim_order, *, role_time=None):
    """A 2-cell cube. `CLASS` carries CSO sentinel codes that look like years; TLIST carries
    the real period. Dimension ORDER is the parameter under test."""
    dims = {
        "C02750V03319A": {"label": "class",
                          "category": {"index": {"3001": 0, "9998": 1},
                                       "label": {"3001": "Not stated", "9998": "All"}}},
        "TLIST(A1)": {"label": "year",
                      "category": {"index": {"2019": 0, "2020": 1},
                                   "label": {"2019": "2019", "2020": "2020"}}},
    }
    payload = {"id": list(dim_order), "size": [2, 2],
               "dimension": {k: dims[k] for k in dim_order},
               "value": [1.0, 2.0, 3.0, 4.0]}
    if role_time:
        payload["role"] = {"time": role_time}
    return payload


def _years(rows):
    return sorted({r[1].year for r in rows})


def test_tlist_wins_even_when_a_numeric_class_dim_comes_first():
    m = _mod()
    rows = m.parse_jsonstat2(_cube(["C02750V03319A", "TLIST(A1)"]), "CSO:TEST")
    assert rows, "parsed nothing"
    assert _years(rows) == [2019, 2020], (
        f"expected the TLIST years, got {_years(rows)} — a classification dimension whose "
        f"codes are 3001/9998 was taken as the time axis")
    assert all(r[1].year < 2100 for r in rows), "an absurd year survived"


def test_tlist_still_wins_when_it_comes_first():
    m = _mod()
    rows = m.parse_jsonstat2(_cube(["TLIST(A1)", "C02750V03319A"]), "CSO:TEST")
    assert _years(rows) == [2019, 2020], _years(rows)


def test_jsonstat_role_time_is_authoritative():
    # A dimension named nothing time-like still wins if JSON-stat declares it.
    m = _mod()
    p = _cube(["C02750V03319A", "TLIST(A1)"], role_time=["TLIST(A1)"])
    assert _years(m.parse_jsonstat2(p, "CSO:TEST")) == [2019, 2020]


def test_heuristic_still_applies_when_nothing_is_named():
    # The sample heuristic is not removed — it is demoted. A table whose only date-shaped axis
    # has an opaque id must still parse, or this fix would trade one silent failure for another.
    m = _mod()
    p = {"id": ["OPAQUE", "M"], "size": [2, 1],
         "dimension": {
             "OPAQUE": {"label": "t", "category": {"index": {"2019": 0, "2020": 1},
                                                   "label": {"2019": "2019", "2020": "2020"}}},
             "M": {"label": "m", "category": {"index": {"X": 0}, "label": {"X": "v"}}}},
         "value": [7.0, 8.0]}
    assert _years(m.parse_jsonstat2(p, "CSO:TEST")) == [2019, 2020]


def test_the_absurd_year_is_what_regressed_before():
    """Negative control: with the OLD first-match rule this cube yielded years 3001/9998."""
    m = _mod()
    rows = m.parse_jsonstat2(_cube(["C02750V03319A", "TLIST(A1)"]), "CSO:TEST")
    assert not any(r[1].year in (3001, 9998) for r in rows), (
        "the classification codes came through as years — the exact defect that put "
        "434,408 rows beyond the year 2100 in the live store")
