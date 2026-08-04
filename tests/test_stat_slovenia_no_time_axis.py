"""A cross-tabulation with NO time axis must not be reported as a structural break.

WHAT THIS PROTECTS. SURS publishes census CROSS-TABULATIONS alongside its time series. 05W is
"Families, census 2002 by SETTLEMENT and FAMILIES": its dimensions are NASELJA (6,152 settlement
codes, '0001'..'6152') and DRUŽINE, and PxWeb marks `time` on NEITHER. It is not a time series and
never was.

Before 2026-08-04 the fetcher could not say so. `_parse_jsonstat2` returns nothing for such a
cube, and the caller then reads "0 rows + on-disk history + a non-empty value array" as a data
shape that changed under us -> `structural_unit()`. It fires on every run, and since a source with
a failing sub-unit never sets `last_success_utc` (R231), stat_slovenia could never report success.
The live run 30879564906 said exactly this:

    stat_slovenia: 4/4 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body

That is the same defect that made ons_uk unable to succeed ONCE in its history: 287 of its 337
"datasets" were Cantabular cross-tabs, downloaded and discarded every run (task #89).

The discriminator is `_time_var_index`, which is the SAME shared resolver `_parse_jsonstat2` keys
obs_date on — so the two cannot disagree about whether a time axis exists (R333). The check must
stay tight: a table that HAS a date-bearing axis and still yields nothing IS a real break, and
this test asserts that case still resolves an axis (i.e. the fix did not simply widen the gate —
R318, where I loosened a working gate for a defect that did not exist).
"""
from __future__ import annotations
import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers.stat_slovenia import _time_var_index  # noqa: E402


def _var(code, values, time=False):
    v = {"code": code, "values": [str(x) for x in values]}
    if time:
        v["time"] = True
    return v


def test_census_cross_tab_has_no_time_axis():
    """05W's real shape, from SURS metadata: settlements + families, `time` on neither."""
    variables = [
        _var("NASELJA", [f"{i:04d}" for i in range(1, 6153)]),
        _var("DRUŽINE", ["1"]),
    ]
    assert _time_var_index(variables) is None, (
        "A 6,152-code settlement axis was accepted as time. Those codes are what produced "
        "obs_date years 1..6152 across 506,605 rows."
    )


def test_real_code_axes_are_rejected_on_their_PARSE_RATE_not_their_look():
    """Code axes are rejected because most of the list is out of band, not because codes 'look
    like' codes. Both real ranges are here, and both must fail the rate test.

    THE LIMIT THIS DOES NOT TEST, stated so nobody adds an assertion demanding it: an axis whose
    codes ALL fall inside 1500..2200 is information-theoretically indistinguishable from a real
    annual series by VALUE alone — 701 four-digit numbers in the calendar window are a plausible
    annual axis, full stop. Nothing here can resolve that; only the publisher's `time` flag or
    the dimension name can. Neither real range has that shape (settlements 1..6152 -> 9.8%
    in-band; municipalities 114..2584 -> 28.4%), so the rate test is sufficient in practice, and
    an earlier draft of this file asserted the impossible instead. R334.
    """
    slovenian_settlements = [_var("NASELJA", [f"{i:04d}" for i in range(1, 6153)]),
                             _var("DRUŽINE", ["1"])]
    assert _time_var_index(slovenian_settlements) is None, "NASELJA 1..6152 accepted as time"

    swedish_municipalities = [_var("Region", [f"{i:04d}" for i in range(114, 2585)]),
                              _var("ContentsCode", ["000001GY"])]
    assert _time_var_index(swedish_municipalities) is None, (
        "Region 0114..2584 accepted as time — that axis is what dated 87,358 scb rows to "
        "years 114..2026 (R331)."
    )


def test_a_real_time_axis_is_still_found():
    """The gate must not have been widened into uselessness (R318).

    05W1605S is one of the ten tables inside 05W that ARE real: it carries a LETO (= YEAR)
    dimension holding Slovenia's census years. Those 1,463 rows are genuine and a repair that
    dropped them would be data loss, so this is the positive control.
    """
    variables = [
        _var("MERITVE", ["1", "2"]),
        _var("NARODNA PRIPADNOST", [str(i) for i in range(35)]),
        _var("LETO", ["1953", "1961", "1971", "1981", "1991", "2002"]),
    ]
    idx = _time_var_index(variables)
    assert idx is not None, "the LETO (year) axis was not found — a re-pull would write 0 rows"
    assert variables[idx]["code"] == "LETO", f"resolved {variables[idx]['code']!r}, expected LETO"


def test_an_authoritative_time_flag_still_wins():
    """PxWeb's own `time: true` remains authoritative even beside a numeric-looking axis."""
    variables = [
        _var("Region", ["0114", "0115", "2584"]),          # codes that parse as bare years
        _var("Tid", ["2020", "2021", "2022"], time=True),
    ]
    idx = _time_var_index(variables)
    assert idx is not None and variables[idx]["code"] == "Tid", (
        f"resolved {variables[idx]['code'] if idx is not None else None!r}; the authoritative "
        f"time flag must beat a numeric code axis — this is scb's 0114..2584 failure (R331)."
    )
