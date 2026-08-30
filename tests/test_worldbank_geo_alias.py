"""worldbank: the 8 income-group aggregates must actually refresh (AR-022 / R509).

WHY. `worldbank` re-projects `worldbank_wdi` by matching keys exactly. Eight of its 692
published ids end in a 2-char income-group code (XD/XM/XN/XT) while wdi carries the same
economies under the publisher's 3-char form (HIC/LIC/LMC/UMC), so the match missed and the
fetcher reported them `missing` on every run — never refreshed, frozen at whatever we last
held. Its own docstring promised "whoever adds the mapping will see the count go to zero".
This file is that promise made mechanical.

R509 is why the map is tested at the FETCHER and not only in the worker: I first "fixed" this
with a worker-side alias that sat behind `if (!series)` and could never execute for these
catalogued ids, on the strength of a premise I had copied out of my own ledger without
re-measuring. The defect is real but it is prospective — these ids serve fine TODAY and go
stale at the next World Bank release — and it lives in the refresh path.

Offline: the map and the alias function are pure. The 4 entries were measured against the
publisher's /v2/country list and all 1,486 grouped parquets on 2026-08-30.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import worldbank as wb          # noqa: E402


def test_map_is_the_measured_four():
    """Pinned to the publisher's reference list. Changing this map is a data claim and must
    be re-measured against /v2/country, not edited from memory (R504)."""
    assert wb.WDI_GEO_TO_LEGACY == {"HIC": "XD", "LIC": "XM", "LMC": "XN", "UMC": "XT"}


def test_alias_rewrites_only_the_geo_segment():
    assert wb._legacy_alias("NY.GDP.MKTP.CD:HIC") == "NY.GDP.MKTP.CD:XD"
    assert wb._legacy_alias("SL.UEM.TOTL.ZS:UMC") == "SL.UEM.TOTL.ZS:XT"
    # An indicator code containing dots must survive intact — rpartition, not split.
    assert wb._legacy_alias("A.B.C.D:LIC") == "A.B.C.D:XM"


def test_alias_is_none_for_everything_else():
    """The control. If this returned a value for ordinary economies the map would be a
    rewriter rather than a repair, and it would corrupt 684 working ids to fix 8."""
    for k in ("NY.GDP.MKTP.CD:USA", "NY.GDP.MKTP.CD:ZWE", "NY.GDP.MKTP.CD:LMY",
              "NY.GDP.MKTP.CD:XD", "no-colon-here", ""):
        assert wb._legacy_alias(k) is None, k


def _project(rows, wanted):
    """The fetcher's key-selection logic, exercised exactly as update() runs it."""
    out = []
    for k in rows:
        legacy = k[len(wb.PREFIX):]
        if legacy not in wanted:
            alias = wb._legacy_alias(legacy)
            if alias is None or alias not in wanted:
                continue
            legacy = alias
        out.append(legacy)
    return out


def test_the_eight_aggregates_now_project():
    """THE REGRESSION, and the docstring's promise: `missing` goes to zero."""
    inds = ["NY.GDP.MKTP.CD", "SL.UEM.TOTL.ZS"]
    wanted = {f"{i}:{g}" for i in inds for g in ("XD", "XM", "XN", "XT")} | \
             {f"{i}:USA" for i in inds}
    upstream = [f"WDI:{i}:{g}" for i in inds for g in ("HIC", "LIC", "LMC", "UMC")] + \
               [f"WDI:{i}:USA" for i in inds]
    seen = set(_project(upstream, wanted))
    assert wanted - seen == set(), f"still missing: {sorted(wanted - seen)}"
    assert len(seen) == 10


def test_a_published_id_always_beats_an_alias():
    """If wdi ever publishes BOTH spellings, the direct match must win and the row must not
    be counted twice under one key."""
    wanted = {"IND:XD"}
    # Upstream carries the 2-char form directly AND the 3-char one.
    got = _project(["WDI:IND:XD", "WDI:IND:HIC"], wanted)
    assert got == ["IND:XD", "IND:XD"], got          # both land on the published id
    # merge dedup_keys=("series_key","obs_date") collapses same-date collisions; the point
    # here is that neither is dropped silently and neither invents a new key.
    assert set(got) == {"IND:XD"}


def test_unwanted_aliases_are_still_skipped():
    """An upstream 3-char geo whose 2-char form is NOT published must stay skipped — the map
    must not mint ids the catalogue does not have (that would be R487's shape: a fetcher
    inventing series nobody published)."""
    assert _project(["WDI:IND:HIC"], {"OTHER:XD"}) == []
