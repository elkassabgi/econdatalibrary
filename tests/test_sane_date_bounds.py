# -*- coding: utf-8 -*-
"""`sane_date` must drop sentinels and KEEP real projections - it had started dropping both.

MAX_SANE_YEAR guards the generated site against parse damage: a period code misread as a year
gives 0002-07-31, and a publisher sentinel gives 9999-12-31, and neither belongs on a page. The
bound was 2126, chosen when that sat "in the empty gap ... far below the lowest sentinel".

The gap stopped being empty. bfs publishes a Swiss period life table titled
"Periodensterbetafeln 2023 fuer die Schweiz (1876-2150)" - 275 stored rows spanning 1876-12-31
to 2150-12-31, none outside 1000-2200 - and at 2126 `sane_date` returned None for its real end
date, so the page lost a true coverage bound. That is the OPPOSITE of what the guard is for, and
nothing would have caught it: dropping a date produces a page that merely looks tidier.

Measured over the whole catalogue before the bound moved: exactly 1 row sits in
(2126-12-31, 2200-01-01] - that projection - and exactly 2 sit above 2200, the eurostat
sentinels. So 2200 admits the true value and still rejects every sentinel, and it matches the
bound tools/audit_impossible_dates.py and tools/audit_catalogue_impossible_dates.py already use.

These are the REAL values from the catalogue, not invented ones, so the test fails if the bound
is moved back under a genuine projection or up over a real sentinel.
"""
from __future__ import annotations

import importlib.util
import os

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_GEN_SITE = os.path.join(_REPO, "catalog", "gen_site.py")


@pytest.fixture(scope="module")
def gs():
    spec = importlib.util.spec_from_file_location("gen_site_under_test", _GEN_SITE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_the_guard_is_actually_guarding(gs):
    """Positive control. If sane_date passed everything, every assertion below is vacuous."""
    assert gs.sane_date("9999-12-31") is None, "the 9999 sentinel is being served"
    # 0002-07-31 is a FABRICATED DATE, not a fabricated value: it is what the X000 branch
    # writes for CBS's `Standaardfout` member, whose measurement is real (R1079). The date
    # must not reach a page; the row behind it is data and must not be deleted.
    assert gs.sane_date("0002-07-31") is None, "a fabricated year-0002 date is being served"
    assert gs.sane_date(None) is None
    assert gs.sane_date("not-a-date") is None


def test_ordinary_dates_survive(gs):
    """Negative control: a guard that rejects everything would pass the test above."""
    for d in ("1876-12-31", "2026-06-30", "1913-01-01"):
        assert gs.sane_date(d) == d, d


def test_a_real_projection_is_kept(gs):
    """bfs's life table genuinely runs to 2150. Dropping it is the bug this bound caused."""
    assert gs.sane_date("2150-12-31") == "2150-12-31", (
        "a real 2150 projection was dropped; MAX_SANE_YEAR is back under a published horizon")


def test_the_bound_is_where_the_audits_put_it(gs):
    """2200 is not arbitrary - the two impossible-date audits already bound there."""
    assert gs.MAX_SANE_YEAR == 2200, gs.MAX_SANE_YEAR
    assert gs.sane_date("2200-01-01") == "2200-01-01"
    assert gs.sane_date("2201-01-01") is None


def test_the_low_bound_is_unchanged(gs):
    """Deep history stays rejected on the site - ggdc/maddison are handled by their own
    phrasing branch, not by widening this bound (see gen_site.py's coverage-row comment)."""
    assert gs.sane_date("0001-01-01") is None
    assert gs.sane_date("0730-01-01") is None
    assert gs.sane_date("1000-01-01") == "1000-01-01"
