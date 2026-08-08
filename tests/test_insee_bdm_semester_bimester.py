"""insee_bdm's parse_period must understand every TIME_PERIOD format BDM actually publishes.

THE DEFECT THIS PINS (found closing #44, 2026-08-08). Two BDM flows were absent from our
store — ENQ-CONJ-COM-GROS and ENQ-CONJ-TRES-IND — although the API served both fine (HTTP
200, 18.5 MB / 839 KB payloads). Their observations parse to None because they publish ONLY
semester ('####-S#', 11,388 obs) and bimester ('####-B#', 199,558 obs) periods, formats
parse_period did not know; every row was dropped and both flows ingested as "0 obs" run
after run. A parser gap on an unknown period format is indistinguishable in the logs from an
empty dataset — the ingester said "0 obs", not "199,558 obs I could not date".

Convention pinned here: period-START, like the existing Q branch (Q1->Jan). S1->Jan, S2->Jul;
B1->Jan, B2->Mar, .. B6->Nov.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs.ingest_insee_bdm import parse_period  # noqa: E402


def test_semesters_parse_to_period_start():
    assert parse_period("2024-S1") == dt.date(2024, 1, 1)
    assert parse_period("2024-S2") == dt.date(2024, 7, 1)


def test_bimesters_parse_to_period_start():
    assert parse_period("2024-B1") == dt.date(2024, 1, 1)
    assert parse_period("2024-B3") == dt.date(2024, 5, 1)
    assert parse_period("2024-B6") == dt.date(2024, 11, 1)


def test_quarters_still_parse_the_same():
    """Regression control: the S/B branches sit above the generic YYYY-MM branch — the
    existing formats must be untouched."""
    assert parse_period("2024-Q1") == dt.date(2024, 1, 1)
    assert parse_period("2024-Q4") == dt.date(2024, 10, 1)
    assert parse_period("2024-07") == dt.date(2024, 7, 1)
    assert parse_period("2024") == dt.date(2024, 12, 31)
    assert parse_period("2024-07-15") == dt.date(2024, 7, 15)


def test_out_of_range_ordinals_stay_none():
    """S3 and B7 do not exist; a typo'd period must drop, not silently date to January."""
    assert parse_period("2024-S3") is None
    assert parse_period("2024-B7") is None
    assert parse_period("2024-B0") is None


def test_fetcher_copy_agrees_with_the_ingester():
    """The daily fetcher (updater/strategies/fetchers/insee_bdm.py) carries its OWN copy
    of the parser under a docstring claiming they are identical — and when the ingester
    grew S/B support the copy did not, so the two new flows would have merged 0 rows on
    every future tick while the ingest snapshot aged. Pin the pair on every format either
    one understands, so the next divergence fails here instead of freezing a flow."""
    from updater.strategies.fetchers.insee_bdm import _parse_period
    cases = [
        "2024-Q1", "2024-Q4", "2024-S1", "2024-S2", "2024-B1", "2024-B3", "2024-B6",
        "2024-01", "2024-12", "2024", "2024-07-15",
        "2024-S3", "2024-B7", "2024-B0", "garbage", "",
    ]
    for s in cases:
        assert parse_period(s) == _parse_period(s), (
            f"parse_period({s!r}) diverges: ingester={parse_period(s)!r} "
            f"fetcher={_parse_period(s)!r}")
