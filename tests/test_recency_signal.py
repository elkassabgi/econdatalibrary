"""newest_obs must never be the death date of dead series (run 32816867502).

A per-series cursor holds one date — that series' LAST period. In a source whose
every LIVING series ends in a projection (unctad_pop*: 918 live cursors at
2050-12-31, 36 dead at <=2011-12-31, store complete 1950..2050), filtering
cursors to <= today leaves ONLY the dead series, and max() over them reported
2011-12-31 as the store's currency — RED-DATA on three complete, current
sources, permanently (the R244 crying-wolf class).

Discriminating pairs pinned here (R414): the shape the fix must SUPPRESS, and
the three shapes whose behaviour must NOT change (all-forward, finished-dataset,
mixed-with-fresh).
"""
from __future__ import annotations

import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

NOW = dt.datetime(2026, 8, 25, tzinfo=dt.timezone.utc)


def _sig(vals):
    from updater.health import _recency_signal
    return _recency_signal(vals, NOW)


def test_dead_series_beside_projections_is_unknown_not_2011():
    # the unctad_pop* shape: live series project to 2050, dead ones ended 2011
    frontier, newest = _sig(["2050-12-31"] * 918 + ["2011-12-31", "1992-12-31"])
    assert frontier == "2050-12-31"
    assert newest is None, \
        "a discontinued series' end date is not the store's currency (RED-DATA artifact)"


def test_all_forward_dated_stays_unknown():
    frontier, newest = _sig(["2050-12-31", "2101-12-31"])
    assert frontier == "2101-12-31" and newest is None


def test_finished_dataset_keeps_its_honest_old_max():
    # imf_hpd: upstream genuinely ends 2015, no forward frontier — RED-DATA must
    # still be able to fire (suppressing this would mask every real freeze)
    frontier, newest = _sig(["2015-12-31", "2011-12-31"])
    assert frontier == "2015-12-31" and newest == "2015-12-31"


def test_projections_beside_fresh_series_keep_the_signal():
    # abs-style mixed store: projections to 2046 AND living current series
    frontier, newest = _sig(["2046-12-31", "2026-08-01", "2011-12-31"])
    assert frontier == "2046-12-31" and newest == "2026-08-01"


def test_projections_beside_moderately_stale_series_still_red_capable():
    # stale-but-not-discontinued (200d < STALE_SERIES_DAYS): the signal survives,
    # so RED-DATA still judges it — the suppression is ONLY for discontinued-old maxima
    frontier, newest = _sig(["2046-12-31", "2026-02-06"])
    assert newest == "2026-02-06"


def test_mostly_observed_store_keeps_its_honest_old_max():
    # imf_psbs_direct shape (adversarial review 2026-08-25): 13,933 of 14,019
    # cursors <= today (max 2021 IS the store's currency) beside 86 projection
    # cursors. The composition test must exclude it — and it is exactly what a
    # REAL freeze looks like (observed ~100%), so the red must stand at ANY age
    # and can never self-clear by getting older.
    vals = ["2021-12-31"] * 13933 + ["2027-12-31"] * 86
    frontier, newest = _sig(vals)
    assert frontier == "2027-12-31" and newest == "2021-12-31"


def test_uv_claim_outranks_the_structural_suppression():
    # fao_ga/gb/ge/gr/gt/gy carry upstream_verified dicts (probed, expiring via
    # UPSTREAM_RECHECK_DAYS). The suppression must NOT pre-empt that machinery:
    # with a claim present the honest old max survives so the RED-DATA branch
    # runs and the claim (with its expiry) decides.
    from updater.health import _recency_signal
    vals = ["2050-12-31"] * 918 + ["2011-12-31"] * 36
    _, newest_no_claim = _recency_signal(vals, NOW, has_uv_claim=False)
    _, newest_claim = _recency_signal(vals, NOW, has_uv_claim=True)
    assert newest_no_claim is None
    assert newest_claim == "2011-12-31"


def test_empty_and_none_inputs():
    assert _sig([]) == (None, None)
