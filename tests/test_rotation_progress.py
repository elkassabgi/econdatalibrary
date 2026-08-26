"""audit_rotation_progress.assess_listing — discriminating pairs from the RECORDED episodes.

The thresholds are calibrated against measured history, not invented: ecb's stuck era was
280/540 files beyond the horizon (52%, must fire) and its healthy state 15/540 (3%, must
stay quiet); ssb's stuck era 103/186 (55%, must fire). R414: each side of the boundary is
pinned so a threshold edit cannot silently disarm the check.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.audit_rotation_progress import assess_listing  # noqa: E402


def _ages(n_stale, n_fresh, stale_age=60.0, fresh_age=2.0):
    return [stale_age] * n_stale + [fresh_age] * n_fresh


def test_ecb_stuck_era_fires():
    share, verdict = assess_listing(_ages(280, 260), cadence_days=1)
    assert verdict == "STUCK" and abs(share - 280 / 540) < 1e-9


def test_ecb_healthy_state_is_quiet():
    share, verdict = assess_listing(_ages(15, 525), cadence_days=1)
    assert verdict == "OK" and share < 0.05


def test_ssb_stuck_era_fires():
    _, verdict = assess_listing(_ages(103, 83), cadence_days=28)
    # ssb is monthly: horizon = max(21, 84) = 84d, so "stale" needs age > 84
    assert verdict == "OK", "at 60d these files are inside a monthly horizon — see next test"
    _, verdict2 = assess_listing(_ages(103, 83, stale_age=120.0), cadence_days=28)
    assert verdict2 == "STUCK"


def test_horizon_respects_cadence_floor():
    # daily cadence: horizon is the 21-day FLOOR, not 3 days — weekend-quiet dailies
    # must not read as stale tails
    _, verdict = assess_listing([10.0] * 100, cadence_days=1)
    assert verdict == "OK"
    _, verdict2 = assess_listing([25.0] * 100, cadence_days=1)
    assert verdict2 == "STUCK"


def test_boundary_is_strictly_greater_than_half():
    _, at_half = assess_listing(_ages(50, 50), cadence_days=1, )
    assert at_half == "OK", "exactly half is not STUCK — the threshold is strict"
    _, past = assess_listing(_ages(51, 49), cadence_days=1)
    assert past == "STUCK"


def test_empty_listing_is_its_own_verdict():
    share, verdict = assess_listing([], cadence_days=1)
    assert verdict == "EMPTY" and share == 0.0
