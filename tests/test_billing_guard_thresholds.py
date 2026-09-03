"""An alarm threshold set above the incident it was written for is not an alarm.

`WARN_R2_A` was 400,000 R2 class-A operations a day. From 2026-08-10 to 08-29 a FINISHED
`derive_noaa` was resurrected roughly 48 times a day by the relaunch guard, because
PowerShell 5.1 returned a null `ExitCode` and the runner never recorded success. Each pass
paged 3,139 `ListObjects` over the `series/noaa%3A` prefix and printed "to derive: 0 (already
present: 3,138,159)" - about 150,672 operations a day, for nothing, at a marginal cost of
$22.50 a month.

The peak TOTAL class-A on any day of that leak was 264,454 (2026-08-28), of which the leak was
59.3%. Every single day sat under the 400,000 threshold. This guard measured the operations,
printed them in its own report, and warned exactly zero times across three weeks.

These tests pin the thresholds against the measurements that justify them, so raising one
again requires deleting the evidence rather than editing a number.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import billing_guard as bg  # noqa: E402

# Measured, not assumed. Source: Cloudflare r2OperationsAdaptiveGroups by date and actionType.
NOAA_LEAK_PEAK_DAY = 264_454        # 2026-08-28, total class-A, 59.3% of it the leak
STEADY_STATE_DAY = 85_000           # the account's own baseline, billing_guard.py:44-47
QUIET_DAY_MEASURED = 47_640         # 2026-09-02, total class-A after both fixes


def test_the_warn_threshold_is_below_the_leak_it_missed():
    assert bg.WARN_R2_A < NOAA_LEAK_PEAK_DAY, (
        "WARN_R2_A=%d would not have fired on any day of the derive_noaa leak, whose worst "
        "day was %d. That is the defect this threshold was lowered to fix."
        % (bg.WARN_R2_A, NOAA_LEAK_PEAK_DAY))


def test_the_warn_threshold_is_above_normal_operation():
    """An alarm that fires every day is an alarm nobody reads - the opposite failure."""
    assert bg.WARN_R2_A > STEADY_STATE_DAY, (
        "WARN_R2_A=%d is at or below the ~%d/day steady state and would fire constantly"
        % (bg.WARN_R2_A, STEADY_STATE_DAY))
    assert bg.WARN_R2_A > QUIET_DAY_MEASURED, (
        "WARN_R2_A=%d would fire on a measured QUIET day (%d on 2026-09-02)"
        % (bg.WARN_R2_A, QUIET_DAY_MEASURED))


def test_alert_is_stricter_than_warn():
    assert bg.ALERT_R2_A > bg.WARN_R2_A, (
        "ALERT must be the louder of the two: got warn=%d alert=%d"
        % (bg.WARN_R2_A, bg.ALERT_R2_A))


def test_alert_still_catches_a_day_that_spends_the_whole_monthly_allowance():
    """1,000,000 class-A operations in one day IS the entire month's included allowance."""
    assert bg.ALERT_R2_A <= bg.R2_CLASS_A_INCLUDED, (
        "ALERT_R2_A=%d is above the monthly included allowance of %d, so a day can spend the "
        "whole allowance without alerting" % (bg.ALERT_R2_A, bg.R2_CLASS_A_INCLUDED))


def test_the_write_thresholds_keep_their_documented_meaning():
    """Pinned so an unrelated edit cannot quietly change what the email means by WARN."""
    assert bg.WARN_WRITES < bg.ALERT_WRITES
    assert bg.WARN_WRITES <= bg.D1_WRITES_INCLUDED, (
        "a day at WARN_WRITES=%d must not already exceed the %d monthly included writes"
        % (bg.WARN_WRITES, bg.D1_WRITES_INCLUDED))
