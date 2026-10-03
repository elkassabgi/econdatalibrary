"""The sec_edgar scan window after T0 reaches back to the last OK local run (tools/refresh_sec_edgar.scan_days).

A fixed --days after T0 loses filings for good whenever the scheduled task misses more days than the window;
before T0 nothing changes (the CI workflow always passes --days)."""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cutover  # noqa: E402
from tools import refresh_sec_edgar as R  # noqa: E402

TODAY = dt.date(2026, 10, 3)


def test_an_explicit_days_always_wins():
    assert R.scan_days(7, True, None, TODAY) == (7, "--days given")
    assert R.scan_days(7, False, "2020-01-01T00:00:00+00:00", TODAY)[0] == 7


def test_before_t0_the_old_default_is_unchanged():
    assert R.scan_days(None, False, None, TODAY)[0] == R.PRE_T0_DEFAULT_DAYS == 3


def test_after_t0_the_window_reaches_the_last_ok_day_minus_the_overlap():
    days, why = R.scan_days(None, True, "2026-10-01T06:12:00+00:00", TODAY)
    assert days == 2 + R.WATERMARK_OVERLAP_DAYS == 5
    assert "2026-10-01" in why
    # filers_since(days) scans today back to today-(days-1): the last OK day minus two more days is included
    assert TODAY - dt.timedelta(days=days - 1) == dt.date(2026, 9, 29)
    assert R.scan_days(None, True, "2026-10-03T01:00:00+00:00", TODAY)[0] == R.WATERMARK_OVERLAP_DAYS


def test_a_machine_that_was_off_for_two_weeks_scans_the_two_weeks():
    assert R.scan_days(None, True, "2026-09-19T06:00:00+00:00", TODAY)[0] == 14 + 3


@pytest.mark.parametrize("value, words", [
    (None, "first local run takes --days"),
    ("", "first local run takes --days"),
    ("not a date", "is not a date"),
    ("2026-10-04T00:00:00+00:00", "after today UTC"),
    ((TODAY - dt.timedelta(days=200)).isoformat(), "past WATERMARK_MAX_DAYS"),
])
def test_after_t0_a_window_it_cannot_stand_behind_is_refused(value, words):
    with pytest.raises(cutover.CutoverRefused) as e:
        R.scan_days(None, True, value, TODAY)
    assert words in str(e.value)


def test_the_largest_window_is_allowed_and_one_more_day_is_not():
    edge = (TODAY - dt.timedelta(days=R.WATERMARK_MAX_DAYS - R.WATERMARK_OVERLAP_DAYS)).isoformat()
    assert R.scan_days(None, True, edge, TODAY)[0] == R.WATERMARK_MAX_DAYS
    past = (TODAY - dt.timedelta(days=R.WATERMARK_MAX_DAYS - R.WATERMARK_OVERLAP_DAYS + 1)).isoformat()
    with pytest.raises(cutover.CutoverRefused):
        R.scan_days(None, True, past, TODAY)


def test_main_uses_the_window_and_reads_the_state_only_after_t0(monkeypatch):
    """The wiring: after T0, without --days, main() reads last_success_utc and scans that many days."""
    seen = {}
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    monkeypatch.setattr(R, "_last_success_utc", lambda: (seen.setdefault("read", True),
                                                         (dt.datetime.now(dt.timezone.utc).date()
                                                          - dt.timedelta(days=4)).isoformat())[1])

    def fake_filers(days):
        seen["days"] = days
        return set(), [], []
    monkeypatch.setattr(R, "filers_since", fake_filers)
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py"])
    assert R.main() == 0                                              # no filers: "nothing to do"
    assert seen == {"read": True, "days": 4 + R.WATERMARK_OVERLAP_DAYS}


def test_main_before_t0_reads_no_state(monkeypatch):
    seen = {}
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    monkeypatch.setattr(R, "_last_success_utc", lambda: seen.setdefault("read", True))

    def fake_filers(days):
        seen["days"] = days
        return set(), [], []
    monkeypatch.setattr(R, "filers_since", fake_filers)
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py"])
    assert R.main() == 0
    assert seen == {"days": 3}


def test_a_partial_day_never_moves_the_window_forward():
    """The window is safe only because a partial day never sets last_success_utc: _refresh_local stamps it on an
    ok day only. Pinned on the source so a refactor that stamps every day fails here."""
    src = open(os.path.join(ROOT, "tools", "refresh_sec_edgar.py"), encoding="utf-8").read()
    assert '**({"last_success_utc": when} if ok_day else {})' in src
