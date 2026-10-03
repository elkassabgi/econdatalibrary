"""The sec_edgar scan window after T0 reaches back to the scan mark (tools/refresh_sec_edgar.scan_days), and only a
daily scan that reached the mark may move it (may_advance; review AR-209).

A fixed --days after T0 loses filings for good whenever the scheduled task misses more days than the window; before
T0 nothing changes (the CI workflow always passes --days)."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import urllib.error

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cutover  # noqa: E402
from tools import refresh_sec_edgar as R  # noqa: E402

TODAY = dt.date(2026, 10, 3)


# ---- the window ---------------------------------------------------------------------------------------------------
def test_an_explicit_days_always_wins():
    assert R.scan_days(7, True, None, TODAY) == (7, "--days given")
    assert R.scan_days(7, False, "2020-01-01T00:00:00+00:00", TODAY)[0] == 7


def test_before_t0_the_old_default_is_unchanged():
    assert R.scan_days(None, False, None, TODAY)[0] == R.PRE_T0_DEFAULT_DAYS == 3


def test_after_t0_the_window_is_the_mark_and_the_two_days_before_it_up_to_today():
    days, why = R.scan_days(None, True, "2026-10-01T06:12:00+00:00", TODAY)
    assert days == 2 + R.WATERMARK_OVERLAP_DAYS == 5
    # filers_since(days) scans today back to today-(days-1): the mark and the two days before it are included
    assert TODAY - dt.timedelta(days=days - 1) == dt.date(2026, 9, 29)
    assert "from 2026-09-29" in why and "2026-10-01" in why
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
    assert "--ciks" not in str(e.value), "a --ciks repair does not cover a gap (AR-209 B1)"


def test_the_largest_window_is_allowed_and_one_more_day_is_not():
    edge = (TODAY - dt.timedelta(days=R.WATERMARK_MAX_DAYS - R.WATERMARK_OVERLAP_DAYS)).isoformat()
    assert R.scan_days(None, True, edge, TODAY)[0] == R.WATERMARK_MAX_DAYS
    past = (TODAY - dt.timedelta(days=R.WATERMARK_MAX_DAYS - R.WATERMARK_OVERLAP_DAYS + 1)).isoformat()
    with pytest.raises(cutover.CutoverRefused):
        R.scan_days(None, True, past, TODAY)


# ---- who may move the mark ----------------------------------------------------------------------------------------
def test_a_scan_that_reached_the_mark_may_move_it():
    assert R.may_advance(5, TODAY, "2026-10-01T06:00:00+00:00", False, ["2026-09-27"]) is True


def test_a_scan_that_stops_short_of_the_mark_may_not():
    # the mark is 2026-09-26; a window of 3 days from 10-03 starts on 10-01: 09-27..09-30 were never scanned
    assert R.may_advance(3, TODAY, "2026-09-26T06:00:00+00:00", False, []) is False
    assert R.may_advance(8, TODAY, "2026-09-26T06:00:00+00:00", False, []) is True     # starts on 09-26


def test_a_limited_run_or_an_unread_index_may_not_move_the_mark():
    assert R.may_advance(5, TODAY, "2026-10-01T06:00:00+00:00", True, []) is False
    assert R.may_advance(5, TODAY, "2026-10-01T06:00:00+00:00", False, ["2026-09-30:ERRHTTPError"]) is False


def test_the_first_run_with_an_explicit_window_may_move_the_mark():
    assert R.may_advance(30, TODAY, None, False, []) is True
    assert R.may_advance(30, TODAY, "garbage", False, []) is False


# ---- main() hands the decision to the writer -----------------------------------------------------------------------
def _wire(monkeypatch, cut, last, missing=(), argv=()):
    seen = {}
    monkeypatch.setattr(cutover, "is_cut_over", lambda: cut)
    monkeypatch.setattr(R, "_last_success_utc", lambda: (seen.setdefault("read", True), last)[1])

    def fake_filers(days, today=None):
        seen["days"], seen["today"] = days, today
        return {320193}, ["x:1"], list(missing)
    monkeypatch.setattr(R, "filers_since", fake_filers)
    monkeypatch.setattr(R, "ticker_map", lambda: {320193: ["AAPL"]})

    def fake_local(a, todo, t2c, advance=False, stamp_at=None):
        seen["advance"], seen["stamp_at"], seen["todo"] = advance, stamp_at, todo
        return 0
    monkeypatch.setattr(R, "_refresh_local", fake_local)
    monkeypatch.setattr(sys, "argv", ["refresh_sec_edgar.py", *argv])
    return seen


def _days_ago(n):
    return (dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=n)).isoformat()


def test_a_daily_run_after_t0_reads_the_mark_scans_to_it_and_may_move_it(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(4))
    assert R.main() == 0
    assert seen["read"] is True and seen["days"] == 4 + R.WATERMARK_OVERLAP_DAYS
    assert seen["advance"] is True
    assert seen["stamp_at"] is not None and seen["stamp_at"].date() == seen["today"], "one clock: window and stamp"


def test_a_ciks_repair_after_a_gap_never_moves_the_mark(monkeypatch):
    """AR-209 B1: last daily OK 7 days ago, a --ciks repair now; stamped, the next daily run would skip the gap."""
    seen = _wire(monkeypatch, True, _days_ago(7), argv=["--ciks", "320193"])
    assert R.main() == 0
    assert seen["advance"] is False and "days" not in seen


def test_a_limited_run_never_moves_the_mark(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(2), argv=["--limit", "1"])
    assert R.main() == 0
    assert seen["advance"] is False


def test_a_short_explicit_window_after_a_gap_never_moves_the_mark(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(7), argv=["--days", "1"])
    assert R.main() == 0
    assert seen["read"] is True and seen["days"] == 1 and seen["advance"] is False


def test_an_unread_index_never_moves_the_mark(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(1), missing=[_days_ago(1) + ":ERRHTTPError"])
    assert R.main() == 0
    assert seen["advance"] is False


def test_main_before_t0_reads_no_state(monkeypatch):
    seen = _wire(monkeypatch, False, "should not be read")
    monkeypatch.setattr(R, "_refresh_local", lambda *a, **k: pytest.fail("before T0 the local writer is not called"))

    def stop(*a, **k):
        raise SystemExit(0)                      # stop at the R2 client: the pre-T0 path is not this test's subject
    import core.r2_util as r2                    # noqa: PLC0415
    monkeypatch.setattr(r2, "client", stop)
    with pytest.raises(SystemExit):
        R.main()
    assert "read" not in seen and seen["days"] == 3


def test_the_writer_stamps_the_mark_only_when_allowed_and_with_no_fetch_failure():
    """_refresh_local moves the mark only on an ok day that main() allowed, with no failed company fetch. Pinned on
    the source (the writer needs a whole store to run)."""
    src = open(os.path.join(ROOT, "tools", "refresh_sec_edgar.py"), encoding="utf-8").read()
    assert 'mark = stamp_at.isoformat(timespec="seconds") if (ok_day and advance and not failed and stamp_at) else None' in src
    assert '**({"last_success_utc": mark} if mark else {})' in src
    assert '**({"last_success_utc": when} if ok_day else {})' not in src, "the old every-ok-day stamp is back"


def test_the_mark_is_read_only_from_the_xbrl_products_own_row(monkeypatch):
    class FakeStore:
        def __init__(self, row):
            self.row = row

        def get_source(self, sid):
            return self.row

        def close(self):
            pass
    import updater.state as state                # noqa: PLC0415
    for row, want in (({"strategy": "edgar_delta", "last_success_utc": "2026-10-01T06:00:00+00:00"},
                       "2026-10-01T06:00:00+00:00"),
                      ({"strategy": "giant_changed_units", "last_success_utc": "2026-09-23T00:00:00+00:00"}, None),
                      ({"strategy": None, "last_success_utc": "2026-09-23T00:00:00+00:00"}, None),
                      (None, None)):
        monkeypatch.setattr(state, "StateStore", lambda r=row: FakeStore(r))
        assert R._last_success_utc() == want, row


# ---- which days have no index: SEC's quarter listing, not a failed fetch --------------------------------------------
def _fake_sec(monkeypatch, listed_days, failing_days=(), listing_ok=True):
    calls = []

    def fake_get(url, timeout=180, binary=False):
        calls.append(url)
        if url.endswith("/index.json"):
            if not listing_ok:
                raise urllib.error.HTTPError(url, 503, "busy", None, None)
            return json.dumps({"directory": {"item": [{"name": f"form.{d:%Y%m%d}.idx"} for d in listed_days]}})
        day = dt.datetime.strptime(url.rsplit("form.", 1)[1][:8], "%Y%m%d").date()
        if day in failing_days or day not in listed_days:
            raise urllib.error.HTTPError(url, 403, "Forbidden", None, None)
        return "header\n" + "-" * 10 + "\n" + "10-K".ljust(74) + "0000320193".ljust(20) + "x" * 10 + "\n"
    monkeypatch.setattr(R, "_get", fake_get)
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    return calls


def test_a_day_sec_does_not_list_is_no_index_and_a_listed_day_that_fails_is_an_error(monkeypatch):
    sat, fri, thu = dt.date(2026, 10, 3), dt.date(2026, 10, 2), dt.date(2026, 10, 1)
    _fake_sec(monkeypatch, listed_days={fri, thu}, failing_days={thu})
    ciks, scanned, missing = R.filers_since(3, today=sat)
    assert ciks == {320193}
    assert missing == ["2026-10-03", "2026-10-01:ERRHTTPError"]
    assert R.may_advance(3, sat, None, False, missing) is False


def test_when_the_listing_cannot_be_read_every_failed_fetch_is_an_error(monkeypatch):
    sat, fri = dt.date(2026, 10, 3), dt.date(2026, 10, 2)
    _fake_sec(monkeypatch, listed_days={fri}, listing_ok=False)
    _ciks, _scanned, missing = R.filers_since(2, today=sat)
    assert missing == ["2026-10-03:ERRHTTPError"], "a weekend cannot be told from a refusal without the listing"


def test_the_listing_is_read_once_per_quarter(monkeypatch):
    days = {dt.date(2026, 10, 1) - dt.timedelta(days=i) for i in range(5)}
    calls = _fake_sec(monkeypatch, listed_days=days)
    R.filers_since(5, today=dt.date(2026, 10, 2))                    # spans Q3 and Q4
    assert sum(1 for c in calls if c.endswith("index.json")) == 2
