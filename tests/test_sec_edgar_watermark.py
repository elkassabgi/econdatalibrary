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
def _wire(monkeypatch, cut, last, missing=(), argv=(), retry=(), filers=(320193,)):
    seen = {}
    monkeypatch.setattr(cutover, "is_cut_over", lambda: cut)
    monkeypatch.setattr(R, "_last_success_utc", lambda: (seen.setdefault("read", True), last)[1])
    monkeypatch.setattr(R, "_load_retry", lambda: set(retry))

    def fake_filers(days, today=None):
        seen["days"], seen["today"] = days, today
        return set(filers), ["x:1"], list(missing)
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
    assert R.main() == 1, "after T0 an unread index fails the run"
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


def test_the_writer_stamps_the_mark_when_allowed_and_saves_the_failed_companies_first():
    """_refresh_local moves the mark on an ok day that main() allowed - fetch failures do not block it (R1381: a few
    fail every day, so "no failure" froze the mark) - and writes the failed companies to the retry list BEFORE the
    stamp. Pinned on the source (the writer needs a whole store to run); the retry list's use is tested below."""
    src = open(os.path.join(ROOT, "tools", "refresh_sec_edgar.py"), encoding="utf-8").read()
    rule = 'mark = stamp_at.isoformat(timespec="seconds") if (ok_day and advance and stamp_at) else None'
    assert rule in src
    assert "not failed and stamp_at" not in src, "the freezing no-failure rule is back"
    body = src.split(rule, 1)[1]
    assert body.index("carry_retry(failed_ciks, absent_ciks, why, mark)") < body.index('st.upsert_source("sec_edgar"'), \
        "save the list first"
    assert '**({"last_success_utc": mark} if mark else {})' in src
    assert '**({"last_success_utc": when} if ok_day else {})' not in src, "the old every-ok-day stamp is back"
    assert src.count("failed_ciks.append(cik)") == 1
    assert "if is_absent(e):\n                    absent_ciks.append(cik)" in src
    assert "failed * 20 <= len(todo) - len(absent_ciks)" in src, "a 404 is neither a failure nor in the 5% base"
    assert "answered = len(todo) - failed - len(absent_ciks)" in src


def test_the_failed_companies_of_the_last_moved_mark_are_fetched_by_the_next_run(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(1), retry={111, 222}, filers=(320193,))
    assert R.main() == 0
    assert seen["todo"] == [111, 222, 320193]


def test_the_retry_list_round_trips_and_a_broken_file_is_never_read_as_empty(monkeypatch, tmp_path):
    p = tmp_path / "sec_edgar_retry_ciks.json"
    monkeypatch.setattr(R, "_retry_path", lambda: str(p))
    assert R._load_retry() == {}, "no file yet = nothing to retry"
    R._save_retry({5: 2, 3: 1})
    assert R._load_retry() == {3: 1, 5: 2}
    R._save_retry({})
    assert R._load_retry() == {}
    p.write_text('{"ciks": [7, 9]}', encoding="utf-8")         # the first form of the file
    assert R._load_retry() == {7: 0, 9: 0}
    p.write_text("{ broken", encoding="utf-8")
    with pytest.raises(ValueError):
        R._load_retry()


def test_after_t0_an_unread_index_fails_the_run_even_with_nothing_to_do(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(1), missing=[_days_ago(1) + ":ERRHTTPError"], filers=())
    assert R.main() == 1
    assert "todo" not in seen


def test_after_t0_an_unread_index_fails_the_run_even_when_the_writer_is_happy(monkeypatch):
    seen = _wire(monkeypatch, True, _days_ago(1), missing=[_days_ago(1) + ":ERRHTTPError"])
    assert R.main() == 1 and seen["advance"] is False


def test_before_t0_an_unread_index_does_not_change_the_exit_code(monkeypatch):
    _wire(monkeypatch, False, None, missing=["2026-10-01:ERRHTTPError"], filers=())
    assert R.main() == 0


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
    sat, fri, thu, wed = dt.date(2026, 10, 3), dt.date(2026, 10, 2), dt.date(2026, 10, 1), dt.date(2026, 9, 30)
    _fake_sec(monkeypatch, listed_days={fri, thu, wed}, failing_days={wed})       # wed: older than the overlap
    ciks, scanned, missing = R.filers_since(4, today=sat)
    assert ciks == {320193}
    assert missing == ["2026-10-03", "2026-09-30:ERRHTTPError"]
    assert R.may_advance(4, sat, None, False, missing) is False


def test_when_the_listing_cannot_be_read_a_failed_fetch_past_the_overlap_is_an_error(monkeypatch):
    sat, fri = dt.date(2026, 10, 3), dt.date(2026, 10, 2)
    _fake_sec(monkeypatch, listed_days={fri}, listing_ok=False)
    _ciks, _scanned, missing = R.filers_since(4, today=sat)
    assert missing == ["2026-10-03:unread-in-overlap-HTTPError", "2026-10-01:unread-in-overlap-HTTPError",
                       "2026-09-30:ERRHTTPError"], "a weekend cannot be told from a refusal without the listing"


def test_an_old_day_after_which_no_listing_names_anything_is_an_error(monkeypatch):
    """AR-209 round 2: a stale or cached listing stops before real days. A day older than the overlap with no
    listed day after it is not read as "no index"."""
    today = dt.date(2026, 10, 9)
    listed = {dt.date(2026, 10, 1), dt.date(2026, 10, 2)}            # the listing stops on 10-02
    _fake_sec(monkeypatch, listed_days=listed)
    _ciks, _s, missing = R.filers_since(9, today=today)
    assert "2026-10-09" in missing and "2026-10-08" in missing, "today and yesterday: not yet posted, plain"
    assert "2026-10-06:ERRunlisted" in missing and "2026-10-05:ERRunlisted" in missing
    assert R.may_advance(9, today, None, False, missing) is False


def test_a_weekend_before_a_listed_weekday_is_plain_missing_also_across_a_quarter(monkeypatch):
    # 2026-09-26/27 is a weekend at the end of QTR3; 2026-10-01 (QTR4) is listed and read first
    listed = {dt.date(2026, 10, 1), dt.date(2026, 9, 30), dt.date(2026, 9, 29), dt.date(2026, 9, 28),
              dt.date(2026, 9, 25)}
    _fake_sec(monkeypatch, listed_days=listed)
    _ciks, _s, missing = R.filers_since(7, today=dt.date(2026, 10, 1))
    assert "2026-09-27" in missing and "2026-09-26" in missing
    assert not [m for m in missing if ":ERR" in m], missing


def test_the_listing_is_read_once_per_quarter(monkeypatch):
    days = {dt.date(2026, 10, 1) - dt.timedelta(days=i) for i in range(5)}
    calls = _fake_sec(monkeypatch, listed_days=days)
    R.filers_since(5, today=dt.date(2026, 10, 2))                    # spans Q3 and Q4
    assert sum(1 for c in calls if c.endswith("index.json")) == 2


# ---- the retry list: a 404 is "absent", and nothing is carried for ever (AR-209 round 3) ----------------------------
def test_a_company_is_carried_while_it_fails_and_dropped_after_the_cap():
    keep, drop = R.next_retry({}, failed_ciks=[1], absent_ciks=[2])
    assert keep == {1: 1, 2: 1} and drop == []
    keep, drop = R.next_retry({1: R.RETRY_MAX_RUNS - 1, 2: R.RETRY_MAX_RUNS, 3: 4}, failed_ciks=[1], absent_ciks=[2])
    assert keep == {1: R.RETRY_MAX_RUNS}, "3 answered this run, so it is not carried; 2 passed the cap"
    assert drop == [2]


def test_only_a_404_is_absent():
    err = lambda code: urllib.error.HTTPError("u", code, "x", None, None)            # noqa: E731
    assert R.is_absent(err(404)) is True
    assert not [c for c in (403, 429, 500, 503) if R.is_absent(err(c))]
    assert R.is_absent(TimeoutError()) is False and R.is_absent(ValueError("bad json")) is False


def test_a_company_from_the_first_file_form_starts_its_count_again():
    keep, _drop = R.next_retry({7: 0}, failed_ciks=[7], absent_ciks=[])
    assert keep == {7: 1}


# ---- the overlap: a day the next window scans again does not block the mark -----------------------------------------
def test_a_listed_day_inside_the_overlap_that_fails_does_not_block_the_mark(monkeypatch):
    today = dt.date(2026, 10, 2)                                      # Friday
    listed = {today - dt.timedelta(days=i) for i in range(4)}         # 09-29 .. 10-02, all weekdays
    _fake_sec(monkeypatch, listed_days=listed, failing_days={today})
    _ciks, _s, missing = R.filers_since(4, today=today)
    assert missing == ["2026-10-02:unread-in-overlap-HTTPError"]
    assert R.may_advance(4, today, None, False, missing) is True


def test_a_listed_day_older_than_the_overlap_that_fails_blocks_the_mark(monkeypatch):
    today = dt.date(2026, 10, 2)
    listed = {today - dt.timedelta(days=i) for i in range(4)}
    _fake_sec(monkeypatch, listed_days=listed, failing_days={dt.date(2026, 9, 29)})
    _ciks, _s, missing = R.filers_since(4, today=today)
    assert missing == ["2026-09-29:ERRHTTPError"]
    assert R.may_advance(4, today, None, False, missing) is False


def test_an_unread_listing_blocks_only_beyond_the_overlap(monkeypatch):
    today = dt.date(2026, 10, 3)                                      # Saturday; the listing cannot be read
    _fake_sec(monkeypatch, listed_days={dt.date(2026, 10, 2)}, listing_ok=False)
    _ciks, _s, missing = R.filers_since(5, today=today)
    assert missing == ["2026-10-03:unread-in-overlap-HTTPError", "2026-10-01:unread-in-overlap-HTTPError",
                       "2026-09-30:ERRHTTPError", "2026-09-29:ERRHTTPError"]


# ---- a different listing per quarter (the shared fake hid a mix-up) ------------------------------------------------
def test_each_quarter_is_judged_by_its_own_listing_and_a_new_quarter_does_not_freeze_the_old(monkeypatch):
    q4 = {dt.date(2027, 12, 28), dt.date(2027, 12, 29), dt.date(2027, 12, 30)}
    q1 = {dt.date(2028, 1, 3)}
    calls = []

    def fake_get(url, timeout=180, binary=False):
        calls.append(url)
        if url.endswith("/index.json"):
            days = q4 if "/2027/QTR4/" in url else q1 if "/2028/QTR1/" in url else set()
            return json.dumps({"directory": {"item": [{"name": f"form.{d:%Y%m%d}.idx"} for d in days]}})
        day = dt.datetime.strptime(url.rsplit("form.", 1)[1][:8], "%Y%m%d").date()
        if day not in q4 | q1:
            raise urllib.error.HTTPError(url, 403, "Forbidden", None, None)
        return "header\n" + "-" * 10 + "\n" + "10-K".ljust(74) + "0000320193".ljust(20) + "x" * 10 + "\n"
    monkeypatch.setattr(R, "_get", fake_get)
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    _ciks, _s, missing = R.filers_since(8, today=dt.date(2028, 1, 4))
    assert missing == ["2028-01-04", "2028-01-02", "2028-01-01", "2027-12-31"], missing
    assert sum(1 for c in calls if c.endswith("index.json")) == 2
    assert not [c for c in calls if "form.20271231" in c], "an unlisted day is not fetched"


# ---- the writer itself, against fakes (AR-209 round 4: the rules were pinned only by source text) ------------------
def _writer(monkeypatch, tmp_path, fetch):
    """Run the real _refresh_local with --apply on 400 companies; `fetch(cik)` answers or raises. The store,
    catalogue, lock and state are fakes; there is no network."""
    import argparse                                            # noqa: PLC0415
    import contextlib                                          # noqa: PLC0415
    import types                                               # noqa: PLC0415
    import core.catalog_path as cp                             # noqa: PLC0415
    import updater.blob as blob                                # noqa: PLC0415
    import updater.state as state                              # noqa: PLC0415
    grouped = tmp_path / "grouped"
    grouped.mkdir()
    monkeypatch.setattr(R, "GROUPED", str(grouped))
    monkeypatch.setattr(R.time, "sleep", lambda s: None)
    monkeypatch.setattr(R, "_get", lambda url, timeout=180, binary=False: fetch(int(url.rsplit("CIK", 1)[1][:10])))
    monkeypatch.setattr(R, "_thirteen_f_blocker", lambda: None)
    monkeypatch.setattr(R, "_waiting_writer_lock", lambda *a, **k: contextlib.nullcontext())
    monkeypatch.setattr(R, "_retry_path", lambda: str(tmp_path / "retry.json"))
    monkeypatch.setattr(R, "_dropped_path", lambda: str(tmp_path / "dropped.jsonl"))
    monkeypatch.setattr(blob, "refuse_unless_live_checkout", lambda *a, **k: None)
    monkeypatch.setattr(blob, "SelfhostBlob", lambda *a, **k: object())

    class FakeCat:
        def execute(self, *a):
            return types.SimpleNamespace(fetchone=lambda: None)

        def close(self):
            pass
    monkeypatch.setattr(cp, "connect", lambda *a, **k: FakeCat())
    stamped = {}

    class FakeStore:
        def upsert_source(self, sid, **kw):
            stamped.update(kw)

        def close(self):
            pass
    monkeypatch.setattr(state, "StateStore", FakeStore)
    a = argparse.Namespace(d1=False, audit=False, respan=None, apply=True, force=False)
    rc = R._refresh_local(a, list(range(1000, 1400)), {}, advance=True,
                          stamp_at=dt.datetime(2026, 10, 3, 6, 0, tzinfo=dt.timezone.utc))
    return rc, stamped


def _http(code):
    def raise_it(cik):
        raise urllib.error.HTTPError("u", code, "x", None, None)
    return raise_it


def test_a_day_of_404s_for_everyone_with_a_broken_canary_is_partial_and_keeps_the_mark(monkeypatch, tmp_path):
    rc, stamped = _writer(monkeypatch, tmp_path, _http(404))         # the canary answers 404 too
    assert rc != 0 and stamped["status"] == "partial" and "last_success_utc" not in stamped
    assert not (tmp_path / "retry.json").exists(), "no mark move, so the list is not touched"


def test_a_day_of_404s_with_a_working_canary_is_ok_and_moves_the_mark(monkeypatch, tmp_path):
    def fetch(cik):
        if cik == R.CANARY_CIK:
            return json.dumps({"facts": {}})
        raise urllib.error.HTTPError("u", 404, "x", None, None)
    rc, stamped = _writer(monkeypatch, tmp_path, fetch)
    assert rc == 0 and stamped["status"] == "ok" and stamped["last_success_utc"] == "2026-10-03T06:00:00+00:00"
    assert len(json.loads((tmp_path / "retry.json").read_text(encoding="utf-8"))["ciks"]) == 400


def test_a_day_of_503s_is_partial(monkeypatch, tmp_path):
    rc, stamped = _writer(monkeypatch, tmp_path, _http(503))
    assert rc != 0 and stamped["status"] == "partial" and "last_success_utc" not in stamped


def test_carry_retry_counts_across_runs_and_appends_every_drop(monkeypatch, tmp_path):
    monkeypatch.setattr(R, "_retry_path", lambda: str(tmp_path / "retry.json"))
    monkeypatch.setattr(R, "_dropped_path", lambda: str(tmp_path / "dropped.jsonl"))
    R.carry_retry([7], [8], {7: "HTTPError503", 8: "HTTPError404"}, "2026-10-03T06:00:00+00:00")
    R.carry_retry([7], [8], {7: "HTTPError503", 8: "HTTPError404"}, "2026-10-04T06:00:00+00:00")
    assert R._load_retry() == {7: 2, 8: 2}, "the second run counts on the first run's file"
    for day in range(5, 5 + R.RETRY_MAX_RUNS - 1):
        R.carry_retry([7], [], {7: "HTTPError503"}, f"2026-10-{day:02d}T06:00:00+00:00")
    assert R._load_retry() == {}, "7 dropped after the cap; 8 answered, so it is not carried"
    rows = [json.loads(ln) for ln in (tmp_path / "dropped.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [(r["cik"], r["last_error"]) for r in rows] == [(7, "HTTPError503")]
