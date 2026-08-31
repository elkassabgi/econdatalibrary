"""The no-TimeDimension classification (R523), held by discriminating pairs.

oecd carries 60 flows whose DSD declares no SDMX TimeDimension (verified against the
publisher's own DSDs 2026-08-30, 18/18 with controls; 0 of 60 ever had rows vs 40/40
controls). Counting them as structural breaks made `finalize()` raise on EVERY oecd run —
one permanent condition reddening a 1,545-flow giant — and `ingest_oecd` booked them as
"full, n_obs=0": a successful ingest of nothing.

Every test here has the direction that would have caught the original defect AND the
direction that keeps the genuine break loud (R414):

  * a readable CSV header WITHOUT TIME_PERIOD  -> "no_time_dimension"
  * garbage / XML                              -> "structural" (unchanged)
  * no_time + store NEVER had rows             -> non-demoting note, vintage advanced
  * no_time + store HAS rows                   -> structural (the column vanished from
                                                  under data we hold — a REAL break)
  * finalize with structural>0                 -> still raises
  * finalize with only no_time                 -> does NOT demote, note names the flows
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import _common  # noqa: E402
from updater.strategies.fetchers import oecd  # noqa: E402


class _FakeResp:
    pass


def _fetch(monkeypatch, body: bytes):
    monkeypatch.setattr(oecd, "http_get", lambda *a, **k: body)
    return oecd.fetch_flow(
        "T1", {"root": "https://x", "agency": "A", "id": "T1", "version": "1.0"},
        None, session=None)


def test_header_without_time_period_is_no_time_dimension(monkeypatch):
    body = b"DATAFLOW,REF_AREA,INDICATOR,OBS_VALUE,UNIT\n" + b"x,US,GDP,1.0,PC\n" * 20
    table, status = _fetch(monkeypatch, body)
    assert status == "no_time_dimension"
    assert table is None


def test_header_with_time_period_but_unparseable_stays_structural(monkeypatch):
    # TIME_PERIOD present but OBS_VALUE missing: parse refuses, and this is NOT the
    # no-TimeDimension shape - it must stay a loud structural.
    body = b"DATAFLOW,REF_AREA,TIME_PERIOD,SOMETHING\n" + b"x,US,2020,1.0\n" * 20
    table, status = _fetch(monkeypatch, body)
    assert status == "structural"


def test_xml_body_stays_structural(monkeypatch):
    body = b"<error>not csv</error>" + b" " * 300
    _, status = _fetch(monkeypatch, body)
    assert status == "structural"


def test_tiny_body_stays_empty(monkeypatch):
    _, status = _fetch(monkeypatch, b"DATAFLOW\n")
    assert status == "empty"


def test_tally_no_time_does_not_demote_and_names_the_flows():
    t = _common.Tally()
    t.added_unit(100)
    for fid in ("DSD_A@DF_X", "DSD_B@DF_Y"):
        t.no_time_unit(fid)
    res = _common.finalize(t, total_rows=100, last_obs="2026-01-01", source="oecd")
    assert res.status == "ok", res
    assert "no SDMX TimeDimension" in (res.error or "")
    assert "DSD_A@DF_X" in (res.error or "")


def test_tally_structural_still_raises_even_beside_no_time():
    t = _common.Tally()
    t.added_unit(100)
    t.no_time_unit("DSD_A@DF_X")
    t.structural_unit("DSD_REAL@DF_BREAK: TIME_PERIOD column GONE")
    with pytest.raises(_common.DefinitiveError):
        _common.finalize(t, total_rows=100, last_obs="2026-01-01", source="oecd")


class _Unit:
    def __init__(self, d):
        self.out_paths = [d]


def _drive(tmp_path, fetch_flow_status, *, vintage="v1"):
    """Run the REAL run_giant end to end with injected callables (it is injectable by
    design). Review finding 3: the earlier version of this test re-executed COPIED branch
    statements — zero lines of run_giant ran, so deleting the branch left it green."""
    from updater.strategies.fetchers import _giant

    def fetch_catalog():
        return {"DSD_T@DF_N": {"vintage": vintage, "filename": "DF_N.parquet"}}

    def fetch_flow(fid, meta, since, session):
        return None, fetch_flow_status

    return _giant.run_giant(
        _Unit(str(tmp_path)), source="oecd_test",
        fetch_catalog=fetch_catalog, fetch_flow=fetch_flow,
        csv_accept="text/csv", rate=0, timeout=5)


def test_run_giant_parks_a_storeless_no_time_flow(tmp_path):
    from updater.strategies.fetchers import _giant

    res = _drive(tmp_path, "no_time_dimension")
    assert res.status == "no_change", res
    assert "no SDMX TimeDimension" in (res.error or "")

    st = _giant.load_state(str(tmp_path))
    assert st["DSD_T@DF_N"]["status"] == "no_time_dimension"
    assert st["DSD_T@DF_N"]["vintage"] == "v1"


def test_parked_flow_is_not_reselected_until_the_publisher_bumps_the_version(tmp_path):
    """The mechanism that actually ends the starvation: parked at the same vintage,
    reselected on a bump. Pinned against the REAL select_flows."""
    from updater.strategies.fetchers import _giant

    _drive(tmp_path, "no_time_dimension", vintage="v1")
    state = _giant.load_state(str(tmp_path))

    # select_flows returns (ids, capped) — membership must test the ID LIST, not the tuple.
    # The first version of this test checked the tuple, so the parking assertion passed
    # VACUOUSLY and the bump assertion failed against a correct implementation (R488: the
    # locator must be proven before its verdict means anything).
    same, _capped = _giant.select_flows(
        {"DSD_T@DF_N": {"vintage": "v1", "filename": "DF_N.parquet"}}, state)
    assert "DSD_T@DF_N" not in same, "parked flow was reselected at the SAME vintage"

    bumped, _capped = _giant.select_flows(
        {"DSD_T@DF_N": {"vintage": "v2", "filename": "DF_N.parquet"}}, state)
    assert "DSD_T@DF_N" in bumped, "publisher version bump must re-probe the flow"


def test_run_giant_treats_no_time_on_a_stored_flow_as_a_real_break(tmp_path, monkeypatch):
    """The had-rows arm: TIME_PERIOD vanishing from under data we HOLD stays loud."""
    import pytest as _pytest
    from updater.strategies.fetchers import _giant

    monkeypatch.setattr(_giant, "_max_obs_date", lambda p: "2026-01-01")
    with _pytest.raises(_common.DefinitiveError):
        _drive(tmp_path, "no_time_dimension")
    st = _giant.load_state(str(tmp_path))
    assert st["DSD_T@DF_N"]["status"] == "definitive_fail"
    assert "vintage" not in st["DSD_T@DF_N"], "a break must NOT advance the vintage"


def test_plain_text_200_body_must_not_park(monkeypatch):
    """The poisoning direction (review finding 1): a 200 whose body is error PROSE also
    'parses' into a TIME_PERIOD-less header — without the SDMX-CSV marker requirement it
    would be silently parked until a version bump, possibly years. It must fall STRUCTURAL
    (loud, retried) instead."""
    body = (b"Service temporarily degraded, please retry later.\n" + b"x" * 300)
    _, status = _fetch(monkeypatch, body)
    assert status == "structural"


def test_json_error_200_body_must_not_park(monkeypatch):
    body = b'{"error": "quota exceeded", "detail": "try again"}' + b" " * 300
    _, status = _fetch(monkeypatch, body)
    assert status == "structural"
