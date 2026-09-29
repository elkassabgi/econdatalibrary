"""dst: a failed data POST is TRANSIENT (the table stays owed), never a quiet empty table - review R1283.

query_table returned [] both for a POST that failed (5xx/timeout after retries, 403) and for a table with no
rows, so dst booked the failure as an empty table and ADVANCED its manifest entry: that release was never taken
until DST republished the table. Measured live 2026-09-29: DNVALD answered HTTP 500 to a data POST of all its
12,551 daily time values (the last 50 or 500 answer 200) and DNINDEX's 1,121 daily values parsed to 0 rows.
Hermetic: only the ingester's HTTP layer (get_json / post_json) is faked; the store is a tmp dir under the
LOCAL backend and the merge is the real one.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import dst as D  # noqa: E402
from jobs import ingest_dst as ING  # noqa: E402


def _jsonstat(times, values, status=None):
    """A DST JSON-stat v1 body: one non-time dimension (X: A) and the Tid time axis."""
    ds = {"dimension": {"X": {"label": "x", "category": {"index": {"A": 0}, "label": {"A": "a"}}},
                        "Tid": {"label": "time", "category": {"index": {t: i for i, t in enumerate(times)},
                                                              "label": {t: t for t in times}}},
                        "id": ["X", "Tid"], "size": [1, len(times)], "role": {"time": ["Tid"]}},
          "value": values}
    if status is not None:
        ds["status"] = status
    return {"dataset": ds}


def _meta(times):
    return {"variables": [{"id": "X", "values": [{"id": "A"}]},
                          {"id": "Tid", "time": True, "values": [{"id": t} for t in times]}]}


@pytest.fixture
def site(monkeypatch):
    """table -> (times, handler(body_times) -> body or None)."""
    tables = {}
    posts = []

    def get_json(url, retries=3):
        tid = url.split("id=")[1].split("&")[0]
        return _meta(tables[tid][0]) if tid in tables else None

    def post_json(url, body, retries=3):
        tid = body["table"]
        tvals = next(v["values"] for v in body["variables"] if v["code"] == "Tid")
        posts.append((tid, len(tvals)))
        return tables[tid][1](tvals)
    monkeypatch.setattr(ING, "get_json", get_json)
    monkeypatch.setattr(ING, "post_json", post_json)
    monkeypatch.setattr(ING, "RATE", 0)
    monkeypatch.setattr(D, "RATE", 0)
    return type("Site", (), {"tables": tables, "posts": posts})


def test_a_failed_data_post_is_transient_not_an_empty_table(site):
    site.tables["DNX"] = (["2024M01", "2024M02"], lambda tv: None)            # 500/timeout/403 -> None
    rows, transient = D._fetch_table_rows("DNX")
    assert rows == [] and transient is True


def test_a_genuinely_empty_table_is_still_empty(site):
    site.tables["BAR"] = (["2024", "2025"], lambda tv: _jsonstat(tv, [None] * len(tv), {"0": "..", "1": ".."}))
    assert D._fetch_table_rows("BAR") == ([], False)
    assert ING.query_table_detailed("BAR", _meta(["2024", "2025"])["variables"])[1] == "all_null"


def test_daily_codes_are_dated_to_the_day(site):
    site.tables["DNI"] = (["2022M04D01", "2022M04D04"], lambda tv: _jsonstat(tv, [1.5, 2.5]))
    rows, transient = D._fetch_table_rows("DNI")
    import datetime as dt
    assert transient is False and sorted(d for _, d, _ in rows) == [dt.date(2022, 4, 1), dt.date(2022, 4, 4)]
    assert ING.parse_date("2022M02D30") is None                               # impossible day stays None


def test_multi_year_windows_are_named_span_time_and_not_retried(site):
    site.tables["REC"] = (["2007:2009", "2008:2010"], lambda tv: _jsonstat(tv, [3.0, 4.0]))
    assert ING.query_table_detailed("REC", _meta(["2007:2009", "2008:2010"])["variables"])[1] == "span_time"
    assert D._fetch_table_rows("REC") == ([], False)                           # processed: nothing to store


def test_values_our_parser_cannot_read_are_ours_and_stay_owed(site):
    site.tables["ODD"] = (["2019X7", "2020X7"], lambda tv: _jsonstat(tv, [5.0, 6.0]))
    assert ING.query_table_detailed("ODD", _meta(["2019X7", "2020X7"])["variables"])[1] == "unparsed"
    assert D._fetch_table_rows("ODD") == ([], True)


def test_a_part_year_window_is_span_time_not_our_gap(site):
    """BARLOV1 publishes 'AUG - DEC 2019': a window the dating convention does not cover, like '2007:2009'."""
    times = ["AUG - DEC 2019", "AUG - DEC 2020"]
    site.tables["BARLOV1"] = (times, lambda tv: _jsonstat(tv, [5.0, 6.0]))
    assert ING.query_table_detailed("BARLOV1", _meta(times)["variables"])[1] == "span_time"
    assert D._fetch_table_rows("BARLOV1") == ([], False)
    assert ING._why_empty(_jsonstat(["AUG - DEC 2019", "2019X7"], [1.0, 2.0])) == "unparsed"   # not ALL windows


def test_an_operator_looking_value_id_is_sent_as_the_wildcard(monkeypatch):
    """AKU240K: DST reads '<15' as a comparison ("Can't find value: 15 (<15)", HTTP 400 on every run); its own
    wildcard '*' asks for the same set and answers 200 (measured live 2026-09-29)."""
    bodies = []
    monkeypatch.setattr(ING, "post_json", lambda url, body, retries=3: bodies.append(body) or {"ok": 1})
    monkeypatch.setattr(ING, "RATE", 0)
    variables = [{"id": "ARBEJDSTID", "values": [{"id": "<15"}, {"id": "15-36"}, {"id": ">48"}]},
                 {"id": "ALDER", "values": [{"id": "TOT"}, {"id": "1524"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024K1"}]}]
    ING._post_data("AKU240K", ING._query_selection(variables))
    assert bodies[0]["variables"] == [{"code": "ARBEJDSTID", "values": ["*"]},
                                      {"code": "ALDER", "values": ["TOT", "1524"]},        # ordinary ids stay ids
                                      {"code": "Tid", "values": ["2024K1"]}]
    # never a wildcard for the time axis (it is chunked by id) or for a restricted (MAX_CELLS) selection
    assert ING._wire_values({"code": "Tid", "values": ["<x"], "_time": True, "_all": True}) == ["<x"]
    assert ING._wire_values({"code": "A", "values": ["<15"], "_time": False, "_all": False}) == ["<15"]


def test_a_restricted_selection_is_not_marked_whole(monkeypatch):
    monkeypatch.setattr(ING, "MAX_CELLS", 2)
    sel = ING._query_selection([{"id": "A", "values": [{"id": "<15"}, {"id": "TOT"}]},
                                {"id": "Tid", "time": True, "values": [{"id": "2024"}, {"id": "2025"}]}])
    assert sel[0]["values"] == ["TOT"] and sel[0]["_all"] is False and sel[1]["_all"] is True


def test_a_table_with_more_time_values_than_a_chunk_is_fetched_in_chunks_from_the_start(site, monkeypatch):
    """The full POST is never tried: for the daily tables it failed every time (3 x 500 + back-off, ~110 s a day)."""
    monkeypatch.setattr(ING, "TIME_CHUNK", 3)
    times = [f"2024M01D{d:02d}" for d in range(1, 11)]                        # 10 daily values
    site.tables["DNV"] = (times, lambda tv: None if len(tv) > 3 else _jsonstat(tv, [1.0] * len(tv)))
    rows, transient = D._fetch_table_rows("DNV")
    assert transient is False and len(rows) == 10
    assert [n for _, n in site.posts] == [3, 3, 3, 1]


def test_a_table_within_one_chunk_is_one_post(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 3)
    site.tables["SML"] = (["2024M01", "2024M02", "2024M03"], lambda tv: _jsonstat(tv, [1.0, 2.0, 3.0]))
    rows, transient = D._fetch_table_rows("SML")
    assert transient is False and len(rows) == 3 and [n for _, n in site.posts] == [3]


def test_an_all_missing_chunk_does_not_discard_the_others(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2024M01D01", "2024M01D02", "2024M01D03", "2024M01D04"]

    def handler(tv):
        if len(tv) > 2:
            return None
        return _jsonstat(tv, [None, None] if tv[0] == "2024M01D03" else [1.0, 2.0])
    site.tables["HOL"] = (times, handler)
    rows, transient = D._fetch_table_rows("HOL")
    assert transient is False and len(rows) == 2


def test_a_chunk_that_still_fails_fails_the_table(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2024M01D01", "2024M01D02", "2024M01D03"]
    site.tables["BAD"] = (times, lambda tv: None if len(tv) > 2 or tv[0] == "2024M01D03" else _jsonstat(tv, [1.0, 2.0]))
    assert D._fetch_table_rows("BAD") == ([], True)


def test_the_units_own_timeout_is_not_one_tables_failure(site, monkeypatch):
    import types
    class UnitTimeout(Exception):
        pass
    monkeypatch.setitem(sys.modules, "updater.orchestrate",
                        types.SimpleNamespace(UnitTimeout=UnitTimeout, UNIT_TIMEOUT_FIRED=False))

    def boom(tv):
        raise UnitTimeout("45-minute limit")
    site.tables["T"] = (["2024"], boom)
    with pytest.raises(UnitTimeout):
        D._fetch_table_rows("T")


# ---- review R1297: the grammars 41 real tables use, the chunk outcomes, and every copy of the timeout re-raise ----

@pytest.mark.parametrize("code, want", [
    ("2007K2", (2007, 4, 1)), ("2026k4", (2026, 10, 1)), ("2007Q2", (2007, 4, 1)),      # kvartal = quarter
    ("2021U52", (2021, 12, 27)), ("2026U01", (2025, 12, 29)), ("2023W01", (2023, 1, 2)),  # uge = ISO week, Monday
    ("2021:2022", (2021, 12, 31)), ("2020/2021", (2020, 12, 31)), ("2003-2004", (2003, 12, 31)),  # one-year span
    ("2024 : 2025", (2024, 12, 31)),
])
def test_the_danish_and_split_year_grammars_are_dated(code, want):
    import datetime as dt
    assert ING.parse_date(code) == dt.date(*want)


@pytest.mark.parametrize("code", ["2007:2009", "2021:2025", "2022:2021", "2022:2022", "2007K5", "2007K0",
                                  "2021U54", "AUG - DEC 2019", "2021:22"])
def test_windows_and_impossible_codes_stay_undated(code):
    assert ING.parse_date(code) is None


def test_a_school_year_table_is_stored_not_owed(site):
    """SKOLM01-12, SCENE*, LABY21/41, HISB7/8/77/BR ... read 'unparsed' on every run before the span rule."""
    times = ["2021:2022", "2022:2023", "2023:2024"]
    site.tables["SKOLM01"] = (times, lambda tv: _jsonstat(tv, [1.0, 2.0, 3.0]))
    rows, transient = D._fetch_table_rows("SKOLM01")
    import datetime as dt
    assert transient is False and sorted(d for _, d, _ in rows) == [dt.date(y, 12, 31) for y in (2021, 2022, 2023)]


def test_why_empty_calls_only_windows_longer_than_a_year_span_time():
    """The span test is `y2 > y1 + 1`: a body of adjacent-year codes that yielded nothing is OUR gap, not a window."""
    assert ING._why_empty(_jsonstat(["2021:2022", "2022:2023"], [1.0, 2.0])) == "unparsed"
    assert ING._why_empty(_jsonstat(["2007:2009", "2021:2022"], [1.0, 2.0])) == "unparsed"   # mixed: not all windows
    assert ING._why_empty(_jsonstat(["2007:2009", "2008:2010"], [1.0, 2.0])) == "span_time"


def test_an_unparsed_chunk_fails_the_whole_table_never_keeping_the_others(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2024M01D01", "2024M01D02", "ODD1", "ODD2", "2024M01D05"]
    site.tables["MIX"] = (times, lambda tv: _jsonstat(tv, [1.0] * len(tv)))
    assert ING.query_table_detailed("MIX", _meta(times)["variables"]) == ([], "unparsed")
    assert D._fetch_table_rows("MIX") == ([], True)


def test_all_missing_chunks_are_an_empty_table_not_a_failure(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2024M01D01", "2024M01D02", "2024M01D03"]
    site.tables["NUL"] = (times, lambda tv: _jsonstat(tv, [None] * len(tv)))
    assert ING.query_table_detailed("NUL", _meta(times)["variables"]) == ([], "all_null")
    assert D._fetch_table_rows("NUL") == ([], False)


def test_a_window_chunk_does_not_discard_the_dated_chunks(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2020", "2021", "2007:2009", "2008:2010"]
    site.tables["WIN"] = (times, lambda tv: _jsonstat(tv, [1.0] * len(tv)))
    rows, outcome = ING.query_table_detailed("WIN", _meta(times)["variables"])
    assert outcome == "ok" and len(rows) == 2
    only = ["2007:2009", "2008:2010", "2009:2011"]
    site.tables["WIN2"] = (only, lambda tv: _jsonstat(tv, [1.0] * len(tv)))
    assert ING.query_table_detailed("WIN2", _meta(only)["variables"]) == ([], "span_time")


@pytest.fixture
def fence(monkeypatch):
    """A stand-in orchestrator: its UnitTimeout class and the fired flag, as the real one exposes them."""
    import types

    class UnitTimeout(Exception):
        pass
    orch = types.SimpleNamespace(UnitTimeout=UnitTimeout, UNIT_TIMEOUT_FIRED=False)
    monkeypatch.setitem(sys.modules, "updater.orchestrate", orch)
    return orch


def test_the_timeout_inside_the_tableinfo_get_ends_the_pass(site, fence, monkeypatch):
    def boom(url, retries=3):
        raise fence.UnitTimeout("45-minute limit")
    monkeypatch.setattr(ING, "get_json", boom)
    with pytest.raises(fence.UnitTimeout):
        D._fetch_table_rows("ANY")


def test_the_timeout_inside_the_store_scan_ends_the_pass(fence, monkeypatch):
    monkeypatch.setattr(D, "_store_names", lambda: ["FOLK.parquet"])

    def boom(p):
        raise fence.UnitTimeout("45-minute limit")
    monkeypatch.setattr(D.blob, "read_table", boom)
    with pytest.raises(fence.UnitTimeout):
        D._global_max_date()


def test_a_plain_error_after_the_timeout_fired_is_the_timeout(site, fence, monkeypatch):
    """The flag branch: the trip can surface as another library's exception."""
    fence.UNIT_TIMEOUT_FIRED = True

    def boom(url, retries=3):
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(ING, "get_json", boom)
    with pytest.raises(RuntimeError, match="Query interrupted"):
        D._fetch_table_rows("ANY")


def test_negative_control_a_plain_error_without_the_timeout_is_one_transient_table(site, fence, monkeypatch):
    def boom(url, retries=3):
        raise RuntimeError("connection reset")
    monkeypatch.setattr(ING, "get_json", boom)
    assert D._fetch_table_rows("ANY") == ([], True)


def test_a_timeout_swallowed_inside_a_call_that_returned_normally_still_ends_the_pass(site, fence, monkeypatch):
    """The alarm lands inside requests, a library absorbs it, and the call returns None: the flag is the evidence."""
    def swallowed(url, retries=3):
        fence.UNIT_TIMEOUT_FIRED = True
        return None
    monkeypatch.setattr(ING, "get_json", swallowed)
    with pytest.raises(fence.UnitTimeout, match="tableinfo"):
        D._fetch_table_rows("ANY")
    fence.UNIT_TIMEOUT_FIRED = False
    site.tables["P"] = (["2024"], lambda tv: (setattr(fence, "UNIT_TIMEOUT_FIRED", True), None)[1])
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: _meta(["2024"]))
    with pytest.raises(fence.UnitTimeout, match="data POST"):
        D._fetch_table_rows("P")


@pytest.mark.parametrize("fn, args", [("get_json", ("https://x/tableinfo",)), ("post_json", ("https://x/data", {}))])
def test_the_ingesters_own_network_helpers_let_the_timeout_through(fence, monkeypatch, fn, args):
    """R1297 Defect 2: get_json/post_json caught every Exception around requests - where the alarm usually lands -
    and returned None, so the pass ran on past its limit. Both directions: the fence class, and anything once
    the flag is set; a plain network error with no fence still returns None after the retries."""
    import requests
    monkeypatch.setattr(ING.time, "sleep", lambda s: None)
    verb = "get" if fn == "get_json" else "post"

    def trip(*a, **k):
        raise fence.UnitTimeout("45-minute limit")
    monkeypatch.setattr(requests, verb, trip)
    with pytest.raises(fence.UnitTimeout):
        getattr(ING, fn)(*args)
    calls = []

    def reset(*a, **k):
        calls.append(1)
        raise requests.ConnectionError("reset")
    monkeypatch.setattr(requests, verb, reset)
    assert getattr(ING, fn)(*args) is None and len(calls) == 3        # negative control: retried, then None
    fence.UNIT_TIMEOUT_FIRED = True
    with pytest.raises(requests.ConnectionError):
        getattr(ING, fn)(*args)


def test_a_timeout_mid_parse_is_not_a_short_table(fence, monkeypatch):
    fence.UNIT_TIMEOUT_FIRED = True

    def boom(*a, **k):
        raise RuntimeError("interrupted mid-parse")
    monkeypatch.setattr(ING._pxweb, "resolve_time_dim", boom)
    with pytest.raises(RuntimeError, match="mid-parse"):
        ING.parse_jsonstat(_jsonstat(["2024"], [1.0]), "T")


def test_update_keeps_a_failed_table_owed_and_reads_partial(site, tmp_path, monkeypatch):
    """End to end: the failed table's manifest entry does NOT advance and the pass is partial; the good one
    advances."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    cat = [{"id": "GOOD1", "updated": "2026-09-01T08:00:00"}, {"id": "FAIL1", "updated": "2026-09-02T08:00:00"}]
    monkeypatch.setattr(D, "_fetch_catalog", lambda *a, **k: [dict(t) for t in cat])
    site.tables["GOOD1"] = (["2024", "2025"], lambda tv: _jsonstat(tv, [1.0, 2.0]))
    site.tables["FAIL1"] = (["2024", "2025"], lambda tv: None)
    res = D.update(None, None)
    assert res.status == "partial" and "FAIL1" in (res.error or ""), (res.status, res.error)
    man = D._load_manifest()["tables"]
    assert man.get("GOOD1") == "2026-09-01T08:00:00" and "FAIL1" not in man, man
