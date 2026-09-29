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
    site.tables["ODD"] = (["AUG - DEC 2019", "AUG - DEC 2020"], lambda tv: _jsonstat(tv, [5.0, 6.0]))
    assert ING.query_table_detailed("ODD", _meta(["AUG - DEC 2019", "AUG - DEC 2020"])["variables"])[1] == "unparsed"
    assert D._fetch_table_rows("ODD") == ([], True)


def test_a_post_too_big_for_one_request_is_retried_in_time_chunks(site, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 3)
    times = [f"2024M01D{d:02d}" for d in range(1, 11)]                        # 10 daily values
    site.tables["DNV"] = (times, lambda tv: None if len(tv) > 3 else _jsonstat(tv, [1.0] * len(tv)))
    rows, transient = D._fetch_table_rows("DNV")
    assert transient is False and len(rows) == 10
    assert site.posts[0] == ("DNV", 10) and [n for _, n in site.posts[1:]] == [3, 3, 3, 1]


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
