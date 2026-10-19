"""dst: when DST's JSONSTAT data POST answers HTTP 500, fetch the same selection in BULK format.

Since 2026-09-30 DST answers EVERY data POST for VNASFK and VNKSFK - one cell included, JSONSTAT and CSV alike - with
HTTP 500 {"errorTypeCode":"SYSTEM-100"}, while its streaming BULK format answers 200 (measured live 2026-10-19; 2 of
the 23 tables with a version variable plus NASFK; the control FOLK1A answers 200 in all three formats). The fallback must give
exactly the rows the JSONSTAT path gives, fire only on a final HTTP 500, and be counted where an operator reads it.

The fixtures in tests/fixtures/dst_bulk/ are live api.statbank.dk answers (CC BY 4.0) recorded 2026-10-19: for each
control table, the fetcher's own selection in JSONSTAT and in BULK (valuePresentation=Code); for VNASFK/VNKSFK the
BULK body only (JSONSTAT answered 500). Hermetic: requests.post is faked; no network.
"""
from __future__ import annotations

import json
import os
import re
import sys

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import dst as D  # noqa: E402
from jobs import ingest_dst as ING  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "dst_bulk")
# Tables that answered 200 in BOTH formats; together they reach every parse branch: quarterly 'K' codes (FOLK1A,
# REGR63), half-years (ANB10), Danish weeks 'U' (VAN1UGE), months with decimals (PRIS111), a version table (VNKN1),
# '..' missing values (ITAV22), cells BULK leaves out and JSON-stat fills with the suppressed value (VAN1UGE 231 of
# 248, REGR63 57 of 1,232), and a table whose every period is an undated window (BARLOV1).
CONTROLS = ["FOLK1A", "VNKN1", "VAN1UGE", "REGR63", "ITAV22", "BARLOV1", "ANB10", "PRIS111"]
FAILING = {"VNASFK": 3, "VNKSFK": 14}          # rows each yields from its recorded BULK body (fetcher selection)


def _fx(tid):
    with open(os.path.join(FIX, f"{tid}.json"), encoding="utf-8") as fh:
        return json.load(fh)


class _Resp:
    def __init__(self, status, payload=None, text=None):
        self.status_code = status
        self._payload = payload
        self.content = (text if text is not None else json.dumps(payload or {})).encode("utf-8")

    def json(self):
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


@pytest.fixture
def wire(monkeypatch):
    """Fake DST /v1/data. `wire.json_answers[tid]` is a list of JSONSTAT answers (one per attempt, last repeats):
    an int status, an Exception, or a dict body. `wire.bulk[tid]` is (status, text). Records every request."""
    w = type("Wire", (), {})()
    w.json_answers, w.bulk, w.calls = {}, {}, []

    def post(url, json=None, headers=None, timeout=None):
        tid, fmt = json["table"], json["format"]
        w.calls.append((tid, fmt, json))
        if fmt == "BULK":
            st, text = w.bulk[tid]
            return _Resp(st, text=text) if st == 200 else _Resp(st, {"errorTypeCode": "X"})
        seq = w.json_answers[tid]
        n = sum(1 for c in w.calls if c[0] == tid and c[1] == "JSONSTAT")
        ans = seq[min(n - 1, len(seq) - 1)]
        if isinstance(ans, Exception):
            raise ans
        if isinstance(ans, int):
            return _Resp(ans, {"errorTypeCode": "SYSTEM-100" if ans == 500 else "OTHER", "message": "x"})
        return _Resp(200, ans)
    monkeypatch.setattr(ING.requests, "post", post)
    monkeypatch.setattr(ING.time, "sleep", lambda s: None)
    monkeypatch.setattr(ING, "RATE", 0)
    monkeypatch.setattr(D, "RATE", 0)
    ING.BULK_FALLBACKS.clear()
    w.fmts = lambda tid: [c[1] for c in w.calls if c[0] == tid]
    return w


# ---- T1: BULK rows == JSONSTAT rows, exactly, on tables that answer both ----

@pytest.mark.parametrize("tid", CONTROLS)
def test_bulk_gives_exactly_the_jsonstat_rows(tid):
    fx = _fx(tid)
    variables = fx["tableinfo_variables"]
    sel = ING._query_selection(variables)
    want = ING.parse_jsonstat(fx["jsonstat"], tid)
    got = ING.parse_bulk(fx["bulk"], tid, variables, sel, fx["suppressedDataValue"])
    assert got is not None
    assert sorted(got) == sorted(want)                         # same multiset of (series_key, obs_date, value)


def test_the_controls_are_not_vacuous():
    n = {t: len(ING.parse_jsonstat(_fx(t)["jsonstat"], t)) for t in CONTROLS}
    assert sum(n.values()) == 3_834 and n["BARLOV1"] == 0 and all(v > 0 for t, v in n.items() if t != "BARLOV1"), n


def test_cells_bulk_leaves_out_are_filled_with_the_suppressed_value_and_only_then_equal():
    fx = _fx("VAN1UGE")
    sel = ING._query_selection(fx["tableinfo_variables"])
    bare = ING.parse_bulk(fx["bulk"], "VAN1UGE", fx["tableinfo_variables"], sel)          # no fill
    assert len(bare) == 17 and len(ING.parse_jsonstat(fx["jsonstat"], "VAN1UGE")) == 248
    nonnum = ING.parse_bulk(fx["bulk"], "VAN1UGE", fx["tableinfo_variables"], sel, "..")   # '..' fills nothing
    assert sorted(nonnum) == sorted(bare)


@pytest.mark.parametrize("tid", CONTROLS)
def test_an_empty_bulk_answer_is_named_as_jsonstat_names_it(tid):
    fx = _fx(tid)
    sel = ING._query_selection(fx["tableinfo_variables"])
    if ING.parse_jsonstat(fx["jsonstat"], tid):
        return
    assert ING._why_empty_bulk(fx["bulk"], sel, fx["suppressedDataValue"]) == ING._why_empty(fx["jsonstat"])


def test_missing_and_non_numeric_bulk_values_are_skipped_like_jsonstat_nulls():
    sel = [{"code": "X", "values": ["A", "B", "C"], "_time": False, "_all": True},
           {"code": "Tid", "values": ["2024"], "_time": True, "_all": True}]
    variables = [{"id": "X", "values": [{"id": "A"}, {"id": "B"}, {"id": "C"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024", "text": "2024"}]}]
    text = "X;TID;INDHOLD\r\nA;2024;..\r\nB;2024;\r\nC;2024;1.5\r\n"
    rows = ING.parse_bulk(text, "T", variables, sel, "0")
    import datetime as dt
    assert rows == [("DST:T:X=C", dt.date(2024, 12, 31), 1.5)]
    assert ING._why_empty_bulk("X;TID;INDHOLD\r\nA;2024;..\r\n", sel[:1] + sel[1:], None) == "all_null"


def test_a_byte_order_mark_does_not_break_the_header():
    """DST's CSV format starts with a BOM (FOLK1A, measured); BULK did not, but the header check must not depend on it."""
    fx = _fx("FOLK1A")
    sel = ING._query_selection(fx["tableinfo_variables"])
    plain = ING.parse_bulk(fx["bulk"], "FOLK1A", fx["tableinfo_variables"], sel, "0")
    assert ING.parse_bulk(chr(0xFEFF) + fx["bulk"], "FOLK1A", fx["tableinfo_variables"], sel, "0") == plain


def test_a_positional_time_code_is_dated_from_its_tableinfo_label():
    sel = [{"code": "Tid", "values": ["0", "1"], "_time": True, "_all": True}]
    variables = [{"id": "Tid", "time": True, "values": [{"id": "0", "text": "2020"}, {"id": "1", "text": "2021"}]}]
    rows = ING.parse_bulk("TID;INDHOLD\r\n0;1\r\n1;2\r\n", "P", variables, sel)
    import datetime as dt
    assert [d for _, d, _ in rows] == [dt.date(2020, 12, 31), dt.date(2021, 12, 31)]


def test_an_all_left_out_window_table_is_span_time_as_in_jsonstat_not_all_null():
    """JSON-stat writes the suppressed 0 into every cell, so a window-only table reads span_time there; a BULK body
    that left every cell out must read the same, and only reads all_null when nothing would be filled."""
    sel = [{"code": "Tid", "values": ["2007:2009", "2008:2010"], "_time": True, "_all": True}]
    assert ING._why_empty_bulk("TID;INDHOLD\r\n", sel, "0") == "span_time"
    assert ING._why_empty_bulk("TID;INDHOLD\r\n", sel, None) == "all_null"
    assert ING._why_empty_bulk("TID;INDHOLD\r\n2007:2009;..\r\n2008:2010;..\r\n", sel, "0") == "all_null"


# ---- T5: shapes the parser refuses, loudly ----

@pytest.mark.parametrize("header", ["OMRÅDE;KØN;ALDER;CIVILSTAND;INDHOLD",            # a column missing
                                    "KØN;OMRÅDE;ALDER;CIVILSTAND;TID;INDHOLD",        # columns reordered
                                    "OMRÅDE;KØN;ALDER;CIVILSTAND;TID;VALUE"])         # value column renamed
def test_a_body_without_the_requested_columns_is_refused_not_guessed(header):
    fx = _fx("FOLK1A")
    sel = ING._query_selection(fx["tableinfo_variables"])
    body = header + "\r\n" + "\r\n".join(fx["bulk"].splitlines()[1:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], sel, "0") is None


def test_a_short_row_is_refused():
    fx = _fx("FOLK1A")
    sel = ING._query_selection(fx["tableinfo_variables"])
    lines = fx["bulk"].splitlines()
    body = "\r\n".join(lines[:2] + ["000;TOT;IALT;2008K1;5"] + lines[3:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], sel, "0") is None


# ---- T2: the fallback fires on a final HTTP 500 and on nothing else ----

def _folk(wire, answers):
    fx = _fx("FOLK1A")
    wire.json_answers["FOLK1A"] = answers
    wire.bulk["FOLK1A"] = (200, fx["bulk"])
    return ING.query_table_detailed("FOLK1A", fx["tableinfo_variables"], suppressed="0")


def test_a_final_500_falls_back_to_bulk_with_codes_and_the_same_selection(wire):
    rows, why = _folk(wire, [500])
    assert why == "ok" and len(rows) == 75
    assert wire.fmts("FOLK1A") == ["JSONSTAT"] * 3 + ["BULK"]          # 3 tries, then ONE bulk request
    js_body = next(c[2] for c in wire.calls if c[1] == "JSONSTAT")
    bk_body = next(c[2] for c in wire.calls if c[1] == "BULK")
    assert bk_body["valuePresentation"] == "Code" and bk_body["variables"] == js_body["variables"]
    assert ING.BULK_FALLBACKS == ["FOLK1A"]
    assert sorted(rows) == sorted(ING.parse_jsonstat(_fx("FOLK1A")["jsonstat"], "FOLK1A"))


@pytest.mark.parametrize("answers", [[400], [403], [404], [429], [502], [503], [504],
                                     [requests.Timeout("t")], [requests.ConnectionError("c")],
                                     [500, 500, 503], [500, 500, requests.Timeout("t")]])
def test_no_other_failure_falls_back(wire, answers):
    rows, why = _folk(wire, answers)
    assert (rows, why) == ([], "failed")
    assert "BULK" not in wire.fmts("FOLK1A") and ING.BULK_FALLBACKS == []


@pytest.mark.parametrize("answers", [[503, 503, 500], [requests.Timeout("t"), 500, 500]])
def test_it_is_the_final_answer_that_counts(wire, answers):
    rows, why = _folk(wire, answers)
    assert why == "ok" and wire.fmts("FOLK1A")[-1] == "BULK"


def test_a_200_never_falls_back_even_when_it_parses_to_nothing(wire):
    fx = _fx("BARLOV1")
    wire.json_answers["BARLOV1"] = [fx["jsonstat"]]
    wire.bulk["BARLOV1"] = (200, fx["bulk"])
    assert ING.query_table_detailed("BARLOV1", fx["tableinfo_variables"], suppressed="0") == ([], "span_time")
    wire.json_answers["FOLK1A"] = [500, 500, _fx("FOLK1A")["jsonstat"]]           # a 500 that recovers
    assert _folk(wire, wire.json_answers["FOLK1A"])[1] == "ok"
    assert "BULK" not in wire.fmts("BARLOV1") + wire.fmts("FOLK1A") and ING.BULK_FALLBACKS == []


def test_a_failed_bulk_request_leaves_the_table_failed(wire):
    fx = _fx("FOLK1A")
    wire.json_answers["FOLK1A"] = [500]
    wire.bulk["FOLK1A"] = (500, "")
    assert ING.query_table_detailed("FOLK1A", fx["tableinfo_variables"], suppressed="0") == ([], "failed")
    assert ING.BULK_FALLBACKS == ["FOLK1A"]                       # it was tried, and that is counted


@pytest.mark.parametrize("status, tries", [(400, 1), (403, 1), (404, 1), (500, 3), (503, 3)])
def test_a_bulk_request_error_is_retried_only_when_retrying_can_help(wire, status, tries):
    fx = _fx("FOLK1A")
    wire.json_answers["FOLK1A"] = [500]
    wire.bulk["FOLK1A"] = (status, "")
    assert ING.query_table_detailed("FOLK1A", fx["tableinfo_variables"], suppressed="0") == ([], "failed")
    assert wire.fmts("FOLK1A").count("BULK") == tries


def test_a_bulk_body_with_the_wrong_columns_is_unparsed_not_rows(wire):
    fx = _fx("FOLK1A")
    wire.json_answers["FOLK1A"] = [500]
    wire.bulk["FOLK1A"] = (200, "A;B\r\n1;2\r\n")
    assert ING.query_table_detailed("FOLK1A", fx["tableinfo_variables"], suppressed="0") == ([], "unparsed")


def test_a_chunked_table_falls_back_for_the_failing_chunk_only(wire, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2021", "2022", "2023"]
    variables = [{"id": "X", "values": [{"id": "A"}]},
                 {"id": "Tid", "time": True, "values": [{"id": t, "text": t} for t in times]}]

    def js(tv, vals):
        return {"dataset": {"dimension": {
            "X": {"category": {"index": {"A": 0}}},
            "Tid": {"category": {"index": {t: i for i, t in enumerate(tv)}}},
            "id": ["X", "Tid"], "size": [1, len(tv)], "role": {"time": ["Tid"]}}, "value": vals}}
    answers = iter([js(["2021", "2022"], [1.0, 2.0]), 500, 500, 500])
    real_post = ING.requests.post

    def post(url, json=None, headers=None, timeout=None):
        if json["format"] == "BULK":
            wire.calls.append(("CH", "BULK", json))
            assert next(v["values"] for v in json["variables"] if v["code"] == "Tid") == ["2023"]
            return _Resp(200, text="X;TID;INDHOLD\r\nA;2023;3\r\n")
        wire.calls.append(("CH", "JSONSTAT", json))
        a = next(answers)
        return _Resp(a, {"errorTypeCode": "SYSTEM-100"}) if isinstance(a, int) else _Resp(200, a)
    monkeypatch.setattr(ING.requests, "post", post)
    rows, why = ING.query_table_detailed("CH", variables, suppressed="0")
    assert why == "ok" and sorted(v for _, _, v in rows) == [1.0, 2.0, 3.0]
    assert wire.fmts("CH") == ["JSONSTAT", "JSONSTAT", "JSONSTAT", "JSONSTAT", "BULK"]
    assert real_post is not None


def test_a_stale_500_from_an_earlier_call_cannot_trigger_the_fallback(wire, monkeypatch):
    """post_json's answer is read per request: a None from a call that never reached the network (a stand-in, or a
    path that returns early) must not inherit the 500 an EARLIER request left behind."""
    ING._LAST_POST.update(status=500, code="SYSTEM-100")
    monkeypatch.setattr(ING, "post_json", lambda url, body, retries=3: None)
    wire.bulk["FOLK1A"] = (200, _fx("FOLK1A")["bulk"])
    assert ING.query_table_detailed("FOLK1A", _fx("FOLK1A")["tableinfo_variables"], suppressed="0") == ([], "failed")
    assert "BULK" not in wire.fmts("FOLK1A")


def test_the_note_counts_a_table_once_however_many_chunks_fell_back(monkeypatch):
    monkeypatch.setattr(ING, "BULK_FALLBACKS", ["DNVALD", "DNVALD", "VNASFK"])
    assert D._bulk_fallback_note() == "BULK fallback for 2 table(s) after a JSONSTAT HTTP 500: [DNVALD, VNASFK]"
    monkeypatch.setattr(ING, "BULK_FALLBACKS", [])
    assert D._bulk_fallback_note() == ""


def test_the_fetcher_hands_the_suppressed_value_to_the_fallback(wire, monkeypatch):
    fx = _fx("VAN1UGE")
    wire.json_answers["VAN1UGE"] = [500]
    wire.bulk["VAN1UGE"] = (200, fx["bulk"])
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": fx["suppressedDataValue"]})
    rows, transient = D._fetch_table_rows("VAN1UGE")
    assert transient is False and sorted(rows) == sorted(ING.parse_jsonstat(fx["jsonstat"], "VAN1UGE"))


# ---- T3: the two tables that fail today parse ----

@pytest.mark.parametrize("tid", sorted(FAILING))
def test_the_failing_tables_come_back_through_the_fallback(wire, monkeypatch, tid):
    fx = _fx(tid)
    assert fx["jsonstat_http_status"] == 500 and fx["jsonstat_error_code"] == "SYSTEM-100"
    wire.json_answers[tid] = [500]
    wire.bulk[tid] = (200, fx["bulk"])
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": "0"})
    rows, transient = D._fetch_table_rows(tid)
    assert transient is False and len(rows) == FAILING[tid]
    shape = re.compile(rf"^DST:{tid}:VERSION=[^:]+:SEKTORNAT=[^:]+:KONTO1=[^:]+:BALANCE=[^:]+:FININS=[^:]+"
                       r":MODSEKNAT=[^:]+$")
    assert all(shape.match(k) for k, _, _ in rows)
    assert ING.BULK_FALLBACKS == [tid]


# ---- T4: counted and named in the pass's note, never silent; status unchanged ----

def _run_update(wire, monkeypatch, tmp_path, fallback: bool):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    cat = [{"id": "FOLK1A", "updated": "2026-08-10T08:00:00"}, {"id": "VNASFK", "updated": "2026-09-30T08:00:00"}]
    monkeypatch.setattr(D, "_fetch_catalog", lambda *a, **k: [dict(t) for t in cat])
    meta = {t: {"variables": _fx(t)["tableinfo_variables"], "suppressedDataValue": "0"} for t in ("FOLK1A", "VNASFK")}
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: meta[url.split("id=")[1].split("&")[0]])
    wire.json_answers["FOLK1A"] = [_fx("FOLK1A")["jsonstat"]]
    wire.bulk["VNASFK"] = (200, _fx("VNASFK")["bulk"])
    if fallback:
        wire.json_answers["VNASFK"] = [500]
    else:                                  # the same table answering in JSONSTAT: a minimal valid body
        wire.json_answers["VNASFK"] = [{"dataset": {"dimension": {
            "VERSION": {"category": {"index": {"V": 0}}}, "Tid": {"category": {"index": {"2024": 0}}},
            "id": ["VERSION", "Tid"], "size": [1, 1], "role": {"time": ["Tid"]}}, "value": [1.0]}}]
    ING.BULK_FALLBACKS[:] = ["STALE_FROM_AN_EARLIER_PASS"]
    return D.update(None, None)


def test_a_pass_that_falls_back_names_it_and_stays_ok(wire, monkeypatch, tmp_path, capsys):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=True)
    assert res.status == "ok", (res.status, res.error)
    assert "BULK fallback for 1 table(s) after a JSONSTAT HTTP 500: [VNASFK]" in (res.error or "")
    assert "STALE_FROM_AN_EARLIER_PASS" not in (res.error or "")
    out = capsys.readouterr().out
    assert "BULK fallback used for 1 table(s)" in out and "JSONSTAT POST answered HTTP 500 (SYSTEM-100)" in out
    assert "DST:VNASFK" in (res.series_cursors or {}), res.series_cursors      # merged: the subject's cursor


def test_a_pass_without_a_fallback_says_nothing_about_bulk(wire, monkeypatch, tmp_path):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=False)
    assert res.status == "ok" and "BULK" not in (res.error or ""), (res.status, res.error)
    assert "BULK" not in [f for c in wire.calls for f in [c[1]]]
