"""dst: when DST's JSONSTAT data POST answers HTTP 500, fetch the same selection in BULK format.

Since 2026-09-30 DST answers EVERY data POST for VNASFK and VNKSFK - one cell included, JSONSTAT and CSV alike - with
HTTP 500 {"errorTypeCode":"SYSTEM-100"}, while its streaming BULK format answers 200 (measured live 2026-10-19; 2 of
the 23 tables with a version variable plus NASFK; the control FOLK1A answers 200 in all three formats). The fallback
must give the rows the JSONSTAT path gives for every cell BULK returns, fire only on a final HTTP 500, and be counted
where an operator reads it.

WHY THE EQUALITY IS "FOR EVERY CELL BULK RETURNS" AND NOT "THE SAME ROWS". BULK streams only the cells a table holds.
JSON-stat is dense and writes a cell DST does not hold as the table's suppressedDataValue ("0"): a zero nobody
published. The BULK path does not copy those zeros (review AR-348: copying them also turns a cut-short body into
zeros with the full row count); it counts them instead. So the tests pin: BULK rows are a sub-multiset of the
JSON-stat rows, every JSON-stat row BULK lacks is one of those 0.0 cells, and their number is exactly the count of
selected cells BULK did not stream.

The fixtures in tests/fixtures/dst_bulk/ are live api.statbank.dk answers (CC BY 4.0) recorded 2026-10-19: for each
control table, the fetcher's own selection in JSONSTAT and in BULK (valuePresentation=Code); for VNASFK/VNKSFK the
BULK body only (JSONSTAT answered 500). Hermetic: requests.post is faked; no network.
"""
from __future__ import annotations

import collections
import datetime as dt
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
# '..' missing values (ITAV22), cells BULK does not stream (VAN1UGE, REGR63), and a table whose every period is an
# undated window (BARLOV1).
CONTROLS = ["FOLK1A", "VNKN1", "VAN1UGE", "REGR63", "ITAV22", "BARLOV1", "ANB10", "PRIS111"]
# rows each failing table yields from its recorded BULK body (fetcher selection), and its selected cells
FAILING = {"VNASFK": (3, 31), "VNKSFK": (14, 110)}


def _fx(tid):
    with open(os.path.join(FIX, f"{tid}.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _sel(fx):
    return ING._query_selection(fx["tableinfo_variables"])


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
    an int status, an Exception, or a dict body. `wire.bulk[tid]` is a list of BULK answers the same way: an int
    status, an Exception, or a str body. Records every request."""
    w = type("Wire", (), {})()
    w.json_answers, w.bulk, w.calls = {}, {}, []

    def _pick(seq, tid, fmt):
        n = sum(1 for c in w.calls if c[0] == tid and c[1] == fmt)
        return seq[min(n - 1, len(seq) - 1)]

    def post(url, json=None, headers=None, timeout=None):
        tid, fmt = json["table"], json["format"]
        w.calls.append((tid, fmt, json))
        ans = _pick(w.bulk[tid] if fmt == "BULK" else w.json_answers[tid], tid, fmt)
        if isinstance(ans, Exception):
            raise ans
        if isinstance(ans, int):
            return _Resp(ans, {"errorTypeCode": "SYSTEM-100" if ans == 500 else "OTHER", "message": "x"})
        return _Resp(200, text=ans) if fmt == "BULK" else _Resp(200, ans)
    monkeypatch.setattr(ING.requests, "post", post)
    monkeypatch.setattr(ING.time, "sleep", lambda s: None)
    monkeypatch.setattr(ING, "RATE", 0)
    monkeypatch.setattr(D, "RATE", 0)
    ING.BULK_FALLBACKS.clear()
    w.fmts = lambda tid: [c[1] for c in w.calls if c[0] == tid]
    return w


# ---- T1: BULK rows == JSON-stat rows for every cell BULK returns ----

@pytest.mark.parametrize("tid", CONTROLS)
def test_bulk_gives_the_jsonstat_rows_of_every_cell_it_streams(tid):
    fx = _fx(tid)
    sel = _sel(fx)
    want = collections.Counter(ING.parse_jsonstat(fx["jsonstat"], tid))
    rows, omitted = ING._bulk_parse(fx["bulk"], tid, fx["tableinfo_variables"], sel)
    got = collections.Counter(rows)
    assert not (got - want), f"BULK rows that JSON-stat does not have: {list((got - want).elements())[:3]}"
    extra = list((want - got).elements())
    # every JSON-stat row BULK lacks is a cell BULK did not stream, written by JSON-stat as the suppressed 0
    assert all(v == 0.0 for _, _, v in extra), extra[:3]
    dated = all(ING.parse_date(c) for c in next(s for s in sel if s.get("_time"))["values"])
    if dated:
        assert len(extra) == omitted, (len(extra), omitted)


def test_the_controls_are_not_vacuous_and_show_the_unstreamed_cells():
    n_json = {t: len(ING.parse_jsonstat(_fx(t)["jsonstat"], t)) for t in CONTROLS}
    parsed = {t: ING._bulk_parse(_fx(t)["bulk"], t, _fx(t)["tableinfo_variables"], _sel(_fx(t))) for t in CONTROLS}
    assert sum(n_json.values()) == 3_834 and n_json["BARLOV1"] == 0
    assert sum(len(r) for r, _ in parsed.values()) == 3_527          # 3,834 - 231 - 57 - 19 unstreamed fills
    assert parsed["ANB10"][1] == 19
    assert parsed["VAN1UGE"][1] == 231 and len(parsed["VAN1UGE"][0]) == 17       # 231 of 248 JSON-stat rows are fills
    assert parsed["REGR63"][1] == 57 and parsed["FOLK1A"][1] == 0


@pytest.mark.parametrize("tid", sorted(FAILING))
def test_the_failing_tables_lose_nothing(tid):
    fx = _fx(tid)
    rows, omitted = ING._bulk_parse(fx["bulk"], tid, fx["tableinfo_variables"], _sel(fx))
    assert (len(rows), omitted, ING._n_cells(_sel(fx))) == (FAILING[tid][0], 0, FAILING[tid][1])


def test_a_body_cut_short_shows_as_unstreamed_cells_not_as_zeros():
    """Review AR-348 G1: half of REGR63's body. With the old suppressed-value fill it gave 1,232 rows (JSON-stat's
    count) with 498 real values turned into 0.0. Now it gives only what arrived, and says how much did not."""
    fx = _fx("REGR63")
    lines = fx["bulk"].splitlines()
    half = "\r\n".join(lines[: 1 + (len(lines) - 1) // 2]) + "\r\n"
    rows, omitted = ING._bulk_parse(half, "REGR63", fx["tableinfo_variables"], _sel(fx))
    assert len(rows) == 587 and omitted == 645
    assert all(r in ING.parse_jsonstat(fx["jsonstat"], "REGR63") for r in rows)


@pytest.mark.parametrize("tid", ["BARLOV1"])
def test_an_empty_bulk_answer_is_named_as_jsonstat_names_it(tid):
    fx = _fx(tid)
    assert ING.parse_jsonstat(fx["jsonstat"], tid) == [] and ING.parse_bulk(fx["bulk"], tid, fx["tableinfo_variables"],
                                                                            _sel(fx)) == []
    assert ING._why_empty_bulk(fx["bulk"], _sel(fx)) == ING._why_empty(fx["jsonstat"]) == "span_time"


def test_missing_and_non_numeric_bulk_values_are_skipped_like_jsonstat_nulls():
    sel = [{"code": "X", "values": ["A", "B", "C"], "_time": False, "_all": True},
           {"code": "Tid", "values": ["2024"], "_time": True, "_all": True}]
    variables = [{"id": "X", "values": [{"id": "A"}, {"id": "B"}, {"id": "C"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024", "text": "2024"}]}]
    text = "X;TID;INDHOLD\r\nA;2024;..\r\nB;2024;\r\nC;2024;1.5\r\n"
    assert ING._bulk_parse(text, "T", variables, sel) == ([("DST:T:X=C", dt.date(2024, 12, 31), 1.5)], 0)
    assert ING._why_empty_bulk("X;TID;INDHOLD\r\nA;2024;..\r\n", sel) == "all_null"


def test_operator_looking_values_parse_and_count_over_the_real_values():
    """AKU240K-shaped: '<15' and '>48' are sent as the '*' wildcard (_wire_values), but the body carries the real
    codes, and the membership check and the cell count must use the real values (review AR-348 Q)."""
    variables = [{"id": "ARBEJDSTID", "values": [{"id": "<15"}, {"id": "15-36"}, {"id": ">48"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024K1", "text": "2024Q1"}]}]
    sel = ING._query_selection(variables)
    assert ING._wire_values(sel[0]) == ["*"]
    text = "ARBEJDSTID;TID;INDHOLD\r\n<15;2024K1;3\r\n>48;2024K1;4\r\n"
    rows, omitted = ING._bulk_parse(text, "AKU240K", variables, sel)
    assert sorted(rows) == [("DST:AKU240K:ARBEJDSTID=<15", dt.date(2024, 1, 1), 3.0),
                            ("DST:AKU240K:ARBEJDSTID=>48", dt.date(2024, 1, 1), 4.0)]
    assert omitted == 1                                                     # '15-36' not streamed, not ['*']


def test_a_byte_order_mark_does_not_break_the_header():
    """DST's CSV format starts with a BOM (FOLK1A, measured); BULK did not, but the header check must not depend on it."""
    fx = _fx("FOLK1A")
    plain = ING.parse_bulk(fx["bulk"], "FOLK1A", fx["tableinfo_variables"], _sel(fx))
    assert ING.parse_bulk(chr(0xFEFF) + fx["bulk"], "FOLK1A", fx["tableinfo_variables"], _sel(fx)) == plain


def test_a_positional_time_code_is_dated_from_its_tableinfo_label():
    sel = [{"code": "Tid", "values": ["0", "1"], "_time": True, "_all": True}]
    variables = [{"id": "Tid", "time": True, "values": [{"id": "0", "text": "2020"}, {"id": "1", "text": "2021"}]}]
    rows = ING.parse_bulk("TID;INDHOLD\r\n0;1\r\n1;2\r\n", "P", variables, sel)
    assert [d for _, d, _ in rows] == [dt.date(2020, 12, 31), dt.date(2021, 12, 31)]


# ---- T5: shapes the parser refuses, loudly ----

@pytest.mark.parametrize("header", ["OMRÅDE;KØN;ALDER;CIVILSTAND;INDHOLD",            # a column missing
                                    "KØN;OMRÅDE;ALDER;CIVILSTAND;TID;INDHOLD",        # columns reordered
                                    "OMRÅDE;KØN;ALDER;CIVILSTAND;TID;VALUE"])         # value column renamed
def test_a_body_without_the_requested_columns_is_refused_not_guessed(header):
    fx = _fx("FOLK1A")
    body = header + "\r\n" + "\r\n".join(fx["bulk"].splitlines()[1:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], _sel(fx)) is None


@pytest.mark.parametrize("short", ["000;TOT;IALT;2008K1;5", "000;5", "000"])
def test_a_short_row_is_refused_not_raised(short):
    fx = _fx("FOLK1A")
    lines = fx["bulk"].splitlines()
    body = "\r\n".join(lines[:2] + [short] + lines[3:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], _sel(fx)) is None


@pytest.mark.parametrize("plant", ["6100811 ", "9999999", "6100811.0"])
def test_a_code_outside_the_selection_is_refused_not_minted(plant):
    """Review AR-348 G2: one REGR63 record with a re-formatted code was accepted and minted a new series_key."""
    fx = _fx("REGR63")
    lines = fx["bulk"].splitlines()
    i = next(k for k, ln in enumerate(lines) if ln.startswith("6100811;"))
    lines[i] = plant + lines[i][len("6100811"):]
    body = "\r\n".join(lines) + "\r\n"
    assert ING.parse_bulk(body, "REGR63", fx["tableinfo_variables"], _sel(fx)) is None
    assert ING.parse_bulk(fx["bulk"], "REGR63", fx["tableinfo_variables"], _sel(fx)) is not None   # control


def test_a_time_code_outside_the_selection_is_refused():
    fx = _fx("FOLK1A")
    lines = fx["bulk"].splitlines()
    lines[1] = lines[1].replace(";2008K1;", ";1999K1;")
    assert ING.parse_bulk("\r\n".join(lines) + "\r\n", "FOLK1A", fx["tableinfo_variables"], _sel(fx)) is None


# ---- T2: the fallback fires on a final HTTP 500 and on nothing else ----

def _folk(wire, answers, bulk=None):
    fx = _fx("FOLK1A")
    wire.json_answers["FOLK1A"] = answers
    wire.bulk["FOLK1A"] = bulk or [fx["bulk"]]
    return ING.query_table_detailed("FOLK1A", fx["tableinfo_variables"])


def test_a_final_500_falls_back_to_bulk_with_codes_and_the_same_selection(wire):
    rows, why = _folk(wire, [500])
    assert why == "ok" and len(rows) == 75
    assert wire.fmts("FOLK1A") == ["JSONSTAT"] * 3 + ["BULK"]          # 3 tries, then the bulk request
    js_body = next(c[2] for c in wire.calls if c[1] == "JSONSTAT")
    bk_body = next(c[2] for c in wire.calls if c[1] == "BULK")
    assert bk_body["valuePresentation"] == "Code" and bk_body["variables"] == js_body["variables"]
    assert ING.BULK_FALLBACKS == [{"table": "FOLK1A", "cells": 75, "omitted": 0}]
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
    wire.bulk["BARLOV1"] = [fx["bulk"]]
    assert ING.query_table_detailed("BARLOV1", fx["tableinfo_variables"]) == ([], "span_time")
    assert _folk(wire, [500, 500, _fx("FOLK1A")["jsonstat"]])[1] == "ok"          # a 500 that recovers
    assert "BULK" not in wire.fmts("BARLOV1") + wire.fmts("FOLK1A") and ING.BULK_FALLBACKS == []


def test_a_failed_bulk_request_leaves_the_table_failed_and_is_counted(wire):
    assert _folk(wire, [500], bulk=[500]) == ([], "failed")
    assert ING.BULK_FALLBACKS == [{"table": "FOLK1A", "cells": 75, "omitted": None}]


@pytest.mark.parametrize("bulk, tries", [([400], 1), ([403], 1), ([404], 1), ([500], 3), ([503], 3),
                                         ([requests.ConnectionError("c")], 3), ([requests.Timeout("t")], 3)])
def test_the_bulk_request_is_retried_up_to_3_times_only_when_retrying_can_help(wire, bulk, tries):
    assert _folk(wire, [500], bulk=bulk) == ([], "failed")
    assert wire.fmts("FOLK1A").count("BULK") == tries


def test_a_bulk_network_error_then_an_answer_is_used(wire):
    """Review AR-348 S: the BULK retry on an exception is pinned."""
    rows, why = _folk(wire, [500], bulk=[requests.ConnectionError("c"), requests.Timeout("t"), _fx("FOLK1A")["bulk"]])
    assert why == "ok" and len(rows) == 75 and wire.fmts("FOLK1A").count("BULK") == 3


def test_a_bulk_body_that_is_not_the_selection_is_unparsed_not_rows(wire):
    assert _folk(wire, [500], bulk=["A;B\r\n1;2\r\n"]) == ([], "unparsed")
    assert ING.BULK_FALLBACKS == [{"table": "FOLK1A", "cells": 75, "omitted": None}]


def test_a_chunked_table_falls_back_for_the_failing_chunk_only(wire, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2021", "2022", "2023"]
    variables = [{"id": "X", "values": [{"id": "A"}]},
                 {"id": "Tid", "time": True, "values": [{"id": t, "text": t} for t in times]}]
    body = {"dataset": {"dimension": {
        "X": {"category": {"index": {"A": 0}}}, "Tid": {"category": {"index": {"2021": 0, "2022": 1}}},
        "id": ["X", "Tid"], "size": [1, 2], "role": {"time": ["Tid"]}}, "value": [1.0, 2.0]}}
    wire.json_answers["CH"] = [body, 500]
    wire.bulk["CH"] = ["X;TID;INDHOLD\r\nA;2023;3\r\n"]
    rows, why = ING.query_table_detailed("CH", variables)
    assert why == "ok" and sorted(v for _, _, v in rows) == [1.0, 2.0, 3.0]
    assert wire.fmts("CH") == ["JSONSTAT", "JSONSTAT", "JSONSTAT", "JSONSTAT", "BULK"]
    bk = next(c[2] for c in wire.calls if c[1] == "BULK")
    assert next(v["values"] for v in bk["variables"] if v["code"] == "Tid") == ["2023"]   # the failing chunk only
    assert ING.BULK_FALLBACKS == [{"table": "CH", "cells": 1, "omitted": 0}]


def test_a_stale_500_from_an_earlier_call_cannot_trigger_the_fallback(wire, monkeypatch):
    """post_json's answer is read per request: a None from a call that never reached the network (a stand-in, or a
    path that returns early) must not inherit the 500 an EARLIER request left behind."""
    ING._LAST_POST.update(status=500, code="SYSTEM-100")
    monkeypatch.setattr(ING, "post_json", lambda url, body, retries=3: None)
    wire.bulk["FOLK1A"] = [_fx("FOLK1A")["bulk"]]
    assert ING.query_table_detailed("FOLK1A", _fx("FOLK1A")["tableinfo_variables"]) == ([], "failed")
    assert "BULK" not in wire.fmts("FOLK1A")


# ---- T3: the two tables that fail today parse ----

@pytest.mark.parametrize("tid", sorted(FAILING))
def test_the_failing_tables_come_back_through_the_fallback(wire, monkeypatch, tid):
    fx = _fx(tid)
    assert fx["jsonstat_http_status"] == 500 and fx["jsonstat_error_code"] == "SYSTEM-100"
    wire.json_answers[tid] = [500]
    wire.bulk[tid] = [fx["bulk"]]
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"]})
    rows, transient = D._fetch_table_rows(tid)
    assert transient is False and len(rows) == FAILING[tid][0]
    shape = re.compile(rf"^DST:{tid}:VERSION=[^:]+:SEKTORNAT=[^:]+:KONTO1=[^:]+:BALANCE=[^:]+:FININS=[^:]+"
                       r":MODSEKNAT=[^:]+$")
    assert all(shape.match(k) for k, _, _ in rows)
    assert ING.BULK_FALLBACKS == [{"table": tid, "cells": FAILING[tid][1], "omitted": 0}]


# ---- T4: counted and named in the pass's note, never silent; status unchanged ----

def test_the_note_counts_a_table_once_and_sums_its_chunks(monkeypatch):
    monkeypatch.setattr(ING, "BULK_FALLBACKS", [{"table": "DNVALD", "cells": 1000, "omitted": 4},
                                                {"table": "DNVALD", "cells": 1000, "omitted": None},
                                                {"table": "VNASFK", "cells": 31, "omitted": 0}])
    assert D._bulk_fallback_note() == ("BULK fallback for 2 table(s) after a JSONSTAT HTTP 500: "
                                       "[DNVALD (4 of 2,000 cells not streamed, 1 BULK answer(s) unusable), "
                                       "VNASFK (0 of 31 cells not streamed)]")
    monkeypatch.setattr(ING, "BULK_FALLBACKS", [])
    assert D._bulk_fallback_note() == ""


def _run_update(wire, monkeypatch, tmp_path, fallback: bool, cap_after_first_chunk: bool = False):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    cat = [{"id": "VNASFK", "updated": "2026-09-30T08:00:00"}, {"id": "VNKSFK", "updated": "2026-09-30T08:00:00"}]
    monkeypatch.setattr(D, "_fetch_catalog", lambda *a, **k: [dict(t) for t in cat])
    meta = {t: {"variables": _fx(t)["tableinfo_variables"]} for t in ("VNASFK", "VNKSFK")}
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: meta[url.split("id=")[1].split("&")[0]])
    for t in ("VNASFK", "VNKSFK"):
        wire.bulk[t] = [_fx(t)["bulk"]]
    if fallback:
        wire.json_answers["VNASFK"] = [500]
    else:                                  # the same table answering in JSONSTAT: a minimal valid body
        wire.json_answers["VNASFK"] = [{"dataset": {"dimension": {
            "VERSION": {"category": {"index": {"V": 0}}}, "Tid": {"category": {"index": {"2024": 0}}},
            "id": ["VERSION", "Tid"], "size": [1, 1], "role": {"time": ["Tid"]}}, "value": [1.0]}}]
    wire.json_answers["VNKSFK"] = wire.json_answers["VNASFK"]
    if cap_after_first_chunk:
        # MAX_TABLES=1 makes each table its own chunk; the deadline is spent after the first one (review AR-348 K)
        monkeypatch.setattr(D, "MAX_TABLES", 1)
        state = {"n": 0}

        class _DL:
            def __init__(self, minutes):
                pass

            def spent(self):
                state["n"] += 1
                return state["n"] > 1

            def elapsed_min(self):
                return 0.0
        monkeypatch.setattr(D, "Deadline", _DL)
    ING.BULK_FALLBACKS[:] = [{"table": "STALE_FROM_AN_EARLIER_PASS", "cells": 1, "omitted": 0}]
    return D.update(None, None)


def test_a_pass_that_falls_back_names_it_and_stays_ok(wire, monkeypatch, tmp_path, capsys):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=True)
    assert res.status == "ok", (res.status, res.error)
    assert ("BULK fallback for 2 table(s) after a JSONSTAT HTTP 500: [VNASFK (0 of 31 cells not streamed), "
            "VNKSFK (0 of 110 cells not streamed)]") in (res.error or "")
    assert "STALE_FROM_AN_EARLIER_PASS" not in (res.error or "")
    out = capsys.readouterr().out
    assert "JSONSTAT POST answered HTTP 500 (SYSTEM-100)" in out and "0 of 31 selected cells were not streamed" in out
    assert "DST:VNASFK" in (res.series_cursors or {}), res.series_cursors      # merged: the subject's cursor


def test_a_budget_capped_pass_keeps_the_fallback_note(wire, monkeypatch, tmp_path):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=True, cap_after_first_chunk=True)
    assert res.status == "partial" and "budget spent with 1 of 2 due table(s)" in (res.error or ""), res.error
    assert "BULK fallback for 1 table(s) after a JSONSTAT HTTP 500: [VNASFK (0 of 31 cells not streamed)]" \
        in (res.error or ""), res.error


def test_a_pass_without_a_fallback_says_nothing_about_bulk(wire, monkeypatch, tmp_path):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=False)
    assert res.status == "ok" and "BULK" not in (res.error or ""), (res.status, res.error)
    assert "BULK" not in [c[1] for c in wire.calls]
