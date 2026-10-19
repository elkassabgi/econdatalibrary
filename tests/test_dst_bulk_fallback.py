"""dst: when DST's JSONSTAT data POST answers HTTP 500, fetch the same selection in BULK format.

Since 2026-09-30 DST answers EVERY data POST for VNASFK and VNKSFK - one cell included, JSONSTAT and CSV alike - with
HTTP 500 {"errorTypeCode":"SYSTEM-100"}, while its streaming BULK format answers 200 (measured live 2026-10-19; 2 of
the 23 tables with a version variable plus NASFK; the control FOLK1A answers 200 in all three formats).

BULK leaves out the combinations a table does not store. DST's API help page defines such a combination as 0 ("Hvis
en kombination af vaerdier mangler, er observationen for denne kombination 0 (tallet nul)") and names tableinfo's
suppressedDataValue as the value for missing-row observations in the streaming formats; JSON-stat writes the same
cells as that value. So the fallback fills every left-out cell with the table's own suppressedDataValue, and with it
BULK gives exactly the JSON-stat rows on every control. A BULK body has no end marker, so a body cut short would
become filled cells: the second read (two halves of the time values) and the store-side overwrite bound guard it.

The fixtures in tests/fixtures/dst_bulk/ are live api.statbank.dk answers (CC BY 4.0) recorded 2026-10-19: for each
control table, the fetcher's own selection in JSONSTAT and in BULK (valuePresentation=Code); for VNASFK/VNKSFK the
BULK body only (JSONSTAT answered 500). Hermetic: requests.post is faked; a BULK request for part of a selection is
answered with the matching records of the recorded body; no network.
"""
from __future__ import annotations

import collections
import datetime as dt
import json
import os
import re
import sys

import pyarrow as pa
import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import dst as D  # noqa: E402
from jobs import ingest_dst as ING  # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures", "dst_bulk")
# Tables that answered 200 in BOTH formats; together they reach every parse branch: quarterly 'K' codes (FOLK1A,
# REGR63), half-years (ANB10), Danish weeks 'U' (VAN1UGE), months with decimals (PRIS111), a version table (VNKN1),
# '..' missing values (ITAV22), cells BULK leaves out (VAN1UGE, REGR63, ANB10), and a table whose every period is an
# undated window (BARLOV1).
CONTROLS = ["FOLK1A", "VNKN1", "VAN1UGE", "REGR63", "ITAV22", "BARLOV1", "ANB10", "PRIS111"]
# rows each failing table yields from its recorded BULK body (fetcher selection), and its selected cells
FAILING = {"VNASFK": (3, 31), "VNKSFK": (14, 110)}


def _fx(tid):
    with open(os.path.join(FIX, f"{tid}.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _sel(fx):
    return ING._query_selection(fx["tableinfo_variables"])


def _part_of(text, req_vars):
    """The records of a recorded BULK body that a request for `req_vars` would get ('*' = every value)."""
    lines = text.splitlines()
    want = [None if v["values"] == ["*"] else set(map(str, v["values"])) for v in req_vars]
    keep = [ln for ln in lines[1:] if ln and all(w is None or f in w
                                                  for w, f in zip(want, ln.split(";")[:len(want)]))]
    return "\r\n".join([lines[0]] + keep) + "\r\n"


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
    """Fake DST /v1/data. `wire.json_answers[tid]`: JSONSTAT answers, one per attempt (last repeats): an int status,
    an Exception, or a dict body. `wire.full[tid]`: the table's whole BULK body; a BULK request is answered with the
    records it selects, unless `wire.bulk[tid]` lists answers for the first BULK requests (an int status, an
    Exception, a str body, or a callable(request) -> str); past that list the full body answers again."""
    w = type("Wire", (), {})()
    w.json_answers, w.bulk, w.full, w.calls = {}, {}, {}, []

    def post(url, json=None, headers=None, timeout=None):
        tid, fmt = json["table"], json["format"]
        w.calls.append((tid, fmt, json))
        n = sum(1 for c in w.calls if c[0] == tid and c[1] == fmt)
        if fmt == "BULK":
            seq = w.bulk.get(tid, [])
            ans = seq[n - 1] if n <= len(seq) else None
            if ans is None:
                return _Resp(200, text=_part_of(w.full[tid], json["variables"]))
            if callable(ans):
                return _Resp(200, text=ans(json))
        else:
            seq = w.json_answers[tid]
            ans = seq[min(n - 1, len(seq) - 1)]
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


# ---- T1: BULK + the table's suppressedDataValue == JSON-stat, all rows ----

@pytest.mark.parametrize("tid", CONTROLS)
def test_bulk_with_the_fill_gives_exactly_the_jsonstat_rows(tid):
    fx = _fx(tid)
    want = ING.parse_jsonstat(fx["jsonstat"], tid)
    got = ING.parse_bulk(fx["bulk"], tid, fx["tableinfo_variables"], _sel(fx), fx["suppressedDataValue"])
    assert got is not None and sorted(got) == sorted(want)


def test_the_controls_are_not_vacuous_and_the_fill_is_what_makes_them_equal():
    tot_json = tot_bare = 0
    filled = {}
    for t in CONTROLS:
        fx = _fx(t)
        tot_json += len(ING.parse_jsonstat(fx["jsonstat"], t))
        ok, body = ING._bulk_records(fx["bulk"], _sel(fx))
        bare, st = ING._bulk_rows(body, t, fx["tableinfo_variables"], _sel(fx), None)
        tot_bare += len(bare)
        filled[t] = ING._bulk_rows(body, t, fx["tableinfo_variables"], _sel(fx), "0")[1]["filled"]
    assert tot_json == 3_834 and tot_bare == 3_527
    assert (filled["VAN1UGE"], filled["REGR63"], filled["ANB10"], filled["FOLK1A"]) == (231, 57, 19, 0)


@pytest.mark.parametrize("supp, fill, rows, nofill", [("0", 0.0, 248, 0), ("0.0", 0.0, 248, 0), ("5", 5.0, 248, 0),
                                                     ("..", None, 17, 231), ("-", None, 17, 231), ("", None, 17, 231),
                                                     (None, None, 17, 231)])
def test_the_fill_is_the_tables_own_suppressed_value_never_a_constant(supp, fill, rows, nofill):
    """VAN1UGE with its tableinfo suppressedDataValue edited: '5' proves the value is READ, not assumed; '..', '-',
    '' and absent fill nothing and count the cells as 'no fill value'."""
    fx = _fx("VAN1UGE")
    ok, body = ING._bulk_records(fx["bulk"], _sel(fx))
    got, st = ING._bulk_rows(body, "VAN1UGE", fx["tableinfo_variables"], _sel(fx), supp)
    assert len(got) == rows and st["nofill"] == nofill and st["filled"] == 248 - 17 - nofill
    streamed = {(k, d) for k, d, _ in ING._bulk_rows(body, "VAN1UGE", fx["tableinfo_variables"], _sel(fx), None)[0]}
    fills = [v for k, d, v in got if (k, d) not in streamed]
    assert fills == [fill] * len(fills) and len(fills) == st["filled"]


def test_a_dotdot_row_is_never_filled():
    """ITAV22: 27 cells are streamed as '..' (JSON-stat null). They are represented cells with no number."""
    fx = _fx("ITAV22")
    ok, body = ING._bulk_records(fx["bulk"], _sel(fx))
    got, st = ING._bulk_rows(body, "ITAV22", fx["tableinfo_variables"], _sel(fx), "5")
    assert sum(r[-1] == ".." for r in body) == 27 and st["filled"] == 0 and len(got) == 933
    assert len(body) == 960 and len(got) == len(body) - 27 and 5.0 not in {v for _, _, v in got}


@pytest.mark.parametrize("tid", sorted(FAILING))
def test_the_failing_tables_stream_every_cell(tid):
    fx = _fx(tid)
    ok, body = ING._bulk_records(fx["bulk"], _sel(fx))
    rows, st = ING._bulk_rows(body, tid, fx["tableinfo_variables"], _sel(fx), "0")
    assert (len(rows), st["streamed"], st["filled"], ING._n_cells(_sel(fx))) == (FAILING[tid][0], FAILING[tid][1], 0,
                                                                                 FAILING[tid][1])


def test_an_empty_bulk_answer_is_named_as_jsonstat_names_it():
    fx = _fx("BARLOV1")
    assert ING._why_empty_bulk(fx["bulk"], _sel(fx), "0") == ING._why_empty(fx["jsonstat"]) == "span_time"
    sel = [{"code": "Tid", "values": ["2007:2009", "2008:2010"], "_time": True, "_all": True}]
    assert ING._why_empty_bulk("TID;INDHOLD\r\n", sel, "0") == "span_time"        # all left out, filled -> numbers
    assert ING._why_empty_bulk("TID;INDHOLD\r\n", sel, "..") == "all_null"        # nothing to fill with
    assert ING._why_empty_bulk("TID;INDHOLD\r\n2007:2009;..\r\n2008:2010;..\r\n", sel, "0") == "all_null"


def test_missing_and_non_numeric_bulk_values_are_skipped_like_jsonstat_nulls():
    sel = [{"code": "X", "values": ["A", "B", "C"], "_time": False, "_all": True},
           {"code": "Tid", "values": ["2024"], "_time": True, "_all": True}]
    variables = [{"id": "X", "values": [{"id": "A"}, {"id": "B"}, {"id": "C"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024", "text": "2024"}]}]
    text = "X;TID;INDHOLD\r\nA;2024;..\r\nB;2024;\r\nC;2024;1.5\r\n"
    assert ING.parse_bulk(text, "T", variables, sel, "0") == [("DST:T:X=C", dt.date(2024, 12, 31), 1.5)]


def test_operator_looking_values_parse_and_fill_over_the_real_values():
    """AKU240K-shaped: '<15' and '>48' are sent as the '*' wildcard, but the membership check and the fill must use
    the real values (review AR-348 Q)."""
    variables = [{"id": "ARBEJDSTID", "values": [{"id": "<15"}, {"id": "15-36"}, {"id": ">48"}]},
                 {"id": "Tid", "time": True, "values": [{"id": "2024K1", "text": "2024Q1"}]}]
    sel = ING._query_selection(variables)
    assert ING._wire_values(sel[0]) == ["*"]
    rows = ING.parse_bulk("ARBEJDSTID;TID;INDHOLD\r\n<15;2024K1;3\r\n>48;2024K1;4\r\n", "AKU240K", variables, sel, "0")
    assert sorted(rows) == [("DST:AKU240K:ARBEJDSTID=15-36", dt.date(2024, 1, 1), 0.0),
                            ("DST:AKU240K:ARBEJDSTID=<15", dt.date(2024, 1, 1), 3.0),
                            ("DST:AKU240K:ARBEJDSTID=>48", dt.date(2024, 1, 1), 4.0)]


def test_a_byte_order_mark_does_not_break_the_header():
    fx = _fx("FOLK1A")
    plain = ING.parse_bulk(fx["bulk"], "FOLK1A", fx["tableinfo_variables"], _sel(fx), "0")
    assert ING.parse_bulk(chr(0xFEFF) + fx["bulk"], "FOLK1A", fx["tableinfo_variables"], _sel(fx), "0") == plain


def test_a_positional_time_code_is_dated_from_its_tableinfo_label():
    sel = [{"code": "Tid", "values": ["0", "1"], "_time": True, "_all": True}]
    variables = [{"id": "Tid", "time": True, "values": [{"id": "0", "text": "2020"}, {"id": "1", "text": "2021"}]}]
    rows = ING.parse_bulk("TID;INDHOLD\r\n0;1\r\n1;2\r\n", "P", variables, sel, "0")
    assert [d for _, d, _ in rows] == [dt.date(2020, 12, 31), dt.date(2021, 12, 31)]


# ---- T5: shapes the parser refuses, loudly ----

@pytest.mark.parametrize("header", ["OMRÅDE;KØN;ALDER;CIVILSTAND;INDHOLD",            # a column missing
                                    "KØN;OMRÅDE;ALDER;CIVILSTAND;TID;INDHOLD",        # columns reordered
                                    "OMRÅDE;KØN;ALDER;CIVILSTAND;TID;VALUE"])         # value column renamed
def test_a_body_without_the_requested_columns_is_refused_not_guessed(header):
    fx = _fx("FOLK1A")
    body = header + "\r\n" + "\r\n".join(fx["bulk"].splitlines()[1:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], _sel(fx), "0") is None


@pytest.mark.parametrize("short", ["000;TOT;IALT;2008K1;5", "000;5", "000"])
def test_a_short_row_is_refused_not_raised(short):
    fx = _fx("FOLK1A")
    lines = fx["bulk"].splitlines()
    body = "\r\n".join(lines[:2] + [short] + lines[3:]) + "\r\n"
    assert ING.parse_bulk(body, "FOLK1A", fx["tableinfo_variables"], _sel(fx), "0") is None


@pytest.mark.parametrize("plant", ["6100811 ", "9999999", "6100811.0"])
def test_a_code_outside_the_selection_is_refused_not_minted(plant):
    """Review AR-348 G2: a re-formatted code was accepted, minted a new series_key, and its real cell was filled."""
    fx = _fx("REGR63")
    lines = fx["bulk"].splitlines()
    i = next(k for k, ln in enumerate(lines) if ln.startswith("6100811;"))
    lines[i] = plant + lines[i][len("6100811"):]
    assert ING.parse_bulk("\r\n".join(lines) + "\r\n", "REGR63", fx["tableinfo_variables"], _sel(fx), "0") is None
    assert ING.parse_bulk(fx["bulk"], "REGR63", fx["tableinfo_variables"], _sel(fx), "0") is not None   # control


# ---- T2: the fallback fires on a final HTTP 500 and on nothing else ----

def _folk(wire, answers, bulk=None, tid="FOLK1A"):
    fx = _fx(tid)
    wire.json_answers[tid] = answers
    wire.full[tid] = fx["bulk"]
    if bulk is not None:
        wire.bulk[tid] = bulk
    return ING.query_table_detailed(tid, fx["tableinfo_variables"], suppressed=fx["suppressedDataValue"])


def test_a_final_500_falls_back_to_bulk_with_codes_and_reads_it_twice(wire):
    rows, why = _folk(wire, [500])
    assert why == "ok" and len(rows) == 75
    assert wire.fmts("FOLK1A") == ["JSONSTAT"] * 3 + ["BULK"] * 3      # 3 JSONSTAT tries; BULK whole + 2 halves
    js_body = next(c[2] for c in wire.calls if c[1] == "JSONSTAT")
    bks = [c[2] for c in wire.calls if c[1] == "BULK"]
    assert all(b["valuePresentation"] == "Code" for b in bks) and bks[0]["variables"] == js_body["variables"]
    tid_vals = [next(v["values"] for v in b["variables"] if v["code"] == "Tid") for b in bks]
    assert tid_vals[1] + tid_vals[2] == tid_vals[0] and len(tid_vals[1]) == 37          # 75 periods split 37 / 38
    rec = ING.BULK_FALLBACKS[0]
    assert (rec["status"], rec["cells"], rec["streamed"], rec["filled"], rec["nofill"]) == ("ok", 75, 75, 0, 0)
    assert sorted(rows) == sorted(ING.parse_jsonstat(_fx("FOLK1A")["jsonstat"], "FOLK1A"))


def test_a_fallback_table_with_left_out_cells_stores_the_jsonstat_rows(wire):
    rows, why = _folk(wire, [500], tid="VAN1UGE")
    assert why == "ok" and sorted(rows) == sorted(ING.parse_jsonstat(_fx("VAN1UGE")["jsonstat"], "VAN1UGE"))
    rec = ING.BULK_FALLBACKS[0]
    assert (rec["streamed"], rec["filled"], rec["nofill"], len(rec["filled_cells"])) == (17, 231, 0, 231)


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
    assert ING.query_table_detailed("BARLOV1", fx["tableinfo_variables"], suppressed="0") == ([], "span_time")
    assert _folk(wire, [500, 500, _fx("FOLK1A")["jsonstat"]])[1] == "ok"          # a 500 that recovers
    assert "BULK" not in wire.fmts("BARLOV1") + wire.fmts("FOLK1A") and ING.BULK_FALLBACKS == []


@pytest.mark.parametrize("bulk, tries", [([400], 1), ([403], 1), ([404], 1), ([500] * 3, 3), ([503] * 3, 3),
                                         ([requests.ConnectionError("c")] * 3, 3), ([requests.Timeout("t")] * 3, 3)])
def test_the_bulk_request_is_retried_up_to_3_times_only_when_retrying_can_help(wire, bulk, tries):
    assert _folk(wire, [500], bulk=bulk) == ([], "failed")
    assert wire.fmts("FOLK1A").count("BULK") == tries
    assert ING.BULK_FALLBACKS[0]["status"] == "bulk_failed"


def test_a_bulk_network_error_then_an_answer_is_used(wire):
    """Review AR-348 S: the BULK retry on an exception is pinned (2 errors, the whole body, then 2 halves)."""
    rows, why = _folk(wire, [500], bulk=[requests.ConnectionError("c"), requests.Timeout("t")])
    assert why == "ok" and len(rows) == 75 and wire.fmts("FOLK1A").count("BULK") == 5


def test_a_bulk_body_that_is_not_the_selection_is_unparsed_not_rows(wire):
    assert _folk(wire, [500], bulk=["A;B\r\n1;2\r\n"]) == ([], "unparsed")
    assert ING.BULK_FALLBACKS[0]["status"] == "not_the_selection"


# ---- T6: the second read, in a different shape (review AR-349 C4a/b) ----

def _cut(text, n):
    lines = text.splitlines()
    return "\r\n".join(lines[: 1 + n]) + "\r\n"


def test_a_body_cut_short_is_caught_by_the_halves_and_never_filled(wire):
    """Review AR-348 G1: half of REGR63's body, filled, gave JSON-stat's 1,232 rows with real values turned to 0."""
    full = _fx("REGR63")["bulk"]
    rows, why = _folk(wire, [500], bulk=[_cut(full, 587)], tid="REGR63")
    assert (rows, why) == ([], "failed") and ING.BULK_FALLBACKS[0]["status"] == "reads_disagree"


def test_a_cut_that_repeats_at_the_same_size_is_caught_by_the_other_shape(wire):
    """A deterministic DST-side cap (the first N records of every answer) gives the same body twice; two halves of
    a different size cannot both reproduce it."""
    full = _fx("REGR63")["bulk"]
    cap = lambda req: _cut(_part_of(full, req["variables"]), 600)          # noqa: E731
    rows, why = _folk(wire, [500], bulk=[cap, cap, cap], tid="REGR63")
    assert why == "failed" and ING.BULK_FALLBACKS[0]["status"] == "reads_disagree"


def test_a_number_cut_in_the_middle_is_caught(wire):
    full = _fx("REGR63")["bulk"]
    lines = full.rstrip("\r\n").splitlines()
    assert lines[-1] == "65200;2026K2;-18837921"
    cut = "\r\n".join(lines[:-1] + ["65200;2026K2;-1883"]) + "\r\n"
    assert _folk(wire, [500], bulk=[cut], tid="REGR63") == ([], "failed")


def test_a_repeated_record_is_a_disagreement(wire):
    """Records are compared as MULTISETS: a first body that repeats a record is not the halves' body (AR-349 F6)."""
    full = _fx("REGR63")["bulk"]
    lines = full.rstrip("\r\n").splitlines()
    dup = "\r\n".join(lines + [lines[5]]) + "\r\n"
    assert _folk(wire, [500], bulk=[dup], tid="REGR63") == ([], "failed")
    assert ING.BULK_FALLBACKS[0]["status"] == "reads_disagree"


def test_a_different_row_order_is_not_a_disagreement(wire):
    """DST does not promise the row order: records are compared as multisets, not bytes."""
    full = _fx("REGR63")["bulk"]
    lines = full.rstrip("\r\n").splitlines()
    shuffled = "\r\n".join([lines[0]] + lines[1:][::-1]) + "\r\n"
    rows, why = _folk(wire, [500], bulk=[shuffled], tid="REGR63")
    assert why == "ok" and len(rows) == 1_232


def test_a_failed_second_read_leaves_the_table_owed(wire):
    rows, why = _folk(wire, [500], bulk=[None, 500, 500, 500], tid="FOLK1A")
    assert (rows, why) == ([], "failed") and ING.BULK_FALLBACKS[0]["status"] == "second_read_failed"


def test_a_half_that_is_not_its_own_selection_is_refused(wire):
    """One half answering with the WHOLE body and the other with nothing gives the right union; it must still fail,
    because a half carrying codes it did not ask for is not that half."""
    full = _fx("FOLK1A")["bulk"]
    header_only = full.splitlines()[0] + "\r\n"
    rows, why = _folk(wire, [500], bulk=[None, full, header_only])
    assert (rows, why) == ([], "failed") and ING.BULK_FALLBACKS[0]["status"] == "second_read_failed"


def test_the_split_prefers_time_and_never_a_comparison_valued_variable():
    t = {"code": "Tid", "values": ["2024"], "_time": True}
    lt = {"code": "A", "values": ["<15", "15-36", ">48", "x"], "_time": False}
    b = {"code": "B", "values": ["1", "2"], "_time": False}
    assert ING._split_index([lt, b, dict(t, values=["2023", "2024"])]) == 2
    assert ING._split_index([lt, b, t]) == 1
    assert ING._split_index([lt, t]) is None
    one = [{"code": "X", "values": ["A"], "_time": False}, t]
    assert ING._split_index(one) is None


def test_a_one_cell_body_is_read_twice_the_same_way(wire):
    sel_vars = [{"id": "X", "values": [{"id": "A"}]}, {"id": "Tid", "time": True, "values": [{"id": "2024"}]}]
    wire.json_answers["ONE"] = [500]
    wire.full["ONE"] = "X;TID;INDHOLD\r\nA;2024;7\r\n"
    rows, why = ING.query_table_detailed("ONE", sel_vars, suppressed="0")
    assert why == "ok" and rows == [("DST:ONE:X=A", dt.date(2024, 12, 31), 7.0)] and wire.fmts("ONE").count("BULK") == 2


def test_a_chunked_table_falls_back_for_the_failing_chunk_only(wire, monkeypatch):
    monkeypatch.setattr(ING, "TIME_CHUNK", 2)
    times = ["2021", "2022", "2023"]
    variables = [{"id": "X", "values": [{"id": "A"}]},
                 {"id": "Tid", "time": True, "values": [{"id": t, "text": t} for t in times]}]
    body = {"dataset": {"dimension": {
        "X": {"category": {"index": {"A": 0}}}, "Tid": {"category": {"index": {"2021": 0, "2022": 1}}},
        "id": ["X", "Tid"], "size": [1, 2], "role": {"time": ["Tid"]}}, "value": [1.0, 2.0]}}
    wire.json_answers["CH"] = [body, 500]
    wire.full["CH"] = "X;TID;INDHOLD\r\nA;2023;3\r\n"
    rows, why = ING.query_table_detailed("CH", variables, suppressed="0")
    assert why == "ok" and sorted(v for _, _, v in rows) == [1.0, 2.0, 3.0]
    bk = next(c[2] for c in wire.calls if c[1] == "BULK")
    assert next(v["values"] for v in bk["variables"] if v["code"] == "Tid") == ["2023"]   # the failing chunk only
    assert ING.BULK_FALLBACKS[0]["cells"] == 1


def test_a_stale_500_from_an_earlier_call_cannot_trigger_the_fallback(wire, monkeypatch):
    ING._LAST_POST.update(status=500, code="SYSTEM-100")
    monkeypatch.setattr(ING, "post_json", lambda url, body, retries=3: None)
    wire.full["FOLK1A"] = _fx("FOLK1A")["bulk"]
    assert ING.query_table_detailed("FOLK1A", _fx("FOLK1A")["tableinfo_variables"], suppressed="0") == ([], "failed")
    assert "BULK" not in wire.fmts("FOLK1A")


# ---- T3: the two tables that fail today parse, end to end ----

@pytest.mark.parametrize("tid", sorted(FAILING))
def test_the_failing_tables_come_back_through_the_fallback(wire, monkeypatch, tmp_path, tid):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    fx = _fx(tid)
    assert fx["jsonstat_http_status"] == 500 and fx["jsonstat_error_code"] == "SYSTEM-100"
    wire.json_answers[tid] = [500]
    wire.full[tid] = fx["bulk"]
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": "0"})
    rows, transient = D._fetch_table_rows(tid)
    assert transient is False and len(rows) == FAILING[tid][0]
    shape = re.compile(rf"^DST:{tid}:VERSION=[^:]+:SEKTORNAT=[^:]+:KONTO1=[^:]+:BALANCE=[^:]+:FININS=[^:]+"
                       r":MODSEKNAT=[^:]+$")
    assert all(shape.match(k) for k, _, _ in rows)
    assert (ING.BULK_FALLBACKS[0]["streamed"], ING.BULK_FALLBACKS[0]["filled"]) == (FAILING[tid][1], 0)


def test_the_fetcher_reads_the_suppressed_value_from_tableinfo(wire, monkeypatch, tmp_path):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    fx = _fx("VAN1UGE")
    wire.json_answers["VAN1UGE"] = [500]
    wire.full["VAN1UGE"] = fx["bulk"]
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": "5"})
    rows, transient = D._fetch_table_rows("VAN1UGE")
    assert transient is False and len(rows) == 248 and sum(v == 5.0 for _, _, v in rows) == 231


# ---- T7: the store-side fill guard (review AR-349 C4c) ----

def _store(tmp_path, tid, rows):
    d = tmp_path / "dst"
    d.mkdir(parents=True, exist_ok=True)
    keys, dates, vals = zip(*rows) if rows else ((), (), ())
    import pyarrow.parquet as pq
    pq.write_table(pa.table({"series_key": pa.array(keys, pa.string()), "obs_date": pa.array(dates, pa.date32()),
                             "value": pa.array(vals, pa.float64())}), str(d / f"{D._subj(tid)}.parquet"))


def _van(wire, monkeypatch, tmp_path, stored_nonzero_fill_cells: int, other_nonzero: int = 0,
         stored_value: float = 9.0, supp: str = "0", latest: bool = False, other_prefix: str = "DST:VAN1UGE:OTHER"):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    fx = _fx("VAN1UGE")
    want = ING.parse_jsonstat(fx["jsonstat"], "VAN1UGE")
    ok, body = ING._bulk_records(fx["bulk"], _sel(fx))
    fills = sorted(ING._bulk_rows(body, "VAN1UGE", fx["tableinfo_variables"], _sel(fx), "0")[1]["filled_cells"],
                   key=lambda kd: (kd[1], kd[0]))
    pick = fills[-stored_nonzero_fill_cells:] if latest and stored_nonzero_fill_cells else \
        fills[:stored_nonzero_fill_cells]
    stored = [(k, d, stored_value) for k, d in pick]
    stored += [(f"{other_prefix}{i}", dt.date(2000, 12, 31), 1.0) for i in range(other_nonzero)]
    _store(tmp_path, "VAN1UGE", stored)
    wire.json_answers["VAN1UGE"] = [500]
    wire.full["VAN1UGE"] = fx["bulk"]
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": supp})
    return D._fetch_table_rows("VAN1UGE"), want


def test_a_fill_that_overwrites_many_stored_values_is_refused(wire, monkeypatch, tmp_path, capsys):
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 11)            # 11 > max(10, 1% of 11)
    assert (rows, transient) == ([], True)
    rec = ING.BULK_FALLBACKS[0]
    assert (rec["status"], rec["overwrites"], rec["bound"]) == ("fill_refused", 11, 10)
    assert "BULK FILL REFUSED - 11 filled cells would overwrite stored non-zero values" in capsys.readouterr().out
    assert "1 fill refused" in D._bulk_fallback_note()


def test_a_fill_within_the_bound_is_stored_and_counted(wire, monkeypatch, tmp_path):
    (rows, transient), want = _van(wire, monkeypatch, tmp_path, 10)
    assert transient is False and sorted(rows) == sorted(want)
    assert (ING.BULK_FALLBACKS[0]["overwrites"], ING.BULK_FALLBACKS[0]["bound"]) == (10, 10)
    assert "10 stored non-zero overwritten" in D._bulk_fallback_note()


def test_the_bound_grows_with_the_tables_stored_non_zero_cells(wire, monkeypatch, tmp_path):
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 12, other_nonzero=1_200)   # 12 <= 1% of 1,212
    assert transient is False and ING.BULK_FALLBACKS[0]["bound"] == 12


def test_the_bound_counts_only_this_tables_stored_cells(wire, monkeypatch, tmp_path):
    """The subject file is shared: another table's 2,000 non-zero cells must not raise this table's bound
    (review AR-349 G3)."""
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 11, other_nonzero=2_000, other_prefix="DST:VAN1UGX:")
    assert (rows, transient) == ([], True) and ING.BULK_FALLBACKS[0]["bound"] == 10


def test_the_guard_sees_the_fill_of_every_chunk(wire, monkeypatch, tmp_path):
    """A chunked table falls back per chunk; overwrites in a LATER chunk count (review AR-349 G6)."""
    monkeypatch.setattr(ING, "TIME_CHUNK", 124)
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 11, latest=True)
    assert (rows, transient) == ([], True) and len(ING.BULK_FALLBACKS) == 2
    assert ING.BULK_FALLBACKS[0]["status"] == "fill_refused" and ING.BULK_FALLBACKS[1]["overwrites"] == 11


def test_a_stored_zero_is_not_a_non_zero_value_to_protect(wire, monkeypatch, tmp_path):
    """Only stored NON-zero values count as overwritten: 11 stored zeros under a fill of 5 do not trip the bound."""
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 11, stored_value=0.0, supp="5")
    assert transient is False and len(rows) == 248 and ING.BULK_FALLBACKS[0]["overwrites"] == 0


def test_a_fill_on_a_first_pull_is_not_blocked(wire, monkeypatch, tmp_path):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    fx = _fx("VAN1UGE")
    wire.json_answers["VAN1UGE"] = [500]
    wire.full["VAN1UGE"] = fx["bulk"]
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: {"variables": fx["tableinfo_variables"],
                                                                  "suppressedDataValue": "0"})
    rows, transient = D._fetch_table_rows("VAN1UGE")
    assert transient is False and len(rows) == 248 and ING.BULK_FALLBACKS[0]["overwrites"] == 0


def test_an_unreadable_store_refuses_the_fill(wire, monkeypatch, tmp_path):
    monkeypatch.setattr(D.blob, "exists", lambda p: True)
    monkeypatch.setattr(D.blob, "read_table", lambda p, columns=None: (_ for _ in ()).throw(OSError("disk")))
    (rows, transient), _ = _van(wire, monkeypatch, tmp_path, 0)
    assert (rows, transient) == ([], True) and ING.BULK_FALLBACKS[0]["status"] == "fill_unchecked"


# ---- T4: counted and named in the pass's note, never silent; status unchanged ----

def test_the_note_counts_a_table_once_and_sums_its_chunks(monkeypatch):
    monkeypatch.setattr(ING, "BULK_FALLBACKS", [
        {"table": "DNVALD", "cells": 1000, "streamed": 990, "filled": 10, "nofill": 0, "fill_value": "0",
         "status": "ok", "overwrites": 2},
        {"table": "DNVALD", "cells": 1000, "streamed": None, "filled": 0, "nofill": 0, "fill_value": "0",
         "status": "reads_disagree"},
        {"table": "VNASFK", "cells": 31, "streamed": 31, "filled": 0, "nofill": 0, "fill_value": "0",
         "status": "ok"}])
    assert D._bulk_fallback_note() == (
        "BULK fallback for 2 table(s) after a JSONSTAT HTTP 500: "
        "[DNVALD (990 of 2,000 cells streamed, 10 filled with suppressedDataValue '0', 0 left out with no fill value, "
        "2 stored non-zero overwritten, 1 reads disagree), "
        "VNASFK (31 of 31 cells streamed, 0 filled with suppressedDataValue '0', 0 left out with no fill value, "
        "0 stored non-zero overwritten)]")
    monkeypatch.setattr(ING, "BULK_FALLBACKS", [])
    assert D._bulk_fallback_note() == ""


def _run_update(wire, monkeypatch, tmp_path, fallback: bool, cap_after_first_chunk: bool = False):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    cat = [{"id": "VNASFK", "updated": "2026-09-30T08:00:00"}, {"id": "VNKSFK", "updated": "2026-09-30T08:00:00"}]
    monkeypatch.setattr(D, "_fetch_catalog", lambda *a, **k: [dict(t) for t in cat])
    meta = {t: {"variables": _fx(t)["tableinfo_variables"], "suppressedDataValue": "0"} for t in ("VNASFK", "VNKSFK")}
    monkeypatch.setattr(ING, "get_json", lambda url, retries=3: meta[url.split("id=")[1].split("&")[0]])
    for t in ("VNASFK", "VNKSFK"):
        wire.full[t] = _fx(t)["bulk"]
    if fallback:
        wire.json_answers["VNASFK"] = [500]
    else:                                  # the same table answering in JSONSTAT: a minimal valid body
        wire.json_answers["VNASFK"] = [{"dataset": {"dimension": {
            "VERSION": {"category": {"index": {"V": 0}}}, "Tid": {"category": {"index": {"2024": 0}}},
            "id": ["VERSION", "Tid"], "size": [1, 1], "role": {"time": ["Tid"]}}, "value": [1.0]}}]
    wire.json_answers["VNKSFK"] = wire.json_answers["VNASFK"]
    if cap_after_first_chunk:
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
    ING.BULK_FALLBACKS[:] = [{"table": "STALE_FROM_AN_EARLIER_PASS", "cells": 1, "streamed": 1, "filled": 0,
                              "nofill": 0, "fill_value": "0", "status": "ok"}]
    return D.update(None, None)


def test_a_pass_that_falls_back_names_it_and_stays_ok(wire, monkeypatch, tmp_path, capsys):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=True)
    assert res.status == "ok", (res.status, res.error)
    assert ("BULK fallback for 2 table(s) after a JSONSTAT HTTP 500: [VNASFK (31 of 31 cells streamed, 0 filled "
            "with suppressedDataValue '0', 0 left out with no fill value, 0 stored non-zero overwritten), VNKSFK "
            "(110 of 110 cells streamed") in (res.error or "")
    assert "STALE_FROM_AN_EARLIER_PASS" not in (res.error or "")
    out = capsys.readouterr().out
    assert "JSONSTAT POST answered HTTP 500 (SYSTEM-100)" in out and "31 of 31 cells streamed, 0 filled" in out
    assert "DST:VNASFK" in (res.series_cursors or {}), res.series_cursors      # merged: the subject's cursor


def test_a_budget_capped_pass_keeps_the_fallback_note(wire, monkeypatch, tmp_path):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=True, cap_after_first_chunk=True)
    assert res.status == "partial" and "budget spent with 1 of 2 due table(s)" in (res.error or ""), res.error
    assert "BULK fallback for 1 table(s) after a JSONSTAT HTTP 500: [VNASFK (31 of 31 cells streamed" \
        in (res.error or ""), res.error


def test_a_pass_without_a_fallback_says_nothing_about_bulk(wire, monkeypatch, tmp_path):
    res = _run_update(wire, monkeypatch, tmp_path, fallback=False)
    assert res.status == "ok" and "BULK" not in (res.error or ""), (res.status, res.error)
    assert "BULK" not in [c[1] for c in wire.calls]
