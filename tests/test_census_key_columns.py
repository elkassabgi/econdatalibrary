"""jobs/ingest_census: every variable chunk carries the row-identity dimensions (R1760, review AR-439).

asm/area2017 and asm/area2012 hold, in their later variable chunks, the NAICS 31-33 total on every NAICS row
(PAYANN 692,979,103 on NAICS 311, 3111, 31111 ...). key_columns kept only the required columns and a fixed
identifier list, so chunks 1..N never asked for NAICS2017 and the API answered them at the 31-33 total. These tests
drive the real pull_slice with only the HTTP layer faked, in the production shape: the API echoes `time` (from the
time= predicate) and the geography (`us`), and a request without the classification column is answered at its
TOTAL. Restoring the old key_columns makes the first test fail (the merge is refused, review AR-439's guard)."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = "timeseries/asm/area2017"
TCLAUSE = "time=from+2017+to+2017"
# allvars in the API's own (unsorted) order; NAICS2017 sits in chunk 0, the measures run on into later chunks
ALLVARS = ["time", "YEAR", "GEO_ID", "NAICS2017", "PAYANN", "EMP", "SECTOR", "VALADD"]
# one row per NAICS code; the TOTAL row (31-33) is what the API returns when NAICS2017 is not requested
DATA = {"31-33": {"PAYANN": "692979103", "EMP": "11633292", "SECTOR": "31", "VALADD": "2606338235"},
        "311":   {"PAYANN": "1001", "EMP": "11", "SECTOR": "31", "VALADD": "5001"},
        "3111":  {"PAYANN": "1002", "EMP": "12", "SECTOR": "31", "VALADD": "5002"},
        "325":   {"PAYANN": "2001", "EMP": "21", "SECTOR": "32", "VALADD": "6001"}}


@pytest.fixture
def ic(monkeypatch):
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    spec = importlib.util.spec_from_file_location("_ingest_census_keys", os.path.join(ROOT, "jobs", "ingest_census.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._varlimit_cache[PATH] = 5          # a small get= cap forces several chunks, as 193 variables do at 45
    return mod


def _m(allvars=ALLVARS, path=PATH):
    return {"allvars": list(allvars), "path": path, "required": ["time"]}


def _api(calls, reverse_later=False):
    """Production-shaped stand-in for _fetch_chunk. Columns: the requested variables, then `time` and `us` echoed
    from the predicates. Rows: one per NAICS code when NAICS2017 is requested, else only the 31-33 TOTAL."""
    def f(path, req_get, geo_for, geo_in, time_clause, extra):
        calls.append(list(req_get))
        head = list(req_get) + [c for c in ("time", "us") if c not in req_get]
        codes = list(DATA) if "NAICS2017" in req_get else ["31-33"]
        if reverse_later and len(calls) > 1:
            codes = list(reversed(codes))

        def cell(code, c):
            if c == "time":
                return "2017"
            if c == "us":
                return "1"
            if c == "GEO_ID":
                return "0100000US"
            if c == "NAICS2017":
                return code
            return DATA[code][c]
        return 200, [head] + [[cell(code, c) for c in head] for code in codes], "ok"
    return f


def _by_naics(header, rows):
    return {r[header.index("NAICS2017")]: {c: r[header.index(c)] for c in ("PAYANN", "EMP", "SECTOR", "VALADD")}
            for r in rows}


def test_asm_later_chunks_merge_per_naics_row_not_the_total(ic, monkeypatch):
    """R1760 / AR-439: chunk 1+ lacked NAICS2017 and came back as the 31-33 total. Now each row keeps its own values.
    Reverting key_columns to the identifiers alone fails this test (MergeRefused: the shared keys name 4 rows)."""
    calls = []
    monkeypatch.setattr(ic, "_fetch_chunk", _api(calls))
    header, rows, status = ic.pull_slice(PATH, _m(), ic.key_columns(_m()), "us:*", None, TCLAUSE, {})
    assert status == "ok" and len(calls) > 1, "several chunks were requested"
    assert all("NAICS2017" in c for c in calls), "every chunk asks for the classification column"
    assert _by_naics(header, rows) == DATA, "each NAICS row carries its own measures, not the 31-33 total"


def test_asm_merge_is_by_key_when_a_later_chunk_comes_back_reordered(ic, monkeypatch):
    calls = []
    monkeypatch.setattr(ic, "_fetch_chunk", _api(calls, reverse_later=True))
    header, rows, status = ic.pull_slice(PATH, _m(), ic.key_columns(_m()), "us:*", None, TCLAUSE, {})
    assert status == "ok" and _by_naics(header, rows) == DATA


def test_chunk0_and_the_header_keep_the_layout_they_had(ic, monkeypatch):
    """A table that merged correctly must not change shape: chunk 0's request and the merged column order are the
    ones the identifier-only keys produced (the order series_key is built in)."""
    calls = []
    monkeypatch.setattr(ic, "_fetch_chunk", _api(calls))
    m = _m()
    assert ic.key_columns(m) == ["time", "YEAR", "GEO_ID", "NAICS2017"]
    header, rows, _ = ic.pull_slice(PATH, m, ic.key_columns(m), "us:*", None, TCLAUSE, {})
    # before R1760: chunk 0 = time,YEAR,GEO_ID + 2 values; time and YEAR are predicates, not in get=
    assert calls[0] == ["GEO_ID", "NAICS2017", "PAYANN"]
    assert header == ["GEO_ID", "NAICS2017", "PAYANN", "time", "us", "EMP", "SECTOR", "VALADD"]


def test_a_classification_column_that_misses_chunk_0_is_refused_before_any_request(ic, monkeypatch):
    """intltrade usda / sitcimport, aies NAICS: the code sits past chunk 0, so chunk 0 was answered at a coarser
    grain. Taking it in would change the table's columns, rows and series_key - refused, not done silently."""
    calls = []
    monkeypatch.setattr(ic, "_fetch_chunk", _api(calls))
    m = _m(["time", "YEAR", "GEO_ID", "PAYANN", "EMP", "NAICS2017", "SECTOR", "VALADD"])
    with pytest.raises(ic.MergeRefused, match=r"\['NAICS2017'\] do not fit in chunk 0"):
        ic.pull_slice(PATH, m, ic.key_columns(m), "us:*", None, TCLAUSE, {})
    assert calls == [], "nothing was requested"


def test_a_positional_splice_needs_keys_that_identify_rows(ic, monkeypatch):
    """Same row count and the shared keys equal row for row is NOT proof when those keys are ['time'] over four
    rows: the splice cannot show which row a value belongs to. It goes to the dict-merge, whose guard refuses it.
    (The old sampled check spliced it.) Removing the uniqueness clause fails this test."""
    def f(path, req_get, geo_for, geo_in, time_clause, extra):
        head = list(req_get) + [c for c in ("time", "us") if c not in req_get]
        val = {"time": "2017", "us": "1", "GEO_ID": "0100000US"}
        return 200, [head] + [[val.get(c, f"{c}{i}") for c in head] for i in range(4)], "ok"
    monkeypatch.setattr(ic, "_fetch_chunk", f)
    with pytest.raises(ic.MergeRefused, match="name several chunk-0 rows"):
        ic.pull_slice(PATH, _m(), ["time", "YEAR", "GEO_ID"], "us:*", None, TCLAUSE, {})


def test_identity_dims_are_codes_not_labels_or_attributes(ic):
    m = _m(["time", "YEAR", "GEO_ID", "NAICS2017", "NAICS2017_LABEL", "SECTOR", "INDLEVEL", "I_COMMODITY",
            "I_COMMODITY_LDESC", "COMM_LVL", "SUMMARY_LVL", "SUMMARY_LVL2", "DISTRICT", "DIST_NAME", "GEN_VAL_MO"])
    assert ic.identity_dims(m) == ["NAICS2017", "I_COMMODITY", "COMM_LVL", "SUMMARY_LVL", "SUMMARY_LVL2",
                                   "DISTRICT"]
