"""jobs/ingest_census.pull_slice: a variable-chunk merge that cannot identify rows is REFUSED (R1760).

The intltrade imports history was written by this merge with keys ['time','YEAR','MONTH']: chunk 1 carried no
commodity column, the API answered it at TOTAL grain, and the dict-merge copied the national total onto every
commodity row of the month. These tests drive the real pull_slice with only the HTTP layer faked."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = "timeseries/intltrade/imports/enduse"


@pytest.fixture
def ic(monkeypatch):
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    spec = importlib.util.spec_from_file_location("_ingest_census_merge", os.path.join(ROOT, "jobs", "ingest_census.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


DETAIL = [["2015-06", "A", "1", "10", "11", "12", "13"],
          ["2015-06", "B", "1", "20", "21", "22", "23"],
          ["2015-06", "-", "TO", "30", "32", "34", "36"]]         # TOTAL last, as the API returns it
HEAD = ["time", "COMM", "LVL", "V1", "V2", "V3", "V4"]


def _fake(rows_for):
    def f(path, req_get, geo_for, geo_in, time_clause, extra):
        rows = rows_for(req_get)
        return 200, [list(req_get)] + rows, "ok"
    return f


def _cols(req, rows):
    return [[r[HEAD.index(c)] for c in req] for r in rows]


def test_a_chunk_at_total_grain_is_refused_not_copied(ic, monkeypatch):
    m = {"allvars": list(HEAD), "path": PATH}
    ic._varlimit_cache[PATH] = 4                     # keys ['time'] -> chunk0 time,COMM,LVL,V1; chunk1 time,V2..V4

    def rows_for(req):
        if "COMM" in req:
            return _cols(req, DETAIL)
        return _cols(req, DETAIL[-1:])               # no dimension asked for: the API answers the TOTAL only
    monkeypatch.setattr(ic, "_fetch_chunk", _fake(rows_for))
    with pytest.raises(ic.MergeRefused, match="R1760"):
        ic.pull_slice(PATH, m, ["time"], None, None, "time=2015-06", {})


def test_a_merge_on_identifying_keys_still_works_when_rows_come_back_reordered(ic, monkeypatch):
    m = {"allvars": list(HEAD), "path": PATH}
    ic._varlimit_cache[PATH] = 5                     # keys time,COMM,LVL -> chunk0 +V1,V2; chunk1 +V3,V4
    keys = ["time", "COMM", "LVL"]

    def rows_for(req):
        rows = _cols(req, DETAIL)
        return rows if "V1" in req else list(reversed(rows))   # chunk 1 in another order: not aligned
    monkeypatch.setattr(ic, "_fetch_chunk", _fake(rows_for))
    header, rows, status = ic.pull_slice(PATH, m, keys, None, None, "time=2015-06", {})
    assert status == "ok"
    got = {tuple(r[header.index(k)] for k in ("COMM", "V1", "V4")) for r in rows}
    assert got == {("A", "10", "13"), ("B", "20", "23"), ("-", "30", "36")}, "each row keeps its own V4"


def test_aligned_chunks_are_unchanged(ic, monkeypatch):
    m = {"allvars": list(HEAD), "path": PATH}
    ic._varlimit_cache[PATH] = 5
    monkeypatch.setattr(ic, "_fetch_chunk", _fake(lambda req: _cols(req, DETAIL)))
    header, rows, status = ic.pull_slice(PATH, m, ["time", "COMM", "LVL"], None, None, "time=2015-06", {})
    assert status == "ok" and [r[header.index("V4")] for r in rows] == ["13", "23", "36"]
