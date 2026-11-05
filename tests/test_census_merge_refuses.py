"""jobs/ingest_census.pull_slice: a variable-chunk merge that cannot identify rows is REFUSED (R1760).

The intltrade imports history was written by this merge with keys ['time','YEAR','MONTH']: chunk 1 carried no
commodity column, the API answered it at TOTAL grain, and the dict-merge copied the national total onto every
commodity row of the month. These tests drive the real pull_slice with only the HTTP layer faked, and the fake
echoes `time` the way the API does (review AR-439: a fake that dropped it reached a different branch).

The two branches that stop a real copy, `multi` and `dup2`, each have a test that ONLY that branch can stop:
removing either one fails its test. The third, `not kcommon`, cannot fire alone - with no shared key and two or
more rows `multi` or `dup2` fires too, and one row on each side takes the aligned path - so its test only proves
that a chunk sharing no key is refused, by whichever branch."""
import importlib.util
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = "timeseries/intltrade/imports/enduse"
HEAD = ["time", "COMM", "LVL", "V1", "V2", "V3", "V4"]
DETAIL = [["2015-06", "A", "1", "10", "11", "12", "13"],
          ["2015-06", "B", "1", "20", "21", "22", "23"],
          ["2015-06", "-", "TO", "30", "32", "34", "36"]]         # TOTAL last, as the API returns it


@pytest.fixture
def ic(monkeypatch):
    monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
    spec = importlib.util.spec_from_file_location("_ingest_census_merge", os.path.join(ROOT, "jobs", "ingest_census.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _fake(rows_for, echo_time=True):
    """An API stand-in: the header is the requested variables plus `time`, which the API echoes from the
    time= predicate even though _not_in_get drops it from get=."""
    def f(path, req_get, geo_for, geo_in, time_clause, extra):
        head = list(req_get) + (["time"] if echo_time and "time" not in req_get else [])
        return 200, [head] + [[r[HEAD.index(c)] for c in head] for r in rows_for(req_get)], "ok"
    return f


def _m():
    return {"allvars": list(HEAD), "path": PATH, "required": ["time"]}


def test_r1760_a_chunk_at_total_grain_is_refused_not_copied(ic, monkeypatch):
    """Branch `multi`: keys ['time'] name all three chunk-0 rows; chunk 1 came back at TOTAL grain."""
    ic._varlimit_cache[PATH] = 4                     # chunk0 time,COMM,LVL,V1; chunk1 time,V2,V3,V4
    monkeypatch.setattr(ic, "_fetch_chunk",
                        _fake(lambda req: DETAIL if "COMM" in req else DETAIL[-1:]))
    with pytest.raises(ic.MergeRefused, match="name several chunk-0 rows"):
        ic.pull_slice(PATH, _m(), ["time"], None, None, "time=2015-06", {})


def test_a_chunk_whose_key_tuples_repeat_is_refused(ic, monkeypatch):
    """Branch `dup2`: chunk 0 is identified by its keys, but chunk 1 repeats a key tuple."""
    ic._varlimit_cache[PATH] = 5                     # keys time,COMM,LVL -> chunk0 +V1,V2; chunk1 +V3,V4
    dup = [DETAIL[0], DETAIL[0], DETAIL[1], DETAIL[2]]
    monkeypatch.setattr(ic, "_fetch_chunk", _fake(lambda req: DETAIL if "V1" in req else dup))
    with pytest.raises(ic.MergeRefused, match="repeat in chunk N"):
        ic.pull_slice(PATH, _m(), ["time", "COMM", "LVL"], None, None, "time=2015-06", {})


def test_a_chunk_sharing_no_key_is_refused(ic, monkeypatch):
    """A chunk 1 that carries no key column at all (and a different row count) is refused, never spliced."""
    ic._varlimit_cache[PATH] = 4
    monkeypatch.setattr(ic, "_fetch_chunk",
                        lambda path, req_get, *a: _fake(lambda req: DETAIL)(path, req_get, *a) if "COMM" in req_get
                        else (200, [["V2", "V3", "V4"], ["11", "12", "13"], ["21", "22", "23"]], "ok"))
    with pytest.raises(ic.MergeRefused, match=r"shared keys \[\]"):
        ic.pull_slice(PATH, _m(), ["time"], None, None, "time=2015-06", {})


def test_a_merge_on_identifying_keys_still_works_when_rows_come_back_reordered(ic, monkeypatch):
    ic._varlimit_cache[PATH] = 5
    monkeypatch.setattr(ic, "_fetch_chunk",
                        _fake(lambda req: DETAIL if "V1" in req else list(reversed(DETAIL))))
    header, rows, status = ic.pull_slice(PATH, _m(), ["time", "COMM", "LVL"], None, None, "time=2015-06", {})
    assert status == "ok"
    got = {tuple(r[header.index(k)] for k in ("COMM", "V1", "V4")) for r in rows}
    assert got == {("A", "10", "13"), ("B", "20", "23"), ("-", "30", "36")}, "each row keeps its own V4"


def test_aligned_chunks_are_unchanged(ic, monkeypatch):
    ic._varlimit_cache[PATH] = 5
    monkeypatch.setattr(ic, "_fetch_chunk", _fake(lambda req: DETAIL))
    header, rows, status = ic.pull_slice(PATH, _m(), ["time", "COMM", "LVL"], None, None, "time=2015-06", {})
    assert status == "ok" and [r[header.index("V4")] for r in rows] == ["13", "23", "36"]


def test_a_refusal_part_way_leaves_the_previous_file_whole(ic, monkeypatch, tmp_path):
    """AR-439: a refusal on slice 2 used to leave a valid SHORTER parquet at the final path, which --resume skips."""
    monkeypatch.setattr(ic, "OUT", str(tmp_path))
    out = tmp_path / (ic.safe_name(PATH) + ".parquet")
    pq.write_table(pa.table({"series_key": [f"k{i}" for i in range(1000)]}), out)
    before = out.read_bytes()
    monkeypatch.setattr(ic, "var_classes", lambda path: (set(), set()))
    monkeypatch.setattr(ic, "_slice_plan", lambda m, mx: iter([(None, None, "a", "time=2015-06", {}),
                                                              (None, None, "b", "time=2015-07", {})]))
    calls = []

    def fake_pull(path, m, keys, gfor, gin, tclause, extra):
        calls.append(tclause)
        if tclause.endswith("07"):
            raise ic.MergeRefused("planted")
        return HEAD, [list(r) for r in DETAIL], "ok"
    monkeypatch.setattr(ic, "pull_slice", fake_pull)
    with pytest.raises(ic.MergeRefused):
        ic.ingest_dataset(_m())
    assert calls == ["time=2015-06", "time=2015-07"], "slice 1 was written before the refusal"
    assert out.read_bytes() == before, "the previous file is untouched"
    assert not (tmp_path / (out.name + ".partial")).exists(), "and no partial is left behind"


def test_a_held_target_keeps_the_complete_partial_and_says_so(ic, monkeypatch, tmp_path):
    """AR-439 r2: a bare os.replace raised WinError 5 under a reader; the retrying helper is used, and when even
    it fails the complete run stays in the .partial with a message naming both paths."""
    monkeypatch.setattr(ic, "OUT", str(tmp_path))
    monkeypatch.setattr(ic, "var_classes", lambda path: (set(), set()))
    monkeypatch.setattr(ic, "_slice_plan", lambda m, mx: iter([(None, None, "a", "time=2015-06", {})]))
    monkeypatch.setattr(ic, "pull_slice", lambda *a: (HEAD, [list(r) for r in DETAIL], "ok"))
    import core.atomic as atomic
    seen = []

    def held(tmp, dest, *a, **k):
        seen.append((tmp, dest))
        raise PermissionError(5, "held open")
    monkeypatch.setattr(atomic, "atomic_replace", held)
    with pytest.raises(RuntimeError, match="nothing was lost"):
        ic.ingest_dataset(_m())
    out = tmp_path / (ic.safe_name(PATH) + ".parquet")
    assert seen and seen[0][1] == str(out), "the retrying helper was called for the final path"
    assert pq.read_metadata(str(out) + ".partial").num_rows == 3, "the complete run is kept"


def test_a_complete_run_replaces_the_file(ic, monkeypatch, tmp_path):
    monkeypatch.setattr(ic, "OUT", str(tmp_path))
    monkeypatch.setattr(ic, "var_classes", lambda path: (set(), set()))
    monkeypatch.setattr(ic, "_slice_plan", lambda m, mx: iter([(None, None, "a", "time=2015-06", {})]))
    monkeypatch.setattr(ic, "pull_slice", lambda *a: (HEAD, [list(r) for r in DETAIL], "ok"))
    n, _, _, truncated = ic.ingest_dataset(_m())
    out = tmp_path / (ic.safe_name(PATH) + ".parquet")
    assert n == 3 and not truncated and pq.read_metadata(out).num_rows == 3
    assert not (tmp_path / (out.name + ".partial")).exists()
