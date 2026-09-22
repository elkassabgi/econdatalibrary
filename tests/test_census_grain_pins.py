"""The fetcher must ask for the grain it STORES, not a finer one.

Measured 2026-09-20 against the live API. intltrade/imports/sitc stores 45 DISTRICT values but
exactly ONE CTY_CODE ('-', the country aggregate), and the request pinned neither - so the API was
asked for every country. That request returns HTTP 500 after ~150 s. Pinning CTY_CODE='-' with the
same 47 columns returns HTTP 200 in 64 s with ~71,377 rows against 71,377 rows stored for that
month: an exact match to the stored grain.

The pins are therefore derived from the store itself, with two rules that are not optional:

  * a dimension must hold ONE value in EVERY key. A dimension missing from some keys is a
    heterogeneous shape, and pinning it would narrow those rows away.
  * a revision marker is never pinned. LAST_UPDATE looks single-valued and pinning it would
    silently exclude exactly the revised rows a refresh exists to collect.

Flows whose explosion is driven by a dimension we genuinely vary (exports/hs and imports/hs, whose
commodity dimensions hold 18,511 and 31,229 stored values) are NOT fixed by this and still fail -
measured, not assumed. This change does not claim them.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import census as cs  # noqa: E402


class _FakeBlob:
    """Just enough of the blob backend for _dims_from_store: a schema and a series_key column."""

    def __init__(self, keys, cols):
        self._keys = keys
        self._cols = cols

    def read_schema(self, path):
        return pa.schema([(c, pa.string()) for c in self._cols])

    def read_table(self, path, columns=None):
        if columns and columns != ["series_key"]:
            # honour the requested column so the split derivation can be driven too
            return pa.table({c: pa.array(self._cols_data.get(c, [])) for c in columns})
        return pa.table({"series_key": pa.array(self._keys)})

    _cols_data: dict = {}


def _dims(monkeypatch, keys, cols=("series_key", "obs_date", "AMOUNT")):
    monkeypatch.setattr(cs, "blob", _FakeBlob(keys, list(cols)))
    return cs._dims_from_store("ignored.parquet")


def test_a_dimension_held_at_one_value_everywhere_is_pinned(monkeypatch):
    keys = [
        "flow|CTY_CODE=-|SITC=0011|DISTRICT=10",
        "flow|CTY_CODE=-|SITC=0012|DISTRICT=27",
        "flow|CTY_CODE=-|SITC=0013|DISTRICT=10",
    ]
    _d, _c, _s, pins = _dims(monkeypatch, keys)
    assert pins == {"CTY_CODE": "-"}, pins


def test_a_dimension_that_varies_is_not_pinned(monkeypatch):
    keys = ["flow|CTY_CODE=-|DISTRICT=10", "flow|CTY_CODE=1220|DISTRICT=10"]
    _d, _c, _s, pins = _dims(monkeypatch, keys)
    assert "CTY_CODE" not in pins
    assert pins == {"DISTRICT": "10"}


def test_a_dimension_missing_from_some_keys_is_not_pinned(monkeypatch):
    """A heterogeneous shape: STATE is single-valued where present, but absent elsewhere. Pinning
    it would narrow away every row that does not carry it."""
    keys = ["flow|SUMMARY_LVL=DET|STATE=06", "flow|SUMMARY_LVL=DET"]
    _d, _c, _s, pins = _dims(monkeypatch, keys)
    assert pins == {"SUMMARY_LVL": "DET"}, pins
    assert "STATE" not in pins


@pytest.mark.parametrize("marker", ["LAST_UPDATE", "TIME", "YEAR", "MONTH", "QUARTER"])
def test_a_revision_or_time_marker_is_never_pinned(monkeypatch, marker):
    keys = [f"flow|{marker}=0|CTY_CODE=-", f"flow|{marker}=0|CTY_CODE=-"]
    _d, _c, _s, pins = _dims(monkeypatch, keys)
    assert marker not in pins, f"{marker} must never be pinned"
    assert pins == {"CTY_CODE": "-"}


def test_no_single_valued_dimension_yields_no_pins(monkeypatch):
    keys = ["flow|A=1|B=1", "flow|A=2|B=2"]
    _d, _c, _s, pins = _dims(monkeypatch, keys)
    assert pins == {}


def test_every_return_path_yields_four_values(monkeypatch):
    """An empty store must not return a differently shaped tuple. The caller unpacks four, so a
    2-tuple here is a ValueError that kills the source - and it sat behind a short-circuit, which
    is exactly how a latent crash survives review."""
    monkeypatch.setattr(cs, "blob", _FakeBlob([], ["series_key", "obs_date"]))
    got = cs._dims_from_store("empty.parquet")
    assert len(got) == 4, got
    dims, cols, shapes, pins = got
    assert dims == [] and shapes == [] and pins == {}


def test_a_flow_with_no_declared_split_sends_one_request(monkeypatch):
    monkeypatch.setattr(cs, "blob", _FakeBlob(["flow|A=1"], ["series_key"]))
    got = cs._split_requests("intltrade/imports/sitc", "p.parquet", ["A"], {"CTY_CODE": "-"})
    assert got == [{"CTY_CODE": "-"}], got


def test_a_declared_flow_splits_into_one_request_per_stored_value(monkeypatch):
    """Each part carries the base pins PLUS its own value of the split dimension."""
    fb = _FakeBlob(["flow|COMM_LVL=NA6"], ["series_key", "COMM_LVL"])
    fb._cols_data = {"COMM_LVL": ["NA6", "NA5", "NA6", "-"]}
    monkeypatch.setattr(cs, "blob", fb)
    got = cs._split_requests("intltrade/exports/naics", "p.parquet", ["COMM_LVL"],
                             {"SUMMARY_LVL": "DET", "CTY_CODE": "-"})
    assert len(got) == 3, got
    assert all(r["SUMMARY_LVL"] == "DET" and r["CTY_CODE"] == "-" for r in got)
    assert sorted(r["COMM_LVL"] for r in got) == ["-", "NA5", "NA6"]


def test_it_refuses_to_split_on_a_dimension_absent_from_the_key(monkeypatch):
    """Splitting on a dimension the key omits would iterate values the key cannot tell apart, so
    rows for a value we do not store would rebuild an existing key and merge into it."""
    fb = _FakeBlob(["flow|OTHER=1"], ["series_key", "COMM_LVL"])
    fb._cols_data = {"COMM_LVL": ["NA6", "NA5"]}
    monkeypatch.setattr(cs, "blob", fb)
    got = cs._split_requests("intltrade/exports/naics", "p.parquet", ["OTHER"], {"A": "1"})
    assert got == [{"A": "1"}], got


def test_a_split_slice_with_no_rows_is_reported_not_swallowed():
    """Unsplit, an empty body leaves `parts` empty and the structural guard fires. Split, it is one
    slice of several: the others merge and the flow reports success over a month that is quietly
    short. It can be legitimate, so it is reported rather than failed - but never silent."""
    import inspect
    code = "\n".join(ln.split("#")[0] for ln in inspect.getsource(cs).splitlines())
    assert "empty_slices.append(" in code, "an empty split slice must be recorded"
    assert "empty_slices: list = []" in code, "the per-flow accumulator must be reset per flow"
    assert "split slice(s) returned no rows" in code, "and it must be printed"


def test_the_split_list_is_declared_not_inferred():
    """A fallback that split whenever a request raised was reviewed and rejected: the exception
    carries no status and no timing, so a network blip would fan out into seven requests."""
    assert cs._SPLIT_DIM == {"intltrade/exports/naics": "COMM_LVL",
                             "intltrade/imports/statehs": "STATE"}, cs._SPLIT_DIM
    # Pinned exactly, so ADDING a flow is a deliberate act that has to update this test - a split
    # multiplies request count and each entry was earned by measuring that its parts reproduce the
    # stored month (naics 73,628 = 73,628 on 7 slices; statehs 4,935 = 4,935 on all 53).


def test_a_single_valued_column_outside_the_key_is_pinned(tmp_path):
    """The case that mattered most. exports/hs and imports/hs each store exactly ONE CTY_CODE, but
    CTY_CODE is NOT in their series_key - so a key-only derivation could not see it and the request
    still asked for every country. Those rows come back at a finer grain, and because the key omits
    CTY_CODE a per-country row rebuilds the SAME key as the world total and ADDS under a published
    id. Pinning it took both flows from HTTP 500 to exactly the stored row count."""
    import pyarrow.parquet as pq_

    f = tmp_path / "flow.parquet"
    pq_.write_table(pa.table({
        "series_key": ["flow|E_COMMODITY=01|COMM_LVL=HS2", "flow|E_COMMODITY=02|COMM_LVL=HS2"],
        "CTY_CODE": ["-", "-"],          # single-valued, NOT in the key
        "DISTRICT": ["-", "-"],          # likewise
        "GEN_VAL_MO": ["1", "2"],        # varies, must not be pinned
        "LAST_UPDATE": ["0", "0"],       # single-valued but a revision marker
        "CTY_NAME": ["TOTAL", "TOTAL"],  # single-valued but a LABEL, not a selector
    }), f)
    got = cs._single_valued_columns(str(f), ["CTY_CODE", "DISTRICT", "GEN_VAL_MO",
                                             "LAST_UPDATE", "CTY_NAME"])
    assert got == {"CTY_CODE": "-", "DISTRICT": "-"}, got


def test_an_unreadable_file_pins_nothing_rather_than_guessing(tmp_path):
    """Fail safe: a column that cannot be PROVEN single-valued is simply not pinned."""
    assert cs._single_valued_columns(str(tmp_path / "nope.parquet"), ["CTY_CODE"]) == {}


def test_the_pins_reach_the_request(monkeypatch):
    """The wiring, not just the derivation: a pin that never reaches params changes nothing."""
    seen = {}

    class _Resp:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        text = "[[\"a\"],[\"1\"]]"

        def json(self):
            return [["a"], ["1"]]

    class _Sess:
        def get(self, url, params=None, timeout=None):
            seen.update(params or {})
            return _Resp()

    cs._fetch(_Sess(), "intltrade/imports/sitc", ["A", "B"], "2026-03", "k", None,
              {"CTY_CODE": "-"})
    assert seen.get("CTY_CODE") == "-", seen
    assert seen.get("get") == "A,B" and seen.get("time") == "2026-03"
    assert "for" not in seen, "pred None must still send no `for`"


def test_a_flow_with_no_pins_sends_none(monkeypatch):
    seen = {}

    class _Resp:
        status_code = 204
        headers = {"Content-Type": "application/json"}
        text = ""

    class _Sess:
        def get(self, url, params=None, timeout=None):
            seen.update(params or {})
            return _Resp()

    cs._fetch(_Sess(), "f", ["A"], "2026-03", None, None, {})
    assert set(seen) == {"get", "time"}, seen
