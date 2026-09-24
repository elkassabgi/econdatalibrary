"""statcan's whole-table refresh: the shared parser and the completeness gate (2026-09-22).

The fetcher used to refresh a changed cube by asking getBulkVectorDataByRange for 250 vectors at a
time; measured at ~0.16 s per vector, the release backlog was months of requests. It now downloads
the cube's full table - what the bulk ingester that built the store does - parses it with that
ingester's own parser, proves it complete, and merges it in.

These tests fake ONLY the network. The store is a real directory, the parse is the real
jobs/ingest_statcan.parse_zip_to_parquet, the key check is real DuckDB and the merge is the real
merge.merge_and_write_bounded - a model of those would not catch them drifting.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_statcan as ing  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402

PID = 24100058
HDR = ["REF_DATE", "GEO", "DGUID", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR",
       "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"]


def _row(ref, geo, vec, coord, val, status=""):
    return [ref, geo, "", "Vehicles", "1", "units", "0", vec, coord, val, status, "", "", "0"]


def _zip(path, rows, header=HDR, extra_lines=()):
    buf = io.StringIO()
    buf.write(",".join(f'"{h}"' for h in header) + "\n")
    for r in rows:
        buf.write(",".join(f'"{c}"' for c in r) + "\n")
    for ln in extra_lines:
        buf.write(ln + "\n")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{PID}.csv", buf.getvalue())
        z.writestr(f"{PID}_MetaData.csv", "meta\n")
    return path


# --------------------------------------------------------------------------- #
# the shared parser counts what it drops
# --------------------------------------------------------------------------- #
def test_the_parser_counts_every_row_it_drops_and_refuses_shifted_rows(tmp_path):
    z = _zip(tmp_path / "a.zip",
             [_row("2026-01", "Windsor", "v1", "1.1", "1.5"),
              _row("not-a-date", "Windsor", "v1", "1.1", "2"),
              _row("2026-02", "Windsor", "", "", "3")],
             extra_lines=['"2026-03","Windsor","","Vehicles","1","units","0","x","v1","1.1","4","","","","0"',
                          '"2026-04","Windsor"', ""])
    st = ing.parse_zip_to_parquet(str(z), str(tmp_path / "a.parquet"))
    assert st["n_obs"] == 1 and st["layout"] == "standard"
    assert st["skipped"] == {"bad_ref_date": 1, "no_key": 1, "long_row": 1, "short_row": 1,
                             "blank_line": 1}, st["skipped"]
    t = pq.read_table(tmp_path / "a.parquet")
    assert t.column("series_key").to_pylist() == ["v1"], "a shifted row must never become key '0'"
    assert os.path.exists(z), "parse_zip_to_parquet leaves the zip to its caller"
    assert not os.path.exists(str(tmp_path / "a.parquet") + ".part")


HDR_DIMS = ["REF_DATE", "GEO", "DGUID", "Characteristics", "UOM", "UOM_ID", "SCALAR_FACTOR",
            "SCALAR_ID", "VECTOR", "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"]


def _dims_row(ref, geo, dguid, char, vec, coord, val):
    return [ref, geo, dguid, char, "Number", "223", "units", "0", vec, coord, val, "", "", "", "0"]


def test_statcans_broken_quoting_is_repaired_from_the_right_not_read_by_position(tmp_path):
    """What StatCan really serves (measured 2026-09-23): an embedded quote splits one text field in
    two. 13100442 splits a DIMENSION, 46100078/9 split GEO. Every trailing column is intact from the
    right, and DGUID says where the split fell."""
    good = _dims_row("2021", "Canada", "2016A000011124", "Total", "v1", "1.1", "10")
    # The fixture writes each list item as one quoted field, so a split is modelled as two items
    # (no embedded quotes: _zip does not escape them, and the field count is what matters here).
    dim_split = ["2021", "Canada", "2016A000011124", "rated good", " very good", "Number", "223",
                 "units", "0", "v2", "1.2", "20", "", "", "", "0"]
    geo_split = ["2021", "Peigan Timber Limit B", " Alberta", "2021A00054803805", "Rural area",
                 "Number", "223", "units", "0", "v3", "1.3", "", "x", "", "", "0"]
    hopeless = ["2021", "a", "b", "c", "d", "Number", "223", "units", "0", "v4", "1.4", "40", "", "",
                "", "0"]                                     # no geography code anywhere
    # DGUID intact, but the stray field is at the END: counted from the right, VECTOR's slot holds
    # the coordinate - no vector id there, so it is refused, never keyed on '1.5'
    trailing = _dims_row("2021", "Canada", "2016A000011124", "Total", "v5", "1.5", "50") + ["junk"]
    # a split INSIDE UOM is the documented limit: indistinguishable from a dimension split, so it is
    # repaired with the right key/value and only UOM's second fragment as `uom` (pinned, not hidden)
    uom_split = ["2021", "Canada", "2016A000011124", "Total", "Number of", " persons", "223",
                 "units", "0", "v6", "1.6", "60", "", "", "", "0"]
    z = _zip(tmp_path / "q.zip", [good, dim_split, geo_split, hopeless, trailing, uom_split],
             header=HDR_DIMS)
    st = ing.parse_zip_to_parquet(str(z), str(tmp_path / "q.parquet"))
    assert st["skipped"] == {"repaired_long_row": 3, "long_row": 2}, st["skipped"]
    rows = {r["series_key"]: r for r in pq.read_table(tmp_path / "q.parquet").to_pylist()}
    assert sorted(rows) == ["v1", "v2", "v3", "v6"], "keys come from VECTOR, counted from the right"
    assert rows["v2"]["value"] == 20.0 and rows["v2"]["coordinate"] == "1.2"
    assert rows["v2"]["geo"] == "Canada", "a dimension split leaves GEO intact"
    assert rows["v3"]["geo"] == "Peigan Timber Limit B, Alberta", "a GEO split is rejoined"
    assert rows["v3"]["value"] is None and rows["v3"]["status"] == "x"
    assert rows["v6"]["value"] == 60.0 and rows["v6"]["uom"] == " persons", "the documented limit"


def test_a_census_row_longer_than_its_header_is_counted_not_read_by_position(tmp_path):
    hdr = ["REF_DATE", "GEO", "Coordinate", "Total [1]", "Symbol", "Male [2]", "Symbol"]
    rows = [["2021", "Canada", "1.1", "5", "", "6", ""],
            ["2021", "Canada", "1.2", "7", "", "8", "", "extra"]]
    z = _zip(tmp_path / "c.zip", rows, header=hdr)
    st = ing.parse_zip_to_parquet(str(z), str(tmp_path / "c.parquet"))
    assert st["layout"] == "census" and st["n_obs"] == 2 and st["skipped"] == {"long_row": 1}


def test_the_bulk_run_says_so_when_it_drops_rows(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(ing, "ERRLOG", str(tmp_path / "_errors.log"))
    z = _zip(tmp_path / "d.zip", [_row("2026-01", "Windsor", "v1", "1.1", "1.5"),
                                  _row("bad", "Windsor", "v1", "1.1", "2")])
    ing._parse_and_write({"productId": PID}, PID, str(z), str(tmp_path / "d.parquet"),
                         str(tmp_path / "d.done"))
    assert "DROPPED" in capsys.readouterr().out
    assert "bad_ref_date" in (tmp_path / "_errors.log").read_text(encoding="utf-8")


def test_the_bulk_run_still_removes_its_zip_and_records_the_skips(tmp_path):
    z = _zip(tmp_path / "b.zip", [_row("2026-01", "Windsor", "v1", "1.1", "1.5")])
    st = ing._parse_and_write({"productId": PID}, PID, str(z), str(tmp_path / "b.parquet"),
                              str(tmp_path / "b.done"))
    assert not os.path.exists(z)
    done = json.loads((tmp_path / "b.done").read_text(encoding="utf-8"))
    assert done["n_obs"] == st["n_obs"] == 1 and done["n_skipped"] == {}


# --------------------------------------------------------------------------- #
# the completeness gate
# --------------------------------------------------------------------------- #
def test_a_standard_table_must_match_statcans_counts_exactly():
    ok = {"layout": "standard", "n_obs": 10, "n_series": 2, "series_capped": False, "skipped": {}}
    assert sc._check_complete(PID, ok, 10, 2) is False
    with pytest.raises(DefinitiveError, match="nbDatapointsCube"):
        sc._check_complete(PID, ok, 11, 2)
    with pytest.raises(DefinitiveError, match="nbSeriesCube"):
        sc._check_complete(PID, ok, 10, 3)
    capped = dict(ok, series_capped=True)
    assert sc._check_complete(PID, capped, 10, 999) is False, "an uncounted series total is not compared"


def test_a_census_table_is_judged_by_its_own_skips_and_asks_for_the_stored_floor():
    """98100073 as measured: StatCan's counts cannot vouch for a Census table."""
    st = {"layout": "census", "n_obs": 16_797_696, "n_series": 2_000_000, "series_capped": True,
          "skipped": {"blank_line": 2}}
    assert sc._check_complete(PID, st, 16_636_456, 1) is True
    with pytest.raises(DefinitiveError, match="dropped rows"):
        sc._check_complete(PID, dict(st, skipped={"short_row": 1}), 16_636_456, 1)


def test_a_coordinate_keyed_standard_table_is_vectorless_too():
    """12100147 as measured: standard layout, nbSeriesCube 1, 244,601 series parsed, and a datapoint
    count 1.7M above the rows even though the parser dropped nothing."""
    st = {"layout": "standard", "n_obs": 18_225_522, "n_series": 244_601, "series_capped": False,
          "skipped": {}}
    assert sc._check_complete(PID, st, 19_940_793, 1) is True
    # negative control: a genuine one-vector cube is still held to the exact count
    one = {"layout": "standard", "n_obs": 10, "n_series": 1, "series_capped": False, "skipped": {}}
    with pytest.raises(DefinitiveError, match="nbDatapointsCube"):
        sc._check_complete(PID, one, 11, 1)


# The end-to-end refresh tests that lived here (a changed cube refreshed, revised, extended and
# relabelled; a short table refused; a corrupt zip transient; the vectorless floor; an identical
# table quiet) now run against the LANE, which does the refresh since 2026-09-23:
# tests/test_statcan_lane.py.
