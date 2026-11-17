"""noaa: catalogued Canadian ids `CA00xxxxxxx` resolve to the store's renamed `CAN0xxxxxxx` keys.

NCEI renamed its Canadian stations from `CA00xxxxxxx` to `CAN0xxxxxxx`. The fetcher copies STATION
verbatim, so the store (gsom__CA, gsoy__CA) holds CAN keys while the catalogue holds CA0 ids, and
every one of those ids resolved to zero rows. The resolver now matches either form for a `CA0`
station, and the store-to-catalogue tools recognise a CAN key as already catalogued under its CA0
id. Every fixture here is hermetic: a few rows written to tmp_path.
"""
from __future__ import annotations

import datetime as dt
import gzip
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))
sys.path.insert(0, os.path.join(ROOT, "tools"))

from econdl import _resolve as R  # noqa: E402


def _shard(root, name, rows):
    """rows: [(series_key, 'YYYY-MM-DD', value)] -> <root>/noaa/<name>.parquet (store schema)."""
    d = os.path.join(str(root), "noaa")
    os.makedirs(d, exist_ok=True)
    keys = [r[0] for r in rows]
    tbl = pa.table({
        "dataset": pa.array([k.split(":")[0] for k in keys], pa.string()),
        "station": pa.array([k.split(":")[1] for k in keys], pa.string()),
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date.fromisoformat(r[1]) for r in rows], pa.date32()),
        "element": pa.array([k.split(":")[2] for k in keys], pa.string()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
        "attributes": pa.array([""] * len(rows), pa.string()),
    })
    path = os.path.join(d, f"{name}.parquet")
    pq.write_table(tbl, path)
    return path


CA_ROWS = [
    ("gsom:CAN04047080:TAVG", "2026-01-01", -12.5),     # renamed: catalogue holds CA004047080
    ("gsom:CAN04047080:TAVG", "2026-02-01", -9.0),
    ("gsom:CAN04047080:PRCP", "2026-01-01", 20.0),
    ("gsom:CA1AB000001:PRCP", "2026-01-01", 3.0),       # CoCoRaHS: never renamed
    ("gsom:CAW00064433:TAVG", "2026-01-01", -1.0),      # WMO-style id: never renamed
    ("gsom:CAN1AB00001:PRCP", "2026-01-01", 99.0),      # a CAN twin a CA1 id must NOT reach
    ("gsom:CAN04047081:TAVG", "2026-01-01", 55.0),      # a neighbouring station, different id
]


@pytest.fixture()
def store(tmp_path):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsom__CA", CA_ROWS)
    _shard(tmp_path, "gsom__US", [("gsom:USW00094728:TAVG", "2026-01-01", 1.5),
                                  ("gsom:USW00094728:TAVG", "2026-02-01", 2.5)])
    yield tmp_path
    R._NOAA_KEYS.clear()


def _read(sid, root):
    res = R.resolve(sid, root=str(root))
    return res, R.read_native(res)


# --- the station-form rule ---------------------------------------------------------------------

def test_station_forms_map_only_an_11_char_ca0_id():
    assert R.noaa_store_station_forms("CA004047080") == ("CA004047080", "CAN04047080")
    for s in ("CA1AB000001", "CAW00064433", "CAN04047080", "USW00094728", "CA00404708",
              "CA0040470800", "ca004047080"):
        assert R.noaa_store_station_forms(s) == (s,), s


def test_catalogue_forms_are_the_exact_inverse():
    assert R.noaa_catalogue_station_forms("CAN04047080") == ("CAN04047080", "CA004047080")
    for s in ("CA004047080", "CA1AB000001", "CAW00064433", "USW00094728", "CAN0404708"):
        assert R.noaa_catalogue_station_forms(s) == (s,), s
    # round trip on the mapped pair
    old = "CA001010066"
    assert R.noaa_catalogue_station_forms(R.noaa_store_station_forms(old)[1])[1] == old


def test_catalogue_ids_for_store_key():
    assert R.noaa_catalogue_ids_for_store_key("gsoy:CAN01010066:DSND") == (
        "noaa:gsoy:CAN01010066:DSND", "noaa:gsoy:CA001010066:DSND")
    assert R.noaa_catalogue_ids_for_store_key("gsom:USW00094728:TAVG") == (
        "noaa:gsom:USW00094728:TAVG",)


# --- the resolver ------------------------------------------------------------------------------

def test_ca0_catalogue_id_reads_the_can_store_rows(store):
    res, t = _read("noaa:gsom:CA004047080:TAVG", store)
    assert res.parquet_path.endswith("gsom__CA.parquet")
    assert set(t.column("series_key").to_pylist()) == {"gsom:CAN04047080:TAVG"}
    assert t.column("value").to_pylist() == [-12.5, -9.0]
    tidy = R.native_to_tidy(res, t)
    assert list(tidy["series_id"].unique()) == ["gsom:CAN04047080:TAVG"]
    # ONE exact equality on the form the store holds, the shape every consumer already handles
    # (tools read the predicate's shape; an OR over both forms is not that shape).
    import pyarrow.compute as pc
    import pyarrow.dataset as pds
    assert res.predicate.equals(pc.equal(pds.field("series_key"), "gsom:CAN04047080:TAVG"))


def test_uppercase_dataset_id_still_resolves_through_the_rename(store):
    _, t = _read("noaa:GSOM:CA004047080:PRCP", store)
    assert t.column("value").to_pylist() == [20.0]


@pytest.mark.parametrize("sid,key,val", [
    ("noaa:gsom:CA1AB000001:PRCP", "gsom:CA1AB000001:PRCP", [3.0]),
    ("noaa:gsom:CAW00064433:TAVG", "gsom:CAW00064433:TAVG", [-1.0]),
])
def test_ca1_and_caw_stations_are_unchanged(store, sid, key, val):
    _, t = _read(sid, store)
    assert set(t.column("series_key").to_pylist()) == {key}
    assert t.column("value").to_pylist() == val


def test_a_ca1_id_never_reaches_a_can_twin(store):
    # gsom:CAN1AB00001:PRCP exists; CA1AB000001 is a CoCoRaHS id and must keep its own rows only.
    _, t = _read("noaa:gsom:CA1AB000001:PRCP", store)
    assert 99.0 not in t.column("value").to_pylist()


def test_a_non_ca_station_is_unchanged(store):
    res, t = _read("noaa:gsom:USW00094728:TAVG", store)
    assert res.parquet_path.endswith("gsom__US.parquet")
    assert t.column("value").to_pylist() == [1.5, 2.5]
    assert R._NOAA_KEYS == {}            # a non-CA0 id never loads a shard's key set


def test_ca0_form_still_resolves_if_ncei_reverts(tmp_path):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsoy__CA", [("gsoy:CA001010066:DSND", "2025-12-31", 4.0)])
    _, t = _read("noaa:gsoy:CA001010066:DSND", tmp_path)
    assert set(t.column("series_key").to_pylist()) == {"gsoy:CA001010066:DSND"}


def test_both_forms_in_the_store_is_refused(tmp_path):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsom__CA", [("gsom:CA004047080:TAVG", "2020-01-01", 1.0),
                                  ("gsom:CAN04047080:TAVG", "2026-01-01", 2.0)])
    with pytest.raises(R.ResolveError, match="BOTH"):
        R.resolve("noaa:gsom:CA004047080:TAVG", root=str(tmp_path))


def test_both_forms_on_another_element_does_not_block_this_one(tmp_path):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsom__CA", [("gsom:CA004047080:PRCP", "2020-01-01", 1.0),
                                  ("gsom:CAN04047080:PRCP", "2026-01-01", 2.0),
                                  ("gsom:CAN04047080:TAVG", "2026-01-01", 3.0)])
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert t.column("value").to_pylist() == [3.0]


def test_a_station_gone_in_both_forms_gives_no_rows_and_no_wrong_match(store):
    # CA004047082 exists in neither form; CAN04047081 (a neighbour) must not be picked up.
    with pytest.raises(R.ResolveError, match="zero rows"):
        _read("noaa:gsom:CA004047082:TAVG", store)
    # an element gone from a station that IS present in the CAN form
    with pytest.raises(R.ResolveError, match="zero rows"):
        _read("noaa:gsom:CA004047080:SNOW", store)


def test_a_rewritten_shard_is_read_again(tmp_path):
    R._NOAA_KEYS.clear()
    p = _shard(tmp_path, "gsom__CA", [("gsom:CA004047080:TAVG", "2020-01-01", 1.0)])
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert t.column("value").to_pylist() == [1.0]
    st = os.stat(p)
    _shard(tmp_path, "gsom__CA", [("gsom:CAN04047080:TAVG", "2026-01-01", 2.0),
                                  ("gsom:CAN04047080:TAVG", "2026-02-01", 2.5)])
    os.utime(p, ns=(st.st_atime_ns, st.st_mtime_ns + 10_000_000_000))
    _, t = _read("noaa:gsom:CA004047080:TAVG", tmp_path)
    assert t.column("value").to_pylist() == [2.0, 2.5]
    assert len(R._NOAA_KEYS) == 1        # the stale key set of the same shard was dropped


# --- the derive (the CSV the worker serves) ----------------------------------------------------

def test_derive_writes_the_native_store_key_in_the_id_column(store, monkeypatch, tmp_path):
    monkeypatch.setenv("ECONDL_DATA", str(store))
    from core import derive_csv as D
    body = D._series_csv_bytes("noaa:gsom:CA004047080:TAVG").decode("utf-8")
    assert body == ("series_id,obs_date,value\n"
                    "gsom:CAN04047080:TAVG,2026-01-01,-12.5\n"
                    "gsom:CAN04047080:TAVG,2026-02-01,-9.0\n")
    # the memory-bounded streaming path writes the same bytes
    out = tmp_path / "s.csv.gz"
    D._series_csv_to_file("noaa:gsom:CA004047080:TAVG", str(out))
    assert gzip.decompress(out.read_bytes()).decode("utf-8") == body


def test_derive_still_refuses_a_station_gone_in_both_forms(store, monkeypatch, tmp_path):
    monkeypatch.setenv("ECONDL_DATA", str(store))
    from core import derive_csv as D
    with pytest.raises(Exception, match="zero rows"):
        D._series_csv_bytes("noaa:gsom:CA004047082:TAVG")
    with pytest.raises(D._ResolveZero):
        D._series_csv_to_file("noaa:gsom:CA004047082:TAVG", str(tmp_path / "x.csv.gz"))


def test_derive_refuses_both_forms(tmp_path, monkeypatch):
    R._NOAA_KEYS.clear()
    _shard(tmp_path, "gsom__CA", [("gsom:CA004047080:TAVG", "2020-01-01", 1.0),
                                  ("gsom:CAN04047080:TAVG", "2026-01-01", 2.0)])
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    from core import derive_csv as D
    for fn in (lambda: D._series_csv_bytes("noaa:gsom:CA004047080:TAVG"),
               lambda: D._series_csv_to_file("noaa:gsom:CA004047080:TAVG",
                                             str(tmp_path / "y.csv.gz"))):
        with pytest.raises(R.ResolveError, match="BOTH"):
            fn()


# --- the store-to-catalogue tools agree --------------------------------------------------------

def test_derive_noaa_missing_counts_a_renamed_key_as_catalogued():
    import derive_noaa_missing as M
    catalogued = {"gsom:CA004047080:TAVG", "gsom:USW00094728:TAVG"}
    keys = {"gsom:CAN04047080:TAVG",     # catalogued under its CA0 id
            "gsom:CAN09999999:TAVG",     # a station new since the rename: truly uncatalogued
            "gsom:USW00094728:TAVG",
            "gsom:CA1AB000001:PRCP"}
    assert M.uncatalogued(keys, catalogued) == ["gsom:CA1AB000001:PRCP", "gsom:CAN09999999:TAVG"]


def test_catalog_complete_counts_a_renamed_key_as_catalogued():
    import catalog_complete as C
    assert C.catalogue_ids_for("noaa", "gsom:CAN04047080:TAVG") == (
        "noaa:gsom:CAN04047080:TAVG", "noaa:gsom:CA004047080:TAVG")
    assert C.catalogue_ids_for("noaa", "gsom:USW00094728:TAVG") == ("noaa:gsom:USW00094728:TAVG",)
    # another source whose key happens to look like a renamed station is never mapped
    assert C.catalogue_ids_for("other", "gsom:CAN04047080:TAVG") == ("other:gsom:CAN04047080:TAVG",)


def test_catalog_noaa_refreshes_the_catalogued_ca0_row():
    import catalog_noaa as N
    con = sqlite3.connect(":memory:")
    con.execute("create table series (series_id text primary key)")
    con.executemany("insert into series values (?)", [("noaa:gsom:CA004047080:TAVG",),
                                                      ("noaa:gsom:CA005050505:PRCP",),
                                                      ("noaa:gsom:CAN05050505:PRCP",)])
    assert N.catalogue_id_for(con, "gsom:CAN04047080:TAVG") == ("noaa:gsom:CA004047080:TAVG", False)
    # never catalogued in either form: the native id, not an invented CA0 one
    assert N.catalogue_id_for(con, "gsom:CAN09999999:TAVG") == ("noaa:gsom:CAN09999999:TAVG", False)
    assert N.catalogue_id_for(con, "gsom:USW00094728:TAVG") == ("noaa:gsom:USW00094728:TAVG", False)
    # catalogued under both: reported, written under the native id
    assert N.catalogue_id_for(con, "gsom:CAN05050505:PRCP") == ("noaa:gsom:CAN05050505:PRCP", True)
