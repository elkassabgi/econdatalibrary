"""derive_usda_tables, derive_census_tables, derive_ilostat_indicators and derive_istat_flows write through the
CSV store (plan step 1): R2 before T0, the self-hosted blob store after it, never local files. Run END TO END
on a small store with the backend variable DELETED (review R1200) and a recording store in place of R2.

Every file these tools write by default (their summary in ROOT/logs - which cataloguers READ - the spill
folder, _split_map.json) is pointed at tmp_path: R1198 was a test that wrote the checkout's own state."""
import datetime as dt
import gzip
import importlib
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
from updater import blob  # noqa: E402


class Store:
    bucket = "econ-data"

    def __init__(self, have=()):
        self.put, self.have = {}, set(have)

    def list_keys(self, prefix):
        return [k for k in self.have if k.startswith(prefix)]

    def put_atomic(self, key, data, plain=False):
        self.__dict__.setdefault("plain", {})[key] = plain       # the encoding the tool asked for (R1206)
        self.put[key] = data


def _long(path, keys):
    pq.write_table(pa.table({"series_key": keys,
                             "obs_date": pa.array([dt.date(2020, 1, 1)] * len(keys), pa.date32()),
                             "value": [float(i + 1) for i in range(len(keys))]}), path)


def _usda(store_dir):
    pq.write_table(pa.table({"SOURCE_DESC": ["CENSUS", "CENSUS"], "AGG_LEVEL_DESC": ["STATE", "STATE"],
                             "SHORT_DESC": ["HOGS - SALES", "HOGS - SALES"],
                             "series_key": ["usda:a|b", "usda:a|c"], "REFERENCE_PERIOD_DESC": ["YEAR", "YEAR"],
                             "obs_date": pa.array([dt.date(2020, 1, 1)] * 2, pa.date32()),
                             "value": [1.0, 2.0]}), store_dir / "p0.parquet")


CASES = {
    "derive_usda_tables": (_usda, []),
    "derive_census_tables": (lambda d: _long(d / "tbl1.parquet", ["A=1|B=2", "A=1|B=3"]), []),
    "derive_ilostat_indicators": (lambda d: _long(d / "IND1.parquet", ["IND1.X", "IND1.Y"]), []),
    "derive_istat_flows": (lambda d: _long(d / "FLOW1.parquet", ["FLOW1:A=1", "FLOW1:A=2"]), []),
}


@pytest.fixture
def run(tmp_path, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    (tmp_path / "logs").mkdir()

    def go(name, store, *extra):
        m = importlib.import_module(name)
        store_dir = tmp_path / "clean_full" / m.SOURCE
        store_dir.mkdir(parents=True, exist_ok=True)
        CASES[name][0](store_dir)
        monkeypatch.setattr(m, "STORE", str(store_dir))
        monkeypatch.setattr(m, "ROOT", str(tmp_path))
        if hasattr(m, "duck_spill"):
            monkeypatch.setattr(m.duck_spill, "ensure_spill", lambda tag, sweep=True: str(tmp_path / "spill"))
            (tmp_path / "spill").mkdir(exist_ok=True)
        if hasattr(m, "_series_csv_bytes"):          # census builds bytes through the resolver
            monkeypatch.setattr(m, "_series_csv_bytes", lambda sid: b"series_id,obs_date,value\nx,2020-01-01,1\n")
        monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)
        monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", *extra])
        return m.main()
    return go


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_csvs_reach_the_csv_store(run, tmp_path, name):
    store = Store()
    rc = run(name, store)
    assert rc in (0, None), rc
    assert store.put, f"{name}: nothing reached the store"
    for k, body in store.put.items():
        assert k.startswith("series/") and k.endswith(".csv"), k
        text = (gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body).decode()
        assert text.startswith("series_id,obs_date,value\n"), text[:60]
    # each tool keeps the encoding it always had (R1206): usda and census stored plain, ilostat and istat gzip
    assert set(store.plain.values()) == {name in PLAIN}, (name, store.plain)
    assert not os.path.exists(os.path.join(ROOT, "data", "_aqueduct", "state.db")), "wrote the checkout's state"


PLAIN = {"derive_usda_tables", "derive_census_tables"}


@pytest.mark.parametrize("name", sorted(CASES))
def test_a_failed_upload_fails_the_run(run, monkeypatch, name):
    """R1204: a tool that ignored _put_with_retry's False survived every test - the store never refused."""
    from updater import derive
    monkeypatch.setattr(derive, "_put_with_retry", lambda *a, **k: False)
    try:
        rc = run(name, Store())
    except SystemExit as e:
        rc = e.code
    assert rc not in (0, None), f"{name}: a failed upload exited {rc!r}"


@pytest.mark.parametrize("name", sorted(CASES))
def test_skip_existing_lists_the_csv_store(run, name):
    first = Store()
    run(name, first)
    again = Store(have=set(first.put))
    run(name, again, "--skip-existing")
    assert again.put == {}, f"{name}: re-put what the store already lists"


@pytest.mark.parametrize("name", sorted(CASES))
def test_a_wrong_bucket_is_refused(run, name):
    with pytest.raises(SystemExit, match="not the CSV store's bucket"):
        run(name, Store(), "--bucket", "other")
