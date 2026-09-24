"""tools/derive_statcan_tables.py writes through the blob store (plan step 1): R2 before T0, the self-hosted
store after it. Run END TO END on a one-table store with a recording store in place of the R2 store blob.csv_store() builds."""
import datetime as dt
import gzip
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import derive_statcan_tables as D  # noqa: E402
from updater import blob  # noqa: E402


class Store:
    bucket = "econ-data"

    def __init__(self, have=()):
        self.put, self.have = {}, set(have)

    def list_keys(self, prefix):
        return [k for k in self.have if k.startswith(prefix)]

    def put_atomic(self, key, data):
        self.put[key] = data


@pytest.fixture
def run(tmp_path, monkeypatch):
    store_dir = tmp_path / "clean_full" / "statcan"
    store_dir.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["v1", "v1", "v2"],
                             "obs_date": [dt.date(2020, 1, 1), dt.date(2021, 1, 1), dt.date(2020, 1, 1)],
                             "value": [1.0, 2.0, 3.0]}), store_dir / "12100001.parquet")
    monkeypatch.setattr(D, "STORE", str(store_dir))
    monkeypatch.setattr(D, "ROOT", str(tmp_path))

    def go(store, *extra):
        monkeypatch.setenv("AQUEDUCT_BACKEND", "x")                       # then DELETED: the default route (R1200)
        monkeypatch.delenv("AQUEDUCT_BACKEND")
        from core import cutover
        monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
        monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)          # what csv_store() builds before T0
        monkeypatch.setattr(sys, "argv", ["derive_statcan_tables.py", "--bucket", "econ-data", "--workers", "1",
                                          *extra])
        return D.main()
    return go


def test_the_csvs_reach_the_blob_store_gzipped(run):
    store = Store()
    run(store)
    assert list(store.put) == [D.csv_key("series", D.unit_id("12100001"))], store.put.keys()
    body = next(iter(store.put.values()))
    assert body[:2] == b"\x1f\x8b", "gzipped at enqueue, handed to put_atomic as-is"
    assert gzip.decompress(body).decode().count("\n") == 4, "header + three rows"


def test_skip_existing_lists_the_blob_store(run):
    key = D.csv_key("series", D.unit_id("12100001"))
    store = Store(have={key})
    run(store, "--skip-existing")
    assert store.put == {}, "a key the store already lists is not written again"


def test_a_bucket_other_than_the_stores_is_refused(run):
    with pytest.raises(SystemExit, match="not the CSV store's bucket"):
        run(Store(), "--bucket", "some-other-bucket")


def test_a_failed_upload_fails_the_run(run, monkeypatch):
    """R1213: statcan ignoring _put_with_retry's False (or exiting 0 on errors) survived every test."""
    from updater import derive
    monkeypatch.setattr(derive, "_put_with_retry", lambda *a, **k: False)
    store = Store()
    assert run(store) == 1
    assert store.put == {}
