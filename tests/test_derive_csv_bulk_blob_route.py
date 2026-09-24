"""tools/derive_csv_bulk.py writes through the blob store (plan step 1): R2 before T0, the self-hosted store
after it. Run END TO END (in process) on a one-parquet store with a recording store for blob.from_env()."""
import datetime as dt
import gzip
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from tools import derive_csv_bulk as dcb  # noqa: E402
from updater import blob  # noqa: E402

UTC = dt.timezone.utc


class Store:
    bucket = "econ-data"

    def __init__(self, listed=()):
        self.put, self.listed = {}, list(listed)

    def list_modified(self, prefix):
        return [(k, t) for k, t in self.listed if k.startswith(prefix)]

    def put_atomic(self, key, data):
        self.put[key] = data


@pytest.fixture
def run(tmp_path, monkeypatch):
    root = tmp_path / "root"
    d = root / "data" / "clean_full" / "zzsrc"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": pa.array(["A", "B"], pa.string()),
                             "obs_date": pa.array([dt.date(2020, 12, 31)] * 2, pa.date32()),
                             "value": pa.array([1.0, 2.0], pa.float64())}), str(d / "t.parquet"))
    monkeypatch.setattr(dcb, "ROOT", str(root))
    import core.derive_csv as cdc
    monkeypatch.setattr(cdc, "_mirror_behind_store", lambda sources, sample=0: [])
    reg = tmp_path / "registry.yaml"
    reg.write_text("- source_id: zzsrc\n", encoding="utf-8")
    from updater import config as ucfg
    monkeypatch.setattr(ucfg, "REGISTRY", str(reg))
    # main() records its campaign in state.db (StateStore() -> config.STATE_DB). Unpatched, the first
    # version of this test wrote the CHECKOUT's data/_aqueduct/state.db, and the swap tests then built
    # their freshness projection from that empty file (26 failures in the whole-suite run).
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setattr(ucfg, "STATE_DIR", str(state_dir))
    monkeypatch.setattr(ucfg, "STATE_DB", str(state_dir / "state.db"))

    def go(store, *extra):
        monkeypatch.setattr(blob, "from_env", lambda *a, **k: store)
        monkeypatch.setattr(sys, "argv", ["derive_csv_bulk.py", "--source", "zzsrc", "--bucket", "econ-data",
                                          "--verify", "0", "--failed-keys-file", str(tmp_path / "failed.tsv"),
                                          *extra])
        return dcb.main()
    return go


def test_plain_csvs_reach_put_atomic(run):
    store = Store()
    rc = run(store)
    assert rc in (0, None), rc
    assert len(store.put) == 2, list(store.put)
    body = next(iter(store.put.values()))
    assert body[:2] != b"\x1f\x8b", "the PLAIN csv: put_atomic gzips it and records its md5 (series_csv_put_args)"
    assert body.decode().startswith("series_id,obs_date,value")


def test_skip_newer_than_uses_the_store_listing_times(run):
    store0 = Store()
    run(store0)
    written = sorted(store0.put)
    old = dt.datetime(2026, 1, 1, tzinfo=UTC)
    new = dt.datetime(2026, 9, 24, 12, tzinfo=UTC)
    store = Store(listed=[(written[0], new), (written[1], old)])
    run(store, "--skip-newer-than", "2026-09-24T00:00:00Z")
    assert sorted(store.put) == [written[1]], "only what this campaign did NOT already write is re-put"


def test_a_bucket_other_than_the_stores_is_refused(run):
    with pytest.raises(SystemExit, match="not the blob store's bucket"):
        run(Store(), "--bucket", "other")
