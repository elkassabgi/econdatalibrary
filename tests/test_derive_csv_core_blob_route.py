"""core/derive_csv.py and the two bulk tools that write through its PUT helper (derive_eia_tables,
derive_usda_bulk) use the CSV store (plan step 1 batch 4): R2 before T0, the self-hosted blob store after it,
never a raw client. Run END TO END on fakes, with the backend variable DELETED and no cutover flag (the
documented command), and every path the tools write pointed at tmp_path."""
import datetime as dt

import importlib
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import derive_csv as dc  # noqa: E402
from updater import blob  # noqa: E402

UTC = dt.timezone.utc


class Store:
    """A recording CSV store: what R2Blob / SelfhostBlob are to these tools."""
    bucket = "econ-data"

    def __init__(self, held=None):
        self.held = dict(held or {})               # key -> last-modified (UTC datetime)
        self.put = {}

    def list_keys(self, prefix):
        return sorted(k for k in self.held if k.startswith(prefix))

    def list_modified(self, prefix):
        return sorted((k, t) for k, t in self.held.items() if k.startswith(prefix))

    def put_atomic(self, key, data, plain=False):
        assert not plain, "these tools always gzipped: the default route"
        self.put[key] = data


@pytest.fixture
def pre_t0(tmp_path, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    return tmp_path


def _route(monkeypatch, store, seen=None):
    def make(*a, **k):
        if seen is not None:
            seen.append(k)
        return store
    monkeypatch.setattr(blob, "R2Blob", make)


BODY = b"series_id,obs_date,value\nk,2024-01-01,1\n"


@pytest.fixture
def core_run(pre_t0, monkeypatch):
    monkeypatch.setattr(dc, "_catalog_ids", lambda limit, source: [("zz:a", "zz"), ("zz:b", "zz")])
    monkeypatch.setattr(dc, "_series_csv_bytes", lambda sid: BODY)
    monkeypatch.setattr(dc, "_mirror_behind_store", lambda sources, sample=0: [])

    def go(store, *extra, seen=None):
        _route(monkeypatch, store, seen)
        monkeypatch.setattr(sys, "argv", ["derive_csv.py", "--bucket", "econ-data", "--source", "zz", *extra])
        return dc.main()
    return go


def test_every_series_reaches_the_csv_store(core_run):
    store = Store()
    core_run(store)
    assert sorted(store.put) == ["series/zz%3Aa.csv", "series/zz%3Ab.csv"]
    assert store.put["series/zz%3Aa.csv"] == BODY, "the plain CSV: put_atomic gzips it (series_csv_put_args)"


def test_a_wide_worker_pool_reaches_the_store_client(core_run):
    seen = []
    core_run(Store(), "--workers", "16", seen=seen)
    assert seen == [{"pool": 20}], seen
    seen.clear()
    core_run(Store(), seen=seen)
    assert seen == [{}], "the default worker count keeps botocore's default pool"


def test_skip_existing_lists_the_store(core_run):
    store = Store(held={"series/zz%3Aa.csv": dt.datetime(2026, 1, 1, tzinfo=UTC)})
    core_run(store, "--skip-existing")
    assert list(store.put) == ["series/zz%3Ab.csv"]


def test_skip_newer_than_reads_the_store_s_times(core_run):
    cutoff = "2026-09-01T00:00:00Z"
    store = Store(held={"series/zz%3Aa.csv": dt.datetime(2026, 9, 2, tzinfo=UTC),      # done this campaign
                        "series/zz%3Ab.csv": dt.datetime(2026, 8, 1, tzinfo=UTC)})     # still old
    core_run(store, "--skip-newer-than", cutoff)
    assert list(store.put) == ["series/zz%3Ab.csv"]


def test_a_failed_put_is_counted_not_hidden(core_run, capsys, monkeypatch):
    class Refusing(Store):
        def put_atomic(self, key, data, plain=False):
            raise OSError("refused")
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: None)          # the helper's 7 tries, without the waits
    with pytest.raises(SystemExit, match="could not be written"):
        core_run(Refusing())
    out = capsys.readouterr().out
    # R1211: an upload failure is its own count and fails the run - not a "store-coverage gap"
    assert "put 0 series CSVs" in out and "0 unresolvable" in out and "2 PUT FAILED" in out, out


def test_a_prefix_other_than_series_is_refused(core_run):
    with pytest.raises(SystemExit):
        core_run(Store(), "--prefix", "other")


def test_put_with_backoff_refuses_a_key_it_would_store_plain():
    """It always gzipped; put_atomic gzips only series/*.csv - any other key is refused, not stored plain."""
    with pytest.raises(ValueError, match="series CSVs"):
        dc._put_with_backoff(Store(), "other/x.csv", BODY)


def test_the_mirror_check_stops_at_t0(tmp_path, monkeypatch, capsys):
    """After T0 the local store IS the published store; the check says so and asks R2 nothing."""
    from core import cutover, r2_util
    flag = tmp_path / "CUTOVER"
    flag.write_text("t0")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    assert dc._mirror_behind_store(["zz"]) == []
    assert "mirror check not run" in capsys.readouterr().out


# ---- derive_eia_tables / derive_usda_bulk ------------------------------------------------------------------------
def _bulk(name, tmp_path, monkeypatch):
    m = importlib.import_module(f"tools.{name}")
    monkeypatch.setattr(m, "ROOT", str(tmp_path))                  # its DuckDB spill folder lives under ROOT
    return m


def test_eia_tables_write_through_the_csv_store(pre_t0, monkeypatch):
    m = _bulk("derive_eia_tables", pre_t0, monkeypatch)
    monkeypatch.setattr(m, "_dataset_files", lambda: [("AEO.2025", "unused.parquet", 1)])
    monkeypatch.setattr(m, "_catalogued", lambda: {"eia:AEO", "eia:COAL"})
    monkeypatch.setattr(m, "_stream", lambda q, path, depth: iter([
        ("eia:AEO", [("AEO.x", "2024-01-01", 1.0)]), ("eia:COAL", [("COAL.y", "2024-01-01", 2.0)])]))
    store = Store(held={"series/eia%3ACOAL.csv": dt.datetime(2026, 1, 1, tzinfo=UTC)})
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--verify", "0"])
    assert m.main() == 0
    assert list(store.put) == ["series/eia%3AAEO.csv"], "the held table is resumed, the other written"
    body = store.put["series/eia%3AAEO.csv"]
    assert body[:2] != b"\x1f\x8b" and body.startswith(b"series_id,obs_date,value"), \
        "the plain CSV reaches put_atomic, which gzips it (series_csv_put_args)"


def test_usda_bulk_writes_through_the_csv_store(pre_t0, monkeypatch):
    m = _bulk("derive_usda_bulk", pre_t0, monkeypatch)
    monkeypatch.setattr(m, "_files", lambda: ["unused.parquet"])
    monkeypatch.setattr(m, "_catalogued", lambda: {"usda:CENSUS|STATE|HOGS"})
    monkeypatch.setattr(m, "_stream", lambda q, files: iter([
        ("usda:CENSUS|STATE|HOGS", [("k", "2024-01-01", 1.0)]), ("usda:NOT|IN|CAT", [("k", "2024-01-01", 1.0)])]))
    store = Store()
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--verify", "0"])
    assert m.main() == 0
    assert list(store.put) == ["series/usda%3ACENSUS%7CSTATE%7CHOGS.csv"]


@pytest.mark.parametrize("name", ["derive_eia_tables", "derive_usda_bulk"])
def test_the_bulk_tools_take_their_store_from_csv_store(pre_t0, monkeypatch, name):
    """R1211: building R2Blob() directly survived the route tests (which fake R2Blob itself). csv_store is the
    only door: it is recorded, and an R2Blob built any other way fails the test."""
    m = _bulk(name, pre_t0, monkeypatch)
    if name == "derive_eia_tables":
        monkeypatch.setattr(m, "_dataset_files", lambda: [("AEO.2025", "unused.parquet", 1)])
        monkeypatch.setattr(m, "_catalogued", lambda: {"eia:AEO"})
        monkeypatch.setattr(m, "_stream", lambda q, path, depth: iter([("eia:AEO", [("AEO.x", "2024-01-01", 1.0)])]))
    else:
        monkeypatch.setattr(m, "_files", lambda: ["unused.parquet"])
        monkeypatch.setattr(m, "_catalogued", lambda: {"usda:A|B|C"})
        monkeypatch.setattr(m, "_stream", lambda q, files: iter([("usda:A|B|C", [("k", "2024-01-01", 1.0)])]))
    store, calls = Store(), []
    monkeypatch.setattr(blob, "csv_store", lambda bucket=None, **k: calls.append(bucket) or store)
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: pytest.fail("an R2Blob built outside csv_store"))
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--verify", "0"])
    assert m.main() == 0
    assert calls == ["econ-data"] and len(store.put) == 1
