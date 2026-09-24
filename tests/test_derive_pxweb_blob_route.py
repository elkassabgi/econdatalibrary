"""tools/derive_pxweb_flowgrain.py writes through the CSV store (plan step 1): R2 before T0, the self-hosted
blob store after it, never local files. END TO END on a one-table store, backend variable DELETED."""
import datetime as dt
import gzip
import importlib.util
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from updater import blob  # noqa: E402

TOOL = os.path.join(ROOT, "tools", "derive_pxweb_flowgrain.py")


class Store:
    bucket = "econ-data"

    def __init__(self, have=()):
        self.put, self.have = {}, set(have)

    def list_keys(self, prefix):
        return [k for k in self.have if k.startswith(prefix)]

    def put_atomic(self, key, data, plain=False):
        self.__dict__.setdefault("plain", {})[key] = plain       # the encoding the tool asked for (R1206)
        self.put[key] = data


@pytest.fixture
def tool(tmp_path, monkeypatch):
    real_isdir = os.path.isdir
    # the module refuses at IMPORT when this checkout has no data/clean_full (a test checkout has none)
    monkeypatch.setattr(os.path, "isdir", lambda p: True)
    spec = importlib.util.spec_from_file_location("derive_pxweb_flowgrain_undertest", TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    monkeypatch.setattr(os.path, "isdir", real_isdir)
    d = tmp_path / "clean_full" / "ssb"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["T1:Region=00:Contents=X", "T1:Region=01:Contents=X"],
                             "obs_date": pa.array([dt.date(2020, 1, 1), dt.date(2020, 1, 1)], pa.date32()),
                             "value": [1.0, 2.0]}), d / "S1.parquet")
    monkeypatch.setattr(m, "DATA", str(tmp_path / "clean_full"))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))

    def go(store, *extra):
        monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)
        monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--source", "ssb", "--threads", "1", *extra])
        return m.main()
    go.module = m                                           # the tool under test, for a test to patch
    return go


def test_one_table_reaches_the_csv_store(tool):
    store = Store()
    tool(store)
    assert list(store.put) == ["series/ssb%3AT1.csv"], sorted(store.put)
    body = store.put["series/ssb%3AT1.csv"]
    text = (gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body).decode()
    assert text.count("\n") == 3 and "T1:Region=01:Contents=X" in text
    assert store.plain == {"series/ssb%3AT1.csv": True} and body[:2] != b"\x1f\x8b", \
        "stored PLAIN, as this tool always did (R1206)"


def test_a_store_that_refuses_fails_the_run(tool, monkeypatch):
    """R1204: pxweb swallowing its last failure survived - no test gave it a store that refuses."""
    import types

    class Refusing(Store):
        def put_atomic(self, key, data, plain=False):
            raise OSError("the store refused")
    import time as _time
    tries = []
    m_time = types.SimpleNamespace(sleep=lambda s: tries.append(s), time=_time.time, perf_counter=_time.perf_counter,
                                   monotonic=_time.monotonic)
    mod = tool.module
    monkeypatch.setattr(mod, "time", m_time)
    with pytest.raises(OSError, match="refused"):
        tool(Refusing())
    assert len(tries) == 6, f"its own 7 tries: 6 waits, got {tries}"


@pytest.mark.parametrize("kind", ["ValueError", "CutoverRefused"])
def test_a_refusal_is_not_retried(tool, monkeypatch, kind):
    """R1222: this loop caught every exception, so a refusal cost 7 tries and 63 s per key."""
    import types
    from core import cutover
    calls = []

    class Refusing(Store):
        def put_atomic(self, key, data, plain=False):
            calls.append(key)
            raise ValueError("not gzip") if kind == "ValueError" else cutover.CutoverRefused("another writer")
    import time as _time
    monkeypatch.setattr(tool.module, "time", types.SimpleNamespace(
        sleep=lambda s: pytest.fail("a refusal is not retried"), time=_time.time,
        perf_counter=_time.perf_counter, monotonic=_time.monotonic))
    with pytest.raises((ValueError, cutover.CutoverRefused)):
        tool(Refusing())
    assert len(calls) == 1


def test_skip_existing_lists_the_csv_store(tool):
    store = Store(have={"series/ssb%3AT1.csv"})
    tool(store, "--skip-existing")
    assert store.put == {}


def test_a_wrong_bucket_is_refused(tool):
    with pytest.raises(SystemExit, match="not the CSV store's bucket"):
        tool(Store(), "--bucket", "other")
