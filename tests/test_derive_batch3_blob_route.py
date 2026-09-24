"""CSV writers batch 3 (plan step 1): derive_unsdg_flows, derive_noaa_missing, flowgrain_insee_melodi,
flowgrain_ons_uk and _derive_bea_bulk write through the CSV store (R2 before T0, the self-hosted blob store
after it), and the two flow-grain tools READ their parquets through updater.blob.store_reader (R2 before
T0, the LOCAL store after it - the self-hosted blob store holds only series/ CSVs, so reading parquets
from it would list nothing and publish zero tables).

Run END TO END on tiny stores with the backend variable DELETED and no cutover flag (the documented
command), a recording store in place of R2, and every path the tools write or read pointed at tmp_path."""
import datetime as dt
import gzip
import importlib
import io
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
from updater import blob, config  # noqa: E402


class Store:
    """A recording object store: what R2Blob / SelfhostBlob are to these tools."""
    bucket = "econ-data"
    root = "<recording store>"
    store = "<opened>"                  # what csv_store touches to open a self-hosted store eagerly

    def __init__(self, objs=None):
        self.objs, self.put = dict(objs or {}), {}

    def list_keys(self, prefix):
        return sorted(k for k in self.objs if k.startswith(prefix))

    def get(self, key):
        return self.objs.get(key)

    def exists(self, key):
        return key in self.objs

    def put_atomic(self, key, data, plain=False):
        self.__dict__.setdefault("plain", {})[key] = plain       # the encoding the tool asked for (R1206)
        self.put[key] = data


def _parquet_bytes(keys, dates=None, values=None):
    buf = io.BytesIO()
    n = len(keys)
    pq.write_table(pa.table({"series_key": keys,
                             "obs_date": pa.array(dates or [dt.date(2020, 1, 1)] * n, pa.date32()),
                             "value": values or [float(i + 1) for i in range(n)]}), buf)
    return buf.getvalue()


def _text(body):
    return (gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body).decode()


def _catalog(path, series=(), sources=()):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT)")
    c.executemany("INSERT INTO series VALUES (?,?)", series)
    c.executemany("INSERT INTO source VALUES (?,?)", sources)
    c.commit()
    c.close()


@pytest.fixture
def pre_t0(tmp_path, monkeypatch):
    """The documented command: no AQUEDUCT_BACKEND, no cutover flag."""
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


# ---- derive_unsdg_flows -------------------------------------------------------------------------------------
def test_unsdg_writes_every_code_to_the_csv_store(pre_t0, monkeypatch):
    m = importlib.import_module("derive_unsdg_flows")
    main = pre_t0 / "main"
    (main / "data" / "clean_full" / "unsdg").mkdir(parents=True)
    (main / "data" / "clean_full" / "unsdg" / "unsdg.parquet").write_bytes(
        _parquet_bytes(["AG_A:AFG", "AG_A:ALB", "AG_B:AFG"]))
    _catalog(str(main / "data" / "catalog.db"), [("unsdg:AG_A", "unsdg"), ("unsdg:AG_B", "unsdg")])
    monkeypatch.setattr(m, "MAIN", str(main))
    monkeypatch.setattr(m, "STORE", str(main / "data" / "clean_full" / "unsdg" / "unsdg.parquet"))
    store = Store()
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--threads", "1"])
    assert m.main() == 0
    assert sorted(store.put) == ["series/unsdg%3AAG_A.csv", "series/unsdg%3AAG_B.csv"]
    assert _text(store.put["series/unsdg%3AAG_A.csv"]).count("\n") == 3
    assert set(store.plain.values()) == {True}, "stored PLAIN, as this tool always did (R1206)"


def test_unsdg_a_failed_put_fails_the_run(pre_t0, monkeypatch):
    m = importlib.import_module("derive_unsdg_flows")
    main = pre_t0 / "main"
    (main / "data" / "clean_full" / "unsdg").mkdir(parents=True)
    (main / "data" / "clean_full" / "unsdg" / "unsdg.parquet").write_bytes(_parquet_bytes(["AG_A:AFG"]))
    _catalog(str(main / "data" / "catalog.db"), [("unsdg:AG_A", "unsdg")])
    monkeypatch.setattr(m, "MAIN", str(main))
    monkeypatch.setattr(m, "STORE", str(main / "data" / "clean_full" / "unsdg" / "unsdg.parquet"))
    _route(monkeypatch, Store())
    from updater import derive
    monkeypatch.setattr(derive, "_put_with_retry", lambda *a, **k: False)
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "econ-data", "--threads", "1"])
    with pytest.raises(RuntimeError, match="gave up"):
        m.main()


# ---- derive_noaa_missing ------------------------------------------------------------------------------------
def test_noaa_missing_writes_only_what_the_csv_store_lacks(pre_t0, monkeypatch):
    m = importlib.import_module("derive_noaa_missing")
    store_dir = pre_t0 / "clean_full" / "noaa"
    store_dir.mkdir(parents=True)
    keys = ["gsom:AYM001:DSND", "gsom:AYM002:DSND", "gsom:AYM003:DSND"]
    pq.write_table(pa.table({"series_key": keys}), store_dir / "gsom__AY__series.parquet")
    (store_dir / "gsom__AY.parquet").write_bytes(_parquet_bytes(keys))
    cat = pre_t0 / "catalog.db"
    _catalog(str(cat), [("noaa:gsom:AYM001:DSND", "noaa")])
    monkeypatch.setattr(m, "STORE", str(store_dir))
    monkeypatch.setattr(m, "CAT", str(cat))
    have = m._r2_key("gsom:AYM002:DSND")
    store = Store({have: b"x"})
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--apply", "--workers", "1"])
    assert m.main() == 0
    assert list(store.put) == [m._r2_key("gsom:AYM003:DSND")], "catalogued and already-held keys are skipped"
    assert _text(store.put[m._r2_key("gsom:AYM003:DSND")]).splitlines()[1].startswith("gsom:AYM003:DSND,")
    assert set(store.plain.values()) == {True}, "stored PLAIN, as this tool always did (R1206)"


def test_noaa_missing_a_dry_run_writes_nothing(pre_t0, monkeypatch):
    m = importlib.import_module("derive_noaa_missing")
    store_dir = pre_t0 / "clean_full" / "noaa"
    store_dir.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["gsom:AYM003:DSND"]}), store_dir / "gsom__AY__series.parquet")
    (store_dir / "gsom__AY.parquet").write_bytes(_parquet_bytes(["gsom:AYM003:DSND"]))
    _catalog(str(pre_t0 / "catalog.db"))
    monkeypatch.setattr(m, "STORE", str(store_dir))
    monkeypatch.setattr(m, "CAT", str(pre_t0 / "catalog.db"))
    store = Store()
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x"])
    assert m.main() == 0 and store.put == {}


# ---- flowgrain_insee_melodi / flowgrain_ons_uk -----------------------------------------------------------------
FLOWGRAIN = {
    "flowgrain_insee_melodi": ("insee_melodi", "flow_titles", ["--threads", "1"]),
    "flowgrain_ons_uk": ("ons_uk", "ons_titles", ["--threads", "1"]),
}


def _flowgrain(name, tmp, monkeypatch):
    m = importlib.import_module(name)
    src, titles_fn, extra = FLOWGRAIN[name]
    cat = tmp / f"{src}_catalog.db"
    _catalog(str(cat), sources=[(src, "lic-1")])
    monkeypatch.setattr(m, "CATALOG", str(cat))
    monkeypatch.setattr(m, titles_fn, lambda: {})
    return m, src, extra


@pytest.mark.parametrize("name", sorted(FLOWGRAIN))
def test_flowgrain_before_t0_reads_and_writes_r2(pre_t0, monkeypatch, name):
    m, src, extra = _flowgrain(name, pre_t0, monkeypatch)
    store = Store({f"clean_full/{src}/DS_A.parquet": _parquet_bytes(["A=1", "A=2"]),
                   f"clean_full/{src}/DS_B.parquet": _parquet_bytes(["B=1"])})
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--upload", *extra])
    m.main()
    assert sorted(store.put) == [f"series/{src}%3ADS_A.csv", f"series/{src}%3ADS_B.csv"]
    assert _text(store.put[f"series/{src}%3ADS_A.csv"]).startswith("series_id,obs_date,value\n")
    assert set(store.plain.values()) == {True}, "stored PLAIN, as this tool always did (R1206)"


@pytest.mark.parametrize("name", sorted(FLOWGRAIN))
def test_flowgrain_after_t0_reads_the_local_store_and_writes_the_blob_store(tmp_path, monkeypatch, name):
    """The plan's T0: parquets are local, the self-hosted blob store holds only series/ CSVs."""
    m, src, extra = _flowgrain(name, tmp_path, monkeypatch)
    from core import cutover
    flag = tmp_path / "flag" / "CUTOVER"
    flag.parent.mkdir()
    flag.write_text("t0")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    assert cutover.is_cut_over(), "precondition: the flag reads as set"
    data_root = tmp_path / "clean_full"
    (data_root / src).mkdir(parents=True)
    (data_root / src / "DS_L.parquet").write_bytes(_parquet_bytes(["L=1"]))
    monkeypatch.setattr(config, "DATA_ROOT", str(data_root))
    selfhost = Store()                                     # holds NO parquets, like the real blob store
    monkeypatch.setattr(blob, "SelfhostBlob", lambda *a, **k: selfhost)
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: pytest.fail("R2 touched after T0"))
    monkeypatch.setattr(sys, "argv", ["x", "--upload", *extra])
    m.main()
    assert list(selfhost.put) == [f"series/{src}%3ADS_L.csv"]
    assert set(selfhost.plain.values()) == {True}, "stored PLAIN, as this tool always did (R1206)"


@pytest.mark.parametrize("name", sorted(FLOWGRAIN))
def test_flowgrain_an_empty_store_is_refused(pre_t0, monkeypatch, name):
    """Before, 0 tables with --catalog deleted every row of the source and wrote none."""
    m, src, extra = _flowgrain(name, pre_t0, monkeypatch)
    _route(monkeypatch, Store())
    monkeypatch.setattr(sys, "argv", ["x", "--catalog", *extra])
    with pytest.raises(SystemExit, match="empty source"):
        m.main()


# ---- _derive_bea_bulk ---------------------------------------------------------------------------------------
def test_bea_writes_through_the_csv_store_with_a_wide_pool(pre_t0, monkeypatch):
    m = importlib.import_module("_derive_bea_bulk")
    store_dir = pre_t0 / "clean_full" / "bea" / "Regional"
    store_dir.mkdir(parents=True)
    rows = {"712:1": [(dt.date(2020, 1, 1), 1.5), (dt.date(2021, 1, 1), 2.5)], "712:2": [(dt.date(2020, 1, 1), 3.0)]}
    keys = [k for k, rr in rows.items() for _ in rr]
    pq.write_table(pa.table({"series_key": keys,
                             "obs_date": pa.array([d for rr in rows.values() for d, _ in rr], pa.date32()),
                             "value": [v for rr in rows.values() for _, v in rr]}), store_dir / "T1.parquet")
    (pre_t0 / "data").mkdir()
    _catalog(str(pre_t0 / "data" / "catalog.db"), [(f"bea:{k}", "bea") for k in rows])
    monkeypatch.setattr(m, "STORE", str(pre_t0 / "clean_full" / "bea"))
    monkeypatch.setattr(m, "ROOT", str(pre_t0))
    monkeypatch.setattr(m, "MUST_VERIFY", [])
    from core import derive_csv
    monkeypatch.setattr(derive_csv, "_series_csv_bytes",
                        lambda sid: m._csv_bytes(sid.split(":", 1)[1], rows[sid.split(":", 1)[1]]))
    held = m.csv_key(m.PREFIX, m.SRC, "712:2")
    store, seen = Store({held: b"x"}), []
    _route(monkeypatch, store, seen)
    assert m.main() == 0
    assert seen == [{"pool": 96}], "the store's R2 client keeps the pool wider than the 64 workers"
    assert list(store.put) == [m.csv_key(m.PREFIX, m.SRC, "712:1")], "the held key is skipped"
    assert _text(store.put[m.csv_key(m.PREFIX, m.SRC, "712:1")]).count("\n") == 3
    assert set(store.plain.values()) == {True}, "stored PLAIN, as this tool always did (R1206)"


# ---- the pieces -------------------------------------------------------------------------------------------
def test_the_pool_reaches_the_r2_client_only_when_asked(monkeypatch):
    from core import r2_util
    calls = []
    monkeypatch.setattr(r2_util, "client", lambda write=False, **kw: calls.append(kw) or object())
    blob.R2Blob(pool=96).client
    blob.R2Blob().client
    assert calls == [{"pool": 96}, {}], "no pool -> the old call shape (tests fake client(write=...))"


def test_the_local_store_reader(tmp_path):
    root = tmp_path / "clean_full"
    (root / "bea" / "Regional").mkdir(parents=True)
    (root / "bea" / "Regional" / "T1.parquet").write_bytes(b"1")
    (root / "bea" / "top.parquet").write_bytes(b"2")
    (root / "bead").mkdir()
    (root / "bead" / "x.parquet").write_bytes(b"3")
    r = blob.LocalStoreReader(str(root))
    assert r.list_keys("clean_full/bea/") == ["clean_full/bea/Regional/T1.parquet", "clean_full/bea/top.parquet"]
    assert r.list_keys("clean_full/bea/Reg") == ["clean_full/bea/Regional/T1.parquet"]
    assert r.list_keys("clean_full/none/") == []
    assert r.get("clean_full/bea/top.parquet") == b"2" and r.get("clean_full/bea/gone.parquet") is None
    for bad in ("series/x.csv", "clean_full/../secrets", "clean_full/bea/../../x"):
        with pytest.raises(ValueError):
            r.get(bad)
    with pytest.raises(ValueError):
        r.list_keys("clean_full/../")


def test_store_reader_selection(tmp_path, monkeypatch):
    from core import cutover
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path / "clean_full"))
    sentinel = Store()
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: sentinel)
    assert blob.store_reader() is sentinel, "before T0: R2, where these tools have always read"
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    assert isinstance(blob.store_reader(), blob.LocalStoreReader)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    flag = tmp_path / "flag" / "CUTOVER"
    flag.parent.mkdir()
    flag.write_text("t0")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    assert isinstance(blob.store_reader(), blob.LocalStoreReader), "after T0 the flag wins over the variable"


# ---- R1206: a store that refuses, a parquet that vanishes, an error that is not "absent" ----------------------
def _never(*a, **k):
    return False


def test_noaa_missing_a_failed_upload_fails_the_run(pre_t0, monkeypatch):
    m = importlib.import_module("derive_noaa_missing")
    store_dir = pre_t0 / "clean_full" / "noaa"
    store_dir.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["gsom:AYM003:DSND"]}), store_dir / "gsom__AY__series.parquet")
    (store_dir / "gsom__AY.parquet").write_bytes(_parquet_bytes(["gsom:AYM003:DSND"]))
    _catalog(str(pre_t0 / "catalog.db"))
    monkeypatch.setattr(m, "STORE", str(store_dir))
    monkeypatch.setattr(m, "CAT", str(pre_t0 / "catalog.db"))
    _route(monkeypatch, Store())
    monkeypatch.setattr(m._derive, "_put_with_retry", _never)
    monkeypatch.setattr(sys, "argv", ["x", "--apply", "--workers", "1"])
    with pytest.raises(SystemExit, match="gave up"):
        m.main()


def test_noaa_missing_an_error_is_not_read_as_absent(pre_t0, monkeypatch):
    """R1206: 'any error means absent' re-wrote objects; now a non-404 error stops the run before any write."""
    m = importlib.import_module("derive_noaa_missing")
    store_dir = pre_t0 / "clean_full" / "noaa"
    store_dir.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["gsom:AYM003:DSND"]}), store_dir / "gsom__AY__series.parquet")
    (store_dir / "gsom__AY.parquet").write_bytes(_parquet_bytes(["gsom:AYM003:DSND"]))
    _catalog(str(pre_t0 / "catalog.db"))
    monkeypatch.setattr(m, "STORE", str(store_dir))
    monkeypatch.setattr(m, "CAT", str(pre_t0 / "catalog.db"))

    class Flaky(Store):
        def exists(self, key):
            raise OSError("503 SlowDown")
    store = Flaky()
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--apply", "--workers", "1"])
    with pytest.raises(OSError, match="SlowDown"):
        m.main()
    assert store.put == {}


@pytest.mark.parametrize("name", sorted(FLOWGRAIN))
def test_flowgrain_a_failed_upload_fails_the_run(pre_t0, monkeypatch, name):
    m, src, extra = _flowgrain(name, pre_t0, monkeypatch)
    _route(monkeypatch, Store({f"clean_full/{src}/DS_A.parquet": _parquet_bytes(["A=1"])}))
    from updater import derive
    monkeypatch.setattr(derive, "_put_with_retry", _never)
    monkeypatch.setattr(sys, "argv", ["x", "--upload", *extra])
    with pytest.raises(SystemExit, match="gave up"):
        m.main()


@pytest.mark.parametrize("name", sorted(FLOWGRAIN))
def test_flowgrain_a_listed_parquet_that_vanished_is_refused(pre_t0, monkeypatch, name):
    """R1206: turning a listed-but-gone parquet into an empty CSV survived; a partial source is refused."""
    m, src, extra = _flowgrain(name, pre_t0, monkeypatch)

    class Vanishing(Store):
        def get(self, key):
            return None
    store = Vanishing({f"clean_full/{src}/DS_A.parquet": _parquet_bytes(["A=1"])})
    _route(monkeypatch, store)
    monkeypatch.setattr(sys, "argv", ["x", "--upload", *extra])
    with pytest.raises(SystemExit, match="listed but is gone"):
        m.main()
    assert store.put == {}


def test_bea_a_failed_upload_fails_the_run(pre_t0, monkeypatch):
    m = importlib.import_module("_derive_bea_bulk")
    store_dir = pre_t0 / "clean_full" / "bea" / "Regional"
    store_dir.mkdir(parents=True)
    rows = {"712:1": [(dt.date(2020, 1, 1), 1.5)]}
    pq.write_table(pa.table({"series_key": ["712:1"], "obs_date": pa.array([dt.date(2020, 1, 1)], pa.date32()),
                             "value": [1.5]}), store_dir / "T1.parquet")
    (pre_t0 / "data").mkdir()
    _catalog(str(pre_t0 / "data" / "catalog.db"), [("bea:712:1", "bea")])
    monkeypatch.setattr(m, "STORE", str(pre_t0 / "clean_full" / "bea"))
    monkeypatch.setattr(m, "ROOT", str(pre_t0))
    monkeypatch.setattr(m, "MUST_VERIFY", [])
    from core import derive_csv
    monkeypatch.setattr(derive_csv, "_series_csv_bytes",
                        lambda sid: m._csv_bytes(sid.split(":", 1)[1], rows[sid.split(":", 1)[1]]))
    _route(monkeypatch, Store())
    monkeypatch.setattr(m._derive, "_put_with_retry", _never)
    assert m.main() == 1, "a failed upload must not exit 0 (R1206: counted as a success before)"


def test_the_pool_reaches_botocore():
    """R1206: dropping the pool inside r2_util survived - the test above stops at r2_util.client."""
    pytest.importorskip("boto3")
    from core import r2_util
    creds = {"endpoint": "https://example.invalid", "key": "k", "secret": "s"}
    assert r2_util._boto3_client(creds, 96).meta.config.max_pool_connections == 96
    assert r2_util._boto3_client(creds).meta.config.max_pool_connections == 10


def test_the_local_store_reader_stays_inside_the_store(tmp_path):
    root = tmp_path / "clean_full"
    (root / "ons_uk").mkdir(parents=True)
    (tmp_path / "secret.txt").write_text("x")
    r = blob.LocalStoreReader(str(root))
    for bad in ("clean_full/ons_uk/..\\..\\secret.txt", "clean_full/C:/secret.txt", "clean_full/ons_uk\\x",
                "clean_full/ons_uk/\x00x"):
        with pytest.raises(ValueError):
            r.get(bad)
    for bad in ("clean_full/ons_uk\\", "clean_full/C:"):
        with pytest.raises(ValueError):
            r.list_keys(bad)


@pytest.mark.skipif(os.name != "nt", reason="an NTFS junction")
def test_a_junction_out_of_the_store_is_refused(tmp_path):
    """R1213: the realpath containment had no test - a key whose folder is a junction to outside the store."""
    import subprocess
    root = tmp_path / "clean_full"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.parquet").write_bytes(b"x")
    r = subprocess.run(["cmd", "/c", "mklink", "/J", str(root / "ons_uk"), str(outside)], capture_output=True, text=True)
    if r.returncode != 0:
        pytest.skip(f"could not make a junction: {r.stderr.strip()[:80]}")
    reader = blob.LocalStoreReader(str(root))
    with pytest.raises(ValueError, match="outside the store"):
        reader.get("clean_full/ons_uk/secret.parquet")


def test_store_reader_reads_r2_with_the_read_key(tmp_path, monkeypatch):
    """R1206: before T0 the reader used the WRITE key, so a --dry-run needed write credentials."""
    from core import cutover
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    seen = []
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: seen.append(k) or Store())
    blob.store_reader()
    assert seen == [{"write": False}]
    from core import r2_util
    calls = []
    monkeypatch.undo()
    monkeypatch.setattr(r2_util, "client", lambda write=False, **kw: calls.append(write) or object())
    blob.R2Blob(write=False).client
    assert calls == [False]
