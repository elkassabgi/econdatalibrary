"""The four IMF table derives (dip, imts, pip, mfs) write through the CSV store (plan step 1): R2 before T0,
the self-hosted blob store after it, never local files. Run END TO END on a small one-parquet store, with the
backend variable DELETED (the default route, review R1200) and a recording store in place of R2."""
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
from updater import blob, config  # noqa: E402


class Store:
    bucket = "econ-data"

    def __init__(self):
        self.put = {}

    def put_atomic(self, key, data, plain=False):
        self.__dict__.setdefault("plain", {})[key] = plain       # the encoding the tool asked for (R1206)
        self.put[key] = data


def _mfs_source():
    from tools.catalog_mfs_tables import FLOWS
    return sorted(FLOWS)[0]


# (module, argv before the common flags, source whose store is read, a key shape with enough parts)
CASES = [
    ("tools.derive_dip_tables", [], "imf_dip_direct", "C1.CP.DV.F.I"),
    ("tools.derive_imts_tables", [], "imf_imts_direct", "C1.X.F.I.P"),
    ("tools.derive_pip_tables", [], "imf_pip_direct", "A.B.C.D.E.F.G"),
]


def _cases():
    out = list(CASES)
    src = _mfs_source()
    out.append(("tools.derive_mfs_tables", [src], src, "PREFIX:A.B.C.D.E.F.G"))
    return out


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path / "clean_full" / s))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "x")
    monkeypatch.delenv("AQUEDUCT_BACKEND")
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_flag" / "CUTOVER"))
    return tmp_path


def _store_file(tmp, source, key):
    d = tmp / "clean_full" / source
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": [key, key],
                             "obs_date": pa.array([dt.date(2020, 1, 1), dt.date(2021, 1, 1)], pa.date32()),
                             "value": [1.5, 2.5]}), d / f"{source}.parquet")


@pytest.mark.parametrize("mod,pre,source,key", _cases(), ids=lambda v: v if isinstance(v, str) and v.startswith("tools.") else "")
def test_the_table_csvs_reach_the_csv_store(env, monkeypatch, mod, pre, source, key):
    _store_file(env, source, key)
    store = Store()
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: store)          # what csv_store() builds before T0
    monkeypatch.setattr(sys, "argv", ["x", *pre, "--bucket", "econ-data", "--threads", "1"])
    m = importlib.import_module(mod)
    rc = m.main()
    assert rc in (0, None), rc
    assert len(store.put) == 1, sorted(store.put)
    (k, body), = store.put.items()
    assert k.startswith("series/") and k.endswith(".csv")
    text = (gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body).decode()
    assert text.startswith("series_id,obs_date,value\n") and text.count("\n") == 3, text
    assert store.plain == {k: True} and body[:2] != b"\x1f\x8b", "stored PLAIN, as this tool always did (R1206)"


@pytest.mark.parametrize("mod,pre,source,key", _cases(), ids=lambda v: v if isinstance(v, str) and v.startswith("tools.") else "")
def test_a_failed_upload_fails_the_run(env, monkeypatch, mod, pre, source, key):
    """R1204: a tool that ignored _put_with_retry's False survived every test - the store never refused."""
    _store_file(env, source, key)
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: Store())
    from updater import derive
    monkeypatch.setattr(derive, "_put_with_retry", lambda *a, **k: False)
    monkeypatch.setattr(sys, "argv", ["x", *pre, "--bucket", "econ-data", "--threads", "1"])
    try:
        rc = importlib.import_module(mod).main()
    except SystemExit as e:
        rc = e.code
    assert rc not in (0, None), f"{mod}: a failed upload exited {rc!r}"


@pytest.mark.parametrize("mod,pre,source,key", _cases()[:1], ids=["dip"])
def test_a_wrong_bucket_is_refused(env, monkeypatch, mod, pre, source, key):
    _store_file(env, source, key)
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: Store())
    monkeypatch.setattr(sys, "argv", ["x", *pre, "--bucket", "other", "--threads", "1"])
    with pytest.raises(SystemExit, match="not the CSV store's bucket"):
        importlib.import_module(mod).main()
