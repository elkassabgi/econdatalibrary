"""core/licence_targets.py + tools/retire_source.py on it (plan change 5: licence tools get their local backend
first). After T0 a retirement must act on the self-hosted places only and leave D1 alone; before T0 it must
make exactly today's calls. Every real path is replaced by a temporary one."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, d1_remote, licence_targets as lt
from updater import blob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
sys.path.insert(0, os.path.join(ROOT, "tools"))
from blobstore import BlobStore  # noqa: E402
import retire_source  # noqa: E402

CSV = b"series_id,obs_date,value\nfoo:a,2020-01-01,1\n"


def _catalogue(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY)")
        c.executemany("INSERT INTO series VALUES (?, ?)", [("foo:a", "foo"), ("foo:b", "foo"),
                                                           ("foo_direct:a", "foo_direct")])
        c.executemany("INSERT INTO source VALUES (?)", [("foo",), ("foo_direct",)])
        for table in ("source_counts", "unit_state", "source_state", "source_data_through"):
            c.execute(f"CREATE TABLE {table} (source_id TEXT, v INTEGER)")
            c.executemany(f"INSERT INTO {table} VALUES (?, 1)", [("foo",), ("foo_direct",)])


def _extras(path):
    with sqlite3.connect(path) as c:
        return {t: sorted(r[0] for r in c.execute(f"SELECT source_id FROM {t}"))
                for t in ("source_counts", "unit_state", "source_state", "source_data_through")}


def _rows(path):
    with sqlite3.connect(path) as c:
        return (sorted(r[0] for r in c.execute("SELECT series_id FROM series")),
                sorted(r[0] for r in c.execute("SELECT source_id FROM source")))


@pytest.fixture
def live(tmp_path, monkeypatch):
    """A cut-over machine: flag, blob store, store files, catalogue build and lock all in tmp_path."""
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    store_root = tmp_path / "store"
    for rel in ("clean_full/foo/x.parquet", "clean_full/foo/sub/y.parquet", "clean_full/foo_direct/z.parquet"):
        p = store_root / "data" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PAR1" + rel.encode())
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(store_root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "live" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "live" / "writer.lock"))
    _catalogue(tmp_path / "live" / "catalog.db")
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    sb = blob.SelfhostBlob()
    for k in ("series/foo%3Aa.csv", "series/foo%3Ab.csv", "series/foo_direct%3Aa.csv"):
        sb.put_atomic(k, CSV)

    def no_d1(*a, **k):
        raise AssertionError("D1 must not be written after T0")
    monkeypatch.setattr(d1_remote, "execute_wrangler", no_d1)
    return tmp_path, store_root, sb


def test_after_t0_a_retirement_acts_only_on_the_self_hosted_places(live, capsys):
    tmp, store, sb = live
    assert retire_source.main(["foo", "--apply"]) == 0
    assert _rows(tmp / "live" / "catalog.db") == (["foo_direct:a"], ["foo_direct"])
    assert set(map(tuple, _extras(tmp / "live" / "catalog.db").values())) == {("foo_direct",)},         "after T0 the freshness tables go too (R709) - D1 no longer does it"
    assert sb.list_keys("series/") == ["series/foo_direct%3Aa.csv"], "the _direct neighbour survives"
    assert not (store / "data" / "clean_full" / "foo" / "x.parquet").exists()
    assert not (store / "data" / "clean_full" / "foo" / "sub" / "y.parquet").exists()
    assert (store / "data" / "clean_full" / "foo_direct" / "z.parquet").exists()
    assert (store / "data" / "archive" / "retired" / "foo" / "x.parquet").read_bytes() == b"PAR1clean_full/foo/x.parquet"
    out = capsys.readouterr().out
    assert "D1: skipped" in out and "edge denylist" in out
    assert catalog_path._held is None, "the writer lock is released"


def test_after_t0_a_dry_run_changes_nothing(live):
    tmp, store, sb = live
    assert retire_source.main(["foo"]) == 0
    assert _rows(tmp / "live" / "catalog.db")[0] == ["foo:a", "foo:b", "foo_direct:a"]
    assert len(sb.list_keys("series/")) == 3
    assert (store / "data" / "clean_full" / "foo" / "x.parquet").exists()


def test_after_t0_another_writer_blocks_the_retirement(live, monkeypatch):
    class Busy:
        def __enter__(self):
            raise cutover.CutoverRefused("refused: another process holds the catalogue writer lock")

        def __exit__(self, *a):
            return False
    monkeypatch.setattr(catalog_path, "writer_lock", lambda: Busy())
    with pytest.raises(cutover.CutoverRefused):
        retire_source.main(["foo", "--apply"])


def test_a_successor_is_never_a_target():
    assert retire_source.main(["foo_direct", "--apply"]) == 1


def test_keys_outside_the_terminated_prefixes_are_refused(live):
    t = lt.Targets()
    with pytest.raises(ValueError, match="outside"):
        t.delete(["series/foo_direct%3Aa.csv"], (lt.csv_prefix("foo"), lt.store_prefix("foo")))


# ---- before T0: exactly today's calls ------------------------------------------------------------------
class FakeS3:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.calls = []

    def list_objects_v2(self, Bucket, Prefix, MaxKeys, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k, "Size": len(self.objects[k])} for k in keys], "IsTruncated": False}

    def copy_object(self, **kw):
        self.calls.append(("copy", kw["CopySource"]["Key"], kw["Key"]))

    def delete_objects(self, Bucket, Delete):
        self.calls.append(("delete", sorted(o["Key"] for o in Delete["Objects"])))


def test_before_t0_the_calls_are_todays(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    _catalogue(tmp_path / "checkout" / "catalog.db")
    s3 = FakeS3({"series/foo%3Aa.csv": b"x", "series/foo_direct%3Aa.csv": b"x",
                 "clean_full/foo/x.parquet": b"p", "clean_full/foo/m.json": b"{}",
                 "clean_full/foo_direct/z.parquet": b"p"})
    from core import r2_util
    monkeypatch.setattr(r2_util, "client", lambda write=False: s3)
    d1 = []
    monkeypatch.setattr(d1_remote, "execute_wrangler", lambda db, stmts, **k: d1.append((db, stmts)) or True)
    assert retire_source.main(["foo", "--apply"]) == 0
    assert s3.calls == [("copy", "clean_full/foo/x.parquet", "archive/retired/foo/x.parquet"),
                        ("delete", ["series/foo%3Aa.csv"]),
                        ("delete", ["clean_full/foo/m.json", "clean_full/foo/x.parquet"])]
    assert d1[0][0] == "econ-catalog" and len(d1[0][1]) == 6
    assert all("source_id='foo'" in s for s in d1[0][1])
    assert _rows(tmp_path / "checkout" / "catalog.db") == (["foo_direct:a"], ["foo_direct"])
    assert set(map(tuple, _extras(tmp_path / "checkout" / "catalog.db").values())) == {("foo", "foo_direct")},         "before T0 only series + source are deleted locally, as always (D1 carries the rest)"


def test_before_t0_a_d1_failure_stops_before_the_purge(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    _catalogue(tmp_path / "checkout" / "catalog.db")
    s3 = FakeS3({"series/foo%3Aa.csv": b"x"})
    from core import r2_util
    monkeypatch.setattr(r2_util, "client", lambda write=False: s3)
    monkeypatch.setattr(d1_remote, "execute_wrangler", lambda db, stmts, **k: False)
    assert retire_source.main(["foo", "--apply"]) == 1
    assert not any(c[0] == "delete" for c in s3.calls), "nothing purged after a D1 failure"


def test_execute_wrangler_refuses_after_t0(tmp_path, monkeypatch):
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    with pytest.raises(cutover.CutoverRefused):
        d1_remote.execute_wrangler("econ-catalog", ["DELETE FROM series"])


# ---- tools/delist_source_rows.py on the same helper -----------------------------------------------------
import delist_source_rows  # noqa: E402


def test_after_t0_a_delisting_removes_rows_only(live, capsys):
    tmp, store, sb = live
    assert delist_source_rows.main(["foo", "--apply"]) == 0
    assert _rows(tmp / "live" / "catalog.db") == (["foo_direct:a"], ["foo_direct"])
    assert set(map(tuple, _extras(tmp / "live" / "catalog.db").values())) == {("foo_direct",)}
    assert len(sb.list_keys("series/")) == 3, "a delisting never touches stored objects"
    assert (store / "data" / "clean_full" / "foo" / "x.parquet").exists()
    assert "D1: skipped" in capsys.readouterr().out


def test_after_t0_the_csv_only_mode_touches_nothing_else(live):
    tmp, store, sb = live
    assert delist_source_rows.main(["foo", "--purge-csv-prefix", "a", "--apply"]) == 0
    assert sb.list_keys("series/") == ["series/foo%3Ab.csv", "series/foo_direct%3Aa.csv"]
    assert _rows(tmp / "live" / "catalog.db")[0] == ["foo:a", "foo:b", "foo_direct:a"], "catalogue untouched"


def test_before_t0_a_delisting_makes_todays_calls(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    _catalogue(tmp_path / "checkout" / "catalog.db")
    from core import r2_util
    monkeypatch.setattr(r2_util, "client", lambda write=False: pytest.fail("a delisting must not touch R2"))
    d1 = []
    monkeypatch.setattr(d1_remote, "execute_wrangler", lambda db, stmts, **k: d1.append((db, stmts)) or True)
    assert delist_source_rows.main(["foo", "--apply"]) == 0
    assert d1 == [("econ-catalog", lt.Targets.d1_statements("foo"))] and len(d1[0][1]) == 6
    assert _rows(tmp_path / "checkout" / "catalog.db") == (["foo_direct:a"], ["foo_direct"])
