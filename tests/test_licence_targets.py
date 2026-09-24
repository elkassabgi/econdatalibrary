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
        c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title)")
        c.executemany("INSERT INTO series_fts VALUES (?, 't')", [("foo:a",), ("foo:b",), ("foo_direct:a",)])
        for table in ("source_counts", "unit_state", "source_state", "source_data_through"):
            c.execute(f"CREATE TABLE {table} (source_id TEXT, v INTEGER)")
            c.executemany(f"INSERT INTO {table} VALUES (?, 1)", [("foo",), ("foo_direct",)])


def _ro(path):
    """A check reads the build READ-ONLY: after T0 core.catalog_path's runtime guard refuses a plain read-write
    open of the build without the lock (R1209)."""
    import pathlib
    return sqlite3.connect(pathlib.Path(path).resolve().as_uri() + "?mode=ro", uri=True)


def _extras(path):
    with _ro(path) as c:
        return {t: sorted(r[0] for r in c.execute(f"SELECT source_id FROM {t}"))
                for t in ("source_counts", "unit_state", "source_state", "source_data_through")}


def _rows(path):
    with _ro(path) as c:
        return (sorted(r[0] for r in c.execute("SELECT series_id FROM series")),
                sorted(r[0] for r in c.execute("SELECT source_id FROM source")))


@pytest.fixture
def live(tmp_path, monkeypatch):
    """A cut-over machine: flag, blob store, store files, catalogue build and lock all in tmp_path. The build is
    made BEFORE the flag: making it is a write, and after T0 core.catalog_path's runtime guard refuses a
    plain read-write open of the build without the lock (R1209)."""
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    store_root = tmp_path / "store"
    for rel in ("clean_full/foo/x.parquet", "clean_full/foo/sub/y.parquet", "clean_full/foo_direct/z.parquet"):
        p = store_root / "data" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"PAR1" + rel.encode())
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(store_root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "live" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "live" / "writer.lock"))
    monkeypatch.setattr(catalog_path, "LIVE_STATE_DIR", str(tmp_path / "live" / "state"))
    _catalogue(tmp_path / "live" / "catalog.db")
    (tmp_path / "CUTOVER").write_text("")                    # T0, now that the build exists
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


# ---- AR-153 ------------------------------------------------------------------------------------------
import json as _json  # noqa: E402


def _fts(path):
    with _ro(path) as c:
        return sorted(r[0] for r in c.execute("SELECT series_id FROM series_fts"))


def _log(tmp):
    p = tmp / "live" / "state" / lt.REMOVAL_LOG
    return [_json.loads(l) for l in open(p, encoding="utf-8")] if p.exists() else []


def test_after_t0_the_search_index_and_the_empty_folders_go_and_the_removal_is_logged(live):
    tmp, store, sb = live
    assert retire_source.main(["foo", "--apply"]) == 0
    assert _fts(tmp / "live" / "catalog.db") == ["foo_direct:a"], "no FTS orphans (the swap check counts them)"
    assert not (store / "data" / "clean_full" / "foo").exists(), "the emptied source folder is removed"
    assert (store / "data" / "clean_full" / "foo_direct").exists()
    assert (store / "data").exists(), "never above the store's data folder"
    log = _log(tmp)
    assert [(e["tool"], e["source"]) for e in log] == [("retire_source", "foo")] and log[0]["at"]
    assert delist_source_rows.main(["foo_direct", "--apply"]) == 0
    assert [(e["tool"], e["source"]) for e in _log(tmp)][-1] == ("delist_source_rows", "foo_direct")


def test_before_t0_nothing_is_logged_and_the_fts_is_untouched_locally(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "LIVE_STATE_DIR", str(tmp_path / "live" / "state"))
    _catalogue(tmp_path / "checkout" / "catalog.db")
    monkeypatch.setattr(d1_remote, "execute_wrangler", lambda db, stmts, **k: True)
    assert delist_source_rows.main(["foo", "--apply"]) == 0
    assert _log(tmp_path) == []
    assert _fts(tmp_path / "checkout" / "catalog.db") == ["foo:a", "foo:b", "foo_direct:a"], "as before T0 always"


def test_archived_series_objects_keep_their_metadata(live):
    tmp, store, sb = live
    t = lt.Targets()
    t.archive("series/foo%3Aa.csv", "archive/retired/foo/foo%3Aa.csv")
    src, dst = sb.store.head("series/foo%3Aa.csv"), sb.store.head("archive/retired/foo/foo%3Aa.csv")
    for k in ("etag", "size", "content_encoding", "content_type", "custom_metadata", "sha256"):
        assert dst[k] == src[k], k
    assert sb.get("archive/retired/foo/foo%3Aa.csv") == sb.get("series/foo%3Aa.csv")


def test_after_t0_a_dry_run_needs_no_lock(live, monkeypatch):
    class Busy:
        def __enter__(self):
            raise cutover.CutoverRefused("refused: another process holds the catalogue writer lock")

        def __exit__(self, *a):
            return False
    monkeypatch.setattr(catalog_path, "writer_lock", lambda: Busy())
    assert retire_source.main(["foo"]) == 0
    assert delist_source_rows.main(["foo"]) == 0


def test_before_t0_a_sharded_source_is_deleted_on_the_shard_too(tmp_path, monkeypatch):
    plan = lt.Targets.d1_plan("noaa")
    assert plan[0] == ("econ-catalog", lt.Targets.d1_statements("noaa"))
    assert plan[1] == ("econ-catalog-climate", ["DELETE FROM series WHERE source_id='noaa';",
                                                "DELETE FROM source_counts WHERE source_id='noaa';"])
    assert lt.Targets.d1_plan("foo") == [("econ-catalog", lt.Targets.d1_statements("foo"))]
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))
    calls = []
    monkeypatch.setattr(d1_remote, "execute_wrangler", lambda db, stmts, **k: calls.append(db) or db != "econ-catalog-climate")
    assert lt.Targets().d1_execute("noaa") is False, "a failure on the shard is a failure"
    assert calls == ["econ-catalog", "econ-catalog-climate"]


def test_residual_rows_fail_the_delisting(live, monkeypatch):
    monkeypatch.setattr(lt.Targets, "remove_source_rows", lambda self, con, src: 1)
    assert delist_source_rows.main(["foo", "--apply"]) == 1


def test_residual_objects_fail_the_csv_purge(live, monkeypatch):
    monkeypatch.setattr(lt.Targets, "delete", lambda self, keys, allowed: 0)        # nothing really removed
    assert delist_source_rows.main(["foo", "--purge-csv-prefix", "a", "--apply"]) == 1
    assert _log(live[0]) == [], "a failed purge is not logged as a removal"
