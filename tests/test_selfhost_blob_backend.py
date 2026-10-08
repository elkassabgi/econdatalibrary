"""updater/blob.py SelfhostBlob (plan change 4): the self-hosted twin of R2Blob. The strongest check is
parity: for the same key and bytes it must store exactly what R2Blob would send to put_object."""
import gzip
import hashlib
import os
import sys

import pytest

from updater import blob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from blobstore import BlobStore  # noqa: E402

CSV = b"series_id,obs_date,value\nx:1,2020-01-01,1.5\nx:1,2020-02-01,2.5\n"


@pytest.fixture
def sb(tmp_path):
    BlobStore(str(tmp_path / "blobs"), create=True)
    return blob.SelfhostBlob(root=str(tmp_path / "blobs"))


class _FakeS3:
    def __init__(self):
        self.puts = []

    def head_object(self, **kw):
        raise RuntimeError("absent")          # r2_holds_csv treats any error as "not held" -> upload

    def put_object(self, **kw):
        self.puts.append(kw)


@pytest.mark.parametrize("key,data", [("series/x%3A1.csv", CSV), ("_aqueduct/stats.json", b'{"a": 1}'),
                                      ("series/pre%3Agz.csv", gzip.compress(CSV, mtime=0)),
                                      ("_aqueduct/listing.csv", CSV)])     # a CSV outside series/ stays plain
def test_parity_with_r2blob(sb, key, data):
    r2 = blob.R2Blob()
    r2._client = _FakeS3()
    r2.put_atomic(key, data)
    sent = r2._client.puts[0]
    sb.put_atomic(key, data)
    meta = sb.store.head(key)
    assert sb.get(key) == sent["Body"], "the same stored bytes"
    assert meta["content_encoding"] == sent.get("ContentEncoding")
    assert meta["content_type"] == sent.get("ContentType")
    assert meta["custom_metadata"] == sent.get("Metadata", {})
    assert meta["etag"] == hashlib.md5(sent["Body"]).hexdigest(), "the etag a single-part R2 PUT reports"


def test_a_series_csv_is_gzip_at_rest_with_its_plain_digest(sb):
    sb.put_atomic("series/x%3A1.csv", CSV)
    stored = sb.get("series/x%3A1.csv")
    assert gzip.decompress(stored) == CSV
    assert sb.store.head("series/x%3A1.csv")["custom_metadata"] == {"csvmd5": hashlib.md5(CSV).hexdigest()}


def test_identical_bytes_are_not_written_again(sb):
    sb.put_atomic("series/x%3A1.csv", CSV)
    before = blob.SKIPPED_IDENTICAL[0]
    stamp = sb.store.head("series/x%3A1.csv")
    sb.put_atomic("series/x%3A1.csv", CSV)
    assert blob.SKIPPED_IDENTICAL[0] == before + 1
    assert sb.store.head("series/x%3A1.csv") == stamp
    sb.put_atomic("series/x%3A1.csv", CSV + b"x:1,2020-03-01,3.5\n")      # a real change is written
    assert gzip.decompress(sb.get("series/x%3A1.csv")).endswith(b"3.5\n")


def test_an_object_without_csvmd5_is_recognised_by_its_etag(sb):
    """Objects imported from R2 before the csvmd5 metadata existed carry only an etag; identical bytes
    must still be skipped (R1176 finding 7: 'the etag fallback always says no' survived)."""
    from core.r2_util import series_csv_put_args
    stored, _kw, _digest = series_csv_put_args(CSV)
    sb.store.put("series/old.csv", stored, etag=hashlib.md5(stored).hexdigest(), content_encoding="gzip",
                 content_type="text/csv", custom_metadata={})
    before = blob.SKIPPED_IDENTICAL[0]
    sb.put_atomic("series/old.csv", CSV)
    assert blob.SKIPPED_IDENTICAL[0] == before + 1
    assert sb.store.head("series/old.csv")["custom_metadata"] == {}, "not rewritten"


def test_a_deleted_key_s_file_is_collected_by_gc(sb):
    """delete() retires the file; gc removes it after the grace (finding 7: a delete with no retired row
    left the file on disk for ever)."""
    sb.put_atomic("series/gone.csv", CSV)
    path = sb.store.head("series/gone.csv")["path"]
    sb.delete("series/gone.csv")
    assert os.path.exists(path), "kept for in-flight reads"
    assert sb.store.gc(grace_hours=-1) == 1 and not os.path.exists(path)


def test_listing_deleting_and_the_small_accessors(sb, tmp_path):
    for k in ("series/a.csv", "series/b.csv", "_aqueduct/x.json"):
        sb.put_atomic(k, CSV if k.endswith(".csv") else b"{}")
    assert sb.list_keys("series/") == ["series/a.csv", "series/b.csv"]
    assert sb.exists("series/a.csv") and sb.size("series/a.csv") == len(sb.get("series/a.csv"))
    assert sb.etag("series/a.csv") == hashlib.md5(sb.get("series/a.csv")).hexdigest()
    sb.delete("series/a.csv")
    assert not sb.exists("series/a.csv") and sb.get("series/a.csv") is None and sb.etag("series/a.csv") is None
    assert sb.list_keys("series/") == ["series/b.csv"]
    src = tmp_path / "f.json"
    src.write_bytes(b'{"k": 2}')
    sb.put_file("_aqueduct/f.json", str(src))
    assert sb.get("_aqueduct/f.json") == b'{"k": 2}'
    assert sb.store.head("_aqueduct/f.json")["content_type"] == "application/json"


def test_a_missing_store_is_an_error_never_created(tmp_path):
    s = blob.SelfhostBlob(root=str(tmp_path / "none"))
    with pytest.raises(FileNotFoundError):
        s.get("series/a.csv")
    assert not (tmp_path / "none").exists()


def test_the_store_keeps_a_given_stored_time_and_lists_it(sb):
    """An imported object keeps R2's LastModified, so a resume that skips what its own campaign wrote
    (derive_csv_bulk --skip-newer-than) does not mistake every imported object for a fresh write."""
    import datetime as dt
    sb.store.put("series/a%3A1.csv", b"x", etag="e1", stored_utc="2026-01-02T03:04:05+00:00")
    sb.put_atomic("series/a%3A2.csv", CSV)                              # default: now
    got = dict(sb.list_modified("series/a%3A"))
    assert got["series/a%3A1.csv"] == dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    assert got["series/a%3A2.csv"] > dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
    assert list(got) == ["series/a%3A1.csv", "series/a%3A2.csv"]


def test_list_stored_stays_inside_its_prefix(sb):
    """R1200 W20: without the upper bound a prefix listing ran on into every later key."""
    for k in ("series/a%3A1.csv", "series/a%3A2.csv", "series/b%3A1.csv", "series/a%3B.csv"):
        sb.store.put(k, b"x", etag="e")
    assert [k for k, _t in sb.store.list_stored("series/a%3A")] == ["series/a%3A1.csv", "series/a%3A2.csv"]


def test_the_importer_passes_r2s_last_modified(tmp_path):
    import datetime as dt
    import import_from_r2
    store = BlobStore(str(tmp_path / "imp"), create=True)
    lm = dt.datetime(2025, 5, 6, 7, 8, 9, tzinfo=dt.timezone(dt.timedelta(hours=-5)))

    class S3:
        def get_object(self, **kw):
            import io
            return {"Body": io.BytesIO(b"abc"), "ETag": '"%s"' % hashlib.md5(b"abc").hexdigest(),   # noqa: S324
                    "LastModified": lm}
    ok, _msg = import_from_r2.copy_one(S3(), store, "series/b%3A1.csv")
    assert ok and store.list_stored("series/") == [("series/b%3A1.csv", "2025-05-06T12:08:09+00:00")]


@pytest.mark.parametrize("cut_over,backend,kind", [
    (False, None, "R2Blob"), (False, "local", "R2Blob"), (False, "r2", "R2Blob"),
    (False, "selfhost", "SelfhostBlob"),                 # a deliberate pre-T0 probe of the self-hosted store
    (True, None, "SelfhostBlob"), (True, "r2", "SelfhostBlob"),       # after T0 the FLAG decides (R1200)
])
def test_the_csv_store_is_never_local_files(tmp_path, monkeypatch, capsys, cut_over, backend, kind):
    """blob.csv_store(): series CSVs live in an object store. from_env()'s LocalBlob default sent a run with
    no backend set to local files in the current folder, and reported success (review R1200, probes P1/P2)."""
    from core import cutover
    flag = tmp_path / "CUTOVER"
    if cut_over:
        flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setenv("AQUEDUCT_BACKEND", backend or "x")
    if backend is None:
        monkeypatch.delenv("AQUEDUCT_BACKEND")
    store = blob.csv_store("econ-data")
    assert type(store).__name__ == kind and store.bucket == "econ-data"
    assert ("self-hosted" if kind == "SelfhostBlob" else "R2 econ-data") in capsys.readouterr().out
    with pytest.raises(SystemExit, match="not the CSV store's bucket"):
        blob.csv_store("other-bucket")


def test_a_missing_self_hosted_store_fails_at_once(tmp_path, monkeypatch):
    """Not after 7 retries per object (R1200 finding 2)."""
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "no_store"))
    with pytest.raises(FileNotFoundError, match="no blob store"):
        blob.csv_store("econ-data")


def test_the_backend_is_selected_by_name():
    assert isinstance(blob.from_env("selfhost"), blob.SelfhostBlob)
    # the DEFAULT, from the source; the instance takes the module attribute, which tests/conftest.py moves (R1230)
    assert 'SELFHOST_BLOB_ROOT = r"F:\\econ_live\\blobs"' in open(blob.__file__, encoding="utf-8").read()
    assert blob.SelfhostBlob().root == blob.SELFHOST_BLOB_ROOT != r"F:\econ_live\blobs"
    with pytest.raises(ValueError, match="selfhost"):
        blob.from_env("nope")


# ---- the rename that makes a put visible is retried (WinError 32: 7 puts of the series copy, 2026-10-02/03) --------
def _flaky_replace(monkeypatch, fails):
    import blobstore as BS                                     # noqa: PLC0415
    real = os.replace
    calls = []

    def replace(src, dst):
        calls.append(os.path.basename(dst))
        if len(calls) <= fails:
            raise PermissionError(32, "The process cannot access the file because it is being used by another process")
        return real(src, dst)
    monkeypatch.setattr(BS.os, "replace", replace)
    monkeypatch.setattr(BS.time, "sleep", lambda s: SLEPT.append(s))
    SLEPT.clear()
    return calls


SLEPT = []


def test_a_put_retries_a_rename_another_process_briefly_holds(tmp_path, monkeypatch):
    st = BlobStore(str(tmp_path / "b"), create=True)
    calls = _flaky_replace(monkeypatch, fails=2)
    sha = st.put("series/x.csv", b"date,value\n2026-01-01,1\n", etag="e1")
    h = st.head("series/x.csv")
    assert h["sha256"] == sha and open(h["path"], "rb").read() == b"date,value\n2026-01-01,1\n"
    assert len(calls) == 3, "two refusals, then the rename"
    import blobstore as BS                                     # noqa: PLC0415
    assert SLEPT == list(BS.REPLACE_BACKOFF_S[:2]), "it waits between tries, the backoff's own waits"


def test_a_rename_that_never_clears_fails_the_put_and_leaves_nothing(tmp_path, monkeypatch):
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    calls = _flaky_replace(monkeypatch, fails=99)
    with pytest.raises(PermissionError):
        st.put("series/y.csv", b"never stored", etag="e2")
    assert len(calls) == len(BS.REPLACE_BACKOFF_S) + 1, "bounded: every wait, then the error"
    assert SLEPT == list(BS.REPLACE_BACKOFF_S), "every wait taken, in order (a retry with no wait is useless)"
    assert st.head("series/y.csv") is None, "no index row for a put that did not happen"
    left = [f for _d, _s, fs in os.walk(tmp_path / "b" / "objects") for f in fs]
    assert left == [], f"no temp file and no object left behind: {left}"


def test_the_waits_are_core_atomics(monkeypatch):
    import blobstore as BS                                     # noqa: PLC0415
    from core import atomic                                    # noqa: PLC0415
    assert BS.REPLACE_BACKOFF_S == atomic.BACKOFF_S, "the blob store's waits are core.atomic's"



def test_a_temp_file_the_holder_also_refuses_to_delete_is_left_and_gc_sweeps_it(tmp_path, monkeypatch):
    """AR-211 finding 3: the handle that refuses the rename denies deletion too, so the put's own cleanup can fail.
    The temp file then stays; gc() removes it once it is older than the grace period."""
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    _flaky_replace(monkeypatch, fails=99)
    real_remove = os.remove

    def remove(path):
        if os.path.basename(path).startswith(".tmp-"):
            raise PermissionError(32, "held")
        return real_remove(path)
    monkeypatch.setattr(BS.os, "remove", remove)
    with pytest.raises(PermissionError):
        st.put("series/z.csv", b"held temp", etag="e3")
    temps = [os.path.join(d, f) for d, _s, fs in os.walk(tmp_path / "b" / "objects") for f in fs]
    assert len(temps) == 1 and os.path.basename(temps[0]).startswith(".tmp-"), temps
    assert SLEPT == list(BS.REPLACE_BACKOFF_S) * 2, "the rename's waits, then the removal's own waits"
    monkeypatch.setattr(BS.os, "remove", real_remove)
    assert st.gc(grace_hours=1) == 0 and os.path.exists(temps[0]), "younger than the grace period: kept"
    old = os.path.getmtime(temps[0]) - 7200
    os.utime(temps[0], (old, old))
    assert st.gc(grace_hours=1) == 1 and not os.path.exists(temps[0]), "older: swept"


@pytest.mark.skipif(os.name != "nt", reason="Windows sharing violations only")
def test_a_real_held_handle_refuses_rename_and_removal_until_it_closes(tmp_path, monkeypatch):
    """No fakes (AR-211 C4): an open Python handle denies delete sharing on Windows, so both the rename and the
    removal are refused with PermissionError while it is open; both go through after it closes."""
    import blobstore as BS                                     # noqa: PLC0415
    monkeypatch.setattr(BS.time, "sleep", lambda s: None)
    src, dst = tmp_path / ".tmp-held", tmp_path / "target"
    src.write_bytes(b"x")
    with open(src, "rb"):
        with pytest.raises(PermissionError):
            BS._replace(str(src), str(dst))
        assert BS._remove_temp(str(src)) is False and src.exists()
    BS._replace(str(src), str(dst))
    assert dst.read_bytes() == b"x"
    other = tmp_path / ".tmp-other"
    other.write_bytes(b"y")
    assert BS._remove_temp(str(other)) is True and not other.exists()


# ---- AR-263: behaviours no test held (each was a surviving mutant) --------------------------------------------------
def _files(root):
    return [f for _d, _s, fs in os.walk(root) for f in fs]


@pytest.mark.parametrize("error", [OSError(28, "No space left on device"), FileNotFoundError(2, "No such file")])
def test_an_error_that_is_not_a_sharing_violation_is_raised_at_once(tmp_path, monkeypatch, error):
    """Only PermissionError is retried (core.atomic's rule): a full disk or a missing folder does not clear in 3 s."""
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    calls = []

    def replace(src, dst):
        calls.append(dst)
        raise error
    monkeypatch.setattr(BS.os, "replace", replace)
    monkeypatch.setattr(BS.time, "sleep", lambda s: SLEPT.append(s))
    SLEPT.clear()
    with pytest.raises(OSError) as ei:
        st.put("series/n.csv", b"not stored", etag="e4")
    assert ei.value is error
    assert len(calls) == 1 and SLEPT == [], "one try, no wait"
    assert st.head("series/n.csv") is None and _files(tmp_path / "b" / "objects") == []


def test_gc_never_sweeps_an_object_however_old_its_file_is(tmp_path):
    """The temp sweep takes .tmp- files only: a live object's file is older than any grace period most of its life."""
    st = BlobStore(str(tmp_path / "b"), create=True)
    st.put("series/old.csv", b"old and live", etag="e")
    path = st.head("series/old.csv")["path"]
    old = os.path.getmtime(path) - 7200
    os.utime(path, (old, old))
    assert st.gc(grace_hours=1) == 0 and os.path.exists(path)
    assert st.gc(grace_hours=-1) == 0 and os.path.exists(path)


def test_an_interrupt_in_the_wait_is_raised_and_the_temp_file_is_removed(tmp_path, monkeypatch):
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    _flaky_replace(monkeypatch, fails=99)

    def interrupt(_s):
        raise KeyboardInterrupt
    monkeypatch.setattr(BS.time, "sleep", interrupt)
    with pytest.raises(KeyboardInterrupt):
        st.put("series/i.csv", b"interrupted", etag="e5")
    assert st.head("series/i.csv") is None and _files(tmp_path / "b" / "objects") == []
    monkeypatch.undo()
    assert st.put("series/i2.csv", b"the next put", etag="e6"), "the write transaction was ended"


def test_a_cleanup_error_never_replaces_the_renames_error(tmp_path, monkeypatch):
    import errno                                               # noqa: PLC0415
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    _flaky_replace(monkeypatch, fails=99)
    real_remove = os.remove

    def remove(path):
        if os.path.basename(path).startswith(".tmp-"):
            raise OSError(errno.EIO, "Input/output error")
        return real_remove(path)
    monkeypatch.setattr(BS.os, "remove", remove)
    with pytest.raises(PermissionError):
        st.put("series/io.csv", b"io error", etag="e7")
    assert SLEPT == list(BS.REPLACE_BACKOFF_S), "the rename's waits only: this removal error is not retried"


def test_gc_removes_a_temp_file_only_inside_its_write_transaction(tmp_path, monkeypatch):
    """A put holds the write transaction from its temp write to its index row, so a temp file that gc removes INSIDE
    its own transaction is no put's in flight - also with no grace period at all (AR-211's race, AR-263)."""
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    d = tmp_path / "b" / "objects" / "ab"
    d.mkdir()
    (d / ".tmp-orphan").write_bytes(b"x")
    seen, real_remove = [], os.remove

    def remove(path):
        if os.path.basename(path).startswith(".tmp-"):
            seen.append((st._w.in_transaction, st._wlock.locked()))
        return real_remove(path)
    monkeypatch.setattr(BS.os, "remove", remove)
    assert st.gc(grace_hours=-1) == 1 and seen == [(True, True)]


def _retire_one(st, key):
    st.put(key, key.encode(), etag="e")
    path = st.head(key)["path"]
    st.delete(key)
    st._w.execute("UPDATE retired SET retired_utc='2000-01-01T00:00:00+00:00'")
    return path


@pytest.mark.parametrize("deny", ["objects", "sub"])
def test_gc_skips_a_directory_it_cannot_list_and_still_collects_retired_files(tmp_path, monkeypatch, deny):
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    path = _retire_one(st, "series/g.csv")
    real = os.scandir

    def scandir(p="."):
        if (os.path.basename(str(p)) == "objects") == (deny == "objects"):
            raise PermissionError(13, "Access is denied")
        return real(p)
    monkeypatch.setattr(BS.os, "scandir", scandir)
    assert st.gc(grace_hours=1) == 1 and not os.path.exists(path)


def test_gc_keeps_a_temp_file_that_is_still_held_and_goes_on(tmp_path, monkeypatch):
    import blobstore as BS                                     # noqa: PLC0415
    st = BlobStore(str(tmp_path / "b"), create=True)
    path = _retire_one(st, "series/h.csv")
    held = os.path.join(os.path.dirname(path), ".tmp-held")
    with open(held, "wb") as fh:
        fh.write(b"x")
    old = os.path.getmtime(held) - 7200
    os.utime(held, (old, old))
    real_remove = os.remove

    def remove(p):
        if os.path.basename(p).startswith(".tmp-"):
            raise PermissionError(32, "held")
        return real_remove(p)
    monkeypatch.setattr(BS.os, "remove", remove)
    assert st.gc(grace_hours=1) == 1 and os.path.exists(held) and not os.path.exists(path)
