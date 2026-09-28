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
    assert 'SELFHOST_BLOB_ROOT = r"E:\\econ_live\\blobs"' in open(blob.__file__, encoding="utf-8").read()
    assert blob.SelfhostBlob().root == blob.SELFHOST_BLOB_ROOT != r"E:\econ_live\blobs"
    with pytest.raises(ValueError, match="selfhost"):
        blob.from_env("nope")
