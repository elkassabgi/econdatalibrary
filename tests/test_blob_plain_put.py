"""put_atomic(plain=True) (R1206): the tools that always stored their series CSVs PLAIN keep doing so after plan
step 1 moved them onto the CSV store. Gzip is not neutral for them - the worker refuses a filter on a gzipped
object above its decompression-ratio limit and serves it without the citation in its body. Both stores: the
bytes as given, no ContentEncoding, no csvmd5; bytes the store already holds are not written again; a gzip
body under plain=True is refused (it would be served as garbage text/csv, the R560 class)."""
import gzip
import hashlib
import os
import sys

import pytest

from updater import blob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from blobstore import BlobStore  # noqa: E402

CSV = b"series_id,obs_date,value\nx:1,2020-01-01,1.5\n"
KEY = "series/x%3A1.csv"


class _S3:
    def __init__(self, held=None):
        self.puts, self.held = [], held

    def head_object(self, **kw):
        if self.held is None:
            raise RuntimeError("absent")
        return {"ETag": f'"{hashlib.md5(self.held).hexdigest()}"', "Metadata": {}}

    def put_object(self, **kw):
        self.puts.append(kw)


def test_r2_stores_the_plain_bytes_with_no_encoding():
    r2 = blob.R2Blob()
    r2._client = _S3()
    r2.put_atomic(KEY, CSV, plain=True)
    (sent,) = r2._client.puts
    assert sent["Body"] == CSV and sent["ContentType"] == "text/csv"
    assert "ContentEncoding" not in sent and "Metadata" not in sent


def test_r2_skips_plain_bytes_it_already_holds():
    r2 = blob.R2Blob()
    r2._client = _S3(held=CSV)
    before = blob.SKIPPED_IDENTICAL[0]
    r2.put_atomic(KEY, CSV, plain=True)
    assert r2._client.puts == [] and blob.SKIPPED_IDENTICAL[0] == before + 1
    r2._client = _S3(held=gzip.compress(CSV, mtime=0))           # held gzipped: rewritten plain
    r2.put_atomic(KEY, CSV, plain=True)
    assert r2._client.puts and r2._client.puts[0]["Body"] == CSV


@pytest.fixture
def sb(tmp_path):
    BlobStore(str(tmp_path / "blobs"), create=True)
    return blob.SelfhostBlob(root=str(tmp_path / "blobs"))


def test_the_self_hosted_store_matches(sb):
    sb.put_atomic(KEY, CSV, plain=True)
    meta = sb.store.head(KEY)
    assert sb.get(KEY) == CSV and meta["content_encoding"] is None and meta["content_type"] == "text/csv"
    assert not meta["custom_metadata"] and meta["etag"] == hashlib.md5(CSV).hexdigest()
    before = blob.SKIPPED_IDENTICAL[0]
    sb.put_atomic(KEY, CSV, plain=True)
    assert blob.SKIPPED_IDENTICAL[0] == before + 1, "held bytes are not written again"
    sb.put_atomic(KEY, CSV)                                        # the default still gzips
    assert sb.store.head(KEY)["content_encoding"] == "gzip"
    sb.put_atomic(KEY, CSV, plain=True)                            # and plain=True turns it back
    assert sb.get(KEY) == CSV and sb.store.head(KEY)["content_encoding"] is None


@pytest.mark.parametrize("make", ["r2", "selfhost"])
def test_a_gzip_body_under_plain_is_refused(sb, make):
    store = sb
    if make == "r2":
        store = blob.R2Blob()
        store._client = _S3()
    with pytest.raises(ValueError, match="plain=True with a gzip body"):
        store.put_atomic(KEY, gzip.compress(CSV, mtime=0), plain=True)


def test_put_with_retry_passes_plain_only_when_set():
    from updater import derive
    calls = []

    class Old:                                                     # a store without the keyword
        def put_atomic(self, key, data):
            calls.append("old")

    class New:
        def put_atomic(self, key, data, plain=False):
            calls.append(plain)
    assert derive._put_with_retry(Old(), KEY, CSV) and derive._put_with_retry(New(), KEY, CSV, plain=True)
    assert calls == ["old", True]


def test_a_plain_put_over_a_gzip_object_with_csvmd5_is_still_sent():
    """R1213: the csvmd5 of a gzip object says nothing about PLAIN bytes at rest - trusting it would leave the
    object gzipped. The held object here is what a real writer leaves: gzip, marked, with csvmd5."""
    held_gz = gzip.compress(CSV, mtime=0)

    class S3(_S3):
        def head_object(self, **kw):
            return {"ETag": f'"{hashlib.md5(held_gz).hexdigest()}"', "ContentEncoding": "gzip",
                    "Metadata": {"csvmd5": hashlib.md5(CSV).hexdigest()}}
    r2 = blob.R2Blob()
    r2._client = S3()
    r2.put_atomic(KEY, CSV, plain=True)
    assert r2._client.puts and r2._client.puts[0]["Body"] == CSV and "ContentEncoding" not in r2._client.puts[0]


def test_head_meta_raises_on_anything_but_not_found():
    """R1211: derive_one fails closed on a head error; a head_meta that read a 403/503 as None ("absent") would
    make that fail open. 404 -> None; 403 -> raises."""
    from botocore.exceptions import ClientError

    class S3:
        def __init__(self, code):
            self.code = code

        def head_object(self, **kw):
            raise ClientError({"Error": {"Code": self.code}, "ResponseMetadata": {"HTTPStatusCode": int(self.code)}},
                              "HeadObject")
    r2 = blob.R2Blob()
    r2._client = S3("404")
    assert r2.head_meta(KEY) is None
    for code in ("403", "503"):
        r2._client = S3(code)
        with pytest.raises(ClientError):
            r2.head_meta(KEY)


def test_the_pool_goes_from_client_to_botocore(monkeypatch):
    """R1213: the pool test stopped at _boto3_client; r2_util.client(write, pool) dropping it survived."""
    from core import r2_util
    seen = []
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {"endpoint": "e", "key": "k", "secret": "s"})
    monkeypatch.setattr(r2_util, "_boto3_client", lambda c, pool=None: seen.append(pool) or object())
    monkeypatch.setattr(r2_util, "guard_client", lambda s3, **k: s3)
    r2_util.client(write=True, pool=96)
    r2_util.client(write=True)
    assert seen == [96, None]


def test_a_cold_self_hosted_store_is_opened_once_by_many_threads(tmp_path, monkeypatch):
    """R1213: 8 threads on an unopened SelfhostBlob built 8 BlobStore instances (derive_and_put shares one)."""
    import threading
    BlobStore(str(tmp_path / "blobs"), create=True)
    built = []
    real = blob._blobstore_module

    class Counting:
        def BlobStore(self, root):
            built.append(root)
            return real().BlobStore(root)
    monkeypatch.setattr(blob, "_blobstore_module", lambda: Counting())
    sb = blob.SelfhostBlob(root=str(tmp_path / "blobs"))
    go = threading.Barrier(8)

    def touch():
        go.wait()
        sb.put_atomic(f"series/t{threading.get_ident()}.csv", CSV)
    ts = [threading.Thread(target=touch) for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len(built) == 1, built
