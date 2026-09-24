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
