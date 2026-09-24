"""An object whose bytes are already in R2 is not uploaded again.

MEASURED 2026-09-02: 7,686,397 of 10.8 million class-A operations over 24 days are PutObject on
econ-data - 71% of the only variable line left on a median day ($30.52 of a $51.44 month), and
about 320,000 uploads a day. The publish path never compared anything, so a source using
`bulk_snapshot_if_changed` republishes identical bytes daily.

THE COMPARISON IS EXACT. `sorted_csv_gz` gzips with mtime=0 and no filename precisely so "the
object matches gzip.compress(csv, mtime=0) exactly", so the same data gives the same bytes and
the same MD5, and R2 reports that MD5 as a single-part object's ETag.

EVERY UNCERTAIN CASE UPLOADS. A multipart ETag is a digest of digests, not of the content. A
head that errors, an absent ETag, a size disagreement - all upload. A wasted class-A operation
costs $0.0000045; a stale served object costs a user the wrong answer. The tests below pin the
refusals as hard as the skip, because a check that gets adventurous here is worse than none.

PLAN STEP 1 (batch 4): the helpers take the CSV STORE (updater.blob.csv_store - R2 before T0, the
self-hosted blob store after it), not a boto3 client. The R2 tests run a real R2Blob over a fake
client, so the store's own head_meta and put_gzip_file are what is exercised.
"""
from __future__ import annotations

import hashlib
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core.derive_csv import (_SKIPPED_IDENTICAL, _put_gzip_file_with_backoff,  # noqa: E402
                             object_is_identical)

BODY = b"\x1f\x8b" + b"pretend this is a gzipped csv" * 40


class FakeS3:
    """head_object returns whatever it was built with; put_object records the call."""

    def __init__(self, head=None, raises=False):
        self._head, self._raises = head, raises
        self.puts = []

    def head_object(self, **kw):
        if self._raises:
            raise RuntimeError("no such key")
        return dict(self._head or {})

    def put_object(self, **kw):
        body = kw.get("Body")
        if hasattr(body, "read"):
            kw = dict(kw, Body=body.read())          # the file handle is closed after the call
        self.puts.append(kw)


def _store(s3):
    from updater import blob
    b = blob.R2Blob()
    b._client = s3
    return b


def _file(tmp_path, body=BODY):
    p = tmp_path / "obj.csv.gz"
    p.write_bytes(body)
    return str(p)


def _head_for(path, etag=None, size=None):
    md5 = hashlib.md5(open(path, "rb").read()).hexdigest()   # noqa: S324
    return {"ETag": f'"{etag or md5}"',
            "ContentLength": size if size is not None else os.path.getsize(path)}


def test_identical_bytes_are_recognised(tmp_path):
    p = _file(tmp_path)
    assert object_is_identical(_store(FakeS3(_head_for(p))), "k", p) is True


def test_different_bytes_are_not(tmp_path):
    p = _file(tmp_path)
    head = _head_for(p, etag="0" * 32)
    assert object_is_identical(_store(FakeS3(head)), "k", p) is False


def test_a_size_disagreement_short_circuits(tmp_path):
    """Cheap check first: a different length cannot be the same content, whatever the ETag."""
    p = _file(tmp_path)
    head = _head_for(p, size=os.path.getsize(p) + 1)
    assert object_is_identical(_store(FakeS3(head)), "k", p) is False


def test_a_MULTIPART_etag_is_refused(tmp_path):
    """`<hex>-<n>` is a digest of part digests, not of the content. Comparing it to a whole-file
    MD5 would be meaningless, so it must upload rather than guess."""
    p = _file(tmp_path)
    head = {"ETag": '"d41d8cd98f00b204e9800998ecf8427e-7"',
            "ContentLength": os.path.getsize(p)}
    assert object_is_identical(_store(FakeS3(head)), "k", p) is False


def test_a_missing_object_is_not_identical(tmp_path):
    p = _file(tmp_path)
    assert object_is_identical(_store(FakeS3(raises=True)), "k", p) is False


def test_a_head_with_no_etag_is_refused(tmp_path):
    p = _file(tmp_path)
    assert object_is_identical(_store(FakeS3({"ContentLength": os.path.getsize(p)})), "k", p) is False


def test_a_missing_local_file_is_refused(tmp_path):
    assert object_is_identical(_store(FakeS3(_head_for(_file(tmp_path)))), "k",
                               str(tmp_path / "gone.gz")) is False


def test_the_put_is_SKIPPED_when_the_object_matches(tmp_path):
    p = _file(tmp_path)
    s3 = FakeS3(_head_for(p))
    before = _SKIPPED_IDENTICAL[0]
    _put_gzip_file_with_backoff(_store(s3), "k", p)
    assert s3.puts == [], "an identical object was uploaded again"
    assert _SKIPPED_IDENTICAL[0] == before + 1, "the saving was not counted"


def test_the_put_HAPPENS_when_the_object_differs(tmp_path):
    p = _file(tmp_path)
    s3 = FakeS3(_head_for(p, etag="0" * 32))
    _put_gzip_file_with_backoff(_store(s3), "k", p)
    assert len(s3.puts) == 1, "changed content was not uploaded"
    assert s3.puts[0]["Key"] == "k" and s3.puts[0]["Body"] == BODY


def test_the_put_HAPPENS_when_the_object_is_absent(tmp_path):
    p = _file(tmp_path)
    s3 = FakeS3(raises=True)
    _put_gzip_file_with_backoff(_store(s3), "k", p)
    assert len(s3.puts) == 1, "a first upload was skipped"


def test_skip_identical_False_forces_the_write(tmp_path):
    """The escape hatch: a caller that knows better must be able to overwrite."""
    p = _file(tmp_path)
    s3 = FakeS3(_head_for(p))
    _put_gzip_file_with_backoff(_store(s3), "k", p, skip_identical=False)
    assert len(s3.puts) == 1, "skip_identical=False did not force the upload"


def test_metadata_still_reaches_a_real_upload(tmp_path):
    """The never-shrink guard records rows and bytes here; skipping must not lose that path."""
    p = _file(tmp_path)
    s3 = FakeS3(raises=True)
    _put_gzip_file_with_backoff(_store(s3), "k", p, metadata={"rows": "7", "bytes": "9"})
    assert s3.puts[0]["Metadata"] == {"rows": "7", "bytes": "9"}


def test_the_put_is_marked_gzip_text_csv(tmp_path):
    """put_gzip_file keeps what the helper always sent: text/csv and ContentEncoding gzip."""
    p = _file(tmp_path)
    s3 = FakeS3(raises=True)
    _put_gzip_file_with_backoff(_store(s3), "series/x%3A1.csv", p)
    (put,) = s3.puts
    assert put["ContentType"] == "text/csv" and put["ContentEncoding"] == "gzip"


def test_a_file_that_is_not_gzip_is_refused(tmp_path):
    """Marked gzip, plain bytes would be served as garbage (the R560 class)."""
    p = _file(tmp_path, body=b"series_id,obs_date,value" + bytes([10]))
    s3 = FakeS3(raises=True)
    with pytest.raises(ValueError, match="not gzip"):
        _put_gzip_file_with_backoff(_store(s3), "series/x%3A1.csv", p, skip_identical=False)
    assert s3.puts == []


def test_the_self_hosted_store_skips_and_writes_the_same_way(tmp_path):
    """SelfhostBlob.put_gzip_file / head_meta: the same bytes, encoding, metadata and skip rule."""
    from updater import blob
    sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
    from blobstore import BlobStore
    BlobStore(str(tmp_path / "blobs"), create=True)
    sb = blob.SelfhostBlob(root=str(tmp_path / "blobs"))
    p = _file(tmp_path)
    key = "series/x%3A1.csv"
    assert object_is_identical(sb, key, p) is False, "absent"
    _put_gzip_file_with_backoff(sb, key, p, metadata={"rows": "3"})
    meta = sb.store.head(key)
    assert sb.get(key) == BODY and meta["content_encoding"] == "gzip" and meta["content_type"] == "text/csv"
    assert meta["custom_metadata"] == {"rows": "3"}
    assert sb.head_meta(key)["ContentLength"] == len(BODY)
    before = _SKIPPED_IDENTICAL[0]
    _put_gzip_file_with_backoff(sb, key, p)
    assert _SKIPPED_IDENTICAL[0] == before + 1, "identical bytes are not written again"
