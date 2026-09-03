"""The DAILY publish path does not re-upload bytes R2 already holds.

I shipped this guard in `core/derive_csv._put_gzip_file_with_backoff` first and told Ahmed it
would take the storage-operations line from $30.52 toward $4. Then I read the daily path:
`updater/derive.py` calls `blob.put_atomic(key, body)` and never touches that function. The
guard covered the manual publish only, and the ~320,000 uploads a day went straight past it.

`R2Blob.put_atomic` is the choke point. It already gzips series CSVs with `mtime=0` "for the
verifier's byte-compare", so the same data gives the same bytes and the same MD5.

SCOPED TO series/*.csv ON PURPOSE, and the tests pin that scope as hard as the skip: manifests
and state files pass through here too, and something may read their LastModified as a freshness
signal. Skipping one of those would leave a stale timestamp reading as "not updated".
"""
from __future__ import annotations

import gzip
import hashlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.blob import SKIPPED_IDENTICAL, R2Blob  # noqa: E402

CSV = b"series_id,obs_date,value\na,2020-01-01,1.0\n"
KEY = "series/probe%3Aone.csv"


class FakeClient:
    def __init__(self, etag=None, raises=False):
        self._etag, self._raises = etag, raises
        self.puts = []
        self.heads = 0

    def head_object(self, **kw):
        self.heads += 1
        if self._raises or self._etag is None:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ETag": f'"{self._etag}"'}

    def put_object(self, **kw):
        self.puts.append(kw)


def blob_with(client):
    b = R2Blob.__new__(R2Blob)
    b.bucket = "econ-data"
    b._client = client
    return b


def gz_md5(data: bytes) -> str:
    return hashlib.md5(gzip.compress(data, mtime=0)).hexdigest()      # noqa: S324


def test_an_identical_series_csv_is_NOT_uploaded():
    c = FakeClient(etag=gz_md5(CSV))
    before = SKIPPED_IDENTICAL[0]
    blob_with(c).put_atomic(KEY, CSV)
    assert c.puts == [], "identical bytes were uploaded again"
    assert SKIPPED_IDENTICAL[0] == before + 1, "the saving was not counted"


def test_changed_content_IS_uploaded():
    c = FakeClient(etag=gz_md5(b"different bytes entirely"))
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1
    assert c.puts[0]["ContentEncoding"] == "gzip"


def test_a_missing_object_IS_uploaded():
    c = FakeClient(etag=None)
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1, "a first upload was skipped"


def test_a_multipart_etag_IS_uploaded():
    """`<hex>-<n>` is a digest of part digests. Comparing it to a whole-object MD5 would be
    meaningless, so it must upload rather than guess."""
    c = FakeClient(etag=gz_md5(CSV) + "-4")
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1


def test_a_head_that_ERRORS_still_uploads():
    c = FakeClient(raises=True)
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1, "an unanswerable head suppressed a needed upload"


def test_a_NON_csv_key_is_never_checked():
    """Manifests and state files come through here too. Something may read their LastModified
    as a freshness signal, so skipping one would leave a stale timestamp reading as
    'not updated'. The cost is entirely in the CSVs, so the guard goes only there."""
    c = FakeClient(etag=hashlib.md5(b'{"a": 1}').hexdigest())          # noqa: S324
    blob_with(c).put_atomic("_aqueduct/manifest.json", b'{"a": 1}')
    assert len(c.puts) == 1, "a manifest write was skipped"
    assert c.heads == 0, "a non-CSV key was needlessly HEADed"


def test_a_csv_outside_the_series_prefix_is_not_checked():
    c = FakeClient(etag=gz_md5(CSV))
    blob_with(c).put_atomic("exports/other.csv", CSV)
    assert len(c.puts) == 1
    assert c.heads == 0


def test_the_comparison_is_against_the_GZIPPED_bytes():
    """put_atomic gzips before writing, so the ETag corresponds to the compressed object. A
    check against the raw bytes would never match and would save nothing at all."""
    c = FakeClient(etag=hashlib.md5(CSV).hexdigest())                 # noqa: S324  raw, not gz
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1, "the guard compared raw bytes and skipped wrongly"


class ErroringClient(FakeClient):
    """head_object fails with something that is NOT a 404, so `etag()` re-raises it."""

    def head_object(self, **kw):
        self.heads += 1
        from botocore.exceptions import ClientError
        raise ClientError({"Error": {"Code": "InternalError"}}, "HeadObject")


def test_a_NON_404_head_failure_still_uploads():
    """`R2Blob.etag` swallows a 404 and RE-RAISES anything else, so a throttle or an outage
    reaches `_already_holds`'s own except. That branch must fall toward uploading.

    My first version of the error test used a 404, which `etag()` absorbs - so it never
    reached this branch at all, and a mutation making the except return True survived the
    whole suite. Caught by the mutation sweep, not by reading."""
    c = ErroringClient()
    before = SKIPPED_IDENTICAL[0]
    blob_with(c).put_atomic(KEY, CSV)
    assert len(c.puts) == 1, "an unanswerable head suppressed a needed upload"
    assert SKIPPED_IDENTICAL[0] == before, "a failed head was counted as a saving"
