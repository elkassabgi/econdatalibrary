"""The skip guard must recognise a CSV that a DIFFERENT compressor wrote.

WHY BYTE-IDENTITY WAS THE WRONG INVARIANT. The guard compared the MD5 of the COMPRESSED bytes
against the object ETag. That only works when both sides ran the same compressor, and they do
not: the desktop's Python 3.14 links zlib-ng ("1.3.1.zlib-ng"), the 3.11 runners link stock
zlib 1.3.1, and the same CSV at level 9 deflates to 787,922 bytes on one and 788,191 on the
other. Measured on 90 real objects written since the 2026-08-18 gzip cutover, each population
was reproducible only by the compressor that made it - 45/45 OS=3 objects rebuilt on 3.11 and
23/45 on 3.14. About a quarter of writes could never be skipped, forever, and no gzip-header
normalisation reaches that.

Normalising the header was still worth doing - it made this repo's own two writers agree - but
it is a prerequisite, not the fix. The fix is to compare the digest of the CSV BEFORE
compression, carried in object metadata: the same number on every machine, Python and zlib.

R383 reached the same conclusion for parquet and is quoted in
`core/derive_csv.py:content_fingerprint_sql`: byte hashes were rejected because the desktop and
CI run different pyarrow versions. This is that lesson arriving through the gzip door.

THE TEST THAT MATTERED AND DID NOT EXIST. `test_gzip_bytes_deterministic` compares two streams
produced by the SAME interpreter linking the SAME zlib, so it is structurally incapable of
seeing cross-compressor divergence - its own assertion message claims to rule out the very
thing it cannot observe. Here the "other machine" is simulated with `zlib.compress(wbits=31)`,
which on this box produces a different OS byte AND is a genuinely separate code path.
"""
from __future__ import annotations

import hashlib
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.r2_util import gzip_bytes            # noqa: E402
from updater import blob as blob_mod           # noqa: E402

CSV = b"series_id,obs_date,value\nsrc:AAA,2020-12-31,1.5\nsrc:AAA,2021-12-31,2.5\n"
KEY = "series/src%3AAAA.csv"


class FakeClient:
    """Minimal S3 stand-in. `stored` is (body, metadata)."""

    def __init__(self, stored=None):
        self.stored = dict(stored or {})
        self.puts = []

    def head_object(self, Bucket, Key):        # noqa: N803
        if Key not in self.stored:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        body, meta = self.stored[Key]
        return {"ContentLength": len(body), "ETag": '"%s"' % hashlib.md5(body).hexdigest(),
                "Metadata": dict(meta)}

    def put_object(self, Bucket, Key, Body, **kw):   # noqa: N803
        self.puts.append((Key, Body, kw))
        self.stored[Key] = (Body, kw.get("Metadata", {}))


def _blob(stored=None):
    b = blob_mod.R2Blob("econ-data")
    b._client = FakeClient(stored)
    return b


def _plain_md5(data):
    return hashlib.md5(data).hexdigest()


def test_put_records_the_pre_compression_digest():
    b = _blob()
    b.put_atomic(KEY, CSV)
    key, body, kw = b._client.puts[0]
    assert kw["ContentEncoding"] == "gzip"
    assert kw["Metadata"] == {"csvmd5": _plain_md5(CSV)}
    assert body != CSV, "the body should be compressed"


def test_it_skips_an_object_a_DIFFERENT_compressor_wrote():
    """The whole point. Same CSV, foreign gzip bytes, matching plain digest -> skip."""
    foreign = zlib.compress(CSV, 9, 31)          # a different code path from gzip_bytes
    assert foreign != gzip_bytes(CSV), (
        "this box produced identical bytes both ways, so the test cannot simulate the split; "
        "pick another stand-in for the second compressor")
    b = _blob({KEY: (foreign, {"csvmd5": _plain_md5(CSV)})})
    before = blob_mod.SKIPPED_IDENTICAL[0]
    b.put_atomic(KEY, CSV)
    assert not b._client.puts, "it re-uploaded a CSV R2 already holds"
    assert blob_mod.SKIPPED_IDENTICAL[0] == before + 1


def test_it_uploads_when_the_content_changed_even_if_the_bytes_look_familiar():
    changed = CSV + b"src:AAA,2022-12-31,3.5\n"
    b = _blob({KEY: (gzip_bytes(CSV), {"csvmd5": _plain_md5(CSV)})})
    b.put_atomic(KEY, changed)
    assert len(b._client.puts) == 1, "a genuine change must be uploaded"
    assert b._client.puts[0][2]["Metadata"] == {"csvmd5": _plain_md5(changed)}


def test_metadata_wins_over_a_coincidentally_matching_etag():
    """If the two disagree, the digest of the actual CSV is the one that is right."""
    stored_body = gzip_bytes(CSV)
    b = _blob({KEY: (stored_body, {"csvmd5": "0" * 32})})   # metadata says: different CSV
    b.put_atomic(KEY, CSV)
    assert len(b._client.puts) == 1, (
        "the ETag matched by construction, but the recorded plain digest says the CSV differs "
        "- the guard must believe the digest")


def test_it_falls_back_to_the_etag_for_legacy_objects():
    """Objects written before the metadata existed must still be skippable."""
    b = _blob({KEY: (gzip_bytes(CSV), {})})
    before = blob_mod.SKIPPED_IDENTICAL[0]
    b.put_atomic(KEY, CSV)
    assert not b._client.puts, "a legacy object with identical bytes should still skip"
    assert blob_mod.SKIPPED_IDENTICAL[0] == before + 1


def test_a_legacy_object_from_the_other_compressor_uploads_once():
    """The converge-don't-stampede case: no metadata AND foreign bytes -> one upload."""
    b = _blob({KEY: (zlib.compress(CSV, 9, 31), {})})
    b.put_atomic(KEY, CSV)
    assert len(b._client.puts) == 1
    assert b._client.puts[0][2]["Metadata"] == {"csvmd5": _plain_md5(CSV)}, (
        "the upload must stamp the digest, or it converges on nothing")


def test_metadata_key_case_does_not_break_the_comparison():
    """boto3 lowercases metadata keys on the way out; S3 does not promise the case."""
    b = _blob({KEY: (zlib.compress(CSV, 9, 31), {"CsvMd5": _plain_md5(CSV)})})
    b.put_atomic(KEY, CSV)
    assert not b._client.puts, "a case difference in the metadata key must not force a re-upload"


def test_non_series_keys_are_untouched():
    """Manifests and state files must not gain metadata or be skipped."""
    b = _blob()
    b.put_atomic("state/manifest.json", b'{"a":1}')
    key, body, kw = b._client.puts[0]
    assert body == b'{"a":1}'
    assert "Metadata" not in kw
    assert "ContentEncoding" not in kw


# ---------------------------------------------------------------------------
# The shared writer helpers, which the tools under tools/ are meant to adopt.
# ---------------------------------------------------------------------------

def test_an_already_gzipped_body_is_not_gzipped_again():
    """R560: about 188 objects were shipped double-gzipped and served as text/csv.

    Some producers compress at enqueue and hand the compressed body straight to the writer.
    `tools/derive_statcan_tables.py:285` carries this magic-byte check privately; the SHARED
    helper must not be the one place that lacks it.
    """
    from core.r2_util import series_csv_put_args
    already = gzip_bytes(CSV)
    body, kw, digest = series_csv_put_args(already)
    assert body == already, "the body was re-compressed"
    assert kw["ContentEncoding"] == "gzip"
    assert digest is None, (
        "a digest of already-compressed bytes is not comparable to one taken before "
        "compression, so the helper must decline to offer one")
    assert "Metadata" not in kw


def test_put_series_csv_skips_what_r2_already_holds():
    from core.r2_util import put_series_csv
    c = FakeClient({KEY: (gzip_bytes(CSV), {"csvmd5": _plain_md5(CSV)})})
    assert put_series_csv(c, "econ-data", KEY, CSV) == "skipped"
    assert not c.puts


def test_put_series_csv_writes_and_stamps_the_digest():
    from core.r2_util import put_series_csv
    c = FakeClient()
    assert put_series_csv(c, "econ-data", KEY, CSV) == "put"
    key, body, kw = c.puts[0]
    assert kw["Metadata"] == {"csvmd5": _plain_md5(CSV)}
    assert kw["ContentEncoding"] == "gzip"


def test_put_series_csv_retries_a_transient_throttle():
    """boto's 5 retries are NOT enough on their own.

    updater/derive.py:50-57 records the reason: R2 throws ServiceUnavailable/SlowDown throttles
    that outlast them, and one killed the 2026-07-02 bulk run at 103k objects. Four of the nine
    tools carry a private 6-or-7-try loop for this; a shared helper without one would silently
    demote them while looking like consolidation.
    """
    from core.r2_util import put_series_csv

    class Flaky(FakeClient):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def put_object(self, Bucket, Key, Body, **kw):      # noqa: N803
            self.attempts += 1
            if self.attempts < 3:
                raise RuntimeError("SlowDown")
            return super().put_object(Bucket=Bucket, Key=Key, Body=Body, **kw)

    c = Flaky()
    import unittest.mock as mock
    with mock.patch("time.sleep", lambda *_a, **_k: None):
        assert put_series_csv(c, "econ-data", KEY, CSV) == "put"
    assert c.attempts == 3
    assert len(c.puts) == 1


def test_put_series_csv_gives_up_loudly_rather_than_silently():
    from core.r2_util import put_series_csv, PUT_TRIES

    class Dead(FakeClient):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def put_object(self, Bucket, Key, Body, **kw):      # noqa: N803
            self.attempts += 1
            raise RuntimeError("SlowDown")

    c = Dead()
    import pytest
    import unittest.mock as mock
    with mock.patch("time.sleep", lambda *_a, **_k: None):
        with pytest.raises(RuntimeError):
            put_series_csv(c, "econ-data", KEY, CSV)
    assert c.attempts == PUT_TRIES, "an exhausted PUT must raise, never return quietly"
