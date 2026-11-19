"""Regression gate: a footer read under the R2 backend fetches the footer, never the object.

WHY. blob.row_count (and read_metadata / read_schema) used to r2.get() the WHOLE object to parse a
few KB of parquet footer. unctad_biotrademerch's store file is 18,834,328,493 bytes: ~9 minutes on
every pass to learn one integer, and the 08-30 run died in botocore's ResponseStreamingError at
17.3 GB. They now read the tail with ranged GETs (blob._r2_footer_buffer).

The fake client below records every get_object call and its Range header; nothing touches the
network or a local server. The pins:
  * a large object -> every request is a ranged tail request, none is a full GET, and the bytes
    moved are a small fraction of the object; num_rows equals pyarrow's on the whole file;
  * a footer larger than the first window -> exactly one more range, conditional on the ETag;
  * the parsed FileMetaData and schema equal pyarrow's on the whole file;
  * a missing object -> 0 / FileNotFoundError, as before; bad magic and a sub-8-byte object -> 0
    from row_count and the SAME pyarrow error type read_metadata raised on the whole object;
  * the object replaced between the two GETs (412) -> the read starts over and stays consistent.
Mutant: point row_count back at r2.get() and test_large_object_reads_only_the_tail fails.
"""
from __future__ import annotations
import io
import os
import re
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater import blob   # noqa: E402

KEY = "clean_full/fake_src/fake_src.parquet"
PATH = "/x/data/" + KEY


def _err(code, status):
    return ClientError({"Error": {"Code": code, "Message": code},
                        "ResponseMetadata": {"HTTPStatusCode": status}}, "GetObject")


class _Body:
    def __init__(self, data):
        self._b = io.BytesIO(data)

    def read(self, *a):
        return self._b.read(*a)


class FakeS3:
    """get_object with S3 Range semantics (suffix and absolute), IfMatch, 404 and 416."""

    def __init__(self, objects):
        self.objects = dict(objects)
        self.calls = []                 # (key, Range or None, IfMatch or None)
        self.bytes_sent = 0
        self.replace_after_first = None  # bytes: swap the object in after the first call

    def get_object(self, Bucket, Key, Range=None, IfMatch=None):
        self.calls.append((Key, Range, IfMatch))
        if Key not in self.objects:
            raise _err("NoSuchKey", 404)
        data = self.objects[Key]
        etag = '"%08x"' % (hash(data) & 0xFFFFFFFF)
        if self.replace_after_first is not None and len(self.calls) == 1:
            self.objects[Key], self.replace_after_first = self.replace_after_first, None
        if IfMatch is not None and IfMatch != '"%08x"' % (hash(self.objects[Key]) & 0xFFFFFFFF):
            raise _err("PreconditionFailed", 412)
        n = len(data)
        if Range is None:
            self.bytes_sent += n
            return {"Body": _Body(data), "ContentLength": n, "ETag": etag}
        m = re.fullmatch(r"bytes=(\d*)-(\d*)", Range)
        assert m, Range
        a, b = m.group(1), m.group(2)
        if a == "":                                  # suffix: the last int(b) bytes
            if n == 0:
                raise _err("InvalidRange", 416)
            start, end = max(0, n - int(b)), n - 1
        else:
            start, end = int(a), min(int(b) if b else n - 1, n - 1)
        chunk = data[start:end + 1]
        self.bytes_sent += len(chunk)
        return {"Body": _Body(chunk), "ContentLength": len(chunk), "ETag": etag,
                "ContentRange": f"bytes {start}-{end}/{n}"}


def _r2(monkeypatch, objects):
    fake = FakeS3(objects)
    r2 = blob.R2Blob()
    r2._client = fake
    monkeypatch.setattr(blob, "_r2_routed", lambda: r2)
    return fake


def _parquet(rows, *, row_group_size=None, cols=1, seed=0):
    import random
    rnd = random.Random(seed)
    data = {f"c{i}": pa.array([rnd.random() for _ in range(rows)], pa.float64()) for i in range(cols)}
    data["series_key"] = pa.array([f"k{rnd.getrandbits(48):x}" for _ in range(rows)], pa.string())
    buf = io.BytesIO()
    # uncompressed random doubles: the data pages are large, so "the object" is genuinely big
    pq.write_table(pa.table(data), buf, compression="none", row_group_size=row_group_size)
    return buf.getvalue()


def _no_full_get(fake):
    full = [c for c in fake.calls if c[1] is None]
    assert not full, f"a full-object GET was made: {full}"
    for _k, rng, _m in fake.calls:
        assert rng.startswith("bytes="), rng


def test_large_object_reads_only_the_tail(monkeypatch):
    data = _parquet(400_000, cols=2)
    assert len(data) > 8 * blob._FOOTER_TAIL_BYTES          # the fixture really is "large"
    fake = _r2(monkeypatch, {KEY: data})
    assert blob.row_count(PATH) == pq.read_metadata(io.BytesIO(data)).num_rows == 400_000
    _no_full_get(fake)
    assert len(fake.calls) == 1, fake.calls
    assert fake.calls[0][1] == f"bytes=-{blob._FOOTER_TAIL_BYTES}"
    assert fake.bytes_sent <= blob._FOOTER_TAIL_BYTES < len(data) // 8


def test_footer_bigger_than_the_window_costs_one_more_range(monkeypatch):
    data = _parquet(60_000, row_group_size=500, cols=3)     # 120 row groups -> a ~55 KB footer
    flen = int.from_bytes(data[-8:-4], "little")
    monkeypatch.setattr(blob, "_FOOTER_TAIL_BYTES", 4096)
    assert flen + 8 > 4096 and len(data) > 4 * (flen + 8)
    fake = _r2(monkeypatch, {KEY: data})
    assert blob.row_count(PATH) == 60_000
    _no_full_get(fake)
    assert len(fake.calls) == 2, fake.calls
    n = len(data)
    assert fake.calls[1][1] == f"bytes={n - flen - 8}-{n - 1}"
    assert fake.calls[1][2], "the second range must be conditional on the first GET's ETag"


def test_metadata_and_schema_equal_the_whole_file(monkeypatch):
    data = _parquet(50_000, row_group_size=7_000, cols=2)
    monkeypatch.setattr(blob, "_FOOTER_TAIL_BYTES", 2048)
    whole = pq.read_metadata(io.BytesIO(data))
    fake = _r2(monkeypatch, {KEY: data})
    md = blob.read_metadata(PATH)
    assert md.num_rows == whole.num_rows and md.num_row_groups == whole.num_row_groups
    assert md.to_dict() == whole.to_dict()                   # row-group stats/offsets included
    assert blob.read_schema(PATH) == pq.read_schema(io.BytesIO(data))
    _no_full_get(fake)


def test_small_object_whole_in_the_first_window(monkeypatch):
    data = _parquet(10)
    assert len(data) < blob._FOOTER_TAIL_BYTES
    fake = _r2(monkeypatch, {KEY: data})
    assert blob.row_count(PATH) == 10
    assert len(fake.calls) == 1 and fake.calls[0][1].startswith("bytes=-")


def test_missing_object(monkeypatch):
    fake = _r2(monkeypatch, {})
    assert blob.row_count(PATH) == 0
    with pytest.raises(FileNotFoundError):
        blob.read_metadata(PATH)
    with pytest.raises(FileNotFoundError):
        blob.read_schema(PATH)
    _no_full_get(fake)


def _whole_file_error(data):
    with pytest.raises(Exception) as ei:
        pq.read_metadata(io.BytesIO(data))
    return type(ei.value)


_CORRUPT = [
    ("bad magic, large", b"\x00" * (3 << 20)),
    ("bad magic, plausible length", b"\x00" * (3 << 20) + (2 << 20).to_bytes(4, "little") + b"XXXX"),
    ("bad magic, small", b"not a parquet file at all"),
    ("under 8 bytes", b"PAR1"),
    ("empty", b""),
    ("footer length beyond the file", b"PAR1" + b"\x00" * (3 << 20) + (10 ** 9).to_bytes(4, "little") + b"PAR1"),
]


@pytest.mark.parametrize("label,data", _CORRUPT, ids=[c[0] for c in _CORRUPT])
def test_corrupt_objects_keep_todays_answers(monkeypatch, label, data):
    expect = _whole_file_error(data)
    fake = _r2(monkeypatch, {KEY: data})
    assert blob.row_count(PATH) == 0, label                  # today: any parse failure counts 0
    with pytest.raises(expect):
        blob.read_metadata(PATH)                             # same error TYPE as on the whole object
    _no_full_get(fake)
    # a trailer that is not parquet's is refused on the FIRST tail: no second range is chased
    assert len(fake.calls) == 2, fake.calls


def test_object_replaced_between_the_two_ranges_restarts(monkeypatch):
    old = _parquet(60_000, row_group_size=500, cols=1, seed=1)
    new = _parquet(40_000, row_group_size=500, cols=1, seed=2)
    monkeypatch.setattr(blob, "_FOOTER_TAIL_BYTES", 4096)
    fake = _r2(monkeypatch, {KEY: old})
    fake.replace_after_first = new
    assert blob.row_count(PATH) == 40_000                   # never a splice of old tail + new footer
    _no_full_get(fake)
    assert any(c[2] for c in fake.calls)


class _Boom(FakeS3):
    def get_object(self, **kw):
        raise _err("InternalError", 500)


def test_transport_errors_still_raise_not_zero(monkeypatch):
    # The GETs run OUTSIDE row_count's `except Exception: return 0`: a failed read must not become
    # before=0 in a fetcher (that would disable merge's never-shrink guard).
    r2 = blob.R2Blob()
    r2._client = _Boom({})
    monkeypatch.setattr(blob, "_r2_routed", lambda: r2)
    for fn in (blob.row_count, blob.read_metadata, blob.read_schema):
        with pytest.raises(ClientError):
            fn(PATH)


def test_a_blob_without_get_range_raises_not_zero(monkeypatch):
    class OnlyGet:                                         # an old-style fake: get() alone
        def get(self, key):
            return _parquet(5)
    monkeypatch.setattr(blob, "_r2_routed", lambda: OnlyGet())
    with pytest.raises(AttributeError):
        blob.row_count(PATH)


def test_bytes_range_twin():
    d = bytes(range(100))
    assert blob.bytes_range(d, "bytes=-10") == (d[-10:], 100, None)
    assert blob.bytes_range(d, "bytes=-1000") == (d, 100, None)
    assert blob.bytes_range(d, "bytes=5-9") == (d[5:10], 100, None)
    assert blob.bytes_range(b"", "bytes=-10") == (b"", 0, None)


def test_localblob_get_range_parity(tmp_path, monkeypatch):
    # LocalBlob/SelfhostBlob carry get_range with the same contract, so the footer read works
    # against any Blob, not only R2Blob.
    data = _parquet(60_000, row_group_size=500, cols=3)
    p = tmp_path / "x.parquet"
    p.write_bytes(data)
    monkeypatch.setattr(blob, "_FOOTER_TAIL_BYTES", 4096)
    lb = blob.LocalBlob()
    buf = blob._r2_footer_buffer(lb, str(p))
    assert pq.read_metadata(io.BytesIO(buf)).to_dict() == pq.read_metadata(io.BytesIO(data)).to_dict()
    assert lb.get_range(str(tmp_path / "nope"), "bytes=-10") is None
    body, total, _ = lb.get_range(str(p), "bytes=-10")
    assert body == data[-10:] and total == len(data)
    assert blob.SelfhostBlob.get_range is not blob.R2Blob.get_range


def test_local_backend_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(blob, "_r2_routed", lambda: None)
    p = tmp_path / "data" / "x.parquet"
    p.parent.mkdir()
    p.write_bytes(_parquet(123))
    assert blob.row_count(str(p)) == 123
    assert blob.row_count(str(tmp_path / "missing.parquet")) == 0
    (tmp_path / "bad.parquet").write_bytes(b"junk")
    assert blob.row_count(str(tmp_path / "bad.parquet")) == 0
