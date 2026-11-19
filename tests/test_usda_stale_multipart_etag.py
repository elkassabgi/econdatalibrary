"""tools/usda_r2_stale_parts.py's BYTE PROOF on multipart ETags (follow-up to #303).

R2Blob.put_file calls boto3 upload_file with no TransferConfig, so any part at or over the 8 MiB
multipart threshold is uploaded in 8,388,608-byte chunks and R2 reports the S3 multipart ETag:
md5(concatenated chunk md5 digests) + "-<chunk count>". byte_mismatches() must accept exactly that
form for the local bytes, and reject the same bytes summarised with any other chunk size - an
ETag it cannot reproduce is a mismatch, never a pass. The expected ETags here are computed by this
file's own helper, not by the tool's, so a wrong chunk size in the tool cannot agree with itself.
No network: R2 is a fake.
"""
from __future__ import annotations

import hashlib
import os
import random
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import ingest_usda as J                     # noqa: E402
from updater import writer_lock                       # noqa: E402

MiB = 1024 * 1024
BOTO3_CHUNK = 8 * MiB                                  # 8,388,608: TransferConfig's multipart_chunksize
KEY0 = "clean_full/usda/demographics/part_000.parquet"
KEY1 = "clean_full/usda/demographics/part_001.parquet"


def s3_etag(data: bytes, chunk: int) -> str:
    """The S3/R2 multipart ETag of `data` uploaded in `chunk`-byte parts (independent of the tool)."""
    digests = [hashlib.md5(data[i:i + chunk]).digest() for i in range(0, len(data), chunk)]
    return f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(digests)}"


class FakeR2:
    objects: dict = {}                                 # key -> (bytes, etag)

    def __init__(self, *a, **k):
        self.bucket = "fake"
        self.client = self

    def list_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def exists(self, key):
        return key in self.objects

    def size(self, key):
        return len(self.objects[key][0]) if key in self.objects else None

    def etag(self, key):
        return self.objects[key][1] if key in self.objects else None

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


@pytest.fixture
def env(tmp_path, monkeypatch):
    from updater import blob
    from core import cutover
    out = tmp_path / "data" / "clean_full" / "usda"
    (out / "demographics").mkdir(parents=True)
    monkeypatch.setattr(J, "OUT", str(out))
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    FakeR2.objects = {}
    monkeypatch.setattr(blob, "R2Blob", FakeR2)
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "usda_r2_stale_parts", os.path.join(ROOT, "tools", "usda_r2_stale_parts.py"))
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    return tool, out


def _local(out, name, nbytes, seed=1):
    data = random.Random(seed).randbytes(nbytes)
    p = out / "demographics" / name
    p.write_bytes(data)
    return data


BIG = 9 * MiB + 12_345                                 # two boto3 chunks: 8 MiB + the rest


def test_a_boto3_multipart_etag_for_a_file_over_8_mib_is_accepted(env):
    tool, out = env
    data = _local(out, "part_000.parquet", BIG)
    FakeR2.objects[KEY0] = (data, s3_etag(data, BOTO3_CHUNK))
    assert FakeR2.objects[KEY0][1].endswith("-2")
    assert tool.byte_mismatches({KEY0}) == []


@pytest.mark.parametrize("chunk", [5 * MiB, 16 * MiB, 8 * 1000 * 1000])
def test_the_same_bytes_with_another_chunk_size_are_a_mismatch(env, chunk):
    tool, out = env
    data = _local(out, "part_000.parquet", BIG)
    FakeR2.objects[KEY0] = (data, s3_etag(data, chunk))
    bad = tool.byte_mismatches({KEY0})
    assert len(bad) == 1 and "ETag" in bad[0], bad


def test_an_exactly_8_mib_file_is_one_part_and_only_the_minus_1_form_or_plain_md5_pass(env):
    tool, out = env
    data = _local(out, "part_000.parquet", BOTO3_CHUNK)
    one = s3_etag(data, BOTO3_CHUNK)
    assert one.endswith("-1")                          # at the threshold boto3 uploads ONE part
    FakeR2.objects[KEY0] = (data, one)
    assert tool.byte_mismatches({KEY0}) == []
    FakeR2.objects[KEY0] = (data, hashlib.md5(data).hexdigest())     # a single PUT of the same bytes
    assert tool.byte_mismatches({KEY0}) == []
    FakeR2.objects[KEY0] = (data, s3_etag(data, 4 * MiB))            # "-2": not what boto3 produced
    assert len(tool.byte_mismatches({KEY0})) == 1
    FakeR2.objects[KEY0] = (data, hashlib.md5(data).hexdigest() + "-1")   # plain md5 dressed as "-1"
    assert len(tool.byte_mismatches({KEY0})) == 1


def test_one_byte_of_difference_under_a_matching_size_is_a_mismatch(env):
    tool, out = env
    data = _local(out, "part_000.parquet", BIG)
    other = bytearray(data)
    other[BOTO3_CHUNK + 7] ^= 0xFF                     # same size, second chunk differs
    FakeR2.objects[KEY0] = (bytes(other), s3_etag(bytes(other), BOTO3_CHUNK))
    assert len(tool.byte_mismatches({KEY0})) == 1


def test_apply_proves_and_cleans_over_multipart_etags(env, monkeypatch, capsys):
    """End to end: a >8 MiB live part on R2 under its boto3 multipart ETag plus a stale part_001.
    --apply accepts the byte proof, removes only part_001, and prints PROVED."""
    tool, out = env
    data = _local(out, "part_000.parquet", BIG)
    live = str(out / "demographics" / "part_000.parquet")
    monkeypatch.setattr(J, "served_parts", lambda: [live])
    monkeypatch.setattr(J, "verify_dir", lambda d, expect_input=None: (True, "ok", 0))
    FakeR2.objects[KEY0] = (data, s3_etag(data, BOTO3_CHUNK))
    FakeR2.objects[KEY1] = (b"old part", hashlib.md5(b"old part").hexdigest())
    assert tool.main(["--apply"]) == 0
    assert KEY0 in FakeR2.objects and KEY1 not in FakeR2.objects
    assert "PROVED" in capsys.readouterr().out
    assert any(k.startswith("_backup/usda_stale_parts/") and k.endswith(KEY1) for k in FakeR2.objects)


def test_apply_refuses_when_the_multipart_etag_was_made_with_another_chunk_size(env, monkeypatch):
    tool, out = env
    data = _local(out, "part_000.parquet", BIG)
    live = str(out / "demographics" / "part_000.parquet")
    monkeypatch.setattr(J, "served_parts", lambda: [live])
    monkeypatch.setattr(J, "verify_dir", lambda d, expect_input=None: (True, "ok", 0))
    FakeR2.objects[KEY0] = (data, s3_etag(data, 5 * MiB))
    FakeR2.objects[KEY1] = (b"old part", hashlib.md5(b"old part").hexdigest())
    assert tool.main(["--apply"]) == 1
    assert KEY1 in FakeR2.objects, "nothing may be deleted when the byte proof fails"
