"""core/r2_util.py's cutover write guard (plan section 3, change 5). No network is ever touched: a second
handler, registered AFTER the guard on the same event, stops every call that gets past the guard with
Reached - which is how each test tells "refused by the guard" from "would have gone to R2"."""
import io

import pytest

from core import cutover, r2_util


class Reached(Exception):
    """The call got past the guard and would now go to R2."""


def _reached(model=None, **_k):
    raise Reached(getattr(model, "name", "?"))



@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {
        "endpoint": "http://127.0.0.1:9", "key": "k", "secret": "s", "mode": "write" if write else "read"})
    c = r2_util.client(write=True)
    c.meta.events.register("before-call.s3", _reached)
    return c


@pytest.fixture
def cut_over(tmp_path, monkeypatch):
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))


@pytest.fixture
def not_cut_over(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))


WRITES = [
    ("put_object", {"Bucket": "econ-data", "Key": "k", "Body": b"x"}),
    ("copy_object", {"Bucket": "econ-data", "Key": "k", "CopySource": {"Bucket": "econ-data", "Key": "j"}}),
    ("delete_object", {"Bucket": "econ-data", "Key": "k"}),
    ("delete_objects", {"Bucket": "econ-data", "Delete": {"Objects": [{"Key": "k"}]}}),
    ("create_multipart_upload", {"Bucket": "econ-data", "Key": "k"}),
    ("upload_part", {"Bucket": "econ-data", "Key": "k", "PartNumber": 1, "UploadId": "u", "Body": b"x"}),
    ("complete_multipart_upload", {"Bucket": "econ-data", "Key": "k", "UploadId": "u"}),
    ("abort_multipart_upload", {"Bucket": "econ-data", "Key": "k", "UploadId": "u"}),
    ("put_bucket_lifecycle_configuration", {"Bucket": "econ-data", "LifecycleConfiguration": {"Rules": []}}),
    ("delete_bucket", {"Bucket": "econ-data"}),
    ("list_parts", {"Bucket": "econ-data", "Key": "k", "UploadId": "u"}),   # not on the read list: refused
]
READS = [
    ("get_object", {"Bucket": "econ-data", "Key": "k"}),
    ("head_object", {"Bucket": "econ-data", "Key": "k"}),
    ("list_objects_v2", {"Bucket": "econ-data"}),
    ("list_objects", {"Bucket": "econ-data"}),
    ("head_bucket", {"Bucket": "econ-data"}),
]


@pytest.mark.parametrize("op,kw", WRITES)
def test_every_write_is_refused_after_cutover(s3, cut_over, op, kw):
    with pytest.raises(cutover.CutoverRefused):
        getattr(s3, op)(**kw)


@pytest.mark.parametrize("op,kw", READS)
def test_reads_still_reach_the_network_after_cutover(s3, cut_over, op, kw):
    with pytest.raises(Reached):
        getattr(s3, op)(**kw)


@pytest.mark.parametrize("op,kw", WRITES[:3])
def test_before_cutover_writes_reach_the_network(s3, not_cut_over, op, kw):
    with pytest.raises(Reached):
        getattr(s3, op)(**kw)


def test_a_multipart_upload_through_s3transfer_is_refused(s3, cut_over):
    from boto3.s3.transfer import TransferConfig
    cfg = TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024,
                         use_threads=False)
    with pytest.raises(cutover.CutoverRefused):
        s3.upload_fileobj(io.BytesIO(b"x" * (11 * 1024 * 1024)), "econ-data", "big", Config=cfg)


def test_a_client_built_before_t0_is_stopped_at_t0(s3, tmp_path, monkeypatch):
    flag = tmp_path / "CUTOVER"
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    with pytest.raises(Reached):
        s3.put_object(Bucket="econ-data", Key="k", Body=b"x")
    flag.write_text("")                                    # T0 arrives while the process runs
    with pytest.raises(cutover.CutoverRefused):
        s3.put_object(Bucket="econ-data", Key="k", Body=b"x")


def test_an_unreadable_flag_refuses(s3, monkeypatch):
    def stat(_p, *a, **k):
        raise PermissionError(13, "denied")
    monkeypatch.setattr(cutover.os, "stat", stat)
    with pytest.raises(cutover.CutoverRefused):
        s3.put_object(Bucket="econ-data", Key="k", Body=b"x")


def test_the_read_list_is_exactly_the_plan_s(s3):
    """An ALLOW-list: widening it is a decision, so it is pinned (plan section 3, change 5)."""
    assert r2_util.READ_OPERATIONS == {"GetObject", "HeadObject", "ListObjects", "ListObjectsV2", "HeadBucket"}
    assert r2_util.guard_client(s3) is s3 and s3._econ_cutover_guard is True
