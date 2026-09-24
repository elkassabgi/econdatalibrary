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


@pytest.fixture
def cloud(monkeypatch):
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {
        "endpoint": "http://127.0.0.1:9", "key": "k", "secret": "s", "mode": "read"})
    c = r2_util.cloud_client()
    c.meta.events.register("before-call.s3", _reached)
    return c


@pytest.mark.parametrize("op,kw", READS)
def test_after_cutover_r2_util_client_refuses_reads_too(s3, cut_over, op, kw):
    """Design review 2026-09-24: a read of the frozen copy is stale data presented as current."""
    with pytest.raises(cutover.CutoverRefused, match="self-hosted"):
        getattr(s3, op)(**kw)


@pytest.mark.parametrize("op,kw", READS)
def test_before_cutover_r2_util_client_reads(s3, not_cut_over, op, kw):
    with pytest.raises(Reached):
        getattr(s3, op)(**kw)


@pytest.mark.parametrize("op,kw", READS)
def test_the_cloud_client_still_reads_after_cutover(cloud, cut_over, op, kw):
    with pytest.raises(Reached):
        getattr(cloud, op)(**kw)


@pytest.mark.parametrize("op,kw", WRITES)
def test_the_cloud_client_never_writes_after_cutover(cloud, cut_over, op, kw):
    with pytest.raises(cutover.CutoverRefused):
        getattr(cloud, op)(**kw)


def test_only_the_named_final_sync_readers_use_the_cloud_client():
    """Plan step 6b's readers. A new caller is a new way to read stale data after T0."""
    import re as _r
    callers = set()
    for rel, p in _walk.code_files((".py",)):
        if _r.search(r"\bcloud_client\(", open(p, encoding="utf-8", errors="replace").read()):
            callers.add(rel)
    callers.discard("core/r2_util.py")
    assert callers == {"tools/footer_diff.py", "tools/mirror_sync.py", "tools/selfhost/import_from_r2.py"}


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
    """An ALLOW-list: widening it is a decision, so it is pinned (plan section 3, change 5). ListBuckets
    was added with AR-151: jobs/r2_bucket_sizes.py reads it, and it writes nothing."""
    assert r2_util.READ_OPERATIONS == {"GetObject", "HeadObject", "ListObjects", "ListObjectsV2", "HeadBucket",
                                       "ListBuckets"}
    assert r2_util.guard_client(s3) is s3 and s3._econ_cutover_guard is True


def test_list_buckets_is_a_read(cloud, cut_over):
    with pytest.raises(Reached):
        cloud.list_buckets()


# ---- the ratchet: no raw boto3 client may skip the guard (AR-151 finding 1) ---------------------------
import re as _re

import _repo_walk as _walk                              # the one shared walk (R1178)

# The ways to get an S3 client without r2_util found SO FAR (R1178, R1179 each measured more getting
# through) - a list, not a proof: a new spelling found later goes here with a planted positive below.
# boto3.client / boto3.resource count WITHOUT a call too: `f = boto3.client` is an alias (R1179).
_RAW = _re.compile(r"\bboto3\.(client|resource)\b|boto3\.session\.Session\s*\(|\bSession\([^)]*\)\s*\.\s*(client|resource)\s*\("
                   r"|\bboto3\.Session\b|from\s+boto3(\.session)?\s+import|import\s+boto3\s+as|\.create_client\s*\(|\bs3fs\b")
_GUARDED = _re.compile(r"guard_client\(\s*boto3\.client\(")


def test_every_boto3_client_in_the_repo_is_guarded():
    """Every raw S3 client must be built INSIDE guard_client(...), or come from r2_util.client(). A new
    unguarded client would be a write road to the retired copy that nothing refuses after T0."""
    bad = []
    for rel, p in _walk.code_files((".py",)):
        # code only: now that an alias without a call counts, a comment naming boto3.client must not
        src = _walk.code_text(open(p, encoding="utf-8", errors="replace").read())
        raw = len(_RAW.findall(src))
        guarded = len(_GUARDED.findall(src))
        if rel == "core/r2_util.py":
            guarded += 1                             # client() builds, then guard_client(s3) on the next line
        if raw > guarded:
            bad.append(f"{rel}: {raw} raw client(s), {guarded} guarded")
    assert not bad, "wrap these in core.r2_util.guard_client(...): " + "; ".join(bad)


def test_the_client_ratchet_can_fail():
    assert _RAW.search('s3 = boto3.client("s3", endpoint_url=e)')
    for form in ('boto3.client ("s3")', "boto3.Session().client('s3')", "from boto3 import client",
                 "import boto3 as b3", "botocore.session.get_session().create_client('s3')",
                 "fs = s3fs.S3FileSystem()", "boto3.session.Session() . client('s3')",
                 "f = boto3.client", "from boto3.session import Session"):     # R1179
        assert _RAW.search(form), form                       # R1178: each of these got through before
    assert not _RAW.search("import boto3") and not _RAW.search("r2_util.client(write=True)")
    assert not _RAW.search("from boto3.s3.transfer import TransferConfig"), "a settings class, not a client"
    assert not _RAW.search(_walk.code_text("# see boto3.client below\nx = 1")), "a comment is not code"
    assert _GUARDED.search(_walk.code_text('s3 = guard_client(boto3.client("s3", endpoint_url=e))')), \
        "the guarded form survives the code reader's re-printing"
    assert _GUARDED.search('s3 = guard_client(boto3.client("s3", endpoint_url=e))')
    assert not _GUARDED.search('s3 = boto3.client("s3", endpoint_url=e)')
