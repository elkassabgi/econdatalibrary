"""SelfhostBlob.put_if (AR-339 Part 2 finding 2): after T0 the withheld list (_aqueduct/withheld.json) and the
operator files are written to the LOCAL store, and both their writers - tools/withheld_list.py and the fetcher
that un-hides a completed table - need a compare-and-swap. R2Blob had one; the self-hosted twin had none.

The condition is checked inside the blob store's own write transaction (BlobStore.put), so it holds against a
writer in another process too; the race test below drives two stores on one root to show it."""
import hashlib
import os
import sys

import pytest

from updater import blob

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from blobstore import BlobStore  # noqa: E402

KEY = "_aqueduct/withheld.json"


@pytest.fixture
def sb(tmp_path):
    BlobStore(str(tmp_path / "blobs"), create=True)
    return blob.SelfhostBlob(root=str(tmp_path / "blobs"))


def test_if_none_match_creates_once(sb):
    e = sb.put_if(KEY, b'{"version":1,"ids":{}}', if_none_match=True)
    assert e == hashlib.md5(b'{"version":1,"ids":{}}').hexdigest() == sb.etag(KEY)
    assert sb.put_if(KEY, b"other", if_none_match=True) is None, "an existing object was overwritten"
    assert sb.get(KEY) == b'{"version":1,"ids":{}}'


def test_if_match_replaces_only_the_named_version(sb):
    e1 = sb.put_if(KEY, b"v1", if_none_match=True)
    e2 = sb.put_if(KEY, b"v2", if_match=e1)
    assert e2 and sb.get(KEY) == b"v2"
    assert sb.put_if(KEY, b"v3", if_match=e1) is None, "a stale etag replaced a newer object"
    assert sb.get(KEY) == b"v2"
    assert sb.put_if(KEY, b"v3", if_match=f'"{e2}"') and sb.get(KEY) == b"v3", "a quoted etag (R2's form) must work"


def test_if_match_on_an_absent_key_writes_nothing(sb):
    assert sb.put_if(KEY, b"v1", if_match="0" * 32) is None
    assert sb.get(KEY) is None


def test_exactly_one_condition(sb):
    with pytest.raises(ValueError):
        sb.put_if(KEY, b"x")
    with pytest.raises(ValueError):
        sb.put_if(KEY, b"x", if_match="a", if_none_match=True)
    with pytest.raises(ValueError):
        sb.put_if(KEY, b"x", if_match='""')


def test_the_content_type_rule_and_no_gzip(sb):
    sb.put_if(KEY, b'{"a": 1}', if_none_match=True)
    meta = sb.store.head(KEY)
    assert meta["content_type"] == "application/json" and not meta["content_encoding"]


def test_two_writers_on_one_store_cannot_both_win(tmp_path):
    """Two BlobStore connections on ONE root (two processes' view): both read the same etag, both try to
    replace it; exactly one write lands."""
    root = str(tmp_path / "blobs")
    BlobStore(root, create=True)
    a, b = blob.SelfhostBlob(root=root), blob.SelfhostBlob(root=root)
    e0 = a.put_if(KEY, b"v0", if_none_match=True)
    ra = a.put_if(KEY, b"from-a", if_match=e0)
    rb = b.put_if(KEY, b"from-b", if_match=e0)
    assert (ra is None) != (rb is None), (ra, rb)
    assert a.get(KEY) in (b"from-a", b"from-b") and b.get(KEY) == a.get(KEY)


def test_a_failed_condition_leaves_no_orphan_file(sb):
    e1 = sb.put_if(KEY, b"v1", if_none_match=True)
    before = sum(len(f) for _, _, f in os.walk(os.path.join(sb.root, "objects")))
    assert sb.put_if(KEY, b"brand-new-bytes", if_match="f" * 32) is None
    after = sum(len(f) for _, _, f in os.walk(os.path.join(sb.root, "objects")))
    assert after == before and sb.etag(KEY) == e1
