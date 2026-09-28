"""The self-hosted origin's blob store and its HTTP sidecar (docs/ECON_SELF_HOSTING_PLAN.md, change 3).

Pins what the worker's SERIES_BUCKET adapter relies on: exact bytes; the R2 etag kept verbatim; a range
answer that still reports the FULL size; If-Match giving 412 with no body on a changed object; 404 for a
missing key; keys that NTFS could not hold as file names; stored gzip sent WITHOUT Content-Encoding; and
a replaced key's old file kept until gc's grace period passes.
"""
import base64
import gzip
import http.client
import json
import os
import sys
import threading
import urllib.parse

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import blob_sidecar  # noqa: E402
from blobstore import BlobStore  # noqa: E402

LONG_KEY = "series/" + urllib.parse.quote("ksh_stadat:" + "X" * 400, safe="") + ".csv"   # > 255 chars
CASE_A, CASE_B = "series/abc%3AX.csv", "series/abc%3Ax.csv"                              # differ by case


@pytest.fixture
def served(tmp_path):
    store = BlobStore(str(tmp_path / "blobs"), create=True)
    srv = blob_sidecar.serve(str(tmp_path / "blobs"), 0)
    port = srv.server_address[1]
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    yield store, port
    srv.shutdown()


def _get(port, key, headers=None, method="GET"):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request(method, "/o/" + urllib.parse.quote(key, safe=""), headers=headers or {})
    r = c.getresponse()
    body = r.read()
    c.close()
    return r.status, {k.lower(): v for k, v in r.getheaders()}, body


def test_a_missing_store_is_an_error_not_an_empty_store(tmp_path):
    with pytest.raises(FileNotFoundError):
        BlobStore(str(tmp_path / "nothing"))
    assert not (tmp_path / "nothing" / "index.db").exists()


def test_bytes_etag_and_metadata_come_back_exactly(served):
    store, port = served
    data = b"series_id,date,value\na,2020-01-01,1\n"
    store.put("series/a.csv", data, etag='"abc123"', content_type="text/csv", custom_metadata={"csvmd5": "x"})
    status, h, body = _get(port, "series/a.csv")
    assert status == 200 and body == data
    assert h["x-blob-etag"] == "abc123" and h["x-blob-size"] == str(len(data))
    assert json.loads(base64.b64decode(h["x-blob-custom-metadata"])) == {"csvmd5": "x"}


def test_stored_gzip_is_sent_without_content_encoding(served):
    store, port = served
    gz = gzip.compress(b"series_id,date,value\n" * 100)
    store.put("series/g.csv", gz, etag="e1", content_encoding="gzip")
    status, h, body = _get(port, "series/g.csv", {"Accept-Encoding": "gzip"})
    assert status == 200 and body == gz, "the gzip bytes, not inflated"
    assert "content-encoding" not in h and h["x-blob-content-encoding"] == "gzip"


def test_a_range_reports_the_full_size(served):
    store, port = served
    data = bytes(range(256)) * 10
    store.put("series/r.csv", data, etag="e2")
    status, h, body = _get(port, "series/r.csv", {"Range": f"bytes={len(data) - 4}-{len(data) - 1}"})
    assert status == 206 and body == data[-4:] and h["x-blob-size"] == str(len(data))


def test_if_match_on_a_changed_object_is_412_with_no_body(served):
    store, port = served
    store.put("series/c.csv", b"v1", etag="old")
    store.put("series/c.csv", b"v2", etag="new")
    status, _h, body = _get(port, "series/c.csv", {"If-Match": '"old"'})
    assert status == 412 and body == b""
    status, _h, body = _get(port, "series/c.csv", {"If-Match": '"new"'})
    assert status == 200 and body == b"v2"


def test_a_missing_key_is_404(served):
    _store, port = served
    assert _get(port, "series/none.csv")[0] == 404


def test_keys_ntfs_cannot_name_are_stored_and_kept_apart(served):
    store, port = served
    store.put(LONG_KEY, b"long", etag="l")
    store.put(CASE_A, b"upper", etag="u")
    store.put(CASE_B, b"lower", etag="w")
    assert len(LONG_KEY) > 255
    assert _get(port, LONG_KEY)[2] == b"long"
    assert _get(port, CASE_A)[2] == b"upper" and _get(port, CASE_B)[2] == b"lower"


def test_a_replaced_file_survives_until_gc_grace_passes(served):
    store, _port = served
    store.put("series/k.csv", b"first", etag="1")
    old_path = store.head("series/k.csv")["path"]
    store.put("series/k.csv", b"second", etag="2")
    assert os.path.exists(old_path), "kept for in-flight reads"
    assert store.gc(grace_hours=24) == 0 and os.path.exists(old_path)
    assert store.gc(grace_hours=-1) == 1 and not os.path.exists(old_path)
    assert store.head("series/k.csv")["etag"] == "2"


def test_concurrent_gets_each_get_their_own_key(served):
    """R1168: one sqlite connection shared by every server thread crossed rows between threads - at 4
    threads 194 of 20,000 GETs returned ANOTHER key's bytes and etag. 8 threads x 1,000 GETs, 100 keys."""
    import concurrent.futures
    import random
    store, port = served
    keys = [f"series/stress%3A{i:03d}.csv" for i in range(100)]
    for i, k in enumerate(keys):
        store.put(k, f"series_id,obs_date,value\nk{i},2020-01-01,{i}\n".encode() * (1 + i % 7), etag=f"etag-{i}")

    def worker(seed):
        rnd = random.Random(seed)
        bad = []
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
        for _ in range(1000):
            i = rnd.randrange(100)
            c.request("GET", "/o/" + urllib.parse.quote(keys[i], safe=""))
            r = c.getresponse()
            body = r.read()
            want = f"series_id,obs_date,value\nk{i},2020-01-01,{i}\n".encode() * (1 + i % 7)
            if r.status != 200 or body != want or r.getheader("x-blob-etag") != f"etag-{i}":
                bad.append((i, r.status, r.getheader("x-blob-etag")))
        c.close()
        return bad

    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        bad = [b for res in ex.map(worker, range(8)) for b in res]
    assert bad == [], bad[:5]


def test_a_mid_range_is_exact_and_inclusive(served):
    store, port = served
    data = bytes(range(256)) * 4
    store.put("series/m.csv", data, etag="e")
    status, _h, body = _get(port, "series/m.csv", {"Range": "bytes=10-19"})
    assert status == 206 and body == data[10:20], (len(body), body[:4])


def test_gc_keeps_a_file_another_key_still_uses(served):
    store, _port = served
    store.put("series/one.csv", b"shared bytes", etag="s")
    store.put("series/two.csv", b"shared bytes", etag="s")      # same content, same file
    path = store.head("series/one.csv")["path"]
    store.put("series/one.csv", b"new bytes", etag="n")          # retires the shared file for key one
    assert store.gc(grace_hours=-1) == 0 and os.path.exists(path), "key two still uses it"
    assert _get(_port, "series/two.csv")[2] == b"shared bytes"


def test_a_missing_file_is_a_500_not_a_broken_200(served):
    store, port = served
    store.put("series/lost.csv", b"gone soon", etag="g")
    os.remove(store.head("series/lost.csv")["path"])
    status, _h, body = _get(port, "series/lost.csv")
    assert status == 500 and body == b""


def test_a_short_file_is_a_500_not_a_broken_200(served):
    store, port = served
    store.put("series/short.csv", b"0123456789", etag="s")
    with open(store.head("series/short.csv")["path"], "r+b") as fh:
        fh.truncate(4)
    status, _h, body = _get(port, "series/short.csv")
    assert status == 500 and body == b""


def test_the_grace_counts_from_the_newest_retirement(served):
    """R1171 minor 4: a file retired long ago, re-used, then retired again must get a FULL grace again."""
    store, _port = served
    store.put("series/a.csv", b"shared", etag="1")
    path = store.head("series/a.csv")["path"]
    store.put("series/a.csv", b"other", etag="2")                  # retires 'shared'
    store._w.execute("UPDATE retired SET retired_utc='2000-01-01T00:00:00+00:00'")   # long ago
    store.put("series/a.csv", b"shared", etag="1")                 # re-used ...
    store.put("series/a.csv", b"other", etag="2")                  # ... and retired again, just now
    assert store.gc(grace_hours=24) == 0 and os.path.exists(path), "the newest retirement is inside the grace"
    assert store.gc(grace_hours=-1) == 1 and not os.path.exists(path)


def test_a_failed_put_leaves_no_orphan_file(served, monkeypatch):
    """R1171 minor 5: a put that fails after writing its file must not leave a file gc can never find."""
    import blobstore
    store, _port = served
    data = b"never indexed"
    orphan = store._path(__import__("hashlib").sha256(data).hexdigest())

    def boom(*_a, **_k):
        raise RuntimeError("index write failed")
    monkeypatch.setattr(blobstore.json, "dumps", boom)
    with pytest.raises(RuntimeError):
        store.put("series/x.csv", data, etag="x")
    monkeypatch.undo()
    assert not os.path.exists(orphan)
    assert store.head("series/x.csv") is None
    store.put("series/x.csv", data, etag="x")                      # and the store still works
    assert os.path.exists(orphan)


def test_the_sidecar_binds_localhost_only(tmp_path):
    BlobStore(str(tmp_path / "b"), create=True)
    srv = blob_sidecar.serve(str(tmp_path / "b"), 0)
    try:
        assert srv.server_address[0] == "127.0.0.1"
    finally:
        srv.server_close()
