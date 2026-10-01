"""tools/selfhost/import_from_r2.py bulk mode (plan step 2: ~14M objects into the blob store).

A fake bucket pages its listing (2 objects per page) like R2. Tested: every object is copied and verified in
parallel; --resume skips exactly the objects the store holds with the listing's etag and size and RE-COPIES one
that changed on R2 since; --limit stops the listing; a bad object is counted, reported and fails the run without
stopping the others; --progress writes the counts.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))

import import_from_r2 as imp  # noqa: E402
from blobstore import BlobStore  # noqa: E402


def _etag(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()  # noqa: S324


class FakeBucket:
    def __init__(self, objects: dict, page=2, corrupt=()):
        self.objects, self.page, self.corrupt, self.gets = dict(objects), page, set(corrupt), []

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        i = int(ContinuationToken or 0)
        chunk = keys[i:i + self.page]
        out = {"Contents": [{"Key": k, "ETag": '"%s"' % _etag(self.objects[k]), "Size": len(self.objects[k])}
                            for k in chunk], "IsTruncated": i + self.page < len(keys)}
        if out["IsTruncated"]:
            out["NextContinuationToken"] = str(i + self.page)
        return out

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        data = self.objects[Key]
        body = data[:-1] + b"X" if Key in self.corrupt else data           # bytes that do not match the etag
        return {"Body": io.BytesIO(body), "ETag": '"%s"' % _etag(data), "ContentType": "text/csv", "Metadata": {}}


def _run(monkeypatch, bucket, tmp_path, *extra):
    import core.r2_util as r2u
    monkeypatch.setattr(r2u, "cloud_client", lambda: bucket)
    argv = ["import_from_r2.py", "--root", str(tmp_path / "blobs"), "--create", "--prefix", "series/",
            "--workers", "4", "--quiet", "--progress", str(tmp_path / "p.json"), *extra]
    monkeypatch.setattr(sys, "argv", argv)
    rc = imp.main()
    return rc, json.load(open(tmp_path / "p.json"))


OBJ = {f"series/k{i}.csv": f"id,v\nk{i},{i}\n".encode() for i in range(7)}


def test_every_object_is_copied_and_verified(monkeypatch, tmp_path):
    rc, p = _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    assert rc == 0 and p["copied"] == 7 and p["failed"] == 0 and p["done"]
    store = BlobStore(str(tmp_path / "blobs"))
    assert sorted(store.list("series/")) == sorted(OBJ)
    for k, v in OBJ.items():
        assert store.head(k)["etag"] == _etag(v) and store.head(k)["size"] == len(v)


def test_resume_skips_held_objects_and_recopies_a_changed_one(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    changed = dict(OBJ, **{"series/k3.csv": b"id,v\nk3,CHANGED ON R2\n"})
    b2 = FakeBucket(changed)
    rc, p = _run(monkeypatch, b2, tmp_path, "--resume")
    assert rc == 0 and p["skipped_held"] == 6 and p["copied"] == 1 and b2.gets == ["series/k3.csv"]
    assert BlobStore(str(tmp_path / "blobs")).head("series/k3.csv")["etag"] == _etag(changed["series/k3.csv"])


def test_control_without_resume_everything_is_fetched_again(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b2 = FakeBucket(OBJ)
    _run(monkeypatch, b2, tmp_path, "--workers", "2")              # bulk, no --resume
    assert sorted(b2.gets) == sorted(OBJ)


def test_limit_stops_the_listing(monkeypatch, tmp_path):
    rc, p = _run(monkeypatch, FakeBucket(OBJ), tmp_path, "--limit", "3")
    assert rc == 0 and p["listed"] == 3 and p["copied"] == 3


def test_a_bad_object_fails_the_run_but_not_the_others(monkeypatch, tmp_path, capsys):
    rc, p = _run(monkeypatch, FakeBucket(OBJ, corrupt={"series/k5.csv"}), tmp_path)
    assert rc == 1 and p["failed"] == 1 and p["copied"] == 6
    assert "FAIL etag mismatch for series/k5.csv" in capsys.readouterr().out
    assert BlobStore(str(tmp_path / "blobs")).head("series/k5.csv") is None
