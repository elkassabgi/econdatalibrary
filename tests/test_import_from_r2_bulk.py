"""tools/selfhost/import_from_r2.py bulk mode (plan step 2: ~14M objects into the blob store).

A fake bucket pages its listing (2 objects per page) like R2. Tested: every object is copied and verified in
parallel; --resume skips exactly the objects the store holds with the listing's etag and size and RE-COPIES one
that changed on R2 since; --limit stops the listing; a bad object is counted, reported and fails the run without
stopping the others; --progress writes the counts.
"""
from __future__ import annotations

import datetime as dt
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


T1 = dt.datetime(2026, 9, 1, 12, 0, 0, 250000, tzinfo=dt.timezone.utc)     # a listing carries milliseconds


class _NotFound(Exception):
    response = {"Error": {"Code": "404"}}


class FakeBucket:
    """unlisted: keys R2 HOLDS (HEAD finds them) that its listing leaves out - a listing gap. order: a sort key
    for the listing other than UTF-8 bytes (a listing out of order)."""
    def __init__(self, objects: dict, page=2, corrupt=(), modified=None, raises=(), size_lie=(), unlisted=(),
                 order=None):
        self.objects, self.page, self.corrupt, self.gets = dict(objects), page, set(corrupt), []
        self.modified = dict(modified or {})
        self.raises, self.size_lie, self.unlisted = set(raises), set(size_lie), set(unlisted)
        self.order, self.heads, self.dup = order or (lambda k: k.encode("utf-8")), [], set()

    def lm(self, k):
        return self.modified.get(k, T1)

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
        if Key in self.objects or Key in self.unlisted:
            return {"ETag": '"x"', "ContentLength": 1, "LastModified": T1}
        raise _NotFound(Key)

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted((k for k in self.objects if k.startswith(Prefix)), key=self.order)
        keys = [x for k in keys for x in ([k, k] if k in self.dup else [k])]
        i = int(ContinuationToken or 0)
        chunk = keys[i:i + self.page]
        out = {"Contents": [{"Key": k, "ETag": '"%s"' % _etag(self.objects[k]),
                             "Size": len(self.objects[k]) + (k in self.size_lie), "LastModified": self.lm(k)}
                            for k in chunk], "IsTruncated": i + self.page < len(keys)}
        if out["IsTruncated"]:
            out["NextContinuationToken"] = str(i + self.page)
        return out

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key in self.raises:
            raise ConnectionError(f"read timeout on {Key}")
        data = self.objects[Key]
        body = data[:-1] + b"X" if Key in self.corrupt else data           # bytes that do not match the etag
        return {"Body": io.BytesIO(body), "ETag": '"%s"' % _etag(data), "ContentType": "text/csv", "Metadata": {},
                "LastModified": self.lm(Key).replace(microsecond=0)}      # a GET's header has whole seconds


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


def _absent(tmp_path):
    p = tmp_path / "absent.txt"
    lines = p.read_text(encoding="utf-8").splitlines() if p.exists() else []
    return [ln.split(" ", 1)[1] for ln in lines if ln.startswith("ABSENT ")]


def test_an_object_deleted_on_r2_is_reported_and_fails_the_run(monkeypatch, tmp_path):
    """Review AR-182 finding 1: a delete on R2 never reached the store and a resume reported success."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k not in ("series/k0.csv", "series/k3.csv", "series/k6.csv")}
    rc, p = _run(monkeypatch, FakeBucket(gone), tmp_path, "--resume", "--absent-out", str(tmp_path / "absent.txt"))
    assert rc == 1 and p["absent"] == 3 and p["copied"] == 0 and p["failed"] == 0
    assert _absent(tmp_path) == ["series/k0.csv", "series/k3.csv", "series/k6.csv"]   # first, middle, last
    assert BlobStore(str(tmp_path / "blobs")).head("series/k3.csv") is not None, "reported, not deleted"


def _receipt(tmp_path):
    p = tmp_path / "absent.txt"
    return p.read_text(encoding="utf-8").splitlines() if p.exists() else []


PRUNE = ("--resume", "--prune-absent", "--absent-out")


def test_prune_absent_deletes_them_and_passes(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k != "series/k4.csv"}
    b = FakeBucket(gone)
    rc, p = _run(monkeypatch, b, tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    store = BlobStore(str(tmp_path / "blobs"))
    assert rc == 0 and p["absent"] == 1 and p["pruned"] == 1 and b.heads == ["series/k4.csv"]
    assert store.head("series/k4.csv") is None and sorted(store.list("series/")) == sorted(gone)
    assert _receipt(tmp_path) == ["ABSENT series/k4.csv", "DELETE series/k4.csv"]


def test_prune_needs_a_receipt(monkeypatch, tmp_path):
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    with pytest.raises(SystemExit):
        _run(monkeypatch, FakeBucket({}), tmp_path, "--resume", "--prune-absent")
    assert sorted(BlobStore(str(tmp_path / "blobs")).list("series/")) == sorted(OBJ)


def test_every_pruned_key_is_in_the_receipt_before_it_goes(monkeypatch, tmp_path):
    """AR-182 round 2 finding 2: 29 pruned, 20 named."""
    many = {f"series/m{i:02d}.csv": b"%d" % i for i in range(30)}
    _run(monkeypatch, FakeBucket(many), tmp_path)
    rc, p = _run(monkeypatch, FakeBucket({"series/m07.csv": b"7"}), tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    deleted = [ln.split(" ", 1)[1] for ln in _receipt(tmp_path) if ln.startswith("DELETE ")]
    assert rc == 0 and p["pruned"] == 29 and sorted(deleted) == sorted(set(many) - {"series/m07.csv"})


def test_an_empty_listing_never_prunes(monkeypatch, tmp_path):
    """AR-182 round 2 P1: an empty, untruncated, error-free listing pruned the whole prefix with rc 0."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    rc, p = _run(monkeypatch, FakeBucket({}), tmp_path, *PRUNE, str(tmp_path / "absent.txt"), "--prune-max", "100")
    assert rc == 1 and p["pruned"] == 0 and "REFUSED: R2 listed nothing" in p["absent_note"]
    assert sorted(BlobStore(str(tmp_path / "blobs")).list("series/")) == sorted(OBJ)


def test_more_candidates_than_the_cap_never_prunes(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    left = {"series/k0.csv": OBJ["series/k0.csv"]}
    rc, p = _run(monkeypatch, FakeBucket(left), tmp_path, *PRUNE, str(tmp_path / "absent.txt"), "--prune-max", "5")
    assert rc == 1 and p["absent"] == 6 and p["pruned"] == 0 and "exceed --prune-max 5" in p["absent_note"]
    assert len(BlobStore(str(tmp_path / "blobs")).list("series/")) == 7
    rc, p = _run(monkeypatch, FakeBucket(left), tmp_path, *PRUNE, str(tmp_path / "absent.txt"), "--prune-max", "6")
    assert rc == 0 and p["pruned"] == 6                                   # control: at the cap it prunes


def test_a_listing_out_of_order_aborts_with_no_delete(monkeypatch, tmp_path):
    """AR-182 round 2 P2: one inversion deleted an object R2 still held, rc 0, for ever."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k != "series/k0.csv"}
    backwards = FakeBucket(gone, order=lambda k: [-x for x in k.encode("utf-8")])
    with pytest.raises(imp.ListingOutOfOrder):
        _run(monkeypatch, backwards, tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    assert sorted(BlobStore(str(tmp_path / "blobs")).list("series/")) == sorted(OBJ)
    assert not any(ln.startswith("DELETE") for ln in _receipt(tmp_path))


def test_a_listing_that_repeats_a_key_aborts_too(monkeypatch, tmp_path):
    """Strictly ascending: a repeated key is a listing fault as well (mutant M12 of round 3)."""
    import argparse
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b = FakeBucket({k: v for k, v in OBJ.items() if k != "series/k0.csv"})
    b.dup = {"series/k4.csv"}
    a = argparse.Namespace(prefix=["series/"], absent_out=str(tmp_path / "absent.txt"), prune_absent=True,
                           prune_max=1000)
    with pytest.raises(imp.ListingOutOfOrder, match="k4"):
        imp._absent_pass(b, BlobStore(str(tmp_path / "blobs")), a, {})
    assert BlobStore(str(tmp_path / "blobs")).head("series/k0.csv") is not None


def test_a_key_r2_still_has_is_kept_and_fails_the_run(monkeypatch, tmp_path):
    """AR-182 round 2 P3: a key missing from the listing (a gap, or copied meanwhile by another importer) but
    found by its own HEAD is never deleted."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k not in ("series/k2.csv", "series/k5.csv")}
    rc, p = _run(monkeypatch, FakeBucket(gone, unlisted={"series/k2.csv"}), tmp_path,
                 *PRUNE, str(tmp_path / "absent.txt"))
    store = BlobStore(str(tmp_path / "blobs"))
    assert rc == 1 and p["pruned"] == 1 and p["kept_on_r2"] == 1
    assert store.head("series/k2.csv") is not None and store.head("series/k5.csv") is None
    assert "KEPT series/k2.csv (R2 HEAD found it)" in _receipt(tmp_path)


def test_a_head_error_other_than_404_deletes_nothing(monkeypatch, tmp_path):
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b = FakeBucket({k: v for k, v in OBJ.items() if k != "series/k1.csv"})
    b.head_object = lambda **kw: (_ for _ in ()).throw(ConnectionError("HEAD timed out"))
    with pytest.raises(ConnectionError):
        _run(monkeypatch, b, tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    assert BlobStore(str(tmp_path / "blobs")).head("series/k1.csv") is not None


def test_the_plain_prefix_command_checks_for_deletions(monkeypatch, tmp_path, capsys):
    """AR-182 round 2 finding 3: '--prefix always takes the bulk path' had no test - the helper always passed
    --workers 4 --progress. This is the bare command line."""
    import core.r2_util as r2u
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    monkeypatch.setattr(r2u, "cloud_client", lambda: FakeBucket({k: v for k, v in OBJ.items() if k != "series/k3.csv"}))
    monkeypatch.setattr(sys, "argv", ["import_from_r2.py", "--root", str(tmp_path / "blobs"), "--prefix", "series/"])
    assert imp.main() == 1
    assert "held but not on R2 1" in capsys.readouterr().out


def test_after_t0_the_absent_check_does_not_run(monkeypatch, tmp_path):
    """AR-182 round 2 mutant M6: the T0 skip itself, without --prune-absent."""
    import argparse
    from core import cutover
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    a = argparse.Namespace(prefix=["series/"], absent_out=None, prune_absent=False, prune_max=1000)
    c = {}
    assert imp._absent_pass(FakeBucket({}), BlobStore(str(tmp_path / "blobs")), a, c) == 0
    assert c["absent"] == 0 and "after T0" in c["absent_note"]
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)             # control: before T0 it finds all 7
    assert imp._absent_pass(FakeBucket({}), BlobStore(str(tmp_path / "blobs")), a, c) == 7


def test_control_nothing_deleted_nothing_absent(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    rc, p = _run(monkeypatch, FakeBucket(OBJ), tmp_path, "--resume", "--absent-out", str(tmp_path / "absent.txt"))
    assert rc == 0 and p["absent"] == 0 and p["skipped_held"] == 7 and _absent(tmp_path) == []


def test_the_absent_check_stays_inside_its_prefix_and_its_byte_order(monkeypatch, tmp_path):
    """Store keys outside the prefix are not R2's to judge; non-ASCII keys (2- and 4-byte UTF-8) and keys before,
    between and after R2's keys are each checked, with a one-object page."""
    objs = {"series/a.csv": b"1", "series/é.csv": b"2", "series/\U0001f600.csv": b"3", "series/z.csv": b"4"}
    _run(monkeypatch, FakeBucket(objs, page=1), tmp_path)
    store = BlobStore(str(tmp_path / "blobs"))
    store.put("other/x.csv", b"o", etag=_etag(b"o"))                       # outside --prefix series/
    store.put("series/0.csv", b"7", etag=_etag(b"7"))                      # before R2's first key
    store.put("series/b.csv", b"6", etag=_etag(b"6"))                      # between two R2 keys
    store.put("series/zz.csv", b"5", etag=_etag(b"5"))                     # past R2's last key
    store.put("series/\U0001f601.csv", b"8", etag=_etag(b"8"))             # a 4-byte key past all (mutant M3)
    store.put("series/", b"9", etag=_etag(b"9"))                           # the prefix itself (mutant M2)
    rc, _p = _run(monkeypatch, FakeBucket(objs, page=1), tmp_path, "--resume",
                  "--absent-out", str(tmp_path / "absent.txt"))
    assert rc == 1 and [ln.split(" ", 1)[1] for ln in _receipt(tmp_path)] == [
        "series/", "series/0.csv", "series/b.csv", "series/zz.csv", "series/\U0001f601.csv"]


def test_iter_keys_pages_without_losing_or_repeating_a_key(tmp_path):
    store = BlobStore(str(tmp_path / "b"), create=True)
    keys = [f"series/k{i:03d}.csv" for i in range(25)]
    for k in keys:
        store.put(k, k.encode(), etag=_etag(k.encode()))
    store.put("seriesX/a.csv", b"x", etag=_etag(b"x"))
    assert list(store.iter_keys("series/", page=4)) == keys
    assert list(store.iter_keys("series/", page=5)) == keys                # a page boundary at the very end
    assert list(store.iter_keys("nothing/", page=4)) == []


def test_resume_recopies_the_same_bytes_with_a_newer_last_modified(monkeypatch, tmp_path):
    """Review AR-182 finding 2: --resume compared etag and size only."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    later = T1 + dt.timedelta(hours=3)
    b2 = FakeBucket(OBJ, modified={"series/k2.csv": later})
    rc, p = _run(monkeypatch, b2, tmp_path, "--resume")
    assert rc == 0 and b2.gets == ["series/k2.csv"] and p["skipped_held"] == 6
    assert BlobStore(str(tmp_path / "blobs")).head("series/k2.csv")["stored_utc"] == "2026-09-01T15:00:00+00:00"


def test_control_milliseconds_alone_do_not_force_a_copy(monkeypatch, tmp_path):
    """The listing has milliseconds, a GET's header does not: the same second is the same object."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b2 = FakeBucket(OBJ, modified={k: T1.replace(microsecond=999000) for k in OBJ})
    rc, p = _run(monkeypatch, b2, tmp_path, "--resume")
    assert rc == 0 and b2.gets == [] and p["skipped_held"] == 7


def test_resume_recopies_when_the_size_differs(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b2 = FakeBucket(OBJ, size_lie={"series/k1.csv"})
    _rc, p = _run(monkeypatch, b2, tmp_path, "--resume")
    assert b2.gets == ["series/k1.csv"] and p["skipped_held"] == 6


def test_a_read_error_is_counted_and_the_rest_are_copied(monkeypatch, tmp_path, capsys):
    """Review AR-182: the exception branch of the worker was untested."""
    rc, p = _run(monkeypatch, FakeBucket(OBJ, raises={"series/k2.csv"}), tmp_path)
    assert rc == 1 and p["failed"] == 1 and p["copied"] == 6
    assert "FAIL series/k2.csv ERROR ConnectionError: read timeout" in capsys.readouterr().out
    assert BlobStore(str(tmp_path / "blobs")).head("series/k2.csv") is None


def test_a_limit_trial_does_not_judge_absence(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k != "series/k0.csv"}           # absent, were the check run
    rc, p = _run(monkeypatch, FakeBucket(gone), tmp_path, "--resume", "--limit", "2")
    assert rc == 0 and p["absent"] == 0 and "NOT RUN" in p["absent_note"]


def test_after_t0_prune_is_refused(monkeypatch, tmp_path, capsys):
    import pytest
    from core import cutover
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    with pytest.raises(SystemExit):
        _run(monkeypatch, FakeBucket({}), tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    assert "refused after T0" in capsys.readouterr().err
    assert sorted(BlobStore(str(tmp_path / "blobs")).list("series/")) == sorted(OBJ)
