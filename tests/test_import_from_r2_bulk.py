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
        short = getattr(self, "short_first_listing", False) and i == 0
        self.short_first_listing = False                                   # only the FIRST listing ends early
        out = {"Contents": [{"Key": k, "ETag": '"%s"' % _etag(self.objects[k]),
                             "Size": len(self.objects[k]) + (k in self.size_lie), "LastModified": self.lm(k)}
                            for k in chunk], "IsTruncated": i + self.page < len(keys) and not short}
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


def _receipt(tmp_path, marks=False):
    p = tmp_path / "absent.txt"
    lines = p.read_text(encoding="utf-8").splitlines() if p.exists() else []
    return lines if marks else [ln for ln in lines if not ln.startswith("# ")]


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
    assert p["phase"] == "done" and p["absent_listed"] == 7 and p["absent_judged"] == 7 and p["r2_not_held"] == 0


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


# ---- AR-182 round 3 ------------------------------------------------------------------------------------------

def _ns(tmp_path, **kw):
    import argparse
    d = dict(prefix=["series/"], absent_out=str(tmp_path / "absent.txt"), prune_absent=True, prune_max=1000)
    d.update(kw)
    return argparse.Namespace(**d)


def test_t0_during_the_prune_stops_the_deletes(monkeypatch, tmp_path):
    """Finding 1: T0 was checked once, before an hours-long merge; the deletes then ran after T0."""
    from core import cutover
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    left = {k: v for k, v in OBJ.items() if k not in ("series/k1.csv", "series/k2.csv", "series/k3.csv")}
    calls = []
    monkeypatch.setattr(cutover, "is_cut_over", lambda: calls.append(1) or len(calls) >= 3)   # set after 1 delete
    c = {}
    unresolved = imp._absent_pass(FakeBucket(left), BlobStore(str(tmp_path / "blobs")), _ns(tmp_path), c)
    store = BlobStore(str(tmp_path / "blobs"))
    assert c["pruned"] == 1 and unresolved == 2 and "T0 began during the prune" in c["absent_note"]
    assert store.head("series/k2.csv") is not None and store.head("series/k3.csv") is not None
    assert _receipt(tmp_path, marks=True)[-1].startswith("# END") and "T0 began" in _receipt(tmp_path, True)[-1]


def test_progress_is_reported_while_the_listing_is_read_with_no_candidates(monkeypatch, tmp_path):
    """Finding 2: report() ran only per candidate, so the normal (zero-candidate) merge looked hung."""
    import time
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock.__setitem__(0, clock[0] + 31) or clock[0])
    reports = []
    c = {}
    assert imp._absent_pass(FakeBucket(OBJ), BlobStore(str(tmp_path / "blobs")),
                            _ns(tmp_path, prune_absent=False), c, lambda: reports.append(dict(c))) == 0
    assert c["absent"] == 0 and len(reports) >= 6 and reports[-1]["absent_listed"] >= 6
    assert reports[-1]["absent_judged"] >= 6


def test_a_finished_receipt_ends_with_end_and_an_aborted_one_says_so(monkeypatch, tmp_path):
    """Finding 3: an aborted pass left ABSENT lines - one for a key R2 holds - shaped like a finished receipt."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    gone = {k: v for k, v in OBJ.items() if k != "series/k1.csv"}
    imp._absent_pass(FakeBucket(gone), BlobStore(str(tmp_path / "blobs")), _ns(tmp_path, prune_absent=False), {})
    lines = _receipt(tmp_path, marks=True)
    assert lines[0].startswith("# ABSENT CHECK BEGIN") and lines[-1].startswith("# END absent 1 missing 0")
    (tmp_path / "absent.txt").unlink()               # a direct call appends; main() starts each run's receipt
    late = FakeBucket(gone, order=lambda k: (k == "series/k5.csv", k.encode("utf-8")))     # k5 listed LAST
    with pytest.raises(imp.ListingOutOfOrder):
        imp._absent_pass(late, BlobStore(str(tmp_path / "blobs")), _ns(tmp_path), {})
    lines = _receipt(tmp_path, marks=True)
    assert lines[-1].startswith("# ABORTED ListingOutOfOrder") and not any(ln.startswith("# END") for ln in lines)
    assert BlobStore(str(tmp_path / "blobs")).head("series/k1.csv") is not None


def test_a_copy_listing_that_ends_early_fails_the_run(monkeypatch, tmp_path):
    """Finding 4: the first listing stopped after one page (IsTruncated false); 2 of 7 were copied, rc was 0."""
    b = FakeBucket(OBJ)
    b.short_first_listing = True
    rc, p = _run(monkeypatch, b, tmp_path, "--absent-out", str(tmp_path / "absent.txt"))
    assert p["copied"] == 2 and p["r2_not_held"] == 5 and rc == 1
    assert sorted(ln.split(" ")[1] for ln in _receipt(tmp_path) if ln.startswith("MISSING ")) == \
        sorted(OBJ)[2:]


def test_a_key_r2_has_between_held_keys_is_missing(monkeypatch, tmp_path):
    """Both places a missing key can sit: before the store's first key, between two held keys (mutant T6 of
    round 4), and after its last."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    more = dict(OBJ, **{"series/a.csv": b"a", "series/k3a.csv": b"m", "series/z.csv": b"z"})
    c = {}
    assert imp._absent_pass(FakeBucket(more), BlobStore(str(tmp_path / "blobs")), _ns(tmp_path, prune_absent=False),
                            c) == 3
    assert [ln for ln in _receipt(tmp_path) if ln.startswith("MISSING ")] == [
        f"MISSING series/{k} 2026-09-01T12:00:00+00:00" for k in ("a.csv", "k3a.csv", "z.csv")]


def test_each_delete_line_is_on_disk_before_its_delete(monkeypatch, tmp_path):
    """Finding 5: writing DELETE after the delete, or not flushing it, survived every test."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    real = BlobStore.delete
    seen = []

    def delete(self, key):
        seen.append(f"DELETE {key}" in (tmp_path / "absent.txt").read_text(encoding="utf-8").splitlines())
        return real(self, key)
    monkeypatch.setattr(BlobStore, "delete", delete)
    left = {k: v for k, v in OBJ.items() if k not in ("series/k0.csv", "series/k6.csv")}
    rc, p = _run(monkeypatch, FakeBucket(left), tmp_path, *PRUNE, str(tmp_path / "absent.txt"))
    assert rc == 0 and p["pruned"] == 2 and seen == [True, True]


def test_without_a_receipt_the_first_20_are_printed(monkeypatch, tmp_path, capsys):
    many = {f"series/m{i:02d}.csv": b"%d" % i for i in range(25)}
    _run(monkeypatch, FakeBucket(many), tmp_path)
    capsys.readouterr()
    rc, p = _run(monkeypatch, FakeBucket({}), tmp_path, "--resume")
    printed = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("ABSENT ")]
    assert rc == 1 and p["absent"] == 25 and len(printed) == 20


def test_two_prefixes_an_empty_listing_under_the_second_refuses(monkeypatch, tmp_path):
    """Finding 6: the per-prefix refusal arithmetic had no test with two prefixes."""
    two = dict(OBJ, **{"other/x.csv": b"x", "other/y.csv": b"y"})
    _run(monkeypatch, FakeBucket(two), tmp_path, "--prefix", "other/")
    c = {}
    imp._absent_pass(FakeBucket(OBJ), BlobStore(str(tmp_path / "blobs")),
                     _ns(tmp_path, prefix=["series/", "other/"]), c)
    assert c["pruned"] == 0 and "R2 listed nothing under 'other/'" in c["absent_note"]
    assert BlobStore(str(tmp_path / "blobs")).head("other/x.csv") is not None


def test_two_prefixes_an_empty_second_prefix_with_nothing_held_still_prunes_the_first(monkeypatch, tmp_path):
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    c = {}
    left = {k: v for k, v in OBJ.items() if k != "series/k4.csv"}
    assert imp._absent_pass(FakeBucket(left), BlobStore(str(tmp_path / "blobs")),
                            _ns(tmp_path, prefix=["series/", "nothing/"]), c) == 0
    assert c["pruned"] == 1


def test_contradictory_flags_are_argument_errors(monkeypatch, tmp_path, capsys):
    """Findings 7 and 8: nested prefixes judged keys twice; a prune with --limit or only --key returned rc 0."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    for extra, msg in ((("--prefix", "series/k"), "lies inside"),
                       ((*PRUNE, str(tmp_path / "a.txt"), "--limit", "2"), "needs a whole --prefix")):
        with pytest.raises(SystemExit):
            _run(monkeypatch, FakeBucket(OBJ), tmp_path, *extra)
        assert msg in capsys.readouterr().err
    import core.r2_util as r2u
    monkeypatch.setattr(r2u, "cloud_client", lambda: FakeBucket(OBJ))
    monkeypatch.setattr(sys, "argv", ["import_from_r2.py", "--root", str(tmp_path / "blobs"), "--key", "series/k1.csv",
                                      "--prune-absent", "--absent-out", str(tmp_path / "a.txt")])
    with pytest.raises(SystemExit):
        imp.main()
    assert "needs a whole --prefix" in capsys.readouterr().err


def test_a_progress_file_held_open_never_ends_the_copy(monkeypatch, tmp_path, capsys):
    """Finding 9: os.replace onto a file another process holds open raises on Windows."""
    real = os.replace

    def replace(src, dst):
        if str(dst).endswith("p.json") and not str(src).endswith("final"):
            raise PermissionError(5, "Access is denied", str(dst))
        return real(src, dst)
    monkeypatch.setattr(imp.os, "replace", replace)
    import core.r2_util as r2u
    monkeypatch.setattr(r2u, "cloud_client", lambda: FakeBucket(OBJ))
    monkeypatch.setattr(sys, "argv", ["import_from_r2.py", "--root", str(tmp_path / "blobs"), "--create",
                                      "--prefix", "series/", "--workers", "2", "--progress", str(tmp_path / "p.json")])
    assert imp.main() == 0
    assert "held open by another process" in capsys.readouterr().out
    assert len(BlobStore(str(tmp_path / "blobs")).list("series/")) == 7


# ---- AR-182 round 4 ------------------------------------------------------------------------------------------

def test_an_unheld_r2_key_is_a_gap_if_older_than_the_run_and_new_if_not(monkeypatch, tmp_path):
    """Finding 1: keys CI wrote during a multi-day run looked like copy gaps, so rc 1 became permanent."""
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    start = "2026-10-01T00:00:00+00:00"
    more = dict(OBJ, **{"series/g.csv": b"g", "series/n.csv": b"n", "series/s.csv": b"s"})
    b = FakeBucket(more, modified={"series/n.csv": dt.datetime(2026, 10, 2, 8, 0, tzinfo=dt.timezone.utc),
                                   "series/s.csv": dt.datetime(2026, 10, 1, 0, 0, 0, 400000,
                                                               tzinfo=dt.timezone.utc)})   # the start's own second
    c = {"run_start_utc": start}
    assert imp._absent_pass(b, BlobStore(str(tmp_path / "blobs")), _ns(tmp_path, prune_absent=False), c) == 1
    assert c["r2_not_held"] == 1 and c["r2_new"] == 2
    tagged = [ln for ln in _receipt(tmp_path) if ln.split(" ")[0] in ("NEW", "MISSING")]
    assert tagged == ["MISSING series/g.csv 2026-09-01T12:00:00+00:00", "NEW series/n.csv 2026-10-02T08:00:00+00:00",
                      "NEW series/s.csv 2026-10-01T00:00:00+00:00"]


def test_a_run_records_its_own_start(monkeypatch, tmp_path):
    _rc, p = _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    assert p["run_start_utc"] <= dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    assert p["run_start_utc"] > "2026-01-01"


def _old_receipt(tmp_path):
    (tmp_path / "absent.txt").write_text("# BEGIN run 2026-09-01\nABSENT series/old.csv\n# END absent 1\n",
                                         encoding="utf-8")


def test_a_run_that_dies_in_the_copy_never_leaves_an_old_end(monkeypatch, tmp_path):
    """Finding 2: Ctrl+C in the copy listing left the previous run's finished receipt byte for byte."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    _old_receipt(tmp_path)
    b = FakeBucket(OBJ)
    b.list_objects_v2 = lambda **kw: (_ for _ in ()).throw(KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        _run(monkeypatch, b, tmp_path, "--resume", "--absent-out", str(tmp_path / "absent.txt"))
    lines = _receipt(tmp_path, marks=True)
    assert lines[0].startswith("# BEGIN run 2026-") and "old.csv" not in "".join(lines)
    assert lines[-1].startswith("# ABORTED KeyboardInterrupt") and not any(ln.startswith("# END") for ln in lines)


def test_skipped_absent_checks_say_not_run(monkeypatch, tmp_path):
    from core import cutover
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    _old_receipt(tmp_path)
    _run(monkeypatch, FakeBucket(OBJ), tmp_path, "--resume", "--limit", "2", "--absent-out", str(tmp_path / "absent.txt"))
    lines = _receipt(tmp_path, marks=True)
    assert lines[0].startswith("# BEGIN run") and lines[-1].startswith("# NOT RUN absent check NOT RUN: --limit")
    _old_receipt(tmp_path)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    monkeypatch.setattr(imp, "copy_one", lambda *x, **k: (True, "kept"))
    _run(monkeypatch, FakeBucket(OBJ), tmp_path, "--absent-out", str(tmp_path / "absent.txt"))
    assert _receipt(tmp_path, marks=True)[-1].startswith("# NOT RUN absent check NOT RUN: after T0")
    import core.r2_util as r2u
    monkeypatch.setattr(r2u, "cloud_client", lambda: FakeBucket(OBJ))
    monkeypatch.setattr(sys, "argv", ["import_from_r2.py", "--root", str(tmp_path / "blobs"), "--key", "series/k1.csv",
                                      "--absent-out", str(tmp_path / "absent.txt")])
    imp.main()
    lines = _receipt(tmp_path, marks=True)
    assert lines[0].startswith("# BEGIN run") and lines[-1] == "# NOT RUN absent check NOT RUN: no --prefix"


def test_ctrl_c_in_the_absent_pass_is_marked_aborted_once(monkeypatch, tmp_path):
    """Finding 3 (U1): ABORTED on Ctrl+C needs `except BaseException`; nothing pinned it."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b = FakeBucket(OBJ)
    real = b.list_objects_v2
    calls = []

    def listing(**kw):
        calls.append(1)
        if len(calls) > 4:                       # the copy listing is 4 pages; the absent listing then dies
            raise KeyboardInterrupt()
        return real(**kw)
    b.list_objects_v2 = listing
    with pytest.raises(KeyboardInterrupt):
        _run(monkeypatch, b, tmp_path, "--resume", "--absent-out", str(tmp_path / "absent.txt"))
    lines = _receipt(tmp_path, marks=True)
    assert lines[-1].startswith("# ABORTED KeyboardInterrupt")
    assert sum(ln.startswith("# ABORTED") for ln in lines) == 1 and any("ABSENT CHECK BEGIN" in ln for ln in lines)


def test_ctrl_c_marks_the_receipt_even_when_the_absent_pass_is_called_alone(monkeypatch, tmp_path):
    """Round-5 mutant V1: through main() the outer handler hides a pass that catches only Exception."""
    import pytest
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    b = FakeBucket(OBJ)
    b.list_objects_v2 = lambda **kw: (_ for _ in ()).throw(KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        imp._absent_pass(b, BlobStore(str(tmp_path / "blobs")), _ns(tmp_path, prune_absent=False), {})
    assert _receipt(tmp_path, marks=True)[-1].startswith("# ABORTED KeyboardInterrupt")


def test_progress_is_reported_during_the_prune(monkeypatch, tmp_path):
    """Round-5 mutant V12: up to --prune-max HEADs and deletes ran with no progress."""
    import time
    _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    left = {k: v for k, v in OBJ.items() if k not in ("series/k1.csv", "series/k2.csv", "series/k3.csv")}
    b = FakeBucket(left)
    clock = [1000.0]
    monkeypatch.setattr(time, "time", lambda: clock.__setitem__(0, clock[0] + 31) or clock[0])
    heads_at_report = []
    c = {}
    imp._absent_pass(b, BlobStore(str(tmp_path / "blobs")), _ns(tmp_path), c,
                     lambda: heads_at_report.append(len(b.heads)))
    assert c["pruned"] == 3 and any(h >= 1 for h in heads_at_report)


def test_a_progress_write_retries_with_back_off_and_succeeds(monkeypatch, tmp_path, capsys):
    """Finding 3 (U3/U4): the only retry test failed every attempt, so neither the retry nor its back-off was
    shown to work."""
    import time
    real = os.replace
    fails = []
    sleeps = []

    def replace(src, dst):
        if str(dst).endswith("p.json") and len(fails) < 2:
            fails.append(1)
            raise PermissionError(5, "Access is denied", str(dst))
        return real(src, dst)
    monkeypatch.setattr(imp.os, "replace", replace)
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    rc, p = _run(monkeypatch, FakeBucket(OBJ), tmp_path)
    assert rc == 0 and p["done"] and sleeps[:2] == [0.2, 0.4] and len(fails) == 2
    assert "held open" not in capsys.readouterr().out


def test_without_a_receipt_the_first_20_missing_are_printed(monkeypatch, tmp_path, capsys):
    """Finding 3 (U7): the 20-line print cap was tested for ABSENT only."""
    BlobStore(str(tmp_path / "blobs"), create=True)
    many = {f"series/m{i:02d}.csv": b"%d" % i for i in range(25)}
    c = {}
    assert imp._absent_pass(FakeBucket(many), BlobStore(str(tmp_path / "blobs")),
                            _ns(tmp_path, absent_out=None, prune_absent=False), c) == 25
    printed = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("MISSING ")]
    assert c["r2_not_held"] == 25 and len(printed) == 20
