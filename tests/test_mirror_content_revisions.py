"""Same-shape REVISIONS: footer_diff --content finds them, mirror_sync --include-revised pulls them.

R1853. On 2026-11-08 the Fed Z1 heal synced fed_board's mirror with footer_diff + mirror_sync,
footer_diff then said "behind 0", and derive_csv's preflight refused the campaign anyway:
SCOOS__series.parquet had the same row count as R2's copy and different VALUES. Both tools compare
row count and max date only (R555), so neither could see it, and mirror_sync would re-classify the
file as 'same' and keep it for ever. No reviewed tool could sync it.

What is pinned here, each with a mutant in the PR's runner that it kills:

  footer_diff --content
    * a same-shape file whose values differ is `revised`, not `same`, with both fingerprints;
    * a pure RE-ENCODE (same values, different bytes) stays `same` - a byte hash would not (R383);
    * behind and ahead files are never content-checked (no download), and a check that cannot
      run is an ERROR, never `same`; under --content exit 0 means LEVEL (no error, no R2-only);
    * --content refuses --all, before any client is built;
    * WITHOUT --content: the JSON keys, the whole stdout and the exit code are today's (the heal
      parses footer_diff's JSON keys behind / r2_only / errors / ahead and its exit code 0 or 1).
  mirror_sync --include-revised
    * pulls a revision only for a source whose R2 copy is authoritative: a registry entry without
      `run_location: local` (matched by source_id or out_dir), before T0; unknown source,
      unreadable registry, local-run source and a cutover flag all refuse - at sync_source() too;
    * re-checks on the bytes in hand: shape still 'same' (behind or ahead refuses), R2's
      fingerprint still the recorded one, the local one too, and the two still different;
    * never pulls a name that is also listed ahead (the merge queue, R388);
    * a fingerprint that cannot be computed is CHECK FAILED, local kept;
    * intent before the replace and outcome after it in the ledger (R612), and the ledger is kept;
    * without --include-revised the default output of main() is today's, byte for byte.

Offline: R2 is a stub that serves files from tmp_path; every other line of both tools runs.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import io
import json
import os
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import cutover  # noqa: E402
from tools import footer_diff as fd  # noqa: E402
from tools import mirror_sync as ms  # noqa: E402

SRC = "_revsrc"
LOCAL_SRC = "_revlocal"
PREFIX = f"clean_full/{SRC}/"


# Every staged object's upload time. A LIST carries milliseconds and a HEAD whole seconds, as R2's
# do (measured on fed_board: ListObjectsV2 2026-09-25T13:31:13.647Z) - the stub reproduces both.
R2_LM = dt.datetime(2026, 7, 30, 21, 47, 8, 385000, tzinfo=dt.timezone.utc)
LOCAL_MTIME = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc).timestamp()  # local files: older than R2


def _md5(p):
    return hashlib.md5(open(p, "rb").read()).hexdigest()


class StubS3:
    """The network boundary only: LIST, HEAD, ranged GET (footer reads) and whole-object download."""

    def __init__(self, objects, fail=(), lm=None):
        self.objects = dict(objects)      # key -> path of the staged R2 object
        self.fail = set(fail)
        self.lm = dict(lm or {})          # key -> LastModified override
        self.downloads = []

    def _lm(self, k):
        return self.lm.get(k, R2_LM)

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, ContinuationToken=None):
        cs = [{"Key": k, "Size": os.path.getsize(p), "LastModified": self._lm(k),
               "ETag": f'"{_md5(p)}"'} for k, p in sorted(self.objects.items()) if k.startswith(Prefix)]
        return {"Contents": cs, "IsTruncated": False}

    def head_object(self, Bucket, Key):
        return {"LastModified": self._lm(Key).replace(microsecond=0),
                "ContentLength": os.path.getsize(self.objects[Key])}

    def get_object(self, Bucket, Key, Range):
        data = open(self.objects[Key], "rb").read()
        a, b = Range[len("bytes="):].split("-")
        return {"Body": io.BytesIO(data[int(a):int(b) + 1])}

    def download_file(self, Bucket, Key, dest):
        self.downloads.append(Key)
        if Key in self.fail:
            raise RuntimeError("download refused by the stub")
        shutil.copyfile(self.objects[Key], dest)


def _sidecar(n_obs, unit=None):
    """The SCOOS__series shape: one row per series, NO date column, values revised in place."""
    keys = [f"K{i}" for i in range(len(n_obs))]
    return pa.table({"series_key": keys,
                     "unit": unit or ["Number"] * len(n_obs),
                     "n_obs": pa.array(n_obs, pa.int64()),
                     "end": [f"2026-0{1 + (v % 8)}-01" for v in n_obs]})


def _dated(n):
    return pa.table({"series_key": [f"S{i}" for i in range(n)],
                     "obs_date": [f"2026-01-{1 + i:02d}" for i in range(n)],
                     "value": [float(i) for i in range(n)]})


@pytest.fixture
def world(tmp_path, monkeypatch):
    """tmp_path is the data root of BOTH tools; R2 objects are staged under tmp_path/r2."""
    monkeypatch.setattr(fd, "ROOT", str(tmp_path))
    monkeypatch.setattr(ms, "ROOT", str(tmp_path))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    reg = tmp_path / "registry.yaml"
    reg.write_text(
        "sources:\n"
        f"- source_id: {SRC}\n  strategy: overwrite_if_changed\n"
        f"- source_id: {LOCAL_SRC}\n  strategy: overwrite_if_changed\n  run_location: local\n"
        "- source_id: _named\n  out_dir: _otherdir\n  run_location: local\n"
        "- source_id: _elsewhere\n  out_dir: _notthisdir\n",
        encoding="utf-8")
    monkeypatch.setattr(ms, "REGISTRY_PATH", str(reg), raising=False)   # absent before this change
    objects = {}

    def put(src, name, local_tbl=None, r2_tbl=None, r2_compression="snappy"):
        if local_tbl is not None:
            d = tmp_path / "data" / "clean_full" / src
            d.mkdir(parents=True, exist_ok=True)
            pq.write_table(local_tbl, str(d / f"{name}.parquet"))
            # written BEFORE R2's object, as a lagging mirror's file is (the direction guard, C1)
            os.utime(str(d / f"{name}.parquet"), (LOCAL_MTIME, LOCAL_MTIME))
        if r2_tbl is not None:
            r = tmp_path / "r2" / src
            r.mkdir(parents=True, exist_ok=True)
            p = str(r / f"{name}.parquet")
            pq.write_table(r2_tbl, p, compression=r2_compression)
            objects[f"clean_full/{src}/{name}.parquet"] = p

    def local(src, name):
        p = tmp_path / "data" / "clean_full" / src / f"{name}.parquet"
        return pq.read_table(str(p)).to_pydict() if p.exists() else None

    def ledgers():
        d = tmp_path / "logs"
        if not d.is_dir():
            return []
        return [io.open(str(d / f), encoding="utf-8").read() for f in sorted(os.listdir(d))
                if f.startswith("_mirror_sync_withdrawals_")]

    class W:
        pass
    w = W()
    w.tmp, w.put, w.local, w.objects, w.ledgers = tmp_path, put, local, objects, ledgers
    return w


def _diff(w, content, src=SRC, fail=()):
    s3 = StubS3(w.objects, fail, getattr(w, "lm", None))
    jp = str(w.tmp / "diff.json")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        # The default is called EXACTLY as before this change (no content keyword), so the DEFAULT
        # tests below also run unmodified against the pre-change tool - the PR's control run.
        rc = fd.one_source(s3, src, 2, jp, content=True) if content else fd.one_source(s3, src, 2, jp)
    return rc, json.load(open(jp, encoding="utf-8")), buf.getvalue(), s3


def _four_files(w):
    w.put(SRC, "rev__series", _sidecar([10, 20, 30]), _sidecar([10, 20, 31]))      # revised
    w.put(SRC, "eq", _dated(3), _dated(3), r2_compression="gzip")                   # re-encoded only
    w.put(SRC, "beh", _dated(3), _dated(4))                                          # behind
    w.put(SRC, "ahd", _dated(4), _dated(3))                                          # ahead


# --------------------------------------------------------------------------- footer_diff --content

def test_content_files_a_same_shape_value_revision_as_revised(world):
    _four_files(world)
    rc, j, out, s3 = _diff(world, content=True)
    assert [x[0] for x in j["revised"]] == ["rev__series"], j
    n, lr, rr, lfp, rfp, r2_lm = j["revised"][0]
    assert (lr, rr) == (3, 3) and lfp != rfp and isinstance(lfp, str) and isinstance(rfp, str)
    assert r2_lm == "2026-07-30T21:47:08+00:00", "R2's LastModified must be recorded for the direction check"
    assert j["same"] == 1, "the revised file must move OUT of `same`"
    assert [x[0] for x in j["behind"]] == ["beh"] and [x[0] for x in j["ahead"]] == ["ahd"]
    assert rc == 1
    assert "REVISED         1" in out
    # only the two same-shape files were downloaded whole; behind/ahead are decided by shape
    assert sorted(s3.downloads) == [PREFIX + "eq.parquet", PREFIX + "rev__series.parquet"]


def test_content_keeps_a_pure_reencode_same_and_exits_0(world):
    """Same values, different bytes (gzip vs snappy): a byte hash would call this revised (R383)."""
    world.put(SRC, "eq", _dated(3), _dated(3), r2_compression="gzip")
    l_bytes = open(world.tmp / "data" / "clean_full" / SRC / "eq.parquet", "rb").read()
    r_bytes = open(world.objects[PREFIX + "eq.parquet"], "rb").read()
    assert l_bytes != r_bytes, "fixture: the two copies must differ in bytes"
    rc, j, _out, _s3 = _diff(world, content=True)
    assert j["revised"] == [] and j["same"] == 1 and j["errors"] == []
    assert rc == 0


def test_content_skips_the_download_when_local_md5_is_the_plain_etag(world):
    """Byte-identical (md5 == single-part ETag) is level with no GET of the body (AR-464 A1)."""
    world.put(SRC, "eq", _dated(3), _dated(3))                     # same writer, same bytes
    assert _md5(world.tmp / "data" / "clean_full" / SRC / "eq.parquet") == _md5(world.objects[PREFIX + "eq.parquet"])
    rc, j, _out, s3 = _diff(world, content=True)
    assert s3.downloads == [] and j["same"] == 1 and j["revised"] == [] and rc == 0


def test_content_does_not_skip_on_a_multipart_etag(world, monkeypatch):
    """A multipart ETag ('<hex>-N') is not an md5 of the bytes: the file must be fingerprinted."""
    world.put(SRC, "eq", _dated(3), _dated(3))
    orig = StubS3.list_objects_v2

    def multipart(self, **kw):
        r = orig(self, **kw)
        for o in r["Contents"]:
            o["ETag"] = o["ETag"].rstrip('"') + '-2"'
        return r
    monkeypatch.setattr(StubS3, "list_objects_v2", multipart)
    rc, j, _out, s3 = _diff(world, content=True)
    assert s3.downloads == [PREFIX + "eq.parquet"] and j["same"] == 1 and rc == 0


def test_content_check_that_cannot_run_is_an_error_not_same(world):
    world.put(SRC, "eq", _dated(3), _dated(3), r2_compression="gzip")    # bytes differ: must download
    rc, j, _out, _s3 = _diff(world, content=True, fail={PREFIX + "eq.parquet"})
    assert j["same"] == 0, "a file whose fingerprint could not be computed is not proven same"
    assert [e[0] for e in j["errors"]] == ["eq"] and "content R2" in j["errors"][0][1]
    assert j["revised"] == []
    assert rc == 1, "under --content an error is not level"


def test_content_r2_only_file_is_not_level(world):
    world.put(SRC, "eq", _dated(3), _dated(3))
    world.put(SRC, "new", None, _dated(2))
    rc, j, _out, _s3 = _diff(world, content=True)
    assert j["r2_only"] == ["new"] and j["revised"] == []
    assert rc == 1


def test_content_refuses_all_before_any_client(monkeypatch):
    from core import r2_util
    monkeypatch.setattr(r2_util, "cloud_client", lambda *a, **k: pytest.fail("R2 reached"))
    for argv in (["--all", "--content"], ["--content"], ["--content", "--source", "a", "--source", "b"]):
        monkeypatch.setattr(sys, "argv", ["footer_diff.py", *argv])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            assert fd.main() == 2, argv
        assert "--content needs exactly one --source" in buf.getvalue()


def test_content_through_main_reaches_the_content_check(world, monkeypatch):
    """The CLI path: `--source X --content --json J` must produce a `revised` list, exit 1."""
    from core import r2_util
    world.put(SRC, "rev__series", _sidecar([10, 20, 30]), _sidecar([10, 20, 31]))
    monkeypatch.setattr(r2_util, "cloud_client", lambda *a, **k: StubS3(world.objects))
    jp = str(world.tmp / "cli.json")
    monkeypatch.setattr(sys, "argv", ["footer_diff.py", "--source", SRC, "--content", "--json", jp])
    with contextlib.redirect_stdout(io.StringIO()):
        rc = fd.main()
    assert rc == 1 and [x[0] for x in json.load(open(jp, encoding="utf-8"))["revised"]] == ["rev__series"]


def test_DEFAULT_footer_diff_is_unchanged_json_stdout_and_exit(world):
    """The heal's contract (run_heal.ps1 FooterDiff): exit 0 or 1, JSON keys behind / r2_only /
    errors / ahead. Without --content NOTHING may change - the WHOLE stdout and the WHOLE key set
    are compared, not prefixes (R1852). R555's blindness is pinned too: the revised file is
    'same' here, which is exactly why --content exists."""
    _four_files(world)
    rc, j, out, s3 = _diff(world, content=False)
    assert set(j) == {"source", "root", "same", "behind", "ahead", "r2_only", "local_only", "errors"}
    assert j["same"] == 2 and j["errors"] == []
    assert rc == 1
    assert s3.downloads == [], "the default path reads footers only"
    jp = str(world.tmp / "diff.json")
    expected = (
        f"{SRC} [clean_full]  local 4 file(s)   R2 4 object(s)\n"
        "\n"
        "SAME            2\n"
        "LOCAL BEHIND    1   (sync these from R2)\n"
        "LOCAL AHEAD     1   (MERGE queue — do NOT overwrite)\n"
        "R2 ONLY         0   local ONLY 0   errors 0\n"
        "   AHEAD  ahd            local         4 rows  R2         3\n"
        "   behind beh            local         3 rows  R2         4\n"
        "\n"
        f"wrote {jp}\n")
    assert out == expected
    # and the JSON FILE, byte for byte (the heal reads behind / r2_only / errors / ahead[x][0])
    expected_json = {"source": SRC, "root": "clean_full", "same": 2,
                     "behind": [["beh", 3, 4]], "ahead": [["ahd", 4, 3]],
                     "r2_only": [], "local_only": [], "errors": []}
    assert open(jp, encoding="utf-8").read() == json.dumps(expected_json, indent=1)


def test_DEFAULT_footer_diff_still_exits_0_on_a_revision_it_cannot_see(world):
    world.put(SRC, "rev__series", _sidecar([10, 20, 30]), _sidecar([10, 20, 31]))
    rc, j, _out, _s3 = _diff(world, content=False)
    assert rc == 0 and j["same"] == 1 and "revised" not in j


def test_DEFAULT_footer_diff_exit_ignores_r2_only_and_errors_as_today(world):
    """Today's default exit is behind/ahead only; --content's stricter LEVEL rule must not leak."""
    world.put(SRC, "eq", _dated(3), _dated(3))
    world.put(SRC, "new", None, _dated(2))
    rc, j, _out, _s3 = _diff(world, content=False)
    assert j["r2_only"] == ["new"] and rc == 0


# --------------------------------------------------------------------------- r2_authoritative

def test_authority_cloud_source_is_authoritative(world):
    assert ms.r2_authoritative(SRC)[0] is True


def test_authority_local_run_source_is_refused(world):
    ok, why = ms.r2_authoritative(LOCAL_SRC)
    assert ok is False and "run_location: local" in why


def test_authority_matches_out_dir_too(world):
    """A mirror directory is the out_dir; a local-run source stored under another name is refused."""
    ok, why = ms.r2_authoritative("_otherdir")
    assert ok is False and "run_location: local" in why


def test_authority_a_source_id_with_its_out_dir_elsewhere_does_not_own_the_directory(world):
    """`_elsewhere` stores under `_notthisdir`; a directory named `_elsewhere` has no owner."""
    ok, why = ms.r2_authoritative("_elsewhere")
    assert ok is False and "no updater/registry.yaml entry" in why
    assert ms.r2_authoritative("_notthisdir")[0] is True


def test_authority_unknown_source_is_refused(world):
    ok, why = ms.r2_authoritative("_nosuch")
    assert ok is False and "no updater/registry.yaml entry" in why


def test_authority_unreadable_registry_is_refused(world, monkeypatch):
    monkeypatch.setattr(ms, "REGISTRY_PATH", str(world.tmp / "missing.yaml"))
    ok, why = ms.r2_authoritative(SRC)
    assert ok is False and "unreadable" in why


def test_authority_after_t0_is_refused(world, monkeypatch):
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    ok, why = ms.r2_authoritative(SRC)
    assert ok is False and "T0" in why


# --------------------------------------------------------------------------- mirror_sync --include-revised

R2_LM_ISO = "2026-07-30T21:47:08+00:00"    # written out, not computed through footer_diff._iso


def _rev_rec(w, src, name, local_fp=None, r2_fp=None, r2_lm=R2_LM_ISO, **extra):
    lp = str(w.tmp / "data" / "clean_full" / src / f"{name}.parquet")
    rp = w.objects[f"clean_full/{src}/{name}.parquet"]
    lfp = local_fp if local_fp is not None else fd.content_fingerprint(lp)
    rfp = r2_fp if r2_fp is not None else fd.content_fingerprint(rp)
    rec = {"source": src, "root": "clean_full", "behind": [], "r2_only": [], "ahead": [],
           "revised": [[name, pq.read_metadata(lp).num_rows, pq.read_metadata(rp).num_rows, lfp, rfp,
                        r2_lm]]}
    rec.update(extra)
    return rec


def _sync(w, rec, include_revised=True, lm=None):
    s3 = StubS3(w.objects, lm=lm)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        pulled = ms.sync_source(s3, rec, apply=True, include_revised=include_revised)[0]
    return pulled, buf.getvalue(), s3


R2_NOBS = [10, 20, 31]
LOCAL_NOBS = [10, 20, 30]


def test_a_revision_is_replaced_with_r2s_copy(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series"))
    assert world.local(SRC, "SCOOS__series")["n_obs"] == R2_NOBS, "R2's revision did not reach disk"
    assert pulled == 1
    assert "1 file(s) were REVISED at the same shape" in out, out
    assert "ROW COUNT ALONE" not in out and "WEAK COMPARISON" not in out, (
        "a revision is decided by its fingerprint, not by a row count")
    led = world.ledgers()
    assert len(led) == 1, "a run that replaced a revision must keep its ledger"
    i, o = led[0].find("REVISION - replacing with R2's copy - intent"), led[0].find(
        "REVISION - replaced with R2's copy")
    assert 0 <= i < o, led[0]


def test_without_include_revised_nothing_is_pulled(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, _out, s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series"), include_revised=False)
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert pulled == 0 and s3.downloads == []


def test_a_local_run_source_is_refused_inside_sync_source(world):
    world.put(LOCAL_SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, _out, s3 = _sync(world, _rev_rec(world, LOCAL_SRC, "SCOOS__series"))
    assert world.local(LOCAL_SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert pulled == 0 and s3.downloads == []


def test_r2_moved_since_the_sweep_is_stale_and_kept(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series", r2_fp="123"))
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert pulled == 0
    assert "no longer match the revision" in out
    assert any("R2's object is not the revision" in b for b in world.ledgers())


def test_local_changed_since_the_sweep_is_stale_and_kept(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series", local_fp="456"))
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert pulled == 0
    assert any("the local file changed since" in b for b in world.ledgers())


def test_already_level_by_content_is_not_replaced(world):
    """Recorded fingerprints equal (a hand-made record): the files are level - nothing to pull.
    The R2 copy is a re-encode, so a replace would be visible as changed bytes."""
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(LOCAL_NOBS), r2_compression="gzip")
    lp = world.tmp / "data" / "clean_full" / SRC / "SCOOS__series.parquet"
    before = open(lp, "rb").read()
    fp = fd.content_fingerprint(str(lp))
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series", local_fp=fp, r2_fp=fp))
    assert open(lp, "rb").read() == before and pulled == 0
    assert any("already equals R2's by content" in b for b in world.ledgers())


@pytest.mark.parametrize("local_n,r2_n,verdict", [(3, 4, "behind"), (4, 3, "ahead")])
def test_a_revision_whose_shape_no_longer_ties_is_refused(world, local_n, r2_n, verdict):
    """Fingerprints recorded from these very files, so only the SHAPE re-check can refuse them."""
    world.put(SRC, "f", _dated(local_n), _dated(r2_n))
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "f"))
    assert len(world.local(SRC, "f")["series_key"]) == local_n, "a non-tying file was replaced"
    assert pulled == 0
    assert any(f"is '{verdict}', not 'same'" in b for b in world.ledgers())


def test_a_name_also_listed_ahead_is_never_pulled(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rec = _rev_rec(world, SRC, "SCOOS__series", ahead=[["SCOOS__series", 3, 3]])
    pulled, _out, s3 = _sync(world, rec)
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert pulled == 0 and s3.downloads == []


def test_a_fingerprint_that_cannot_run_is_check_failed_and_kept(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rec = _rev_rec(world, SRC, "SCOOS__series")
    lp = world.tmp / "data" / "clean_full" / SRC / "SCOOS__series.parquet"
    lp.write_bytes(b"not a parquet file")
    pulled, _out, _s3 = _sync(world, rec)
    assert lp.read_bytes() == b"not a parquet file" and pulled == 0
    led = world.ledgers()
    assert any("CHECK FAILED - not replaced: fingerprint failed" in b for b in led), led
    assert not any("replacing it" in b for b in led)


# --------------------------------------------------------------------------- direction (AR-464 C1)

def _touch(w, src, name, when):
    p = str(w.tmp / "data" / "clean_full" / src / f"{name}.parquet")
    os.utime(p, (when, when))


def test_C1_a_local_file_written_after_r2s_object_is_kept(world):
    """The measured case: a local edit (mtime 2026-11-08) must not be pulled over by R2's
    2026-07-30 object just because the two differ - content cannot say which side is newer."""
    world.put(SRC, "E2__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    _touch(world, SRC, "E2__series", dt.datetime(2026, 11, 8, 19, 35, 21, tzinfo=dt.timezone.utc).timestamp())
    pulled, out, _s3 = _sync(world, _rev_rec(world, SRC, "E2__series"))
    assert world.local(SRC, "E2__series")["n_obs"] == LOCAL_NOBS, "LOCAL EDIT DESTROYED"
    assert pulled == 0
    assert "written LOCALLY at or after R2's object" in out
    assert any("LOCAL NEWER - local kept" in b for b in world.ledgers())


def test_C1_a_local_file_written_at_the_same_second_is_kept(world):
    """At or after, not only after: equal times cannot show R2 is the later write."""
    world.put(SRC, "E2__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    # exactly the whole second R2's HEAD reports (2026-07-30T21:47:08Z), not the LIST's .385
    _touch(world, SRC, "E2__series", dt.datetime(2026, 7, 30, 21, 47, 8, tzinfo=dt.timezone.utc).timestamp())
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "E2__series"))
    assert world.local(SRC, "E2__series")["n_obs"] == LOCAL_NOBS and pulled == 0


def test_C1_r2_last_modified_moved_since_the_sweep_is_stale(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    later = {PREFIX + "SCOOS__series.parquet": dt.datetime(2026, 11, 9, tzinfo=dt.timezone.utc)}
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series"), lm=later)
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS and pulled == 0
    assert any("LastModified moved" in b for b in world.ledgers())


def test_C1_no_recorded_last_modified_is_check_failed(world):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    pulled, _out, _s3 = _sync(world, _rev_rec(world, SRC, "SCOOS__series", r2_lm=None))
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS and pulled == 0
    assert any("CHECK FAILED - not replaced: no R2 LastModified" in b for b in world.ledgers())


# --------------------------------------------------------------------------- main()

def _main(world, monkeypatch, recs, *flags):
    jp = world.tmp / "classified.json"
    jp.write_text(json.dumps(recs[0] if len(recs) == 1 else {"sources": recs}), encoding="utf-8")
    monkeypatch.setattr(ms, "s3", StubS3(world.objects), raising=False)
    monkeypatch.setattr(sys, "argv", ["mirror_sync.py", "--from-json", str(jp), *flags])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = ms.main()
    return rc, buf.getvalue(), str(jp)


def test_DEFAULT_main_output_and_exit_are_unchanged(world, monkeypatch):
    """The heal runs `mirror_sync --from-json <json> --source fed_board --apply` and requires exit 0.
    The WHOLE stdout of a default run is compared."""
    world.put(SRC, "beh", _dated(2), _dated(3))
    rec = {"source": SRC, "root": "clean_full", "same": 0, "behind": [["beh", 2, 3]], "ahead": [],
           "r2_only": [], "local_only": [], "errors": []}
    rc, out, jp = _main(world, monkeypatch, [rec], "--source", SRC, "--apply")
    assert rc == 0
    assert len(world.local(SRC, "beh")["series_key"]) == 3
    assert out == (f"classification: {jp} (0.0 h old)\n"
                   "MODE: APPLY   1 source(s) with files to pull\n"
                   "\n"
                   "  " + SRC.ljust(22) + "    1 behind +    0 R2-only =    1 pulled\n"
                   "\n"
                   "1 file(s) pulled\n")


def test_main_without_the_flag_names_the_revised_files_it_left(world, monkeypatch):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, SRC, "SCOOS__series")], "--apply")
    assert rc == 0
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert "MODE: APPLY   0 source(s) with files to pull\n" in out
    assert "1 same-shape revised file(s) in this JSON were NOT considered" in out


def test_main_without_the_flag_does_not_judge_a_local_run_source(world, monkeypatch):
    """No flag, no authority decision and no refusal exit: the revision is simply not considered."""
    world.put(LOCAL_SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, LOCAL_SRC, "SCOOS__series")], "--apply")
    assert rc == 0 and "REVISED NOT PULLED" not in out


def test_main_include_revised_pulls_and_counts(world, monkeypatch):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, SRC, "SCOOS__series")],
                         "--include-revised", "--apply")
    assert rc == 0
    assert world.local(SRC, "SCOOS__series")["n_obs"] == R2_NOBS
    assert ("  " + SRC.ljust(22) + "    0 behind +    0 R2-only +    1 revised =    1 pulled\n") in out
    assert "\n1 file(s) pulled\n" in out


def test_main_report_only_with_include_revised_writes_nothing(world, monkeypatch):
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, SRC, "SCOOS__series")], "--include-revised")
    assert rc == 0
    assert world.local(SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert "1 revised =    1 pulled" in out and "1 file(s) would be pulled" in out


def test_main_refuses_a_local_run_source_loudly_with_exit_3(world, monkeypatch):
    world.put(LOCAL_SRC, "SCOOS__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, LOCAL_SRC, "SCOOS__series")],
                         "--include-revised", "--apply")
    assert world.local(LOCAL_SRC, "SCOOS__series")["n_obs"] == LOCAL_NOBS
    assert f"REVISED NOT PULLED  {LOCAL_SRC}: 1 same-shape revised file(s) left alone" in out
    assert rc == 3


def test_main_exits_3_when_a_revised_file_is_refused_per_file(world, monkeypatch):
    """An authoritative source, but the one revised file is a newer local edit: not replaced, and an
    explicit --include-revised run that left a revision behind does not exit 0."""
    world.put(SRC, "E2__series", _sidecar(LOCAL_NOBS), _sidecar(R2_NOBS))
    _touch(world, SRC, "E2__series", dt.datetime(2026, 11, 8, tzinfo=dt.timezone.utc).timestamp())
    rc, out, _jp = _main(world, monkeypatch, [_rev_rec(world, SRC, "E2__series")],
                         "--include-revised", "--apply")
    assert world.local(SRC, "E2__series")["n_obs"] == LOCAL_NOBS
    assert f"REVISED NOT REPLACED (1): {SRC}/E2__series" in out
    assert rc == 3


# --------------------------------------------------------------------------- end to end

def test_end_to_end_content_diff_then_sync_leaves_the_source_level(world):
    """footer_diff --content -> mirror_sync --include-revised -> footer_diff --content says LEVEL."""
    world.put(SRC, "SCOOS__series", _sidecar(LOCAL_NOBS, ["Number"] * 3),
              _sidecar(LOCAL_NOBS, ["Number", "Percentage", "Number"]))
    world.put(SRC, "eq", _dated(3), _dated(3), r2_compression="gzip")
    rc1, j1, _o, _s = _diff(world, content=True)
    assert rc1 == 1 and [x[0] for x in j1["revised"]] == ["SCOOS__series"]
    pulled, _out, _s3 = _sync(world, j1)
    assert pulled == 1
    rc2, j2, _o, _s = _diff(world, content=True)
    assert rc2 == 0 and j2["revised"] == [] and j2["same"] == 2
