"""usda re-ingest is atomic per cube (jobs/ingest_usda.py, 2026-11-19).

The bug this pins: a cube without `_complete` was "resumed" from the LIVE cube's row count, so a
new vintage kept every old part and only appended the new dump's rows past the old count - and a
new dump that was not longer wrote nothing, under a fresh `_complete`. Every test below runs the
REAL jobs.ingest_usda.main on tiny gzipped dumps with PART_ROWS patched small, in tmp_path (run
with --basetemp on F:), with no network and no servers.
"""
from __future__ import annotations

import glob
import gzip
import io
import os
import shutil
import sys

import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from jobs import ingest_usda as J                     # noqa: E402
from updater import writer_lock                       # noqa: E402

# 39 columns: every column the ingest reads, padded with fillers (NASS's own count).
NEEDED = J.STORE_COLS[1:] + ["VALUE", "CV_%"]
HEADER = NEEDED + [f"FILL_{i}" for i in range(J.EXPECTED_NCOLS - len(NEEDED))]
assert len(HEADER) == J.EXPECTED_NCOLS


def _row(sector, i, value):
    d = {c: "" for c in HEADER}
    d.update({"SOURCE_DESC": "SURVEY", "SECTOR_DESC": sector.upper(), "GROUP_DESC": "G",
              "COMMODITY_DESC": "C", "SHORT_DESC": f"ITEM {i:03d}", "AGG_LEVEL_DESC": "NATIONAL",
              "LOCATION_DESC": "US TOTAL", "YEAR": str(2000 + i), "FREQ_DESC": "ANNUAL",
              "REFERENCE_PERIOD_DESC": "YEAR", "VALUE": value})
    return "\t".join(d[c] for c in HEADER)


def write_dump(raw, sector, date, values, malformed=0):
    """qs.<sector>_<date>.txt.gz with one row per value; removes the sector's other dumps."""
    for old in glob.glob(os.path.join(raw, f"qs.{sector}_*.txt.gz")):
        os.remove(old)
    lines = ["\t".join(HEADER)] + [_row(sector, i, v) for i, v in enumerate(values)]
    lines += ["short\trow"] * malformed
    path = os.path.join(raw, f"qs.{sector}_{date}.txt.gz")
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


def cube_rows(d):
    """[(SHORT_DESC, value)] across a cube's parts, in part order."""
    out = []
    for p in sorted(glob.glob(os.path.join(d, "part_*.parquet"))):
        t = pq.read_table(p, columns=["SHORT_DESC", "value"])
        out += list(zip(t.column(0).to_pylist(), t.column(1).to_pylist()))
    return out


def expect(values):
    return [(f"ITEM {i:03d}", float(v.replace(",", ""))) for i, v in enumerate(values)]


V1 = [str(100 + i) for i in range(10)]          # 10 rows -> 4 parts at PART_ROWS=3
V2 = [str(900 + i) for i in range(5)]           # FEWER and CHANGED -> 2 parts
V3 = [str(5000 + i) for i in range(8)]


@pytest.fixture
def env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    raw, out = data / "raw" / "usda", data / "clean_full" / "usda"
    raw.mkdir(parents=True)
    monkeypatch.setattr(J, "RAW", str(raw))
    monkeypatch.setattr(J, "OUT", str(out))
    monkeypatch.setattr(J, "PART_ROWS", 3)
    monkeypatch.setattr(J, "BATCH_ROWS", 2)
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(sys, "argv", ["ingest_usda.py"])

    class E:
        pass
    e = E()
    e.raw, e.out, e.data = str(raw), str(out), str(data)
    e.live = lambda grp="demographics": os.path.join(str(out), grp)
    e.stage = lambda: J.staging_root()
    return e


def _stage_entries():
    r = J.staging_root()
    return sorted(os.listdir(r)) if os.path.isdir(r) else []


# ----------------------------------------------------------------------------- the core bug
def test_new_vintage_with_fewer_changed_rows_replaces_the_cube_exactly(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    assert cube_rows(env.live()) == expect(V1)
    assert len(J.cube_parts(env.live())) == 4

    write_dump(env.raw, "demographics", "20261118", V2, malformed=2)
    os.remove(os.path.join(env.live(), "_complete"))            # what the fetcher does
    J.main()
    assert cube_rows(env.live()) == expect(V2), "store must equal the NEW file exactly"
    names = [os.path.basename(p) for p in J.cube_parts(env.live())]
    assert names == ["part_000.parquet", "part_001.parquet"], "no old part may survive"
    assert os.path.exists(os.path.join(env.live(), "_complete"))
    b = J.read_build(env.live())
    assert b["input"]["fname"] == "qs.demographics_20261118.txt.gz" and b["rows"] == 5
    assert b["malformed"] == 2
    assert _stage_entries() == [], "no staged or retired copy left behind"
    import json
    m = json.load(open(os.path.join(env.out, "_manifest.json"), encoding="utf-8"))
    assert m["total_rows"] == 5 and m["cube_detail"]["demographics"]["parts"] == names
    assert m["cube_detail"]["demographics"]["min_date"] == "2000-12-31"
    assert m["cube_detail"]["demographics"]["max_date"] == "2004-12-31"


def test_a_new_dump_is_rebuilt_even_when_complete_was_not_cleared(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)        # sentinel left in place
    J.main()
    assert cube_rows(env.live()) == expect(V2)


def test_a_legacy_cube_without_build_json_is_rebuilt_not_trusted(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    os.remove(os.path.join(env.live(), J.BUILD_FILE))           # the pre-2026-11-19 store shape
    write_dump(env.raw, "demographics", "20260603", V2)          # same name, different bytes
    J.main()
    assert cube_rows(env.live()) == expect(V2)


def test_same_input_is_skipped_and_its_sentinel_restored(env, capsys):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    os.remove(os.path.join(env.live(), "_complete"))
    capsys.readouterr()
    J.main()
    assert "SKIP (verified build" in capsys.readouterr().out
    assert os.path.exists(os.path.join(env.live(), "_complete"))
    assert cube_rows(env.live()) == expect(V1)


# ----------------------------------------------------------------------------- kill mid-rebuild
def _kill_writer_at(monkeypatch, at):
    real = J.PartWriter.add
    seen = {"n": 0}

    def add(self, *a, **k):
        seen["n"] += 1
        if seen["n"] == at:
            raise KeyboardInterrupt("simulated kill inside the writer")
        return real(self, *a, **k)
    monkeypatch.setattr(J.PartWriter, "add", add)
    return lambda: monkeypatch.setattr(J.PartWriter, "add", real)


def test_kill_inside_the_writer_leaves_the_old_cube_intact_then_resumes(env, monkeypatch, capsys):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V3)          # 8 rows
    undo = _kill_writer_at(monkeypatch, 8)                       # 2 parts closed, a torn 3rd
    with pytest.raises(KeyboardInterrupt):
        J.main()
    assert cube_rows(env.live()) == expect(V1), "the live cube must be untouched"
    assert J.verify_dir(env.live())[0], "and still a verified, servable build"
    assert _stage_entries() == ["demographics.new"]
    assert not os.path.exists(writer_lock.lock_path(J.LOCK_NAME)), "lock released on a kill"
    undo()
    capsys.readouterr()
    J.main()
    log = capsys.readouterr().out
    assert "RESUME staged build of qs.demographics_20261118.txt.gz from 6 rows" in log
    assert cube_rows(env.live()) == expect(V3)
    assert _stage_entries() == []


def test_resume_with_a_different_input_wipes_the_staging_dir(env, monkeypatch, capsys):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V3)
    undo = _kill_writer_at(monkeypatch, 8)
    with pytest.raises(KeyboardInterrupt):
        J.main()
    undo()
    write_dump(env.raw, "demographics", "20261201", V2)          # a different dump arrived
    capsys.readouterr()
    J.main()
    log = capsys.readouterr().out
    assert "wiping it" in log and "RESUME" not in log
    assert cube_rows(env.live()) == expect(V2)


# ----------------------------------------------------------------------------- verify before swap
def test_a_staged_build_that_lost_rows_is_never_swapped_in(env, monkeypatch):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V3)
    real = J.PartWriter.add
    seen = {"n": 0}

    def lossy(self, *a, **k):                                    # a writer that drops one row
        seen["n"] += 1
        if seen["n"] == 4:
            return None
        return real(self, *a, **k)
    monkeypatch.setattr(J.PartWriter, "add", lossy)
    with pytest.raises(J.IngestRefused, match="staged parts hold 7 rows"):
        J.main()
    assert cube_rows(env.live()) == expect(V1)
    assert _stage_entries() == [], "a failed verification wipes its staging"


# ----------------------------------------------------------------------------- kill at each swap step
def _resolver_files(env):
    from econdl import _resolve as R
    res = R._resolve_usda("usda:SURVEY|NATIONAL|ITEM 000", os.path.dirname(env.out))
    p = res.parquet_path
    return sorted(p if isinstance(p, list) else [p])


@pytest.mark.parametrize("step", ["after_live_to_old", "after_new_to_live", "before_drop_old"])
def test_a_kill_at_each_swap_step_leaves_one_copy_and_recovers(env, monkeypatch, step):
    write_dump(env.raw, "demographics", "20260603", V1)
    write_dump(env.raw, "economics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)

    def crash(name):
        if name == step:
            raise KeyboardInterrupt(f"simulated kill at {name}")
    monkeypatch.setattr(J, "_crash_point", crash)
    with pytest.raises(KeyboardInterrupt):
        J.main()

    # EXACTLY ONE COPY of each cube is visible to the resolver's glob and to the publish set.
    files = _resolver_files(env)
    pub = J.served_parts()
    demo = [f for f in files if "/demographics" in f.replace("\\", "/")]
    assert all(".old" not in f and ".new" not in f for f in files + pub)
    assert sorted(os.path.normcase(f) for f in pub) == sorted(os.path.normcase(f) for f in files)
    econ = [f for f in files if "/economics/" in f.replace("\\", "/")]
    assert sum(pq.read_metadata(f).num_rows for f in econ) == 10
    if step == "after_live_to_old":
        assert demo == [], "between the two renames the cube is in staging only"
    else:
        assert cube_rows(env.live()) == expect(V2)               # past the commit point

    monkeypatch.setattr(J, "_crash_point", lambda name: None)
    J.main()                                                     # recovery runs first
    assert cube_rows(env.live()) == expect(V2)
    assert cube_rows(env.live("economics")) == expect(V1)
    assert _stage_entries() == []


def test_old_only_is_rolled_back(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    os.makedirs(J.staging_root(), exist_ok=True)
    os.rename(env.live(), os.path.join(J.staging_root(), "demographics.old"))
    assert J.recover_cube("demographics").startswith("rolled back")
    assert cube_rows(env.live()) == expect(V1)


def test_a_leftover_old_is_removed_as_a_leftover_never_retried_as_a_lock(env, monkeypatch, capsys):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    os.makedirs(J.staging_root(), exist_ok=True)
    shutil.copytree(env.live(), os.path.join(J.staging_root(), "demographics.old"))
    import time
    from core import atomic

    def boom(*a, **k):
        raise AssertionError("a leftover .old must not be waited on or retried")
    monkeypatch.setattr(time, "sleep", boom)
    monkeypatch.setattr(atomic, "atomic_replace", boom)
    capsys.readouterr()
    J.main()
    assert "leftover .old beside a live cube; live verified, .old removed" in capsys.readouterr().out
    assert _stage_entries() == []
    assert cube_rows(env.live()) == expect(V1)


# ----------------------------------------------------------------------------- rename failures
def _fail_rename(monkeypatch, which):
    real = os.rename

    def ren(src, dst):
        s = os.path.normcase(os.path.abspath(src))
        if which == "live" and s == os.path.normcase(os.path.abspath(J.OUT + os.sep + "demographics")):
            raise PermissionError(5, "Access is denied (simulated open handle)")
        if which == "new" and s.endswith(os.path.normcase("demographics.new")):
            raise PermissionError(5, "Access is denied (simulated open handle)")
        return real(src, dst)
    monkeypatch.setattr(os, "rename", ren)
    return lambda: monkeypatch.setattr(os, "rename", real)


def test_a_blocked_first_rename_fails_cleanly_and_keeps_the_old_cube(env, monkeypatch, capsys):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)
    undo = _fail_rename(monkeypatch, "live")
    with pytest.raises(J.IngestRefused, match="could not move the live cube aside"):
        J.main()
    assert cube_rows(env.live()) == expect(V1)
    assert _stage_entries() == ["demographics.new"]
    undo()
    capsys.readouterr()
    J.main()
    assert "staged build already verified - swapping it" in capsys.readouterr().out
    assert cube_rows(env.live()) == expect(V2)


def test_a_blocked_second_rename_restores_the_old_cube(env, monkeypatch):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)
    _fail_rename(monkeypatch, "new")
    with pytest.raises(J.IngestRefused, match="old cube restored"):
        J.main()
    assert cube_rows(env.live()) == expect(V1)
    assert "demographics.old" not in _stage_entries()


@pytest.mark.skipif(os.name != "nt", reason="an open handle blocks a directory rename only on Windows")
def test_a_real_open_handle_on_a_live_part_fails_cleanly(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)
    fh = open(J.cube_parts(env.live())[0], "rb")
    try:
        with pytest.raises(J.IngestRefused):
            J.main()
        assert cube_rows(env.live()) == expect(V1)
    finally:
        fh.close()
    J.main()
    assert cube_rows(env.live()) == expect(V2)


# ----------------------------------------------------------------------------- lock + vintage check
def test_the_rebuild_refuses_while_another_usda_writer_holds_the_lock(env):
    write_dump(env.raw, "demographics", "20260603", V1)
    assert writer_lock.acquire(J.LOCK_NAME, what="a derive")
    try:
        with pytest.raises(J.IngestRefused, match="another usda writer"):
            J.main()
        assert not os.path.isdir(env.live())
    finally:
        writer_lock.release(J.LOCK_NAME)


def test_the_retry_drain_skips_usda_while_a_rebuild_holds_the_lock(monkeypatch):
    from updater import orchestrate as O

    class Unit:
        source_id = "usda"

    class Store:
        def csv_retries(self, sid):
            raise AssertionError("the drain must not touch the queue while usda is rebuilt")
    monkeypatch.setattr(writer_lock, "owner",
                        lambda name: {"pid": -1, "what": "jobs/ingest_usda.py"} if name == "usda_writer" else None)
    summ = O._drain_csv_retry_queue(Unit(), None, Store())
    assert summ["attempted"] == 0 and summ["cleared"] == 0


def test_all_committed_names_exactly_the_five_listing_files(env):
    names = []
    for s in J.SECTOR_FILES:
        names.append(os.path.basename(write_dump(env.raw, s, "20261118", V2)))
    J.main()
    assert J.all_committed(names)
    assert not J.all_committed(names[:4] + ["qs.environmental_20261201.txt.gz"])
    os.remove(os.path.join(env.live("crops"), J.BUILD_FILE))
    assert not J.all_committed(names)


# ----------------------------------------------------------------------------- R2 stale part keys
class _FakeR2:
    objects: dict = {}

    def __init__(self, *a, **k):
        self.bucket = "fake"
        self.client = self

    def list_keys(self, prefix):
        return sorted(k for k in self.objects if k.startswith(prefix))

    def exists(self, key):
        return key in self.objects

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def size(self, key):
        return len(self.objects[key]) if key in self.objects else None

    def etag(self, key):
        import hashlib
        return hashlib.md5(self.objects[key]).hexdigest() if key in self.objects else None


def _load_stale_tool():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "usda_r2_stale_parts", os.path.join(ROOT, "tools", "usda_r2_stale_parts.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_stale_r2_part_keys_are_backed_up_removed_and_the_key_sets_proved_equal(env, monkeypatch):
    from updater import blob
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    _FakeR2.objects = {}
    monkeypatch.setattr(blob, "R2Blob", _FakeR2)
    tool = _load_stale_tool()

    def publish():                                               # what the fetcher's publish does
        for p in J.served_parts():
            _FakeR2.objects[blob._path_to_key(p)] = open(p, "rb").read()

    for s in J.SECTOR_FILES:
        write_dump(env.raw, s, "20260603", V1)
    J.main()
    publish()
    write_dump(env.raw, "demographics", "20261118", V2)
    J.main()
    publish()
    stale = {"clean_full/usda/demographics/part_002.parquet",
             "clean_full/usda/demographics/part_003.parquet"}
    local, r2, _ = tool.key_sets()
    assert r2 - local == stale

    before = dict(_FakeR2.objects)
    assert tool.main([]) == 0 and _FakeR2.objects == before, "report mode changes nothing"
    assert tool.main(["--apply"]) == 0
    local, r2, _ = tool.key_sets()
    assert local == r2
    backups = [k for k in _FakeR2.objects if k.startswith("_backup/usda_stale_parts/")]
    assert sorted(k.split("/", 3)[3] for k in backups) == sorted(stale)


def test_stale_cleanup_refuses_while_r2_lacks_live_parts(env, monkeypatch):
    from updater import blob
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    _FakeR2.objects = {"clean_full/usda/demographics/part_009.parquet": b"x"}
    monkeypatch.setattr(blob, "R2Blob", _FakeR2)
    tool = _load_stale_tool()
    for s in J.SECTOR_FILES:
        write_dump(env.raw, s, "20260603", V1)
    J.main()
    assert tool.main(["--apply"]) == 1
    assert "clean_full/usda/demographics/part_009.parquet" in _FakeR2.objects


# ----------------------------------------------------------------------------- the production call shape
def test_recovery_with_old_and_an_uncommitted_new_rolls_back_and_never_swaps(env, monkeypatch):
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V3)
    undo = _kill_writer_at(monkeypatch, 8)
    with pytest.raises(KeyboardInterrupt):
        J.main()
    undo()
    os.rename(env.live(), os.path.join(J.staging_root(), "demographics.old"))   # kill after rename 1
    assert J.recover_cube("demographics").startswith("rolled back")
    assert cube_rows(env.live()) == expect(V1), "an uncommitted build must never be swapped in"
    assert J.read_build(os.path.join(J.staging_root(), "demographics.new"))["verified"] is False
    J.main()                                                     # and it still resumes to the new file
    assert cube_rows(env.live()) == expect(V3)


def test_keep_old_writes_the_manifest_from_the_live_cubes_and_drop_all_old_clears_staging(env):
    import json
    write_dump(env.raw, "demographics", "20260603", V1)
    J.main()
    write_dump(env.raw, "demographics", "20261118", V2)
    J.main(keep_old=True)
    assert _stage_entries() == ["demographics.old"], "keep_old keeps the previous copy"
    m = json.load(open(os.path.join(env.out, "_manifest.json"), encoding="utf-8"))
    assert m["total_rows"] == 5 and m["cube_detail"]["demographics"]["rows"] == 5
    assert m["cube_detail"]["demographics"]["parts"] == ["part_000.parquet", "part_001.parquet"]
    assert J.drop_all_old() == ["demographics"]
    assert _stage_entries() == [] and cube_rows(env.live()) == expect(V2)


class _Resp:
    def __init__(self, data):
        self.data, self.headers = data, {"Content-Length": str(len(data))}

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size=1):
        yield self.data


FILES_URL = "https://www.nass.usda.gov/sites/default/files/datasets/"


def _entry(path, date):
    """A _listing() value: (fname, date, absolute download url from the page's href)."""
    return (os.path.basename(path), date, FILES_URL + os.path.basename(path))


def _fetcher_rig(env, monkeypatch, fail=()):
    """The usda fetcher over the REAL ingest in env: a fake listing and download session
    serving dumps from <root>/upstream; a download whose basename is in `fail` raises."""
    from updater import blob, config
    from updater.strategies.fetchers import usda as F
    root = os.path.dirname(env.data)
    upstream = os.path.join(root, "upstream")
    os.makedirs(upstream, exist_ok=True)
    monkeypatch.setattr(F, "_root", lambda: root)
    monkeypatch.setattr(config, "source_dir", lambda s: env.out)
    monkeypatch.setattr(blob, "publish_file", lambda path: os.path.getsize(path))

    class Sess:
        def get(self, url, headers=None, timeout=None, stream=False):
            name = url.rsplit("/", 1)[-1]
            if name in fail:
                raise OSError(f"connection reset on {name}")
            return _Resp(open(os.path.join(upstream, name), "rb").read())
    monkeypatch.setattr(F.requests, "Session", Sess)
    listing = {}
    for s in J.SECTOR_FILES:
        listing[s] = _entry(write_dump(upstream, s, "20260603", V1), "20260603")
    monkeypatch.setattr(F, "_listing", lambda sess=None: dict(listing))
    return F, upstream, listing


def test_vintage_is_stamped_when_clean_and_all_five_cubes_committed(env, monkeypatch):
    F, _up, listing = _fetcher_rig(env, monkeypatch)
    res = F.update(None, None)
    assert res.status == "ok", res
    assert J.all_committed([listing[s][0] for s in J.SECTOR_FILES])
    assert res.new_vintage == F._vintage_of(listing) and res.new_vintage.startswith("usda:")


def test_vintage_is_not_stamped_when_a_cube_is_not_a_build_of_the_listed_file(env, monkeypatch):
    """Nothing failed, but demographics is still the 20260603 build while the listing names
    20261118 - e.g. the ingest skipped it. The run must be DEMOTED to partial with a named unit,
    not merely return None (the strategy refills None; see the run_once test below)."""
    F, upstream, listing = _fetcher_rig(env, monkeypatch)
    assert F.update(None, None).new_vintage == F._vintage_of(listing)          # control
    listing["demographics"] = _entry(write_dump(upstream, "demographics", "20261118", V2), "20261118")
    monkeypatch.setattr(J, "main", lambda *a, **k: None)                       # no rebuild
    res = F.update(None, None)
    assert not J.all_committed([listing[s][0] for s in J.SECTOR_FILES])
    assert res.status == "partial", res
    assert "vintage_uncommitted" in (res.error or "")
    assert res.new_vintage is None


@pytest.fixture
def orch(env, monkeypatch, tmp_path):
    """The REAL orchestrator run_once over a temp state.db (pattern: test_date_tail_probe_seal)."""
    from updater import config, orchestrate
    from updater.state import StateStore
    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_LOCATION",
              "AQUEDUCT_RUN_BUDGET_MIN", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN",
              "ECONDL_DATA"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    # The CSV re-derive stage is not under test here: a clean no-op, so it cannot demote the run.
    monkeypatch.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, store=None: ([], None, [], {}))
    sdir = tmp_path / "state"
    sdir.mkdir()
    monkeypatch.setattr(config, "DATA_ROOT", os.path.dirname(env.out))
    monkeypatch.setattr(config, "STATE_DIR", str(sdir))
    monkeypatch.setattr(config, "STATE_DB", str(sdir / "state.db"))
    st = StateStore(str(sdir / "state.db"))

    def run(stored):
        st.upsert_unit("usda", "_all", strategy="bulk_snapshot_if_changed", status="ok",
                       upstream_vintage=stored, last_success_utc="2020-01-01T00:00:00+00:00",
                       last_attempt_utc="2020-01-01T00:00:00+00:00")
        results = orchestrate.run_once(sources=["usda"], store=st, blob=None, dry=False)
        row = st.db.execute("SELECT status, upstream_vintage FROM unit_state "
                            "WHERE source_id='usda' AND unit_id='_all'").fetchone()
        return results, tuple(row)
    yield run
    st.close()


def test_run_once_does_not_advance_the_vintage_when_a_cube_is_uncommitted(env, monkeypatch, orch):
    """END TO END: probe -> BulkSnapshotIfChanged.run (which refills a None new_vintage with the
    probed token) -> the orchestrator's unit_state write. In the ok-but-uncommitted case the
    stored upstream_vintage must NOT move to the listing hash."""
    F, upstream, listing = _fetcher_rig(env, monkeypatch)
    _res, (status, stored) = orch("usda:previous")                    # control: a clean run seals
    assert status == "ok" and stored == F._vintage_of(listing), (_res, status, stored)

    listing["demographics"] = _entry(write_dump(upstream, "demographics", "20261118", V2), "20261118")
    monkeypatch.setattr(J, "main", lambda *a, **k: None)              # the ingest rebuilds nothing
    new_hash = F._vintage_of(listing)
    results, (status, stored) = orch("usda:previous")
    assert status == "partial", (results, status, stored)
    assert stored == "usda:previous" and stored != new_hash, (results, status, stored)


def test_vintage_is_not_stamped_on_a_partial(env, monkeypatch):
    """One sector's download fails: transient unit -> partial -> None, whatever the cubes hold."""
    F, upstream, listing = _fetcher_rig(env, monkeypatch)
    F.update(None, None)
    new = write_dump(upstream, "crops", "20261118", V2)
    listing["crops"] = _entry(new, "20261118")
    monkeypatch.setattr(F.requests, "Session", type("S", (), {
        "get": lambda self, url, **k: (_ for _ in ()).throw(OSError("reset"))
        if url.endswith(os.path.basename(new)) else
        _Resp(open(os.path.join(upstream, url.rsplit("/", 1)[-1]), "rb").read())}))
    res = F.update(None, None)
    assert res.status == "partial", res
    assert "crops" in (res.error or "")
    assert res.new_vintage is None


def test_the_fetcher_rebuilds_publishes_the_live_parts_then_retires_old(env, monkeypatch):
    """update(): main(keep_old=True) -> publish exactly the live parts (while .old still exists) ->
    drop_all_old; manifest == live. No network: the listing and the download session are fakes."""
    import json
    from updater import blob, config
    from updater.strategies.fetchers import usda as F
    root = os.path.dirname(env.data)
    upstream = os.path.join(root, "upstream")
    os.makedirs(upstream)
    monkeypatch.setattr(F, "_root", lambda: root)
    monkeypatch.setattr(config, "source_dir", lambda s: env.out)

    class Sess:
        def get(self, url, headers=None, timeout=None, stream=False):
            return _Resp(open(os.path.join(upstream, url.rsplit("/", 1)[-1]), "rb").read())
    monkeypatch.setattr(F.requests, "Session", Sess)

    listing = {}
    for s in J.SECTOR_FILES:
        p = write_dump(upstream, s, "20260603", V1)
        listing[s] = _entry(p, "20260603")
    monkeypatch.setattr(F, "_listing", lambda sess=None: dict(listing))

    events = []
    real_main, real_drop = J.main, J.drop_all_old

    def main_spy(*a, **k):
        events.append(("main", k.get("keep_old")))
        return real_main(*a, **k)

    def drop_spy():
        events.append(("drop", None))
        return real_drop()

    def publish_spy(path):
        cube = os.path.basename(os.path.dirname(path))
        events.append(("publish", os.path.relpath(path, env.out).replace(os.sep, "/"),
                       os.path.isdir(os.path.join(J.staging_root(), cube + ".old"))))
        return os.path.getsize(path)
    monkeypatch.setattr(J, "main", main_spy)
    monkeypatch.setattr(J, "drop_all_old", drop_spy)
    monkeypatch.setattr(blob, "publish_file", publish_spy)

    res = F.update(None, None)
    assert res.status == "ok", res
    assert res.new_vintage == F._vintage_of(listing)
    p = write_dump(upstream, "demographics", "20261118", V2)
    listing["demographics"] = _entry(p, "20261118")
    events.clear()
    res = F.update(None, None)
    assert res.status == "ok", res
    assert res.new_vintage == F._vintage_of(listing)

    assert events[0] == ("main", True), events[:2]
    pubs = [e for e in events if e[0] == "publish"]
    assert events[-1] == ("drop", None) and all(events.index(e) < len(events) - 1 for e in pubs)
    want = sorted(os.path.relpath(q, env.out).replace(os.sep, "/") for q in J.served_parts())
    assert sorted(e[1] for e in pubs) == want, "publish exactly the live parts"
    assert all(".old" not in e[1] and ".new" not in e[1] for e in pubs)
    assert any(e[2] for e in pubs if e[1].startswith("demographics/")), \
        "the previous copy must still exist while the publish runs"
    assert _stage_entries() == [], "and is retired after it"
    assert cube_rows(env.live()) == expect(V2)
    m = json.load(open(os.path.join(env.out, "_manifest.json"), encoding="utf-8"))
    live_rows = {g: sum(pq.read_metadata(q).num_rows for q in J.cube_parts(env.live(g)))
                 for g in J.SECTOR_FILES}
    assert {g: m["cube_detail"][g]["rows"] for g in J.SECTOR_FILES} == live_rows
    assert m["cube_detail"]["demographics"]["rows"] == 5


def test_stale_apply_refuses_when_a_live_key_on_r2_holds_old_bytes(env, monkeypatch):
    """An interrupted publish can leave OLD bytes under a low-numbered key: same key set, wrong bytes."""
    from updater import blob
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    _FakeR2.objects = {}
    monkeypatch.setattr(blob, "R2Blob", _FakeR2)
    tool = _load_stale_tool()
    for s in J.SECTOR_FILES:
        write_dump(env.raw, s, "20260603", V1)
    J.main()
    old_bytes = {blob._path_to_key(p): open(p, "rb").read() for p in J.served_parts()}
    write_dump(env.raw, "demographics", "20261118", V2)
    J.main()
    for p in J.served_parts():                                   # publish everything ...
        _FakeR2.objects[blob._path_to_key(p)] = open(p, "rb").read()
    k0 = "clean_full/usda/demographics/part_000.parquet"
    _FakeR2.objects[k0] = old_bytes[k0]                          # ... but part_000 kept its old bytes
    for k in ("clean_full/usda/demographics/part_002.parquet", "clean_full/usda/demographics/part_003.parquet"):
        _FakeR2.objects[k] = old_bytes[k]
    assert tool.main(["--apply"]) == 1
    assert "clean_full/usda/demographics/part_003.parquet" in _FakeR2.objects, "nothing deleted"
