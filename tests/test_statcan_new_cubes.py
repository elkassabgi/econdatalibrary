"""tools/statcan_new_cubes.py - a cube the statcan lane lists as NEW is stored, served and catalogued like a
held cube, and handed to the lane; and every guard refuses before anything is written."""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import importlib.util
import io
import json
import os
import sqlite3
import sys
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_statcan as ing  # noqa: E402
import jobs.statcan_lane as lane  # noqa: E402
from core import cutover  # noqa: E402
from updater import blob, writer_lock  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, *rel.split("/")))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


tool = _load("_statcan_new_cubes_under_test", "tools/statcan_new_cubes.py")

NEW_PID = "33101178"
HELD_PID = "24100058"
REL = "2026-08-31T12:30"
TITLE = "Business or organization expectations over the next three months, third quarter of 2026"
HDR = ["REF_DATE", "GEO", "DGUID", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR",
       "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"]


def _row(ref, geo, vec, coord, val):
    return [ref, geo, "", "Percent", "1", "units", "0", vec, coord, val, "", "", "", "0"]


ROWS = [_row("2026-01", "Windsor - other locations", "v1", "1.1", "1"),
        _row("2026-02", "Windsor - other locations", "v1", "1.1", "2.5"),
        _row("2026-03", "Windsor - other locations", "v1", "1.1", "3"),
        _row("2026-01", "Windsor - other locations", "v2", "1.2", "5"),
        _row("2026-01", "Aden", "v3", "2.1", "7")]


def _zip(path, rows, pid=NEW_PID):
    buf = io.StringIO()
    buf.write(",".join(f'"{h}"' for h in HDR) + "\n")
    for r in rows:
        buf.write(",".join(f'"{c}"' for c in r) + "\n")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{pid}.csv", buf.getvalue())
        z.writestr(f"{pid}_MetaData.csv", "meta\n")


class _R2:
    """The bucket behind updater.blob.R2Blob - put_object and head_object, what put_atomic uses."""

    def __init__(self):
        self.objects = {}
        self.fail = False

    def put_object(self, Bucket, Key, Body, **kw):
        assert Bucket == blob.R2_BUCKET, Bucket
        if self.fail:
            raise ValueError("refused")
        self.objects[Key] = (Body, kw)

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        body, kw = self.objects[Key]
        return {"Metadata": dict(kw.get("Metadata") or {}), "ETag": '"%s"' % hashlib.md5(body).hexdigest()}

    def served(self):
        return {k: gzip.decompress(b).decode("utf-8") for k, (b, _kw) in self.objects.items()}


def _catalogue(path):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER,
            commercial_ok INTEGER, attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT);
        CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT,
            attribution TEXT, terms_url TEXT);
        CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT,
            unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,
            last_updated TEXT, metadata TEXT);
        CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
        INSERT INTO license VALUES ('statcan-open', 'Statistics Canada Open Licence', 1, 1, 1, 0, NULL);
        INSERT INTO source VALUES ('statcan', 'Statistics Canada', 'https://www150.statcan.gc.ca/',
            'statcan-open', 'Source: Statistics Canada.', NULL);
        INSERT INTO series (series_id, source_id, title) VALUES ('statcan:24100058#Aden', 'statcan', 'held');
        INSERT INTO series_fts VALUES ('statcan:24100058#Aden', 'held', 'Canada');
    """)
    con.commit()
    con.close()


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "_spill"))
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    d = tmp_path / "data" / "clean_full" / "statcan"
    d.mkdir(parents=True)
    monkeypatch.setattr(sc, "OUT_DIR", str(d))
    monkeypatch.setattr(sc, "TMP_DIR", str(d / "_incr_tmp"))
    for name, base in (("STATE", "_lane_state.json"), ("PROGRESS", "_lane_progress.json"),
                       ("DEBT", "_catalogue_debt.json")):
        monkeypatch.setattr(lane, name, str(d / base))
    monkeypatch.setattr(lane, "LOCAL_PROGRESS", str(tmp_path / "logs" / "statcan_lane.progress.json"))
    monkeypatch.setattr(lane, "SPLIT_MAP", str(d / "_split_map.json"))
    monkeypatch.setattr(lane, "CATALOGUE", str(tmp_path / "data" / "catalog.db"))
    monkeypatch.setattr(lane, "MAX_ROWS", 4)                    # 5 rows: the new cube must split
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr(tool, "CATALOGUE", str(tmp_path / "data" / "catalog.db"))
    monkeypatch.setattr(tool, "RECEIPTS", str(tmp_path / "logs" / "statcan_new_cubes"))
    monkeypatch.setattr(tool, "LIVE_ROOT", str(tmp_path))          # tmp_path plays the live checkout
    monkeypatch.setattr(sc, "_LITE_FLOOR", 1)
    (d / "_split_map.json").write_text(json.dumps({HELD_PID: {"dim": "geo", "parts": 2, "rows": 9}}))
    (d / f"{HELD_PID}.parquet").write_bytes(b"held")
    _catalogue(tmp_path / "data" / "catalog.db")
    (d / "_lane_state.json").write_text(json.dumps({
        "version": 1, "floor": "2026-07-29", "parsed_rows": {}, "last_enum_utc": "2026-10-17T20:00:00Z",
        "cubes": {NEW_PID: {"release": REL, "merged": REL, "served": REL, "not_held": True,
                            "bytes": None, "est_min": None}},
        "new_cubes": {NEW_PID: REL}}))
    (d / "_catalogue_debt.json").write_text(json.dumps({"cubes": {}, "new_cubes": {NEW_PID: REL}}))
    r2 = _R2()
    monkeypatch.setattr(blob.R2Blob, "client", property(lambda self: r2))
    net = {"counts": (5, 3), "rows": ROWS, "lite": True, "downloads": 0}

    def _get(ep, **k):
        if ep == "getAllCubesListLite":
            cubes = [{"productId": int(HELD_PID), "cubeTitleEn": "held"}]
            if net["lite"]:
                cubes.append({"productId": int(NEW_PID), "cubeTitleEn": TITLE, "cansimId": None,
                              "frequencyCode": 18, "archived": "2", "subjectCode": ["33"]})
            return cubes
        return {"status": "SUCCESS", "object": "https://example.invalid/t.zip"}

    def _download(url, dest, tries=5):
        net["downloads"] += 1
        _zip(dest, net["rows"])
        return os.path.getsize(dest)
    monkeypatch.setattr(sc, "_get", _get)
    monkeypatch.setattr(sc, "_cube_counts", lambda pid: net["counts"])
    net["meta_status"] = "SUCCESS"

    def _post(ep, payload, **k):                       # getCubeMetadata - the withdrawn evidence
        status = "FAILED" if not net["lite"] and net["meta_status"] == "SUCCESS" else net["meta_status"]
        return [{"status": status, "object": "The cube product ID does not exist." if status == "FAILED" else {}}]
    monkeypatch.setattr(sc, "_post", _post)
    monkeypatch.setattr(sc, "_download", _download)
    return {"d": d, "tmp": tmp_path, "r2": r2, "net": net}


def _args(**kw):
    import argparse
    return argparse.Namespace(**{"apply": False, "fetch": False, "pids": None, "max_minutes": 90.0,
                                 "retry_refused": False, **kw})


def _snapshot(w):
    files = sorted(p.name for p in w["d"].rglob("*") if p.is_file())
    con = sqlite3.connect(w["tmp"] / "data" / "catalog.db")
    n = con.execute("SELECT count(*) FROM series").fetchone()[0]
    f = con.execute("SELECT count(*) FROM series_fts").fetchone()[0]
    con.close()
    state = (w["d"] / "_lane_state.json").read_text()
    smap = (w["d"] / "_split_map.json").read_text()
    return files, n, f, state, smap, dict(w["r2"].objects)


def _cat_rows(w, pid=NEW_PID):
    con = sqlite3.connect(w["tmp"] / "data" / "catalog.db")
    lo = f"statcan:{pid}"
    rows = con.execute("SELECT series_id, title, license_id, start_date, end_date FROM series "
                       "WHERE series_id >= ? AND series_id < ? ORDER BY 1", (lo, lo + "$")).fetchall()
    fts = sorted(r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_id LIKE ?",
                                           (lo + "%",)))
    con.close()
    return rows, fts


# --------------------------------------------------------------------------- #
# the whole path
# --------------------------------------------------------------------------- #
def test_a_dry_run_writes_nothing(world):
    before = _snapshot(world)
    assert tool.run(_args()) == 0
    assert tool.run(_args(fetch=True)) == 0
    assert world["net"]["downloads"] == 1
    assert _snapshot(world) == before
    assert not os.path.exists(tool.RECEIPTS)


def test_a_new_cube_is_stored_served_catalogued_and_handed_to_the_lane(world):
    assert tool.run(_args(apply=True)) == 0
    d = world["d"]
    # stored, with the ingester's own .done record (the cataloguer's title source)
    assert pq.read_metadata(d / f"{NEW_PID}.parquet").num_rows == 5
    done = json.loads((d / f"{NEW_PID}.done").read_text())
    assert done["title"] == TITLE and done["n_obs"] == 5 and done["file"] == f"{NEW_PID}.parquet"
    assert set(done) == set(ing.done_record({}, 1, {"n_series": 0, "series_capped": False, "n_obs": 0,
                                                    "n_null": 0, "skipped": {}, "start": None, "end": None},
                                            str(d / f"{NEW_PID}.parquet")))
    # split under the store's cap, recorded in the map beside the held cube's entry
    smap = json.loads((d / "_split_map.json").read_text())
    assert smap[NEW_PID] == {"dim": "geo", "parts": 2, "rows": 5} and HELD_PID in smap
    # served: one object per part, through the CSV store (gzip at rest)
    served = world["r2"].served()
    assert sorted(served) == ["series/statcan%3A33101178%23Aden.csv",
                              "series/statcan%3A33101178%23Windsor%20-%20other%20locations.csv"]
    assert served["series/statcan%3A33101178%23Aden.csv"] == "series_id,obs_date,value\nv3,2026-01-01,7.0\n"
    # catalogued in series AND series_fts, rows from the cataloguer's builder
    rows, fts = _cat_rows(world)
    assert [r[0] for r in rows] == fts == ["statcan:33101178#Aden",
                                           "statcan:33101178#Windsor - other locations"]
    assert rows[0][1] == f"{TITLE} — geo Aden" and rows[0][2] == "statcan-open"
    assert rows[1][3:] == ("2026-01-01", "2026-03-01")
    # handed to the lane: held now, out of the debt
    st = json.loads((d / "_lane_state.json").read_text())
    c = st["cubes"][NEW_PID]
    assert NEW_PID not in st["new_cubes"] and "not_held" not in c
    assert c["merged"] == c["served"] == REL and c["rows"] == 5
    assert NEW_PID not in json.loads((d / "_catalogue_debt.json").read_text())["new_cubes"]
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "done" and rec["ids"] == 2 and len(rec["csv_keys"]) == 2
    # a second run does nothing
    objs = dict(world["r2"].objects)
    assert tool.run(_args(apply=True)) == 0
    assert world["r2"].objects == objs and world["net"]["downloads"] == 1


def test_the_lane_then_refreshes_it_as_a_held_cube(world):
    assert tool.run(_args(apply=True)) == 0
    world["net"]["rows"] = ROWS[:-1] + [_row("2026-01", "Aden", "v3", "2.1", "8")]   # Aden revised
    world["r2"].objects.clear()
    lane.run(enumerate_releases=lambda since: {int(NEW_PID): "2026-11-30T12:30"})
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert st["cubes"][NEW_PID]["merged"] == st["cubes"][NEW_PID]["served"] == "2026-11-30T12:30"
    assert NEW_PID not in st["new_cubes"]
    assert world["r2"].served()["series/statcan%3A33101178%23Aden.csv"] == \
        "series_id,obs_date,value\nv3,2026-01-01,8.0\n"


def test_its_objects_are_byte_identical_to_the_derive_tools(world, monkeypatch, tmp_path):
    """Parity with tools/derive_statcan_tables.py's writing path for a cube with no public ids yet (it
    chooses the split as a first derive does) - both through the real shared CSV store."""
    assert tool.run(_args(apply=True)) == 0
    mine = world["r2"].served()
    t = _load("_dst_parity_new", "tools/derive_statcan_tables.py")
    other = tmp_path / "other"
    store = other / "data" / "clean_full" / "statcan"
    store.mkdir(parents=True)
    (other / "logs").mkdir()
    import shutil
    shutil.copy(world["d"] / f"{NEW_PID}.parquet", store / f"{NEW_PID}.parquet")
    (store / "_split_map.json").write_text(json.dumps({HELD_PID: {"dim": "geo", "parts": 2, "rows": 9}}))
    _catalogue(other / "catalog.db")
    r2 = _R2()
    monkeypatch.setattr(blob.R2Blob, "client", property(lambda self: r2))
    monkeypatch.setattr(t, "STORE", str(store))
    monkeypatch.setattr(t, "ROOT", str(other))
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", blob.R2_BUCKET, "--only", NEW_PID,
                                      "--max-rows", "4", "--catalog-db", str(other / "catalog.db")])
    assert t.main() == 0
    assert mine == r2.served() and len(mine) == 2
    assert json.loads((store / "_split_map.json").read_text())[NEW_PID]["dim"] == "geo"


def test_a_cube_under_the_cap_is_served_and_catalogued_whole(world, monkeypatch):
    monkeypatch.setattr(lane, "MAX_ROWS", 3_000_000)
    assert tool.run(_args(apply=True)) == 0
    assert NEW_PID not in json.loads((world["d"] / "_split_map.json").read_text())
    assert sorted(world["r2"].served()) == ["series/statcan%3A33101178.csv"]
    rows, fts = _cat_rows(world)
    assert [r[0] for r in rows] == fts == ["statcan:33101178"] and rows[0][1] == TITLE
    assert rows[0][3:] == ("2026-01-01", "2026-03-01")


# --------------------------------------------------------------------------- #
# refusals: nothing written
# --------------------------------------------------------------------------- #
def _nothing_written(w, before):
    files, n, f, state, smap, objs = _snapshot(w)
    assert (files, n, f, smap, objs) == before[:3] + before[4:], "something was written"
    assert json.loads(state)["new_cubes"] == {NEW_PID: REL}


def test_a_failed_completeness_gate_writes_nothing(world):
    """AR-340 finding 7: named as a refusal (incomplete against StatCan), never ingested, not fetched again."""
    before = _snapshot(world)
    world["net"]["counts"] = (6, 3)                        # StatCan says 6 datapoints; the parse has 5
    assert tool.run(_args(apply=True)) == 0
    _nothing_written(world, before)
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused" and rec["reason"].startswith("incomplete against StatCan")
    assert tool.run(_args(apply=True)) == 0 and world["net"]["downloads"] == 1


def test_a_vectorless_new_cube_is_refused(world):
    """AR-340 finding 7: StatCan's counts cannot prove it whole and there is no stored floor."""
    before = _snapshot(world)
    world["net"]["counts"] = (5, 1)                        # nbSeriesCube 1 for a table of 3 series
    assert tool.run(_args(apply=True)) == 0
    _nothing_written(world, before)
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused" and rec["reason"].startswith("vectorless")


def test_a_withdrawn_cube_is_refused_by_name_and_leaves_new_cubes(world):
    """AR-340 finding 5: with both pieces of evidence it moves to withdrawn_upstream (state and debt)."""
    before = _snapshot(world)
    world["net"]["lite"] = False
    assert tool.run(_args(apply=True)) == 0
    after = _snapshot(world)
    assert (after[0], after[1], after[2], after[4], after[5]) == (before[0], before[1], before[2], before[4],
                                                                  before[5])
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused" and "withdrawn" in rec["reason"]
    assert world["net"]["downloads"] == 0
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert NEW_PID not in st["new_cubes"] and "getCubeMetadata FAILED" in st["withdrawn_upstream"][NEW_PID]["evidence"]
    assert NEW_PID not in json.loads((world["d"] / "_catalogue_debt.json").read_text())["new_cubes"]


def test_absent_from_the_list_but_known_to_the_metadata_is_not_called_withdrawn(world):
    world["net"]["lite"] = False
    world["net"]["meta_status"] = "SUCCESS-KEEP"
    assert tool.run(_args(apply=True)) == 0
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert NEW_PID in st["new_cubes"] and NEW_PID not in st.get("withdrawn_upstream", {})


def test_a_splitter_refusal_writes_nothing(world, monkeypatch):
    before = _snapshot(world)
    monkeypatch.setattr(tool, "choose", lambda path, n: ("", 0))
    assert tool.run(_args(apply=True)) == 0
    _nothing_written(world, before)
    assert not os.listdir(world["d"] / "_incr_tmp")


@pytest.mark.parametrize("held", ["store", "parquet", "done", "map", "catalogue"])
def test_a_cube_held_anywhere_is_refused(world, held, monkeypatch):
    d = world["d"]
    if held == "store":                                    # in the store of record (R2), not the mirror
        real = blob.exists
        monkeypatch.setattr(blob, "exists", lambda p: p.endswith(f"{NEW_PID}.parquet") or real(p))
    elif held == "parquet":
        (d / f"{NEW_PID}.parquet").write_bytes(b"x")
    elif held == "done":
        (d / f"{NEW_PID}.done").write_text("{}")
    elif held == "map":
        m = json.loads((d / "_split_map.json").read_text())
        m[NEW_PID] = {"dim": "uom", "parts": 2, "rows": 5}
        (d / "_split_map.json").write_text(json.dumps(m))
    else:
        con = sqlite3.connect(world["tmp"] / "data" / "catalog.db")
        con.execute("INSERT INTO series (series_id) VALUES ('statcan:33101178#x')")
        con.commit()
        con.close()
    before = _snapshot(world)
    assert tool.run(_args(apply=True)) == 0
    assert _snapshot(world)[1:] == before[1:] and world["net"]["downloads"] == 0
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused" and "already held" in rec["reason"]


def test_only_the_lanes_new_cubes_are_in_scope(world):
    with pytest.raises(SystemExit, match="not in the lane's new_cubes"):
        tool.run(_args(apply=True, pids=HELD_PID))


def test_after_t0_it_refuses(world, monkeypatch):
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    before = _snapshot(world)
    with pytest.raises(cutover.CutoverRefused):
        tool.run(_args(apply=True))
    with pytest.raises(cutover.CutoverRefused):
        tool.main(["--apply"])
    _nothing_written(world, before)


def test_apply_runs_only_from_the_live_checkout(world, monkeypatch, tmp_path):
    monkeypatch.setattr(tool, "LIVE_ROOT", str(tmp_path / "elsewhere"))
    monkeypatch.setattr(lane, "pin_backend", lambda *a, **k: None)   # it sets os.environ itself
    before = _snapshot(world)
    with pytest.raises(SystemExit, match="only the live checkout's files"):
        tool.run(_args(apply=True))
    with pytest.raises(SystemExit, match="only the live checkout's files"):
        tool.main(["--apply"])
    _nothing_written(world, before)
    assert tool.run(_args()) == 0                          # a DRY run is fine anywhere


def test_apply_refuses_while_the_lane_holds_the_writer_lock(world, monkeypatch):
    monkeypatch.setattr(writer_lock, "acquire", lambda *a, **k: False)
    monkeypatch.setattr(writer_lock, "owner", lambda name: {"pid": 48712, "what": "jobs/statcan_lane.py"})
    monkeypatch.setattr(lane, "pin_backend", lambda *a, **k: None)
    before = _snapshot(world)
    with pytest.raises(SystemExit, match="held by pid 48712"):
        tool.main(["--apply"])
    _nothing_written(world, before)


def test_main_pins_the_r2_backend_and_refuses_another(world, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "selfhost")
    with pytest.raises(SystemExit, match="must be r2"):
        tool.main([])


# --------------------------------------------------------------------------- #
# resumption
# --------------------------------------------------------------------------- #
def test_a_failed_put_leaves_no_catalogue_row_and_a_rerun_resumes_at_serving(world):
    world["r2"].fail = True
    assert tool.run(_args(apply=True)) == 1
    rows, fts = _cat_rows(world)
    assert rows == [] and fts == []
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "committed"
    assert NEW_PID in json.loads((world["d"] / "_lane_state.json").read_text())["new_cubes"]
    world["r2"].fail = False
    assert tool.run(_args(apply=True)) == 0
    assert world["net"]["downloads"] == 1, "the stored cube is not fetched again"
    rows, fts = _cat_rows(world)
    assert len(rows) == len(fts) == 2


def test_a_run_stopped_after_the_lane_record_finishes_without_recording_twice(world, monkeypatch):
    real = tool.save_receipt
    calls = {"n": 0}

    def stop_before_last(rec):
        if rec.get("stage") == "done":
            raise OSError("killed")
        real(rec)
    monkeypatch.setattr(tool, "save_receipt", stop_before_last)
    assert tool.run(_args(apply=True)) == 1
    st1 = json.loads((world["d"] / "_lane_state.json").read_text())
    assert NEW_PID not in st1["new_cubes"]
    monkeypatch.setattr(tool, "save_receipt", real)
    assert tool.run(_args(apply=True)) == 0
    assert json.loads((world["d"] / "_lane_state.json").read_text()) == st1
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "done" and calls["n"] == 0


def test_an_fts_id_present_after_commit_rolls_the_catalogue_back(world, monkeypatch):
    world["r2"].fail = True
    assert tool.run(_args(apply=True)) == 1                        # committed, not served
    con = sqlite3.connect(world["tmp"] / "data" / "catalog.db")
    con.execute("INSERT INTO series_fts VALUES ('statcan:33101178#Aden', 'stray', 'Canada')")
    con.commit()
    con.close()
    world["r2"].fail = False
    assert tool.run(_args(apply=True)) == 1                        # refused part-way: a failure
    rows, _fts = _cat_rows(world)
    assert rows == []


# --------------------------------------------------------------------------- #
# the two refactors, and what the tool never touches
# --------------------------------------------------------------------------- #
def test_cube_rows_is_the_cataloguers_row_builder(world, tmp_path):
    cat = _load("_cat_rows_test", "tools/catalog_statcan_tables.py")
    p = tmp_path / "c.parquet"
    zp = tmp_path / "c.zip"
    _zip(zp, ROWS)
    st = ing.parse_zip_to_parquet(str(zp), str(p))
    side = ing.done_record({"cubeTitleEn": TITLE, "frequencyCode": 18, "archived": "2",
                            "subjectCode": ["33"]}, 33101178, st, str(p))
    whole, untitled = cat.cube_rows(NEW_PID, str(p).replace("\\", "/"), side, None, str(tmp_path))
    assert not untitled and len(whole) == 1
    sid, src, title, freq, unit, geo, catg, lic, d0, d1, meta = whole[0]
    assert (sid, src, title, freq, geo, lic, d0, d1) == (f"statcan:{NEW_PID}", "statcan", TITLE, "M",
                                                         "Canada", "statcan-open", "2026-01-01",
                                                         "2026-03-01")
    assert json.loads(meta) == {**cat.BASE_META, "archived": "2", "subject_code": ["33"]}
    parts, _ = cat.cube_rows(NEW_PID, str(p).replace("\\", "/"), side, {"dim": "geo"}, str(tmp_path))
    assert [(r[0], r[2], r[8], r[9]) for r in parts] == [
        ("statcan:33101178#Aden", f"{TITLE} — geo Aden", "2026-01-01", "2026-01-01"),
        ("statcan:33101178#Windsor - other locations", f"{TITLE} — geo Windsor - other locations",
         "2026-01-01", "2026-03-01")]
    nameless, untitled = cat.cube_rows(NEW_PID, str(p).replace("\\", "/"), {}, None, str(tmp_path))
    assert untitled and nameless[0][2] == NEW_PID              # no title: the Product ID, never invented


def test_the_tool_names_no_d1_and_no_aqueduct_key():
    src = open(os.path.join(ROOT, "tools", "statcan_new_cubes.py"), encoding="utf-8").read().lower()
    for word in ("d1_remote", "wrangler", "sync_catalog_d1", "_aqueduct", "d1 execute"):
        assert word not in src, word


def test_a_stray_row_in_the_cubes_range_rolls_the_whole_insert_back(world):
    world["r2"].fail = True
    assert tool.run(_args(apply=True)) == 1                        # committed, not served
    con = sqlite3.connect(world["tmp"] / "data" / "catalog.db")
    con.execute("INSERT INTO series (series_id) VALUES ('statcan:33101178#zzz')")
    con.commit()
    con.close()
    world["r2"].fail = False
    assert tool.run(_args(apply=True)) == 1                        # 3 rows in the range, 2 wanted
    rows, fts = _cat_rows(world)
    assert [r[0] for r in rows] == ["statcan:33101178#zzz"] and fts == []


def test_a_run_killed_after_the_publish_resumes_without_a_second_fetch(world, monkeypatch):
    real = tool.finish_commit
    monkeypatch.setattr(tool, "finish_commit", lambda *a: (_ for _ in ()).throw(OSError("killed")))
    assert tool.run(_args(apply=True)) == 1
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "committing" and (world["d"] / f"{NEW_PID}.parquet").exists()
    assert not (world["d"] / f"{NEW_PID}.done").exists()
    monkeypatch.setattr(tool, "finish_commit", real)
    assert tool.run(_args(apply=True)) == 0
    assert world["net"]["downloads"] == 1
    assert json.loads((world["d"] / f"{NEW_PID}.done").read_text())["title"] == TITLE
    assert len(_cat_rows(world)[0]) == 2


def test_a_publish_that_reached_the_mirror_only_is_published_again(world, monkeypatch):
    calls = {"n": 0}
    real_commit, real_exists = blob.commit_local_file, blob.exists

    def commit(src, dst):
        calls["n"] += 1
        real_commit(src, dst)

    def exists(p):                                     # the store PUT of the first publish was lost
        if p.endswith(f"{NEW_PID}.parquet"):
            return calls["n"] >= 2
        return real_exists(p)
    monkeypatch.setattr(blob, "commit_local_file", commit)
    monkeypatch.setattr(blob, "exists", exists)
    assert tool.run(_args(apply=True)) == 0
    assert calls["n"] == 2 and world["net"]["downloads"] == 1
    assert pq.read_metadata(world["d"] / f"{NEW_PID}.parquet").num_rows == 5


def test_a_run_killed_after_the_catalogue_commit_finishes_without_inserting_twice(world, monkeypatch):
    real = tool.save_receipt

    def stop(rec):
        if rec.get("stage") == "catalogued":
            raise OSError("killed")
        real(rec)
    monkeypatch.setattr(tool, "save_receipt", stop)
    assert tool.run(_args(apply=True)) == 1
    monkeypatch.setattr(tool, "save_receipt", real)
    assert tool.run(_args(apply=True)) == 0
    rows, fts = _cat_rows(world)
    assert len(rows) == len(fts) == 2
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "done" and rec["catalogued_by"] == "an earlier run"


# --------------------------------------------------------------------------- #
# review findings F4-F9 (2026-10-17)
# --------------------------------------------------------------------------- #
def test_the_lane_taking_the_cube_over_between_two_runs_is_finished_not_refused(world):
    """F4: a new release reaches the lane while the cube is committed but not yet catalogued. The lane merges
    the parquet this tool published and serves nothing (no catalogued id yet). The next tool run must
    catalogue it, close the new-cube debt and owe the lane a serve, and the lane must then serve it."""
    world["r2"].fail = True
    assert tool.run(_args(apply=True)) == 1                        # committed, not served
    world["r2"].fail = False
    rel2 = "2026-11-30T12:30"
    world["net"]["rows"] = ROWS[:-1] + [_row("2026-01", "Aden", "v3", "2.1", "8")]
    lane.run(enumerate_releases=lambda since: {int(NEW_PID): rel2})
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert "not_held" not in st["cubes"][NEW_PID] and NEW_PID in st["new_cubes"]
    world["r2"].objects.clear()
    assert tool.run(_args(apply=True)) == 0
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert NEW_PID not in st["new_cubes"] and st["cubes"][NEW_PID]["served"] is None
    assert NEW_PID not in json.loads((world["d"] / "_catalogue_debt.json").read_text())["new_cubes"]
    world["r2"].objects.clear()
    lane.run(enumerate_releases=lambda since: {int(NEW_PID): rel2})
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert st["cubes"][NEW_PID]["served"] == rel2
    want = "series_id,obs_date,value" + chr(10) + "v3,2026-01-01,8.0" + chr(10)
    assert world["r2"].served()["series/statcan%3A33101178%23Aden.csv"] == want
    assert NEW_PID not in json.loads((world["d"] / "_catalogue_debt.json").read_text()).get("cubes", {})


def test_the_run_starts_no_cube_past_its_time_budget(world):
    """F5: the lane cannot beat while this run holds the lock; past --max-minutes no new cube starts."""
    before = _snapshot(world)
    assert tool.run(_args(apply=True, max_minutes=0)) == 0
    _nothing_written(world, before)
    assert world["net"]["downloads"] == 0
    logs = world["tmp"] / "logs" / "statcan_new_cubes"
    runs = [p for p in os.listdir(logs) if p.startswith("_run_")]
    assert json.loads((logs / runs[0]).read_text())["deferred_by_budget"] == [NEW_PID]


def test_a_whole_cube_with_no_value_is_refused_before_anything_is_written(world, monkeypatch):
    """F6: the sidecar's dates would give it a catalogue id, but the serving scan writes nothing for it."""
    monkeypatch.setattr(lane, "MAX_ROWS", 3_000_000)
    world["net"]["rows"] = [_row("2026-01", "Aden", "v3", "2.1", "..")]
    world["net"]["counts"] = (1, 1)
    before = _snapshot(world)
    assert tool.run(_args(apply=True)) == 0
    _nothing_written(world, before)
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused" and "catalogue 1 id(s) and serve 0" in rec["reason"]


@pytest.mark.parametrize("break_it", ["licence", "source", "fts"])
def test_a_catalogue_that_cannot_take_the_cube_refuses_before_the_publish(world, break_it):
    """F7: the licence, source and series_fts checks run in `prepare`, before any object is written."""
    con = sqlite3.connect(world["tmp"] / "data" / "catalog.db")
    if break_it == "licence":
        con.execute("UPDATE license SET reservable=0")
    elif break_it == "source":
        con.execute("DELETE FROM source")
    else:
        con.execute("INSERT INTO series_fts VALUES ('statcan:33101178#Aden', 'stray', 'Canada')")
    con.commit()
    con.close()
    before = _snapshot(world)
    assert tool.run(_args(apply=True)) == 0
    after = _snapshot(world)
    assert after[0] == before[0] and after[4:] == before[4:], "an object or the map was written"
    rec = json.loads((world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json").read_text())
    assert rec["stage"] == "refused"


@pytest.mark.parametrize("which", ["store dir", "lock dir", "catalogue"])
def test_apply_refuses_when_any_written_path_is_not_the_live_checkouts(world, monkeypatch, tmp_path, which):
    """F8: the guard checks the PATHS it writes, not only the code's location."""
    other = str(tmp_path / "elsewhere")
    if which == "store dir":
        monkeypatch.setattr(sc, "OUT_DIR", other)
    elif which == "lock dir":
        monkeypatch.setattr(writer_lock, "LOCK_DIR", other)
    else:
        monkeypatch.setattr(tool, "CATALOGUE", os.path.join(other, "catalog.db"))
    with pytest.raises(SystemExit, match="only the live checkout's files"):
        tool.run(_args(apply=True))
    assert world["net"]["downloads"] == 0


def test_a_refused_release_is_not_downloaded_again(world, monkeypatch):
    """F9: a giant the splitter refuses is ~0.6 GB; the same release is not fetched on every run."""
    monkeypatch.setattr(tool, "choose", lambda path, n: ("", 0))
    assert tool.run(_args(apply=True)) == 0
    assert tool.run(_args(apply=True)) == 0
    assert world["net"]["downloads"] == 1
    assert tool.run(_args(apply=True, retry_refused=True)) == 0
    assert world["net"]["downloads"] == 2


def test_a_row_appearing_inside_the_transaction_rolls_it_back(world):
    """The in-transaction check: a trigger adds a row in the cube's range during the INSERT; the count no
    longer matches and nothing of the cube is committed."""
    con = sqlite3.connect(world["tmp"] / "data" / "catalog.db")
    con.execute("CREATE TRIGGER ghost AFTER INSERT ON series WHEN NEW.series_id = 'statcan:33101178#Aden' "
                "BEGIN INSERT INTO series (series_id) VALUES ('statcan:33101178#ghost'); END")
    con.commit()
    con.close()
    assert tool.run(_args(apply=True)) == 1
    rows, fts = _cat_rows(world)
    assert rows == [] and fts == []


# --------------------------------------------------------------------------- #
# second review (VERDICT2, 2026-10-17) conditions C1, C3, C4
# --------------------------------------------------------------------------- #
def test_the_budget_counts_the_series_fts_read(world, monkeypatch):
    """C1: the clock starts at the lock, so a slow series_fts read uses up the budget instead of adding to it."""
    t = {"now": 60 * 60.0}                         # an hour passed between taking the lock and run()
    real = tool.load_fts

    def slow_read(pids):
        real(pids)
        t["now"] += 31 * 60.0                      # 91 min since the lock; only 31 since run() began
    monkeypatch.setattr(tool, "load_fts", slow_read)
    before = _snapshot(world)
    assert tool.run(_args(apply=True, max_minutes=90), clock=lambda: t["now"], started=0.0) == 0
    _nothing_written(world, before)
    assert world["net"]["downloads"] == 0


def test_main_starts_the_budget_when_it_takes_the_lock(world, monkeypatch):
    seen = {}
    monkeypatch.setattr(lane, "pin_backend", lambda *a, **k: None)
    monkeypatch.setattr(writer_lock, "hold_or_refuse", lambda *a, **k: seen.setdefault("lock", tool.time.monotonic()))
    monkeypatch.setattr(tool, "run", lambda a, started=None, **k: seen.setdefault("started", started) and 0)
    tool.main(["--apply"])
    assert seen["started"] is not None and seen["started"] >= seen["lock"]


def test_a_lane_merge_after_the_publish_and_before_the_done_record_is_carried_on(world, monkeypatch):
    """C3: killed between the publish and .done, then the lane merges a new release over the published
    parquet; the next run goes on with what is stored instead of refusing for ever."""
    real = tool.finish_commit
    monkeypatch.setattr(tool, "finish_commit", lambda *a: (_ for _ in ()).throw(OSError("killed")))
    assert tool.run(_args(apply=True)) == 1
    monkeypatch.setattr(tool, "finish_commit", real)
    world["net"]["rows"] = ROWS + [_row("2026-04", "Windsor - other locations", "v1", "1.1", "4")]
    world["net"]["counts"] = (6, 3)
    lane.run(enumerate_releases=lambda since: {int(NEW_PID): "2026-11-30T12:30"})
    assert pq.read_metadata(world["d"] / f"{NEW_PID}.parquet").num_rows == 6
    assert tool.run(_args(apply=True)) == 0
    rows, fts = _cat_rows(world)
    assert len(rows) == len(fts) == 2
    st = json.loads((world["d"] / "_lane_state.json").read_text())
    assert NEW_PID not in st["new_cubes"]


def test_a_repeated_refusal_keeps_its_first_reason(world, monkeypatch):
    """C4: the reason is not wrapped inside itself on every run."""
    monkeypatch.setattr(tool, "choose", lambda path, n: ("", 0))
    p = world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json"
    assert tool.run(_args(apply=True)) == 0
    first = json.loads(p.read_text())["reason"]
    for _ in range(3):
        assert tool.run(_args(apply=True)) == 0
    assert json.loads(p.read_text())["reason"] == first and "refused before" not in first


# --------------------------------------------------------------------------- #
# AR-340 (R1606) - the committing reset, the R2-routed lane files, and the required findings
# --------------------------------------------------------------------------- #
def test_ar340_a_committing_receipt_whose_publish_never_happened_is_reset_on_disk(world, monkeypatch):
    """R1606: the reset of a `committing` receipt with no parquet was in memory only, so a later refusal
    reloaded `committing` from disk and booked a part-way failure (rc 1) on every run."""
    monkeypatch.setattr(blob, "commit_local_file", lambda *a, **k: (_ for _ in ()).throw(OSError("put died")))
    assert tool.run(_args(apply=True)) == 1
    p = world["tmp"] / "logs" / "statcan_new_cubes" / f"{NEW_PID}.json"
    rec = json.loads(p.read_text())
    assert rec["stage"] == "committing" and not (world["d"] / f"{NEW_PID}.parquet").exists()
    world["net"]["lite"] = False                           # withdrawn upstream before the re-run
    assert tool.run(_args(apply=True)) == 0
    rec2 = json.loads(p.read_text())
    assert rec2["stage"] == "refused" and "withdrawn" in rec2["reason"], rec2
    assert tool.run(_args(apply=True)) == 0                # and it stays a refusal, never a failure


def test_a_quarantined_held_cube_is_refused_even_when_named(world):
    """AR-340 finding 9: the 4 quarantined held cubes are not in new_cubes, so --pids cannot reach them."""
    p = world["d"] / "_lane_state.json"
    st = json.loads(p.read_text())
    st["cubes"]["18100212"] = {"release": REL, "merged": "2026-09-01T12:30", "quarantined": True}
    p.write_text(json.dumps(st))
    with pytest.raises(SystemExit, match="not in the lane's new_cubes"):
        tool.run(_args(apply=True, pids="18100212"))


def test_the_tool_never_names_the_d1_sync_queue_or_its_manifest():
    """AR-340 finding 8: the rows are LOCAL ONLY; nothing may queue them for the frozen D1 sync."""
    src = open(os.path.join(ROOT, "tools", "statcan_new_cubes.py"), encoding="utf-8").read()
    for name in ("pending_catalog_sync", "catalog_sync_sent", "sync_catalog_d1"):
        assert name not in src, name


class _R2Store(_R2):
    """A fuller bucket for AQUEDUCT_BACKEND=r2: get_object / head_object answer a real 404 ClientError."""

    def _missing(self, op, key):
        from botocore.exceptions import ClientError
        return ClientError({"Error": {"Code": "404", "Message": key},
                            "ResponseMetadata": {"HTTPStatusCode": 404}}, op)

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self._missing("GetObject", Key)
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise self._missing("HeadObject", Key)
        body, kw = self.objects[Key]
        return {"Metadata": dict(kw.get("Metadata") or {}), "ETag": '"%s"' % hashlib.md5(body).hexdigest(),
                "ContentLength": len(body)}

    def plain(self, key):
        return self.objects[key][0]


def test_under_the_r2_backend_the_lane_files_and_the_parquet_are_written_to_the_store(world, monkeypatch):
    """AR-340 finding 3 / M11: the lane state, the debt and the parquet live in the R2 store before T0, so they
    are read and written THROUGH blob. A local-only write leaves R2's debt still listing the cube."""
    pytest.importorskip("botocore")
    r2 = _R2Store()
    d = world["d"]
    for name in ("_lane_state.json", "_catalogue_debt.json"):
        r2.objects[f"clean_full/statcan/{name}"] = ((d / name).read_bytes(), {})
    monkeypatch.setattr(blob.R2Blob, "client", property(lambda self: r2))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    assert tool.run(_args(apply=True)) == 0
    st = json.loads(r2.plain("clean_full/statcan/_lane_state.json"))
    assert NEW_PID not in st["new_cubes"] and "not_held" not in st["cubes"][NEW_PID]
    debt = json.loads(r2.plain("clean_full/statcan/_catalogue_debt.json"))
    assert NEW_PID not in debt["new_cubes"]
    held = r2.plain(f"clean_full/statcan/{NEW_PID}.parquet")
    assert pq.read_metadata(io.BytesIO(held)).num_rows == 5
    assert sorted(k for k in r2.objects if k.startswith("series/")) == [
        "series/statcan%3A33101178%23Aden.csv", "series/statcan%3A33101178%23Windsor%20-%20other%20locations.csv"]
