"""The statcan lane (jobs/statcan_lane.py), its reporter, and the single-writer lock (2026-09-23).

These tests fake ONLY the network and the bucket. The store is a real directory (local backend), the
parse is the real jobs/ingest_statcan.parse_zip_to_parquet, the gates are the fetcher's own, the merge
is the real merge.merge_and_write_bounded and the serving scan is real DuckDB - a model of those would
not catch them drifting.
"""
from __future__ import annotations

import datetime as dt
import gzip
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
import zipfile

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_statcan as ing  # noqa: E402
import jobs.statcan_lane as lane  # noqa: E402
from updater import orchestrate, registry, writer_lock  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402

PID = 24100058
REL = "2026-09-12T08:30"
HDR = ["REF_DATE", "GEO", "DGUID", "UOM", "UOM_ID", "SCALAR_FACTOR", "SCALAR_ID", "VECTOR",
       "COORDINATE", "VALUE", "STATUS", "SYMBOL", "TERMINATED", "DECIMALS"]


def _row(ref, geo, vec, coord, val):
    return [ref, geo, "", "Vehicles", "1", "units", "0", vec, coord, val, "", "", "", "0"]


def _zip(path, rows, pid=PID):
    buf = io.StringIO()
    buf.write(",".join(f'"{h}"' for h in HDR) + "\n")
    for r in rows:
        buf.write(",".join(f'"{c}"' for c in r) + "\n")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{pid}.csv", buf.getvalue())
        z.writestr(f"{pid}_MetaData.csv", "meta\n")


def _stored(path, rows):
    """rows: (series_key, 'YYYY-MM-DD', value, geo, coordinate)"""
    t = pa.table({
        "series_key": [r[0] for r in rows],
        "obs_date": pa.array([dt.date.fromisoformat(r[1]) for r in rows], pa.date32()),
        "value": [r[2] for r in rows], "geo": [r[3] for r in rows],
        "uom": ["Vehicles"] * len(rows), "coordinate": [r[4] for r in rows],
        "status": [""] * len(rows)}, schema=ing.SCHEMA)
    pq.write_table(t, path)


# the refreshed table: Windsor relabelled, Aden revised, v1 extended by a month
NEW = [_row("2026-01", "Windsor - other locations", "v1", "1.1", "1"),
       _row("2026-02", "Windsor - other locations", "v1", "1.1", "2.5"),
       _row("2026-03", "Windsor - other locations", "v1", "1.1", "3"),
       _row("2026-01", "Windsor - other locations", "v2", "1.2", "5"),
       _row("2026-01", "Aden", "v3", "2.1", "7")]


class _Bucket:
    def __init__(self):
        self.put = {}

    def __call__(self, key, body):
        assert body[:2] == b"\x1f\x8b", "served bodies are gzip at rest"
        self.put[key] = gzip.decompress(body).decode("utf-8")


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "_spill"))
    d = tmp_path / "data" / "clean_full" / "statcan"
    d.mkdir(parents=True)
    monkeypatch.setattr(sc, "OUT_DIR", str(d))
    monkeypatch.setattr(sc, "TMP_DIR", str(d / "_incr_tmp"))
    for name in ("STATE", "PROGRESS", "DEBT"):
        monkeypatch.setattr(lane, name, str(d / f"_lane_{name.lower()}.json"))
    monkeypatch.setattr(lane, "LOCAL_PROGRESS", str(tmp_path / "logs" / "statcan_lane.progress.json"))
    monkeypatch.setattr(lane, "SPLIT_MAP", str(d / "_split_map.json"))
    monkeypatch.setattr(lane, "CATALOG_DB", str(tmp_path / "catalog.db"))
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path / "logs"))
    _stored(d / f"{PID}.parquet", [("v1", "2026-01-01", 1.0, "Windsor", "1.1"),
                                   ("v1", "2026-02-01", 2.0, "Windsor", "1.1"),
                                   ("v2", "2026-01-01", 5.0, "Windsor", "1.2"),
                                   ("v3", "2026-01-01", 6.0, "Aden", "2.1")])
    smap = {str(PID): {"dim": "geo", "parts": 2, "rows": 4},
            "33333333": {"dim": "uom", "parts": 4, "rows": 900}}
    (d / "_split_map.json").write_text(json.dumps(smap))
    con = sqlite3.connect(tmp_path / "catalog.db")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO series VALUES (?)", [(f"statcan:{PID}#Windsor",),
                                                      (f"statcan:{PID}#Aden",)])
    con.commit()
    con.close()
    return d


def _net(monkeypatch, rows, counts, *, corrupt=False, calls=None):
    monkeypatch.setattr(sc, "_cube_counts", lambda pid: counts)
    monkeypatch.setattr(sc, "_get", lambda ep, **k: {"status": "SUCCESS",
                                                     "object": "https://example.invalid/t.zip"})

    def _download(url, dest, tries=5):
        if calls is not None:
            calls.append(url)
        _zip(dest, rows)
        if corrupt:
            with open(dest, "r+b") as f:
                f.truncate(os.path.getsize(dest) // 2)
        return os.path.getsize(dest)
    monkeypatch.setattr(sc, "_download", _download)


def _rows(path):
    t = pq.read_table(path).sort_by([("series_key", "ascending"), ("obs_date", "ascending")])
    return [(r["series_key"], r["obs_date"].isoformat(), r["value"], r["geo"]) for r in t.to_pylist()]


def _state(d):
    return json.loads((d / "_lane_state.json").read_text())


def _hours(h):
    """A clock h hours ahead: an idle lane re-enumerates only past ENUM_MIN_INTERVAL_MIN."""
    return lambda: lane._now() + dt.timedelta(hours=h)


def _rel(mapping):
    return lambda since: dict(mapping)


# --------------------------------------------------------------------------- #
# refresh, then serve
# --------------------------------------------------------------------------- #
def test_a_released_cube_is_merged_then_its_catalogued_parts_served(world, monkeypatch):
    _net(monkeypatch, NEW, (5, 3))
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)
    assert _rows(world / f"{PID}.parquet") == [
        ("v1", "2026-01-01", 1.0, "Windsor - other locations"),
        ("v1", "2026-02-01", 2.5, "Windsor - other locations"),
        ("v1", "2026-03-01", 3.0, "Windsor - other locations"),
        ("v2", "2026-01-01", 5.0, "Windsor - other locations"),
        ("v3", "2026-01-01", 7.0, "Aden")]
    # ONLY a catalogued id is written; the relabelled part has no catalogue row yet
    assert b.put == {"series/statcan%3A24100058%23Aden.csv": "series_id,obs_date,value\nv3,2026-01-01,7.0\n"}
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == REL and c["served"] == REL and c["max_obs"] == "2026-03-01"
    debt = json.loads((world / "_lane_debt.json").read_text())["cubes"][str(PID)]
    assert debt["new"] == [f"statcan:{PID}#Windsor - other locations"]
    assert debt["vanished"] == [f"statcan:{PID}#Windsor"], "the stranded id is booked, never retired here"
    assert os.listdir(world / "_incr_tmp") == [], "scratch files must not outlive the cube"
    prog = json.loads((world / "_lane_progress.json").read_text())
    assert prog["state"] == "idle" and prog["counters"]["cubes_merged"] == 1
    assert prog["counters"]["cubes_served"] == 1 and prog["owed"]["merge"] == 0


def test_serving_is_byte_identical_to_the_derive_tool(world, monkeypatch, tmp_path):
    """Parity with tools/derive_statcan_tables.py's own writing path on the same cube and split."""
    _net(monkeypatch, NEW, (5, 3))
    b = _Bucket()
    con = sqlite3.connect(tmp_path / "catalog.db")               # catalogue the relabelled part too
    con.execute("INSERT INTO series VALUES (?)", (f"statcan:{PID}#Windsor - other locations",))
    con.commit()
    con.close()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)

    spec = importlib.util.spec_from_file_location("_dst_parity", os.path.join(ROOT, "tools",
                                                                              "derive_statcan_tables.py"))
    t = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(t)

    class _S3:
        put = {}

        def put_object(self, Bucket, Key, Body, ContentType=None, ContentEncoding=None):
            self.put[Key] = gzip.decompress(Body).decode("utf-8")
    s3 = _S3()
    (tmp_path / "logs").mkdir(exist_ok=True)
    monkeypatch.setattr(t, "STORE", str(world))
    monkeypatch.setattr(t, "ROOT", str(tmp_path))
    monkeypatch.setattr(t.r2_util, "client", lambda write=False: s3)
    # --rekey exists once PR #63 (writing runs pin) is merged; this branch may run before it. At
    # --max-rows 4 both versions choose the geo split the map records (5 rows, largest geo group 4).
    extra = ["--rekey"] if "--rekey" in open(spec.origin, encoding="utf-8").read() else []
    monkeypatch.setattr(sys, "argv", ["x", "--bucket", "b", "--only", str(PID), "--max-rows", "4",
                                      *extra])
    t.main()
    assert b.put and b.put == s3.put, (sorted(b.put), sorted(s3.put))


def test_a_kill_between_merge_and_serve_leaves_the_serve_owed_and_nothing_is_refetched(world, monkeypatch):
    class _Kill(BaseException):
        pass
    _net(monkeypatch, NEW, (5, 3))
    real = lane.serve_cube

    def _die(*a, **k):
        raise _Kill()
    monkeypatch.setattr(lane, "serve_cube", _die)
    with pytest.raises(_Kill):
        lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None, "merged is on record BEFORE serving"

    monkeypatch.setattr(lane, "serve_cube", real)
    fetched = []
    _net(monkeypatch, NEW, (5, 3), calls=fetched)
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)
    assert fetched == [], "a merged cube is not downloaded again to be served"
    assert _state(world)["cubes"][str(PID)]["served"] == REL and b.put


SAME = [_row("2026-01", "Windsor", "v1", "1.1", "1"), _row("2026-02", "Windsor", "v1", "1.1", "2"),
        _row("2026-01", "Windsor", "v2", "1.2", "5"), _row("2026-01", "Aden", "v3", "2.1", "6")]


def test_a_first_visit_serves_even_an_identical_table_and_a_later_identical_one_does_not(world,
                                                                                         monkeypatch):
    """Review P1/(3): the served CSVs before the lane were built by whatever ran before - the old
    fetcher merged while the inline derive got ~0 minutes - so a first visit always serves."""
    _net(monkeypatch, SAME, (4, 3))
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)
    assert b.put, "first visit: served although the table was identical"
    b2 = _Bucket()
    lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=b2, now_fn=_hours(2))
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == c["served"] == "2026-09-19T08:30" and b2.put == {}, \
        "identical to what this lane last served: settled without serving"


def test_a_kill_between_the_store_publish_and_the_state_record_still_serves(world, monkeypatch):
    """Review P1: the merge had published the new store, the state had not recorded it; the
    relaunch merged the identical table, saw no change and settled 'served' with zero PUTs."""
    _net(monkeypatch, SAME, (4, 3))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())            # served once

    class _Kill(BaseException):
        pass
    real = lane.merge.merge_and_write_bounded

    def _publish_then_die(*a, **k):
        real(*a, **k)
        raise _Kill()
    monkeypatch.setattr(lane.merge, "merge_and_write_bounded", _publish_then_die)
    _net(monkeypatch, NEW, (5, 3))
    with pytest.raises(_Kill):
        lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=_Bucket(), now_fn=_hours(2))
    c = _state(world)["cubes"][str(PID)]
    assert c["merge_in_flight"] == "2026-09-19T08:30" and c["merged"] == REL

    monkeypatch.setattr(lane.merge, "merge_and_write_bounded", real)
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=b, now_fn=_hours(4))
    assert b.put.get("series/statcan%3A24100058%23Aden.csv") == \
        "series_id,obs_date,value\nv3,2026-01-01,7.0\n", sorted(b.put)
    c = _state(world)["cubes"][str(PID)]
    assert c["served"] == "2026-09-19T08:30" and "merge_in_flight" not in c


def test_an_uncatalogued_cube_is_settled_with_its_ids_as_debt(world, monkeypatch, tmp_path):
    con = sqlite3.connect(tmp_path / "catalog.db")
    con.execute("DELETE FROM series")
    con.commit()
    con.close()
    _net(monkeypatch, NEW, (5, 3))
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)
    c = _state(world)["cubes"][str(PID)]
    assert c["served"] == REL and b.put == {}, "nothing is written for an id no user can reach"
    debt = json.loads((world / "_lane_debt.json").read_text())["cubes"][str(PID)]
    assert set(debt["new"]) == {f"statcan:{PID}#Aden", f"statcan:{PID}#Windsor - other locations"}


# --------------------------------------------------------------------------- #
# refusals, quarantine, transient backoff
# --------------------------------------------------------------------------- #
def test_a_short_table_is_refused_three_times_then_quarantined_until_re_released(world, monkeypatch):
    before = (world / f"{PID}.parquet").read_bytes()
    _net(monkeypatch, NEW[:4], (5, 3))
    for n in (1, 2, 3):
        lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
        c = _state(world)["cubes"][str(PID)]
        assert c["fails"] == n and c.get("merged") is None
    assert c["quarantined"] is True
    assert (world / f"{PID}.parquet").read_bytes() == before, "the store is untouched"
    assert json.loads((world / "_lane_progress.json").read_text())["owed"]["quarantined"] == [str(PID)]
    _net(monkeypatch, NEW, (5, 3))
    # with only a quarantined cube on record the lane is IDLE, so it re-enumerates hourly, not on
    # every guard tick: the re-release is seen on the first launch past ENUM_MIN_INTERVAL_MIN
    assert lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=_Bucket())["enumerated"] is False
    later = lambda: lane._now() + dt.timedelta(minutes=lane.ENUM_MIN_INTERVAL_MIN + 1)  # noqa: E731
    lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=_Bucket(), now_fn=later)
    c = _state(world)["cubes"][str(PID)]
    assert not c.get("quarantined") and c["merged"] == "2026-09-19T08:30", "a new release retries it"


def test_a_corrupt_zip_is_transient_backs_off_and_never_counts_toward_quarantine(world, monkeypatch):
    before = (world / f"{PID}.parquet").read_bytes()
    _net(monkeypatch, NEW, (5, 3), corrupt=True)
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    c = _state(world)["cubes"][str(PID)]
    assert not c.get("fails") and c["transient_fails"] == 1 and c["retry_after"]
    assert (world / f"{PID}.parquet").read_bytes() == before

    def _no_enum(since):
        raise AssertionError("a launch with nothing DUE must not call StatCan")
    assert lane.run(enumerate_releases=_no_enum, put=_Bucket()) == {"enumerated": False, "worked": 0}


def test_a_re_keyed_table_is_refused(world, monkeypatch):
    rekeyed = [_row("2026-01", "Windsor", f"v9{i}", "1.1", "1") for i in range(4)]
    _net(monkeypatch, rekeyed, (4, 4))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    c = _state(world)["cubes"][str(PID)]
    assert c["fails"] == 1 and "re-keyed" in c["last_error"]


def test_a_vectorless_cube_is_floored_on_its_last_accepted_parse_not_the_growing_store(world,
                                                                                        monkeypatch):
    """Ported from the fetcher (probe E): release 1 swaps one member and merges (keep-old: the store
    grows to 11 rows); release 2 is the same 10-row table. Floored on the STORED rows it was refused
    for ever; floored on the last accepted parse it merges. A table short of THAT is refused."""
    coord = [_row("2026-01", "Windsor", "", f"1.{i}", str(i)) for i in range(1, 11)]
    _stored(world / f"{PID}.parquet", [(f"1.{i}", "2026-01-01", float(i), "Windsor", f"1.{i}")
                                       for i in range(1, 11)])
    release1 = coord[:9] + [_row("2026-01", "Windsor", "", "1.11", "11")]      # member 1.10 -> 1.11
    _net(monkeypatch, release1, (99, 1))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    assert pq.read_metadata(world / f"{PID}.parquet").num_rows == 11, "keep-old: the store grew"
    assert _state(world)["parsed_rows"][str(PID)] == 10

    _net(monkeypatch, release1, (99, 1))                                       # release 2: same 10
    lane.run(enumerate_releases=_rel({PID: "2026-09-19T08:30"}), put=_Bucket(), now_fn=_hours(2))
    assert _state(world)["cubes"][str(PID)]["merged"] == "2026-09-19T08:30"

    _net(monkeypatch, release1[:8], (99, 1))                                   # release 3: 8 rows
    lane.run(enumerate_releases=_rel({PID: "2026-09-26T08:30"}), put=_Bucket(), now_fn=_hours(4))
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == "2026-09-19T08:30" and "vectorless" in c["last_error"]


def test_the_oldest_release_goes_first(world, monkeypatch):
    _stored(world / "10100001.parquet", [("v7", "2026-01-01", 1.0, "Aden", "2.1")])
    _net(monkeypatch, NEW, (5, 3))
    done = []
    real = lane.merge_cube

    def _spy(pid, *a, **k):
        done.append(pid)
        return real(pid, *a, **k)
    monkeypatch.setattr(lane, "merge_cube", _spy)
    lane.run(enumerate_releases=_rel({PID: "2026-08-01T08:30", 10100001: "2026-09-20T08:30"}),
             put=_Bucket(), max_cubes=1)
    assert done == [str(PID)], "the bigger cube, released first, goes first (R1092: never smallest-first)"


def test_size_breaks_a_tie_between_equal_releases(world, monkeypatch):
    _stored(world / "10100001.parquet", [("v7", "2026-01-01", 1.0, "Aden", "2.1")])
    assert os.path.getsize(world / "10100001.parquet") < os.path.getsize(world / f"{PID}.parquet")
    _net(monkeypatch, NEW, (5, 3))
    done = []
    real = lane.merge_cube

    def _spy(pid, *a, **k):
        done.append(pid)
        return real(pid, *a, **k)
    monkeypatch.setattr(lane, "merge_cube", _spy)
    lane.run(enumerate_releases=_rel({PID: REL, 10100001: REL}), put=_Bucket(), max_cubes=1)
    assert done == ["10100001"], "same release: the smaller cube first"


# --------------------------------------------------------------------------- #
# cubes we do not hold, and the store-absent guard
# --------------------------------------------------------------------------- #
def test_a_released_cube_we_do_not_hold_is_booked_as_debt(world, monkeypatch):
    _net(monkeypatch, NEW, (5, 3))
    lane.run(enumerate_releases=_rel({PID: REL, 99999999: REL}), put=_Bucket())
    st = _state(world)
    assert st["new_cubes"] == {"99999999": REL}
    assert json.loads((world / "_lane_debt.json").read_text())["new_cubes"] == {"99999999": REL}
    assert st["cubes"]["99999999"]["not_held"] is True


def test_a_cube_the_ingest_record_says_we_hold_but_the_store_lacks_is_a_fault(world, monkeypatch):
    (world / "99999999.done").write_text("")
    _net(monkeypatch, NEW, (5, 3))
    lane.run(enumerate_releases=_rel({PID: REL, 99999999: REL}), put=_Bucket())
    c = _state(world)["cubes"]["99999999"]
    assert c.get("merged") is None and "store fault" in c["last_error"]


def test_an_empty_store_is_unreachable_not_a_quiet_publisher(world, monkeypatch):
    os.remove(world / f"{PID}.parquet")
    _net(monkeypatch, NEW, (5, 3))
    with pytest.raises(SystemExit, match="ZERO cubes"):
        lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())


# --------------------------------------------------------------------------- #
# what may be served
# --------------------------------------------------------------------------- #
def test_serve_plan_rules():
    cat_parts = {f"statcan:{PID}#Aden"}
    cat_whole = {f"statcan:{PID}"}
    smap = {str(PID): {"dim": "geo"}}
    assert lane.serve_plan(str(PID), 10, ["geo"], smap, cat_parts) == ("geo", None, [])
    assert lane.serve_plan(str(PID), 10, ["geo"], {}, cat_parts)[1].startswith("served as parts")
    assert "lacks" in lane.serve_plan(str(PID), 10, ["uom"], smap, cat_parts)[1]
    dim, refusal, notes = lane.serve_plan(str(PID), 10, ["geo"], smap, set())
    assert (dim, refusal) == ("geo", None) and notes[0].startswith("not catalogued"), \
        "an uncatalogued cube is served (nothing written) plus debt, never refused (review P2)"
    assert lane.serve_plan(str(PID), 10, [], {}, cat_whole) == (None, None, [])
    dim, refusal, notes = lane.serve_plan(str(PID), lane.MAX_ROWS + 1, [], {}, cat_whole)
    assert dim is None and refusal is None and "over the" in notes[0], \
        "a whole cube past the cap is still served whole - a stale object is worse than a large one"


def test_a_parts_cube_with_no_recorded_split_stays_serve_owed(world, monkeypatch):
    smap = {"33333333": {"dim": "uom", "parts": 4, "rows": 900}}            # PID's entry is gone
    (world / "_split_map.json").write_text(json.dumps(smap))
    _net(monkeypatch, NEW, (5, 3))
    b = _Bucket()
    lane.run(enumerate_releases=_rel({PID: REL}), put=b)
    c = _state(world)["cubes"][str(PID)]
    assert c["merged"] == REL and c.get("served") is None and b.put == {}
    assert "no split is recorded" in c["serve_refusal"]


def test_the_lane_never_writes_the_split_map(world, monkeypatch):
    before = (world / "_split_map.json").read_bytes()
    _net(monkeypatch, NEW, (5, 3))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    assert (world / "_split_map.json").read_bytes() == before
    src = open(lane.__file__, encoding="utf-8").read()
    assert "open(SPLIT_MAP, encoding=\"utf-8\")" in src and "SPLIT_MAP, \"w\"" not in src


def test_an_unreadable_split_map_stops_the_launch(world, monkeypatch):
    (world / "_split_map.json").write_text("{not json")
    _net(monkeypatch, NEW, (5, 3))
    with pytest.raises(SystemExit, match="split map"):
        lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())


# --------------------------------------------------------------------------- #
# the reporter
# --------------------------------------------------------------------------- #
NOW = dt.datetime(2026, 9, 23, 12, 0, tzinfo=dt.timezone.utc)


def _prog(beat_h_ago, current=None, **owed):
    beat = (NOW - dt.timedelta(hours=beat_h_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    o = {"merge": 0, "serve": 0, "quarantined": [], "new_cubes": 0, "oldest_owed_release": None}
    o.update(owed)
    return {"beat_utc": beat, "current": current, "state": "working" if current else "idle", "owed": o}


STATE = {"cubes": {"1": {"max_obs": "2026-08-01"}, "2": {"max_obs": "2026-09-01"}}}


def test_the_reporter_is_ok_when_current_and_passes_the_newest_obs():
    assert sc.verdict(STATE, _prog(0.5), NOW) == ("ok", sc.verdict(STATE, _prog(0.5), NOW)[1], "2026-09-01")


def test_the_reporter_is_red_without_any_record():
    with pytest.raises(DefinitiveError, match="no progress"):
        sc.verdict(None, None, NOW)


def test_the_reporter_allows_a_long_cube_its_estimate_but_no_more():
    cur = {"pid": "12100152", "phase": "merge", "est_min": 380}
    assert sc.verdict(STATE, _prog(9.0, current=cur), NOW)[0] == "ok", "380 min x 1.5 = 9.5 h"
    with pytest.raises(DefinitiveError, match="DEAD or wedged"):
        sc.verdict(STATE, _prog(10.0, current=cur), NOW)
    with pytest.raises(DefinitiveError, match="DEAD or wedged"):
        sc.verdict(STATE, _prog(3.5), NOW)              # idle: 3 h


def test_the_reporter_is_red_when_the_oldest_owed_release_is_past_the_sla():
    assert sc.verdict(STATE, _prog(0.1, merge=3, oldest_owed_release="2026-09-10T08:30"), NOW)[0] == "ok"
    with pytest.raises(DefinitiveError, match="BEHIND and NOT PROGRESSING"):
        sc.verdict(STATE, _prog(0.1, merge=3, oldest_owed_release="2026-09-08T08:30"), NOW)


def test_a_lane_behind_but_finishing_cubes_is_draining_not_red():
    """Round-2 advisory / design condition 7: the backlog is past the SLA on the first beat, so an
    age rule alone reads RED for the whole drain. Progress keeps it amber; no progress is red."""
    p = _prog(0.1, merge=300, oldest_owed_release="2026-07-29T08:30")
    p["last_progress_utc"] = (NOW - dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    status, msg, _ = sc.verdict(STATE, p, NOW)
    assert status == "partial" and "DRAINING" in msg
    p["last_progress_utc"] = (NOW - dt.timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    with pytest.raises(DefinitiveError, match="NOT PROGRESSING"):
        sc.verdict(STATE, p, NOW)
    cur = {"pid": "12100152", "phase": "merge", "est_min": 380}                   # 9.5 h allowance
    p = dict(_prog(0.1, current=cur, merge=300, oldest_owed_release="2026-07-29T08:30"),
             last_progress_utc=(NOW - dt.timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert sc.verdict(STATE, p, NOW)[0] == "partial", "a giant in flight earns its own allowance"


def test_the_draining_verdict_survives_idle_iterations_and_a_relaunch(world, monkeypatch):
    """Round 3, P8/P9: the stamp lived in the per-iteration Progress, so the idle iteration after a
    served cube - and every relaunch - published a beat without it, and a healthy drain read red."""
    _stored(world / "10100001.parquet", [("v7", "2026-01-01", 1.0, "Aden", "2.1")])
    monkeypatch.setattr(sc, "_cube_counts", lambda pid: (5, 3))
    monkeypatch.setattr(sc, "_get", lambda ep, **k: {"status": "SUCCESS",
                                                     "object": f"https://example.invalid/{ep}.zip"})

    def _download(url, dest, tries=5):
        _zip(dest, NEW)
        if "10100001" in url:                     # this cube's transfer keeps breaking: owed, backing off
            with open(dest, "r+b") as f:
                f.truncate(os.path.getsize(dest) // 2)
        return os.path.getsize(dest)
    monkeypatch.setattr(sc, "_download", _download)
    rel = {PID: "2026-07-30T08:30", 10100001: "2026-07-30T08:30"}              # both past the SLA
    lane.run(enumerate_releases=_rel(rel), put=_Bucket())                       # iteration 1: serves PID
    now = dt.datetime.now(dt.timezone.utc)

    def _verdict():
        return sc.verdict(_state(world), json.loads((world / "_lane_progress.json").read_text()), now)
    assert _verdict()[0] == "partial" and "DRAINING" in _verdict()[1]
    lane.run(enumerate_releases=_rel(rel), put=_Bucket())                       # iteration 2: idle
    assert "DRAINING" in _verdict()[1], "an idle iteration must not erase the progress stamp"
    monkeypatch.setattr(lane, "_now", lambda: dt.datetime.now(dt.timezone.utc))  # a relaunch: new run()
    lane.run(enumerate_releases=_rel(rel), put=_Bucket())
    assert "DRAINING" in _verdict()[1]


def test_a_stamp_from_the_future_vouches_for_nothing():
    fut = (NOW + dt.timedelta(hours=48)).strftime("%Y-%m-%dT%H:%M:%SZ")
    p = dict(_prog(0.1, merge=3, oldest_owed_release="2026-07-29T08:30"), last_progress_utc=fut)
    with pytest.raises(DefinitiveError, match="NOT PROGRESSING"):
        sc.verdict(STATE, p, NOW)
    with pytest.raises(DefinitiveError, match="DEAD or wedged"):
        sc.verdict(STATE, dict(_prog(0.1), beat_utc=fut), NOW)


def test_the_lane_records_progress_when_it_finishes_work(world, monkeypatch):
    _net(monkeypatch, NEW, (5, 3))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    prog = json.loads((world / "_lane_progress.json").read_text())
    assert prog.get("last_progress_utc"), prog


def test_the_reporter_names_quarantined_cubes_as_partial():
    status, msg, _ = sc.verdict(STATE, _prog(0.1, quarantined=["24100058"]), NOW)
    assert status == "partial" and "24100058" in msg


def test_a_cube_failing_transiently_again_and_again_is_red_not_ok():
    """Review P3: 60 transient failures (a .done with no store object) read 'ok - lane current'."""
    st = {"cubes": {"24100058": {"release": "2026-09-20T08:30", "transient_fails": 6,
                                 "last_error": "the ingest record says it is held; the store does not"}}}
    owed = lane.summarise(st)
    assert owed["failing"] and "store does not" in owed["failing"][0]
    with pytest.raises(DefinitiveError, match="FAILING"):
        sc.verdict(STATE, dict(_prog(0.1), owed=owed), NOW)
    st["cubes"]["24100058"]["transient_fails"] = 5
    assert sc.verdict(STATE, dict(_prog(0.1), owed=lane.summarise(st)), NOW)[0] == "ok"


def test_a_serve_refusal_is_named_and_kept_off_the_age_clock():
    """Review P2: a refusal nothing can clear aged into a permanent RED."""
    st = {"cubes": {"24100058": {"release": "2026-08-01T08:30", "merged": "2026-08-01T08:30",
                                 "serve_refusal": "served as parts but no split is recorded"}}}
    owed = lane.summarise(st)
    assert owed["oldest_owed_release"] is None and owed["serve_refused"]
    status, msg, _ = sc.verdict(STATE, dict(_prog(0.1), owed=owed), NOW)
    assert status == "partial" and "cannot be reproduced" in msg


def test_old_new_cube_debt_turns_amber():
    st = {"cubes": {}, "new_cubes": {"99999999": "2026-08-01T08:30"}}
    status, msg, _ = sc.verdict(STATE, dict(_prog(0.1), owed=lane.summarise(st)), NOW)
    assert status == "partial" and "not held" in msg
    st["new_cubes"] = {"99999999": "2026-09-20T08:30"}
    assert sc.verdict(STATE, dict(_prog(0.1), owed=lane.summarise(st)), NOW)[0] == "ok"


def test_the_reporter_writes_nothing_and_changes_no_keys(world, monkeypatch):
    _net(monkeypatch, NEW, (5, 3))
    lane.run(enumerate_releases=_rel({PID: REL}), put=_Bucket())
    monkeypatch.setattr(sc, "LANE_STATE", lane.STATE)
    monkeypatch.setattr(sc, "LANE_PROGRESS", lane.PROGRESS)
    before = {p: (world / p).read_bytes() for p in os.listdir(world) if os.path.isfile(world / p)}
    res = sc.update(None, None)
    assert res.status == "ok" and res.changed_keys == {} and res.last_obs_date == "2026-03-01"
    assert {p: (world / p).read_bytes() for p in os.listdir(world) if os.path.isfile(world / p)} == before


def _heartbeat():
    spec = importlib.util.spec_from_file_location("_gh_lane", os.path.join(ROOT, "tools",
                                                                           "guard_heartbeat.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_ci_sees_a_dead_or_lagging_lane_through_the_guard_heartbeat(monkeypatch):
    """statcan is run_location: local, so the cloud health gate does not judge it; the heartbeat CI
    already checks is where a dead lane must show (design review finding 8). Same predicate as the
    reporter."""
    gh = _heartbeat()
    now = dt.datetime.now(dt.timezone.utc)
    fresh = _prog(0.2)
    fresh["beat_utc"] = (now - dt.timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert gh._lane_problem(fresh, now) is None
    stale = dict(fresh, beat_utc=(now - dt.timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    assert "DEAD or wedged" in gh._lane_problem(stale, now)
    behind = dict(fresh, owed={"merge": 9, "oldest_owed_release": "2026-07-29T08:30"})
    assert "BEHIND" in gh._lane_problem(behind, now)
    quarantined = dict(fresh, owed={"quarantined": ["24100058"]})
    assert gh._lane_problem(quarantined, now) is None, "a quarantine needs a human, not a red run"
    assert "unreadable" in gh._lane_problem({"unreadable": "ValueError"}, now)
    monkeypatch.setattr(gh, "_lane_expected", lambda: True)
    assert "never run" in gh._lane_problem(None, now)
    monkeypatch.setattr(gh, "_lane_expected", lambda: False)
    assert gh._lane_problem(None, now) is None


def test_the_ci_check_fails_on_a_stale_lane_and_names_an_older_publisher(monkeypatch, capsys):
    gh = _heartbeat()
    now = dt.datetime.now(dt.timezone.utc)
    beat = {"utc": now.isoformat(), "host": "ws", "jobs_alive": [], "tracked": [], "table_ok": True,
            "emptiness": {"ran": True, "fetch_without_write": 0}}

    class _C:
        def __init__(self, body):
            self.body = body

        def get_object(self, Bucket=None, Key=None):             # noqa: N803
            return {"Body": io.BytesIO(json.dumps(self.body).encode("utf-8"))}
    stale = dict(_prog(0), beat_utc=(now - dt.timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: _C(dict(beat, statcan_lane=stale)))
    assert gh.check(45.0) == 1 and "STATCAN LANE" in capsys.readouterr().out
    fresh = dict(_prog(0), beat_utc=now.strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: _C(dict(beat, statcan_lane=fresh)))
    assert gh.check(45.0) == 0
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: _C(beat))       # older publisher
    assert gh.check(45.0) == 0 and "predates the statcan lane" in capsys.readouterr().out


# --------------------------------------------------------------------------- #
# one writer
# --------------------------------------------------------------------------- #
_HOLDER = ("import sys, time; sys.path.insert(0, sys.argv[1]); from updater import writer_lock as w; "
           "w.LOCK_DIR = sys.argv[2]; assert w.acquire('statcan_writer', 'test holder'); "
           "print('held', flush=True); time.sleep(60)")


def test_a_live_holder_refuses_every_other_writer_and_a_dead_one_does_not(tmp_path, monkeypatch):
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path))
    p = subprocess.Popen([sys.executable, "-c", _HOLDER, ROOT, str(tmp_path)],
                         stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        assert writer_lock.owner("statcan_writer")["pid"] == p.pid
        with pytest.raises(SystemExit, match="REFUSING core.derive_csv"):
            writer_lock.refuse_if_held("statcan_writer", "core.derive_csv")
        assert writer_lock.acquire("statcan_writer") is False
    finally:
        p.kill()
        p.wait()
    assert writer_lock.owner("statcan_writer") is None, "a dead owner's file is stale"
    assert writer_lock.acquire("statcan_writer") is True
    writer_lock.refuse_if_held("statcan_writer", "self")          # the holder itself passes
    writer_lock.release("statcan_writer")
    assert not os.path.exists(writer_lock.lock_path("statcan_writer"))


def test_a_recycled_pid_is_not_the_owner(tmp_path, monkeypatch):
    """R600: this process's pid with another start time is a DIFFERENT process."""
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path))
    with open(writer_lock.lock_path("statcan_writer"), "w") as fh:
        json.dump({"pid": os.getpid(), "started": time.time() - 86400}, fh)
    assert writer_lock.owner("statcan_writer") is None


def test_an_unreadable_lock_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path))
    with open(writer_lock.lock_path("statcan_writer"), "w") as fh:
        fh.write("{garbage")
    with pytest.raises(SystemExit, match="cannot be judged"):
        writer_lock.refuse_if_held("statcan_writer", "a writer")


def test_the_other_statcan_writers_TAKE_the_lock():
    """Round-2 P6: checking alone was one-way - a writer that passed the check held nothing, and the
    lane acquired in the gap between its iterations and served over it."""
    for rel in ("core/derive_csv.py", "tools/upload_statcan_store.py", "jobs/ingest_statcan.py",
                "tools/derive_statcan_tables.py", "tools/derive_csv_bulk.py"):
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        assert 'hold_or_refuse("statcan_writer"' in src, rel
        assert 'refuse_if_held("statcan_writer"' not in src, rel


_OTHER_WRITER = ("import sys, time; sys.path.insert(0, sys.argv[1]); from updater import writer_lock as w; "
                 "w.LOCK_DIR = sys.argv[2]; w.hold_or_refuse('statcan_writer', 'a derive'); "
                 "print('writing', flush=True); time.sleep(60)")


def test_a_writer_that_holds_the_lock_keeps_the_lane_out(tmp_path, monkeypatch):
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path))
    p = subprocess.Popen([sys.executable, "-c", _OTHER_WRITER, ROOT, str(tmp_path)],
                         stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "writing"
        assert writer_lock.acquire("statcan_writer", "jobs/statcan_lane.py") is False, \
            "the lane must not start an iteration while another writer is writing"
    finally:
        p.kill()
        p.wait()
    assert writer_lock.acquire("statcan_writer", "jobs/statcan_lane.py") is True


def test_the_lane_holding_the_lock_keeps_another_writer_out(tmp_path, monkeypatch):
    monkeypatch.setattr(writer_lock, "LOCK_DIR", str(tmp_path))
    assert writer_lock.acquire("statcan_writer", "jobs/statcan_lane.py")
    try:
        r = subprocess.run([sys.executable, "-c", _OTHER_WRITER, ROOT, str(tmp_path)],
                           capture_output=True, text=True, timeout=60)
        assert r.returncode != 0 and "REFUSING a derive" in (r.stderr + r.stdout), (r.stdout, r.stderr)
    finally:
        writer_lock.release("statcan_writer")
    src = open(os.path.join(ROOT, "tools", "_delete_statcan_r2.py"), encoding="utf-8").read()
    assert src.index("raise SystemExit(\"RETIRED") < src.index("import boto3")


def test_the_lane_sets_r2_when_unset_and_refuses_any_other_backend(monkeypatch):
    class _Cfg:
        BACKEND = "local"
    env, cfg = {}, _Cfg()
    lane.pin_backend(env, cfg)
    assert env["AQUEDUCT_BACKEND"] == "r2" and cfg.BACKEND == "r2"
    with pytest.raises(SystemExit, match="must be r2"):
        lane.pin_backend({"AQUEDUCT_BACKEND": "local"}, _Cfg())
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    with pytest.raises(SystemExit, match="must be r2"):
        lane.main([])                                   # before any lock, network or store


# --------------------------------------------------------------------------- #
# the orchestrator leaves a lane-served source alone
# --------------------------------------------------------------------------- #
def test_statcan_is_served_by_its_lane_and_the_csv_phase_skips_it():
    orchestrate._served_by_lane.__dict__.pop("_cache", None)
    assert orchestrate._served_by_lane("statcan") is True
    assert orchestrate._served_by_lane("fhfa") is False
    src = open(orchestrate.__file__, encoding="utf-8").read()
    assert "_should_derive_csvs(status) and not dry and not _served_by_lane(unit.source_id)" in src


def test_a_mistyped_served_by_is_a_registry_error():
    bad = {"sources": [{"source_id": "x", "strategy": "extend_by_date", "cadence": "weekly",
                        "served_by": "lanes"}]}
    assert any("served_by" in p for p in registry.validate(bad))
