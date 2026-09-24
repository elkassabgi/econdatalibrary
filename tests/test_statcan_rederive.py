"""statcan's table-part ids after a refresh, and the split map scoped runs must never truncate (2026-09-23).

statcan is served as table parts, `statcan:<pid>#<value of the split column>`. The updater now refreshes
whole cubes and the orchestrator re-derives their CATALOGUED parts under the recorded split. What nothing
else can say is which ids a refresh CREATES (a relabelled member: no catalogue row yet) or STRANDS
(catalogued, no longer produced). `--pin-split --parts-report` answers exactly that, and writes nothing.
Measured on 24100058: +2 ids, 1 stranded, 133 unchanged.

Also pinned here: scoped runs (--only, --limit) used to overwrite _split_map.json with THEIR OWN entries
only - mid-run, and for --limit at the end too - so every other cube lost its split; a refused cube lost
its entry too; and a NULL split value was written under the WHOLE-cube id.
"""
from __future__ import annotations

import datetime as dt
import gzip
import importlib.util
import json
import os
import sqlite3
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "derive_statcan_tables.py")
sys.path.insert(0, os.path.dirname(_HERE))


def _load():
    spec = importlib.util.spec_from_file_location("_derive_rederive_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


d = _load()


# --------------------------------------------------------------------------- #
# the pure decisions
# --------------------------------------------------------------------------- #
def test_a_recorded_split_is_reused_verbatim():
    pinned = {"24100058": {"dim": "geo", "parts": 134, "rows": 52_664_539}}
    assert d.pinned_split("24100058", 53_335_935, pinned, 3_000_000, served_whole=False) == ("geo", 134)


def test_a_cube_served_whole_stays_whole_while_it_fits_the_cap():
    assert d.pinned_split("10100001", 400, {}, 3_000_000, served_whole=True) == (None, 1)


def test_a_whole_cube_that_outgrew_the_cap_is_refused_not_silently_rekeyed():
    """One rule with the cataloguer, which refuses the WHOLE catalogue when an over-cap cube has
    no split: splitting it here would re-key its id, keeping it whole would block cataloguing."""
    assert d.pinned_split("10100001", 3_000_001, {}, 3_000_000, served_whole=True) == ("", 0)


def test_a_cube_served_as_parts_with_no_recorded_split_is_refused():
    """Choosing a split now would rename every served part (round-2 review, probe P3)."""
    assert d.pinned_split("11111111", 3, {}, 1, served_whole=False, served_parts=True) == ("", 0)


def test_a_cube_with_no_public_ids_is_decided_like_a_first_derive():
    assert d.pinned_split("99999998", 10, {}, 3_000_000, served_whole=False) == (d.CHOOSE, 0)


def test_a_pinned_split_must_still_name_existing_columns():
    assert d.pinned_columns_present("geo", ["geo", "uom"])
    assert d.pinned_columns_present("coordinate:3", ["coordinate"])
    assert d.pinned_columns_present("uom+geo", ["geo", "uom"])
    assert not d.pinned_columns_present("uom+geo", ["geo"])
    assert not d.pinned_columns_present("geo", ["uom"])


def test_part_diff_names_new_and_stranded_ids():
    got = d.part_diff({"s:1#a", "s:1#b2", "s:1#c"}, {"s:1#a", "s:1#b", "s:1#c"})
    assert got == {"new": ["s:1#b2"], "vanished": ["s:1#b"]}
    assert d.part_diff({"x"}, {"x"}) == {"new": [], "vanished": []}


def test_catalogued_ids_reads_exactly_one_cube(tmp_path):
    db = tmp_path / "catalog.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    ids = ["statcan:24100057#z", "statcan:24100058", "statcan:24100058#Windsor",
           "statcan:24100058#Aden", "statcan:24100059", "statcan:24100059#a",
           "statcan:241000580", "statcan:241000580#x"]          # 9-digit neighbours
    con.executemany("INSERT INTO series VALUES (?)", [(i,) for i in ids])
    con.commit()
    con.close()
    assert d.catalogued_ids(str(db), "24100058") == {
        "statcan:24100058", "statcan:24100058#Windsor", "statcan:24100058#Aden"}


# --------------------------------------------------------------------------- #
# end to end: real store dir, real DuckDB, a fake bucket
# --------------------------------------------------------------------------- #
class _FakeS3:
    def __init__(self, fail=()):
        self.put = {}
        self.fail = set(fail)

    def put_object(self, Bucket, Key, Body, ContentType=None, ContentEncoding=None):
        if any(f in Key for f in self.fail):
            raise RuntimeError("pretend R2 refused this PUT")
        self.put[Key] = gzip.decompress(Body).decode("utf-8") if Body[:2] == b"\x1f\x8b" else Body


def _cube(path, rows):
    t = pa.table({
        "series_key": [r[0] for r in rows],
        "obs_date": pa.array([dt.date(2026, 1, 1)] * len(rows), pa.date32()),
        "value": [float(i) for i in range(len(rows))],
        "geo": pa.array([r[1] for r in rows], pa.string()), "uom": ["Vehicles"] * len(rows),
        "coordinate": [r[2] for r in rows], "status": [""] * len(rows)})
    pq.write_table(t, path)


@pytest.fixture
def world(tmp_path, monkeypatch):
    store = tmp_path / "statcan"
    store.mkdir()
    # 11111111: split by geo; its refreshed data relabels "Windsor"
    _cube(store / "11111111.parquet", [("v1", "Windsor - other locations", "1.1"),
                                        ("v2", "Aden", "2.1"), ("v3", "Aden", "2.2")])
    # 22222222: served WHOLE (catalogued as the bare id), not in the map
    _cube(store / "22222222.parquet", [("v9", "x", "1.1")])
    # 55555555: IN the store and IN the map - a --limit run that stops before it must keep it
    _cube(store / "55555555.parquet", [("v5", "p", "1.1"), ("v6", "q", "1.2")])
    smap = {"11111111": {"dim": "geo", "parts": 2, "rows": 3},
            "33333333": {"dim": "uom", "parts": 4, "rows": 900},
            "44444444": {"dim": "coordinate:2", "parts": 7, "rows": 800},
            "55555555": {"dim": "geo", "parts": 2, "rows": 2}}
    (store / "_split_map.json").write_text(json.dumps(smap))
    db = tmp_path / "catalog.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.executemany("INSERT INTO series VALUES (?)",
                    [("statcan:11111111#Windsor",), ("statcan:11111111#Aden",),
                     ("statcan:22222222",)])
    con.commit()
    con.close()
    s3 = _FakeS3()
    (tmp_path / "logs").mkdir()
    monkeypatch.setattr(d, "STORE", str(store))
    monkeypatch.setattr(d, "ROOT", str(tmp_path))
    monkeypatch.setattr(d.r2_util, "client", lambda write=False: s3)
    return store, db, s3, smap


def _run(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["derive_statcan_tables.py", *args])
    return d.main()


def _pin(monkeypatch, db, report, *extra):
    return _run(monkeypatch, "--pin-split", "--max-rows", "1", "--catalog-db", str(db),
                "--parts-report", str(report), *extra)


def test_a_pinned_report_names_new_and_stranded_ids_and_writes_nothing(world, monkeypatch, tmp_path):
    store, db, s3, smap = world
    report = tmp_path / "parts.json"
    rc = _pin(monkeypatch, db, report, "--only", "11111111")
    assert rc == 0 and s3.put == {}, "a pinned run is a REPORT: nothing reaches the bucket"
    assert json.loads((store / "_split_map.json").read_text()) == smap, "nor the split map"
    rep = json.loads(report.read_text())["cubes"]["11111111"]
    assert rep == {"new": ["statcan:11111111#Windsor - other locations"],
                   "vanished": ["statcan:11111111#Windsor"], "status": "ok", "unwritten": []}


def test_a_pinned_report_leaves_the_full_runs_summary_byte_identical(world, monkeypatch, tmp_path):
    """Probe P7: a routine report overwrote the only record of the last FULL run - its cap and its
    refused giants - with scope=dry_run."""
    store, db, s3, smap = world
    summary = tmp_path / "logs" / "statcan_tables_summary.json"
    summary.write_text(json.dumps({"scope": "full", "max_rows": 1, "considered": 8207,
                                   "refused": [{"table": "98100174", "rows": 315_000_000}]}))
    before = summary.read_bytes()
    _pin(monkeypatch, db, tmp_path / "parts.json", "--only", "11111111")
    assert summary.read_bytes() == before


def test_a_full_run_keeps_a_refused_cubes_map_entry(world, monkeypatch):
    """Probe P9: the full branch rebuilt the map from this run's decisions only."""
    store, db, s3, smap = world
    real = d.choose_split
    monkeypatch.setattr(d, "choose_split",
                        lambda con, f, n, m: ("", 0) if "11111111" in f else real(con, f, n, m))
    _run(monkeypatch, "--bucket", "b", "--max-rows", "1", "--rekey")
    assert json.loads((store / "_split_map.json").read_text())["11111111"] == smap["11111111"]


def test_an_empty_string_split_value_is_never_written_under_the_whole_cube_id(world, monkeypatch):
    store, db, s3, smap = world
    _cube(store / "11111111.parquet", [("v1", "Aden", "1.1"), ("v2", "", "2.1")])
    monkeypatch.setattr(d, "choose_split", lambda con, f, n, m: ("geo", 2))
    _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    assert "series/statcan%3A11111111.csv" not in s3.put, sorted(s3.put)


def test_a_refused_cube_reports_no_vanished_ids(world, monkeypatch, tmp_path):
    """Listing all a refused cube's ids as 'vanished' would invite retiring every one of them."""
    store, db, s3, smap = world
    m = dict(smap)
    m.pop("11111111")
    (store / "_split_map.json").write_text(json.dumps(m))
    report = tmp_path / "parts.json"
    _pin(monkeypatch, db, report, "--only", "11111111")
    rep = json.loads(report.read_text())["cubes"]["11111111"]
    assert rep["status"] == "refused" and rep["vanished"] == [] and rep["new"] == []


def test_a_cube_with_no_public_ids_is_split_as_a_first_derive_would(world, monkeypatch, tmp_path):
    store, db, s3, smap = world
    _cube(store / "66666666.parquet", [("v1", "a", "1.1"), ("v2", "b", "2.1")])
    report = tmp_path / "parts.json"
    _pin(monkeypatch, db, report, "--only", "66666666")
    rep = json.loads(report.read_text())["cubes"]["66666666"]
    assert rep["status"] == "ok" and rep["new"] and all("#" in s for s in rep["new"]), rep


@pytest.mark.parametrize("setup, why", [
    ("over_cap_whole", "a whole-served cube over the cap"),
    ("lost_column", "a recorded split on a column the cube lost"),
    ("parts_unmapped", "a cube served as parts with no recorded split"),
])
def test_cubes_that_cannot_be_judged_are_refused_and_fail_the_run(world, monkeypatch, tmp_path,
                                                                  setup, why):
    store, db, s3, smap = world
    pid = "11111111"
    if setup == "over_cap_whole":
        pid = "22222222"
        _cube(store / "22222222.parquet", [("v9", "x", "1.1"), ("v8", "y", "1.2")])   # 2 > cap 1
    elif setup == "lost_column":
        m = dict(smap)
        m["11111111"] = {"dim": "region", "parts": 2, "rows": 3}
        (store / "_split_map.json").write_text(json.dumps(m))
    else:
        m = dict(smap)
        m.pop("11111111")
        (store / "_split_map.json").write_text(json.dumps(m))
    report = tmp_path / "parts.json"
    rc = _pin(monkeypatch, db, report, "--only", pid)
    assert rc == 1, f"{why}: a report that could not judge a cube must say so in its exit code"
    assert json.loads(report.read_text())["cubes"][pid]["status"] == "refused", why


def test_pin_split_requires_the_stores_cap_from_a_FULL_run(world, monkeypatch, tmp_path):
    store, db, s3, smap = world
    report = tmp_path / "parts.json"
    base = ["--pin-split", "--catalog-db", str(db), "--parts-report", str(report), "--only", "11111111"]
    with pytest.raises(SystemExit, match="without --max-rows"):
        _run(monkeypatch, *base)
    summary = tmp_path / "logs" / "statcan_tables_summary.json"
    summary.write_text(json.dumps({"max_rows": 5, "scope": "dry_run"}))
    with pytest.raises(SystemExit, match="without --max-rows"):
        _run(monkeypatch, *base)                       # a dry run's cap is not evidence (P2)
    summary.write_text(json.dumps({"max_rows": 5, "scope": "full"}))
    with pytest.raises(SystemExit, match="disagrees with the store's recorded cap"):
        _run(monkeypatch, *base, "--max-rows", "1")
    assert _run(monkeypatch, *base) == 0, "a full run's recorded cap is adopted"


def test_pin_split_refuses_without_a_readable_or_non_empty_map(world, monkeypatch, tmp_path):
    store, db, s3, smap = world
    report = tmp_path / "parts.json"
    (store / "_split_map.json").write_text("{not json")
    with pytest.raises(SystemExit, match="REFUSING --pin-split"):
        _pin(monkeypatch, db, report, "--only", "11111111")
    (store / "_split_map.json").write_text("{}")
    with pytest.raises(SystemExit, match="holds no split decisions"):
        _pin(monkeypatch, db, report, "--only", "11111111")


# --------------------------------------------------------------------------- #
# real (writing) scoped runs: the map survives, failures are reported
# --------------------------------------------------------------------------- #
def test_a_limited_run_keeps_the_whole_map(world, monkeypatch):
    """The round-1 reviewer's probe: --limit (no --only) wrote back only its own entries."""
    store, db, s3, smap = world
    _run(monkeypatch, "--bucket", "b", "--max-rows", "1", "--limit", "1", "--rekey")
    after = json.loads((store / "_split_map.json").read_text())
    assert {"11111111", "33333333", "44444444", "55555555"} <= set(after), sorted(after)


def test_the_mid_run_map_write_keeps_the_other_cubes(world, monkeypatch):
    """The truncation happened MID-run, so check the file while the run is still going: kill it
    right after the first split decision is persisted."""
    store, db, s3, smap = world

    class _Stop(BaseException):
        pass
    real_replace = os.replace

    def _replace(src, dst):
        real_replace(src, dst)
        if str(dst).endswith("_split_map.json"):
            raise _Stop()
    monkeypatch.setattr(d.os, "replace", _replace)
    with pytest.raises(_Stop):
        _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    on_disk = json.loads((store / "_split_map.json").read_text())
    assert set(on_disk) == {"11111111", "33333333", "44444444", "55555555"}, sorted(on_disk)


def test_a_refused_cube_keeps_its_map_entry(world, monkeypatch):
    """Probe P1: a scoped run that refuses a cube wrote nothing for it - its catalogued parts still
    resolve through the old entry, which must survive both map writes."""
    store, db, s3, smap = world
    monkeypatch.setattr(d, "choose_split", lambda con, f, n, m: ("", 0))
    _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    assert json.loads((store / "_split_map.json").read_text())["11111111"] == smap["11111111"]


def test_a_failed_put_fails_the_run_and_is_reported_unwritten(world, monkeypatch, tmp_path):
    store, db, s3, smap = world
    monkeypatch.setattr(d, "choose_split", lambda con, f, n, m: ("geo", 2))
    s3.fail = {"Windsor"}
    report = tmp_path / "parts.json"
    rc = _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1",
              "--parts-report", str(report), "--catalog-db", str(db))
    assert rc == 1, "every PUT failing used to exit 0 (probe P5)"
    rep = json.loads(report.read_text())["cubes"]["11111111"]
    assert "statcan:11111111#Windsor - other locations" in rep["unwritten"]


def test_a_null_split_value_is_never_written_under_the_whole_cube_id(world, monkeypatch):
    """Probe P6: NULL-geo rows were written as `statcan:<pid>` - the id the resolver serves as the
    WHOLE cube - holding only the NULL slice."""
    store, db, s3, smap = world
    _cube(store / "11111111.parquet", [("v1", "Aden", "1.1"), ("v2", None, "2.1")])
    monkeypatch.setattr(d, "choose_split", lambda con, f, n, m: ("geo", 2))
    _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    assert "series/statcan%3A11111111.csv" not in s3.put, sorted(s3.put)
    assert "series/statcan%3A11111111%23Aden.csv" in s3.put


def test_negative_control_an_unpinned_scoped_run_writes_and_keeps_the_map(world, monkeypatch):
    store, db, s3, smap = world
    _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    assert s3.put, "an ordinary derive still writes"
    after = json.loads((store / "_split_map.json").read_text())
    assert "33333333" in after and "44444444" in after


# --------------------------------------------------------------------------- #
# round 4: atomic map, pinned writing runs, the cap, the exit code
# --------------------------------------------------------------------------- #
def test_a_kill_inside_the_map_write_leaves_the_old_map_whole(world, monkeypatch):
    """Probe P1: open(map, "w") empties the file first, so a kill mid-write left invalid JSON and
    the resolver raised for every # part id. Now the old map is intact until the new one is whole."""
    store, db, s3, smap = world

    class _Kill(BaseException):
        pass
    real_dump = json.dump

    def _half(obj, fh, *a, **k):
        if "_split_map.json" in getattr(fh, "name", ""):
            fh.write('{"11111111": {"di')
            raise _Kill()
        return real_dump(obj, fh, *a, **k)
    monkeypatch.setattr(d.json, "dump", _half)
    with pytest.raises(_Kill):
        _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1", "--rekey")
    assert json.loads((store / "_split_map.json").read_text()) == smap
    assert not [p for p in os.listdir(store) if p.endswith(".tmp")], "the temp file is cleaned up"


def test_a_writing_run_keeps_a_parts_cubes_recorded_split(world, monkeypatch):
    """Probe P2: a real --only run re-chose the split of a cube served as parts, wrote the whole-cube
    object, dropped the map entry and exited 0. Without --rekey it now keeps the recorded split."""
    store, db, s3, smap = world
    monkeypatch.setattr(d, "choose_split", lambda con, f, n, m: (None, 1))   # would serve it whole
    rc = _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1",
              "--catalog-db", str(db))
    assert rc == 0
    assert "series/statcan%3A11111111.csv" not in s3.put, sorted(s3.put)
    assert "series/statcan%3A11111111%23Aden.csv" in s3.put, sorted(s3.put)
    assert json.loads((store / "_split_map.json").read_text()) == smap


def test_a_pinned_full_run_keeps_entries_for_cubes_it_did_not_see(world, monkeypatch):
    """Round-5 minor 1: 33333333 and 44444444 are in the map with no store file; a full run that
    pins must not rebuild the map from only the cubes it found."""
    store, db, s3, smap = world
    assert _run(monkeypatch, "--bucket", "b", "--max-rows", "1", "--catalog-db", str(db)) == 0
    after = json.loads((store / "_split_map.json").read_text())
    assert {"33333333", "44444444"} <= set(after), sorted(after)


def test_a_writing_run_refuses_a_parts_cube_with_no_recorded_split(world, monkeypatch):
    store, db, s3, smap = world
    m = dict(smap)
    m.pop("11111111")
    (store / "_split_map.json").write_text(json.dumps(m))
    rc = _run(monkeypatch, "--bucket", "b", "--only", "11111111", "--max-rows", "1",
              "--catalog-db", str(db))
    assert rc == 1, "its served ids could not be reproduced: that is a failure, not a skip"
    assert not [k for k in s3.put if "11111111" in k], sorted(s3.put)
    assert json.loads((store / "_split_map.json").read_text()) == m, "the map never shrinks"


def test_every_writing_run_needs_the_stores_cap(world, monkeypatch, tmp_path):
    """Probe P3: a writing run without --max-rows split at the 500,000 default."""
    store, db, s3, smap = world
    base = ["--bucket", "b", "--only", "11111111", "--catalog-db", str(db)]
    with pytest.raises(SystemExit, match="without --max-rows"):
        _run(monkeypatch, *base)
    summary = tmp_path / "logs" / "statcan_tables_summary.json"
    summary.write_text(json.dumps({"max_rows": 1, "scope": "full"}))
    with pytest.raises(SystemExit, match="disagrees with the store's recorded cap"):
        _run(monkeypatch, *base, "--max-rows", "7")
    assert _run(monkeypatch, *base) == 0, "a full run's recorded cap is adopted"


def test_a_known_structural_refusal_does_not_fail_a_rekey_run(world, monkeypatch):
    """Probe P4: five cubes are refused by every full run; exiting 1 on them meant a guarded job
    never earned its done-sentinel and relaunched a ~7.8-day derive for ever."""
    store, db, s3, smap = world
    real = d.choose_split
    monkeypatch.setattr(d, "choose_split",
                        lambda con, f, n, m: ("", 0) if "11111111" in f else real(con, f, n, m))
    assert _run(monkeypatch, "--bucket", "b", "--max-rows", "1", "--rekey") == 0


def test_a_writing_run_refuses_an_unreadable_map(world, monkeypatch):
    """Probe P5: a full run started from {} and dropped a refused cube's entry."""
    store, db, s3, smap = world
    (store / "_split_map.json").write_text("{not json")
    for extra in ([], ["--rekey"]):
        with pytest.raises(SystemExit, match="unreadable"):
            _run(monkeypatch, "--bucket", "b", "--max-rows", "1", "--catalog-db", str(db), *extra)
    assert (store / "_split_map.json").read_text() == "{not json", "and it is left for a human"
    assert s3.put == {}
