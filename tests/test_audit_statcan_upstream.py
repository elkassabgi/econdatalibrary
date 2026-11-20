"""tools/audit_statcan_upstream.py: a statcan store behind its publisher must be named, whatever its
release date, and a cube it could not measure must never read as current.

Planted here (no network, no server): footers are real parquet bytes held in memory and read through
the real updater.blob._r2_footer_buffer (a fake blob whose get_range is blob.bytes_range).

  positives  annual 2024-12-31 vs publisher 2025-01-01 (one period); monthly two periods; a cube
             released before the lane floor; quarterly; weekly one week; a 6-day holiday week
  negatives  annual same period 364 and 365 (leap) days apart; monthly current by start and by end
             date; weekly same week; daily same day
  unmeasured absent object, a row group without statistics, unknown frequency, month-step end date
             not on day 01, non-date obs_date -> rc 2, "at least N"
  fatal      no catalogued cube, nothing measurable, a WDS FAILED body, a partial cube list -> rc 1
  withdrawn  a catalogued cube StatCan no longer lists: its own status, rc unchanged
  zip        stale / fresh / probe error, and "zip not probed" without --probe-zip
  read-only  the hook on the tool's own R2 client refuses PutObject before anything is sent
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import io
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_TOOL = os.path.join(_ROOT, "tools", "audit_statcan_upstream.py")
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from updater import blob  # noqa: E402


def _load():
    spec = importlib.util.spec_from_file_location("_statcan_upstream_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


T = _load()
NOW = dt.datetime(2026, 11, 19, 20, 0, tzinfo=dt.timezone.utc)
D = dt.date.fromisoformat


def _parquet(dates, *, row_group_size=None, stats=True, as_string=False) -> bytes:
    col = pa.array([str(d) for d in dates]) if as_string else pa.array([D(d) for d in dates], pa.date32())
    t = pa.table({"series_key": pa.array([f"v{i}" for i in range(len(dates))]), "obs_date": col,
                  "value": pa.array([1.0] * len(dates))})
    b = io.BytesIO()
    pq.write_table(t, b, row_group_size=row_group_size, write_statistics=stats)
    return b.getvalue()


class FakeStore:
    """get_range over in-memory objects (the contract _r2_footer_buffer reads)."""

    def __init__(self, objects):
        self.objects = objects

    def get_range(self, key, byte_range, *, if_match=None):
        data = self.objects.get(key)
        return None if data is None else blob.bytes_range(data, byte_range)


def _cube(pid, end, freq, release="2026-11-04T13:30:00Z", archived="2"):
    return {"productId": int(pid), "cubeEndDate": f"{end}T05:00:00Z", "releaseTime": release,
            "frequencyCode": freq, "archived": archived}


def _run(cubes_store, tmp_path, *, extra_cubes=(), probe=False, http_get=None, http_head=None, pids=None):
    """cubes_store: {pid: (cube dict, parquet bytes or None)}. Returns (rc, stdout rows, by-pid rows)."""
    import json
    cubes = [c for c, _ in cubes_store.values()] + list(extra_cubes)
    cj = tmp_path / "cubes.json"
    cj.write_text(json.dumps(cubes), encoding="utf-8")
    store = FakeStore({T.KEY_FMT.format(pid=p): b for p, (_, b) in cubes_store.items() if b is not None})
    out = tmp_path / "out.tsv"
    argv = ["--pids", ",".join(pids or cubes_store), "--cubes-json", str(cj), "--out", str(out), "--threads", "2"]
    if probe:
        argv.append("--probe-zip")
    rc = T.main(argv, store=store, http_get=http_get, http_head=http_head, now=NOW, sleep=lambda s: None)
    lines = out.read_text(encoding="utf-8").splitlines() if out.exists() else []
    rows = {}
    if lines:
        head = lines[0].split("\t")
        for ln in lines[1:]:
            r = dict(zip(head, ln.split("\t")))
            rows[r["pid"]] = r
    return rc, rows


# ------------------------------------------------------------- the period comparison (unit)

@pytest.mark.parametrize("store,pub,freq,status,lag", [
    ("2024-12-31", "2025-01-01", 12, "behind", 1),     # annual: end-dated store one period behind
    ("2023-12-31", "2025-01-01", 12, "behind", 2),
    ("2025-12-31", "2025-01-01", 12, "current", 0),    # 364 days apart, same period
    ("2024-12-31", "2024-01-01", 12, "current", 0),    # 365 days apart (leap year), same period
    ("2025-01-01", "2025-01-01", 12, "current", 0),    # start-dated store
    ("2026-01-01", "2025-01-01", 12, "ahead", 0),
    ("2026-07-01", "2026-09-01", 6, "behind", 2),      # monthly
    ("2026-09-01", "2026-09-01", 6, "current", 0),
    ("2026-09-30", "2026-09-01", 6, "current", 0),     # end-dated monthly
    ("2023-09-01", "2026-09-01", 6, "behind", 36),     # the CIMT shape
    ("2026-06-30", "2026-07-01", 9, "behind", 1),      # quarterly: Q2 end vs Q3 start
    ("2026-03-31", "2026-07-01", 9, "behind", 2),
    ("2026-09-30", "2026-07-01", 9, "current", 0),
    ("2025-06-30", "2025-07-01", 11, "behind", 1),     # semi-annual
    ("2021-12-31", "2026-01-01", 16, "behind", 1),     # every 5 years
    ("2020-01-01", "2020-01-01", 18, "current", 0),    # occasional
    ("2026-11-07", "2026-11-14", 2, "behind", 1),      # weekly, one week
    ("2026-11-08", "2026-11-14", 2, "behind", 1),      # weekly, a 6-day holiday week
    ("2026-11-14", "2026-11-14", 2, "current", 0),
    ("2026-11-16", "2026-11-17", 1, "behind", 1),      # daily
    ("2026-11-17", "2026-11-17", 1, "current", 0),
    ("2026-11-21", "2026-11-14", 2, "ahead", 0),       # weekly, a week beyond the publisher
    ("2026-11-20", "2026-11-14", 2, "current", 0),
    ("2026-11-18", "2026-11-17", 1, "ahead", 0),       # daily, a day beyond
])
def test_compare(store, pub, freq, status, lag):
    assert T.compare(D(store), D(pub), freq) == (status, lag)


def test_compare_refuses_unknown_frequency_and_off_start_month_step():
    with pytest.raises(T.Unmeasurable, match="unknown frequencyCode"):
        T.compare(D("2026-01-01"), D("2026-01-01"), 99)
    with pytest.raises(T.Unmeasurable, match="not a period start"):
        T.compare(D("2026-01-01"), D("2026-01-15"), 6)


# ------------------------------------------------------------- end to end through the footer reader

def test_planted_positives_negatives_and_rc0(tmp_path):
    cs = {
        "35100182": (_cube("35100182", "2025-01-01", 12, "2026-07-22T12:30:00Z"), _parquet(["2023-12-31", "2024-12-31"])),
        "37100266": (_cube("37100266", "2023-01-01", 12, "2026-06-10T12:30:00Z"), _parquet(["2022-12-31"])),  # pre-floor release
        "12100152": (_cube("12100152", "2026-09-01", 6), _parquet(["2023-08-01", "2023-09-01"], row_group_size=1)),
        "11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
        "11100002": (_cube("11100002", "2024-01-01", 12), _parquet(["2024-12-31"])),
        "18100004": (_cube("18100004", "2026-09-01", 6), _parquet(["2026-08-01", "2026-09-01"], row_group_size=1)),
    }
    rc, rows = _run(cs, tmp_path)
    assert rc == 0
    assert {p for p, r in rows.items() if r["status"] == "behind"} == {"35100182", "37100266", "12100152"}
    assert rows["12100152"]["lag_periods"] == "36"
    assert rows["12100152"]["store_max"] == "2023-09-01"      # max over BOTH row groups
    assert {rows[p]["status"] for p in ("11100001", "11100002", "18100004")} == {"current"}


def test_summary_line_counts(tmp_path, capsys):
    cs = {
        "35100182": (_cube("35100182", "2025-01-01", 12, "2026-07-22T12:30:00Z"), _parquet(["2024-12-31"])),
        "16100047": (_cube("16100047", "2026-09-01", 6, "2026-11-19T13:30:00Z"), _parquet(["2026-08-01"])),
        "11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
    }
    rc, _ = _run(cs, tmp_path)
    line = capsys.readouterr().out.splitlines()[0]
    assert rc == 0
    assert line.startswith("statcan upstream: 2 cubes behind publisher (zip not probed); overdue 1 (release >48h), recent 1;")
    assert "measured 3 of 3 catalogued; unmeasured 0" in line and "[cube level only]" in line


def test_summary_overdue_recent_ahead_archived_are_each_counted(tmp_path, capsys):
    cs = {
        "35100182": (_cube("35100182", "2025-01-01", 12, "2026-07-22T12:30:00Z"), _parquet(["2024-12-31"])),
        "35100181": (_cube("35100181", "2025-01-01", 12, "2026-07-22T12:30:00Z", archived="1"), _parquet(["2024-12-31"])),
        "16100047": (_cube("16100047", "2026-09-01", 6, "2026-11-19T13:30:00Z"), _parquet(["2026-08-01"])),
        "11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2026-12-31"])),            # ahead
        "11100002": (_cube("11100002", "2025-01-01", 12), _parquet(["2025-12-31"])),
    }
    rc, rows = _run(cs, tmp_path)
    line = capsys.readouterr().out.splitlines()[0]
    assert rc == 0 and rows["11100001"]["status"] == "ahead"
    assert "3 cubes behind publisher (zip not probed); overdue 2 (release >48h), recent 1;" in line
    assert "archived among behind 1; ahead 1; withdrawn 0; measured 5 of 5 catalogued" in line


def test_malformed_catalogue_ids_reach_the_summary(tmp_path, capsys):
    db = tmp_path / "cat.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 'statcan')",
                    [("statcan:11100001#1",), ("statcan:123#x",), ("statcanX",)])
    con.commit()
    con.close()
    import json
    cj = tmp_path / "c.json"
    cj.write_text(json.dumps([_cube("11100001", "2025-01-01", 12)]), encoding="utf-8")
    store = FakeStore({T.KEY_FMT.format(pid="11100001"): _parquet(["2025-12-31"])})
    rc = T.main(["--catalog", str(db), "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")],
                store=store, now=NOW)
    assert rc == 0 and "malformed statcan series ids 2" in capsys.readouterr().out


def test_naive_release_instant_is_unmeasured(tmp_path):
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", "2025-01-01", 12, release="2026-07-22T12:30:00"), _parquet(["2024-12-31"]))}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and "without a time zone" in rows["22220000"]["reason"]


@pytest.mark.parametrize("obj,reason", [
    (None, "store object absent"),
    (_parquet(["2024-12-31"], stats=False), "no obs_date statistics"),
    (_parquet(["2024-12-31"], as_string=True), "not a date column"),
])
def test_unmeasured_store_is_rc2_and_at_least(tmp_path, capsys, obj, reason):
    cs = {
        "35100182": (_cube("35100182", "2025-01-01", 12), _parquet(["2024-12-31"])),
        "11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
        "22220000": (_cube("22220000", "2025-01-01", 12), obj),
    }
    rc, rows = _run(cs, tmp_path)
    out = capsys.readouterr().out
    assert rc == 2
    assert rows["22220000"]["status"] == "unmeasured" and reason in rows["22220000"]["reason"]
    assert out.startswith("statcan upstream: at least 1 cubes behind publisher")
    assert "skipped 22220000" in out


def test_one_row_group_without_stats_among_others_is_unmeasured():
    t = pa.table({"obs_date": pa.array([D("2020-01-01")], pa.date32())})
    b1, b2 = io.BytesIO(), io.BytesIO()
    pq.write_table(t, b1)
    pq.write_table(t, b2, write_statistics=False)
    md_ok = pq.read_metadata(io.BytesIO(b1.getvalue()))
    assert T.store_max_date(md_ok) == D("2020-01-01")

    class MD:                                    # two row groups: the second has no statistics
        num_rows, num_row_groups, schema = 2, 2, md_ok.schema

        def row_group(self, g):
            return (md_ok if g == 0 else pq.read_metadata(io.BytesIO(b2.getvalue()))).row_group(0)
    with pytest.raises(T.Unmeasurable, match="row group 1"):
        T.store_max_date(MD())


@pytest.mark.parametrize("freq,end", [(99, "2025-01-01"), (6, "2026-09-15")])
def test_unknown_frequency_or_off_start_end_is_unmeasured(tmp_path, freq, end):
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", end, freq), _parquet(["2020-12-31"]))}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and rows["22220000"]["status"] == "unmeasured"


def test_withdrawn_is_its_own_status_and_keeps_rc0(tmp_path, capsys):
    # 1 of 2 catalogued pids in the list is below the 95% floor: a partial list, Fatal, no TSV
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"]))}
    rc, rows = _run(cs, tmp_path, pids=["11100001", "13100906"])
    assert rc == 1 and rows == {}
    assert "not a full list" in capsys.readouterr().out
    # with many catalogued cubes one withdrawn cube is a measured fact, not a failure
    many = {f"3{i:07d}": (_cube(f"3{i:07d}", "2025-01-01", 12), _parquet(["2025-12-31"])) for i in range(40)}
    rc, rows = _run(many, tmp_path, pids=list(many) + ["13100906"])
    out = capsys.readouterr().out
    assert rc == 0 and rows["13100906"]["status"] == "withdrawn"
    assert "withdrawn 1; measured 40 of 41 catalogued" in out


def test_nothing_measured_is_rc1_not_zero_behind(tmp_path, capsys):
    cs = {p: (_cube(p, "2025-01-01", 12), None) for p in ("11100001", "11100002")}
    rc, _ = _run(cs, tmp_path)
    out = capsys.readouterr().out
    assert rc == 1
    assert "NOT MEASURED - 0 of 2" in out and "0 cubes behind" not in out


def test_no_catalogued_cube_is_rc1(tmp_path, capsys):
    db = tmp_path / "cat.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.execute("INSERT INTO series VALUES ('ons:x', 'ons')")
    con.commit()
    con.close()
    cj = tmp_path / "c.json"
    cj.write_text('[{"productId": 11100001, "cubeEndDate": "2025-01-01T05:00:00Z", "frequencyCode": 12}]', encoding="utf-8")
    rc = T.main(["--catalog", str(db), "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")],
                store=FakeStore({}), now=NOW)
    assert rc == 1 and "no catalogued statcan cube" in capsys.readouterr().out


def test_catalogue_pids_and_malformed_ids_counted(tmp_path):
    db = tmp_path / "cat.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 'statcan')",
                    [("statcan:12100152#1.1.100.1",), ("statcan:12100152#1.1.100.2",), ("statcan:10100001",),
                     ("statcan:123#x",), ("statcanX",)])
    con.commit()
    con.close()
    pids, bad = T.catalogued_pids(str(db), allow_live=False)
    assert pids == ["10100001", "12100152"] and bad == 2


def test_live_catalogue_path_refused(monkeypatch, tmp_path):
    from core import catalog_path
    db = tmp_path / "catalog.db"
    sqlite3.connect(db).close()
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(db))
    with pytest.raises(T.Fatal, match="live catalogue"):
        T.catalogued_pids(str(db), allow_live=False)


def _post_t0(monkeypatch, tmp_path):
    """Simulate T0 the way production sees it: the flag FILE exists at cutover.FLAG_PATH. Patching the
    function would miss core.catalog_path, which imports is_cut_over by name (review R1973)."""
    from core import cutover
    flag = tmp_path / "CUTOVER"
    flag.write_text("", encoding="utf-8")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    from core import catalog_path
    assert cutover.is_cut_over() and catalog_path.is_cut_over()


def _statcan_db(path, pids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 'statcan')", [(f"statcan:{p}#1",) for p in pids])
    con.commit()
    con.close()


def _pre_t0(monkeypatch, tmp_path):
    """Pin BEFORE T0: point cutover.FLAG_PATH at a file that does not exist (a runner that has the real
    flag must not turn these into post-T0 tests)."""
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "NO_CUTOVER_FLAG"))
    from core import catalog_path
    assert not cutover.is_cut_over() and not catalog_path.is_cut_over()


def test_pre_t0_live_build_is_read_with_live_catalogue(monkeypatch, tmp_path, capsys):
    """Before T0 --live-catalogue opens the live build (review R1974, mutant RVF: a refusal that ignored
    allow_live passed every other test)."""
    import json
    from core import catalog_path
    build = tmp_path / "build" / "catalog.db"
    build.parent.mkdir()
    _statcan_db(build, ["11100001", "22220000"])
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(tmp_path / "checkout" / "catalog.db"))
    _pre_t0(monkeypatch, tmp_path)
    with pytest.raises(T.Fatal, match="pass a copy"):
        T.catalogued_pids(str(build), allow_live=False)
    assert T.catalogued_pids(str(build), allow_live=True) == (["11100001", "22220000"], 0)
    cj = tmp_path / "c.json"
    cj.write_text(json.dumps([_cube("11100001", "2025-01-01", 12), _cube("22220000", "2025-01-01", 12)]),
                  encoding="utf-8")
    store = FakeStore({T.KEY_FMT.format(pid="11100001"): _parquet(["2025-12-31"]),
                       T.KEY_FMT.format(pid="22220000"): _parquet(["2024-12-31"])})
    rc = T.main(["--catalog", str(build), "--live-catalogue", "--cubes-json", str(cj),
                 "--out", str(tmp_path / "o.tsv")], store=store, now=NOW)
    out = capsys.readouterr().out
    assert rc == 0 and "1 cubes behind publisher" in out and "measured 2 of 2 catalogued" in out


def test_pre_t0_checkout_catalogue_refused_without_live_catalogue(monkeypatch, tmp_path):
    """Before T0 the checkout's data/catalog.db is live too, not only the build (review R1974, mutant RVA:
    a live set of {build} alone passed every other test)."""
    from core import catalog_path
    checkout = tmp_path / "checkout" / "data" / "catalog.db"
    checkout.parent.mkdir(parents=True)
    _statcan_db(checkout, ["11100001"])
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "build" / "catalog.db"))
    monkeypatch.setattr(catalog_path, "CHECKOUT_PATH", str(checkout))
    _pre_t0(monkeypatch, tmp_path)
    with pytest.raises(T.Fatal, match="is a live catalogue; pass a copy"):
        T.catalogued_pids(str(checkout), allow_live=False)
    assert T.catalogued_pids(str(checkout), allow_live=True) == (["11100001"], 0)
    copy = tmp_path / "copy.db"                       # negative control: a copy is read without the flag
    _statcan_db(copy, ["11100001"])
    assert T.catalogued_pids(str(copy), allow_live=False) == (["11100001"], 0)


def test_post_t0_catalog_copy_refused_with_the_route(monkeypatch, tmp_path, capsys):
    import json
    from core import catalog_path
    build = tmp_path / "build" / "catalog.db"
    build.parent.mkdir()
    _statcan_db(build, ["11100001"])
    copy = tmp_path / "copy.db"
    _statcan_db(copy, ["11100001"])
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    _post_t0(monkeypatch, tmp_path)
    with pytest.raises(Exception, match="after T0"):         # connect_path itself refuses the copy
        catalog_path.connect_path(str(copy), write=False).close()
    cj = tmp_path / "c.json"
    cj.write_text(json.dumps([_cube("11100001", "2025-01-01", 12)]), encoding="utf-8")
    store = FakeStore({T.KEY_FMT.format(pid="11100001"): _parquet(["2025-12-31"])})
    for extra in ([], ["--live-catalogue"]):
        rc = T.main(["--catalog", str(copy), *extra, "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")],
                    store=store, now=NOW)
        out = capsys.readouterr().out
        assert rc == 1 and "NOT MEASURED" in out and "after T0 the only readable catalogue is the build" in out
        assert f"--catalog {build} --live-catalogue" in out and "pass a copy" not in out


def test_post_t0_build_needs_live_catalogue_and_is_then_read(monkeypatch, tmp_path, capsys):
    import json
    from core import catalog_path
    build = tmp_path / "build" / "catalog.db"
    build.parent.mkdir()
    _statcan_db(build, ["11100001", "22220000"])
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    _post_t0(monkeypatch, tmp_path)
    cj = tmp_path / "c.json"
    cj.write_text(json.dumps([_cube("11100001", "2025-01-01", 12), _cube("22220000", "2025-01-01", 12)]),
                  encoding="utf-8")
    store = FakeStore({T.KEY_FMT.format(pid="11100001"): _parquet(["2025-12-31"]),
                       T.KEY_FMT.format(pid="22220000"): _parquet(["2024-12-31"])})
    rc = T.main(["--catalog", str(build), "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")],
                store=store, now=NOW)
    out = capsys.readouterr().out
    assert rc == 1 and "pass --live-catalogue to read it" in out and "pass a copy" not in out
    assert T.catalogued_pids(str(build), allow_live=True) == (["11100001", "22220000"], 0)
    rc = T.main(["--catalog", str(build), "--live-catalogue", "--cubes-json", str(cj),
                 "--out", str(tmp_path / "o.tsv")], store=store, now=NOW)
    out = capsys.readouterr().out
    assert rc == 0 and "1 cubes behind publisher" in out and "measured 2 of 2 catalogued" in out


class Resp:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def json(self):
        return self._body


def test_wds_failed_body_is_rc1(tmp_path, capsys):
    rc = T.main(["--pids", "11100001", "--out", str(tmp_path / "o.tsv")], store=FakeStore({}),
                http_get=lambda url: Resp(200, {"status": "FAILED", "object": "x"}), now=NOW)
    assert rc == 1 and "NOT MEASURED" in capsys.readouterr().out


def _zip_http(lm_by_pid, fail=()):
    def get(url):
        pid = url.rstrip("/").split("/")[-2]
        if pid in fail:
            return Resp(200, {"status": "FAILED"})
        return Resp(200, {"status": "SUCCESS", "object": f"https://www150.statcan.gc.ca/n1/tbl/csv/{pid}-eng.zip"})

    def head(url):
        pid = url.rsplit("/", 1)[1][:8]
        return Resp(200, headers={"Last-Modified": lm_by_pid[pid]})
    return get, head


def test_zip_probe_stale_fresh_and_error(tmp_path, capsys):
    cs = {
        "12100147": (_cube("12100147", "2026-09-01", 6, "2026-11-04T13:30:00Z"), _parquet(["2023-09-01"])),
        "35100182": (_cube("35100182", "2025-01-01", 12, "2026-07-22T12:30:00Z"), _parquet(["2024-12-31"])),
        "35100181": (_cube("35100181", "2025-01-01", 12, "2026-07-22T12:30:00Z"), _parquet(["2024-12-31"])),
        "11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
    }
    get, head = _zip_http({"12100147": "Thu, 09 Nov 2023 14:00:00 GMT",
                           "35100182": "Wed, 22 Jul 2026 12:31:31 GMT",
                           "35100181": "Wed, 22 Jul 2026 12:31:23 GMT"}, fail={"35100181"})
    rc, rows = _run(cs, tmp_path, probe=True, http_get=get, http_head=head)
    out = capsys.readouterr().out
    assert rows["12100147"]["zip_class"] == "stale"
    assert rows["35100182"]["zip_class"] == "fresh"
    assert rows["35100181"]["zip_class"] == "probe_error"
    assert rows["11100001"]["zip_class"] == "not_probed"            # only BEHIND cubes are probed
    assert rc == 2
    assert out.startswith("statcan upstream: 3 cubes behind publisher (at least 1 zip-stale)")


def test_zip_slack_is_one_hour():
    cs_release = "2026-11-17T13:30:00Z"
    rel = T._instant(cs_release)
    assert rel - dt.timedelta(minutes=61) < rel - T.ZIP_SLACK < rel - dt.timedelta(minutes=59)


def test_zip_two_hours_before_release_is_stale(tmp_path):
    cs = {"10100145": (_cube("10100145", "2026-11-11", 1, "2026-11-13T13:30:00Z"), _parquet(["2026-11-10"]))}
    get, head = _zip_http({"10100145": "Fri, 13 Nov 2026 11:30:00 GMT"})
    rc, rows = _run(cs, tmp_path, probe=True, http_get=get, http_head=head)
    assert rc == 0 and rows["10100145"]["zip_class"] == "stale"


def test_read_only_hook_on_the_tools_own_client(monkeypatch):
    from core import r2_util
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {
        "endpoint": "https://example.invalid", "key": "k", "secret": "s", "mode": "read"})
    store = T.make_store("r2")
    with pytest.raises(PermissionError, match="refused S3 operation 'PutObject'"):
        store.client.put_object(Bucket="econ-data", Key="clean_full/statcan/x.parquet", Body=b"x")
    with pytest.raises(PermissionError, match="DeleteObject"):
        store.client.delete_object(Bucket="econ-data", Key="clean_full/statcan/x.parquet")


def test_injected_client_gets_the_same_hook():
    import boto3
    c = boto3.client("s3", endpoint_url="https://example.invalid", aws_access_key_id="k",
                     aws_secret_access_key="s", region_name="auto")
    store = T.make_store("r2", c)
    assert store.client is c
    assert T.describe_store(store) == "r2 econ-data"          # the default pre-T0 run names its bucket
    with pytest.raises(PermissionError, match="PutObject"):
        c.put_object(Bucket="econ-data", Key="x", Body=b"x")


def test_r2_backend_refused_after_t0(monkeypatch):
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    with pytest.raises(T.Fatal, match="--backend local"):
        T.make_store("r2")


# ------------------------------------------------------------- after T0: the LOCAL parquet store (R1969)

def _no_selfhost(monkeypatch):
    """Any construction of the self-hosted blob store fails the test (it opens the live index for writing)."""
    class Boom:
        def __init__(self, *a, **k):
            raise AssertionError("SelfhostBlob constructed")

    def boom_module():
        raise AssertionError("tools/selfhost/blobstore.py loaded")
    monkeypatch.setattr(blob, "SelfhostBlob", Boom)
    monkeypatch.setattr(blob, "_blobstore_module", boom_module)


def _local_root(tmp_path, objects):
    root = tmp_path / "data" / "clean_full"
    for pid, data in objects.items():
        f = root / "statcan" / f"{pid}.parquet"
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(data)
    return root


def test_auto_after_t0_measures_the_local_parquet_store(monkeypatch, tmp_path, capsys):
    """A planted behind cube and a current one, on disk under a tmp DATA_ROOT, measured with no store
    injected: auto must pick the local backend after T0 and read the footers through _r2_footer_buffer."""
    import json
    from core import cutover
    from updater import config
    _no_selfhost(monkeypatch)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: True)
    big = _parquet([f"2020-{m:02d}-01" for m in range(1, 13)] * 3000 + ["2024-12-31"], row_group_size=500)
    assert len(big) > blob._FOOTER_TAIL_BYTES // 64          # several row groups, a real footer
    root = _local_root(tmp_path, {"11100001": big, "11100002": _parquet(["2025-12-31"])})
    monkeypatch.setattr(config, "DATA_ROOT", str(root))
    calls = []
    real = blob._r2_footer_buffer
    monkeypatch.setattr(blob, "_r2_footer_buffer", lambda st, k: calls.append(type(st).__name__) or real(st, k))
    cj = tmp_path / "c.json"
    cj.write_text(json.dumps([_cube("11100001", "2025-01-01", 12), _cube("11100002", "2025-01-01", 12)]),
                  encoding="utf-8")
    out = tmp_path / "o.tsv"
    rc = T.main(["--pids", "11100001,11100002", "--cubes-json", str(cj), "--out", str(out)], now=NOW)
    text = capsys.readouterr().out
    assert rc == 0, text
    rows = {ln.split("\t")[0]: ln for ln in out.read_text(encoding="utf-8").splitlines()[1:]}
    assert "\tbehind\t" in rows["11100001"] and "2024-12-31" in rows["11100001"]
    assert "\tcurrent\t" in rows["11100002"]
    assert "1 cubes behind publisher" in text
    assert f"[store: local {os.path.abspath(str(root))}]" in text
    assert calls == ["LocalParquetStore", "LocalParquetStore"]


def _big_footer_parquet(last="2024-12-31") -> bytes:
    """A parquet whose footer is longer than the 1 MiB tail window: the footer reader must make its
    second, conditional read (138 of 8,264 local statcan stores have such footers, 2026-11-20)."""
    n, cols = 600, 40
    data = {f"c{j:02d}_{'x' * 40}": pa.array([float(i) for i in range(n)]) for j in range(cols)}
    data["obs_date"] = pa.array([D("2020-01-01")] * (n - 1) + [D(last)], pa.date32())
    b = io.BytesIO()
    pq.write_table(pa.table(data), b, row_group_size=1, write_statistics=True)
    raw = b.getvalue()
    assert int.from_bytes(raw[-8:-4], "little") > blob._FOOTER_TAIL_BYTES
    return raw


def test_local_big_footer_second_read_is_conditional(monkeypatch, tmp_path):
    _no_selfhost(monkeypatch)
    raw = _big_footer_parquet()
    root = _local_root(tmp_path, {"11100001": raw})
    st = T.make_store("local", data_root=str(root))
    seen = []
    real = blob._file_range
    monkeypatch.setattr(blob, "_file_range", lambda p, r, m: seen.append((r, m)) or real(p, r, m))
    assert T.read_store_max(st, "11100001") == D("2024-12-31")
    assert len(seen) == 2 and seen[0][1] is None
    f = root / "statcan" / "11100001.parquet"
    assert seen[1][1] == f"{f.stat().st_size}:{f.stat().st_mtime_ns}"      # the first read's token
    assert pq.read_metadata(str(f)).num_row_groups == 600


def test_local_big_footer_replaced_between_reads_is_unmeasured(monkeypatch, tmp_path):
    """The file is replaced after the tail read: the conditional footer read must refuse, never splice
    two versions or answer from the new file under the old file's tail."""
    _no_selfhost(monkeypatch)
    root = _local_root(tmp_path, {"11100001": _big_footer_parquet()})
    f = root / "statcan" / "11100001.parquet"
    replacement = _big_footer_parquet(last="2026-12-31")
    st = T.make_store("local", data_root=str(root))
    real = blob._file_range
    n = []

    def swap(p, r, m):
        out = real(p, r, m)
        if not n:
            f.write_bytes(replacement + b"")
            os.utime(f, ns=(1, 1))                                          # a different token for sure
        n.append(r)
        return out
    monkeypatch.setattr(blob, "_file_range", swap)
    with pytest.raises(T.Unmeasurable, match="footer read failed"):
        T.read_store_max(st, "11100001")


def test_local_backend_absent_object_and_bad_root(monkeypatch, tmp_path):
    _no_selfhost(monkeypatch)
    root = _local_root(tmp_path, {})
    (root / "statcan").mkdir(parents=True)
    st = T.make_store("local", data_root=str(root))
    with pytest.raises(T.Unmeasurable, match="store object absent"):
        T.read_store_max(st, "11100001")
    with pytest.raises(ValueError):                           # LocalStoreReader's key check still applies
        st.get_range("clean_full/../x.parquet", "bytes=-8")
    with pytest.raises(T.Fatal, match="is not a directory"):
        T.make_store("local", data_root=str(tmp_path / "nowhere"))


def test_local_store_has_no_write_method():
    names = {n for n in dir(T.LocalParquetStore) if not n.startswith("__")}
    assert names == {"get_range"}


def test_auto_before_t0_is_r2(monkeypatch):
    from core import cutover, r2_util
    _no_selfhost(monkeypatch)
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    monkeypatch.setattr(r2_util, "creds", lambda write=False: {
        "endpoint": "https://example.invalid", "key": "k", "secret": "s", "mode": "read"})
    assert isinstance(T.make_store("auto"), blob.R2Blob)


def test_no_selfhost_store_is_ever_constructed(monkeypatch, tmp_path):
    """Every backend path, with the self-hosted store booby-trapped; and no code path names it."""
    import ast
    import io as _io
    import tokenize
    from core import cutover
    _no_selfhost(monkeypatch)
    root = _local_root(tmp_path, {"11100001": _parquet(["2025-12-31"])})
    for cut in (True, False):
        monkeypatch.setattr(cutover, "is_cut_over", lambda cut=cut: cut)
        for backend in ("auto", "local", "r2", "selfhost"):
            try:
                T.make_store(backend, data_root=str(root))
            except (T.Fatal, Exception) as e:                 # r2 without creds / refused / unknown
                assert "constructed" not in str(e) and "loaded" not in str(e)
    src = open(_TOOL, encoding="utf-8").read()
    doc = {ln for n in ast.walk(ast.parse(src))
           if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body
           and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)
           for ln in range(n.body[0].lineno, n.body[0].end_lineno + 1)}
    code = " ".join(t.string for t in tokenize.generate_tokens(_io.StringIO(src).readline)
                    if t.type == tokenize.NAME and t.start[0] not in doc)
    for word in ("SelfhostBlob", "BlobStore", "_blobstore_module", "from_env", "csv_store", "selfhost"):
        assert word not in code.split(), word


# ------------------------------------------------------------- exit codes (R1969)

def test_usage_error_and_partial_run_have_different_codes(tmp_path, capsys):
    usage = T.main(["--pids", "11100001"])                                  # --out missing
    bad_choice = T.main(["--pids", "11100001", "--out", "x", "--backend", "selfhost"])
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", "2025-01-01", 12), None)}
    partial, _ = _run(cs, tmp_path)
    assert usage == 1 and bad_choice == 1 and partial == 2
    assert "[store: injected FakeStore]" in capsys.readouterr().out
    assert T.main(["--help"]) == 0
    help_text = capsys.readouterr().out
    assert "ALARM ON THE BEHIND COUNT" in help_text and "NEVER ON rc ALONE" in help_text
    assert "2  PARTIAL" in help_text


@pytest.mark.parametrize("freq", [[12], {"a": 1}, True, None, 12.0])
def test_malformed_frequency_code_is_unmeasured_not_a_crash(tmp_path, freq):
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", "2025-01-01", freq), _parquet(["2020-12-31"]))}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and rows["22220000"]["status"] == "unmeasured"
    assert "malformed frequencyCode" in rows["22220000"]["reason"]


def test_no_r2_credentials_is_rc1_not_a_traceback(monkeypatch, tmp_path, capsys):
    from core import r2_util
    monkeypatch.setattr(r2_util, "creds", lambda write=False: None)
    cj = tmp_path / "c.json"
    cj.write_text('[{"productId": 11100001, "cubeEndDate": "2025-01-01T05:00:00Z", "frequencyCode": 12}]',
                  encoding="utf-8")
    rc = T.main(["--pids", "11100001", "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")], now=NOW)
    assert rc == 1 and "R2 read client unavailable" in capsys.readouterr().out


def test_tool_never_imports_the_lane():
    import ast
    tree = ast.parse(open(_TOOL, encoding="utf-8").read())
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            names.update(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            names.add(n.module or "")
            names.update(f"{n.module}.{a.name}" for a in n.names)
    assert not [x for x in names if "statcan_lane" in x or x.startswith("jobs")]
    assert not [x for x in names if "ingest" in x]


# ------------------------------------------------------------- guard branches (review R1977, mutants RW*)

def test_day_step_lag_rounds_up():
    """RW25: 13 days at a 7-day step is 2 weeks behind, not 1 (the 6-day case alone cannot tell
    ceiling from floor, because max(1, 0) is 1 too)."""
    assert T.compare(D("2026-11-01"), D("2026-11-14"), 2) == ("behind", 2)
    assert T.compare(D("2026-11-08"), D("2026-11-14"), 2) == ("behind", 1)        # the 6-day holiday week


@pytest.mark.parametrize("end", ["20250101", "2025-1-1", 20250101])
def test_short_or_basic_format_publisher_date_is_unmeasured(tmp_path, end):
    """RW03: Python 3.11+ date.fromisoformat accepts '20250101'; the length check must refuse it."""
    with pytest.raises(T.Unmeasurable, match="unparseable publisher date"):
        T._date_part(end)
    c = _cube("22220000", "2025-01-01", 12)
    c["cubeEndDate"] = end
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (c, _parquet(["2020-12-31"]))}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and rows["22220000"]["status"] == "unmeasured"
    assert "unparseable publisher date" in rows["22220000"]["reason"]


def test_zero_row_store_is_unmeasured_with_its_reason(tmp_path):
    """RW07: an empty table is still written with one row group, so the reason must say 0 rows."""
    empty = _parquet([])
    md = pq.read_metadata(io.BytesIO(empty))
    assert md.num_rows == 0 and md.num_row_groups == 1
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", "2025-01-01", 12), empty)}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and rows["22220000"]["status"] == "unmeasured"
    assert rows["22220000"]["reason"] == "store has 0 rows"


def test_all_null_obs_date_has_no_min_max_and_is_unmeasured(tmp_path):
    """RW05: an all-null obs_date column carries statistics (not None) without min/max."""
    t = pa.table({"obs_date": pa.array([None, None], pa.date32())})
    b = io.BytesIO()
    pq.write_table(t, b)
    st = pq.read_metadata(io.BytesIO(b.getvalue())).row_group(0).column(0).statistics
    assert st is not None and not st.has_min_max
    cs = {"11100001": (_cube("11100001", "2025-01-01", 12), _parquet(["2025-12-31"])),
          "22220000": (_cube("22220000", "2025-01-01", 12), b.getvalue())}
    rc, rows = _run(cs, tmp_path)
    assert rc == 2 and rows["22220000"]["reason"] == "no obs_date statistics in row group 0"


def _cube_list_run(tmp_path, http_get):
    return T.main(["--pids", "11100001", "--out", str(tmp_path / "o.tsv")],
                  store=FakeStore({T.KEY_FMT.format(pid="11100001"): _parquet(["2025-12-31"])}),
                  http_get=http_get, now=NOW)


def test_cube_list_non_200_is_rc1_even_with_a_list_body(tmp_path, capsys):
    """RW08: a 503 whose body happens to be a valid cube list must not be measured."""
    body = [_cube("11100001", "2025-01-01", 12)]
    assert _cube_list_run(tmp_path, lambda url: Resp(200, body)) == 0              # negative control
    capsys.readouterr()
    assert _cube_list_run(tmp_path, lambda url: Resp(503, body)) == 1
    assert "getAllCubesListLite answered HTTP 503" in capsys.readouterr().out


@pytest.mark.parametrize("bad", ["11100001", 11100001, None, ["x"], {"cubeEndDate": "2025-01-01"}])
def test_cube_list_bad_element_is_rc1(tmp_path, capsys, bad):
    """RW09: a non-object element, or an object without productId, is Fatal (rc 1) - never a traceback."""
    import json
    body = [_cube("11100001", "2025-01-01", 12), bad]
    assert _cube_list_run(tmp_path, lambda url: Resp(200, body)) == 1
    assert "element without productId" in capsys.readouterr().out
    cj = tmp_path / "c.json"                                    # the --cubes-json route too
    cj.write_text(json.dumps(body), encoding="utf-8")
    rc = T.main(["--pids", "11100001", "--cubes-json", str(cj), "--out", str(tmp_path / "o.tsv")],
                store=FakeStore({}), now=NOW)
    assert rc == 1 and "element without productId" in capsys.readouterr().out


def test_unreadable_catalogue_names_its_reason(monkeypatch, tmp_path, capsys):
    """RW28: a catalogue that cannot be read is 'catalogue unreadable', not 'no catalogued cube'."""
    _pre_t0(monkeypatch, tmp_path)
    db = tmp_path / "copy.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE other (x)")                       # no series table
    con.commit()
    con.close()
    with pytest.raises(T.Fatal, match="catalogue unreadable"):
        T.catalogued_pids(str(db), allow_live=False)
    rc = T.main(["--catalog", str(db), "--cubes-json", str(tmp_path / "unused.json"),
                 "--out", str(tmp_path / "o.tsv")], store=FakeStore({}), now=NOW)
    out = capsys.readouterr().out
    assert rc == 1 and "catalogue unreadable" in out and "no catalogued statcan cube" not in out


def test_one_bad_pid_among_many_good_is_rc1_not_withdrawn(tmp_path, capsys):
    """RW29: with 40 good ids one bad id would pass the 95% list check as 'withdrawn' with rc 0."""
    many = {f"3{i:07d}": (_cube(f"3{i:07d}", "2025-01-01", 12), _parquet(["2025-12-31"])) for i in range(40)}
    rc, rows = _run(many, tmp_path, pids=list(many))                # negative control
    assert rc == 0 and len(rows) == 40
    capsys.readouterr()
    for bad in ("1110000x", "1234567", "123456789"):
        out_tsv = tmp_path / "out.tsv"
        if out_tsv.exists():
            out_tsv.unlink()
        rc, rows = _run(many, tmp_path, pids=list(many) + [bad])
        out = capsys.readouterr().out
        assert rc == 1 and rows == {}, (bad, out)
        assert "not 8-digit product ids" in out and bad in out


def _behind_cube():
    return {"12100147": (_cube("12100147", "2026-09-01", 6, "2026-11-04T13:30:00Z"), _parquet(["2023-09-01"]))}


def test_zip_url_must_be_https(tmp_path):
    """RW10: an http:// zip URL is refused (probe_error), never HEADed and classed."""
    heads = []

    def get(url):
        return Resp(200, {"status": "SUCCESS", "object": "http://www150.statcan.gc.ca/n1/tbl/csv/12100147-eng.zip"})

    def head(url):
        heads.append(url)
        return Resp(200, headers={"Last-Modified": "Thu, 09 Nov 2023 14:00:00 GMT"})
    rc, rows = _run(_behind_cube(), tmp_path, probe=True, http_get=get, http_head=head)
    assert rc == 2 and rows["12100147"]["zip_class"] == "probe_error"
    assert "gave no zip URL" in rows["12100147"]["reason"] and heads == []


@pytest.mark.parametrize("status", [404, 403, 304, 500])
def test_zip_head_not_200_is_probe_error_even_with_last_modified(tmp_path, status):
    """RW11: a non-200 HEAD that still carries Last-Modified must not be classed fresh or stale."""
    get, _ = _zip_http({})

    def head(url):
        return Resp(status, headers={"Last-Modified": "Thu, 09 Nov 2023 14:00:00 GMT"})
    rc, rows = _run(_behind_cube(), tmp_path, probe=True, http_get=get, http_head=head)
    assert rc == 2 and rows["12100147"]["zip_class"] == "probe_error"
    assert rows["12100147"]["reason"] == f"zip HEAD HTTP {status}"


def test_zip_naive_last_modified_is_probe_error_not_a_traceback(tmp_path, capsys):
    """RW12: RFC 2822 '-0000' parses to a NAIVE datetime; compared with the aware release time it would
    raise TypeError and kill the run. It must end as probe_error with rc 2."""
    from email.utils import parsedate_to_datetime
    lm = "Thu, 09 Nov 2023 14:00:00 -0000"
    assert parsedate_to_datetime(lm).tzinfo is None
    get, head = _zip_http({"12100147": lm})
    rc, rows = _run(_behind_cube(), tmp_path, probe=True, http_get=get, http_head=head)
    assert rc == 2 and rows["12100147"]["zip_class"] == "probe_error"
    assert rows["12100147"]["reason"] == "zip Last-Modified without a time zone"
    assert "zip probe errors 1" in capsys.readouterr().out
