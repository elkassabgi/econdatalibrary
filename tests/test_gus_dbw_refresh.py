"""The gus_dbw year-boundary upsert, which is the only thing standing between a refresh
and 1.24 billion rows of silently destroyed data.

gus_dbw froze on 2026-08-24: the backfill finished, wrote `logs/gus_dbw.DONE`, and the guard
reads that filename as "never relaunch" (R475). The refresh path that unfreezes it replaces
every observation from the tail year onward with a fresh fetch, so these tests pin the two
directions that matter — that history BELOW the cutoff is never touched, and that a genuine
upstream withdrawal above it IS mirrored rather than papered over.
"""
from __future__ import annotations

import datetime as dt
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from jobs import gus_dbw_refresh as R


def _write(path, rows):
    """rows = [(series_key, date, value)]"""
    pq.write_table(pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
    }), path, compression="zstd")
    return path


def _read(path):
    t = pq.read_table(path)
    return sorted(zip(t.column("series_key").to_pylist(),
                      [d.isoformat() for d in t.column("obs_date").to_pylist()],
                      t.column("value").to_pylist()))


@pytest.fixture
def area(tmp_path):
    final = _write(str(tmp_path / "area_1.parquet"), [
        ("v1:s1:a", dt.date(2019, 12, 31), 1.0),
        ("v1:s1:a", dt.date(2020, 12, 31), 2.0),
        ("v1:s1:a", dt.date(2025, 12, 31), 3.0),     # tail: stale value
        ("v1:s1:b", dt.date(2026, 12, 31), 4.0),     # tail: series GUS later withdraws
    ])
    return str(tmp_path), final


def test_history_below_the_cutoff_is_untouched(area, tmp_path):
    d, final = area
    tail = _write(str(tmp_path / "tail.parquet"), [("v1:s1:a", dt.date(2025, 12, 31), 99.0)])
    R.rebuild_with_tail(final, [tail], 2025)
    got = _read(final)
    assert ("v1:s1:a", "2019-12-31", 1.0) in got
    assert ("v1:s1:a", "2020-12-31", 2.0) in got


def test_a_revised_value_in_the_tail_is_replaced_not_duplicated(area, tmp_path):
    d, final = area
    tail = _write(str(tmp_path / "tail.parquet"), [("v1:s1:a", dt.date(2025, 12, 31), 99.0)])
    R.rebuild_with_tail(final, [tail], 2025)
    got = _read(final)
    assert ("v1:s1:a", "2025-12-31", 99.0) in got
    assert ("v1:s1:a", "2025-12-31", 3.0) not in got, "the stale value must be gone"
    assert sum(1 for k, dd, _ in got if k == "v1:s1:a" and dd == "2025-12-31") == 1


def test_an_upstream_withdrawal_is_mirrored(area, tmp_path):
    """A complete fetch that omits a series means GUS withdrew it. That is a real revision."""
    d, final = area
    tail = _write(str(tmp_path / "tail.parquet"), [("v1:s1:a", dt.date(2025, 12, 31), 99.0)])
    res = R.rebuild_with_tail(final, [tail], 2025)
    got = _read(final)
    assert not [k for k, _, _ in got if k == "v1:s1:b"], "withdrawn series must not survive"
    assert res["rows_before"] == 4 and res["rows_after"] == 3
    assert res["dropped"] == 2 and res["added"] == 1 and res["kept"] == 2


def test_counts_describe_what_actually_changed(area, tmp_path):
    d, final = area
    tail = _write(str(tmp_path / "tail.parquet"), [
        ("v1:s1:a", dt.date(2025, 12, 31), 3.0),
        ("v1:s1:b", dt.date(2026, 12, 31), 4.0),
    ])
    res = R.rebuild_with_tail(final, [tail], 2025)
    assert res["rows_before"] == res["rows_after"] == 4, "an unchanged tail must not change counts"


def test_an_empty_tail_removes_the_tail_years(area, tmp_path):
    """Not a bug: an area whose recent years GUS no longer publishes loses those years."""
    d, final = area
    res = R.rebuild_with_tail(final, [], 2025)
    assert res["rows_after"] == 2 and res["added"] == 0
    assert all(int(dd[:4]) < 2025 for _, dd, _ in _read(final))


def test_the_cutoff_is_inclusive_of_january_first(tmp_path):
    final = _write(str(tmp_path / "a.parquet"), [
        ("k", dt.date(2024, 12, 31), 1.0),
        ("k", dt.date(2025, 1, 1), 2.0),
    ])
    res = R.rebuild_with_tail(final, [], 2025)
    assert res["kept"] == 1 and res["dropped"] == 1, "2025-01-01 is inside the 2025 tail"


def test_a_failed_rebuild_leaves_the_original_intact(area, tmp_path):
    d, final = area
    before = _read(final)
    bad = str(tmp_path / "corrupt.parquet")
    with open(bad, "wb") as f:
        f.write(b"not a parquet file")
    with pytest.raises(Exception):
        R.rebuild_with_tail(final, [bad], 2025)
    assert _read(final) == before, "the served copy must survive a failed refresh"
    assert not [f for f in os.listdir(d) if f.endswith(".refresh.tmp")], "no temp left behind"


def test_streaming_across_many_batches_preserves_every_row(tmp_path, monkeypatch):
    """area_46 holds 529M rows; the rebuild must never depend on the file fitting in memory."""
    monkeypatch.setattr(R, "BATCH_ROWS", 7)
    rows = [("k%d" % i, dt.date(2000 + (i % 20), 1, 1), float(i)) for i in range(500)]
    final = _write(str(tmp_path / "big.parquet"), rows)
    res = R.rebuild_with_tail(final, [], 2019)
    expected_kept = sum(1 for _, dd, _ in rows if dd.year < 2019)
    assert res["kept"] == expected_kept
    assert res["rows_before"] == 500


# ----------------------------------------------------------------- scheduling
def test_an_area_never_refreshed_is_due():
    assert R.area_due(1, {}) is True


def test_a_recently_refreshed_area_is_not_due():
    now = dt.datetime(2026, 8, 25, 12, 0, 0)
    st = R.mark_refreshed(1, {}, when=now - dt.timedelta(days=3))
    assert R.area_due(1, st, now=now) is False


def test_an_area_past_the_cadence_is_due():
    now = dt.datetime(2026, 8, 25, 12, 0, 0)
    st = R.mark_refreshed(1, {}, when=now - dt.timedelta(days=R.REFRESH_DAYS))
    assert R.area_due(1, st, now=now) is True


@pytest.mark.parametrize("bad", [{"last_refresh": "garbage"}, {"last_refresh": None}, "notadict"])
def test_unreadable_state_fails_towards_refreshing(bad):
    """Failing the other way would freeze the area silently — the defect this module ends."""
    assert R.area_due(1, {"1": bad}) is True


def test_state_round_trips(tmp_path):
    st = R.mark_refreshed(7, {}, rows_before=10, rows_after=12)
    R.save_state(st, out_dir=str(tmp_path))
    back = R.load_state(out_dir=str(tmp_path))
    assert back["7"]["rows_after"] == 12
    assert R.area_due(7, back) is False


def test_tail_start_year_covers_two_calendar_years():
    assert R.tail_start_year(dt.date(2026, 8, 25)) == 2025
    assert R.tail_start_year(dt.date(2026, 1, 1)) == 2025
    assert R.tail_start_year(dt.date(2026, 8, 25), years=1) == 2026


# ----------------------------------------------------------------- wiring
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRAWLER = os.path.join(ROOT, "jobs", "ingest_gus_dbw.py")


def test_the_crawler_imports_the_way_the_guard_launches_it():
    """RELAUNCH_GUARD.ps1 runs `python jobs/ingest_gus_dbw.py` from the repo root.

    That puts jobs/ on sys.path and NOT the repo root, so `from jobs import ...` raised
    ModuleNotFoundError before main() was reached — the refresh wiring would have taken the
    crawler from frozen to crashing-on-startup. Pinned because nothing else would catch it:
    every other import path in the test suite happens to work.
    """
    import subprocess
    import sys
    r = subprocess.run(
        [sys.executable, "-c",
         'import runpy; runpy.run_path(%r, run_name="not_main")' % CRAWLER],
        capture_output=True, text=True, timeout=180, cwd=ROOT)
    assert r.returncode == 0, (r.stdout + r.stderr)[-900:]


def test_the_crawler_never_writes_the_guards_retire_flag():
    """logs/gus_dbw.DONE means 'never relaunch' to the guard (R475).

    The crawler used to write it to mean 'this pass finished', which retired the source with
    1.24 billion rows and no update path. A success sentinel and a retire flag must not be the
    same file, so the success record has its own name.
    """
    src = open(CRAWLER, encoding="utf-8").read()
    assert '"gus_dbw.DONE"' not in src and "'gus_dbw.DONE'" not in src
    assert "gus_dbw.BACKFILL_COMPLETE" in src


def test_the_refresh_checkpoints_under_its_own_key():
    """Sharing done_year_sections would make the backfill and the refresh read each other's
    progress as their own — the refresh would look already-done on its first run."""
    import inspect
    import sys
    sys.path.insert(0, ROOT)
    from jobs import ingest_gus_dbw as G
    sig = inspect.signature(G.fetch_section)
    assert sig.parameters["ckpt_key"].default == "done_year_sections"
    assert "refresh_year_sections" in inspect.getsource(G.refresh_pass)
    assert "section_dir=area_dir" in inspect.getsource(G.refresh_pass)


def test_an_incomplete_sweep_never_rebuilds_the_area():
    """The safety interlock: a partial fetch must not be mistaken for an upstream withdrawal."""
    import inspect
    import sys
    sys.path.insert(0, ROOT)
    from jobs import ingest_gus_dbw as G
    src = inspect.getsource(G.refresh_pass)
    i_guard = src.index("if not area_ok:")
    i_rebuild = src.index("rebuild_with_tail")
    assert i_guard < i_rebuild, "the completeness check must gate the rebuild"
    assert "continue" in src[i_guard:i_rebuild]
