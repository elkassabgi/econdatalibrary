"""A no-cursors pass books a full_rederive_owed debt only when the store CHANGED since the last accounted turn.

THE DEFECT (gate re-triage 2026-11-11). The no-cursors branch of _derive_changed_csvs booked a debt
whenever a fetcher reported neither series_cursors nor merged_rows and `res.obs` (the STORE TOTAL) was
non-zero. Passes that merged nothing booked debts nothing pays: bfs 2026-10-20 (a 9.3 s pass stopped
by a catalogue HTTP 429), nyfed 2026-10-21 (8/8 sub-units transient), dst 2026-10-18.

THE PREDICATE (orchestrate._no_store_write_proven): no `*.parquet` under the unit's out dirs changed
after the END of the unit's newest ACCOUNTED turn - a main-path runs row (ok/partial/no_change, obs > 0)
written after its derive phase. Writes left by a failed turn (obs 0 rows), an unrecorded kill, or a CI
pass whose state push was lost all postdate that row, so they still book (review R1865). Measured on the
R2 listing under r2, on local mtimes otherwise. Every doubt books. merged_rows > 0 always books.

Existing owed rows are not touched by this change.
"""
from __future__ import annotations

import os
import sys
import time
import types
import unittest.mock as mock
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate, registry                     # noqa: E402
from updater.errors import TransientError, DefinitiveError    # noqa: E402
from updater.state import StateStore                          # noqa: E402
from updater.strategies import fetchers                       # noqa: E402
from updater.strategies.base import Result, Strategy, Unit    # noqa: E402

HOUR = 3600.0


def _ago(seconds):
    return time.time() - seconds


def _iso_ago(seconds):
    return datetime.fromtimestamp(_ago(seconds), timezone.utc).isoformat()


def _write_parquet(path, seconds_ago=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": ["2026-01-01"], "value": [1.0]}), path)
    if seconds_ago is not None:
        t = _ago(seconds_ago)
        os.utime(path, (t, t))


class _U:
    """A workstation-route unit by default: the only route whose local mtimes are trusted without r2."""

    def __init__(self, sid, out_paths, uid="_all", run_location="local"):
        self.source_id, self.unit_id, self.out_paths = sid, uid, out_paths
        self.key = f"{sid}/{uid}"
        self.config = {"run_location": run_location}


def _res(**kw):
    base = dict(status="partial", obs=5_408_880, series_cursors=None, new_vintage="date-tail")
    base.update(kw)
    return Result(**base)


def _derive(unit, res, store):
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=582, create=True):
        return orchestrate._derive_changed_csvs(unit, res, blob=None, store=store)


def _row(st, sid, status, seconds_ago, obs=5_408_880, note="n", uid="_all"):
    st.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES(?,?,?,?,?,?,?)",
                  (_iso_ago(seconds_ago), sid, uid, status, obs, 10.0, note))
    st.db.commit()


def _store(tmp_path, sid, floor_ago=1 * HOUR, status="ok"):
    st = StateStore(path=str(tmp_path / "state.db"))
    if floor_ago is not None:
        _row(st, sid, status, floor_ago)
    return st


def _dir(tmp_path, sid):
    return str(tmp_path / "store" / sid)


@pytest.fixture(autouse=True)
def _local_backend(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")


def _books(note):
    return note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


# ---- the branch itself (local backend) --------------------------------------------------------------------

def test_a_zero_write_pass_books_no_debt(tmp_path):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    failed, note, deferred, reasons = _derive(_U("bfs", [d]), _res(), _store(tmp_path, "bfs"))
    assert note is None and failed == [] and deferred == [] and reasons == {}


def test_negative_control_a_pass_that_wrote_parquet_without_cursors_still_books(tmp_path):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"))                 # written during this pass
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), _store(tmp_path, "bfs"))
    assert _books(note)


def test_an_unrecorded_killed_turn_s_write_books_on_the_next_quiet_pass(tmp_path):
    """Review R1865's probe: ok turn 2 h ago, then a turn that wrote and was killed WITHOUT a runs row
    (1 h ago), then this quiet pass. Its own walk of 'this pass' would be empty; the floor is the ok turn."""
    d = _dir(tmp_path, "dst")
    _write_parquet(os.path.join(d, "AKU110.parquet"), seconds_ago=1 * HOUR)
    _, note, _, _ = _derive(_U("dst", [d]), _res(), _store(tmp_path, "dst", floor_ago=2 * HOUR))
    assert _books(note)


@pytest.mark.parametrize("status", ["transient_fail", "partial", "killed_external", "locked",
                                    "window_stopped", "no_change"])
def test_a_failure_or_non_turn_row_is_never_the_floor(tmp_path, status):
    """A failure row (obs 0, the _record path - a pre-fix DefinitiveError partial included), a kill
    record, or a non-turn row written AFTER the write must not move the floor past it."""
    d = _dir(tmp_path, "dst")
    st = _store(tmp_path, "dst", floor_ago=3 * HOUR)
    _write_parquet(os.path.join(d, "AKU110.parquet"), seconds_ago=1 * HOUR)
    _row(st, "dst", status, 0.5 * HOUR, obs=0, note="structural: 1/25 parsed 0 rows")
    _, note, _, _ = _derive(_U("dst", [d]), _res(), st)
    assert _books(note)


def test_a_no_change_turn_that_rewrote_the_store_is_not_the_floor(tmp_path):
    """Review R1868's probe: ok 3 h ago, a same-period rewrite 2 h ago by a turn that finalize called
    no_change (obs = the store total, > 0; no_change never derives), then this quiet pass: it books."""
    d = _dir(tmp_path, "dst")
    st = _store(tmp_path, "dst", floor_ago=3 * HOUR)
    _write_parquet(os.path.join(d, "AKU110.parquet"), seconds_ago=2 * HOUR)
    _row(st, "dst", "no_change", 1 * HOUR, obs=11_159_165, note="no new rows")
    _, note, _, _ = _derive(_U("dst", [d]), _res(), st)
    assert _books(note)


def test_a_cloud_route_unit_is_not_judged_by_a_local_walk(tmp_path):
    """Without r2, a cloud-route unit's store writes are in R2 and invisible here: book, never skip."""
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    for loc in ("any", None):
        _, note, _, _ = _derive(_U("bfs", [d], run_location=loc), _res(), _store(tmp_path, "bfs"))
        assert _books(note), loc


def test_a_newer_accounted_turn_moves_the_floor(tmp_path):
    d = _dir(tmp_path, "nyfed")
    st = _store(tmp_path, "nyfed", floor_ago=3 * HOUR)
    _write_parquet(os.path.join(d, "nyfed.parquet"), seconds_ago=2 * HOUR)
    _row(st, "nyfed", "partial", 1 * HOUR, obs=17_407)                # main path, after the write
    _, note, _, _ = _derive(_U("nyfed", [d]), _res(obs=17_407), st)
    assert note is None


def test_another_unit_s_floor_does_not_count(tmp_path):
    d = _dir(tmp_path, "dst")
    st = _store(tmp_path, "dst", floor_ago=3 * HOUR)
    _write_parquet(os.path.join(d, "AKU110.parquet"), seconds_ago=2 * HOUR)
    _row(st, "dst", "ok", 1 * HOUR, uid="_other")
    _, note, _, _ = _derive(_U("dst", [d]), _res(), st)
    assert _books(note)


def test_no_history_books(tmp_path):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), _store(tmp_path, "bfs", floor_ago=None))
    assert _books(note)


def test_a_naive_timestamp_books(tmp_path):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    st = _store(tmp_path, "bfs", floor_ago=None)
    st.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) "
                  "VALUES('2026-11-10T10:00:00','bfs','_all','ok',5,1.0,'n')")
    st.db.commit()
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st)
    assert _books(note)


def test_a_store_without_a_runs_table_books():
    class _Duck:
        pass
    assert orchestrate._accounted_until(_Duck(), _U("bfs", [])) is None
    assert orchestrate._accounted_until(None, _U("bfs", [])) is None


def test_a_write_in_a_nested_directory_is_seen(tmp_path):
    d = _dir(tmp_path, "bea")
    _write_parquet(os.path.join(d, "Regional", "CAINC5S.parquet"))
    _, note, _, _ = _derive(_U("bea", [d]), _res(), _store(tmp_path, "bea"))
    assert _books(note)


def test_a_file_grain_out_path_walks_its_directory(tmp_path):
    d = _dir(tmp_path, "eurostat")
    _write_parquet(os.path.join(d, "OTHER_FLOW.parquet"))
    _, note, _, _ = _derive(_U("eurostat", [os.path.join(d, "MY_FLOW.parquet")]), _res(),
                            _store(tmp_path, "eurostat"))
    assert _books(note), "a sibling written in the same directory is a write (conservative)"


def test_sidecars_and_temp_files_are_not_store_writes(tmp_path):
    d = _dir(tmp_path, "dst")
    _write_parquet(os.path.join(d, "AKU110.parquet"), seconds_ago=24 * HOUR)
    for name in ("_manifest.json", "_rotation.json", "AKU110.parquet.123.ab.tmp"):
        with open(os.path.join(d, name), "w") as fh:
            fh.write("{}")
    _, note, _, _ = _derive(_U("dst", [d]), _res(), _store(tmp_path, "dst"))
    assert note is None


def test_merged_rows_positive_always_books(tmp_path):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    _, note, _, _ = _derive(_U("bfs", [d]), _res(merged_rows=5), _store(tmp_path, "bfs"))
    assert _books(note)


@pytest.mark.parametrize("out_paths", [[], None])
def test_no_out_dirs_books(tmp_path, out_paths):
    _, note, _, _ = _derive(_U("bfs", out_paths), _res(), _store(tmp_path, "bfs"))
    assert _books(note)


def test_a_walk_error_books(tmp_path, monkeypatch):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    st = _store(tmp_path, "bfs")

    def bad_walk(top, onerror=None, **k):
        if onerror:
            onerror(OSError("denied"))
        return iter(())
    monkeypatch.setattr(orchestrate.os, "walk", bad_walk)
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st)
    assert _books(note)


def test_a_stat_error_books(tmp_path, monkeypatch):
    d = _dir(tmp_path, "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), seconds_ago=24 * HOUR)
    st = _store(tmp_path, "bfs")
    monkeypatch.setattr(orchestrate.os.path, "getmtime",
                        lambda p: (_ for _ in ()).throw(OSError("gone")))
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st)
    assert _books(note)


# ---- under r2: the R2 listing, not the runner's disk -------------------------------------------------------

class _FakeR2:
    def __init__(self, objects=None, boom=False):
        self.objects, self.boom, self.prefixes = objects or [], boom, []

    def list_modified(self, prefix):
        self.prefixes.append(prefix)
        if self.boom:
            raise RuntimeError("ServiceUnavailable")
        return [(k, m) for k, m in self.objects if k.startswith(prefix)]


def _r2(monkeypatch, fake):
    monkeypatch.setattr(orchestrate, "_store_blob_r2", lambda: fake)


def _utc_ago(seconds):
    return datetime.now(timezone.utc) - timedelta(seconds=seconds)


@pytest.mark.parametrize("objects,books", [
    ([("clean_full/bfs/BFS_a.parquet", 24 * HOUR)], False),
    ([("clean_full/bfs/BFS_a.parquet", 24 * HOUR), ("clean_full/bfs/BFS_b.parquet", 0.5 * HOUR)], True),
    ([("clean_full/bfs/Sub/BFS_c.parquet", 0.5 * HOUR)], True),            # nested keys are listed
    ([("clean_full/bfs/_rotation.json", 0.5 * HOUR)], False),               # sidecar, not a store parquet
    ([("clean_full/bfsx/BFS_a.parquet", 0.5 * HOUR)], False),               # another source's prefix
])
def test_under_r2_the_listing_decides(tmp_path, monkeypatch, objects, books):
    """The runner's disk holds only this run's files; the store the reader sees is R2."""
    fake = _FakeR2([(k, _utc_ago(s)) for k, s in objects])
    _r2(monkeypatch, fake)
    d = os.path.join(str(tmp_path), "data", "clean_full", "bfs")      # a store path with a /data/ segment
    _, note, _, _ = _derive(_U("bfs", [d], run_location="any"), _res(), _store(tmp_path, "bfs"))
    assert _books(note) is books, (objects, note)
    assert fake.prefixes == ["clean_full/bfs/"]


def test_under_r2_a_listing_error_books(tmp_path, monkeypatch):
    _r2(monkeypatch, _FakeR2(boom=True))
    d = os.path.join(str(tmp_path), "data", "clean_full", "bfs")
    _, note, _, _ = _derive(_U("bfs", [d], run_location="any"), _res(), _store(tmp_path, "bfs"))
    assert _books(note)


def test_under_r2_an_old_local_file_republished_shows_in_the_listing(tmp_path, monkeypatch):
    """publish_file re-PUTs old local parquet; its R2 LastModified is the PUT time, and that is what is read."""
    d = os.path.join(str(tmp_path), "data", "clean_full", "noaa")
    _write_parquet(os.path.join(d, "gsom.parquet"), seconds_ago=30 * 24 * HOUR)   # old local mtime
    _r2(monkeypatch, _FakeR2([("clean_full/noaa/gsom.parquet", _utc_ago(60))]))
    _, note, _, _ = _derive(_U("noaa", [d]), _res(), _store(tmp_path, "noaa"))
    assert _books(note)


# ---- through the REAL run_once -----------------------------------------------------------------------------

class _Strat(Strategy):
    name = "sim"

    def __init__(self, script):
        self.script = script

    def detect_change(self, unit, us):
        return "v-new"

    def run(self, unit, since=None):
        return self.script.pop(0)(unit)


def _run_once(tmp_path, monkeypatch, sid, script):
    d = _dir(tmp_path, sid)
    unit = Unit(source_id=sid, unit_id="_all", strategy="sim", cadence="daily", out_paths=[d],
                config={"run_location": "local", "live": True})
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    mp = monkeypatch
    mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: [unit])
    mp.setattr(fetchers, "get", lambda s: types.SimpleNamespace())
    mp.setattr(orchestrate, "_has_adapter", lambda u: True)
    mp.setattr(orchestrate, "_wrong_location", lambda u: False)
    mp.setattr(orchestrate, "_protected", lambda u: False)
    mp.setattr(orchestrate, "_is_live", lambda u: True)
    mp.setattr(orchestrate, "_served_by_lane", lambda s: False)
    mp.setattr(orchestrate, "_catalog_series_count", lambda s: 582, raising=False)
    mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
    mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    strat = _Strat(script)
    mp.setattr(orchestrate, "get_strategy", lambda name: strat)
    store = StateStore(str(tmp_path / "state.db"))
    orchestrate.run_once(store=store, force=True)
    return store


def _quiet(unit):
    return _res(obs=11_159_165, error="2/25 sub-unit(s) transient-failed; will retry")


def _writes(unit):
    _write_parquet(os.path.join(unit.out_paths[0], "dst_b.parquet"))
    return _quiet(unit)


def _owed(store, sid):
    return [r for r in store.full_rederives_owed() if r["source_id"] == sid]


def _prime(tmp_path, sid):
    """An accounted turn 2 h ago, and a store last written a day ago."""
    _write_parquet(os.path.join(_dir(tmp_path, sid), f"{sid}_a.parquet"), seconds_ago=24 * HOUR)
    st = StateStore(str(tmp_path / "state.db"))
    _row(st, sid, "ok", 2 * HOUR, obs=11_159_165, note="+3790 new rows")


def test_run_once_zero_write_pass_books_nothing_and_a_writing_pass_books(tmp_path, monkeypatch):
    _prime(tmp_path, "dst")
    store = _run_once(tmp_path, monkeypatch, "dst", [_quiet])
    assert _owed(store, "dst") == []
    store = _run_once(tmp_path, monkeypatch, "dst", [_writes])
    assert len(_owed(store, "dst")) == 1, "negative control: a pass that wrote parquet books"


@pytest.mark.parametrize("exc", [TransientError("dst: 2 tables HTTP 500"),
                                 DefinitiveError("dst: structural"),
                                 RuntimeError("unexpected")])
def test_run_once_a_write_then_failure_makes_the_next_quiet_pass_book(tmp_path, monkeypatch, exc):
    _prime(tmp_path, "dst")

    def write_then_raise(unit):
        _writes(unit)
        raise exc
    store = _run_once(tmp_path, monkeypatch, "dst", [write_then_raise])
    assert _owed(store, "dst") == [], "the failed pass itself books nothing (unchanged)"
    store = _run_once(tmp_path, monkeypatch, "dst", [_quiet])
    assert len(_owed(store, "dst")) == 1, "the write before the failure is booked by the next quiet pass"


def test_run_once_a_quiet_failure_does_not_poison_the_next_quiet_pass(tmp_path, monkeypatch):
    _prime(tmp_path, "nyfed")

    def raise_only(unit):
        raise TransientError("nyfed: 8/8 transient")
    _run_once(tmp_path, monkeypatch, "nyfed", [raise_only])
    store = _run_once(tmp_path, monkeypatch, "nyfed", [_quiet])
    assert _owed(store, "nyfed") == []


def test_run_once_the_failure_path_logs_obs_zero():
    """The floor rule rests on it: _record (every failure branch) logs runs.obs = 0."""
    import inspect
    src = inspect.getsource(orchestrate._record)
    assert "store.log_run(unit.source_id, unit.unit_id, status, dur_s=round(dur, 1), note=err)" in src
    import updater.state as state_mod
    assert inspect.signature(state_mod.StateStore.log_run).parameters["obs"].default == 0
