"""A pass that wrote NO store parquet must not book a full_rederive_owed debt (the false-debt fix).

THE DEFECT (gate re-triage 2026-11-11). The no-cursors branch of _derive_changed_csvs booked a debt
whenever a fetcher reported neither series_cursors nor merged_rows and `res.obs` (the STORE TOTAL) was
non-zero. Passes that merged nothing booked debts nothing pays: bfs 2026-10-20 (a 9.3 s pass stopped
by a catalogue HTTP 429), nyfed 2026-10-21 (8/8 sub-units transient), dst 2026-10-18. A served CSV is
derived from store parquet only, so a pass that wrote none cannot have made one stale.

THE PREDICATE (orchestrate._no_store_write_proven), each leg pinned below with its negative control:
  * no `*.parquet` under the unit's out dirs (recursive) has mtime >= the unit's start;
  * the walk can see the fetcher's writes (not a publish_file fetcher under r2);
  * the unit's previous turn left no unaccounted write (a FAILED pass stamps what it saw, and a
    pass that wrote parquet and then failed never reached the derive phase).
Every doubt books the debt, as before. merged_rows > 0 always books; merged_rows == 0 never did.

Existing owed rows are not touched by this change.
"""
from __future__ import annotations

import os
import sys
import time
import types
import unittest.mock as mock

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


def _write_parquet(path, mtime=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": ["2026-01-01"], "value": [1.0]}), path)
    if mtime is not None:
        os.utime(path, (mtime, mtime))


class _U:
    def __init__(self, sid, out_paths, uid="_all"):
        self.source_id, self.unit_id, self.out_paths = sid, uid, out_paths
        self.key = f"{sid}/{uid}"


def _res(**kw):
    base = dict(status="partial", obs=5_408_880, series_cursors=None, new_vintage="date-tail")
    base.update(kw)
    return Result(**base)


def _derive(unit, res, store, started_at):
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=582, create=True):
        return orchestrate._derive_changed_csvs(unit, res, blob=None, store=store, started_at=started_at)


def _store_with_prev(tmp_path, sid, status="ok", note="+17407 new rows"):
    st = StateStore(path=str(tmp_path / "state.db"))
    if status is not None:
        st.log_run(sid, "_all", status, note=note)
    return st


@pytest.fixture(autouse=True)
def _local_backend(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")


# ---- the branch itself ------------------------------------------------------------------------------------

def test_a_zero_write_pass_books_no_debt(tmp_path):
    d = str(tmp_path / "store" / "bfs")
    old = time.time() - 86_400
    _write_parquet(os.path.join(d, "BFS_a.parquet"), mtime=old)      # yesterday's store, untouched today
    st = _store_with_prev(tmp_path, "bfs")
    failed, note, deferred, reasons = _derive(_U("bfs", [d]), _res(), st, started_at=time.time() - 9.3)
    assert note is None and failed == [] and deferred == [] and reasons == {}


def test_negative_control_a_pass_that_wrote_parquet_without_cursors_still_books(tmp_path):
    d = str(tmp_path / "store" / "bfs")
    t0 = time.time() - 60
    _write_parquet(os.path.join(d, "BFS_a.parquet"))                 # written during this pass
    st = _store_with_prev(tmp_path, "bfs")
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st, started_at=t0)
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_a_write_in_a_nested_directory_is_seen(tmp_path):
    d = str(tmp_path / "store" / "bea")
    t0 = time.time() - 60
    _write_parquet(os.path.join(d, "Regional", "CAINC5S.parquet"))
    st = _store_with_prev(tmp_path, "bea")
    _, note, _, _ = _derive(_U("bea", [d]), _res(), st, started_at=t0)
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_a_file_grain_out_path_walks_its_directory(tmp_path):
    d = str(tmp_path / "store" / "eurostat")
    t0 = time.time() - 60
    _write_parquet(os.path.join(d, "OTHER_FLOW.parquet"))
    st = _store_with_prev(tmp_path, "eurostat")
    _, note, _, _ = _derive(_U("eurostat", [os.path.join(d, "MY_FLOW.parquet")]), _res(), st, started_at=t0)
    assert note is not None, "a sibling written in the same directory is a write (conservative)"


def test_sidecars_and_temp_files_are_not_store_writes(tmp_path):
    d = str(tmp_path / "store" / "dst")
    _write_parquet(os.path.join(d, "AKU110.parquet"), mtime=time.time() - 86_400)
    for name in ("_manifest.json", "_rotation.json", "AKU110.parquet.123.ab.tmp"):
        with open(os.path.join(d, name), "w") as fh:
            fh.write("{}")
    st = _store_with_prev(tmp_path, "dst")
    _, note, _, _ = _derive(_U("dst", [d]), _res(), st, started_at=time.time() - 30)
    assert note is None


def test_merged_rows_positive_always_books(tmp_path):
    d = str(tmp_path / "store" / "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "bfs")
    _, note, _, _ = _derive(_U("bfs", [d]), _res(merged_rows=5), st, started_at=time.time() - 30)
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_no_start_time_keeps_the_old_behaviour(tmp_path):
    d = str(tmp_path / "store" / "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "bfs")
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st, started_at=None)
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


@pytest.mark.parametrize("out_paths", [[], None])
def test_no_out_dirs_books(tmp_path, out_paths):
    st = _store_with_prev(tmp_path, "bfs")
    _, note, _, _ = _derive(_U("bfs", out_paths), _res(), st, started_at=time.time() - 30)
    assert note is not None


def test_a_walk_error_books(tmp_path, monkeypatch):
    d = str(tmp_path / "store" / "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "bfs")

    def bad_walk(top, onerror=None, **k):
        if onerror:
            onerror(OSError("denied"))
        return iter(())
    monkeypatch.setattr(orchestrate.os, "walk", bad_walk)
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st, started_at=time.time() - 30)
    assert note is not None


def test_a_stat_error_books(tmp_path, monkeypatch):
    d = str(tmp_path / "store" / "bfs")
    _write_parquet(os.path.join(d, "BFS_a.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "bfs")
    monkeypatch.setattr(orchestrate.os.path, "getmtime",
                        lambda p: (_ for _ in ()).throw(OSError("gone")))
    _, note, _, _ = _derive(_U("bfs", [d]), _res(), st, started_at=time.time() - 30)
    assert note is not None


def test_under_r2_a_publish_file_fetcher_always_books(tmp_path, monkeypatch):
    """publish_file re-PUTs every local parquet in the directory, old mtimes included."""
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    for sid, books in (("noaa", True), ("bfs", False)):
        d = str(tmp_path / "store" / sid)
        _write_parquet(os.path.join(d, "x.parquet"), mtime=time.time() - 86_400)
        st = StateStore(path=str(tmp_path / f"state_{sid}.db"))
        st.log_run(sid, "_all", "ok", note="+1 new rows")
        _, note, _, _ = _derive(_U(sid, [d]), _res(), st, started_at=time.time() - 30)
        assert (note is not None) is books, (sid, note)


# ---- the previous turn ------------------------------------------------------------------------------------

@pytest.mark.parametrize("status,note,books", [
    ("transient_fail", "boom; " + orchestrate._STORE_WRITE_YES, True),
    ("partial", "structural; " + orchestrate._STORE_WRITE_YES, True),
    ("transient_fail", "boom; " + orchestrate._STORE_WRITE_UNKNOWN, True),
    ("transient_fail", "UnitTimeout('bfs/_all exceeded 45 min')", True),     # unstamped (pre-fix row)
    ("killed_external", "killed by the window guard", True),
    (None, None, True),                                                       # no history
    ("transient_fail", "boom; " + orchestrate._STORE_WRITE_NO, False),
    ("ok", "+17407 new rows", False),
    ("partial", "8/8 sub-unit(s) transient-failed; will retry", False),
])
def test_the_previous_turn_decides_whether_an_unaccounted_write_may_exist(tmp_path, status, note, books):
    d = str(tmp_path / "store" / "nyfed")
    _write_parquet(os.path.join(d, "nyfed.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "nyfed", status=status, note=note)
    _, got, _, _ = _derive(_U("nyfed", [d]), _res(obs=17_407), st, started_at=time.time() - 30)
    assert (got is not None) is books, (status, note, got)


def test_non_turn_rows_are_skipped_when_finding_the_previous_turn(tmp_path):
    d = str(tmp_path / "store" / "nyfed")
    _write_parquet(os.path.join(d, "nyfed.parquet"), mtime=time.time() - 86_400)
    st = _store_with_prev(tmp_path, "nyfed", status="transient_fail",
                          note="boom; " + orchestrate._STORE_WRITE_YES)
    for s in orchestrate._NOT_A_TURN:
        st.log_run("nyfed", "_all", s, note="later, but not a turn")
    _, got, _, _ = _derive(_U("nyfed", [d]), _res(obs=17_407), st, started_at=time.time() - 30)
    assert got is not None


def test_a_store_without_a_runs_table_books():
    class _Duck:
        pass
    assert orchestrate._previous_turn_clean(_Duck(), _U("bfs", [])) is False
    assert orchestrate._previous_turn_clean(None, _U("bfs", [])) is False


def test_the_marker_is_appended_after_clipping(tmp_path):
    d = str(tmp_path / "store" / "bfs")
    long = orchestrate._clip_err("x, " * 2000)
    out = orchestrate._with_store_marker(long, _U("bfs", [d]), time.time() - 30)
    assert out.endswith(orchestrate._STORE_WRITE_NO), out[-120:]


# ---- through the REAL run_once: a pass that wrote and failed, then a pass that wrote nothing ---------------

class _Strat(Strategy):
    name = "sim"

    def __init__(self, script):
        self.script = script

    def detect_change(self, unit, us):
        return "v-new"

    def run(self, unit, since=None):
        return self.script.pop(0)(unit)


def _run_once(tmp_path, monkeypatch, sid, script):
    d = str(tmp_path / "store" / sid)
    st_path = str(tmp_path / "state.db")
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
    store = StateStore(st_path)
    orchestrate.run_once(store=store, force=True)
    return store, d


def _writes(sid):
    def w(unit):
        _write_parquet(os.path.join(unit.out_paths[0], f"{sid}_a.parquet"))
        return _res(obs=11_159_165, error="2/25 sub-unit(s) transient-failed; will retry")
    return w


def _quiet(unit):
    return _res(obs=11_159_165, error="2/25 sub-unit(s) transient-failed; will retry")


def _owed(store, sid):
    return [r for r in store.full_rederives_owed() if r["source_id"] == sid]


def _prime_old_store(tmp_path, sid):
    _write_parquet(os.path.join(str(tmp_path / "store" / sid), f"{sid}_a.parquet"),
                   mtime=time.time() - 86_400)


def test_run_once_zero_write_pass_books_nothing_and_a_writing_pass_books(tmp_path, monkeypatch):
    _prime_old_store(tmp_path, "dst")
    StateStore(str(tmp_path / "state.db")).log_run("dst", "_all", "ok", note="+3790 new rows")
    store, _ = _run_once(tmp_path, monkeypatch, "dst", [_quiet])
    assert _owed(store, "dst") == []
    store, _ = _run_once(tmp_path, monkeypatch, "dst", [_writes("dst")])
    assert len(_owed(store, "dst")) == 1, "negative control: a pass that wrote parquet books"


@pytest.mark.parametrize("exc", [TransientError("dst: 2 tables HTTP 500"),
                                 DefinitiveError("dst: structural"),
                                 RuntimeError("unexpected")])
def test_run_once_a_write_then_failure_makes_the_next_quiet_pass_book(tmp_path, monkeypatch, exc):
    _prime_old_store(tmp_path, "dst")
    StateStore(str(tmp_path / "state.db")).log_run("dst", "_all", "ok", note="+3790 new rows")

    def write_then_raise(unit):
        _write_parquet(os.path.join(unit.out_paths[0], "dst_b.parquet"))
        raise exc
    store, _ = _run_once(tmp_path, monkeypatch, "dst", [write_then_raise])
    last = store.db.execute("SELECT note FROM runs ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert last.endswith(orchestrate._STORE_WRITE_YES), last
    assert _owed(store, "dst") == [], "the failed pass itself books nothing (unchanged)"
    # the next pass writes nothing: its own walk is empty, but the previous turn wrote and failed
    for f in os.listdir(str(tmp_path / "store" / "dst")):
        os.utime(os.path.join(str(tmp_path / "store" / "dst"), f), (time.time() - 3600,) * 2)
    store, _ = _run_once(tmp_path, monkeypatch, "dst", [_quiet])
    assert len(_owed(store, "dst")) == 1, "the write before the failure is booked by the next quiet pass"


def test_run_once_a_quiet_failure_does_not_poison_the_next_quiet_pass(tmp_path, monkeypatch):
    _prime_old_store(tmp_path, "nyfed")
    StateStore(str(tmp_path / "state.db")).log_run("nyfed", "_all", "ok", note="+17407 new rows")

    def raise_only(unit):
        raise TransientError("nyfed: 8/8 transient")
    store, _ = _run_once(tmp_path, monkeypatch, "nyfed", [raise_only])
    last = store.db.execute("SELECT note FROM runs ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert last.endswith(orchestrate._STORE_WRITE_NO), last
    store, _ = _run_once(tmp_path, monkeypatch, "nyfed", [_quiet])
    assert _owed(store, "nyfed") == []


# ---- the evidence the walk rests on ----------------------------------------------------------------------

def test_no_fetcher_writes_the_store_except_through_the_local_path_primitives():
    """The mtime walk sees only writes that land on the local store path first. A fetcher that PUT to R2
    itself, or handed merge_and_write an explicit blob=, would be invisible to it - and its debt dropped.
    publish_file callers are handled separately (they always book under r2)."""
    import re
    root = os.path.join(ROOT, "updater", "strategies")
    # JSON sidecars, not store parquet, each read at its own line (2026-11-11): insee_melodi's
    # compare-and-swap of _aqueduct/withheld.json and _melodi_state's _flows.json. A NEW direct PUT
    # fails this test until someone reads it and either routes it through blob's path primitives or
    # adds it here with what it writes. Count-pinned so a fourth site in these files is not hidden.
    allowed = {("insee_melodi.py", r"\.put_if\("): 1, ("_melodi_state.py", r"\.put_if\("): 2}
    seen: dict = {}
    bad = []
    for dirpath, _d, files in os.walk(root):
        for f in files:
            if not f.endswith(".py"):
                continue
            src = open(os.path.join(dirpath, f), encoding="utf-8").read()
            for pat in (r"\.put_atomic\(", r"\.put_file\(", r"\.put_gzip_file\(", r"\.put_if\(",
                        r"merge_and_write(?:_bounded)?\([^)]*\bblob\s*="):
                for m in re.finditer(pat, src):
                    seen[(f, pat)] = seen.get((f, pat), 0) + 1
                    if seen[(f, pat)] > allowed.get((f, pat), 0):
                        bad.append(f"{f}:{src[:m.start()].count(chr(10)) + 1}: {pat}")
    assert not bad, bad


def test_the_publish_file_detector_finds_the_known_callers():
    for sid in ("noaa", "usda", "cepii_baci", "eia", "fhfa", "fed_board", "ilostat"):
        assert orchestrate._publishes_whole_dir(sid) is True, sid
    for sid in ("bfs", "nyfed", "dst", "ecb"):
        assert orchestrate._publishes_whole_dir(sid) is False, sid
    assert orchestrate._publishes_whole_dir("no_such_source_xyz") is True, "unreadable must book"
