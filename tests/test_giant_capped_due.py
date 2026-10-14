"""A capped giant pass that left fresh changes beyond its cap is due at the next run.

WHAT WAS WRONG. run_giant's capped branch (more flows selected than max_flows) returned `partial`,
and its text said "remainder re-runs next tick". base.Strategy.is_due makes a `partial` unit with no
recorded success wait min(cadence, PARTIAL_RETRY_DAYS) days: eurostat's capped pass of 2026-10-06
was followed by the next pass on 2026-10-13.

THE RULE (report_changed_flows callers only). A capped pass returns `transient_fail` with its changed
set - no success, no vintage, due at the next run, CSVs derived in the pass - when BOTH hold:
  (1) flows upstream changed since we last finished them remain beyond the cap (never attempted, or
      a moved vintage after a last attempt that finished), and
  (2) the pass itself merged at least one flow that changed served data.
Otherwise it returns `partial` as before. Flows that failed last time are retries, never changes, so
a flow that fails every time cannot keep a source on the every-run schedule; and a pass that saw no
real change (vintages that move without new data, or a head of failing flows) keeps the partial clock.

HOW. The tests drive the REAL orchestrate.run_once for the registry entry `eurostat`, with a strategy
that calls the REAL _giant.run_giant (cap 2 over four flows) and the real mapping and derive_and_put.
The catalogue, the store, the per-flow state and state.db are in the test's folder; the CSV upload goes
to a list. Nothing goes to the network.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import config, derive, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import get as get_strategy  # noqa: E402
from updater.strategies.fetchers import _giant, oecd  # noqa: E402

FLOWS = ["f1", "f2", "f3", "f4"]
UNIT_KEY = "eurostat/_all"


def _cat(vintage="v1"):
    return {f: {"vintage": vintage, "filename": f.upper() + ".parquet"} for f in FLOWS}


def _tbl(fid, value=1.0):
    return pa.table({"series_key": pa.array([f"geo={fid}"] * 2, pa.string()),
                     "obs_date": pa.array([dt.date(2024, 12, 31), dt.date(2025, 12, 31)], pa.date32()),
                     "value": pa.array([value, value], pa.float64())})


class _Blob:
    def __init__(self):
        self.keys = []

    def put_atomic(self, key, data):
        self.keys.append(key)


class _Res:
    def __init__(self, path):
        self.parquet_path = path


class _Strategy:
    def __init__(self, catalog, fetch, opt_in=True):
        self.catalog, self.fetch, self.opt_in = catalog, fetch, opt_in
        self.fetched = []

    def is_due(self, unit, us):
        return True

    def detect_change(self, unit, us):
        return "cat:test"

    def run(self, unit, since=None):
        def fetch_flow(fid, meta, since, session):
            self.fetched.append(fid)
            return self.fetch(fid)

        return _giant.run_giant(unit, source=unit.source_id, fetch_catalog=lambda: dict(self.catalog),
                                fetch_flow=fetch_flow, csv_accept="text/csv", rate=0, timeout=1,
                                max_flows=2, report_changed_flows=self.opt_in, bounded_merge=True,
                                checkpoint_every=10)


class _Rig:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp, self.mp = tmp_path, monkeypatch
        self.store_dir = tmp_path / "data" / "clean_full" / "eurostat"
        os.makedirs(self.store_dir)
        self.blob = _Blob()
        self.state = StateStore(str(tmp_path / "data" / "_aqueduct" / "state.db"))
        self.strategy = None

    def seed(self, sidecar=None, stored=()):
        if sidecar is not None:
            (self.store_dir / "_giant_state.json").write_text(json.dumps(sidecar), encoding="utf-8")
        for f in stored:
            pq.write_table(_tbl(f), self.store_dir / (f.upper() + ".parquet"))

    def run(self, catalog, fetch, opt_in=True):
        self.blob.keys.clear()
        self.strategy = _Strategy(catalog, fetch, opt_in)
        self.mp.setattr(orchestrate, "get_strategy", lambda name: self.strategy)
        return orchestrate.run_once(sources=["eurostat"], store=self.state, blob=self.blob)

    def unit(self):
        return self.state.get_unit("eurostat", "_all")

    def put_ids(self):
        want = {derive.r2_key(f"eurostat:{f}"): f for f in FLOWS}
        assert all(k in want for k in self.blob.keys), self.blob.keys
        return sorted(want[k] for k in self.blob.keys)

    def due_an_hour_later(self, status=None):
        row = self.unit()
        if status:
            row = dict(row, status=status)
        unit = next(u for u in registry.all_units() if u.source_id == "eurostat")
        later = dt.datetime.fromisoformat(row["last_attempt_utc"]) + dt.timedelta(hours=1)
        return get_strategy(unit.strategy).is_due(unit, row, now=later)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    import core.derive_csv as dc
    from econdl import _resolve

    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)   # run_once sets it
    data = tmp_path / "data"                     # every default path of the code under test, moved here
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    os.makedirs(data / "_aqueduct")
    r = _Rig(tmp_path, monkeypatch)
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"eurostat:{f}", "eurostat") for f in FLOWS])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: _Res(
        str(r.store_dir / (sid.split(":", 1)[1].upper() + ".parquet"))))
    monkeypatch.setattr(dc, "resolved_paths", lambda res: [res.parquet_path])

    def stream(sid, out_path):
        pq.read_table(_resolve.resolve(sid).parquet_path)            # absent file -> the id fails
        with open(out_path, "wb") as fh:
            fh.write(gzip.compress(b"series_id,obs_date,value\n", mtime=0))
        return os.path.getsize(out_path)

    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", stream)
    monkeypatch.setattr(derive, "_series_csv_bytes",
                        lambda sid: (_ for _ in ()).throw(AssertionError("in-memory path used")))
    yield r
    r.state.close()


def _ok(fid):
    return _tbl(fid), "ok"


# ---- the rule ---------------------------------------------------------------------------------

def test_fresh_changes_beyond_the_cap_make_the_pass_due_at_the_next_run_and_the_count_shrinks(rig, capsys):
    results = rig.run(_cat(), _ok)                       # four new flows, cap 2
    out = capsys.readouterr().out
    assert rig.strategy.fetched == ["f1", "f2"]
    assert results == [(UNIT_KEY, "transient_fail")]
    assert rig.put_ids() == ["f1", "f2"], "the CSV step ran on the pass's changed set"
    row = rig.unit()
    assert row["status"] == "transient_fail" and row["last_success_utc"] is None
    assert row["upstream_vintage"] is None
    assert row["last_error"].startswith("selected>cap: fetched 2 flows; 2 changed or new and 0 retry "
                                        "flow(s) wait beyond the cap; the unit is due again at the next run")
    assert "re-runs next tick" not in row["last_error"] and "fetched at the next run" not in row["last_error"]
    assert "(CAPPED - the rest is selected again by a later pass)" in out and "remainder next tick" not in out
    assert ("[eurostat] capped at 2 flows: beyond the cap 2 changed or new flow(s) and 0 retry flow(s); "
            "this pass merged 2 changed flow(s) -> due at the next run") in out
    assert rig.due_an_hour_later() is True
    assert rig.due_an_hour_later(status="partial") is False, "the control: the partial clock"
    # the next pass takes the two left; nothing is left beyond the cap, so it is not capped at all
    results = rig.run(_cat(), _ok)
    assert rig.strategy.fetched == ["f3", "f4"]
    assert results == [(UNIT_KEY, "ok")] and rig.put_ids() == ["f3", "f4"]
    assert "capped at" not in capsys.readouterr().out


def test_only_retries_beyond_the_cap_keep_the_partial_clock(rig, capsys):
    # f3 and f4 have only ever failed: no failure path records a vintage, so they carry none (the
    # real eurostat state holds 66 such rows); f1 and f2 are new and come first in catalogue order
    rig.seed({"f3": {"status": "transient_fail"},
              "f4": {"status": "definitive_fail", "error": "HTTP 404 on the data endpoint"}})
    results = rig.run(_cat(), _ok)
    out = capsys.readouterr().out
    assert rig.strategy.fetched == ["f1", "f2"]
    assert results == [(UNIT_KEY, "partial")]
    assert rig.put_ids() == ["f1", "f2"], "a partial derives its changed set, as before"
    row = rig.unit()
    assert row["status"] == "partial" and row["last_success_utc"] is None
    assert row["last_error"].startswith("selected>cap: fetched 2 flows; 0 changed or new and 2 retry "
                                        "flow(s) wait beyond the cap (+")
    assert "beyond the cap 0 changed or new flow(s) and 2 retry flow(s)" in out and "-> partial" in out
    assert rig.due_an_hour_later() is False


# ---- the counting artefacts: none of them may keep the source on the every-run schedule ---------

def test_flows_that_failed_last_time_count_as_retries_even_when_their_vintage_moved(rig):
    """A flow that fails every time keeps its old vintage, so it reads 'changed' for ever."""
    rig.seed({"f3": {"vintage": "v0", "status": "transient_fail"},
              "f4": {"vintage": "v0", "status": "partial"}})
    results = rig.run(_cat("v1"), _ok)
    assert rig.strategy.fetched == ["f1", "f2"]
    assert results == [(UNIT_KEY, "partial")]
    assert "0 changed or new and 2 retry flow(s)" in rig.unit()["last_error"]
    assert rig.due_an_hour_later() is False


def test_a_head_of_failing_flows_cannot_hold_the_every_run_schedule(rig):
    """f1 and f2 fail every pass and take both slots; f3 and f4 wait. Nothing merged, so the pass does
    not ask for the next run, however many fresh flows wait behind it."""
    def fetch(fid):
        return (None, "transient") if fid in ("f1", "f2") else _ok(fid)

    for _ in range(2):
        results = rig.run(_cat(), fetch)
        assert rig.strategy.fetched == ["f1", "f2"]
        assert results == [(UNIT_KEY, "partial")]
        assert rig.due_an_hour_later() is False


def test_vintages_that_move_without_new_data_keep_the_partial_clock(rig):
    """Every flow is stored and finished at v1; the catalogue says v2 but the rows are the same. The
    no-progress guard (the pass must merge a change) also holds this all-idempotent pass back: the
    documented behaviour, kept from before this change."""
    rig.seed({f: {"vintage": "v1", "status": "ok"} for f in FLOWS}, stored=FLOWS)
    results = rig.run(_cat("v2"), _ok)
    assert rig.strategy.fetched == ["f1", "f2"]
    assert results == [(UNIT_KEY, "partial")], "the two merges changed nothing"
    assert "2 changed or new and 0 retry flow(s)" in rig.unit()["last_error"]
    assert rig.blob.keys == []
    assert rig.due_an_hour_later() is False


def test_a_vintage_that_moved_after_a_finished_attempt_is_a_fresh_change(rig):
    rig.seed({f: {"vintage": "v1", "status": s} for f, s in
              (("f1", "ok"), ("f2", "ok"), ("f3", "empty"), ("f4", "no_time_dimension"))}, stored=FLOWS)
    results = rig.run(_cat("v2"), lambda fid: (_tbl(fid, value=2.0), "ok"))    # new values: real change
    assert results == [(UNIT_KEY, "transient_fail")]
    assert "2 changed or new and 0 retry flow(s) wait beyond the cap; the unit is due again at the next run" \
        in rig.unit()["last_error"]
    assert rig.due_an_hour_later() is True


def test_the_count_itself():
    cat = {f: {"vintage": "v2", "filename": f} for f in "abcdefgh"}
    state = {"a": {"vintage": "v2", "status": "ok"},                  # finished, unchanged: neither
             "b": {"vintage": "v1", "status": "ok"},                  # moved after a finish: fresh
             "c": {"vintage": "v1", "status": "empty"},               # moved after a finish: fresh
             "d": {"vintage": "v1", "status": "transient_fail"},      # moved after a failure: retry
             "e": {"vintage": "v1", "status": "definitive_fail"},     # retry
             "f": {"vintage": "v2", "status": "partial"},             # redo: retry
             "g": {"vintage": "v2", "status": "no_time_dimension"}}   # settled: neither
    assert _giant._beyond_the_cap(cat, state, []) == (3, 3)           # b, c, h | d, e, f
    assert _giant._beyond_the_cap(cat, state, ["b", "h", "d"]) == (1, 2)


# ---- callers without the opt-in ----------------------------------------------------------------

def test_without_the_opt_in_a_capped_pass_stays_partial(rig, capsys):
    results = rig.run(_cat(), _ok, opt_in=False)
    out = capsys.readouterr().out
    assert results == [(UNIT_KEY, "partial")]
    row = rig.unit()
    assert row["status"] == "partial" and row["last_success_utc"] is None
    assert "2 changed or new and 0 retry flow(s) wait beyond the cap (+" in row["last_error"]
    assert "due again" not in row["last_error"]
    assert "-> partial" in out
    assert rig.due_an_hour_later() is False


def test_the_oecd_path_stays_partial_when_capped(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(oecd, "RATE", 0)
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: _cat())
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, session: _ok(fid))
    monkeypatch.setitem(_giant.run_giant.__kwdefaults__, "max_flows", 2)   # oecd uses the default cap

    class _U:
        out_paths = [str(tmp_path)]

    res = oecd.update(_U(), None)
    assert res.status == "partial" and res.changed_keys is None
    assert res.error.startswith("selected>cap: fetched 2 flows; 2 changed or new and 0 retry flow(s)")
