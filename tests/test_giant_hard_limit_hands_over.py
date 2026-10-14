"""run_giant hands its changed flows to the CSV step when the sweep cannot finish.

WHAT WAS WRONG. eurostat runs through _giant.run_giant with report_changed_flows=True. When the
orchestrator's hard per-unit limit fired in the flow loop, the handlers saved a checkpoint and
raised the limit again. The orchestrator recorded the unit as transient_fail and ran no CSV step.
The flows that pass had merged with a change kept their new vintage in the saved checkpoint, so
the next pass did not select them; a re-fetch merges equal rows and reports no change. Their
CSVs were not derived until the publisher changed the flow again. A failed final save lost the
changed set the same way.

WHAT THESE TESTS HOLD.
  (a) the limit fires in flow 3 of 5 after flows 1-2 merged a change: the CSVs of flows 1-2 are
      PUT in the same run, and the unit is recorded `partial`, with no success time;
  (b) the same when every save of the state file fails; and a failed final save alone;
  (c) a caller without the opt-in gets the limit and the save error raised, as before (the oecd
      path too), and select_flows answers as before;
  (d) a limit that fires before any flow merged a change is raised as before: the unit is
      recorded transient_fail and no CSV step runs.

HOW. (a), (b) and (d) drive the REAL orchestrate.run_once for the real registry entry `eurostat`,
with the strategy replaced by one that calls the REAL _giant.run_giant with eurostat's options.
The limit is raised by the orchestrator's own alarm body (_deliver_alarm), which sets its flag and
raises UnitTimeout. The mapping to catalogue ids and derive_and_put are the real functions; the
catalogue is a small file of this test, the store is a folder of this test, and the CSV upload
goes to a list. Nothing goes to the network, and no path outside tmp_path is written.
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

from updater import config, derive, merge, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import _giant, oecd  # noqa: E402

FLOWS = ["f1", "f2", "f3", "f4", "f5"]
CAT = {f: {"vintage": "v1", "filename": f.upper() + ".parquet"} for f in FLOWS}
UNIT_KEY = "eurostat/_all"


def _tbl(rows):
    return pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                     "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                     "value": pa.array([float(r[2]) for r in rows], pa.float64())})


def _rows(fid, value=1.0):
    return [(f"geo={fid}", dt.date(2024, 12, 31), value), (f"geo={fid}", dt.date(2025, 12, 31), value)]


def _alarm():
    """The orchestrator's alarm, as its SIGALRM handler delivers it: flag first, then UnitTimeout."""
    orchestrate._deliver_alarm(UNIT_KEY, 45.0)


class _Blob:
    def __init__(self):
        self.keys = []

    def put_atomic(self, key, data):
        self.keys.append(key)


class _Res:
    def __init__(self, path):
        self.parquet_path = path


class _Strategy:
    """is_due: always. detect_change: a token. run: the REAL run_giant with eurostat's options."""

    def __init__(self, fetch_flow, opt_in=True):
        self.fetch_flow, self.opt_in = fetch_flow, opt_in
        self.fetched = []

    def is_due(self, unit, us):
        return True

    def detect_change(self, unit, us):
        return "cat:test"

    def run(self, unit, since=None):
        def fetch_flow(fid, meta, since, session):
            self.fetched.append(fid)
            return self.fetch_flow(fid)

        return _giant.run_giant(unit, source=unit.source_id, fetch_catalog=lambda: dict(CAT),
                                fetch_flow=fetch_flow, csv_accept="text/csv", rate=0, timeout=1,
                                report_changed_flows=self.opt_in, bounded_merge=True,
                                checkpoint_every=10)


class _Rig:
    def __init__(self, tmp_path, monkeypatch):
        self.tmp, self.mp = tmp_path, monkeypatch
        self.store_dir = tmp_path / "data" / "clean_full" / "eurostat"
        self.blob = _Blob()
        self.state = StateStore(str(tmp_path / "data" / "_aqueduct" / "state.db"))
        self.strategy = None

    def run(self, fetch_flow, opt_in=True):
        self.strategy = _Strategy(fetch_flow, opt_in)
        self.mp.setattr(orchestrate, "get_strategy", lambda name: self.strategy)
        return orchestrate.run_once(sources=["eurostat"], store=self.state, blob=self.blob)

    def sidecar(self):
        p = self.store_dir / "_giant_state.json"
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None

    def unit(self):
        return self.state.get_unit("eurostat", "_all")

    def runs(self):
        return self.state.db.execute("SELECT status, note FROM runs WHERE source_id='eurostat'").fetchall()

    def put_ids(self):
        want = {derive.r2_key(f"eurostat:{f}"): f for f in FLOWS}
        assert all(k in want for k in self.blob.keys), self.blob.keys
        return sorted(want[k] for k in self.blob.keys)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    import core.derive_csv as dc
    from econdl import _resolve

    for k in ("AQUEDUCT_BACKEND", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)   # run_once sets it
    # raising=False: the same file runs against the code before this change, to show it fails there
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_ANSWERED", False, raising=False)
    # every default path of the code under test is moved into this test's folder
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    os.makedirs(data / "_aqueduct")
    r = _Rig(tmp_path, monkeypatch)
    # the catalogue the mapping reads: one id per flow, as eurostat is catalogued
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"eurostat:{f}", "eurostat") for f in FLOWS])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    # the derive resolves an id to the flow's store file ON THIS MACHINE; an absent file fails the id
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: _Res(
        str(r.store_dir / (sid.split(":", 1)[1].upper() + ".parquet"))))
    monkeypatch.setattr(dc, "resolved_paths", lambda res: [res.parquet_path])

    def stream(sid, out_path):
        t = pq.read_table(_resolve.resolve(sid).parquet_path)
        body = "series_id,obs_date,value\n" + "".join(
            f"{k},{d},{v}\n" for k, d, v in zip(*(t.column(c).to_pylist() for c in t.column_names)))
        with open(out_path, "wb") as fh:
            fh.write(gzip.compress(body.encode("utf-8"), mtime=0))
        return os.path.getsize(out_path)

    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", stream)
    monkeypatch.setattr(derive, "_series_csv_bytes",
                        lambda sid: (_ for _ in ()).throw(AssertionError("in-memory path used")))
    yield r
    r.state.close()


def _limit_in_flow_3(fid):
    if fid == "f3":
        _alarm()
    return _tbl(_rows(fid)), "ok"


# ---- (a) the limit fires after two flows merged a change ------------------------------------

def test_the_limit_in_flow_3_of_5_derives_the_two_merged_flows_in_the_same_run(rig, capsys):
    results = rig.run(_limit_in_flow_3)
    out = capsys.readouterr().out
    assert rig.strategy.fetched == ["f1", "f2", "f3"], "the sweep ended at the limit"
    assert rig.put_ids() == ["f1", "f2"], "the CSVs of the two merged flows were PUT in this run"
    assert results == [(UNIT_KEY, "partial")]
    row = rig.unit()
    assert row["status"] == "partial"
    assert row["last_success_utc"] is None, "a pass that did not finish is never a success"
    assert row["last_attempt_utc"]
    assert row["upstream_vintage"] is None, "the vintage does not advance on a partial"
    assert "hard limit reached at 3 of 5 flows" in row["last_error"], row["last_error"]
    assert rig.runs() and [r[0] for r in rig.runs()] == ["partial"]
    assert rig.state.get_source("eurostat") is None, "no source-level success either"
    # the saved state holds the two finished flows, with their new vintage, and not the third
    side = rig.sidecar()
    assert {f for f, s in side.items() if s.get("status") == "ok" and s.get("vintage") == "v1"} == {"f1", "f2"}
    assert "f3" not in side
    # nothing was queued or booked as a debt: both ids derived
    assert rig.state.csv_retries("eurostat") == [] and rig.state.csv_desktop_owed("eurostat") == []
    # the log says what happened, and does not say the alarm was swallowed or the unit timed out
    assert "[eurostat] hard limit reached at 3 of 5 flows" in out
    assert "[eurostat] changed flows (merge-measured): 2 of 2 merged" in out
    assert "hard limit FIRED at" in out and "returned the work it had done" in out
    assert "the raise was swallowed" not in out and f"TIMEOUT {UNIT_KEY}" not in out
    assert orchestrate.UNIT_TIMEOUT_FIRED is False and orchestrate.UNIT_TIMEOUT_ANSWERED is False


def test_the_store_and_the_state_of_the_test_are_inside_its_own_folder(rig):
    """The fence of this file: the unit the registry builds points into tmp_path."""
    rig.run(_limit_in_flow_3)
    assert os.path.isdir(rig.store_dir) and (rig.store_dir / "F1.parquet").exists()
    assert not (rig.store_dir / "F3.parquet").exists()
    assert os.path.exists(os.path.join(config.STATE_DIR, "pending_catalog_sync.txt"))
    assert str(rig.tmp) in config.STATE_DIR and str(rig.tmp) in config.DATA_ROOT


def test_an_error_of_another_type_raised_after_the_alarm_fired_hands_over_too(rig):
    """A library can swallow the alarm's own exception; what surfaces later is any error, with the
    orchestrator's flag set (run 38407606463 ended "with TooBigForRunner" after 225 flows and derived
    nothing). The handlers treat that as the limit, and so does the hand-over."""
    from updater.strategies.fetchers import eurostat

    def fetch(fid):
        if fid == "f3":
            orchestrate.UNIT_TIMEOUT_FIRED = True            # the alarm fired; its raise was lost
            raise eurostat.TooBigForRunner("gzip body holds ~30,000,000 rows")
        return _tbl(_rows(fid)), "ok"

    results = rig.run(fetch)
    assert results == [(UNIT_KEY, "partial")] and rig.put_ids() == ["f1", "f2"]
    assert "hard limit reached at 3 of 5 flows" in rig.unit()["last_error"]


def test_an_alarm_whose_raise_was_swallowed_ends_the_sweep_at_the_next_flow(rig, capsys):
    """The alarm fired during flow 2 and nothing raised: the sweep stops before flow 3 is fetched,
    instead of running on with no limit."""
    def fetch(fid):
        if fid == "f2":
            orchestrate.UNIT_TIMEOUT_FIRED = True            # the alarm fired; its raise was retried away
        return _tbl(_rows(fid)), "ok"

    results = rig.run(fetch)
    out = capsys.readouterr().out
    assert rig.strategy.fetched == ["f1", "f2"], "no flow is started after the alarm fired"
    assert results == [(UNIT_KEY, "partial")] and rig.put_ids() == ["f1", "f2"]
    row = rig.unit()
    assert row["status"] == "partial" and row["last_success_utc"] is None
    assert "hard limit reached at 3 of 5 flows" in row["last_error"]
    side = rig.sidecar()
    assert {f for f, s in side.items() if s.get("status") == "ok"} == {"f1", "f2"}
    assert "the unit then stopped its sweep and returned the work it had done" in out
    assert "the raise was swallowed" not in out


def test_without_the_opt_in_a_swallowed_alarm_does_not_stop_the_sweep_as_before(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    fetched = []

    def fetch(fid):
        fetched.append(fid)
        if fid == "f2":
            orchestrate.UNIT_TIMEOUT_FIRED = True
        return _tbl(_rows(fid)), "ok"

    res = _run_giant(tmp_path, fetch, report_changed_flows=False)
    assert fetched == FLOWS and res.status == "ok"


# ---- (b) the state file cannot be saved ------------------------------------------------------

def test_the_limit_with_every_state_save_failing_still_derives_the_merged_flows(rig, monkeypatch, capsys):
    def no_save(source_dir, state):
        raise OSError("PUT of the state file failed")

    monkeypatch.setattr(_giant, "save_state", no_save)
    results = rig.run(_limit_in_flow_3)
    out = capsys.readouterr().out
    assert rig.put_ids() == ["f1", "f2"]
    assert results == [(UNIT_KEY, "partial")]
    row = rig.unit()
    assert row["status"] == "partial" and row["last_success_utc"] is None
    assert "hard limit reached at 3 of 5 flows" in row["last_error"]
    assert "the state file was NOT saved (OSError: PUT of the state file failed)" in row["last_error"]
    assert rig.sidecar() is None, "no save succeeded"
    assert "the state file was NOT saved" in out and "2 changed flow(s) still go to the CSV step" in out


def test_a_failed_final_save_alone_returns_partial_with_the_changed_set(tmp_path, monkeypatch):
    """No limit: all five flows merge, then the final save fails."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    saves = []

    def no_save(source_dir, state):
        saves.append(sorted(state))
        raise OSError("PUT of the state file failed")

    monkeypatch.setattr(_giant, "save_state", no_save)
    res = _run_giant(tmp_path, lambda fid: (_tbl(_rows(fid)), "ok"))
    assert res.status == "partial"
    assert sorted(res.changed_keys) == FLOWS
    assert res.changed_keys["f1"] == "2025-12-31"
    assert "the state file was NOT saved (OSError: PUT of the state file failed)" in res.error
    assert "hard limit" not in res.error
    assert saves == [FLOWS], "one final save was tried, with every flow in it"


def test_a_failed_final_save_with_nothing_changed_is_raised_as_before(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    _run_giant(tmp_path, lambda fid: (_tbl(_rows(fid)), "ok"))          # seed: every flow stored
    moved = {f: dict(m, vintage="v2") for f, m in CAT.items()}

    def no_save(source_dir, state):
        raise OSError("PUT of the state file failed")

    monkeypatch.setattr(_giant, "save_state", no_save)
    with pytest.raises(OSError, match="PUT of the state file failed"):
        _run_giant(tmp_path, lambda fid: (_tbl(_rows(fid)), "ok"), catalog=moved)   # equal rows: no change


# ---- (c) callers without the opt-in, and select_flows ---------------------------------------

class _Unit:
    def __init__(self, d):
        self.out_paths = [str(d)]


def _run_giant(tmp_path, fetch, *, catalog=None, **kw):
    opts = dict(report_changed_flows=True, bounded_merge=True, checkpoint_every=10)
    opts.update(kw)
    return _giant.run_giant(_Unit(tmp_path), source="zzgiant", fetch_catalog=lambda: dict(catalog or CAT),
                            fetch_flow=lambda fid, meta, since, session: fetch(fid),
                            csv_accept="text/csv", rate=0, timeout=1, **opts)


def test_without_the_opt_in_the_limit_is_raised_after_the_checkpoint_as_before(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_ANSWERED", False, raising=False)
    with pytest.raises(orchestrate.UnitTimeout):
        _run_giant(tmp_path, _limit_in_flow_3, report_changed_flows=False)
    side = json.loads((tmp_path / "_giant_state.json").read_text(encoding="utf-8"))
    assert {f for f, s in side.items() if s.get("status") == "ok"} == {"f1", "f2"}
    assert orchestrate.UNIT_TIMEOUT_ANSWERED is False


def test_without_the_opt_in_the_orchestrator_records_the_timeout_as_before(rig, capsys):
    results = rig.run(_limit_in_flow_3, opt_in=False)
    out = capsys.readouterr().out
    assert results == [(UNIT_KEY, "timeout")]
    assert rig.blob.keys == [], "no CSV step runs for a unit that timed out"
    row = rig.unit()
    assert row["status"] == "transient_fail" and row["last_success_utc"] is None
    assert "exceeded its 45-minute hard limit" in row["last_error"]
    assert f"TIMEOUT {UNIT_KEY}" in out and "hard limit FIRED" not in out


def test_without_the_opt_in_a_failed_final_save_is_raised_as_before(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)

    def no_save(source_dir, state):
        raise OSError("PUT of the state file failed")

    monkeypatch.setattr(_giant, "save_state", no_save)
    with pytest.raises(OSError, match="PUT of the state file failed"):
        _run_giant(tmp_path, lambda fid: (_tbl(_rows(fid)), "ok"), report_changed_flows=False)


def test_the_oecd_path_raises_the_limit_and_the_save_error_as_before(tmp_path, monkeypatch):
    """oecd calls run_giant with no opt-in at all. A limit that leaves its flow loop is raised, with
    no state saved; a limit raised inside its fetch is booked as one failed flow and the sweep goes on;
    a failed final save is raised. All three as before this change."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_ANSWERED", False, raising=False)
    monkeypatch.setattr(oecd, "RATE", 0)
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: dict(CAT))
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, session: (_tbl(_rows(fid)), "ok"))
    real = merge.merge_and_write

    def merge_with_alarm(out_path, table, **kw):
        if out_path.endswith("F3.parquet"):
            _alarm()
        return real(out_path, table, **kw)

    monkeypatch.setattr(merge, "merge_and_write", merge_with_alarm)
    with pytest.raises(orchestrate.UnitTimeout):
        oecd.update(_Unit(tmp_path / "a"), None)
    assert not (tmp_path / "a" / "_giant_state.json").exists(), "no checkpoint, no final save: as before"
    assert orchestrate.UNIT_TIMEOUT_ANSWERED is False
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(merge, "merge_and_write", real)

    def fetch_with_alarm(fid, meta, since, session):
        if fid == "f3":
            _alarm()
        return _tbl(_rows(fid)), "ok"

    monkeypatch.setattr(oecd, "fetch_flow", fetch_with_alarm)
    res = oecd.update(_Unit(tmp_path / "b"), None)
    assert res.status == "partial" and res.changed_keys is None
    assert res.error.startswith("1/5 sub-unit(s) transient-failed") and "f3" in res.error
    side = json.loads((tmp_path / "b" / "_giant_state.json").read_text(encoding="utf-8"))
    assert {f for f, s in side.items() if s.get("status") == "ok"} == {"f1", "f2", "f4", "f5"}
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(oecd, "fetch_flow", lambda fid, meta, since, session: (_tbl(_rows(fid)), "ok"))

    def no_save(source_dir, state):
        raise OSError("PUT of the state file failed")

    monkeypatch.setattr(_giant, "save_state", no_save)
    with pytest.raises(OSError, match="PUT of the state file failed"):
        oecd.update(_Unit(tmp_path / "c"), None)


def test_select_flows_answers_as_before():
    """Order: new and moved-vintage flows in catalogue order, then the flows to redo; then the cap."""
    cat = {f: {"vintage": "v2", "filename": f + ".parquet"} for f in "abcdefgh"}
    state = {"a": {"vintage": "v2", "status": "ok"},                 # unchanged and fine: not selected
             "b": {"vintage": "v1", "status": "ok"},                 # vintage moved
             "c": {"vintage": "v2", "status": "transient_fail"},     # redo
             "d": {"vintage": "v2", "status": "partial"},            # redo
             "e": {"vintage": "v2", "status": "empty"},              # redo
             "f": {"vintage": "v2", "status": "no_time_dimension"},  # settled: not selected
             "g": {"vintage": "v2"}}                                 # no status: redo
    assert _giant.select_flows(cat, state) == (["b", "h", "c", "d", "e", "g"], False)
    assert _giant.select_flows(cat, state, max_flows=2) == (["b", "h"], True)
    assert _giant.select_flows(cat, {}) == (list("abcdefgh"), False)
    assert _giant.select_flows({}, state) == ([], False)


# ---- (d) the limit fires before any flow merged a change -------------------------------------

def test_the_limit_in_the_first_flow_is_recorded_as_a_timeout_as_before(rig, capsys):
    def fetch(fid):
        _alarm()

    results = rig.run(fetch)
    out = capsys.readouterr().out
    assert rig.strategy.fetched == ["f1"]
    assert results == [(UNIT_KEY, "timeout")]
    assert rig.blob.keys == []
    row = rig.unit()
    assert row["status"] == "transient_fail" and row["last_success_utc"] is None
    assert "exceeded its 45-minute hard limit" in row["last_error"]
    assert [r[0] for r in rig.runs()] == ["transient_fail"]
    assert f"TIMEOUT {UNIT_KEY}" in out and "hard limit FIRED" not in out


def test_the_limit_after_merges_that_changed_nothing_is_recorded_as_a_timeout_as_before(rig):
    os.makedirs(rig.store_dir)
    for f in ("f1", "f2"):                       # the store already holds the rows the fetch returns
        pq.write_table(_tbl(_rows(f)), rig.store_dir / (f.upper() + ".parquet"))
    results = rig.run(_limit_in_flow_3)
    assert rig.strategy.fetched == ["f1", "f2", "f3"]
    assert results == [(UNIT_KEY, "timeout")] and rig.blob.keys == []
    assert rig.unit()["status"] == "transient_fail"
    side = rig.sidecar()                         # the handler's checkpoint, as before
    assert {f for f, s in side.items() if s.get("status") == "ok"} == {"f1", "f2"}
