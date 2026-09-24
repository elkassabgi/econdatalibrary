"""statcan: a pass whose merges only REVISE stored values must read ok and reach the CSV phase (R1125).

statcan booked each cube as added_unit(n - before), i.e. net NEW rows. StatCan's release-dated tail
carries revisions to old periods, and a merge that only revises adds no row - so such a pass finalized
`no_change`, orchestrate._should_derive_csvs skipped the CSV phase, and the resume set / watermark moved
on: the served CSV kept the old value under a green unit. The same bug was fixed for ember (#76).

The real update(), the real merge on a real parquet, the real finalize(), the real status gate and the
real changed-key -> catalogue id mapping all run. Only StatCan's two network calls are faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import statcan as sc  # noqa: E402

PID = "10000001"
V_REV, V_SAME = 41690973, 41690974
D_OLD = dt.date(2024, 1, 1)


def _store(tmp_path, rows, pid=PID):
    """Write the cube in the on-disk schema. rows: [(vector id, date, value)]."""
    t = pa.table({
        "series_key": pa.array([f"v{v}" for v, _d, _x in rows], pa.string()),
        "obs_date": pa.array([d for _v, d, _x in rows], pa.date32()),
        "value": pa.array([x for _v, _d, x in rows], pa.float64()),
        "geo": pa.array(["Canada"] * len(rows), pa.string()),
        "uom": pa.array(["Dollars"] * len(rows), pa.string()),
        "coordinate": pa.array(["1.1"] * len(rows), pa.string()),
        "status": pa.array([""] * len(rows), pa.string()),
    }, schema=sc.SCHEMA)
    pq.write_table(t, str(tmp_path / f"{pid}.parquet"))


def _wire(monkeypatch, tmp_path, tail, pids=(PID,)):
    """tail: [(vector id, date, value)] - what getBulkVectorDataByRange returns (by vector, across cubes)."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(sc.config, "BACKEND", "local")
    monkeypatch.setattr(sc, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(sc, "STATE", str(tmp_path / "_incr_state.json"))
    monkeypatch.setattr(sc, "_changed_pids", lambda since: set(pids))
    monkeypatch.setattr(sc.time, "sleep", lambda s: None)

    def _post(endpoint, payload, tries=5):
        assert endpoint == "getBulkVectorDataByRange", endpoint
        asked = {int(v) for v in payload["vectorIds"]}
        by_vid: dict = {}
        for v, d, x in tail:
            if v in asked:
                by_vid.setdefault(v, []).append({"refPer": d.isoformat(), "value": str(x)})
        return [{"status": "SUCCESS", "object": {"vectorId": v, "vectorDataPoint": dps}}
                for v, dps in by_vid.items()]
    monkeypatch.setattr(sc, "_post", _post)


def _catalog(tmp_path, monkeypatch, ids):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "statcan") for i in ids])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))


def _derived(monkeypatch, tmp_path, res, backend=None):
    _catalog(tmp_path, monkeypatch, [f"statcan:V{V_REV}", f"statcan:V{V_SAME}"])
    if backend:
        monkeypatch.setattr(orchestrate.config, "BACKEND", backend)   # the local route runs statcan on r2
    orchestrate._REG_ENTRIES = None
    asked = []
    from updater import derive
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="statcan/_all", source_id="statcan", unit_id="_all")
    orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    return asked


def _value(tmp_path, vid):
    t = pq.read_table(str(tmp_path / f"{PID}.parquet")).to_pylist()
    return [r["value"] for r in t if r["series_key"] == f"v{vid}"]


def test_a_revision_only_pass_reads_ok_and_derives_the_revised_id(monkeypatch, tmp_path):
    _store(tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V_SAME, D_OLD, 2.0)])   # one value revised, no new row
    res = sc.update(None, None)
    assert _value(tmp_path, V_REV) == [1.5], "the merge did not revise the stored value"
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat()}, res.changed_keys
    assert orchestrate._should_derive_csvs(res.status), (res.status, res.error)
    assert res.status == "ok" and "1 sub-unit(s) revised stored values without adding rows" in res.error, \
        (res.status, res.error)
    assert _derived(monkeypatch, tmp_path, res) == [f"statcan:V{V_REV}"]


def test_negative_control_an_identical_republish_stays_no_change(monkeypatch, tmp_path):
    _store(tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])   # same numbers re-released
    res = sc.update(None, None)
    assert res.changed_keys == {}, res.changed_keys
    assert res.status == "no_change" and not orchestrate._should_derive_csvs(res.status), (res.status, res.error)
    assert "revised" not in (res.error or ""), res.error


def test_control_a_new_period_is_still_booked_as_new_rows(monkeypatch, tmp_path):
    _store(tmp_path, [(V_REV, D_OLD, 1.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, dt.date(2024, 2, 1), 3.0)])
    res = sc.update(None, None)
    assert res.status == "ok" and "+1 new rows" in res.error and "revised" not in res.error, res.error


def test_a_revision_riding_with_a_new_row_is_booked_as_added(monkeypatch, tmp_path):
    """A cube that adds a row AND revises one is data-bearing already: booked as added, not twice."""
    _store(tmp_path, [(V_REV, D_OLD, 1.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V_REV, dt.date(2024, 2, 1), 3.0)])
    res = sc.update(None, None)
    assert res.status == "ok" and "+1 new rows" in res.error and "revised" not in res.error, res.error


def test_a_tail_over_the_merges_default_cap_is_still_measured_and_derived_under_r2(monkeypatch, tmp_path):
    """R1244: a tail over the merge's default report cap (2M rows) was merged WITHOUT a report, booked empty
    - R1125 left open - and dropped changed_keys to None, which under r2 maps to no id. Every merge now asks
    for a cap of its own size. The default cap is lowered to 1 so a 2-row tail is 'over' it."""
    _store(tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V_SAME, D_OLD, 2.0)])
    monkeypatch.setattr(sc.merge, "CHANGED_KEYS_CAP", 1)
    seen, real = [], sc.merge.merge_and_write

    def _spy(path, tbl, **kw):
        seen.append((tbl.num_rows, kw.get("report_changed_keys"), kw.get("changed_keys_cap")))
        return real(path, tbl, **kw)
    monkeypatch.setattr(sc.merge, "merge_and_write", _spy)
    res = sc.update(None, None)
    assert seen == [(2, True, 2)], seen
    assert res.status == "ok" and "1 sub-unit(s) revised" in res.error, (res.status, res.error)
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat()}, res.changed_keys
    assert _derived(monkeypatch, tmp_path, res, backend="r2") == [f"statcan:V{V_REV}"]


def test_two_cubes_one_revised_one_identical(monkeypatch, tmp_path):
    """A revision in one cube must not make the next cube 'revised' (R1244 R1: `bool(changed_all)` leaked it),
    and after a revised cube the resume set closes and the watermark advances like any finished window."""
    import json
    P2 = "10000002"
    V2 = 50000001
    _store(tmp_path, [(V_REV, D_OLD, 1.0)], pid=PID)
    _store(tmp_path, [(V2, D_OLD, 7.0)], pid=P2)
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V2, D_OLD, 7.0)], pids=(PID, P2))   # PID sorts first
    res = sc.update(None, None)
    assert res.status == "ok" and "1 sub-unit(s) revised" in res.error, res.error
    assert "2 sub-unit(s) revised" not in res.error, res.error
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat()}, res.changed_keys
    st = json.loads((tmp_path / "_incr_state.json").read_text())
    assert st.get(sc.RESUME_WINDOW_KEY) == {}, st                          # window closed
    assert st.get("last_release_date") == dt.date.today().isoformat(), st   # watermark advanced


def test_finalize_many_revised_sub_units_are_ok_not_an_all_empty_break():
    """The _common part (from #76): revised sub-units never count toward the all-empty structural raise.
    statcan's floor is 10**9 so its own passes cannot reach it; the default floor (10) can."""
    from updater.strategies.fetchers._common import Tally, finalize
    t = Tally()
    for i in range(12):
        t.revised_unit(f"c{i}")
    res = finalize(t, 0, "2024-01-01", source="x")
    assert res.status == "ok" and "12 sub-unit(s) revised" in res.error, (res.status, res.error)
    t = Tally()
    for i in range(12):
        t.empty_unit()
    with pytest.raises(Exception, match="all 12 attempted sub-units returned empty"):
        finalize(t, 0, "2024-01-01", source="x")


def test_a_revised_sub_unit_counts_as_attempted():
    """A revised cube is one attempted sub-unit: a pass with one revised and one transient cube reads 1/2."""
    from updater.strategies.fetchers._common import Tally, finalize
    t = Tally()
    t.revised_unit("c1")
    t.transient_unit("c2")
    res = finalize(t, 0, "2024-01-01", source="x")
    assert res.status == "partial" and "1/2 sub-unit(s) transient-failed" in res.error, (res.status, res.error)


def test_the_status_gate_admits_exactly_ok_and_partial():
    """R1244 O2: the predicate itself - a revision-only pass reads ok, and ok must be admitted."""
    assert [s for s in ("ok", "partial", "no_change", "transient_fail") if orchestrate._should_derive_csvs(s)] \
        == ["ok", "partial"]


def test_run_once_runs_the_csv_phase_behind_that_gate():
    """R1244 O1: a mutant that never fired run_once's gate survived 73 run_once tests. Pinned through the
    parser: the one call of _derive_changed_csvs in run_once sits under `if _should_derive_csvs(status) and
    not dry:`."""
    import ast
    import inspect
    fn = next(n for n in ast.parse(inspect.getsource(orchestrate)).body
              if isinstance(n, ast.FunctionDef) and n.name == "run_once")
    gates = [n for n in ast.walk(fn) if isinstance(n, ast.If)
             and ast.unparse(n.test) == "_should_derive_csvs(status) and (not dry)"]
    assert len(gates) == 1, [ast.unparse(g.test) for g in gates]
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_derive_changed_csvs"]
    inside = [n for s in gates[0].body for n in ast.walk(s)
              if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_derive_changed_csvs"]
    assert len(calls) == 1 and len(inside) == 1, (len(calls), len(inside))


P2, V2 = "10000002", 50000001


class _OneCube:
    """The pass's budget lets exactly one cube START (statcan's Deadline between cubes)."""
    budget_min = 45.0

    def __init__(self, *a, **k):
        self.n = 0

    def spent(self):
        self.n += 1
        return self.n > 1


def test_a_capped_pass_still_carries_and_derives_its_revision(monkeypatch, tmp_path):
    """R1250 B9: `changed_keys = changed_all if not capped else None` survived the whole suite - a capped pass
    (statcan's most common: the budget ends mid-window) must still report the cube it revised; that cube is
    already in `done`, so the next pass will not look at it again."""
    _store(tmp_path, [(V_REV, D_OLD, 1.0)], pid=PID)
    _store(tmp_path, [(V2, D_OLD, 7.0)], pid=P2)
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V2, D_OLD, 8.0)], pids=(PID, P2))
    monkeypatch.setattr(sc, "Deadline", _OneCube)
    res = sc.update(None, None)
    assert res.new_vintage is None, "a capped pass never stamps a full vintage"
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat()}, res.changed_keys
    assert orchestrate._should_derive_csvs(res.status), (res.status, res.error)
    assert _derived(monkeypatch, tmp_path, res, backend="r2") == [f"statcan:V{V_REV}"]


def test_a_transient_cube_first_does_not_hide_a_later_revision(monkeypatch, tmp_path):
    """R1250 B14: `changed_all.update(_ch if all_ok else {})` survived - after a transient cube the pass is
    partial, and the revised cube behind it must still be reported and derived."""
    from updater.errors import TransientError
    _store(tmp_path, [(V2, D_OLD, 7.0)], pid="10000000")                   # sorts FIRST, and fails
    _store(tmp_path, [(V_REV, D_OLD, 1.0)], pid=PID)
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5)], pids=("10000000", PID))
    real_post = sc._post

    def _post(endpoint, payload, tries=5):
        if str(V2) in payload["vectorIds"]:
            raise TransientError("503 from WDS")
        return real_post(endpoint, payload, tries)
    monkeypatch.setattr(sc, "_post", _post)
    res = sc.update(None, None)
    assert res.status == "partial", (res.status, res.error)
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat()}, res.changed_keys
    assert _derived(monkeypatch, tmp_path, res, backend="r2") == [f"statcan:V{V_REV}"]


def test_a_report_over_the_default_cap_is_complete(monkeypatch, tmp_path):
    """R1250 B13: more CHANGED rows than the default cap must all be reported, not only the first cap-many."""
    _store(tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V_SAME, D_OLD, 2.5)])
    monkeypatch.setattr(sc.merge, "CHANGED_KEYS_CAP", 1)
    res = sc.update(None, None)
    assert res.changed_keys == {f"v{V_REV}": D_OLD.isoformat(), f"v{V_SAME}": D_OLD.isoformat()}, res.changed_keys
