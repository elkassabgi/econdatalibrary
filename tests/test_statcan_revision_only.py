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


def _store(tmp_path, rows):
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
    pq.write_table(t, str(tmp_path / f"{PID}.parquet"))


def _wire(monkeypatch, tmp_path, tail):
    """tail: [(vector id, date, value)] - what getBulkVectorDataByRange returns for the cube."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(sc.config, "BACKEND", "local")
    monkeypatch.setattr(sc, "OUT_DIR", str(tmp_path))
    monkeypatch.setattr(sc, "STATE", str(tmp_path / "_incr_state.json"))
    monkeypatch.setattr(sc, "_changed_pids", lambda since: {PID})
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


def _derived(monkeypatch, tmp_path, res):
    _catalog(tmp_path, monkeypatch, [f"statcan:V{V_REV}", f"statcan:V{V_SAME}"])
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


def test_an_over_cap_merge_without_a_report_is_not_claimed_as_revised(monkeypatch, tmp_path):
    """No report means no measurement: the over-cap path keeps its old booking and poisons changed_keys."""
    _store(tmp_path, [(V_REV, D_OLD, 1.0), (V_SAME, D_OLD, 2.0)])
    _wire(monkeypatch, tmp_path, [(V_REV, D_OLD, 1.5), (V_SAME, D_OLD, 2.0)])
    real = sc.merge.merge_and_write

    def _no_report(path, tbl, **kw):
        assert not kw.get("report_changed_keys"), "the over-cap path must not ask for a report"
        return real(path, tbl, **kw)
    # route to the over-cap branch without building 2M rows: the branch keys on tbl.num_rows
    orig_tail = sc._fetch_cube_tail

    class _Big:
        def __init__(self, t):
            self._t = t

        def __getattr__(self, a):
            return getattr(self._t, a)

        @property
        def num_rows(self):
            return 2_000_001
    monkeypatch.setattr(sc, "_fetch_cube_tail", lambda *a, **k: _Big(orig_tail(*a, **k)))
    monkeypatch.setattr(sc.merge, "merge_and_write", lambda path, tbl, **kw: _no_report(path, tbl._t, **kw))
    res = sc.update(None, None)
    assert res.changed_keys is None and res.status == "no_change", (res.status, res.changed_keys, res.error)


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
