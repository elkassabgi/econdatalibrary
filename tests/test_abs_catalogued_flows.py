"""abs: the health gate reads the CATALOGUED series, and the live catalogued flows are fetched every pass.

Daily run 39677388361 read `RED-DATA abs ... 295d 2025-12-31`. That date was an annual ERP key: the
gate's newest_obs is max(series_cursor <= today), and abs's cursors were a capped fold of bare store
keys filled in rotation order by census flows - none of the 18 catalogued series was ever a cursor
(review AR-367). LF sat at rotation position 1,168 of 1,222, so it was also a release behind ABS.

These tests run the real update(), the real merge and the real health.assess(); ABS and the clock
are faked. State is recorded as the orchestrator records it (put_series_cursors upserts; the unit's
last_obs_date never regresses), beside the legacy rows production holds: a bare ERP cursor at
2025-12-31 and a 2031 projection cursor, and the unit's 2071-12-31 frontier.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from connectors.abs.connector import SERIES  # noqa: E402
from updater import health  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers import abs as A  # noqa: E402

TODAY = dt.datetime.now(dt.timezone.utc).date()
LF_KEY = "M13.3.1599.20.AUS.M"           # abs:LF:M13.3.1599.20.AUS.M, the unemployment rate
CPI_KEY = "1.10001.10.50.Q"              # abs:CPI:1.10001.10.50.Q


def _days(n):
    return TODAY - dt.timedelta(days=n)


class _Deadline:
    def __init__(self, allow):
        self.allow, self.asked, self.budget_min = allow, 0, 35

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 35.0


def _plant(tmp_path, flows):
    """flows: {FLOW: [(series_key, date), ...]} -> one parquet per flow (only if absent)."""
    for f, rows in flows.items():
        p = tmp_path / f"{f}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": [k for k, _ in rows],
                                     "obs_date": pa.array([d for _, d in rows], pa.date32()),
                                     "value": [1.0] * len(rows)}), p)


def _wire(monkeypatch, tmp_path, flows, tails=None, allow=99, fail=()):
    """tails: {FLOW: [(key, date), ...]} what ABS returns for the date-tail (default: the stored rows,
    an idempotent re-fetch). fail: flows whose fetch raises. Returns the list of flows asked."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(A.config, "source_dir", lambda source: str(tmp_path))
    _plant(tmp_path, flows)
    monkeypatch.setattr(A, "Deadline", lambda minutes=None: _Deadline(allow))
    monkeypatch.setattr(A.ing, "session", lambda: None)
    tails = tails or {}
    asked = []

    def _collect(sess, flow, key, params=None):
        asked.append(flow)
        if flow in fail:
            raise ConnectionError("pretend ABS dropped the body")
        rows = tails.get(flow, flows.get(flow, []))
        return [k for k, _ in rows], [d for _, d in rows], [2.0] * len(rows)
    monkeypatch.setattr(A.ing, "collect", _collect)
    return asked


def _gate(res, extra_cursors=None):
    """The abs row of health.assess() after recording `res` the way orchestrate.py does."""
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    # production's legacy rows (state copy 2026-10-20): bare keys, never deleted by an upsert
    st.put_series_cursors("abs", {"ERP.AUS.AUS.A": "2025-12-31", "PROJ.AUS.A": "2031-12-31"})
    if res.series_cursors:
        st.put_series_cursors("abs", res.series_cursors)
    if extra_cursors:
        st.put_series_cursors("abs", extra_cursors)
    last = max(x for x in (res.last_obs_date, "2071-12-31") if x)     # never regresses (orchestrate)
    st.upsert_unit("abs", "_all", strategy="sdmx_delta", status=res.status, last_obs_date=last,
                   obs_count=res.obs, last_attempt_utc=now, last_error=res.error)
    return next(r for r in health.assess(store=st)["sources"] if r["source"] == "abs")


def _store(stale_or_fresh_lf, cpi):
    return {
        "LF": [(LF_KEY, _days(400)), (LF_KEY, stale_or_fresh_lf), ("M99.OTHER.AUS.M", _days(3))],
        "CPI": [(CPI_KEY, cpi)],
        "POPULATION_CLOCK": [("PC.AUS", _days(5))],       # uncatalogued, FRESH
        "POP_PROJ": [("PP.AUS.A", dt.date(2071, 12, 31))],  # uncatalogued projection
        "ZZZ_REST": [("Z.1", _days(900))],                # rotation tail the budget does not reach
    }


def test_a_stale_catalogued_set_reads_red_data_even_beside_a_fresh_uncatalogued_flow(monkeypatch, tmp_path):
    stale = _days(100)                                   # the monthly data clock is 84 days
    flows = _store(stale, _days(120))
    _wire(monkeypatch, tmp_path, flows, allow=4)         # head LF, CPI + 2 rotation flows
    res = A.update(None, None)
    row = _gate(res)
    assert row["health"] == "RED-DATA", row
    assert row["newest_obs"] == stale.isoformat(), row


def test_a_fresh_catalogued_flow_does_not_read_red_data(monkeypatch, tmp_path):
    fresh = _days(20)
    flows = _store(_days(100), _days(120))
    # ABS publishes LF up to 20 days ago; the head fetch merges it on this pass
    _wire(monkeypatch, tmp_path, flows, tails={"LF": [(LF_KEY, fresh)]}, allow=4)
    res = A.update(None, None)
    row = _gate(res)
    assert row["health"] != "RED-DATA", row
    assert row["newest_obs"] == fresh.isoformat(), row
    assert res.series_cursors[f"LF:{LF_KEY}"] == fresh.isoformat()


def test_a_transient_uncatalogued_flow_leaves_no_flow_sentinel_and_the_gate_stays_red(monkeypatch, tmp_path):
    """AR-371 C2: the removed '__flow__<FLOW>' sentinel fed the gate an uncatalogued flow's frontier on a
    transient failure. A fresh POPULATION_CLOCK that fails must not keep a stale catalogued set green."""
    # A failing pass reads ATTENTION whatever its cursors (health.py checks attention before data), so the
    # sentinel's harm is the row it LEAVES in state: pass 1 fails, pass 2 is a clean rotation, and the gate
    # reads the state both passes wrote (upserts accumulate).
    flows = {**_store(_days(100), _days(120)), "ZZZ_REST2": [("Z.2", _days(900))]}
    _wire(monkeypatch, tmp_path, flows, allow=4, fail=("POPULATION_CLOCK",))
    res1 = A.update(None, None)
    assert "POPULATION_CLOCK" in res1.error, res1.error                  # it really failed
    assert not [k for k in res1.series_cursors if k.startswith("__flow__")], res1.series_cursors
    _wire(monkeypatch, tmp_path, flows, allow=4)                         # clean; the rest deferred
    res2 = A.update(None, None)
    assert res2.status == "partial" and "none failed" in res2.error, res2.error
    row = _gate(res2, extra_cursors=res1.series_cursors)
    assert row["health"] == "RED-DATA" and row["newest_obs"] == _days(100).isoformat(), row


def test_a_head_flow_merge_reaches_changed_keys_and_the_derive(monkeypatch, tmp_path):
    """AR-371 C1: the #74 path (merge-measured changed_keys) is how the served LF/CPI/WPI/ANA_AGG/AWE CSVs
    re-derive. A new LF row on an under-cap pass must reach changed_keys AND the derive."""
    import sqlite3
    import types
    from updater import derive, orchestrate
    flows = {"AAA": [("a", _days(900))], "LF": [(LF_KEY, _days(40))]}
    _wire(monkeypatch, tmp_path, flows, tails={"LF": [(LF_KEY, _days(40)), (LF_KEY, _days(10))]})
    res = A.update(None, None)
    # (AAA:a changes too: the fake re-fetch carries value 2.0 over the stored 1.0 - an uncatalogued key)
    assert res.changed_keys.get(f"LF:{LF_KEY}") == _days(10).isoformat(), res.changed_keys
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"abs:{k}", "abs") for k in sorted(A.CATALOGUED)])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    orchestrate._REG_ENTRIES = None
    asked = []
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert asked == [f"abs:LF:{LF_KEY}"] and failed == [], (asked, failed)


def test_an_older_fetched_row_never_lowers_a_cursor(monkeypatch, tmp_path):
    """AR-371 C3: the cursor is raised by the published rows, never lowered - an older (revised) row
    arriving after a newer one must not win."""
    flows = {"LF": [(LF_KEY, _days(30))]}
    _wire(monkeypatch, tmp_path, flows, tails={"LF": [(LF_KEY, _days(10)), (LF_KEY, _days(50))]})
    res = A.update(None, None)
    assert res.series_cursors[f"LF:{LF_KEY}"] == _days(10).isoformat(), res.series_cursors


def test_negative_control_a_fresh_cursor_in_state_does_move_the_gate(monkeypatch, tmp_path):
    """The RED above is the fetcher's doing, not a gate that ignores cursors: the same stale pass
    with one fresh cursor in state (what the removed bare-key fold could put there) reads green."""
    flows = _store(_days(100), _days(120))
    _wire(monkeypatch, tmp_path, flows, allow=4)
    res = A.update(None, None)
    row = _gate(res, extra_cursors={"PC.AUS": _days(5).isoformat()})
    assert row["health"] != "RED-DATA", row


def test_cursors_are_exactly_the_catalogued_series_in_the_catalogue_form(monkeypatch, tmp_path):
    flows = _store(_days(30), _days(60))
    _wire(monkeypatch, tmp_path, flows)                  # every flow visited
    res = A.update(None, None)
    assert res.series_cursors == {f"LF:{LF_KEY}": _days(30).isoformat(),
                                  f"CPI:{CPI_KEY}": _days(60).isoformat()}, res.series_cursors
    assert set(res.series_cursors) <= A.CATALOGUED


def test_a_catalogued_flow_whose_fetch_fails_still_reports_its_store_frontier(monkeypatch, tmp_path):
    flows = _store(_days(30), _days(60))
    _wire(monkeypatch, tmp_path, flows, fail=("LF",))
    res = A.update(None, None)
    assert res.status == "partial"
    assert res.series_cursors[f"LF:{LF_KEY}"] == _days(30).isoformat(), res.series_cursors


def test_a_refused_write_reports_the_store_frontier_not_the_fetched_rows(monkeypatch, tmp_path):
    flows = _store(_days(30), _days(60))
    _wire(monkeypatch, tmp_path, flows, tails={"LF": [(LF_KEY, _days(2))]})
    real = A.merge.merge_and_write

    def _refuse(path, tbl, **k):
        if path.endswith("LF.parquet"):
            raise A.DefinitiveError("pretend the shrink guard refused")
        return real(path, tbl, **k)
    monkeypatch.setattr(A.merge, "merge_and_write", _refuse)
    res = A.update(None, None)
    assert res.series_cursors[f"LF:{LF_KEY}"] == _days(30).isoformat(), res.series_cursors


def test_the_live_catalogued_flows_are_fetched_first_on_every_pass(monkeypatch, tmp_path):
    flows = {"AAA": [("a", _days(900))], "BBB": [("b", _days(900))], "CCC": [("c", _days(900))],
             "LF": [(LF_KEY, _days(30))], "WPI": [("1.THRPIB.7.TOT.10.AUS.Q", _days(100))]}
    passes = []
    for _ in range(3):
        asked = _wire(monkeypatch, tmp_path, flows, allow=3)   # the head (2) + one rotation flow
        A.update(None, None)
        passes.append(list(asked))
    assert passes == [["LF", "WPI", "AAA"], ["LF", "WPI", "BBB"], ["LF", "WPI", "CCC"]], passes


def test_head_visits_do_not_move_the_rotation_bookmark(monkeypatch, tmp_path):
    flows = {"AAA": [("a", _days(900))], "BBB": [("b", _days(900))], "LF": [(LF_KEY, _days(30))]}
    _wire(monkeypatch, tmp_path, flows, allow=2)               # LF + AAA
    A.update(None, None)
    assert json.loads((tmp_path / "_rotation.json").read_text())["after"] == "AAA.parquet"
    asked = _wire(monkeypatch, tmp_path, flows, allow=1)       # LF only, BBB deferred
    res = A.update(None, None)
    assert asked == ["LF"] and "BBB deferred" in res.error, (asked, res.error)
    assert json.loads((tmp_path / "_rotation.json").read_text())["after"] == "AAA.parquet"


def test_an_old_bookmark_naming_a_head_flow_keeps_its_position(monkeypatch, tmp_path):
    flows = {"AAA": [("a", _days(900))], "LF": [(LF_KEY, _days(30))], "MMM": [("m", _days(900))],
             "ZZZ": [("z", _days(900))]}
    _plant(tmp_path, flows)
    (tmp_path / "_rotation.json").write_text(json.dumps({"after": "LF.parquet"}))
    asked = _wire(monkeypatch, tmp_path, flows, allow=2)
    A.update(None, None)
    assert asked == ["LF", "MMM"], asked                       # resumes after LF, not at the top


def test_the_ceased_flows_are_not_fetched_on_every_pass(monkeypatch, tmp_path):
    """CPI_M and RT end at the publisher (ABS: 'CEASED - Dataset: ...'). They are rotated once per
    cycle like any other flow - that visit writes their cursors - and are not in the every-pass head."""
    flows = {"CPI_M": [("1.10001.10.50.M", dt.date(2025, 9, 30))],
             "RT": [("M1.20.20.AUS.M", dt.date(2025, 6, 30))],
             "LF": [(LF_KEY, _days(30))], "ZZZ": [("z", _days(900))]}
    asked1 = _wire(monkeypatch, tmp_path, flows, allow=3)      # LF + CPI_M + RT; ZZZ owed
    res1 = A.update(None, None)
    assert asked1 == ["LF", "CPI_M", "RT"], asked1
    assert res1.series_cursors["CPI_M:1.10001.10.50.M"] == "2025-09-30"
    assert res1.series_cursors["RT:M1.20.20.AUS.M"] == "2025-06-30"
    asked2 = _wire(monkeypatch, tmp_path, flows, allow=99)
    A.update(None, None)
    assert asked2 == ["LF", "ZZZ"], "a ceased flow already visited this cycle is not fetched again"


def test_the_head_spends_the_same_budget(monkeypatch, tmp_path):
    flows = {"AAA": [("a", _days(900))], "LF": [(LF_KEY, _days(30))]}
    asked = _wire(monkeypatch, tmp_path, flows, allow=0)
    res = A.update(None, None)
    assert asked == [] and "LF deferred" in res.error, (asked, res.error)


def test_over_the_changed_keys_cap_the_cursor_path_carries_the_catalogued_series(monkeypatch, tmp_path):
    """The over-cap fallback derives from series_cursors: they are now the catalogued series in the
    mappable '<FLOW>:<key>' form, not bare keys (and the truncation is still flagged, AR-132)."""
    monkeypatch.setattr(A.merge, "CHANGED_KEYS_CAP", 1)
    flows = {"AAA": [("a1", _days(900)), ("a2", _days(900))], "LF": [(LF_KEY, _days(30))]}
    _wire(monkeypatch, tmp_path, flows, tails={"AAA": [("a1", _days(800)), ("a2", _days(800))],
                                               "LF": [(LF_KEY, _days(10))]})
    res = A.update(None, None)
    assert res.changed_keys is None and res.cursor_cap_hit is True
    assert res.series_cursors == {f"LF:{LF_KEY}": _days(10).isoformat()}, res.series_cursors


def test_over_the_cap_the_orchestrator_derives_the_visited_catalogued_ids(monkeypatch, tmp_path):
    """Review of this change: with A2 the fallback cursors MAP, so the over-cap path now derives the
    visited catalogued ids (a superset of the changed ones); the truncation flag still stands."""
    import sqlite3
    import types
    from updater import derive, orchestrate
    monkeypatch.setattr(A.merge, "CHANGED_KEYS_CAP", 1)
    flows = {"AAA": [("a1", _days(900)), ("a2", _days(900))], "LF": [(LF_KEY, _days(30))],
             "CPI": [(CPI_KEY, _days(60))]}
    _wire(monkeypatch, tmp_path, flows, tails={"AAA": [("a1", _days(800)), ("a2", _days(800))]})
    res = A.update(None, None)
    assert res.changed_keys is None and res.cursor_cap_hit is True
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"abs:{k}", "abs") for k in sorted(A.CATALOGUED)])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    orchestrate._REG_ENTRIES = None
    asked = []
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert sorted(asked) == [f"abs:CPI:{CPI_KEY}", f"abs:LF:{LF_KEY}"], asked
    assert failed == [], failed
    # The debt row itself is booked by orchestrate.py's cursor_cap_hit branch from the flag asserted
    # above - unchanged code, not re-driven here.


def test_when_only_the_head_ran_the_log_does_not_say_no_flow_was_attempted(monkeypatch, tmp_path, capsys):
    flows = {"AAA": [("a", _days(900))], "LF": [(LF_KEY, _days(30))]}
    _wire(monkeypatch, tmp_path, flows, allow=1)
    A.update(None, None)
    out = capsys.readouterr().out
    assert "only the every-pass head (1 flow(s)) ran" in out and "NO flow was attempted at all" not in out, out


def test_an_unreadable_store_frontier_is_said_not_swallowed(monkeypatch, tmp_path, capsys):
    flows = {"LF": [(LF_KEY, _days(30))]}
    _wire(monkeypatch, tmp_path, flows)

    def _boom(path, columns=None):
        raise OSError("pretend R2 refused the read")
    monkeypatch.setattr(A.blob, "read_table", _boom)
    assert A._store_frontier(str(tmp_path / "LF.parquet"), A.CATALOGUED_KEYS["LF"]) == (None, {})
    assert "store frontier unreadable (OSError" in capsys.readouterr().out


def test_the_head_is_the_connector_set_minus_the_ceased_pair():
    """18 catalogued ids = the connector's SERIES (measured equal to the local catalog.db's abs rows,
    by primary-key range, 2026-10-22). If a series is catalogued later this test names the change."""
    assert A.CATALOGUED == frozenset(SERIES) and len(A.CATALOGUED) == 18
    assert set(A.CATALOGUED_KEYS) == {"ANA_AGG", "AWE", "CPI", "CPI_M", "LF", "RT", "WPI"}
    assert A.CEASED_FLOWS <= set(A.CATALOGUED_KEYS)
    assert A.HEAD_FLOWS == ("ANA_AGG", "AWE", "CPI", "LF", "WPI")
