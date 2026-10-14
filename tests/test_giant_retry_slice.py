"""The retry slice: failed flows get a bounded, backed-off slice at the head of a pass; fresh flows the rest.

WHAT WAS WRONG. select_flows put every flow that failed last time at the head of the selection, in
catalogue order. On 2026-10-14 the first 83 of eurostat's 400 selected flows were exactly the 83 flows
that failed last time, so every pass began by fetching the same failures; five of them are bodies over
the runner's row ceiling (91.5M, 30.5M, 30.5M, 20.1M and 20.1M values in Eurostat's own catalogue)
and cost 200-260 s each, every pass, for nothing.

WHAT THESE TESTS HOLD (run_giant with retry_slice, eurostat's opt-in):
  - failed flows enter a pass ONLY through the retry slice: at most 20 flows and 300 s, oldest failure
    first; then fresh flows in catalogue order;
  - a failed flow waits 12 h, 24 h, 2 d, ... at most 16 d after its last failure;
  - the attempt is written into the row before the fetch, so a flow the hard limit interrupts is
    recorded as a failure and backs off instead of staying first in line;
  - a body over the row ceiling is booked `too_big` (its `vintage` untouched) and not fetched again
    until the ceiling covers it or the parser changes; it is listed and never served;
  - a pass that left failed or too_big flows unattempted never returns `ok`;
  - the old-writer rule, the shared classification, oecd unchanged.

Several tests drive the REAL orchestrate.run_once for the registry entry `eurostat` with a strategy that
calls the REAL run_giant with eurostat's options; the rest drive run_giant directly. The catalogue, the
store, the per-flow state and state.db are in the test's folder; the CSV upload goes to a list.
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

from updater import config, derive, health, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import get as get_strategy  # noqa: E402
from updater.strategies.fetchers import _giant, eurostat, oecd  # noqa: E402

CFG = {"ceiling": 20_000_000, "parser": 1}
T0 = dt.datetime(2026, 10, 14, 6, 0, tzinfo=dt.timezone.utc)
UNIT_KEY = "eurostat/_all"


def _cat(flows, vintage="v1"):
    return {f: {"vintage": vintage, "filename": f.upper() + ".parquet"} for f in flows}


def _tbl(fid, value=1.0):
    return pa.table({"series_key": pa.array([f"geo={fid}"] * 2, pa.string()),
                     "obs_date": pa.array([dt.date(2024, 12, 31), dt.date(2025, 12, 31)], pa.date32()),
                     "value": pa.array([value, value], pa.float64())})


class _Now:
    """The time run_giant sees (_giant._utcnow), moved by the test between passes."""

    def __init__(self, monkeypatch, t=T0):
        self.t = t
        monkeypatch.setattr(_giant, "_utcnow", lambda: self.t)

    def add(self, **kw):
        self.t = self.t + dt.timedelta(**kw)


class _Unit:
    def __init__(self, d):
        self.out_paths = [str(d)]


def _seed(d, rows):
    os.makedirs(d, exist_ok=True)
    (d / "_giant_state.json").write_text(json.dumps(rows), encoding="utf-8")


def _state(d):
    return json.loads((d / "_giant_state.json").read_text(encoding="utf-8"))


def _run(d, catalog, fetch, *, cfg=CFG, max_flows=400, fetched=None):
    def fetch_flow(fid, meta, since, session):
        if fetched is not None:
            fetched.append(fid)
        return fetch(fid)

    return _giant.run_giant(_Unit(d), source="zzgiant", fetch_catalog=lambda: dict(catalog),
                            fetch_flow=fetch_flow, csv_accept="text/csv", rate=0, timeout=1,
                            max_flows=max_flows, report_changed_flows=True, bounded_merge=True,
                            checkpoint_every=10, retry_slice=cfg)


def _ok(fid):
    return _tbl(fid), "ok"


def _failed_rows(names, status="definitive_fail", **extra):
    return {f: dict({"status": status}, **extra) for f in names}


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_ANSWERED", False)


# ---- the plan and the shared classification ---------------------------------------------------

def test_failed_flows_come_only_through_a_bounded_slice_then_fresh_in_catalogue_order():
    failed = [f"a{i:02d}" for i in range(25)]
    fresh = [f"z{i:02d}" for i in range(5)]
    cat = _cat(failed + fresh)
    state = _failed_rows(failed)
    plan = _giant._plan(cat, state, max_flows=400, now=T0, cfg=CFG)
    assert plan["selected"] == failed[:20] + fresh
    assert plan["waiting"] == failed[20:] and plan["capped"] is False


def test_a_backed_off_failure_is_not_selected_through_redo_and_does_not_make_the_pass_capped():
    cat = _cat(["b1", "b2", "z1", "z2"])
    # b1 carries the catalogue vintage (it succeeded once): main's redo bucket would take it
    state = {"b1": {"status": "definitive_fail", "vintage": "v1", "fails": 2,
                    "failed_at": (T0 - dt.timedelta(hours=1)).isoformat()},
             "b2": {"status": "transient_fail", "fails": 1, "failed_at": (T0 - dt.timedelta(hours=1)).isoformat()}}
    plan = _giant._plan(cat, state, max_flows=2, now=T0, cfg=CFG)
    assert plan["selected"] == ["z1", "z2"] and plan["capped"] is False
    assert sorted(plan["backed_off"]) == ["b1", "b2"]


def test_one_classification_serves_the_capped_count_and_the_plan():
    cat = _cat(list("abcdefgh"), vintage="v2")
    state = {"a": {"vintage": "v2", "status": "ok"},                  # settled
             "b": {"vintage": "v1", "status": "ok"},                  # fresh
             "c": {"vintage": "v1", "status": "empty"},               # fresh
             "d": {"vintage": "v1", "status": "transient_fail"},      # failed
             "e": {"vintage": "v2", "status": "definitive_fail"},     # failed
             "f": {"vintage": "v2", "status": "empty"},               # redo
             "g": {"status": "too_big", "too_big_rows": 9e9, "too_big_ceiling": 1, "too_big_parser": 1}}
    want = {"a": "settled", "b": "fresh", "c": "fresh", "d": "failed", "e": "failed", "f": "redo",
            "g": "too_big", "h": "fresh"}
    assert {k: _giant._flow_class(cat[k], state.get(k)) for k in cat} == want
    assert _giant._beyond_the_cap(cat, state, []) == (3, 3)             # b c h | d e f; g neither
    plan = _giant._plan(cat, state, max_flows=400, now=T0, cfg=CFG)
    assert plan["selected"] == ["d", "e", "b", "c", "h", "f"] and plan["too_big"] == ["g"]


def test_back_off_grows_and_is_capped():
    hours = [_giant._backoff({"fails": k}).total_seconds() / 3600 for k in range(1, 9)]
    assert hours == [12, 24, 48, 96, 192, 384, 384, 384]


# ---- back-off, through run_giant -----------------------------------------------------------------

def test_a_flow_that_fails_waits_out_its_back_off(tmp_path, monkeypatch):
    now = _Now(monkeypatch)
    cat = _cat(["f1", "z1"])

    def fetch(fid):
        return (None, "transient") if fid == "f1" else _ok(fid)

    seen = []
    _run(tmp_path, cat, fetch, fetched=seen)
    row = _state(tmp_path)["f1"]
    assert row["status"] == "transient_fail" and row["fails"] == 1 and row["fail_kind"] == "transient"
    assert row["failed_vintage"] == "v1"
    for hours, want, fails in ((11, [], 1), (1, ["f1"], 2), (23, [], 2), (1, ["f1"], 3)):
        now.add(hours=hours)
        seen.clear()
        _run(tmp_path, cat, fetch, fetched=seen)
        assert seen == want, (hours, seen)
        assert _state(tmp_path)["f1"]["fails"] == fails


def test_an_old_ok_row_carrying_fails_starts_again_at_one(tmp_path, monkeypatch):
    """Old code keeps unknown keys: an `ok` row may still carry fails=5. Its next failure is the first."""
    now = _Now(monkeypatch)
    _seed(tmp_path, {"f1": {"status": "ok", "vintage": "v0", "fails": 5,
                            "failed_at": (T0 - dt.timedelta(days=30)).isoformat()}})
    _run(tmp_path, _cat(["f1"]), lambda fid: (None, "transient"))
    row = _state(tmp_path)["f1"]
    assert row["fails"] == 1 and row["fail_kind"] == "transient"
    seen = []
    now.add(hours=11)
    _run(tmp_path, _cat(["f1"]), lambda fid: (None, "transient"), fetched=seen)
    assert seen == [], "12 h after the first failure, not 16 days"
    now.add(hours=1)
    _run(tmp_path, _cat(["f1"]), lambda fid: (None, "transient"), fetched=seen)
    assert seen == ["f1"]


def test_a_success_clears_the_failure_keys(tmp_path, monkeypatch):
    now = _Now(monkeypatch)
    _seed(tmp_path, {"f1": {"status": "transient_fail", "fails": 3, "fail_kind": "transient",
                            "failed_at": (T0 - dt.timedelta(days=10)).isoformat()}})
    _run(tmp_path, _cat(["f1"]), _ok)
    row = _state(tmp_path)["f1"]
    assert row["status"] == "ok" and row["vintage"] == "v1"
    assert not set(row) & set(_giant._FAILURE_KEYS + _giant._TOO_BIG_KEYS), row


# ---- the slice's two bounds, and no starvation ------------------------------------------------------

def test_the_slice_stops_at_its_time_limit_and_leaves_the_rest_due_and_untouched(tmp_path, monkeypatch, capsys):
    _Now(monkeypatch)
    failed = [f"a{i}" for i in range(6)]
    _seed(tmp_path, _failed_rows(failed))
    ticks = iter(range(0, 10_000, 50))                       # every _clock() call moves 50 s
    monkeypatch.setattr(_giant, "_clock", lambda: next(ticks))
    seen = []
    res = _run(tmp_path, _cat(failed + ["z1"]), lambda fid: (None, "structural") if fid != "z1" else _ok(fid),
               fetched=seen)
    out = capsys.readouterr().out
    # each slice flow reads the clock 4 times (start, save start, save end, end): 150 s, of which the
    # save is 50 s; two flows reach the 300 s limit
    assert seen == ["a0", "a1", "z1"], seen
    st = _state(tmp_path)
    assert all(st[f] == {"status": "definitive_fail"} for f in failed[2:]), "untouched, still due"
    assert "4 due failed flow(s) wait for a later retry slice (4 stopped by its 300 s limit)" in res.error
    assert "[zzgiant] retry a0: definitive_fail (structural), 150.0s including the state save of 50.0s" in out


def test_no_due_failed_flow_waits_more_than_ceil_due_over_20_passes(tmp_path, monkeypatch):
    now = _Now(monkeypatch)
    failed = [f"a{i:02d}" for i in range(45)]
    _seed(tmp_path, _failed_rows(failed))
    turns = {f: 0 for f in failed}
    for _ in range(3):                                       # ceil(45 / 20) = 3
        seen = []
        _run(tmp_path, _cat(failed), lambda fid: (None, "transient"), fetched=seen)
        for f in seen:
            turns[f] += 1
        now.add(minutes=10)
    assert set(turns.values()) == {1}, turns


def test_the_retry_slice_flow_is_saved_before_its_fetch(tmp_path, monkeypatch):
    _Now(monkeypatch)
    _seed(tmp_path, _failed_rows(["a1"]))
    saved = []
    real = _giant.save_state

    def spy(d, state):
        saved.append(json.loads(json.dumps(state.get("a1"))))
        return real(d, state)

    monkeypatch.setattr(_giant, "save_state", spy)

    at_fetch = []

    def fetch(fid):
        # recorded here and asserted below: an assert raised inside fetch_flow is caught by run_giant
        at_fetch.append(list(saved))
        return None, "transient"

    _run(tmp_path, _cat(["a1"]), fetch)
    assert at_fetch and at_fetch[0], "the state was saved before the fetch"
    assert at_fetch[0][-1]["fail_kind"] == "interrupted" and at_fetch[0][-1]["fails"] == 2


# ---- F2: a flow the limit interrupts is recorded, and backs off ------------------------------------

@pytest.mark.parametrize("how", ["unit_timeout", "too_big_after_the_alarm"])
def test_a_flow_the_limit_interrupts_is_recorded_and_not_first_next_pass(tmp_path, monkeypatch, how):
    now = _Now(monkeypatch)
    cat = _cat(["f1", "f2", "f3"])

    def fetch(fid):
        if fid == "f2":
            if how == "unit_timeout":
                orchestrate._deliver_alarm("zz/_all", 45.0)
            orchestrate.UNIT_TIMEOUT_FIRED = True
            raise eurostat.TooBigForRunner("gzip body holds ~20,141,824 rows", rows=20_141_824)
        return _ok(fid)

    res = _run(tmp_path, cat, fetch)
    assert res.status == "transient_fail" and sorted(res.changed_keys) == ["f1"]
    row = _state(tmp_path)["f2"]
    assert row["status"] == "transient_fail" and row["fail_kind"] == "interrupted"
    assert row["fails"] == 1 and row["failed_vintage"] == "v1"
    orchestrate.UNIT_TIMEOUT_FIRED = False
    now.add(hours=1)
    seen = []
    _run(tmp_path, cat, _ok, fetched=seen)
    assert seen == ["f3"], "f2 waits out its back-off; it is not first in line again"


def test_an_interrupted_flow_that_keeps_failing_is_named_as_outlasting_the_limit(tmp_path, monkeypatch):
    _Now(monkeypatch)
    _seed(tmp_path, {"f9": {"status": "transient_fail", "fail_kind": "interrupted", "fails": 3,
                            "failed_at": T0.isoformat()}})
    res = _run(tmp_path, _cat(["f9", "z1"]), _ok)
    assert "outlasts the limit: 1 [f9]" in res.error


# ---- F4: too big for the runner ------------------------------------------------------------------

def test_a_body_over_the_ceiling_is_booked_too_big_and_not_fetched_again(tmp_path, monkeypatch, capsys):
    now = _Now(monkeypatch)
    _seed(tmp_path, {"p1": {"status": "ok", "vintage": "v0", "last_obs_date": "2100-12-31"}})

    def fetch(fid):
        return ({"rows": 30_511_872}, "too_big") if fid == "p1" else _ok(fid)

    res = _run(tmp_path, _cat(["p1", "z1"]), fetch)
    row = _state(tmp_path)["p1"]
    assert row["status"] == "too_big" and row["fail_kind"] == "too_big"
    assert row["vintage"] == "v0", "C1: vintage untouched - the stored data is NOT that upstream version"
    assert row["too_big_vintage"] == "v1" and row["too_big_rows"] == 30_511_872
    assert row["too_big_ceiling"] == 20_000_000 and row["too_big_parser"] == 1
    assert res.status == "partial" and "p1" not in (res.changed_keys or {})
    assert "too big for the runner (not fetched, not served): 1 [p1]" in res.error
    capsys.readouterr()
    for vintage in ("v1", "v2"):                             # not even when the publisher republishes
        now.add(days=20)
        seen = []
        res = _run(tmp_path, _cat(["p1", "z1"], vintage=vintage), fetch, fetched=seen)
        assert "p1" not in seen and res.status == "partial"
        assert "too big for the runner (not fetched, not served): 1 [p1]" in capsys.readouterr().out


def test_a_too_big_row_is_still_selected_by_the_ordering_without_the_opt_in():
    """C1, rollback: code without the booking (select_flows, as on main) fetches the row again and
    fails loudly, instead of skipping it as current and returning ok."""
    cat = _cat(["p1"])
    for row in ({"status": "too_big", "too_big_vintage": "v1"},
                {"status": "too_big", "vintage": "v0", "too_big_vintage": "v1"}):
        assert _giant.select_flows(cat, {"p1": row})[0] == ["p1"]


def test_a_too_big_row_comes_back_when_the_ceiling_covers_it_or_the_parser_changes():
    cat = _cat(["p1"])
    row = {"status": "too_big", "too_big_rows": 30_511_872, "too_big_ceiling": 20_000_000,
           "too_big_parser": 1, "too_big_vintage": "v1"}
    plan = lambda cfg: _giant._plan(cat, {"p1": row}, max_flows=400, now=T0, cfg=cfg)  # noqa: E731
    assert plan(CFG)["selected"] == [] and plan(CFG)["too_big"] == ["p1"]
    assert plan({"ceiling": 25_000_000, "parser": 1})["selected"] == [], "still over the ceiling"
    assert plan({"ceiling": 40_000_000, "parser": 1})["selected"] == ["p1"]
    assert plan({"ceiling": 20_000_000, "parser": 2})["selected"] == ["p1"]
    no_size = dict(row, too_big_rows=None)
    assert _giant._plan(cat, {"p1": no_size}, max_flows=400, now=T0,
                        cfg={"ceiling": 20_000_001, "parser": 1})["selected"] == ["p1"]


def test_eurostat_books_a_body_over_its_ceiling_as_too_big_and_names_the_flow(monkeypatch, capsys):
    head = "DATAFLOW,LAST UPDATE,freq,geo,TIME_PERIOD,OBS_VALUE\n"
    body = (head + "".join(f"ESTAT:ZZ(1.0),01/01/26 11:00:00,A,G{i},2024,{i}\n" for i in range(5))).encode()
    monkeypatch.setattr(eurostat, "http_get", lambda *a, **k: body)
    monkeypatch.setattr(eurostat, "MAX_FLOW_ROWS", 3)
    out, status = eurostat.fetch_flow("proj_19rp3", {}, None, None)
    assert status == "too_big" and out == {"rows": 5}
    assert "[eurostat] proj_19rp3: too big for the runner" in capsys.readouterr().out


def test_eurostat_passes_its_ceiling_and_parser_version():
    src = open(eurostat.__file__, encoding="utf-8").read()
    assert 'retry_slice={"ceiling": MAX_FLOW_ROWS, "parser": PARSE_VERSION}' in src


# ---- through run_once: F1 and the CSV step --------------------------------------------------------

class _Strategy:
    def __init__(self, catalog, fetch):
        self.catalog, self.fetch = catalog, fetch

    def is_due(self, unit, us):
        return True

    def detect_change(self, unit, us):
        return "cat:test"

    def run(self, unit, since=None):
        return _giant.run_giant(unit, source=unit.source_id, fetch_catalog=lambda: dict(self.catalog),
                                fetch_flow=lambda fid, meta, since, s: self.fetch(fid), csv_accept="text/csv",
                                rate=0, timeout=1, report_changed_flows=True, bounded_merge=True,
                                checkpoint_every=10, retry_slice=CFG)


class _Blob:
    def __init__(self):
        self.keys = []

    def put_atomic(self, key, data):
        self.keys.append(key)


class _Res:
    def __init__(self, path):
        self.parquet_path = path


@pytest.fixture
def rig(tmp_path, monkeypatch):
    import core.derive_csv as dc
    from econdl import _resolve

    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_BUDGET_MIN", "AQUEDUCT_UNIT_TIMEOUT_MIN",
              "AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_FLOW_DERIVE_MAX_ROWS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    data = tmp_path / "data"                     # every default path of the code under test, moved here
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    os.makedirs(data / "_aqueduct")
    store_dir = data / "clean_full" / "eurostat"
    os.makedirs(store_dir)
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [(f"eurostat:{f}", "eurostat") for f in ("f1", "f2", "p1", "z1", "z2")])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: _Res(
        str(store_dir / (sid.split(":", 1)[1].upper() + ".parquet"))))
    monkeypatch.setattr(dc, "resolved_paths", lambda res: [res.parquet_path])

    def stream(sid, out_path):
        pq.read_table(_resolve.resolve(sid).parquet_path)
        with open(out_path, "wb") as fh:
            fh.write(gzip.compress(b"series_id,obs_date,value\n", mtime=0))
        return os.path.getsize(out_path)

    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", stream)
    state = StateStore(str(data / "_aqueduct" / "state.db"))
    blob = _Blob()

    def run(catalog, fetch):
        blob.keys.clear()
        strat = _Strategy(catalog, fetch)
        monkeypatch.setattr(orchestrate, "get_strategy", lambda name: strat)
        return orchestrate.run_once(sources=["eurostat"], store=state, blob=blob)

    yield {"run": run, "state": state, "blob": blob, "store": store_dir}
    state.close()


def _due_an_hour_later(state):
    row = state.get_unit("eurostat", "_all")
    unit = next(u for u in registry.all_units() if u.source_id == "eurostat")
    later = dt.datetime.fromisoformat(row["last_attempt_utc"]) + dt.timedelta(hours=1)
    return get_strategy(unit.strategy).is_due(unit, row, now=later)


def test_backed_off_failures_keep_a_clean_pass_from_reading_ok(rig, monkeypatch):
    """F1: every fresh flow succeeds and the failed ones wait out their back-off. finalize alone would
    say ok: last_success set and the 28-day monthly clock while flows fail."""
    _Now(monkeypatch)
    _seed(rig["store"], {"f1": {"status": "definitive_fail", "fails": 1, "failed_at": T0.isoformat()},
                         "f2": {"status": "transient_fail", "fails": 2, "failed_at": T0.isoformat()}})
    results = rig["run"](_cat(["f1", "f2", "z1", "z2"]), _ok)
    assert results == [(UNIT_KEY, "partial")]
    row = rig["state"].get_unit("eurostat", "_all")
    assert row["status"] == "partial" and row["last_success_utc"] is None and row["upstream_vintage"] is None
    assert "retry note: 2 failed flow(s) wait out their back-off" in row["last_error"]
    assert not health._deferral_only([row]), "never read as a healthy rotation"
    assert sorted(k.split("%3A")[-1].split(".")[0] for k in rig["blob"].keys) == ["z1", "z2"]
    assert _due_an_hour_later(rig["state"]) is False, "the partial clock, not the monthly one"


def test_a_too_big_flow_is_never_a_success_and_gets_no_csv(rig, monkeypatch):
    _Now(monkeypatch)

    def fetch(fid):
        return ({"rows": 91_535_616}, "too_big") if fid == "p1" else _ok(fid)

    results = rig["run"](_cat(["p1", "z1"]), fetch)
    assert results == [(UNIT_KEY, "partial")]
    row = rig["state"].get_unit("eurostat", "_all")
    assert row["last_success_utc"] is None
    assert "too big for the runner (not fetched, not served): 1 [p1]" in row["last_error"]
    assert [k for k in rig["blob"].keys if "p1" in k] == []
    assert _state(rig["store"])["p1"]["status"] == "too_big"


def test_a_clean_pass_with_nothing_waiting_is_still_ok(rig, monkeypatch):
    """The control for F1: with no failed or too_big flow waiting, the pass is ok and carries no note."""
    _Now(monkeypatch)
    results = rig["run"](_cat(["z1", "z2"]), _ok)
    assert results == [(UNIT_KEY, "ok")]
    assert "retry note" not in (rig["state"].get_unit("eurostat", "_all")["last_error"] or "")


# ---- callers without the opt-in -------------------------------------------------------------------

def test_oecd_has_no_retry_slice_and_keeps_its_ordering(tmp_path, monkeypatch):
    monkeypatch.setattr(oecd, "RATE", 0)
    cat = _cat(["a1", "z1"])
    _seed(tmp_path, {"a1": {"status": "definitive_fail", "fails": 3,
                            "failed_at": dt.datetime.now(dt.timezone.utc).isoformat()}})
    monkeypatch.setattr(oecd, "fetch_catalog", lambda: dict(cat))
    seen = []
    monkeypatch.setattr(oecd, "fetch_flow",
                        lambda fid, meta, since, s: seen.append(fid) or ((None, "structural") if fid == "a1"
                                                                         else _ok(fid)))
    with pytest.raises(_giant.DefinitiveError):
        oecd.update(_Unit(tmp_path), None)
    assert seen == ["a1", "z1"], "a backed-off failure is fetched first, as before"
    row = _state(tmp_path)["a1"]
    assert row["status"] == "definitive_fail" and "fail_kind" not in row and row["fails"] == 3


def test_retry_slice_needs_the_changed_set_and_checkpoints(tmp_path):
    with pytest.raises(ValueError, match="retry_slice requires"):
        _giant.run_giant(_Unit(tmp_path), source="zz", fetch_catalog=lambda: _cat(["a"]),
                         fetch_flow=lambda *a: _ok("a"), csv_accept="", rate=0, timeout=1, retry_slice=CFG)
