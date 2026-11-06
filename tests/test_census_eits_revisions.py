"""census EITS: a publisher revision must reach the store (ledger R1830, R1819; review AR-449).

Measured 2026-11-06 against the 2026-10-27 store (census.py's own request path, F:/work/eits_measure,
re-derived independently by AR-449):
  * CURRENT YEAR: `time=from <boundary year>` fetched every month of the year, and census.py's row
    filter `d < mx` then threw away every month before the stored max - marts 652, ftd 30 and resconst
    67 rows that differed from the API, never merged and never documented.
  * PRIOR YEARS: never requested at all (marts 2021 929/2,016 rows differ, 2024 1,099/2,016).
These tests drive census.update() with a fake Census API over a local store, offline, clock pinned.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import _census_retired as R  # noqa: E402
from updater.strategies.fetchers import _census_revision as rv  # noqa: E402
from updater.strategies.fetchers import census as cs  # noqa: E402
from tests.test_census_window_cap import _ingest_stub, _Resp, _Sess  # noqa: E402

UNIT = types.SimpleNamespace(source_id="census", unit_id="_all")
TODAY = dt.date(2026, 11, 5)
HDR = ["cell_value", "category_code", "program_code", "geo_level_code", "time_slot_id", "time", "us"]
CATS = ("C1", "C2")


def _months(first, last):
    y, m = int(first[:4]), int(first[5:7])
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def _v(month, cat):
    return str(100 + int(month[2:4]) * 20 + int(month[5:7]) + (5 if cat == "C2" else 0))


def _key(flow, cat):
    return f"{flow}|category_code={cat}|program_code=PX|geo_level_code=US|us=1"


class _API:
    """A Census-shaped EITS API: `time=from YYYY`, `time=YYYY` or `time=YYYY-MM`."""

    def __init__(self):
        self.data = {}                 # (flow, month, cat) -> value served
        self.extra = []                # (flow, row) appended to every answer covering that row's month
        self.calls = []

    def serve(self, flow, first, last):
        for mo in _months(first, last):
            for c in CATS:
                self.data[(flow, mo, c)] = _v(mo, c)

    def get(self, url, params=None, timeout=None):
        flow = url.split("/timeseries/", 1)[1]
        tv = dict(params or {})["time"]
        self.calls.append((flow, tv))
        if tv.startswith("from "):
            ok = lambda mo: mo[:4] >= tv[5:]                       # noqa: E731
        elif len(tv) == 4:
            ok = lambda mo: mo[:4] == tv                           # noqa: E731
        else:
            ok = lambda mo: mo == tv                               # noqa: E731
        rows = [[v, c, "PX", "US", "0", mo, "1"]
                for (f, mo, c), v in sorted(self.data.items()) if f == flow and ok(mo)]
        rows += [r for f, r in self.extra if f == flow and ok(r[5])]
        return _Resp(200, [HDR] + rows) if rows else _Resp(204)


def _write(d, flow, first, last, dup=None):
    rows = [(mo, c) for mo in _months(first, last) for c in CATS]
    if dup:
        rows.append(dup)                                      # a second stored row under one key
    pq.write_table(pa.table({
        "series_key": [_key(flow, c) for _mo, c in rows],
        "obs_date": pa.array([dt.date(int(mo[:4]), int(mo[5:]), 1) for mo, _c in rows], pa.date32()),
        "cell_value": [_v(mo, c) for mo, c in rows], "category_code": [c for _mo, c in rows],
        "program_code": ["PX"] * len(rows), "geo_level_code": ["US"] * len(rows),
        "time_slot_id": ["0"] * len(rows), "time": [mo for mo, _c in rows], "us": ["1"] * len(rows)}),
        os.path.join(d, cs._store_name(flow) + ".parquet"))


def _store(d, flow):
    t = pq.read_table(os.path.join(d, cs._store_name(flow) + ".parquet")).to_pylist()
    return [(r["series_key"].split("category_code=")[1][:2], str(r["obs_date"]), r["cell_value"])
            for r in t]


def _val(d, flow, cat, day):
    vals = [v for c, o, v in _store(d, flow) if c == cat and o == day]
    assert len(vals) == 1, (cat, day, vals)
    return vals[0]


@pytest.fixture
def env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    out = data / "clean_full" / "census"
    out.mkdir(parents=True)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    for v in ("AQUEDUCT_BUDGET_MIN_OVERRIDE", "AQUEDUCT_CENSUS_MARGIN_MIN", R.ENV_ACTIVATE,
              rv.ENV_ACTIVATE, rv.ENV_ACTIVATE_EITS, "CENSUS_REVISION_BUDGET_MIN",
              "CENSUS_REVISION_PROBE_DAYS", "CENSUS_REVISION_START_MMDD"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(R, "today_utc", lambda: TODAY)
    monkeypatch.setattr(cs.config, "source_dir", lambda s: str(out))
    monkeypatch.setattr(rv, "staging_root", lambda: str(data / "census_revision_staging"))
    monkeypatch.setattr(cs, "_ingest", _ingest_stub)
    monkeypatch.setattr(cs, "_api_key", lambda: "test-key-not-real")
    monkeypatch.setattr(cs.time, "sleep", lambda s: None)
    monkeypatch.setattr(cs, "RATE", 0.0)
    api = _API()
    monkeypatch.setattr(cs.requests, "Session", lambda: _Sess(api))
    from updater import orchestrate as O
    monkeypatch.setattr(O, "_RUN_DEADLINE_TS", None)
    return types.SimpleNamespace(d=str(out), data=str(data), api=api, mp=monkeypatch, O=O)


def _owed(env):
    p = os.path.join(env.d, cs.OWED_FILE)
    return json.load(open(p)) if os.path.exists(p) else {}


# ---- (a) the current year: every month the request fetched is merged ----------------------------

def test_a_revised_february_before_the_stored_max_is_merged(env, capsys):
    _write(env.d, "eits/fa", "2026-01", "2026-03")
    env.api.serve("eits/fa", "2026-01", "2026-03")
    for c in CATS:
        env.api.data[("eits/fa", "2026-02", c)] = "999.5"         # the publisher revised February
    res = cs.update(UNIT, None)
    # replaced IN PLACE: one row per key, the revised value, nothing else touched
    assert _val(env.d, "eits/fa", "C1", "2026-02-01") == "999.5"
    assert _val(env.d, "eits/fa", "C2", "2026-02-01") == "999.5"
    assert _val(env.d, "eits/fa", "C1", "2026-01-01") == _v("2026-01", "C1")
    assert len(_store(env.d, "eits/fa")) == 6
    # a revision is a change: cursored, handed to the CSV step, booked as a revision
    assert res.status == "ok" and res.series_cursors == {"eits__fa": "2026-03-01"}, res
    assert env.O._should_derive_csvs(res.status, res.changed_keys)
    assert "revised stored values" in (res.error or ""), res.error
    assert _owed(env) == {}, "the hand-over settled the mark"
    out = capsys.readouterr().out
    assert ("eits/fa: window>=2026-01-01 changed=2 added=0 held=0 (held = stored rows the fetch did "
            "not return, kept); not merged: empty-measure=0 unknown-shape=0 new-series=0") in out, out
    assert "eits/fa: RECEIPT window>=2026-01-01 returned=6 skipped_new=0 stored=6 MATCH" in out, out


def test_without_the_floor_the_same_revision_is_dropped(env):
    """THE MUTANT, so the test above is shown able to fail: the pre-fix floor (the stored max)."""
    env.mp.setattr(cs, "_tail_floor", lambda flow, mx: mx)
    _write(env.d, "eits/fa", "2026-01", "2026-03")
    env.api.serve("eits/fa", "2026-01", "2026-03")
    for c in CATS:
        env.api.data[("eits/fa", "2026-02", c)] = "999.5"
    res = cs.update(UNIT, None)
    assert _val(env.d, "eits/fa", "C1", "2026-02-01") == _v("2026-02", "C1")    # never merged
    assert res.status == "no_change" and not res.series_cursors, res


def test_the_floor_is_the_first_period_the_request_asks_for():
    assert cs._time_windows("eits/fa", dt.date(2026, 3, 1)) == ["from 2026"]
    assert cs._tail_floor("eits/fa", dt.date(2026, 3, 1)) == dt.date(2026, 1, 1)
    assert cs._tail_floor("eits/qfr", dt.date(2026, 4, 1)) == dt.date(2026, 1, 1)
    # intltrade asks from its boundary month and still merges from it
    assert cs._tail_floor("intltrade/exports/hs", dt.date(2026, 3, 1)) == dt.date(2026, 3, 1)


def test_an_identical_whole_year_refetch_is_no_change(env):
    _write(env.d, "eits/fa", "2026-01", "2026-03")
    env.api.serve("eits/fa", "2026-01", "2026-03")
    res = cs.update(UNIT, None)
    assert res.status == "no_change" and not res.series_cursors and res.merged_rows == 0, res
    assert _owed(env) == {}


def test_a_stored_month_the_fetch_omits_is_kept_and_counted_as_held(env, capsys):
    _write(env.d, "eits/fa", "2026-01", "2026-03")
    env.api.serve("eits/fa", "2026-01", "2026-03")
    del env.api.data[("eits/fa", "2026-01", "C2")]
    env.api.data[("eits/fa", "2026-01", "C1")] = "1.0"
    res = cs.update(UNIT, None)
    assert _val(env.d, "eits/fa", "C2", "2026-01-01") == _v("2026-01", "C2")   # never deleted
    assert _val(env.d, "eits/fa", "C1", "2026-01-01") == "1.0"
    assert res.series_cursors == {"eits__fa": "2026-03-01"}, res
    assert "eits/fa: window>=2026-01-01 changed=1 added=0 held=1" in capsys.readouterr().out


def test_a_key_collision_in_the_fetch_is_refused(env):
    _write(env.d, "eits/fa", "2026-01", "2026-03")
    env.api.serve("eits/fa", "2026-01", "2026-03")
    env.api.data[("eits/fa", "2026-02", "C1")] = "999.5"
    # the publisher answers TWO rows for one (series_key, obs_date), with different values
    env.api.extra.append(("eits/fa", ["777.0", "C1", "PX", "US", "0", "2026-02", "1"]))
    res = cs.update(UNIT, None)
    assert _val(env.d, "eits/fa", "C1", "2026-02-01") == _v("2026-02", "C1"), "nothing merged"
    assert res.status == "partial" and not res.series_cursors, res
    assert "merge refused - key" in res.error and "not unique" in res.error, res.error
    assert "1 duplicate fetched row(s)" in res.error, res.error
    assert _owed(env) == {}, "no mark is left for a merge that never ran"


def test_a_key_collision_in_the_stored_window_is_refused(env):
    _write(env.d, "eits/fa", "2026-01", "2026-03", dup=("2026-02", "C2"))
    env.api.serve("eits/fa", "2026-01", "2026-03")
    env.api.data[("eits/fa", "2026-02", "C1")] = "999.5"
    before = sorted(_store(env.d, "eits/fa"))
    res = cs.update(UNIT, None)
    assert sorted(_store(env.d, "eits/fa")) == before, "the store is untouched"
    assert res.status == "partial" and "1 duplicate stored row(s)" in res.error, res


# ---- (b) prior years: the annual re-pull, EITS families, OFF by default ------------------------

def _prior_calls(env):
    return [t for _f, t in env.api.calls if not t.startswith("from ")]


def _revise_2024(env, flow):
    for mo in _months("2024-01", "2024-12"):
        for c in CATS:
            env.api.data[(flow, mo, c)] = f"{mo}{c}r"


def test_eits_prior_years_are_never_requested_by_default(env):
    _write(env.d, "eits/fa", "2024-01", "2026-03")
    env.api.serve("eits/fa", "2024-01", "2026-03")
    _revise_2024(env, "eits/fa")
    res = cs.update(UNIT, None)
    assert _prior_calls(env) == [], env.api.calls
    assert _val(env.d, "eits/fa", "C1", "2024-05-01") == _v("2024-05", "C1")
    assert not os.path.exists(os.path.join(env.d, rv.STATE_FILE))
    assert rv.OPERATOR_ACTIVATED_EITS_ON is None and rv.OPERATOR_ACTIVATED_ON is None
    assert res.status == "no_change", res


def test_the_intltrade_switch_does_not_arm_the_eits_re_pull(env):
    env.mp.setenv(rv.ENV_ACTIVATE, "2026-01-01")
    _write(env.d, "eits/fa", "2024-01", "2026-03")
    env.api.serve("eits/fa", "2024-01", "2026-03")
    _revise_2024(env, "eits/fa")
    cs.update(UNIT, None)
    assert _prior_calls(env) == [], env.api.calls


def test_with_its_switch_on_a_revised_prior_year_is_re_pulled_and_handed_over(env):
    env.mp.setenv(rv.ENV_ACTIVATE_EITS, "2026-01-01")
    _write(env.d, "eits/fa", "2024-01", "2026-03")
    _write(env.d, "eits/fb", "2025-01", "2026-03")
    env.api.serve("eits/fa", "2024-01", "2026-03")
    env.api.serve("eits/fb", "2025-01", "2026-03")
    _revise_2024(env, "eits/fa")
    res = cs.update(UNIT, None)
    # year pieces only, and only the years the store holds inside the window (2021-2025): the
    # pieces (fa 2024, 2025; fb 2025), each family's verify re-fetch and its post-publish sentinel
    calls = _prior_calls(env)
    assert set(calls) == {"2024", "2025"}, env.api.calls
    assert sorted(calls) == ["2024"] * 3 + ["2025"] * 4, env.api.calls
    assert _val(env.d, "eits/fa", "C1", "2024-05-01") == "2024-05C1r"
    assert _val(env.d, "eits/fa", "C2", "2025-05-01") == _v("2025-05", "C2")
    assert len(_store(env.d, "eits/fa")) == 2 * 27, "replaced in place, no row added"
    assert "eits__fa" in (res.series_cursors or {}), res
    fams = json.load(open(os.path.join(env.d, rv.STATE_FILE)))["families"]
    assert sorted(fams) == ["eits-fa", "eits-fb"], fams              # one family per EITS flow
    assert fams["eits-fa"]["phase"] == "done", fams["eits-fa"]
    assert fams["eits-fa"]["counts"]["eits/fa"]["2024"]["changed"] == 24, fams["eits-fa"]
    assert "2025" not in fams["eits-fa"]["counts"]["eits/fa"], fams["eits-fa"]   # no difference there


def test_an_eits_flow_with_nothing_stored_in_the_window_asks_nothing(env):
    env.mp.setenv(rv.ENV_ACTIVATE_EITS, "2026-01-01")
    _write(env.d, "eits/mhs", "2014-01", "2014-10")                  # retired survey
    env.api.serve("eits/mhs", "2014-01", "2014-10")
    res = cs.update(UNIT, None)
    assert _prior_calls(env) == [], env.api.calls
    fams = json.load(open(os.path.join(env.d, rv.STATE_FILE)))["families"]
    assert fams["eits-mhs"]["phase"] == "done"
    assert "nothing to re-pull" in (res.error or ""), res.error


def test_family_and_piece_helpers():
    assert rv.family_of("eits/marts") == "eits-marts"
    assert rv.family_of("intltrade/imports/hitech") == "imports"
    assert rv.family_of("eits") is None
    assert rv.sentinel_of("eits-marts") == "eits/marts"
    assert rv.sentinel_of("imports") == "intltrade/imports/hitech"
    assert rv.window_pieces("eits-marts", 2026) == ["2021", "2022", "2023", "2024", "2025"]
    assert rv.window_pieces("imports", 2026)[0] == "2023-01"
    assert rv._month_bounds("2024") == (dt.date(2024, 1, 1), dt.date(2025, 1, 1))
    assert rv._month_bounds("2024-12") == (dt.date(2024, 12, 1), dt.date(2025, 1, 1))
