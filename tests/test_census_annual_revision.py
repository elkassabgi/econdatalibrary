"""census intltrade: the annual revision re-pull (ledger R1819; design review AR-444).

The tail only walks forward from each flow's newest stored month, so a publisher restatement of a
complete prior year never reached the store (imports/hitech 2023-01..2025-12, AR-442). These tests drive
census.update() with a fake Census API and a local store, offline, and pin the clock.
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
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import blob  # noqa: E402
from updater.strategies.fetchers import _census_retired as R  # noqa: E402
from updater.strategies.fetchers import _census_revision as rv  # noqa: E402
from updater.strategies.fetchers import census as cs  # noqa: E402

UNIT = types.SimpleNamespace(source_id="census", unit_id="_all")
TODAY = dt.date(2026, 11, 5)
HT = "intltrade/imports/hitech"
EU = "intltrade/imports/enduse"
SH = "intltrade/imports/statehs"
BETA = "intltrade/imports/hitechimport"
DIM = {HT: "HITECH", EU: "I_ENDUSE", SH: "I_COMMODITY", BETA: "HITECH"}
STATES = ("01", "02", "06")


def _ingest_stub():
    def series_key_of(row, hidx, dim_cols, path):
        parts = [path.replace("timeseries/", "")]
        for c in dim_cols:
            i = hidx.get(c)
            if i is not None and i < len(row) and row[i] not in (None, ""):
                parts.append(f"{c}={row[i]}")
        return "|".join(parts)

    def parse_obs_date(d):
        y, m = str(d).split("-")[:2]
        return dt.date(int(y), int(m), 1)
    return types.SimpleNamespace(series_key_of=series_key_of, parse_obs_date=parse_obs_date)


def _months(first, last):
    y, m = int(first[:4]), int(first[5:7])
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


DELTA = {"old": 0, "new": 7, "mid": 3, "new2": 9}


def _val(flow, month, code, vintage, state=None):
    base = 1000 + int(month[2:4]) * 100 + int(month[5:7]) + int(code) + (int(state) if state else 0)
    return str(base + (DELTA[vintage] if month[:4] in ("2023", "2024", "2025") else 0))


def _rows(flow, month, vintage, codes=("01", "02")):
    d = DIM[flow]
    if flow == SH:
        hdr = [d, "STATE", "CTY_CODE", "GEN_VAL_MO", "time"]
        return hdr, [[c, s, "-", _val(flow, month, c, vintage, s), month] for c in codes for s in STATES]
    hdr = [d, "CTY_CODE", "GEN_VAL_MO", "time"]
    return hdr, [[c, "-", _val(flow, month, c, vintage), month] for c in codes]


class _Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.text = "" if body is None else json.dumps(body)
        self.headers = {"Content-Type": "application/json"}

    def json(self):
        return self._body


class _API:
    def __init__(self):
        self.vintage = "new"          # old = what the store holds; new = 2023-2025 restated
        self.last = "2026-01"         # newest month published
        self.drop = set()             # (flow, month, code) the publisher no longer serves
        self.empty = set()            # (flow, month) answered 204
        self.label = {}               # (flow, month) -> HITECH_DESC value served
        self.extra = set()            # (flow, month): also serve a row the store lacks (CTY_CODE 5700)
        self.refuse = set()           # flows whose months before 2026-01 answer HTTP 400
        self.refuse_from = {}         # (flow, month) -> answer HTTP 400 from this call number on
        self.calls = []
        self.on_call = None

    def get(self, url, params=None, timeout=None):
        flow = url.split("/timeseries/", 1)[1]
        p = dict(params or {})
        tv = p["time"]
        self.calls.append((flow, tv, p.get("STATE")))
        if self.on_call:
            self.on_call(self, flow, tv)
        n_asked = sum(1 for c in self.calls if c[:2] == (flow, tv))
        if (flow, tv) in self.refuse_from and n_asked >= self.refuse_from[(flow, tv)]:
            return _Resp(400, ["error: unknown variable"])
        if flow in self.refuse and tv < "2026-01":
            return _Resp(400, ["error: unknown variable"])
        if tv > self.last or (flow, tv) in self.empty or flow not in DIM:
            return _Resp(204)
        hdr, rows = _rows(flow, tv, self.vintage)
        rows = [r for r in rows if (flow, tv, r[0]) not in self.drop]
        if flow == SH and p.get("STATE"):
            rows = [r for r in rows if r[1] == p["STATE"]]
        if (flow, tv) in self.extra:
            rows = rows + [[rows[0][0], "5700", "1", tv]]       # a known series, a new CTY_CODE
        if (flow, tv) in self.label:
            hdr = hdr[:-1] + ["HITECH_DESC", "time"]
            rows = [r[:-1] + [self.label[(flow, tv)], r[-1]] for r in rows]
        elif flow == HT and "HITECH_DESC" in (p.get("get") or ""):
            hdr = hdr[:-1] + ["HITECH_DESC", "time"]
            rows = [r[:-1] + [f"desc{r[0]}", r[-1]] for r in rows]
        return _Resp(200, [hdr] + rows)


class _Sess:
    def __init__(self, api):
        self.api = api
        self.headers = {}

    def get(self, url, params=None, timeout=None):
        return self.api.get(url, params=params, timeout=timeout)


def _write(d, flow, first, last, label=False):
    hdr0, _ = _rows(flow, first, "old")
    cols = {c: [] for c in hdr0}
    keys, dates = [], []
    for mo in _months(first, last):
        hdr, rows = _rows(flow, mo, "old")
        for r in rows:
            row = dict(zip(hdr, r))
            k = f"{flow}|{DIM[flow]}={row[DIM[flow]]}"
            if flow == SH:
                k += f"|STATE={row['STATE']}"
            keys.append(k)
            dates.append(dt.date(int(mo[:4]), int(mo[5:]), 1))
            for c in hdr0:
                cols[c].append(row[c])
    arrays = {"series_key": keys, "obs_date": pa.array(dates, pa.date32())}
    arrays.update(cols)
    if label:
        arrays["HITECH_DESC"] = [f"desc{v}" for v in cols["HITECH"]]
    pq.write_table(pa.table(arrays), os.path.join(d, cs._store_name(flow) + ".parquet"))


def _store(d, flow):
    t = pq.read_table(os.path.join(d, cs._store_name(flow) + ".parquet")).to_pylist()
    return {(r["series_key"], str(r["obs_date"])): r for r in t}


class _Clock:
    """Advances `step` seconds on every reading, so a budget in seconds is a count of readings."""

    def __init__(self, step=0.0):
        self.t = 1_000_000.0
        self.step = step

    def time(self):
        self.t += self.step
        return self.t

    def sleep(self, s):
        pass


@pytest.fixture
def env(tmp_path, monkeypatch):
    data = tmp_path / "data"
    out = data / "clean_full" / "census"
    out.mkdir(parents=True)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    for v in ("AQUEDUCT_BUDGET_MIN_OVERRIDE", "AQUEDUCT_CENSUS_MARGIN_MIN", R.ENV_ACTIVATE,
              rv.ENV_ACTIVATE, "CENSUS_REVISION_BUDGET_MIN", "CENSUS_REVISION_PROBE_DAYS",
              "CENSUS_REVISION_START_MMDD"):
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
    return types.SimpleNamespace(d=str(out), data=str(data), api=api, mp=monkeypatch)


def _on(env):
    env.mp.setenv(rv.ENV_ACTIVATE, "2026-01-01")


def _state(env):
    p = os.path.join(env.d, rv.STATE_FILE)
    return json.load(open(p))["families"] if os.path.exists(p) else {}


def _window_calls(env):
    return [c for c in env.api.calls if c[1] < "2026-01"]


# ---- (e) off by default --------------------------------------------------------------------------

def test_off_by_default_no_prior_month_is_requested_and_nothing_is_written(env):
    _write(env.d, HT, "2025-01", "2026-01")
    before = _store(env.d, HT)
    res = cs.update(UNIT, None)
    assert _window_calls(env) == [], env.api.calls
    assert _store(env.d, HT) == before
    assert not os.path.exists(os.path.join(env.d, rv.STATE_FILE))
    assert not os.path.exists(os.path.join(env.data, "census_revision_staging"))
    assert rv.OPERATOR_ACTIVATED_ON is None          # merging the code must not arm the write
    assert "annual revision" not in (res.error or "")


def test_not_due_before_the_cycle_start_date(env):
    _on(env)
    env.mp.setattr(R, "today_utc", lambda: dt.date(2026, 3, 1))
    _write(env.d, HT, "2025-01", "2026-01")
    cs.update(UNIT, None)
    assert _window_calls(env) == []


# ---- the whole campaign in one pass ------------------------------------------------------------------

def test_one_pass_re_pulls_the_three_prior_years_of_every_family_flow_and_never_deletes(env):
    _on(env)
    _write(env.d, HT, "2025-01", "2026-01")
    _write(env.d, EU, "2025-06", "2026-01")
    env.api.drop.add((HT, "2025-03", "02"))           # the publisher no longer serves this row
    res = cs.update(UNIT, None)
    ht, eu = _store(env.d, HT), _store(env.d, EU)
    # revised values reached every stored month of the window, in BOTH tables
    for mo in _months("2025-01", "2025-12"):
        for c in ("01",) + (() if mo == "2025-03" else ("02",)):
            assert ht[(f"{HT}|HITECH={c}", f"{mo}-01")]["GEN_VAL_MO"] == _val(HT, mo, c, "new")
    for mo in _months("2025-06", "2025-12"):
        assert eu[(f"{EU}|I_ENDUSE=01", f"{mo}-01")]["GEN_VAL_MO"] == _val(EU, mo, "01", "new")
    # the store-only row is KEPT at its old value and reported as held
    kept = ht[(f"{HT}|HITECH=02", "2025-03-01")]
    assert kept["GEN_VAL_MO"] == _val(HT, "2025-03", "02", "old")
    fam = _state(env)["imports"]
    assert fam["phase"] == "done" and fam["held_rows"][HT]["count"] == 1, fam
    assert "HELD 1 store-only row" in res.error and "NOT deleted" in res.error, res.error
    # months outside the window and months the store never held are not asked by the re-pull
    asked = {(f, t) for f, t, _s in env.api.calls}
    assert not any(t.startswith(("2023", "2024")) for _f, t in asked), asked
    assert (EU, "2025-05") not in asked
    # 2026-01 (outside the window) is untouched
    assert ht[(f"{HT}|HITECH=01", "2026-01-01")]["GEN_VAL_MO"] == _val(HT, "2026-01", "01", "old")
    # both tables are handed to the CSV step
    assert {"intltrade__imports__hitech", "intltrade__imports__enduse"} <= set(res.series_cursors)
    assert res.status in ("ok", "partial"), res


def test_staging_is_never_under_the_source_store_directory(env):
    """AR-444 a: tools/series_census.py and make_servable count every parquet under clean_full/<src>."""
    _on(env)
    _write(env.d, HT, "2025-01", "2026-01")
    env.mp.setenv("CENSUS_REVISION_BUDGET_MIN", "0.0001")
    clk = _Clock(step=0.001)
    env.mp.setattr(rv, "time", clk)
    cs.update(UNIT, None)
    assert _state(env)["imports"]["staged"], "the planted pass must stage something"
    def walk(root):
        return sorted(os.path.relpath(os.path.join(a, f), root)
                      for a, _d, fs in os.walk(root) for f in fs if f.endswith(".parquet"))
    assert walk(env.d) == ["intltrade__imports__hitech.parquet"], walk(env.d)
    # the source tree as blob lists it, recursively: the staged months are not in it
    assert [os.path.basename(p) for p in blob.list_parquets(env.d, recursive=True)] == \
        ["intltrade__imports__hitech.parquet"]
    assert walk(os.path.join(env.data, "census_revision_staging")), "staged pieces exist elsewhere"


def test_the_real_staging_root_is_outside_every_source_store_directory():
    """The fixture patches staging_root, so the UNPATCHED function is pinned here."""
    from updater import config
    root = os.path.normcase(os.path.abspath(rv.staging_root()))
    data_root = os.path.normcase(os.path.abspath(config.DATA_ROOT))
    assert os.path.commonpath([root, data_root]) != data_root, (root, data_root)
    assert "/data/" in rv.staging_root().replace("\\", "/") + "/"     # still a blob-routable path


# ---- resumable ------------------------------------------------------------------------------------

def test_a_small_cap_converges_over_passes_and_each_pass_moves(env):
    _on(env)
    _write(env.d, HT, "2025-01", "2026-01")
    _write(env.d, EU, "2025-06", "2026-01")
    env.mp.setenv("CENSUS_REVISION_BUDGET_MIN", str(3 / 60))     # 3 s of fake clock
    env.mp.setattr(rv, "time", _Clock(step=1.0))
    staged, done_at = [], None
    for k in range(40):
        res = cs.update(UNIT, None)
        fam = _state(env)["imports"]
        staged.append(sum(len(v) for v in fam["staged"].values()))
        if fam["phase"] == "done":
            done_at = k
            break
        assert res.status == "partial", res                     # never ok over an open campaign
        assert "not yet fetched" in res.error or "verify" in res.error or "publish" in res.error
    assert done_at is not None, staged
    assert all(b >= a for a, b in zip(staged, staged[1:])) and staged[0] < staged[-1], staged
    assert _store(env.d, EU)[(f"{EU}|I_ENDUSE=02", "2025-07-01")]["GEN_VAL_MO"] == \
        _val(EU, "2025-07", "02", "new")


def test_a_started_piece_finishes_past_the_cap(env):
    """AR-444 d: the cap gates the START of a piece; a split month slower than the cap still stages."""
    _on(env)
    _write(env.d, SH, "2025-11", "2026-01")
    env.mp.setenv("CENSUS_REVISION_BUDGET_MIN", str(1.5 / 60))   # one and a half clock steps
    env.mp.setattr(rv, "time", _Clock(step=1.0))
    cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["staged"] == {SH: ["2025-11"]}, fam                # 3 state requests, one piece
    sh_calls = [c for c in env.api.calls if c[0] == SH and c[1] == "2025-11"]
    assert sorted(c[2] for c in sh_calls) == list(STATES)


def test_a_stop_inside_a_piece_stages_nothing_for_that_month(env):
    _on(env)
    _write(env.d, SH, "2025-11", "2026-01")

    def stop_after_tail(api, flow, tv):
        if tv == "2025-11" and len([c for c in api.calls if c[1] == "2025-11"]) == 2:
            env.mp.setattr(cs.Deadline, "spent", lambda self: True)
    env.api.on_call = stop_after_tail
    env.mp.setattr(cs.Deadline, "spent", lambda self: False)
    res = cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["staged"] == {} and fam["phase"] == "fetch", fam
    assert res.status == "partial"


# ---- (b) a flow retired mid-campaign ----------------------------------------------------------------

def test_a_flow_retired_mid_campaign_drops_out_and_the_campaign_completes(env):
    _on(env)
    _write(env.d, HT, "2025-10", "2026-01")
    _write(env.d, BETA, "2025-01", "2025-12")
    env.mp.setenv("CENSUS_REVISION_BUDGET_MIN", str(2 / 60))
    env.mp.setattr(rv, "time", _Clock(step=1.0))
    cs.update(UNIT, None)
    assert _state(env)["imports"]["phase"] == "fetch"
    later = dt.date(2026, 11, 20)                                  # after _BETA_REMOVAL
    env.mp.setattr(R, "today_utc", lambda: later)
    for _ in range(10):
        cs.update(UNIT, None)
        if _state(env)["imports"]["phase"] == "done":
            break
    fam = _state(env)["imports"]
    assert fam["phase"] == "done", fam
    assert BETA not in fam["published"], fam                     # retired: not part of the publish
    beta = _store(env.d, BETA)                                    # and its stored rows are untouched
    assert beta[(f"{BETA}|HITECH=01", "2025-06-01")]["GEN_VAL_MO"] == _val(BETA, "2025-06", "01", "old")
    assert _store(env.d, HT)[(f"{HT}|HITECH=01", "2025-10-01")]["GEN_VAL_MO"] == \
        _val(HT, "2025-10", "01", "new")


# ---- (c) verify, restarts and the attempt cap ---------------------------------------------------------

def test_a_publisher_change_during_the_campaign_restarts_it_and_publishes_one_vintage(env):
    _on(env)
    _write(env.d, HT, "2025-10", "2026-01")
    _write(env.d, EU, "2025-10", "2026-01")
    env.api.vintage = "mid"

    def flip(api, flow, tv):                 # the publisher moves right after the FIRST piece
        if len(_window_calls(env)) == 2:
            api.vintage = "new2"
    env.api.on_call = flip
    res1 = cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["attempt"] == 2 and fam["phase"] == "fetch", fam
    assert "restarted" in res1.error
    # nothing of the mixed attempt 1 was published
    assert _store(env.d, HT)[(f"{HT}|HITECH=01", "2025-11-01")]["GEN_VAL_MO"] == \
        _val(HT, "2025-11", "01", "old")
    cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["phase"] == "done" and fam["attempt"] == 2, fam
    for f in (HT, EU):
        st = _store(env.d, f)
        for m in _months("2025-10", "2025-12"):
            for c in ("01", "02"):
                assert st[(f"{f}|{DIM[f]}={c}", f"{m}-01")]["GEN_VAL_MO"] == _val(f, m, c, "new2")


def test_a_publisher_that_always_differs_stops_at_the_attempt_cap(env):
    _on(env)
    _write(env.d, HT, "2025-11", "2026-01")
    n = [0]
    orig = _val

    def val(flow, month, code, vintage, state=None):
        n[0] += 1
        return str(int(orig(flow, month, code, "old")) + n[0])
    env.mp.setattr(sys.modules[__name__], "_val", val)
    for _ in range(8):
        res = cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["attempt"] == rv.MAX_ATTEMPTS_PER_CYCLE and fam["stuck"], fam
    n_before = len(_window_calls(env))
    res = cs.update(UNIT, None)
    assert len(_window_calls(env)) == n_before                   # a stuck cycle asks nothing more
    assert "STUCK" in res.error
    # and nothing of any attempt was published
    assert _store(env.d, HT)[(f"{HT}|HITECH=01", "2025-11-01")]["GEN_VAL_MO"] == \
        orig(HT, "2025-11", "01", "old")


# ---- holds --------------------------------------------------------------------------------------

def test_an_empty_answer_for_a_stored_month_is_held_not_staged_as_empty(env):
    _on(env)
    _write(env.d, HT, "2025-10", "2026-01")
    env.api.empty.add((HT, "2025-11"))
    res = cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert "2025-11" in fam["holds"][HT] and fam["phase"] == "done", fam
    ht = _store(env.d, HT)
    assert ht[(f"{HT}|HITECH=01", "2025-11-01")]["GEN_VAL_MO"] == _val(HT, "2025-11", "01", "old")
    assert "answered no rows on re-pull" in res.error


def test_a_label_only_difference_is_held_and_never_adds_a_second_row(env):
    _on(env)
    _write(env.d, HT, "2025-11", "2026-01", label=True)
    env.api.label[(HT, "2025-11")] = "renamed by the publisher"
    env.api.label[(HT, "2025-12")] = "renamed by the publisher"
    before = len(_store(env.d, HT))
    res = cs.update(UNIT, None)
    rows = pq.read_table(os.path.join(env.d, "intltrade__imports__hitech.parquet")).num_rows
    assert rows == before, (rows, before)
    fam = _state(env)["imports"]
    assert fam["label_conflicts"].get(HT), fam
    assert "label column" in res.error


# ---- the sentinel probe ---------------------------------------------------------------------------------

def test_the_probe_waits_its_interval_then_restarts_on_drift(env):
    _on(env)
    _write(env.d, HT, "2025-01", "2026-01")
    cs.update(UNIT, None)
    assert _state(env)["imports"]["phase"] == "done"
    n0 = len(_window_calls(env))
    env.mp.setattr(R, "today_utc", lambda: TODAY + dt.timedelta(days=3))
    cs.update(UNIT, None)
    assert len(_window_calls(env)) == n0                       # inside PROBE_DAYS: no request
    env.mp.setattr(R, "today_utc", lambda: TODAY + dt.timedelta(days=8))
    env.api.vintage = "old"                                      # the publisher now differs again
    cs.update(UNIT, None)
    fam = _state(env)["imports"]
    assert fam["attempt"] == 2, fam
    assert "sentinel drift" in fam["why"]


def test_a_malformed_state_file_stops_the_re_pull_not_the_tail(env):
    _on(env)
    _write(env.d, HT, "2025-01", "2026-01")
    open(os.path.join(env.d, rv.STATE_FILE), "w").write("[1,2]")
    env.api.last = "2026-02"
    res = cs.update(UNIT, None)
    assert "annual revision not run" in res.error
    assert ("intltrade/imports/hitech|HITECH=01", "2026-02-01") in _store(env.d, HT)
    assert _window_calls(env) == []


# ---- R1824 D1: the label rule, against the REAL key columns of all 24 tables --------------------------

# The dedup-key columns (minus series_key, obs_date, time) of every intltrade table, copied from the
# real store schemas (review AR-444 schema.log, 2026-11-06). Written out here, independently of the
# module, so a rule that drifts from the real names fails.
REAL_KEY_COLS = {
    "exports/enduse": "DF SUMMARY_LVL COMM_LVL E_ENDUSE_LDESC E_ENDUSE DISTRICT SUMMARY_LVL2 CTY_NAME "
                      "E_ENDUSE_SDESC LAST_UPDATE DIST_NAME CTY_CODE",
    "exports/enduseexport": "E_ENDUSE DF SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "exports/hitech": "DF SUMMARY_LVL COMM_LVL DISTRICT SUMMARY_LVL2 CTY_NAME HITECH_DESC HITECH "
                      "LAST_UPDATE DIST_NAME CTY_CODE",
    "exports/hitechexport": "HITECH DF SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "exports/hs": "E_COMMODITY DF SUMMARY_LVL SUMMARY_LVL2 COMM_LVL UNIT_QY1 LAST_UPDATE MONTH CTY_CODE "
                  "DISTRICT",
    "exports/naics": "DF SUMMARY_LVL COMM_LVL NAICS DISTRICT NAICS_LDESC SUMMARY_LVL2 CTY_NAME NAICS_SDESC "
                     "LAST_UPDATE DIST_NAME CTY_CODE",
    "exports/sitc": "SITC_LDESC DF SUMMARY_LVL COMM_LVL DISTRICT SUMMARY_LVL2 CTY_NAME SITC SITC_SDESC "
                    "LAST_UPDATE DIST_NAME CTY_CODE",
    "exports/sitcexport": "SITC DF SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "exports/statehs": "SUMMARY_LVL COMM_LVL STATE E_COMMODITY_SDESC SUMMARY_LVL2 CTY_NAME "
                       "E_COMMODITY_LDESC LAST_UPDATE E_COMMODITY CTY_CODE",
    "exports/statenaics": "SUMMARY_LVL COMM_LVL STATE NAICS NAICS_LDESC SUMMARY_LVL2 CTY_NAME NAICS_SDESC "
                          "LAST_UPDATE CTY_CODE",
    "exports/usda": "DF SUMMARY_LVL COMM_LVL DISTRICT SUMMARY_LVL2 CTY_NAME LAST_UPDATE DIST_NAME USDA "
                    "CTY_CODE",
    "exports/usdaexport": "USDA DF SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "imports/enduse": "SUMMARY_LVL COMM_LVL LAST_UPDATE DIST_NAME I_ENDUSE_LDESC I_ENDUSE DISTRICT "
                      "SUMMARY_LVL2 CTY_NAME I_ENDUSE_SDESC CTY_CODE",
    "imports/enduseimport": "I_ENDUSE SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "imports/hitech": "SUMMARY_LVL COMM_LVL HITECH LAST_UPDATE DIST_NAME DISTRICT SUMMARY_LVL2 CTY_NAME "
                      "HITECH_DESC CTY_CODE",
    "imports/hitechimport": "HITECH SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "imports/hs": "I_COMMODITY SUMMARY_LVL SUMMARY_LVL2 COMM_LVL UNIT_QY1 LAST_UPDATE MONTH CTY_CODE DISTRICT",
    "imports/naics": "SUMMARY_LVL COMM_LVL NAICS NAICS_LDESC LAST_UPDATE DIST_NAME DISTRICT SUMMARY_LVL2 "
                     "CTY_NAME NAICS_SDESC CTY_CODE",
    "imports/sitc": "SITC_LDESC SUMMARY_LVL COMM_LVL SITC_SDESC LAST_UPDATE DIST_NAME DISTRICT SUMMARY_LVL2 "
                    "CTY_NAME SITC CTY_CODE",
    "imports/sitcimport": "SITC SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
    "imports/statehs": "I_COMMODITY SUMMARY_LVL SUMMARY_LVL2 LAST_UPDATE MONTH CTY_CODE STATE COMM_LVL",
    "imports/statenaics": "SUMMARY_LVL COMM_LVL STATE NAICS NAICS_LDESC SUMMARY_LVL2 CTY_NAME NAICS_SDESC "
                          "LAST_UPDATE CTY_CODE",
    "imports/usda": "SUMMARY_LVL COMM_LVL DISTRICT SUMMARY_LVL2 CTY_NAME LAST_UPDATE DIST_NAME CTY_CODE USDA",
    "imports/usdaimport": "USDA SUMMARY_LVL SUMMARY_LVL2 COMM_LVL DISTRICT world",
}
# Classified by reading each name: codes select a row, labels describe it.
REAL_LABELS = {"E_ENDUSE_LDESC", "E_ENDUSE_SDESC", "I_ENDUSE_LDESC", "I_ENDUSE_SDESC", "HITECH_DESC",
               "NAICS_LDESC", "NAICS_SDESC", "SITC_LDESC", "SITC_SDESC", "E_COMMODITY_LDESC",
               "E_COMMODITY_SDESC", "CTY_NAME", "DIST_NAME", "UNIT_QY1", "LAST_UPDATE"}
REAL_CODES = {"DF", "SUMMARY_LVL", "SUMMARY_LVL2", "COMM_LVL", "DISTRICT", "CTY_CODE", "STATE", "MONTH",
              "world", "E_ENDUSE", "I_ENDUSE", "HITECH", "E_COMMODITY", "I_COMMODITY", "NAICS", "SITC",
              "USDA"}


def test_every_real_key_column_of_every_table_is_classified_correctly():
    assert len(REAL_KEY_COLS) == 24
    seen = set()
    for table, cols in REAL_KEY_COLS.items():
        for c in cols.split():
            seen.add(c)
            assert c in REAL_LABELS or c in REAL_CODES, (table, c)     # the test's own lists are complete
            assert rv.is_label(c) == (c in REAL_LABELS), (table, c)
    assert seen == REAL_LABELS | REAL_CODES                             # no list entry is invented
    assert rv.is_label("HITECH_DESC") and rv.is_label("UNIT_QY1")      # the two R1824 misses
    for k in ("series_key", "obs_date", "time"):
        assert not rv.is_label(k)


def test_the_label_guard_holds_a_relabel_under_the_real_column_name(env):
    """R1824: with HITECH_DESC the old guard kept 6 rows AND added 4 (10), reporting them 'held'."""
    _on(env)
    _write(env.d, HT, "2025-11", "2026-01", label=True)
    for mo in ("2025-11", "2025-12"):
        env.api.label[(HT, mo)] = "renamed by the publisher"
    before = pq.read_table(os.path.join(env.d, "intltrade__imports__hitech.parquet")).num_rows
    res = cs.update(UNIT, None)
    after = pq.read_table(os.path.join(env.d, "intltrade__imports__hitech.parquet")).num_rows
    assert after == before == 6, (before, after)
    assert "label column" in res.error and "HELD 4 store-only" not in res.error, res.error


# ---- R1824 D2: rows the publisher serves and the store lacks ----------------------------------------------

def test_a_planted_fetch_only_row_is_added_and_counted_per_year(env):
    _on(env)
    _write(env.d, HT, "2025-11", "2026-01")
    env.api.extra.add((HT, "2025-12"))
    before = len(_store(env.d, HT))
    res = cs.update(UNIT, None)
    st = pq.read_table(os.path.join(env.d, "intltrade__imports__hitech.parquet")).to_pylist()
    assert len(st) == before + 1, (len(st), before)
    assert any(r["CTY_CODE"] == "5700" for r in st)
    assert "2025 changed=4 added=1 held=0" in res.error, res.error
    assert _state(env)["imports"]["counts"][HT]["2025"] == {"changed": 4, "added": 1, "held": 0}


def test_a_flow_that_would_add_more_than_the_bound_is_held_not_merged(env):
    _on(env)
    _write(env.d, HT, "2025-11", "2026-01")
    env.api.extra.add((HT, "2025-12"))
    env.mp.setenv("CENSUS_REVISION_ADDED_CAP_MIN_ROWS", "0")
    env.mp.setenv("CENSUS_REVISION_ADDED_CAP_PCT", "0")
    before = _store(env.d, HT)
    res = cs.update(UNIT, None)
    assert _store(env.d, HT) == before                                 # nothing merged at all
    fam = _state(env)["imports"]
    assert fam["growth_held"][HT]["added"] == 1, fam
    assert "would ADD 1 row(s)" in res.error and "held for approval" in res.error, res.error


# ---- review AR-444 phase 3: a flow refused on every pass must not block its family ------------------------

def test_a_flow_refused_on_every_pass_is_held_and_the_family_still_publishes(env):
    from updater.errors import DefinitiveError
    _on(env)
    _write(env.d, HT, "2025-10", "2026-01")
    _write(env.d, EU, "2025-11", "2026-01")
    env.api.refuse.add(EU)                    # its window months answer HTTP 400; its tail still works
    notes = []
    for _ in range(5):                        # a FIXED bound: the cap must be reached by then
        try:
            res = cs.update(UNIT, None)
            notes.append(res.error or "")
        except DefinitiveError as e:          # a refusal is structural, as the tail books it
            notes.append(str(e))
        if _state(env)["imports"]["phase"] == "done":
            break
    fam = _state(env)["imports"]
    assert fam["phase"] == "done", fam
    assert EU in fam["failed_flows"] and EU not in fam["published"], fam
    assert _store(env.d, HT)[(f"{HT}|HITECH=01", "2025-11-01")]["GEN_VAL_MO"] == \
        _val(HT, "2025-11", "01", "new")
    assert _store(env.d, EU)[(f"{EU}|I_ENDUSE=01", "2025-11-01")]["GEN_VAL_MO"] == \
        _val(EU, "2025-11", "01", "old")
    for n in notes[:rv.PIECE_FAIL_CAP]:      # the passes while the cap is being reached
        assert "REFUSED by the API" in n and "HTTP 400" in n, n
        assert "returned 200" not in n and "transient-failed" not in n, n
    assert "failed on consecutive passes" in notes[-1], notes[-1]
    refused = [c for c in env.api.calls if c[0] == EU and c[1] < "2026-01"]
    assert len(refused) <= 6, len(refused)                        # not asked again once held


def test_a_verify_month_refused_on_every_pass_holds_its_flow_and_the_family_publishes(env):
    """Review AR-444 R1828: the verify re-fetch had no cap, so one refused month blocked the family."""
    from updater.errors import DefinitiveError
    _on(env)
    _write(env.d, HT, "2025-10", "2026-01")
    _write(env.d, EU, "2025-11", "2026-01")
    env.api.refuse_from[(HT, "2025-10")] = 2        # staged fine, then refused at every verify
    notes = []
    for _ in range(6):                               # a FIXED bound: the cap must be reached by then
        try:
            notes.append(cs.update(UNIT, None).error or "")
        except DefinitiveError as e:
            notes.append(str(e))
        if _state(env)["imports"]["phase"] == "done":
            break
    fam = _state(env)["imports"]
    assert fam["phase"] == "done", (fam, notes)
    assert HT in fam["failed_flows"] and HT not in fam["published"], fam
    assert _store(env.d, EU)[(f"{EU}|I_ENDUSE=01", "2025-11-01")]["GEN_VAL_MO"] == \
        _val(EU, "2025-11", "01", "new")
    assert _store(env.d, HT)[(f"{HT}|HITECH=01", "2025-11-01")]["GEN_VAL_MO"] == \
        _val(HT, "2025-11", "01", "old")
    asked = [c for c in env.api.calls if c[:2] == (HT, "2025-10")]
    assert len(asked) <= 1 + 3, len(asked)                    # one fetch + at most the cap of verifies
    assert "REFUSED by the API" in notes[0] and "transient-failed" not in notes[0], notes[0]
