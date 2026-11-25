"""The data-clock re-check (design F:/work/econ_finish/monthly_recheck_20261124/DESIGN.md sections 4 and 9, with the
review conditions C1-C9, R2-C1..C6, R3-C1..C6 and R4-C1..C7).

THE DEFECT. Two clocks never read each other. The schedule re-checks a monthly unit 25.2 days after its last success
(base.cadence_due); the gate's data clock fires 84 days after its newest observed obs (health). On 2026-11-24
imf_commodity and nasa_giss went RED-DATA at the 09:07Z gate while their next regular check was 2026-12-03T17:47Z:
their clean 11-08 passes came before the October value was out. The re-check looks at every scheduled pass from 7
days before the clock to 28 days after it (spaced 0.5 d from the last attempt), only when the regular check would
come after the clock.

THE PLANT P0 is imf_commodity's state row of 2026-11-17 as offsets from an instant T: last_success = last_attempt =
T - 9.71 d, status no_change, unit last_obs_date and cursor max = T - 78.25 d, five clean runs rows (dur <= 400 s,
inserted in time order: "the last 5" is id DESC, state.py run_cost_estimate). The unit is the REAL id
imf_commodity/_all, so the rule reads the real registry entry (monthly, no data_cadence). fire = T + 5.75 d; the
regular check = T + 15.49 d.

ENTRY POINTS. F2 and the guards call the 57cd1015-era label `_budget_skip_due_label(unit, store, False)` (3 arguments,
R3-C1), which exists on main before this change, so they fail or pass by an ASSERTION about due-ness (C9). The
RE-CHECK / HELD lines are read from the real run_once (dry, a strategy whose probe stops the unit). THE CLOCKS: the
plant is relative to the WALL clock at test time (T = now), and orchestrate.now_utc is pinned to T in production's
type, an ISO string (R2-C2, R4-C1): old code reads datetime.now there, new code reads now_utc - the same instant to
the second. The assess tests pin health.datetime instead (assess takes no `now`).

Must fail on main before this change: F2, G6b, G6c, G6d, G7b, the RE-CHECK / HELD line tests, the base / health
unit tests.
Must pass before and after: G1-G8, G13 (due-ness), G9 and the assess verdicts (G10b). Each guard names the mutant
it kills; tools/monthly_recheck_mutants.py applies them on a git-archive copy.
"""
from __future__ import annotations

import datetime as dt
import re

import pytest

from updater import health, orchestrate, registry
from updater.state import StateStore
from updater.strategies import base
from updater.strategies.base import Strategy

UTC = dt.timezone.utc
SID, UID = "imf_commodity", "_all"
_REAL_ENTRY = next(e for e in registry.load()["sources"] if e["source_id"] == SID)


def _iso(d: dt.datetime) -> str:
    return d.astimezone(UTC).isoformat(timespec="seconds")


def _entry(**over) -> dict:
    e = dict(_REAL_ENTRY)
    for k, v in over.items():
        if v is None:
            e.pop(k, None)
        else:
            e[k] = v
    return e


def _wall_T() -> dt.datetime:
    return dt.datetime.now(UTC).replace(microsecond=0)


CLEAN5 = (("ok", 398.7), ("no_change", 5.8), ("ok", 120.0), ("no_change", 45.0), ("no_change", 30.0))


def plant(tmp_path, T, s=9.71, a=None, n=78.25, rows=CLEAN5, row_days=None, status="no_change"):
    """P0 (or a variation) relative to T. rows: (status, dur_s) OLDEST FIRST; row_days: their ages in days (default
    s + 104, + 78, + 52, + 26, + 0 - the newest is the last pass)."""
    store = StateStore(str(tmp_path / "state.db"))
    a = s if a is None else a
    ls, la, nv = _iso(T - dt.timedelta(days=s)), _iso(T - dt.timedelta(days=a)), _iso(T - dt.timedelta(days=n))
    store.upsert_unit(SID, UID, strategy="overwrite_if_changed", status=status, last_success_utc=ls,
                      last_attempt_utc=la, last_obs_date=nv)
    store.upsert_source(SID, strategy="overwrite_if_changed", cadence="monthly", status="ok",
                        last_success_utc=ls, last_attempt_utc=ls)
    store.put_series_cursors(SID, {"IMF_COMMODITY:PALLFNF.IX": nv})
    days = row_days or [s + 26.0 * k for k in range(len(rows) - 1, -1, -1)]
    store.db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s) VALUES (?,?,?,?,1,?)",
                         [(_iso(T - dt.timedelta(days=d)), SID, UID, st, dur) for d, (st, dur) in zip(days, rows)])
    store.db.commit()
    return store


def _unit(entry=None):
    return registry.to_units(entry or _REAL_ENTRY)[0]


def _pin_now(monkeypatch, when: dt.datetime):
    monkeypatch.setattr(orchestrate, "now_utc", lambda: when.isoformat(timespec="seconds"))


def _precondition_not_due(unit, store, now):
    us = store.get_unit(SID, UID)
    assert not orchestrate.get_strategy(unit.strategy).is_due(unit, us, now=now), \
        "precondition: the regular schedule says not due, so a 'due' can only be the re-check's"


# ------------------------------------------------------------------ F2: the label through the 3-argument form

def test_f2_the_budget_skip_label_says_due_when_the_clock_fires_before_the_regular_check(tmp_path, monkeypatch):
    """F2. On main before the change the label asks is_due on the wall clock (9.71 d < 25.2 d): 'not due'. The plant
    is relative to the wall clock at test time, so this cannot stop failing on a calendar date (R2-C2)."""
    T = _wall_T()
    store = plant(tmp_path, T)
    _pin_now(monkeypatch, T)
    unit = _unit()
    _precondition_not_due(unit, store, T)
    assert orchestrate._budget_skip_due_label(unit, store, False) == "due"


# ------------------------------------------------------------------ the guards (due-ness; pass before and after)

def _g(name, plant_kw=None, entry_kw=None, now_days=0.0, want="not due"):
    return pytest.param(plant_kw or {}, entry_kw or {}, now_days, want, id=name)


GUARDS = [
    # G1: s + C = T + 2.2 <= fire = T + 3, lead/cap/spacing hold. Kills: `s + C > fire` removed or inverted (-> due)
    _g("G1-regular-check-first", {"s": 23.0, "n": 81.0}),
    # G2: read at T - 2.25 d: fire = now + 8 > now + 7. Kills: LEAD removed (-> due)
    _g("G2-before-the-lead", now_days=-2.25),
    # G3: n = T - 170.25 d: fire + CAP = T - 58.25 d < now. Kills: CAP removed (-> due)
    _g("G3-past-the-cap", {"n": 170.25}),
    # G4: a = T - 0.3 d < 0.45 d (0.5 x 0.9), newest row transient_fail. Kills: spacing removed (-> due)
    _g("G4-spacing", {"a": 0.3, "status": "transient_fail", "rows": CLEAN5[1:] + (("transient_fail", 12.0),),
                      "row_days": [87.71, 61.71, 35.71, 9.71, 0.3]}),
    # G5: one of the last 5 rows at 601 s. Kills: the cost gate removed (-> due)
    _g("G5-cost", {"rows": (("ok", 601.0),) + CLEAN5[1:]}),
    # G6: one of the last 5 rows partial. Kills: the history gate removed (-> due)
    _g("G6-history-partial", {"rows": (("partial", 120.0),) + CLEAN5[1:]}),
    # G6b: a transient_fail row BEFORE the last clean pass (a stays T - 9.71 d), dur <= 600, none partial: DUE.
    #      Kills: the stricter residual.py rule, any non-clean row holds (-> not due). Fails on main before the change.
    _g("G6b-transient-does-not-hold", {"rows": (("ok", 398.7), ("transient_fail", 300.0), ("ok", 120.0),
                                                ("no_change", 45.0), ("no_change", 30.0))}, want="due"),
    # G6c: SIX rows in time order (id order); the OLDEST is partial, the newest 5 are clean: DUE. With exactly 5 rows
    #      the oldest 5 and the newest 5 are the same set, so only a 6th row pins "newest by id" (review O-2).
    #      Kills: rule 3 reads the oldest 5 (ORDER BY id ASC, review m3) (-> not due). Fails on main before the change.
    _g("G6c-only-the-newest-5", {"rows": (("partial", 120.0),) + CLEAN5}, want="due"),
    # G6d: a `locked` and a `window_stopped` row in the last 5, none partial or killed_external: DUE (rule 3 holds
    #      on partial and killed_external only; window_stopped is also outside run_cost_estimate's window).
    #      Kills: `locked` or `window_stopped` added to RECHECK_HOLD_STATUSES (-> not due). Fails on main before.
    _g("G6d-locked-and-window-stopped-do-not-hold", {"rows": (("ok", 398.7), ("locked", 0.0), ("ok", 120.0),
                                                              ("window_stopped", 30.0), ("no_change", 30.0))},
       want="due"),
    # G7: a LIVE upstream_verified claim covers n. Kills: the uv exclusion removed (-> due)
    _g("G7-uv-live", entry_kw={"uv_checked_days": 10.0}),
    # G7b: the same claim EXPIRED (checked T - 200 d > 180): DUE. Kills: the exclusion ignores expiry (-> not due)
    _g("G7b-uv-expired", entry_kw={"uv_checked_days": 200.0}, want="due"),
    # G8: data_cadence annual, n = T - 1,090 d: fire = T + 5 under D = 1,095. Kills: the scope test removed (-> due)
    _g("G8-scope", {"n": 1090.0}, {"data_cadence": "annual"}),
    # G13: the unit declares window_stop. Kills: rule 6 removed (-> due)  (R4-C4)
    _g("G13-window-stop", entry_kw={"window_stop": True}),
]


def _guard_entry(T, n_days, entry_kw):
    kw = dict(entry_kw)
    if "uv_checked_days" in kw:
        kw["upstream_verified"] = {"latest_obs": _iso(T - dt.timedelta(days=n_days)),
                                   "checked": _iso(T - dt.timedelta(days=kw.pop("uv_checked_days")))[:10]}
    return _entry(**kw)


@pytest.mark.parametrize("plant_kw, entry_kw, now_days, want", GUARDS)
def test_the_guards_through_the_label(tmp_path, monkeypatch, plant_kw, entry_kw, now_days, want):
    T = _wall_T()
    store = plant(tmp_path, T, **plant_kw)
    entry = _guard_entry(T, plant_kw.get("n", 78.25), entry_kw)
    monkeypatch.setattr(registry, "load", lambda *a, **k: {"sources": [entry]})     # the label's own lookup (R2-C5)
    now = T + dt.timedelta(days=now_days)
    _pin_now(monkeypatch, now)
    unit = _unit(entry)
    _precondition_not_due(unit, store, now)
    assert orchestrate._budget_skip_due_label(unit, store, False) == want


# ------------------------------------------------------------------ the lines run_once prints (dry)

class _Probe(Strategy):
    """The REAL Strategy.is_due; its probe answers 'cannot tell', which ends a dry unit right after the due check
    (nothing is fetched or written). `probed` therefore lists the units the due check let through."""
    name = "probe"

    def __init__(self):
        self.probed = []

    def detect_change(self, unit, us):
        self.probed.append(unit.key)
        return None

    def run(self, unit, since=None):
        raise AssertionError("a dry run never fetches")


def _dry(monkeypatch, capsys, store, unit):
    probe = _Probe()
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "0")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    monkeypatch.setattr(registry, "validate", lambda *a, **k: [])
    monkeypatch.setattr(registry, "all_units", lambda *a, **k: [unit])
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: probe)
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda u: True)
    monkeypatch.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
    orchestrate.run_once(sources=[SID], dry=True, store=store)
    return probe.probed, capsys.readouterr().out


def test_p0_is_rechecked_and_says_why(tmp_path, monkeypatch, capsys):
    """The real run_once: P0 passes the due check and the line names both clocks (fails before the change: NOT DUE)."""
    T = _wall_T()
    store = plant(tmp_path, T)
    _pin_now(monkeypatch, T)
    probed, out = _dry(monkeypatch, capsys, store, _unit())
    assert probed == [f"{SID}/{UID}"], out[-1500:]
    fire = (T - dt.timedelta(days=78.25) + dt.timedelta(days=84)).strftime("%Y-%m-%dT%H:%MZ")
    assert re.search(rf"RE-CHECK {SID}/{UID} - data clock {fire}, regular check \S+Z: due by the data-clock "
                     rf"re-check", out), out[-1500:]
    assert f"NOT DUE {SID}" not in out and "RE-CHECK HELD" not in out


HELD = [
    pytest.param({"rows": (("ok", 601.0),) + CLEAN5[1:]}, {}, r"cost 601\.0 s > 600 s", id="G5-cost"),
    pytest.param({"rows": (("partial", 120.0),) + CLEAN5[1:]}, {}, r"history: partial in its last 5 runs",
                 id="G6-history"),
    pytest.param({"rows": (("killed_external", 120.0),) + CLEAN5[1:]}, {}, r"history: killed_external",
                 id="G6-history-killed"),
    pytest.param({}, {"uv_checked_days": 10.0}, r"upstream_verified: a live claim", id="G7-uv"),
    pytest.param({"n": 1090.0}, {"data_cadence": "annual"}, r"scope: its lateness clock is annual", id="G8-scope"),
    pytest.param({}, {"window_stop": True}, r"window_stop: ", id="G13-window-stop"),
    pytest.param({}, {"units": [{"unit_id": "a"}, {"unit_id": "b"}]}, r"units: its source has 2 units",
                 id="units"),
]


@pytest.mark.parametrize("plant_kw, entry_kw, reason", HELD)
def test_a_held_unit_whose_date_test_holds_says_why(tmp_path, monkeypatch, capsys, plant_kw, entry_kw, reason):
    T = _wall_T()
    store = plant(tmp_path, T, **plant_kw)
    entry = _guard_entry(T, plant_kw.get("n", 78.25), entry_kw)
    monkeypatch.setattr(registry, "load", lambda *a, **k: {"sources": [entry]})
    _pin_now(monkeypatch, T)
    unit = registry.to_units(entry)[0]
    if unit.unit_id != UID:                    # the two-unit entry: plant the first unit's row under its own id
        store.db.execute("UPDATE unit_state SET unit_id=? WHERE source_id=?", (unit.unit_id, SID))
        store.db.execute("UPDATE runs SET unit_id=? WHERE source_id=?", (unit.unit_id, SID))
        store.db.commit()
    probed, out = _dry(monkeypatch, capsys, store, unit)
    assert probed == [] and f"NOT DUE {SID}/{unit.unit_id}" in out, out[-1500:]
    assert re.search(rf"RE-CHECK HELD {SID}/{unit.unit_id} - {reason}", out), out[-1500:]


@pytest.mark.parametrize("plant_kw, now_days", [({"s": 23.0, "n": 81.0}, 0.0), ({}, -2.25), ({"n": 170.25}, 0.0)],
                         ids=["G1", "G2", "G3"])
def test_no_held_line_when_the_date_test_does_not_hold(tmp_path, monkeypatch, capsys, plant_kw, now_days):
    """R4-C7: a HELD line only when the date test holds - otherwise every pass prints one per monthly unit. Here the
    unit is NOT DUE, its NOT DUE line carries the clock, and nothing else is said."""
    T = _wall_T()
    store = plant(tmp_path, T, rows=(("ok", 601.0),) + CLEAN5[1:], **plant_kw)       # cost-held, if it got that far
    _pin_now(monkeypatch, T + dt.timedelta(days=now_days))
    probed, out = _dry(monkeypatch, capsys, store, _unit())
    assert probed == [] and "RE-CHECK" not in out, out[-1500:]
    assert re.search(rf"NOT DUE {SID}/{UID} .* data clock \S+Z, regular check \S+Z; skipped", out), out[-1500:]


def test_g12_an_exception_in_the_recheck_is_held_and_never_raises(tmp_path, monkeypatch, capsys):
    """G12 (R4-C1), the label and the dry loop: a store read the re-check needs raises; the answer is the regular
    schedule's (not due) and the pass says so. The run-level G12 (the next unit still runs) is in
    tests/test_monthly_recheck_replay.py."""
    T = _wall_T()
    store = plant(tmp_path, T)
    _pin_now(monkeypatch, T)

    def boom(sid):
        raise RuntimeError("cursor table unreadable")
    monkeypatch.setattr(store, "series_cursors", boom)
    unit = _unit()
    assert orchestrate._budget_skip_due_label(unit, store, False) == "not due"
    probed, out = _dry(monkeypatch, capsys, store, unit)
    assert probed == [] and f"NOT DUE {SID}/{UID}" in out
    assert f"RE-CHECK HELD {SID}/{UID} - error RuntimeError" in out, out[-1500:]


# ------------------------------------------------------------------ the rule itself (base.clock_recheck_due)

def test_the_rule_on_p0_and_its_bounds():
    """P0's arithmetic, then each bound one minute either side (fails before the change: no such function)."""
    T = dt.datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
    s, n = _iso(T - dt.timedelta(days=9.71)), _iso(T - dt.timedelta(days=78.25))
    D, C = 84.0, base.CADENCE_DAYS["monthly"]
    fire, regular = base.clock_recheck_times(s, n, D, C)
    assert fire == T + dt.timedelta(days=5.75)
    assert regular == T - dt.timedelta(days=9.71) + dt.timedelta(days=C * 0.9)    # T + 15.49 d
    assert regular > fire                                                          # the regular check comes after
    due = lambda now, a=s: base.clock_recheck_due(s, a, n, D, C, now)            # noqa: E731
    one = dt.timedelta(minutes=1)
    assert due(T)
    lead = fire - dt.timedelta(days=base.RECHECK_LEAD_DAYS)
    assert not due(lead - one) and due(lead + one)
    cap = fire + dt.timedelta(days=base.RECHECK_CAP_DAYS)
    assert due(cap - one) and not due(cap + one)
    gap = dt.timedelta(days=base.RECHECK_RETRY_DAYS * 0.9)
    assert not due(T, _iso(T - gap + one)) and due(T, _iso(T - gap - one))
    assert base.RECHECK_RETRY_DAYS == 0.5, "R4-C3: one scheduled pass a day at a moving hour needs 0.5 d"
    # the regular check first: s + C <= fire -> never
    s2 = _iso(T - dt.timedelta(days=23.0))
    n2 = _iso(T - dt.timedelta(days=81.0))
    assert not base.clock_recheck_due(s2, s2, n2, D, C, T)
    # a missing stamp -> never (the rule needs a recorded success and an observed obs)
    assert not base.clock_recheck_due(None, None, n, D, C, T) and not base.clock_recheck_due(s, s, None, D, C, T)


def test_an_obs_date_reads_as_midnight_utc_as_the_gate_reads_it():
    T = dt.datetime(2026, 11, 8, 12, 59, tzinfo=UTC)
    fire, _ = base.clock_recheck_times(_iso(T), "2026-09-01", 84.0, 28)
    assert fire == dt.datetime(2026, 11, 24, tzinfo=UTC)                      # 30 + 31 + 23 days


# ------------------------------------------------------------------ G9 / G10: the gate is unchanged (R762)

_REAL_DT = health.datetime


def _assess_at(monkeypatch, store, entry, when):
    class _At(_REAL_DT):
        @classmethod
        def now(cls, tz=None):
            return when
    monkeypatch.setattr(registry, "load", lambda *a, **k: {"sources": [entry]})
    monkeypatch.setattr(health, "datetime", _At)
    rows = {r["source"]: r for r in health.assess(store)["sources"]}
    monkeypatch.setattr(health, "datetime", _REAL_DT)
    return rows[SID]


T_FIX = dt.datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
ASSESS = [
    # G9: P0 read at T + 6 d (age 84.25 > 84), no new period: RED-DATA. Kills: a widened D, a skipped RED branch
    pytest.param({}, {}, 6.0, "RED-DATA", None, id="G9-red"),
    pytest.param({}, {}, 5.5, "OK", None, id="age-83.75-ok"),
    pytest.param({}, {"uv_checked_days": 10.0}, 6.0, "OK", None, id="uv-live-ok"),
    pytest.param({}, {"uv_checked_days": 200.0}, 6.0, "ATTENTION", r"upstream_verified is 20[67]d old",
                 id="uv-expired"),
    pytest.param({}, {"upstream_verified": "measured by hand"}, 6.0, "RED-DATA", r"upstream_verified is str",
                 id="uv-malformed"),
    pytest.param({}, {"publication_lag_days": 10}, 6.0, "OK", None, id="publication-lag"),
    # the allowance is clamped at 2 periods: D = 84 + min(1000, 56) = 140 < the age 141 -> RED-DATA (unclamped: OK)
    pytest.param({"n": 141.0}, {"publication_lag_days": 1000}, 0.0, "RED-DATA", None, id="publication-lag-clamped"),
    pytest.param({"n": 139.0}, {"publication_lag_days": 1000}, 0.0, "OK", None, id="publication-lag-inside"),
]


@pytest.mark.parametrize("plant_kw, entry_kw, at_days, want, note", ASSESS)
def test_g9_g10_the_gate_reads_as_before(tmp_path, monkeypatch, plant_kw, entry_kw, at_days, want, note):
    """The system's own assess on a planted P0, before and after the extraction (G10b): the same verdicts, so the
    re-check mutes nothing (R762) and the extraction changed no output."""
    T = T_FIX
    store = plant(tmp_path, T, **plant_kw)
    entry = _guard_entry(T, plant_kw.get("n", 78.25), entry_kw)
    row = _assess_at(monkeypatch, store, entry, T + dt.timedelta(days=at_days))
    assert row["health"] == want, row
    if note:
        assert any(re.search(note, a) for a in row["attention"]), row["attention"]


def test_g10_the_extracted_clock_is_the_old_inline_formula_for_every_entry():
    """G10a: data_clock_days(e) over all registry entries equals the formula assess held inline at 57cd1015."""
    def old(e):
        cadence = e.get("cadence", "monthly")
        period = base.CADENCE_DAYS.get(cadence, 7)
        lat = e.get("data_cadence") or cadence
        d = health.LATENESS_PERIOD.get(lat, base.CADENCE_DAYS.get(lat, period)) * (health.SLA_TOLERANCE
                                                                                 + health.DATA_SLACK_PERIODS)
        lag = e.get("publication_lag_days")
        if isinstance(lag, (int, float)) and not isinstance(lag, bool):
            d += min(max(0.0, float(lag)), health.publication_lag_cap(lat, period))
        return d
    entries = registry.load()["sources"]
    assert len(entries) > 200
    assert {e["source_id"]: health.data_clock_days(e) for e in entries} == {e["source_id"]: old(e) for e in entries}
    assert health.data_clock_days(_REAL_ENTRY) == 84.0


def test_only_a_live_claim_covers():
    T = T_FIX
    n = _iso(T - dt.timedelta(days=78.25))
    live = _entry(upstream_verified={"latest_obs": n, "checked": "2026-06-05"})
    old = _entry(upstream_verified={"latest_obs": n, "checked": "2025-11-01"})
    behind = _entry(upstream_verified={"latest_obs": "2027-01-01", "checked": "2026-06-05"})
    assert health.upstream_claim_covers(live, n, T)
    assert not health.upstream_claim_covers(old, n, T) and health.upstream_claim(old, n, T)[0] == "expired"
    assert not health.upstream_claim_covers(behind, n, T) and not health.upstream_claim_covers(_entry(), n, T)
    assert health.upstream_claim(_entry(upstream_verified="x"), n, T) == (None, None, "str")
