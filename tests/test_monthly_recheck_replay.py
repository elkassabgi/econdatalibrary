"""The data-clock re-check end to end: the REAL run_once and the REAL health.assess over a virtual year (design
DESIGN.md section 9, F3; review conditions R2-C2, R3-C3, R3-C4, R4-C1, R4-C3).

F3. A PAST virtual window, 2025-11-08T12:59Z .. 2026-11-08T12:59Z: production's dates moved back one whole year, so
all virtual time precedes any wall clock the test can run at (that year has a 28-day February, as 2026-27 does). P0
shifted to that calendar: the last pass 2025-11-08T12:59Z, n = 2025-09-01. A scripted publisher: month m's value is
out 10 d after the month's last day at 14:00Z (October's at 2025-11-10T14:00Z: simulate_drift.py's lag 9, whose
lead-7 maximum is 0.0 RED days). A scripted fetch of 45 virtual seconds (the 2026-11-24 measurement; it must stay
<= 600 s or the cost gate holds the unit, R3-C4). The gate: the real assess at 09:07Z each virtual day.
  * variant "06Z+22Z": two passes a day, as the model;
  * variant "one a day, 13Z/07Z": ONE pass a day whose hour moves 6 h earlier every other day - production's shape for
    these cloud units (one scheduled CI pass a day; imf_commodity's kept passes reached it at 05:08, 18:47, 07:24,
    21:46, 00:21 and 12:59Z). A 1-day spacing refuses the 07Z pass after a 13Z one (18 h < 21.6 h); 0.5 d does not
    (R4-C3).
Assert 0 RED-DATA days. Before the change the first red read is deterministic: 2025-11-24T09:07Z (the clock
2025-11-24T00:00Z; the next regular check 2025-12-03T17:47Z), the shape of production on 2026-11-24.

THE CLOCKS (R2-C2): orchestrate.now_utc and state.now_utc (ISO stamps), orchestrate.time (the virtual _Clock), the
strategy's `now` (its is_due forces the virtual clock, as the replay harness's does) and health.datetime. Left
UNPINNED on purpose (R3-C3): claim_lease's expiry reads datetime.now (state.py), harmless in one process - the same
OWNER re-claims its own lease and a release deletes the row.

G12 (R4-C1), run level: a store read the re-check needs raises; run_once goes on to the next unit, the planted unit
is NOT DUE and says `RE-CHECK HELD ... - error RuntimeError`. The mutant without the try ends run_once.
"""
from __future__ import annotations

import datetime as dt
import re

import pytest

from updater import health, orchestrate, registry
from updater import state as state_mod
from updater.state import StateStore
from updater.strategies.base import Result, Strategy, Unit

UTC = dt.timezone.utc
SID, UID, KEY = "imf_commodity", "_all", "IMF_COMMODITY:PALLFNF.IX"
_REAL_ENTRY = next(e for e in registry.load()["sources"] if e["source_id"] == SID)
START = dt.datetime(2025, 11, 8, 12, 59, tzinfo=UTC)
END = dt.datetime(2026, 11, 8, 12, 59, tzinfo=UTC)
FETCH_S = 45.0
CLEAN5 = (("ok", 398.7), ("no_change", 5.8), ("ok", 120.0), ("no_change", 45.0), ("no_change", 30.0))


class _Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


def _iso(d: dt.datetime) -> str:
    return d.astimezone(UTC).isoformat(timespec="seconds")


def _month_after(d: dt.date) -> dt.date:
    return dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def newest_published(at: dt.datetime, lag_days: float = 10.0) -> dt.date:
    """The period START of the newest value out at `at`: month m is out at (m's last day + lag_days) 14:00Z."""
    m = dt.date(at.year, at.month, 1)
    for _ in range(6):
        last = _month_after(m) - dt.timedelta(days=1)
        if dt.datetime(last.year, last.month, last.day, 14, tzinfo=UTC) + dt.timedelta(days=lag_days) <= at:
            return m
        m = dt.date(m.year - (m.month == 1), (m.month - 2) % 12 + 1, 1)
    raise AssertionError(f"no value out by {at}")


class Rig:
    """run_once and assess on one virtual clock over a planted P0 (the real imf_commodity entry and unit)."""

    def __init__(self, tmp_path, mp, start=START, n="2025-09-01", extra=(), lag_days=10.0):
        self.mp, self.clock, self.lag_days = mp, _Clock(start.timestamp()), lag_days
        self.store = StateStore(str(tmp_path / "state.db"))
        self.unit = registry.to_units(_REAL_ENTRY)[0]
        self.units = [self.unit] + list(extra)
        self.fetches, self.ran, self.n_seen = 0, [], []
        s = _iso(start)
        self.store.upsert_unit(SID, UID, strategy=self.unit.strategy, status="no_change", last_success_utc=s,
                               last_attempt_utc=s, last_obs_date=n, upstream_vintage="date-tail")
        self.store.upsert_source(SID, strategy=self.unit.strategy, cadence="monthly", status="ok",
                                 last_success_utc=s, last_attempt_utc=s)
        self.store.put_series_cursors(SID, {KEY: n})
        rows = [(_iso(start - dt.timedelta(days=26.0 * k)), SID, UID, st, dur)        # oldest first (id DESC)
                for k, (st, dur) in zip(range(len(CLEAN5) - 1, -1, -1), CLEAN5)]
        self.store.db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s) "
                                  "VALUES (?,?,?,?,1,?)", rows)
        self.store.db.commit()
        for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
            mp.delenv(k, raising=False)
        mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", "240")
        now = lambda: _iso(self.now())                                             # noqa: E731
        mp.setattr(orchestrate, "time", self.clock)
        mp.setattr(orchestrate, "now_utc", now)
        mp.setattr(state_mod, "now_utc", now)
        mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
        mp.setattr(orchestrate, "hard_timeout_available", lambda: False)
        # the registry run_once and assess read: the REAL imf_commodity entry alone (rv_assess.py's filter - a
        # source's row depends only on its own entry and state)
        mp.setattr(registry, "load", lambda *a, **k: {"sources": [_REAL_ENTRY]})
        mp.setattr(registry, "validate", lambda *a, **k: [])
        mp.setattr(registry, "all_units", lambda *a, **k: list(self.units))
        mp.setattr(orchestrate, "_has_adapter", lambda u: True)
        mp.setattr(orchestrate, "_wrong_location", lambda u: False)
        mp.setattr(orchestrate, "_protected", lambda u: False)
        mp.setattr(orchestrate, "_is_live", lambda u: True)
        mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
        mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
        mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
        mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
        rig = self
        real_dt = health.datetime

        class _At(real_dt):
            @classmethod
            def now(cls, tz=None):
                return rig.now()
        mp.setattr(health, "datetime", _At)

        class _Strat(Strategy):
            name = "sim"

            def is_due(self, unit, us, now=None):
                return Strategy.is_due(self, unit, us, now=rig.now())

            def detect_change(self, unit, us):
                return "v-new"                       # a date-tail unit: every due tick is a full fetch

            def run(self, unit, since=None):
                return rig.fetch(unit)

        self.strat = _Strat()
        mp.setattr(orchestrate, "get_strategy", lambda name: self.strat)

    def now(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.clock.t, UTC)

    def fetch(self, unit):
        self.ran.append(unit.source_id)
        if unit.source_id != SID:
            self.clock.t += 60.0
            return Result(status="ok", obs=1, new_vintage="v-new", merged_rows=1)
        self.fetches += 1
        newest = newest_published(self.now(), self.lag_days).isoformat()
        self.clock.t += FETCH_S
        stored = (self.store.get_unit(SID, UID) or {}).get("last_obs_date") or ""
        self.n_seen.append(newest)
        if newest > stored:
            return Result(status="ok", obs=1, new_vintage="v-new", last_obs_date=newest,
                          series_cursors={KEY: newest}, merged_rows=1)
        return Result(status="no_change", obs=0, new_vintage="v-new", last_obs_date=newest, merged_rows=0)

    def run_pass(self, at: dt.datetime, capsys) -> str:
        self.clock.t = max(self.clock.t, at.timestamp())
        orchestrate.run_once(store=self.store)
        return capsys.readouterr().out

    def gate(self, at: dt.datetime) -> str:
        self.clock.t = max(self.clock.t, at.timestamp())
        rows = {r["source"]: r for r in health.assess(self.store)["sources"]}
        return rows[SID]["health"]


def _events(variant):
    out = []
    day0 = dt.datetime(START.year, START.month, START.day, tzinfo=UTC)
    for k in range(0, 367):
        d = day0 + dt.timedelta(days=k)
        if variant == "06Z+22Z":
            hours = (6.0, 22.0)
        else:
            hours = (13.0 if k % 2 == 0 else 7.0,)
        out += [(d + dt.timedelta(hours=h), "pass") for h in hours]
        out.append((d + dt.timedelta(hours=9, minutes=7), "gate"))
    return sorted(e for e in out if START < e[0] < END)


@pytest.mark.parametrize("variant", ["06Z+22Z", "one a day, 13Z/07Z"])
def test_f3_a_virtual_year_has_no_red_data_day(tmp_path, monkeypatch, capsys, variant):
    rig = Rig(tmp_path, monkeypatch)
    us = rig.store.get_unit(SID, UID)
    assert not Strategy.is_due(rig.strat, rig.unit, us, now=dt.datetime(2025, 11, 24, 9, 7, tzinfo=UTC)), \
        "precondition: the regular schedule does not look before the clock fires (2025-11-24T00:00Z)"
    red, rechecks, passes = [], 0, 0
    for at, kind in _events(variant):
        if kind == "pass":
            passes += 1
            out = rig.run_pass(at, capsys)
            rechecks += len(re.findall(rf"\] RE-CHECK {SID}/{UID} - ", out))
            assert "RE-CHECK HELD" not in out, out[-1500:]
        elif rig.gate(at) == "RED-DATA":
            red.append(f"{at:%Y-%m-%dT%H:%MZ}")
    final_n = rig.store.get_unit(SID, UID)["last_obs_date"]
    print(f"{variant}: {passes} passes, {rig.fetches} fetches ({rig.fetches / 12.0:.2f} a month), {rechecks} by "
          f"the re-check, final n {final_n}, RED-DATA days {len(red)}")
    assert red == [], f"{len(red)} RED-DATA gate reads, first {red[:5]}"
    # not vacuous: every monthly value landed (Sep 2026's is out 2026-10-10; Oct's 2026-11-10, after the window),
    # and the re-check is what took some of them
    assert final_n == "2026-09-01"
    assert sorted(set(rig.n_seen)) == [f"{y}-{m:02d}-01" for y, m in
                                       [(2025, 10), (2025, 11), (2025, 12)] + [(2026, m) for m in range(1, 10)]]
    assert rechecks > 0


def test_f3_before_the_change_the_first_red_read_is_2025_11_24(tmp_path, monkeypatch, capsys):
    """The pencil for the old rule, on the same rig with the re-check switched OFF: the 2025-11-24T09:07Z read is the
    first RED-DATA one (11-23 reads OK). This pins the rig, so F3's 0 cannot come from a rig that never reddens."""
    monkeypatch.setattr(orchestrate, "recheck_verdict", lambda *a, **k: (False, None, None, None), raising=False)
    rig = Rig(tmp_path, monkeypatch)
    first = None
    for at, kind in _events("06Z+22Z"):
        if at > dt.datetime(2025, 11, 25, tzinfo=UTC):
            break
        if kind == "pass":
            rig.run_pass(at, capsys)
        elif rig.gate(at) == "RED-DATA" and first is None:
            first = at
    assert first == dt.datetime(2025, 11, 24, 9, 7, tzinfo=UTC), first
    assert rig.fetches == 0, "no regular check before 2025-12-03T17:47Z"


def test_r4c3_a_pass_that_comes_6_h_earlier_on_the_clock_day_still_rechecks(tmp_path, monkeypatch, capsys):
    """R4-C3, the case the spacing decides. ONE pass a day, alternating 13Z and 07Z (production's moving reach hour),
    and a LATE publisher: October's value is out 2025-11-23T14:00Z (23 d after the month), one hour after that day's
    13Z pass and ten hours before the clock (2025-11-24T00:00Z). The next pass is 2025-11-24T07:00Z, 18 h after the
    last attempt: a 0.5-day spacing (10.8 h) takes it and the 09:07Z gate reads OK; a 1-day spacing (21.6 h) refuses
    it and the gate reads RED-DATA - the mutant `spacing-one-day` is killed here, not by F3 (with lag 9 the first look
    inside the lead window already finds the value)."""
    rig = Rig(tmp_path, monkeypatch, lag_days=23.0)
    seen = []
    for k in range(9):                                           # 2025-11-17 .. 11-25
        d = dt.datetime(2025, 11, 17, tzinfo=UTC) + dt.timedelta(days=k)
        events = [(d + dt.timedelta(hours=13 if k % 2 == 0 else 7), "pass"),
                  (d + dt.timedelta(hours=9, minutes=7), "gate")]
        for at, kind in sorted(events):
            if kind == "pass":
                rig.run_pass(at, capsys)
            else:
                seen.append((f"{at:%m-%dT%H:%M}", rig.gate(at)))
    assert seen[7][0] == "11-24T09:07", seen
    assert all(h != "RED-DATA" for _t, h in seen), seen
    assert rig.store.get_unit(SID, UID)["last_obs_date"] == "2025-10-01" and rig.fetches >= 4


def test_g12_an_exception_in_the_recheck_never_ends_the_run(tmp_path, monkeypatch, capsys):
    """G12 (R4-C1): the planted unit's cursor read raises. run_once still reaches the next unit (band 2, after
    imf_commodity's band 1), the planted unit is NOT DUE and the pass names the error. Mutant: no try -> the
    exception leaves run_once and `zz_other` never runs."""
    other = Unit(source_id="zz_other", unit_id="_all", strategy="sim", cadence="daily", out_paths=[],
                 config={"run_location": "any", "live": True, "window_stop": False})
    at = dt.datetime(2025, 11, 20, 6, 0, tzinfo=UTC)                 # inside imf_commodity's lead window
    rig = Rig(tmp_path, monkeypatch, extra=[other])
    old = _iso(at - dt.timedelta(days=2))
    rig.store.upsert_unit("zz_other", "_all", strategy="sim", status="ok", last_success_utc=old, last_attempt_utc=old)
    rig.store.db.execute("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s) VALUES (?,?,?,?,1,?)",
                         (old, "zz_other", "_all", "ok", 900.0))
    rig.store.db.commit()
    real = rig.store.series_cursors

    def cursors(sid):
        if sid == SID:
            raise RuntimeError("cursor table unreadable")
        return real(sid)
    monkeypatch.setattr(rig.store, "series_cursors", cursors)
    out = rig.run_pass(at, capsys)
    assert rig.ran == ["zz_other"], out[-2000:]
    assert f"NOT DUE {SID}/{UID}" in out
    assert f"RE-CHECK HELD {SID}/{UID} - error RuntimeError" in out, out[-2000:]
    assert out.index(f"NOT DUE {SID}/{UID}") < out.index(">>> zz_other/_all"), "the planted unit came first"
