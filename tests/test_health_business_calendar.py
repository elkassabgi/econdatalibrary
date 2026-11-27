"""A declared publisher holiday calendar keeps a holiday out of the daily data clock - and nothing else changes.

2026-11-27: ofr read RED-DATA on the 06Z gate the day after Thanksgiving while it held the Office of Financial
Research's own newest observation (2026-11-24, checked against OFR's API that morning). The daily clock is 3
business days, and health._business_age_days deducted only Saturdays and Sundays, so the holiday counted: obs
11-24 at Fri 09:30Z scored 3.4. The same false red comes back on Mon 2026-12-28 and Mon 2027-01-04.
`business_calendar: us_federal_reserve` (updater/calendars.py) declares the publisher's holidays.

Pinned here:
  1. the rule date by date for 2026, 2027 and 2028 (hand-derived lists, not computed by the rule);
  2. the rule against the publisher's own data in both directions (tests/fixtures/business_calendar/), with
     two controls that must FAIL the same check (one holiday removed; the federal-employee Saturday->Friday
     shift);
  3. the ofr cases through assess(): 2026-11-27 with newest 11-24 and 11-25, 2026-12-28, 2027-01-04 - each with
     a negative control (the declaration removed -> the old false RED) and a real lag that still goes RED;
  4. no change without the declaration: the function equals the pre-change implementation over a grid, and
     cnb (daily, no declaration) reads the same verdict as before on 2026-11-27;
  5. the registry: a known name only, only on a daily source, each declaration with a MEASURED comment.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
from datetime import date, timedelta

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import calendars, config, health, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402

REGISTRY = os.path.join(ROOT, "updater", "registry.yaml")
FIXTURE = os.path.join(ROOT, "tests", "fixtures", "business_calendar", "ofr_weekday_closures.json")
UTC = dt.timezone.utc
FED = calendars.US_FEDERAL_RESERVE


def D(s):
    return date.fromisoformat(s)


def at(s):
    """A UTC instant from 'YYYY-MM-DD HH:MM'."""
    return dt.datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)


# ---- 1. the rule, date by date (derived by hand from 5 U.S.C. 6103(a) and a calendar, NOT by the code) ----

EXPECTED = {
    2026: ["2026-01-01", "2026-01-19", "2026-02-16", "2026-05-25", "2026-06-19",
           "2026-09-07", "2026-10-12", "2026-11-11", "2026-11-26", "2026-12-25"],
    # Juneteenth (Sat 06-19) and Christmas (Sat 12-25) close no weekday; Independence Day (Sun 07-04) -> Mon 07-05.
    2027: ["2027-01-01", "2027-01-18", "2027-02-15", "2027-05-31", "2027-07-05",
           "2027-09-06", "2027-10-11", "2027-11-11", "2027-11-25"],
    # New Year's Day (Sat 01-01) and Veterans Day (Sat 11-11) close no weekday.
    2028: ["2028-01-17", "2028-02-21", "2028-05-29", "2028-06-19", "2028-07-04",
           "2028-09-04", "2028-10-09", "2028-11-23", "2028-12-25"],
}


@pytest.mark.parametrize("year", sorted(EXPECTED))
def test_the_rule_names_exactly_these_weekdays(year):
    assert sorted(d.isoformat() for d in calendars.us_federal_reserve(year)) == EXPECTED[year]


def test_saturday_holidays_close_no_friday_and_the_open_days_stay_open():
    closed = calendars.holiday_test(FED)
    for open_day in ("2026-07-03",            # Independence Day is Saturday 2026-07-04 (OFR has data on 07-03)
                     "2027-06-18", "2027-12-24", "2027-12-31", "2028-11-10",   # Saturday holidays
                     "2026-11-27", "2027-11-26",   # the day after Thanksgiving (OFR has data every year)
                     "2026-12-24", "2026-12-31",   # Christmas Eve / New Year's Eve
                     "2026-04-03", "2027-03-26"):  # Good Friday
        assert not closed(D(open_day)), open_day
    assert all(d.weekday() < 5 for y in range(2016, 2031) for d in calendars.us_federal_reserve(y))


# ---- 2. the rule against the publisher's own data ----

def _fixture():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)


def _mismatches(rule, block):
    """(rule days WITH data, data-less weekdays the rule MISSES) over the block's window."""
    lo, hi = (D(x) for x in block["window"])
    closed = {D(x) for x in block["weekdays_without_any_value"]}
    named = {d for y in range(lo.year, hi.year + 1) for d in rule(y) if lo <= d <= hi}
    return sorted(named - closed), sorted(closed - named)


def _opm_rule(year):
    """The federal-EMPLOYEE rule (a Saturday holiday is taken the Friday before) - the control that must fail."""
    out = set(calendars.us_federal_reserve(year))
    for d in calendars.federal_holidays(year):
        if d.weekday() == 5:
            out.add(d - timedelta(days=1))
    return out


def _drop_one(name):
    """The rule with one holiday taken out (on its actual date, or the Monday a Sunday one is closed)."""
    def rule(year):
        gone = {d + timedelta(days=1) if d.weekday() == 6 else d
                for d, n in calendars.federal_holidays(year).items() if n == name}
        return set(calendars.us_federal_reserve(year)) - gone
    return rule


@pytest.mark.parametrize("block", ["ofr", "nyfed_equivalent"])
def test_the_rule_is_the_publishers_own_closures_in_both_directions(block):
    b = _fixture()[block]
    assert len(b["weekdays_without_any_value"]) >= 80, "the fixture must cover years of data"
    assert _mismatches(calendars.us_federal_reserve, b) == ([], [])


@pytest.mark.parametrize("block", ["ofr", "nyfed_equivalent"])
def test_control_the_saturday_to_friday_shift_is_refuted_by_the_data(block):
    b = _fixture()[block]
    wrong, missed = _mismatches(_opm_rule, b)
    fridays = b["saturday_holiday_fridays_series_with_a_value"]
    assert wrong and missed == []
    assert wrong == sorted(D(f) for f, n in fridays.items() if n > 0)


@pytest.mark.parametrize("dropped", ["Thanksgiving Day", "Columbus Day", "Juneteenth National Independence Day"])
def test_control_a_rule_missing_one_holiday_fails_the_same_check(dropped):
    wrong, missed = _mismatches(_drop_one(dropped), _fixture()["ofr"])
    assert wrong == [] and missed, f"dropping {dropped} must leave data-less weekdays the rule misses"


# ---- 3. the ofr cases through assess() ----

def _freeze(monkeypatch, now):
    class _Now(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is not None else now.replace(tzinfo=None)
    monkeypatch.setattr(health, "datetime", _Now)


def _without_calendar(monkeypatch, sid):
    """Re-read the registry with `sid`'s business_calendar removed (assess() reloads it on every call)."""
    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    for e in reg.get("sources"):
        if e.get("source_id") == sid:
            popped = e.pop("business_calendar", None)
            assert popped == FED, f"{sid} must declare the calendar, or this control removes nothing"
    monkeypatch.setattr(yaml, "safe_load", lambda *a, **k: reg)


def _report(monkeypatch, held, now):
    """assess() at the frozen instant `now`; each {sid: newest} in `held` succeeded at `now` holding `newest`."""
    _freeze(monkeypatch, now)
    st = StateStore(path=":memory:")
    for sid, newest in held.items():
        st.upsert_source(sid, status="ok", last_success_utc=now.isoformat(), last_attempt_utc=now.isoformat())
        st.upsert_unit(sid, "_all", status="ok", last_obs_date=newest, last_attempt_utc=now.isoformat())
    return health.assess(store=st)


def _health(monkeypatch, sid, newest, now):
    """The verdict of `sid` in _report()."""
    return next(r for r in _report(monkeypatch, {sid: newest}, now)["sources"] if r["source"] == sid)["health"]


FRI_1127 = at("2026-11-27 09:30")     # the 06Z slot's gate ran at about 09:30Z on 2026-11-27
MON_1228 = at("2026-12-28 09:30")
MON_0104 = at("2027-01-04 09:30")


@pytest.mark.parametrize("newest, with_cal, without_cal", [
    ("2026-11-24", 2.395833, 3.395833),   # what ofr held: the false RED (3.4 > 3) is gone
    ("2026-11-25", 1.395833, 2.395833),   # one more day in the store: OK either way
])
def test_ofr_2026_11_27_ages_both_ways(newest, with_cal, without_cal):
    assert health._business_age_days(newest, FRI_1127, FED) == with_cal
    assert health._business_age_days(newest, FRI_1127) == without_cal


def test_ofr_2026_11_27_holding_11_24_is_not_red(monkeypatch):
    assert _health(monkeypatch, "ofr", "2026-11-24", FRI_1127) == "OK"


def test_negative_control_without_the_calendar_11_24_is_the_old_false_red(monkeypatch):
    _without_calendar(monkeypatch, "ofr")
    assert _health(monkeypatch, "ofr", "2026-11-24", FRI_1127) == "RED-DATA"


@pytest.mark.parametrize("newest, age", [
    ("2026-11-23", 3.395833),   # one business day behind what OFR held (11-24): still RED
    ("2026-11-20", 4.395833),   # two business days behind (11-23, 11-24 missing): RED
])
def test_ofr_a_real_lag_still_turns_red_with_the_calendar(monkeypatch, newest, age):
    assert health._business_age_days(newest, FRI_1127, FED) == age
    assert _health(monkeypatch, "ofr", newest, FRI_1127) == "RED-DATA"


@pytest.mark.parametrize("now, held, behind", [
    (MON_1228, "2026-12-23", "2026-12-22"),   # Christmas Fri 12-25 (12-24 is a business day)
    (MON_0104, "2026-12-30", "2026-12-29"),   # New Year's Day Fri 2027-01-01
])
def test_ofr_the_two_forecast_recurrences(monkeypatch, now, held, behind):
    assert health._business_age_days(held, now, FED) == 2.395833
    assert health._business_age_days(held, now) == 3.395833          # the forecast false RED
    assert health._business_age_days(behind, now, FED) == 3.395833   # a real day behind
    assert _health(monkeypatch, "ofr", held, now) == "OK"
    assert _health(monkeypatch, "ofr", behind, now) == "RED-DATA"
    monkeypatch.undo()
    _without_calendar(monkeypatch, "ofr")
    assert _health(monkeypatch, "ofr", held, now) == "RED-DATA"      # negative control


def test_the_saturday_after_thanksgiving_reads_like_any_saturday():
    """Sat 06Z holding Tue (one business day behind OFR's Wed) is 3.0, the value a normal week gives a store one
    business day behind on a Saturday; without the calendar it was 4.0."""
    sat = at("2026-11-28 06:00")
    assert health._business_age_days("2026-11-24", sat, FED) == 3.0
    assert health._business_age_days("2026-11-24", sat) == 4.0
    assert health._business_age_days("2026-11-25", sat, FED) == 2.0
    assert health._business_age_days("2026-11-18", at("2026-11-21 06:00")) == 3.0   # a normal Saturday


def test_today_a_holiday_counts_like_today_a_saturday():
    """The partial day we stand in is not business time on a holiday, as on a weekend."""
    assert health._business_age_days("2026-11-24", at("2026-11-26 09:30"), FED) == 2.0
    assert health._business_age_days("2026-11-24", at("2026-11-26 09:30")) == 2.395833


def test_the_row_and_the_table_say_which_calendar_the_clock_applied(monkeypatch, capsys, tmp_path):
    """obs_age stays calendar days (3.4 for ofr on 2026-11-27); data_clock_age_d is the age the verdict used,
    and business_calendar names the calendar it applied - None for a daily source without one (cnb) and for a
    source that is not daily (fhfa, weekly)."""
    rep = _report(monkeypatch, {"ofr": "2026-11-24", "cnb": "2026-11-24", "fhfa": "2026-10-01"}, FRI_1127)
    rows = {r["source"]: r for r in rep["sources"]}
    assert (rows["ofr"]["health"], rows["ofr"]["business_calendar"]) == ("OK", FED)
    assert (rows["ofr"]["newest_obs_age_d"], rows["ofr"]["data_clock_age_d"]) == (3.4, 2.4)
    assert (rows["cnb"]["health"], rows["cnb"]["business_calendar"]) == ("RED-DATA", None)
    assert (rows["cnb"]["newest_obs_age_d"], rows["cnb"]["data_clock_age_d"]) == (3.4, 3.4)
    assert rows["fhfa"]["business_calendar"] is None
    assert rows["fhfa"]["data_clock_age_d"] == round(rows["fhfa"]["newest_obs_age_d"], 1) == 57.4
    assert all(r.get("business_calendar") in (None, FED) for r in rep["sources"])
    # the table: the mark on ofr's line, and on no other line
    monkeypatch.setattr(health, "assess", lambda *a, **k: rep)
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["updater.health"])
    health.main()
    lines = capsys.readouterr().out.splitlines()
    marked = [ln for ln in lines if "bcal=" in ln]
    assert len(marked) == 1 and " ofr " in marked[0] and "bcal=us_federal_reserve:2.4bd" in marked[0], marked
    assert (tmp_path / "health.json").exists()


# ---- 4. nothing changes without the declaration ----

def _old_business_age_days(iso, now):
    """health._business_age_days as it was before business_calendar (main 4a5bf725), verbatim apart from names."""
    cal = health._age_days(iso, now)
    if cal is None:
        return None
    s = str(iso)
    if len(s) == 10:
        s += "T00:00:00+00:00"
    try:
        obs = dt.datetime.fromisoformat(s)
    except Exception:
        return cal
    weekend = 0.0
    d = obs.date()
    while d < now.date():
        if d.weekday() >= 5:
            weekend += 1.0
        d += timedelta(days=1)
    if now.weekday() >= 5:
        weekend += (now - dt.datetime.combine(now.date(), dt.time(0), tzinfo=now.tzinfo)).total_seconds() / 86400.0
    return round(max(0.0, cal - weekend), 6)


def test_without_a_calendar_the_function_is_the_old_one():
    """Every holiday of 2026-11 .. 2027-01 inside the grid, both obs forms, three times of day, and the ways a
    declaration can be absent or unrecognised (all of them must read weekends only)."""
    obs_days = [date(2026, 11, 1) + timedelta(days=i) for i in range(72)]          # 11-01 .. 01-11
    now_days = [date(2026, 11, 1) + timedelta(days=i) for i in range(77)]          # 11-01 .. 01-16
    times = (dt.time(6, 0), dt.time(9, 30), dt.time(23, 59, 59))
    n = 0
    for o in obs_days:
        for nd in now_days:
            for t in times:
                now = dt.datetime.combine(nd, t, tzinfo=UTC)
                for iso in (o.isoformat(), o.isoformat() + "T00:00:00+00:00"):
                    old = _old_business_age_days(iso, now)
                    for cal in ((), (None,), ("us_federal",)):
                        assert health._business_age_days(iso, now, *cal) == old, (iso, now, cal)
                    n += 1
    assert n == 72 * 77 * 3 * 2
    for o in obs_days[::7]:
        for cal in ("", True, ["us_federal_reserve"], {"name": FED}):
            assert health._business_age_days(o.isoformat(), FRI_1127, cal) == \
                _old_business_age_days(o.isoformat(), FRI_1127), (o, cal)
    for bad in (None, "", "not-a-date"):
        assert health._business_age_days(bad, FRI_1127, FED) == _old_business_age_days(bad, FRI_1127)


def test_with_the_calendar_the_only_difference_is_the_holidays():
    for o in (date(2026, 11, 1) + timedelta(days=i) for i in range(72)):
        for nd in (date(2026, 11, 1) + timedelta(days=i) for i in range(77)):
            now = dt.datetime.combine(nd, dt.time(9, 30), tzinfo=UTC)
            hol = [d for d in (o + timedelta(days=k) for k in range(max(0, (nd - o).days)))
                   if d.weekday() < 5 and d in calendars.us_federal_reserve(d.year)]
            today = 0.395833 if nd.weekday() < 5 and nd in calendars.us_federal_reserve(nd.year) else 0.0
            old = _old_business_age_days(o.isoformat(), now)
            new = health._business_age_days(o.isoformat(), now, FED)
            assert new == pytest.approx(max(0.0, old - len(hol) - today), abs=2e-6), (o, nd)


def test_a_daily_source_without_the_declaration_reads_as_before(monkeypatch):
    """cnb (Czech National Bank) declares no calendar: on 2026-11-27 its verdict is the old one."""
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    assert reg["cnb"].get("cadence") == "daily" and "business_calendar" not in reg["cnb"]
    assert _health(monkeypatch, "cnb", "2026-11-24", FRI_1127) == "RED-DATA"
    assert _health(monkeypatch, "cnb", "2026-11-25", FRI_1127) == "OK"


# ---- 5. the registry ----

def _reg(**fields):
    e = {"source_id": "zz", "strategy": "extend_by_date", "cadence": "daily", "live": False}
    e.update(fields)
    return {"sources": [e]}


def _cal_problems(reg):
    return [p for p in registry.validate(reg) if "business_calendar" in p]


def test_validate_accepts_the_known_calendar_on_a_daily_source():
    assert _cal_problems(_reg(business_calendar=FED)) == []
    assert _cal_problems(_reg()) == []


@pytest.mark.parametrize("bad", ["us_federal", "US_FEDERAL_RESERVE", "", None, True, ["us_federal_reserve"]])
def test_validate_refuses_an_unknown_calendar(bad):
    assert _cal_problems(_reg(business_calendar=bad))


@pytest.mark.parametrize("cadence", ["weekly", "monthly", "irregular"])
def test_validate_refuses_a_calendar_nothing_would_read(cadence):
    """Only a daily source is aged in business days, so the declaration would be read by nothing (R202)."""
    assert _cal_problems(_reg(business_calendar=FED, cadence=cadence))


def _declarations():
    text = open(REGISTRY, encoding="utf-8").read().splitlines()
    out, sid = [], None
    for i, ln in enumerate(text):
        m = re.match(r"^- source_id:\s*(\S+)", ln)
        if m:
            sid = m.group(1)
        m = re.match(r"^  business_calendar:\s*(.*)$", ln)
        if m:
            j = i
            while j > 0 and text[j - 1].lstrip().startswith("#"):
                j -= 1
            out.append((sid, m.group(1).strip(), "\n".join(text[j:i])))
    return out


def test_the_registry_declares_it_for_ofr_and_nyfed_only_each_measured():
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    declared = {sid: e["business_calendar"] for sid, e in reg.items() if "business_calendar" in e}
    assert declared == {"ofr": FED, "nyfed": FED}
    decl = _declarations()
    assert sorted(s for s, _, _ in decl) == ["nyfed", "ofr"]
    for sid, raw, above in decl:
        assert raw == FED, (sid, raw)
        assert "MEASURED" in above, f"{sid}: business_calendar without a MEASURED comment above it"
    probs = registry.validate(registry.load(), expected_count=config.EXPECTED_SOURCE_COUNT)
    assert [p for p in probs if "business_calendar" in p] == []
