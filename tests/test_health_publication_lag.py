"""A publisher's measured release lag widens the data clock - and only by what was measured.

fhfa read RED-DATA at 85 days on 2026-09-23 while holding FHFA's latest release exactly. Measured from
FHFA's calendar and the store's REAL dating (monthly rows at month START, quarterly at quarter END), its
newest observation is 89-122 days old on release day, ~131 at worst once intra-day release time, weekly
run granularity and one failed run are added, against an 84-day monthly clock. `publication_lag_days` declares the allowance. It can hide staleness,
so health.py clamps it (a real number, <= 2 data periods) and every declaration needs a MEASURED comment.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import health  # noqa: E402
from updater.state import StateStore  # noqa: E402

REGISTRY = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "updater", "registry.yaml")


def _row(newest_age_days):
    """assess() for a fabricated fhfa that succeeded just now with its newest obs this old."""
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    newest = (now.date() - dt.timedelta(days=newest_age_days)).isoformat()
    st.upsert_source("fhfa", status="ok", last_success_utc=now.isoformat(),
                     last_attempt_utc=now.isoformat())
    st.upsert_unit("fhfa", "_all", status="ok", last_obs_date=newest,
                   last_attempt_utc=now.isoformat())
    return next(r for r in health.assess(store=st)["sources"] if r["source"] == "fhfa")


def _with_fhfa(monkeypatch, **fields):
    """Re-read the registry with fhfa's lag replaced (assess() reloads it on every call)."""
    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    src = reg if isinstance(reg, list) else reg.get("sources")
    for e in src:
        if e.get("source_id") == "fhfa":
            e.pop("publication_lag_days", None)
            e.update(fields)
    monkeypatch.setattr(yaml, "safe_load", lambda *a, **k: reg)


def test_the_measured_worst_case_is_not_red():
    """122 on the 2027-08-31 release day + 1 intra-day + 7 run granularity (a failed run eats less)."""
    assert _row(130)["health"] == "OK", _row(130)


def test_a_genuine_freeze_still_turns_red():
    """84 (the monthly clock) + 47 (the declared allowance): past it is stale, lag or no lag."""
    assert _row(84 + 47 + 1)["health"] == "RED-DATA"


def test_negative_control_without_the_lag_the_same_age_is_red(monkeypatch):
    _with_fhfa(monkeypatch)
    assert _row(130)["health"] == "RED-DATA", "the allowance, and only it, is what clears it"


def test_fhfa_is_judged_on_its_monthly_data_not_its_weekly_polling():
    """cadence weekly would make the clock 21 days; data_cadence keeps it on the data."""
    assert _row(60)["health"] == "OK"


def test_the_code_clamps_what_a_registry_typo_could_mute(monkeypatch):
    for bad in (float("inf"), 1e9):
        _with_fhfa(monkeypatch, publication_lag_days=bad)
        assert _row(84 + 2 * 28 + 1)["health"] == "RED-DATA", f"{bad!r} must clamp to 2 periods"
        monkeypatch.undo()
    for bad in ("62", True, None, [5]):
        _with_fhfa(monkeypatch, publication_lag_days=bad)
        assert _row(90)["health"] == "RED-DATA", f"{bad!r} is not a number and must be ignored"
        monkeypatch.undo()


def _declarations():
    text = open(REGISTRY, encoding="utf-8").read().splitlines()
    out = []
    for i, ln in enumerate(text):
        m = re.match(r"^  publication_lag_days:\s*(.*)$", ln)
        if m:
            # the comment block directly above the field (contiguous '#' lines)
            j = i
            while j > 0 and text[j - 1].lstrip().startswith("#"):
                j -= 1
            out.append((i + 1, m.group(1).strip(), "\n".join(text[j:i])))
    return out


def test_every_declaration_is_a_bounded_measured_number():
    reg = yaml.safe_load(open(REGISTRY, encoding="utf-8"))
    src = reg if isinstance(reg, list) else reg.get("sources")
    entries = {e["source_id"]: e for e in src if "publication_lag_days" in e}
    decl = _declarations()
    assert len(decl) == len(entries) >= 1, (decl, sorted(entries))
    for line, raw, above in decl:
        assert "MEASURED" in above, f"registry.yaml:{line}: publication_lag_days without a MEASURED comment"
    for sid, e in entries.items():
        lag = e["publication_lag_days"]
        assert isinstance(lag, (int, float)) and not isinstance(lag, bool) and lag >= 0, (sid, lag)
        cap = health.publication_lag_cap(e.get("data_cadence") or e["cadence"])
        assert lag <= cap, f"{sid}: {lag} days is over the {cap:g}-day cap - the code would clamp it"
