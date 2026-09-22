"""Which sources have been `partial` for longer than their own cadence allows?

THE GAP THIS FILLS, stated precisely, because the obvious framing is wrong. `updater.health` is NOT
blind to these: `partial` is in ATTENTION_STATUSES, every one of them is printed as ATTENTION, and
the age is in the table. What ATTENTION does not do is DISTINGUISH. A source that went partial on
this morning's run and a source that has been partial for ninety days land in the same bucket, and
on 2026-09-22 that bucket held 25 entries.

The non-escalation is deliberate and should stay. `updater/health.py::_stuck_transient` escalates
"only `transient_fail`, never `partial`", because "a big multi-unit source is permanently partial
by design and NEVER sets last_success, so escalating on age would redden" it - the wolf-crying the
gate exists to avoid. So this tool changes no verdict and is not a gate. It answers one question:

    of the sources currently partial, WHICH have been so for more than SLA_TOLERANCE cadence
    periods, and for how long?

Measured when this was written: 26 units were `partial` with `last_success_utc` NULL, of which
eight had last been attempted 30 days or more earlier - 90, 44, 36, 35, 34, 32, 32 and 30 days.
The 90-day one reads "1/165 sub-unit(s) transient-failed; will retry"; a transient failure that has
persisted three months is not transient. One is a LIVE DAILY source at 30 days, i.e. 30x its own
cadence.

It reuses `CADENCE_DAYS` and `SLA_TOLERANCE` from the modules that own them rather than restating
either - a hand-restated threshold is how the schedule-coverage figure went wrong three times
(R143, R157, R142).

    python tools/audit_stuck_partials.py
    python tools/audit_stuck_partials.py --min-periods 4
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config                                    # noqa: E402
from updater.health import SLA_TOLERANCE                      # noqa: E402
from updater.state import StateStore                          # noqa: E402
from updater.strategies.base import CADENCE_DAYS              # noqa: E402


def _age_days(stamp, now):
    if not stamp:
        return None
    try:
        t = dt.datetime.fromisoformat(str(stamp))
    except ValueError:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.timezone.utc)
    return (now - t).total_seconds() / 86400.0


def stuck_partials(units, cadences, now, min_periods=SLA_TOLERANCE):
    """[(source, unit, age_days, cadence, multiple)] for partials past min_periods of cadence.

    A unit with no attempt timestamp is returned with age None rather than dropped: "I cannot tell
    how long" is not "it is fine", and a silently dropped row is how a sweep reports a clean half.
    """
    out = []
    for u in units:
        if (u.get("status") or "") != "partial":
            continue
        age = _age_days(u.get("last_attempt_utc"), now)
        sid = u.get("source_id")
        known = sid in cadences
        cad = cadences.get(sid) or "NOT-IN-REGISTRY"
        if age is None:
            out.append((sid, u.get("unit_id"), None, cad, None))
            continue
        if not known:
            # NO MULTIPLE for a source the registry does not list. The first version fell back to
            # the 7-day default and printed "12.9x its cadence" for sources that have no cadence
            # at all - a fabricated ratio, stated with the same confidence as a real one. Report
            # the age, which is measured, and refuse the ratio, which is not.
            out.append((sid, u.get("unit_id"), age, cad, None))
            continue
        period = CADENCE_DAYS.get(cad, 7)
        if age > period * min_periods:
            out.append((sid, u.get("unit_id"), age, cad, age / period))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-periods", type=float, default=SLA_TOLERANCE,
                    help=f"how many cadence periods before a partial counts as stuck "
                         f"(default {SLA_TOLERANCE}, the gate's own SLA_TOLERANCE)")
    a = ap.parse_args()

    from updater import registry
    # EXACTLY health.py:277-280's shape. `registry.load()` returns a DICT whose "sources" key holds
    # the list; iterating the dict itself yields KEY STRINGS, and a first version of this did that,
    # silently produced an empty cadence map, and defaulted every source to the 7-day fallback -
    # wrong multiples reported with total confidence. Default "monthly" to match health.py.
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    if not reg:
        print("REFUSING: parsed ZERO sources from the registry - every cadence would fall back to "
              "the default and the multiples below would be fiction", file=sys.stderr)
        return 2
    cadences = {sid: (e.get("cadence") or "monthly") for sid, e in reg.items()}

    store = StateStore(os.path.join(config.STATE_DIR, "state.db"))
    units = store.all_units() if hasattr(store, "all_units") else []
    if not units:
        print("REFUSING: read ZERO units from the state store - an empty read is 'I could not "
              "look', not 'nothing is stuck'", file=sys.stderr)
        return 2

    now = dt.datetime.now(dt.timezone.utc)
    partials = [u for u in units if (u.get("status") or "") == "partial"]
    rows = stuck_partials(units, cadences, now, a.min_periods)
    unknown = [r for r in rows if r[2] is None]
    aged = sorted((r for r in rows if r[2] is not None), key=lambda r: -r[2])

    print(f"units in the state store : {len(units)}")
    print(f"currently `partial`      : {len(partials)}")
    print(f"partial past {a.min_periods:g} cadence periods : {len(aged)}"
          f"   (age unknown: {len(unknown)})")
    if aged:
        print(f"\n{'source':<34} {'unit':<10} {'age':>7} {'cadence':<10} {'x cadence':>10}")
        for src, unit, age, cad, mult in aged:
            m = f"{mult:>9.1f}x" if mult is not None else f"{'n/a':>10}"
            print(f"{src:<34} {str(unit):<10} {age:>6.0f}d {cad:<16} {m}")
    for src, unit, _a, cad, _m in unknown:
        print(f"{src:<34} {str(unit):<10}      ?  {cad:<10}  no attempt timestamp")
    print("\nThis is NOT a gate and changes no verdict. `updater.health` already reports every one "
          "of these as ATTENTION; what it does not do is separate a partial from this run from a "
          "partial from three months ago.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
