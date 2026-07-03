"""updater/health.py — freshness / staleness monitor (CONTINUOUS_UPDATE_DESIGN.md section 7).

Reads ONLY the StateStore (+ registry for cadence expectations). This is the layer
that makes silent staleness impossible to miss: a source past its SLA, a source
whose newest observation has drifted beyond its cadence, or a unit stuck in
partial/failed — each surfaces RED even when the last job "succeeded". It is the
enforcement behind the project's core promise (continuously-updated, no whack-a-mole)
and the standing answer to the "a quiet no_change looks fresh forever" hazard:
freshness is judged on DATA recency (last_obs drift), not just on last_success.

  python -m updater.health           # colored table + write data/_aqueduct/health.json
  python -m updater.health --json    # emit JSON only
  python -m updater.health --red     # only show sources needing attention; exit 1 if any
"""
from __future__ import annotations
import json
import sys
from datetime import datetime, timezone

from . import config, registry
from .state import StateStore
from .strategies.base import CADENCE_DAYS

# A source is RED once it is this many cadence-periods past its last success / newest obs.
SLA_TOLERANCE = 2.0
# Extra slack (in periods) for DATA recency, since a publication can legitimately lag a period.
DATA_SLACK_PERIODS = 1.0
ATTENTION_STATUSES = ("partial", "definitive_fail", "transient_fail", "running")
# Per-series staleness uses a CONSERVATIVE absolute floor rather than the source
# cadence: a single source mixes frequencies (a monthly series inside a "daily"
# source is not stale at >3 days). >2 years with no new obs is genuinely dead for
# any frequency up to annual, so it flags real death without false-alarming slow series.
STALE_SERIES_DAYS = 730


def _adapter_ready(e):
    """True if this source's strategy is implemented AND (for fetcher-backed strategies)
    its per-source fetcher exists — i.e. it SHOULD be producing state. A source whose
    adapter isn't built yet is 'pending' (expected during rollout), not a failure; a source
    that IS built but has produced no state is a real problem (RED-UNRUN)."""
    from .strategies import REGISTRY as SREG
    from .strategies.fetchers import implemented as fimpl
    strat = e.get("strategy")
    if strat not in SREG:
        return False
    if strat in ("extend_by_date", "overwrite_if_changed"):
        return fimpl(e.get("source_id"))
    return True


def _age_days(iso, now):
    """Age in days; FUTURE-dated values (forecasts/auction calendars like CBO GDPPOT
    to 2036 or Treasury auctions) are not stale -> clamped to 0 (fresh)."""
    if not iso:
        return None
    s = str(iso)
    if len(s) == 10:  # a bare YYYY-MM-DD obs_date
        s += "T00:00:00+00:00"
    try:
        age = (now - datetime.fromisoformat(s)).total_seconds() / 86400.0
    except Exception:
        return None
    return max(0.0, age)


def assess(store=None) -> dict:
    store = store or StateStore()
    now = datetime.now(timezone.utc)
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    rows = []
    for sid, e in sorted(reg.items()):
        cadence = e.get("cadence", "monthly")
        period = CADENCE_DAYS.get(cadence, 7)
        sla_days = period * SLA_TOLERANCE
        data_days = period * (SLA_TOLERANCE + DATA_SLACK_PERIODS)

        src = store.get_source(sid)
        units = store.units_for_source(sid)
        cursors = store.series_cursors(sid)

        last_success = src.get("last_success_utc") if src else None
        succ_age = _age_days(last_success, now)

        attention = [f"{u['unit_id']}:{u['status']}" for u in units
                     if u.get("status") in ATTENTION_STATUSES]

        # DATA recency: newest obs across per-series cursors and unit-level last_obs.
        obs_vals = [v for v in cursors.values() if v]
        obs_vals += [u.get("last_obs_date") for u in units if u.get("last_obs_date")]
        newest_obs = max(obs_vals) if obs_vals else None
        obs_age = _age_days(newest_obs, now)

        # Discontinued series: individual series with no new obs in >2 years. These are
        # INFORMATIONAL only (old currencies like ATS/BEF/ECU, retired indicators) — a
        # source actively publishing fresh data is healthy even while it carries dead
        # history, so this never by itself makes a source RED (else every long-history
        # FX source would be permanently red). A genuine source-wide freeze is caught by
        # RED-DATA (newest_obs drift) instead.
        discontinued = sorted(k for k, v in cursors.items()
                              if (a := _age_days(v, now)) is not None and a > STALE_SERIES_DAYS)

        if src is None and not units:
            # never produced state: RED only if its adapter is built (should have run);
            # PENDING if the adapter isn't built yet (expected during the rollout).
            health = "RED-UNRUN" if _adapter_ready(e) else "PENDING"
        elif attention:
            health = "ATTENTION"
        elif succ_age is None:
            health = "RED-UNRUN"   # has state rows but never succeeded -> surface it
        elif succ_age > sla_days:
            health = "RED-SLA"            # job hasn't succeeded within tolerance
        elif obs_age is not None and obs_age > data_days:
            health = "RED-DATA"           # job 'succeeds' but the source's NEWEST data has gone stale
        else:
            health = "OK"

        rows.append({
            "source": sid, "strategy": e.get("strategy"), "cadence": cadence,
            # rollout perimeter flag (registry `live: true`) — the --fail-past-2x-sla
            # CI gate judges ONLY live-tier sources (§5.3)
            "live": bool(e.get("live", False)),
            "health": health,
            "last_success_age_d": round(succ_age, 1) if succ_age is not None else None,
            "newest_obs": newest_obs,
            "newest_obs_age_d": round(obs_age, 1) if obs_age is not None else None,
            "n_series_tracked": len(cursors),
            "n_discontinued": len(discontinued),
            "discontinued_series": discontinued[:10],
            "attention": attention[:10],
        })

    order = {"RED-SLA": 0, "RED-DATA": 1, "RED-UNRUN": 2, "ATTENTION": 3, "PENDING": 4, "OK": 5}
    rows.sort(key=lambda r: (order.get(r["health"], 9), r["source"]))
    summary = {}
    for r in rows:
        summary[r["health"]] = summary.get(r["health"], 0) + 1
    return {"generated_utc": now.isoformat(timespec="seconds"),
            "sla_tolerance_periods": SLA_TOLERANCE, "summary": summary, "sources": rows}


def gate_failures(report: dict) -> list[str]:
    """The --fail-past-2x-sla CI gate (UPDATER_BUILD_PLAN.md §5.3): live-tier
    sources in a failing state. RED-SLA / RED-DATA = past 2x SLA (job or data);
    RED-UNRUN / ATTENTION = failed or never-ran while live; PENDING = live with
    no adapter (mirrors the orchestrator's run-failure rule). Sources outside
    the rollout perimeter (`live: true` in registry.yaml) never fail the gate —
    they surface in the table but the rollout is judged only on what we have
    actually committed to keep fresh."""
    bad = ("RED-SLA", "RED-DATA", "RED-UNRUN", "ATTENTION", "PENDING")
    return [f"{r['health']:<10} {r['source']}" for r in report["sources"]
            if r.get("live") and r["health"] in bad]


_KNOWN_ARGS = {"--json", "--red", "--fail-past-2x-sla"}


def main():
    import os
    # Refuse unknown flags LOUDLY. This module used to ignore unrecognized
    # arguments, which made `--fail-past-2x-sla` in updater-daily.yml a silent
    # exit-0 no-op — the exact silent-pass §5.3 exists to prevent. Never again.
    unknown = [a for a in sys.argv[1:] if a not in _KNOWN_ARGS]
    if unknown:
        print(f"updater.health: unknown argument(s) {unknown}; "
              f"known: {sorted(_KNOWN_ARGS)}", file=sys.stderr)
        sys.exit(2)
    report = assess()
    config.ensure_dirs()
    out = os.path.join(config.STATE_DIR, "health.json")
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    if "--json" in sys.argv:
        print(json.dumps(report["summary"]))
        print(f"wrote {out}")
        return

    red_only = "--red" in sys.argv
    bad = ("RED-SLA", "RED-DATA", "RED-UNRUN", "ATTENTION")
    print(f"=== Aqueduct health  {report['generated_utc']}  ===")
    print("summary:", json.dumps(report["summary"]))
    print(f"{'HEALTH':<13} {'SOURCE':<18} {'CAD':<9} {'succ_age':>8} {'obs_age':>8}  newest_obs  notes")
    for r in report["sources"]:
        if red_only and r["health"] not in bad:
            continue
        note = ""
        if r["attention"]:
            note = "attn=" + ",".join(r["attention"])
        elif r["n_discontinued"]:
            note = f"{r['n_discontinued']} discontinued series (info)"
        sa = "" if r["last_success_age_d"] is None else f"{r['last_success_age_d']:.0f}d"
        oa = "" if r["newest_obs_age_d"] is None else f"{r['newest_obs_age_d']:.0f}d"
        print(f"{r['health']:<13} {r['source']:<18} {r['cadence'][:8]:<9} {sa:>8} {oa:>8}  "
              f"{str(r['newest_obs'] or '-'):<11} {note}")

    if "--red" in sys.argv:
        n = sum(report["summary"].get(k, 0) for k in bad)
        sys.exit(1 if n else 0)

    if "--fail-past-2x-sla" in sys.argv:
        fails = gate_failures(report)
        if fails:
            print(f"\nHEALTH GATE FAILED: {len(fails)} live-tier source(s) "
                  f"unhealthy (§5.3 — red run, GH notification):")
            for line in fails:
                print(f"  {line}")
        else:
            print("\nhealth gate OK: no live-tier source past its SLA")
        sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
