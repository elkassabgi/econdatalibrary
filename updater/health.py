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
from datetime import datetime, time as dtime, timedelta, timezone

from . import config, registry
from .state import StateStore
from .strategies.base import CADENCE_DAYS

# A source is RED once it is this many cadence-periods past its last success / newest obs.
SLA_TOLERANCE = 2.0
# Extra slack (in periods) for DATA recency, since a publication can legitimately lag a period.
DATA_SLACK_PERIODS = 1.0
ATTENTION_STATUSES = ("partial", "definitive_fail", "transient_fail", "running")
# How long an `upstream_verified` claim is trusted before it must be re-probed. A
# dataset that is finished today can resume publishing tomorrow, so the exemption it
# buys is deliberately perishable rather than permanent.
UPSTREAM_RECHECK_DAYS = 180.0
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
    # MUST match orchestrate._has_adapter's strategy list exactly. It previously
    # checked fetchers for only 2 of the orchestrator's 5 fetcher-backed
    # strategies, so ~35 unbuilt-fetcher sources (sdmx_delta / manual_vintage /
    # bulk_snapshot_if_changed) showed RED-UNRUN ("built but never ran") when the
    # orchestrator was correctly no-opping them as PENDING ("no adapter built").
    # Found 2026-07-14 ground-truthing the first-pass rollout: CI runs exited 0
    # while ingesting nothing.
    if strat in ("extend_by_date", "overwrite_if_changed", "sdmx_delta",
                 "manual_vintage", "bulk_snapshot_if_changed"):
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


def _business_age_days(iso, now) -> "float | None":
    """Age counted in BUSINESS days (Mon-Fri) — for daily-cadence market/FX
    sources that legitimately publish nothing on weekends. A Friday observation
    checked Monday 08:45 UTC is 3.04 calendar days old but only ~1 business day
    old; the calendar version red-flagged every live FX source every Monday
    morning (observed 2026-07-13: cnb + frankfurter, succ_age 0d, gate failure).
    Weekend days between the observation and now simply don't count."""
    cal = _age_days(iso, now)
    if cal is None:
        return None
    s = str(iso)
    if len(s) == 10:
        s += "T00:00:00+00:00"
    try:
        obs = datetime.fromisoformat(s)
    except Exception:
        return cal
    weekend = 0.0
    d = obs.date()
    while d < now.date():
        if d.weekday() >= 5:  # whole Sat/Sun days between obs and now
            weekend += 1.0
        d += timedelta(days=1)
    # ...and the PARTIAL day we are standing in. The loop stops at now.date(), so the
    # hours elapsed so far TODAY were previously counted as business time even when today
    # is a Saturday or Sunday. That lone gap re-created the exact false alarm this function
    # exists to prevent: on Sat 2026-07-25 an obs of Wed 07-22 scored 3.27 "business" days
    # and tripped the >3.0 daily gate, while bcrp and ofr in fact held their upstreams'
    # newest observation (both verified 2026-07-22 by direct query). Deducting today's
    # elapsed fraction makes weekend ages land on whole days, so a source that is level
    # with upstream stays quiet Sat/Sun and only goes RED once a business day passes it by.
    if now.weekday() >= 5:
        weekend += (now - datetime.combine(now.date(), dtime(0), tzinfo=now.tzinfo)).total_seconds() / 86400.0
    # Round before returning: cal and weekend are both fractions of a day, so their
    # difference lands on values like 3.0000000000000004 at some times of day but 3.0 at
    # others. Compared against a whole-day threshold that made the gate flap by the HOUR —
    # red at 13:00 Sunday, green at 02:00 and 23:00 the same day, on identical data.
    return round(max(0.0, cal - weekend), 6)


def assess(store=None) -> dict:
    store = store or StateStore()
    now = datetime.now(timezone.utc)
    reg = {e["source_id"]: e for e in registry.load().get("sources", [])}
    rows = []
    for sid, e in sorted(reg.items()):
        cadence = e.get("cadence", "monthly")
        period = CADENCE_DAYS.get(cadence, 7)
        sla_days = period * SLA_TOLERANCE

        # HOW OFTEN WE CHECK IS NOT HOW SOON DATA IS LATE. `period` comes from CADENCE_DAYS,
        # which drives SCHEDULING, and "irregular" is not a key there - it falls to the 7-day
        # default. That is right for checking (poll an unpredictable publisher weekly) and
        # wrong for lateness: it declares data RED after 21 days for sources that publish
        # every year or two. Judged that way, un_wpp went red 31 days after a release,
        # yale_epi at 578 days when the EPI is biennial, and gapminder and imf_weo on annual
        # data that was simply the publisher's own latest.
        #
        # This never mattered before because all four also carried forward-dated projections,
        # so their obs_age was negative and RED-DATA could not fire at all. Fixing the recency
        # signal (below) exposed the threshold underneath it. Correcting one without the other
        # would trade silent-green for four false reds - and the gate's own note says a
        # cry-wolf gate is how a real freeze gets ignored.
        #
        # So the LATENESS clock for an irregular publisher is a year: a genuine multi-year
        # freeze still turns red (365 * 3 = 1,095 days), while a normal annual or biennial
        # release cycle does not. Scheduling is untouched - CADENCE_DAYS still says 7.
        #
        # A source may also declare `data_cadence` — how often the PUBLISHER releases,
        # a different fact from `cadence` (how often WE poll). It affects ONLY the
        # lateness clock here; scheduling stays on CADENCE_DAYS[cadence].
        #
        # Forced by dst on 2026-08-02: its CHECK cadence had to go monthly -> daily so it
        # could converge on a publisher moving ~10 tables a day, but its DATA is monthly
        # (37 distinct obs_dates in the trailing 3 years). Judged on the check clock that
        # is a 3-day tolerance against a 62-day-old newest observation — a permanent false
        # red, and one that would have surfaced only once dst became HEALTHY, because
        # ATTENTION outranks the data check and was masking it.
        #
        # Every data_cadence in the registry is MEASURED (distinct obs_date over the
        # trailing 3 years, the number recorded in that entry's comment), never guessed.
        # This field CAN hide staleness, so declaring one without evidence is the abuse
        # case. It cuts both ways: a source polled annually that publishes monthly gets a
        # TIGHTER clock, not a looser one.
        LATENESS_PERIOD = {"irregular": 365}
        lat_cadence = e.get("data_cadence") or cadence
        data_days = (LATENESS_PERIOD.get(lat_cadence,
                                         CADENCE_DAYS.get(lat_cadence, period))
                     * (SLA_TOLERANCE + DATA_SLACK_PERIODS))

        src = store.get_source(sid)
        units = store.units_for_source(sid)
        cursors = store.series_cursors(sid)

        last_success = src.get("last_success_utc") if src else None
        succ_age = _age_days(last_success, now)

        attention = [f"{u['unit_id']}:{u['status']}" for u in units
                     if u.get("status") in ATTENTION_STATUSES]

        # DATA recency: newest obs across per-series cursors and unit-level last_obs.
        #
        # FORWARD-DATED PERIODS ARE NOT EVIDENCE OF RECENCY. Many sources legitimately publish
        # beyond today - ABS population/family/household projections run to 2046 and 2071,
        # UN WPP to 2101, IMF WEO forecasts to 2031, ABS CAPEX_EST records expected capital
        # expenditure a year out. Those rows are real data, not corruption (verified per
        # dataflow: dense annual runs, no nulls, plausible values). But taking max() over them
        # answers the wrong question. "Has this source received anything lately?" is about when
        # data ARRIVED; a projection to 2071 published in 2019 still reads 2071 for ever.
        #
        # Measured 2026-07-31: 28 of 93 units reported a frontier in the future, so their
        # obs_age was NEGATIVE and the staleness gate could never fire on them, whatever
        # happened upstream. abs sat at 2046-12-31 - 7,458 days "ahead" - while 805 of its
        # 1,222 sub-units were transient-failing.
        #
        # So recency is judged on the newest OBSERVED period, and the true frontier is kept
        # separately for display: the board should still be able to say the store reaches 2071.
        # If a unit holds nothing but forward-dated rows, recency is UNKNOWN (None) rather than
        # a fabricated "ahead of schedule" - last_success_utc still governs whether it ran.
        obs_vals = [v for v in cursors.values() if v]
        obs_vals += [u.get("last_obs_date") for u in units if u.get("last_obs_date")]
        today_iso = now.date().isoformat() if hasattr(now, "date") else str(now)[:10]
        observed = [v for v in obs_vals if str(v)[:10] <= today_iso]
        frontier = max(obs_vals) if obs_vals else None          # incl. projections, display only
        newest_obs = max(observed) if observed else None        # recency signal
        obs_age = _age_days(newest_obs, now)
        # staleness is judged in BUSINESS days for daily sources (FX/market feeds
        # publish nothing Sat/Sun — calendar age red-flagged every Monday morning);
        # calendar days for everything else. Display keeps calendar obs_age.
        eff_obs_age = _business_age_days(newest_obs, now) if cadence == "daily" else obs_age

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
        elif eff_obs_age is not None and eff_obs_age > data_days:
            # "Our newest observation is old" answers a question about US. The
            # question that matters is whether we are BEHIND THE PUBLISHER, and those
            # differ whenever a dataset is simply finished. IMF's Historical Public
            # Debt ends at 2015 and Fiscal Decentralization at 2020 — upstream's own
            # latest, matched exactly by our copy. Calling those RED says we are
            # missing data that does not exist, and a gate that cries wolf on
            # complete sources is how a real freeze gets ignored (this is R73 from
            # the other side: measure against the publisher, not against a clock).
            #
            # So a source may declare an upstream check, and it is an ASSERTION WITH
            # AN EXPIRY, never a permanent mute: it must name the date upstream
            # actually ends and when that was verified, and it lapses to ATTENTION
            # once stale so somebody re-probes instead of trusting a claim made years
            # ago. If our data ever falls BEHIND the declared upstream end, the
            # declaration stops applying and RED-DATA stands.
            # ONE MALFORMED ENTRY MUST NOT KILL THE ASSESSMENT FOR ALL 217 SOURCES. This read
            # was `.get()` on whatever the registry held, and ten entries carry
            # `upstream_verified` as a free-text NOTE rather than the structured claim, so the
            # gate died on the first one it met:
            #     AttributeError: 'str' object has no attribute 'get'   (health.py:226)
            # It had been reddening the 06:00 UTC run daily WITHOUT ASSESSING ANYTHING — and a
            # gate that crashes is worse than one that fails, because a red run reads as a
            # verdict about the data rather than about the gate.
            #
            # A non-dict cannot carry latest_obs/checked, so it cannot suppress RED-DATA. That
            # is the safe direction: suppression is the privileged outcome and must never be
            # granted by accident. It is SURFACED rather than swallowed, because a field in the
            # wrong shape is a defect someone should fix, not one to route around in silence.
            uv = e.get("upstream_verified")
            if uv is not None and not isinstance(uv, dict):
                attention = list(attention) + [
                    f"upstream_verified is {type(uv).__name__}, not a mapping with "
                    f"latest_obs/checked — cannot support a completeness claim, ignored"]
                uv = {}
            uv = uv or {}
            u_latest, u_checked = uv.get("latest_obs"), uv.get("checked")
            check_age = _age_days(u_checked, now) if u_checked else None
            if (u_latest and newest_obs and str(newest_obs) >= str(u_latest)
                    and check_age is not None):
                if check_age > UPSTREAM_RECHECK_DAYS:
                    health = "ATTENTION"  # claim has expired — re-verify upstream
                    attention = list(attention) + [
                        f"upstream_verified is {check_age:.0f}d old "
                        f"(>{UPSTREAM_RECHECK_DAYS:.0f}d) — re-probe {sid}"]
                else:
                    health = "OK"         # complete upstream, and we match it
            else:
                health = "RED-DATA"       # job 'succeeds' but our NEWEST data is stale
        else:
            health = "OK"

        rows.append({
            "source": sid, "strategy": e.get("strategy"), "cadence": cadence,
            # rollout perimeter flag (registry `live: true`) — the --fail-past-2x-sla
            # CI gate judges ONLY live-tier sources (§5.3)
            "live": bool(e.get("live", False)),
            # WHERE this source is supposed to run. The gate reads ONE state store, so a
            # source that runs somewhere else can only ever look unrun here (see
            # gate_failures).
            "run_location": e.get("run_location") or "cloud",
            "health": health,
            "last_success_age_d": round(succ_age, 1) if succ_age is not None else None,
            "newest_obs": newest_obs,
            # The furthest period the store holds, projections included. Distinct from
            # newest_obs on purpose: one answers "how current is this", the other "how far
            # does it reach". Equal for the ~70% of units that publish no forward data.
            "frontier_obs": frontier,
            "newest_obs_age_d": round(obs_age, 1) if obs_age is not None else None,
            # Surfaced so an OK that depends on an upstream claim is never silent —
            # a reader can see WHICH claim is holding the source green, and when it
            # was last checked.
            "upstream_verified": (e.get("upstream_verified") or None),
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
    actually committed to keep fresh.

    A GATE MUST NOT JUDGE WHAT IT CANNOT SEE. There is one state store per location,
    and 6 live sources declare `run_location: local` (bea, cepii_gravity, comtrade,
    noaa, ons_uk, wid). The cloud never runs them, so their rows in the cloud state
    are absent or frozen by construction and the CI gate red-flagged them EVERY day
    no matter how healthy they actually were — noaa sat in the failure list purely
    for this. That is how a gate stops being read: on 2026-08-02 it had been failing
    for three days straight while not assessing anything at all (R244), and nobody
    looked, because it was always red anyway.

    So the CLOUD gate declines to judge sources that run elsewhere. The narrowing is
    one-directional on purpose: a LOCAL invocation still judges everything, because a
    human running this by hand wants the full picture and a rule that hides rows from
    them would be the same mistake in a new place. Only the automated cloud gate — the
    one whose crying wolf is what stops getting read — is narrowed.

    This is deliberately NOT silence: main() prints every source it declined to judge
    under a heading saying so, because "we cannot see this from here" is a different
    statement from "this is fine", and only one of them is true."""
    bad = ("RED-SLA", "RED-DATA", "RED-UNRUN", "ATTENTION", "PENDING")
    return [f"{r['health']:<10} {r['source']}" for r in report["sources"]
            if r.get("live") and r["health"] in bad and _judged_here(r)]


def _judged_here(row: dict) -> bool:
    """False only when the CLOUD gate meets a source that runs somewhere else."""
    if execution_location() != "cloud":
        return True                      # local invocation judges everything
    return (row.get("run_location") or "cloud") == "cloud"


def execution_location() -> str:
    """Where this process runs: 'cloud' under the r2 backend (that IS the CI runner's
    configuration, AQUEDUCT_BACKEND=r2), 'local' otherwise. Used to decide which
    sources this gate is entitled to judge."""
    return "cloud" if getattr(config, "BACKEND", None) == "r2" else "local"


def unjudged_live(report: dict) -> list[str]:
    """Live sources this gate deliberately did NOT judge because they run elsewhere.
    Printed, never swallowed — see gate_failures."""
    return [f"{r['health']:<10} {r['source']:<18} runs={r.get('run_location') or 'cloud'}"
            for r in report["sources"]
            if r.get("live") and not _judged_here(r)]


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
        here = execution_location()
        # Print what this gate is NOT entitled to judge BEFORE its verdict, so the
        # verdict is never mistaken for a statement about those sources.
        skipped = unjudged_live(report)
        if skipped:
            print(f"\nNOT JUDGED HERE ({len(skipped)} live source(s) run elsewhere; this "
                  f"gate runs '{here}' and reads only the '{here}' state store).")
            print("  Their health below is what THIS store shows, which for a source that "
                  "runs elsewhere is stale or absent by construction —")
            print("  it is not evidence about them. They must be judged where they run.")
            for line in skipped:
                print(f"  {line}")
        fails = gate_failures(report)
        if fails:
            print(f"\nHEALTH GATE FAILED: {len(fails)} live-tier source(s) "
                  f"unhealthy (§5.3 — red run, GH notification):")
            for line in fails:
                print(f"  {line}")
        else:
            print(f"\nhealth gate OK: no live-tier '{here}' source past its SLA")
        sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
