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
import re as _re
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
STUCK_TRANSIENT_DAYS = 14   # actively attempted, failing, and this far from a success
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


def _recency_signal(obs_vals, now, has_uv_claim=False) -> "tuple[str | None, str | None]":
    """(frontier, newest_obs) from per-series cursors + unit last_obs dates.

    frontier   = max over everything, projections included — display only.
    newest_obs = the recency signal: max over values <= today — EXCEPT in the
    dead-minority-beside-projections shape, where it becomes None (UNKNOWN).

    A per-series cursor holds only that series' LAST period, so in a source
    whose living MAJORITY ends in a projection, the <=-today population is the
    DEAD series — and max() over them reports the death date of retired history
    as the store's currency. Measured 2026-08-25 (run 32816867502):
    unctad_poptotal held 918 live cursors at 2050-12-31 beside 36 dead ones
    ending 2011-12-31; the store carries every year 1950..2050, yet the gate
    printed newest_obs=2011-12-31 and RED-DATA'd three complete, current
    sources — the R244 crying-wolf class.

    The suppression fires ONLY when all three hold (adversarial review
    2026-08-25 — the first cut fired on 11 sources and broke two shapes):
      1. the frontier is forward-dated, AND
      2. the <=-today max is discontinued-old (> STALE_SERIES_DAYS), AND
      3. the <=-today cursors are a MINORITY (< half) of all values — the
         composition test that separates the artifact from a REAL freeze. In a
         true freeze every series' cursor is <= today (observed ~100%), so the
         red stands at any age and cannot self-clear by getting older;
         imf_psbs_direct (13,933 of 14,019 cursors <= today, max 2021 = the
         store's genuine currency, 86 projection cursors) keeps its honest max.
    And NEVER when the registry entry carries a structured `upstream_verified`
    claim (has_uv_claim): those sources (fao_ga/gb/ge/gr/gt/gy) earn their OK
    through a probed, EXPIRING claim (UPSTREAM_RECHECK_DAYS) — a structural,
    permanent suppression would silently revoke that deliberate perishability.

    Unchanged shapes: all-forward stays UNKNOWN; a finished dataset with no
    forward frontier (imf_hpd ends 2015) keeps its old max so RED-DATA still
    fires; a mixed store with a fresh <=-today max (abs) keeps its signal."""
    today_iso = now.date().isoformat() if hasattr(now, "date") else str(now)[:10]
    observed = [v for v in obs_vals if str(v)[:10] <= today_iso]
    frontier = max(obs_vals) if obs_vals else None      # incl. projections, display only
    newest_obs = max(observed) if observed else None    # recency signal
    if (not has_uv_claim
            and newest_obs and frontier and str(frontier)[:10] > today_iso
            and len(observed) * 2 < len(obs_vals)
            and (_age_days(newest_obs, now) or 0) > STALE_SERIES_DAYS):
        newest_obs = None
    return frontier, newest_obs


# The EXACT note finalize() emits for a zero-failure budgeted sweep, anchored end to end:
# 'N sub-unit(s) attempted, none failed; M deferred by budget and taken next tick[ ids...]'.
# attempted must be >= 1 ([1-9]) — a ZERO-attempt deferral note is a wedged rotator wearing
# the deferral costume (class-D breakage), never healthy rotation. Anything appended beyond
# the optional _named() id suffix — 'csv_derive crashed', a transient tail, a clipped note
# (orchestrate._clip_err announces truncation mid-string) — fails the anchor and the source
# stays ATTENTION: the WHITELIST direction, demanded by adversarial review 2026-08-26 after
# the first cut's substring BLACKLIST accepted unsdg's live note '46 sub-unit(s) attempted,
# none failed; 667 deferred by budget and taken next tick; csv_derive crashed (50564 series
# queued): UnitTimeout(...)' — a demoting tail the blacklist simply had no entry for (R382:
# a filter picked from the examples, not the mechanism).
_DEFERRAL_BASE = _re.compile(
    r"^[1-9]\d* sub-unit\(s\) attempted, none failed; "
    r"\d+ deferred by budget and taken next tick(?: \[[^\]]*\])?$")


def _deferral_only(units) -> bool:
    """True when EVERY attention-status unit is a pure budget-deferral partial —
    the note finalize() emits when a bounded sweep ran out of slice with ZERO
    failures ('N sub-unit(s) attempted, none failed; M deferred by budget and
    taken next tick', _common.py finalize; the drift test pins matcher to
    emitter, R349).

    WHY A CLASS OF ITS OWN. finalize books `partial` on any deferral because the
    tick did not cover everything — honest for the VINTAGE, but the gate turned
    that honesty into a permanent failure: a budget-bounded source (ecb 540
    files / 35 min, abs 1,222 units / 45 min...) can NEVER run deferral-free,
    so it is ATTENTION by construction, and on 2026-08-26 the CI gate carried
    22 such sources red on EVERY run — 40+ consecutive failures nobody could
    act on, which is exactly how gates stop being read (R244, R359: a check the
    median healthy source cannot pass measures its own policy, not the fleet).

    SAID PLAINLY, NOT OVERSOLD: ROTATING certifies that nothing FAILED and the
    budget stopped the sweep — it does NOT certify the rotation is advancing or
    that the un-reached tail is fresh. Neither did ATTENTION: tail-rot is
    invisible to every signal this module reads (newest_obs is a MAX, so one
    fresh head series keeps it green — the R379/R190 ecb episode). The honest
    instrument for rotation PROGRESS is a run-over-run position check
    (R377/R285: does run N+1 start where run N did not), which reads the runs
    table, not a state snapshot — still owed, tracked in TODO. Anything mixed
    in with the deferrals — a transient, a csv_derive failure, coherence unmet,
    missing cursors — keeps the source in ATTENTION and the gate."""
    att = [u for u in units if u.get("status") in ATTENTION_STATUSES]
    if not att:
        return False
    for u in att:
        if u.get("status") != "partial":
            return False
        err = str(u.get("last_error") or "")
        # 'csv coverage note:' tails are NON-failures by design (R372: budget-deferred
        # derive ids, proven-uncatalogued residue, an abandoned csv fence — none demote)
        # and may be joined onto the deferral note; strip them, then the remainder must
        # match the anchored emitter grammar EXACTLY.
        base = err.split("; csv coverage note:")[0].strip()
        if not _DEFERRAL_BASE.match(base):
            return False
    return True


def _stuck_transient(units, succ_age, sla_days, attempt_age) -> bool:
    """True when a "transient" failure has outlived any honest reading of the word.

    ATTENTION is evaluated BEFORE the age branches, so a status in ATTENTION_STATUSES
    pinned a source at amber no matter how long it had been failing. istat sat there for
    40 DAYS: attempted on every run, recorded transient_fail every time, never escalating,
    while its only working host had been dead since 2026-07-14. A label that predicts
    recovery must expire when recovery does not arrive, or it is just a way of not looking.

    Deliberately narrow, and the narrowness is the point:

      * only `transient_fail`, never `partial`. A big multi-unit source is permanently
        partial by design and NEVER sets last_success, so escalating on age would redden
        ecb, defillama, eia, ssb, sec_edgar, norgesbank, insee_melodi and stat_estonia -
        all eight measured FRESH on R2 today, ecb having written to the bucket this
        morning while its last_success read 38 days old. That is the wolf-crying this
        file already warns about, and it would bury the one real signal.

      * only when a real last_success EXISTS and is past SLA. A source that has never
        succeeded keeps its current handling; "never" is a different diagnosis from
        "stopped", and it is not this branch's job to conflate them.

    Measured blast radius when added: 4 sources carry transient_fail (istat, census,
    hagstofa, imf_imts_direct) and only istat has a real last_success, so exactly one
    classification changes.
    """
    if not any(u.get("status") == "transient_fail" for u in units):
        return False
    if succ_age is None:
        return False        # never succeeded is a different diagnosis; not this branch
    # Past its own SLA: unambiguous.
    if sla_days is not None and succ_age > sla_days:
        return True
    # Or: being ATTEMPTED and failing on every pass. This is the case cadence hides.
    # istat is monthly, so 2x SLA is 60 days and 40 days of continuous nightly failure
    # was still "within tolerance" - the clock said waiting, the reality was failing. A
    # source touched in the last two days that has not succeeded in fourteen is not
    # waiting for its cadence, it is broken.
    return attempt_age is not None and attempt_age <= 2 and succ_age > STUCK_TRANSIENT_DAYS


def assess(store=None) -> dict:
    store = store or StateStore()
    now = datetime.now(timezone.utc)
    # Sources whose SERVED CSV corpus is known-stale relative to the store (§5.7 debt
    # rows) — see the owed branch inside the loop.
    owed = {r["source_id"]: r for r in store.full_rederives_owed()}
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

        # HOW OLD IS THIS VERDICT? A `partial` never sets last_success_utc (R231), so the whole
        # ATTENTION column is a snapshot of whenever the source last RAN — which for many of
        # them is not recent. Measured 2026-08-02 on production state: of 54 partial sources,
        # 24 (44%) had not been attempted for 2+ days, the oldest 39 and 24 days, because the
        # daily run reaches ~20 of ~106 cloud sources within its budget (R246).
        #
        # Without this column a reader cannot tell "degraded now" from "last seen degraded
        # three weeks ago", and three sources I chased today — cso, insee_melodi and the
        # imf_*_direct family — turned out to be healthy upstream with a stale verdict. The
        # gate's severity ordering is unchanged; this only makes the age visible.
        # FALL BACK TO THE UNITS. The source-level row is written on SUCCESS, so a
        # permanently-partial source has no source_state row at all (R231) — which is exactly
        # the population this column exists to describe, and taking the source-level value
        # alone left it blank on every ATTENTION row. unit_state carries last_attempt_utc per
        # unit whether or not the unit succeeded, so the newest of those is when the source
        # was really last touched.
        _attempts = [u.get("last_attempt_utc") for u in units if u.get("last_attempt_utc")]
        last_attempt = (max(_attempts) if _attempts else None) \
            or (src.get("last_attempt_utc") if src else None) or last_success
        attempt_age = _age_days(last_attempt, now)

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
        # _recency_signal also refuses a dead-MINORITY max as the recency signal when the
        # frontier is forward-dated (unctad_pop*: the gate read retired series' 2011 end
        # date as the store's currency and RED-DATA'd three complete sources). Sources
        # with a structured upstream_verified claim keep the claim's expiring machinery.
        frontier, newest_obs = _recency_signal(
            obs_vals, now, has_uv_claim=isinstance(e.get("upstream_verified"), dict))
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

        # Pure budget-deferral partials rotate by design and are not failures — see
        # _deferral_only. A rotator SKIPS the attention/RED-UNRUN/RED-SLA branches (a
        # partial never sets last_success_utc, R231, so the age branches would re-redden
        # every healthy rotator and rebuild the disease) but deliberately FALLS THROUGH
        # to the data-recency branch below: the one real alarm the old undifferentiated
        # red carried was the TOTAL-FREEZE case, and a rotator whose newest observation
        # ages past its data clock must go RED-DATA, not ROTATING-green (adversarial
        # review 2026-08-26 — measured live: abs at 238d and ilostat at 147d against
        # 90-day clocks re-redden under exactly this rule). The upstream_verified
        # machinery in that branch applies to rotators unchanged — one predicate, one
        # place (R10).
        rotating = bool(attention) and _deferral_only(units)
        if src is None and not units:
            # never produced state: RED only if its adapter is built (should have run);
            # PENDING if the adapter isn't built yet (expected during the rollout).
            health = "RED-UNRUN" if _adapter_ready(e) else "PENDING"
        elif _stuck_transient(units, succ_age, sla_days, attempt_age):
            # Explicitly RED, not merely "not ATTENTION". Bypassing the amber branch let
            # istat fall through every age check and land on OK - 40 days broken and
            # reading green, which is worse than the amber it replaced.
            health = "RED-SLA"
        elif attention and not rotating:
            health = "ATTENTION"
        elif succ_age is None and not rotating:
            health = "RED-UNRUN"   # has state rows but never succeeded -> surface it
        elif not rotating and succ_age > sla_days:
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

        # A rotator that survived the data clock (or holds a live upstream_verified
        # claim) reads ROTATING, never a bare OK — the deferral state stays visible in
        # the table, the summary and the digest. NOTE: like the gate, `--red` excludes
        # ROTATING (it means "needs attention", and gate parity is deliberate).
        if rotating and health == "OK":
            health = "ROTATING"

        # A full_rederive_owed row means the source's SERVED CSV corpus is stale
        # relative to the store: a fetcher merged observations but reported no
        # series_cursors (orchestrate §5.7), so per-series derive queuing was
        # impossible while the vintage sidecar already marked the pull "done". No
        # other signal in this module can see that — newest_obs reads the STORE,
        # which is exactly the half that IS current; noaa served 3,138,159 CSVs one
        # restatement behind with every instrument green. The row is written by the
        # orchestrator when the debt is incurred and cleared ONLY by
        # derive_csv_bulk's zero-error campaign stamp, so the source is held at
        # ATTENTION (never green, never ROTATING) until a completed campaign proves
        # the debt paid. A worse verdict (RED-*) stands — the note still shows.
        owe = owed.get(sid)
        if owe:
            # PREPENDED, not appended: attention is truncated to 10 for display, and a
            # note that CHANGES THE VERDICT must survive the truncation — appended, a
            # ≥10-unit rotator would flip ATTENTION with its reason cut off (the
            # reviewer's finding; abs carried 805 attention units in R379's episode).
            attention = [
                f"full re-derive OWED since {str(owe.get('noted_utc') or '?')[:10]} "
                f"(store vintage {owe.get('vintage') or '?'}): served CSVs predate the "
                f"store — run tools/derive_csv_bulk.py --source {sid}; its zero-error "
                f"campaign stamp clears this row"] + list(attention)
            if health in ("OK", "ROTATING"):
                health = "ATTENTION"

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
            # Age of the RUN this verdict came from — see the comment where it is computed.
            "last_attempt_age_d": round(attempt_age, 1) if attempt_age is not None else None,
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

    # An owed row whose source has LEFT the registry must not vanish with it — the loop
    # above iterates registry entries only, so without this a de-registration silently
    # buries an un-repaid debt (the reviewer's finding; send_digest lists orphan state
    # rows for the same reason). `live: False` keeps the CI gate out of it — an
    # unregistered source is not a rollout commitment — but the row is in the table,
    # the summary and health.json where a reader will meet it.
    for sid in sorted(set(owed) - set(reg)):
        owe = owed[sid]
        rows.append({
            "source": sid, "strategy": None, "cadence": "unknown", "live": False,
            "run_location": "unknown", "health": "ATTENTION",
            "last_success_age_d": None, "last_attempt_age_d": None,
            "newest_obs": None, "frontier_obs": None, "newest_obs_age_d": None,
            "upstream_verified": None, "n_series_tracked": 0, "n_discontinued": 0,
            "discontinued_series": [],
            "attention": [
                f"full re-derive OWED since {str(owe.get('noted_utc') or '?')[:10]} but "
                f"{sid} is NOT in registry.yaml — the debt survives de-registration; "
                f"either re-derive and clear it, or clear it deliberately with the "
                f"retirement"],
        })

    order = {"RED-SLA": 0, "RED-DATA": 1, "RED-UNRUN": 2, "ATTENTION": 3, "PENDING": 4,
             "ROTATING": 5, "OK": 6}
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


# A route is DOWN once nothing on it has succeeded in this many days. Deliberately generous:
# the claim is "that machine has stopped reporting entirely", not "one source is late".
ROUTE_SILENCE_DAYS = 3.0


def route_silence(report: dict) -> "list[str]":
    """Routes that have delivered NO successful run at all lately.

    WHY THIS EXISTS. gate_failures correctly refuses to judge sources that run elsewhere —
    a gate must not pronounce on runs it cannot see. But "not judged here" plus "not judged
    anywhere else" adds up to NOT JUDGED. The workstation route is the ONLY update path for
    the 17 cloud-infeasible sources, `eia` among them on a DAILY cadence, and if that machine
    stops nothing in CI says so.

    WHAT THIS DOES AND DOES NOT CATCH — stated plainly, because I first wrote it believing it
    would have caught the 2026-08-02 outage and it would NOT. That day the guard loop died at
    15:16 and the local heavy pass went ~7h past due, but bis, bls, cepii_gravity and faostat
    still carried successes from the day before, so no three-day threshold could have fired.
    A short outage on a route whose pass runs every ~20h is INVISIBLE here by construction,
    and the instrument for it is the guard's own heartbeat on that machine, not this.

    What this catches is the sustained case: the machine is off, or wedged, for days, and the
    whole route stops delivering. That is the genuinely dangerous version — a 10-hour gap
    costs one deferred pass, a 10-day gap silently rots 6.7M series — and it was equally
    invisible before.

    THE CLAIM IS DIFFERENT FROM THE ONE gate_failures DECLINES TO MAKE, which is what makes it
    legitimate from the cloud. We cannot tell whether `noaa` is healthy. We CAN tell that the
    shared state store holds no successful local run in three days: a statement about the
    ROUTE, about whether that machine reports at all, not about any source on it.

    Two guards against crying wolf, since a gate that fires wrongly is one nobody reads:
      * every source on the route must be silent — one fresh success clears it, because a
        single stalled source is a SOURCE problem, judged where it runs;
      * the route must have succeeded at some point. A route where nothing has EVER succeeded
        is unconfigured, not silent, and would otherwise be permanently red from day one.
    """
    now = datetime.now(timezone.utc)
    routes: dict = {}
    for r in report["sources"]:
        if not r.get("live"):
            continue
        loc = r.get("run_location") or "cloud"
        if loc == execution_location():
            continue                       # this gate judges its own route source-by-source
        routes.setdefault(loc, []).append(r)

    out = []
    for loc, rows in sorted(routes.items()):
        ages = [r.get("last_success_age_d") for r in rows]
        fresh = [a for a in ages if a is not None and a <= ROUTE_SILENCE_DAYS]
        if fresh:
            continue                       # something on that route reported recently
        seen = [a for a in ages if a is not None]
        if not seen:
            continue                       # never configured, not gone silent — see above
        newest = f"{min(seen):.1f}d ago"
        # SAY WHETHER THE MACHINE IS RUNNING. "Not one has succeeded" is true and, on its own,
        # misleading: a multi-unit source is permanently `partial` by design and partial never
        # sets last_success, so a route can be attempting every night and still read as
        # silent. Distinguishing the two costs one line and no new data — and getting it
        # wrong sent a whole investigation down the wrong path, which started from this
        # sentence and concluded the machine had stopped (R625/R629).
        attempts = [r.get("last_attempt_age_d") for r in rows]
        recent_attempts = [a for a in attempts if a is not None and a <= ROUTE_SILENCE_DAYS]
        if recent_attempts:
            colour = (f"{len(recent_attempts)} of them WERE ATTEMPTED within "
                      f"{ROUTE_SILENCE_DAYS:.0f}d (newest attempt {min(recent_attempts):.1f}d "
                      f"ago), so the route is running and those sources are not reaching a "
                      f"state that sets last_success — a different diagnosis from a stopped "
                      f"machine, and one this gate cannot resolve from here.")
        else:
            colour = ("NONE of them was even attempted in that window, so the route itself "
                      "has stopped reporting: check the machine's guard loop and its "
                      "heartbeat.")
        out.append(
            f"ROUTE '{loc}' SILENT — {len(rows)} live source(s) run there and NOT ONE has "
            f"succeeded within {ROUTE_SILENCE_DAYS:.0f}d (newest success: {newest}). "
            f"{colour}")
    return out


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
    print(f"{'HEALTH':<13} {'SOURCE':<18} {'CAD':<9} {'succ_age':>8} {'ran':>6} {'obs_age':>8}  newest_obs  notes")
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
        ra = "" if r.get("last_attempt_age_d") is None else f"{r['last_attempt_age_d']:.0f}d"
        print(f"{r['health']:<13} {r['source']:<18} {r['cadence'][:8]:<9} {sa:>8} {ra:>6} {oa:>8}  "
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
        # ...but "we cannot judge those sources" must not become "we have nothing to say about
        # that machine". A route that has gone completely silent is a fact THIS gate can
        # establish from the shared state, and it is the failure that hid a ten-hour
        # workstation outage on 2026-08-02 (see route_silence).
        silent = route_silence(report)
        for line in silent:
            print(f"\n{line}")
        fails = gate_failures(report)
        if fails:
            print(f"\nHEALTH GATE FAILED: {len(fails)} live-tier source(s) "
                  f"unhealthy (§5.3 — red run, GH notification):")
            for line in fails:
                print(f"  {line}")
        elif silent:
            print(f"\nHEALTH GATE FAILED: no '{here}' source is past its SLA, but "
                  f"{len(silent)} route(s) have stopped reporting entirely (above).")
        else:
            print(f"\nhealth gate OK: no live-tier '{here}' source past its SLA, "
                  f"and every other route is still reporting")
        sys.exit(1 if (fails or silent) else 0)


if __name__ == "__main__":
    main()
