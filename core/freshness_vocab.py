"""The public freshness vocabulary: "late", "paused", "retired" (owner decisions, Ahmed, 2026-10-20).

WHAT THE API SAID BEFORE. /v1/sources freshness.status is source_state.status, which the updater writes ONLY
on a success (R231): a source that keeps running and ends `partial` every time keeps reading "ok" from its
last success for ever (istat read "ok" 98 days after it). /v1/last-updates printed whatever unit_state held,
and promised a next update for sources nothing runs any more (ksh: 2027-06-24). The three decisions:

  late     a source that keeps running but whose last full success is older than 2x its cadence. The
           clock is the health gate's own: updater.strategies.base.CADENCE_DAYS (irregular = 7) times
           updater.health.SLA_TOLERANCE, compared with `>` as health.py's RED-SLA branch does. Applied
           at the SOURCE grain: /v1/sources freshness.status, and every one of the source's unit rows on
           /v1/last-updates whose stored status is ok / no_change / partial / transient_fail.
           Two exceptions, for sources whose currency a success stamp cannot show, both DECLARED in the
           registry and both judged on the LATEST pass, which must have run within the same window:
             * `rotating: true` (ecb): a budget rotator. Current while its latest pass REFRESHED: a budget
               deferral that failed nothing, a pass in which fewer than half of the attempted sub-units
               transient-failed, or a completed fetch - whatever CSV-phase tail follows (ROTATION_* below).
               Late when that pass died whole (transient_fail), refused structurally, failed half or more
               of what it attempted, or ran outside the window.
             * `served_by: lane` (statcan): current while the reporter's latest verdict says the lane is
               current (updater/strategies/fetchers/statcan.py LANE_CURRENT_PATTERN).
           The grammar alone is not enough and is not used alone: cso's latest note is a pure deferral
           too ("4 sub-unit(s) attempted, none failed; 10740 deferred ..."), and cso is late.
  paused   a registry `paused:` key (insee_melodi today). From the registry, never from a state row, so
           nothing has to be deleted on un-pause. next_update_expected is null.
  retired  a RULE, not a list: not in updater/discontinued.yaml, not paused, and named by NO run
           mechanism - tools/audit_schedule_coverage.scheduled_sources() (registry live:true, the
           updater-heavy matrix, sec-edgar-daily, run_location: local), including the entries it calls
           stranded (on a schedule but missing an adapter: a first-pass crawler is not retired), plus
           registry `served_by: lane` and the D1-only writers (core.sync_state_d1.DATA_THROUGH_FROM_D1).
           next_update_expected is null.

Precedence: discontinued (core/sync_state_d1.py publishes it) > paused > retired > late > the stored value.

THE WORKER IS THE PRODUCTION READER. It cannot read the registry at run time, so
tools/gen_freshness_classes.py writes what it needs from classes() into api/worker/src/freshnessClasses.ts,
and tests/test_freshness_vocab.py fails when that file is stale. source_label() here is the same rule in
Python: the dev shim (api/devserver.py) uses it, and a parity test runs both on the same cases. The classes
are NOT filtered by the redistribution gate (see classes()); every reader applies the gate first.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

LATE = "late"
PAUSED = "paused"
RETIRED = "retired"
DISCONTINUED = "discontinued"
# Stored statuses that "late" may replace. Source grain: what source_state holds (written on success).
LATE_FROM_SOURCE = ("ok", "no_change")
# Unit grain: the statuses a RUNNING source's unit can carry.
LATE_FROM_UNIT = ("ok", "no_change", "partial", "transient_fail")
_UNIT_OK = ("ok", "no_change")

# A DECLARED ROTATOR'S PASS REFRESHED THE SOURCE (registry `rotating: true`). The openings of the notes
# updater/strategies/fetchers/_common.finalize() writes - tests/test_freshness_vocab.py runs finalize() and
# pins each one - matched at the START of the stored note only, so the CSV phase's tails do not decide it.
#
# WHY NOT health._deferral_only's exact grammar (the first version of this rule, review R1649): replayed hour
# by hour on ecb's passes 2026-09-20..10-20 it read late 146 of 736 hours (19.8%) - 101 of them after a pass
# where 1 or 2 of 30-340 files answered HTTP 504, 39 after a clean deferral whose note carried a "csv
# coherence unmet" tail. Neither says ecb stopped refreshing: a failed file is retried on the next pass, and
# coherence-unmet names changed store keys with no catalogue row (ecb is catalog_scope: subset - 35 served
# ids) - a CSV-phase signal the health gate keeps showing as ATTENTION. The source is late when its pass
# REFRESHED NOTHING worth the name: a whole-pass crash (status transient_fail), a structural refusal, half
# or more of the attempted sub-units failing, or no pass inside the window. "Half" is a choice, not a
# measurement: ecb's failing passes failed 1-2 sub-units (2/68, 2/30, 1/134), a refusing upstream fails all.
ROTATION_DEFERRAL = r"^[1-9]\d* sub-unit\(s\) attempted, none failed; \d+ deferred by budget"
ROTATION_SOME_FAILED = r"^([1-9]\d*)/([1-9]\d*) sub-unit\(s\) transient-failed; will retry"
ROTATION_COMPLETED = r"^(?:\+\d+ new rows|no new rows)(?:;|$)"


def classes() -> dict:
    """Everything the rule needs that is not in a state row, read from the code (never typed here).

    {"sla_tolerance", "cadence_days", "paused": {sid: reason}, "run_by", "discontinued", "lane", "rotating",
     "lane_current": regex, "rotation_deferral" / "rotation_some_failed" / "rotation_completed": regexes}
    """
    import yaml

    from core import sync_state_d1
    from tools import audit_schedule_coverage as cov
    from updater import health
    from updater.strategies import base
    from updater.strategies.fetchers import statcan

    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))
    entries = {e["source_id"]: e for e in reg["sources"]}
    disc = sync_state_d1._discontinued_ids(os.path.join(ROOT, "updater", "discontinued.yaml"))

    paused = {}
    for sid, e in entries.items():
        pz = e.get("paused")
        if isinstance(pz, str) and pz.strip():
            paused[sid] = pz.strip()
    lane = {sid for sid, e in entries.items() if e.get("served_by") == "lane"}
    rotating = {sid for sid, e in entries.items() if e.get("rotating") is True}
    scheduled, _why = cov.scheduled_sources()
    stranded = set(getattr(cov.scheduled_sources, "stranded", {}) or {})
    run_by = set(scheduled) | stranded | lane | set(sync_state_d1.DATA_THROUGH_FROM_D1)

    # NOT filtered by the redistribution gate, on purpose. Every class here is a function of public files
    # (the registry, discontinued.yaml, the workflows). Filtering would make the generated file differ from
    # those files by exactly the gated registered ids - publishing the gated set by absence. Instead
    # tests/test_freshness_vocab.py asserts that no class holds a gated id at all (true today), so the file
    # names none; a gated source that gains a runner fails that test and needs a decision. The worker drops
    # gated rows before it reads any class, so membership would change no answer either way.
    def keep(ids):
        return sorted(ids)

    return {
        "sla_tolerance": float(health.SLA_TOLERANCE),
        "cadence_days": {k: int(v) for k, v in base.CADENCE_DAYS.items()},
        "paused": {s: paused[s] for s in keep(paused)},
        "run_by": keep(run_by - set(paused)),
        "discontinued": keep(disc),
        "lane": keep(lane),
        "rotating": keep(rotating),
        "lane_current": statcan.LANE_CURRENT_PATTERN,
        "rotation_deferral": ROTATION_DEFERRAL,
        "rotation_some_failed": ROTATION_SOME_FAILED,
        "rotation_completed": ROTATION_COMPLETED,
    }


def _ts(v) -> dt.datetime | None:
    if not v:
        return None
    try:
        t = dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def _over(ts, days: float, now: dt.datetime) -> bool | None:
    """True when `ts` is more than `days` before `now`; None when `ts` is absent or unreadable."""
    t = _ts(ts)
    if t is None:
        return None
    return (now - t).total_seconds() / 86400.0 > days


def rotation_pass_refreshed(note: str, cls: dict) -> bool:
    """A declared rotator's partial pass refreshed the source (see ROTATION_* above): a clean deferral, a pass
    where fewer than half of the attempted sub-units transient-failed, or a completed fetch the CSV phase
    demoted. Judged on the note's opening only."""
    note = str(note or "")
    if re.match(cls["rotation_deferral"], note) or re.match(cls["rotation_completed"], note):
        return True
    m = re.match(cls["rotation_some_failed"], note)
    return bool(m) and int(m.group(1)) * 2 < int(m.group(2))


def latest_pass_current(kind: str, units: list[dict], window_days: float, now: dt.datetime, cls: dict) -> bool:
    """The declared source's LATEST pass says it is current (kind "rotating" or "lane"), and it ran within the
    window. Every unit must say so."""
    if not units:
        return False
    for u in units:
        st = u.get("status")
        if st in _UNIT_OK:
            continue
        if st != "partial":
            return False
        note = str(u.get("last_error") or "")
        if kind == "lane":
            if not re.match(cls["lane_current"], note):
                return False
        elif not rotation_pass_refreshed(note, cls):
            return False
    attempts = [u.get("last_attempt_utc") for u in units if _ts(u.get("last_attempt_utc"))]
    if not attempts:
        return False
    return _over(max(attempts, key=_ts), window_days, now) is False


def source_label(sid: str, src: dict | None, units: list[dict], now: dt.datetime, cls: dict) -> str | None:
    """The new status for a whole source, or None (keep what is stored).

    `src` is its source_state row (status, last_success_utc, cadence) or None; `units` its unit_state rows
    (status, last_error, last_attempt_utc, last_success_utc)."""
    s_status = (src or {}).get("status")
    if s_status == DISCONTINUED or sid in set(cls["discontinued"]):
        return None
    if sid in cls["paused"]:
        return PAUSED
    if sid not in set(cls["run_by"]):
        return RETIRED
    if s_status not in LATE_FROM_SOURCE:
        return None
    period = cls["cadence_days"].get((src or {}).get("cadence") or "")
    if period is None:
        return None
    window = period * cls["sla_tolerance"]
    # The newest success anyone recorded for it: a source whose source_state is stamped by its own
    # refresher (sec_edgar, R1193) can hold an older unit row, and that row is not the source's truth.
    succ = [t for t in [(src or {}).get("last_success_utc")] + [u.get("last_success_utc") for u in units]
            if _ts(t)]
    if not succ or _over(max(succ, key=_ts), window, now) is not True:
        return None
    if sid in set(cls["rotating"]) and latest_pass_current("rotating", units, window, now, cls):
        return None
    if sid in set(cls["lane"]) and latest_pass_current("lane", units, window, now, cls):
        return None
    return LATE


def unit_status(stored: str | None, label: str | None) -> str | None:
    """What /v1/last-updates prints for one unit of a source whose source_label() is `label`."""
    if label in (PAUSED, RETIRED):
        return label
    if label == LATE and stored in LATE_FROM_UNIT:
        return LATE
    return stored


def promises_next_update(label: str | None) -> bool:
    """paused and retired promise nothing: next_update_expected is null for them."""
    return label not in (PAUSED, RETIRED)
