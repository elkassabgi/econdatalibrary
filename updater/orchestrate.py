"""The orchestrator: due-check -> change-detect -> dispatch -> record state.

Drives every registry unit through its strategy, honoring leases (no double-runs),
the Transient/Definitive failure contract, and the first-pass protection (never
touch the 3 in-flight backfill jobs or their data). A unit is marked `ok` only
after its strategy reports a successful atomic publish.
"""
from __future__ import annotations
import os
import time

from . import registry
from .state import StateStore, now_utc
from .strategies import get as get_strategy
from .strategies.fetchers import implemented as fetcher_implemented
from .errors import TransientError, DefinitiveError

# In-flight first-pass backfills — NEVER touched by updates. These are the
# clean_full/<dir> names the running jobs write to (cbs_nl, gus_dbw, and the
# DBnomics-ISTAT re-pull which writes clean_full/dbnomics/). We match on BOTH
# source_id and the unit's output directory so a registry source that maps onto
# one of these dirs can never slip through (the source_id alone was brittle).
FIRSTPASS_DIRS = {"cbs_nl", "gus_dbw", "dbnomics"}


def _protected(unit) -> bool:
    if unit.source_id in FIRSTPASS_DIRS:
        return True
    for p in (unit.out_paths or []):
        if os.path.basename(str(p).rstrip("/\\")) in FIRSTPASS_DIRS:
            return True
    return False


# Per-process owner token so leases are genuinely exclusive across runners.
OWNER = f"orch-{os.getpid()}"

# Lease TTL scaled to how long a unit may legitimately run, so a slow large/giant
# pull can't have its lease expire mid-run and let a second runner in.
_TTL_BY_COST = {"fast": 7200, "medium": 7200, "large": 43200, "giant": 172800}


def _ttl(unit) -> int:
    return _TTL_BY_COST.get((unit.config or {}).get("refresh_cost"), 7200)


def _has_adapter(unit) -> bool:
    """True if this unit's strategy is runnable now (strategy registered, and for
    extend_by_date the per-source fetcher exists). Lets Phase-3 roll out incrementally."""
    try:
        get_strategy(unit.strategy)
    except KeyError:
        return False
    if unit.strategy in ("extend_by_date", "overwrite_if_changed", "sdmx_delta",
                         "manual_vintage", "bulk_snapshot_if_changed"):
        return fetcher_implemented(unit.source_id)
    return True


def run_once(sources=None, strategies=None, cadences=None, force=False, dry=False, store=None):
    store = store or StateStore()
    # Validate the registry up front — fail loudly rather than silently run a
    # malformed/incomplete registry (the documented coverage gate).
    problems = registry.validate(registry.load())
    if problems:
        raise SystemExit("registry invalid (fix before running):\n  " + "\n  ".join(problems[:20]))
    units = registry.all_units()
    results = []
    skipped_no_adapter = []
    for unit in units:
        if sources and unit.source_id not in sources:
            continue
        if strategies and unit.strategy not in strategies:
            continue
        if cadences and unit.cadence not in cadences:
            continue
        if _protected(unit):
            continue  # protected in-flight backfill (by source_id or output dir)
        if not _has_adapter(unit):
            skipped_no_adapter.append(unit.source_id)
            continue  # adapter not built yet (incremental rollout)

        us = store.get_unit(unit.source_id, unit.unit_id)
        strat = get_strategy(unit.strategy)

        if not force and not strat.is_due(unit, us):
            continue

        try:
            vintage = strat.detect_change(unit, us)
        except TransientError as e:
            _record(store, unit, "transient_fail", err=f"detect:{e}")
            results.append((unit.key, "transient_fail"))
            continue
        if not force and vintage is None:
            continue  # upstream unchanged

        if dry:
            results.append((unit.key, "due"))
            continue

        if not store.claim_lease(unit.key, owner=OWNER, ttl_s=_ttl(unit)):
            results.append((unit.key, "locked"))
            continue

        t0 = time.time()
        try:
            res = strat.run(unit, since=(us or {}).get("last_obs_date"))
            ok = res.status in ("ok", "no_change")
            # last_obs_date must never regress (a run that wrote only some units can
            # report a lower max than what's already stored).
            old_last = (us or {}).get("last_obs_date")
            new_last = res.last_obs_date or old_last
            if old_last and new_last and str(new_last) < str(old_last):
                new_last = old_last
            store.upsert_unit(
                unit.source_id, unit.unit_id, strategy=unit.strategy,
                # only advance the recorded vintage on a clean success — a partial/failed
                # run must NOT bump it, or the next detect_change would skip a source that
                # never fully fetched (silent staleness on partials).
                upstream_vintage=((res.new_vintage or vintage) if ok else (us or {}).get("upstream_vintage")),
                status=res.status,
                last_obs_date=new_last,
                obs_count=(res.obs if res.obs else (us or {}).get("obs_count")),
                last_success_utc=(now_utc() if ok else (us or {}).get("last_success_utc")),
                last_attempt_utc=now_utc(), last_error=res.error)
            if res.series_cursors:
                store.put_series_cursors(unit.source_id, res.series_cursors)  # per-series freshness
            if ok:
                store.upsert_source(unit.source_id, strategy=unit.strategy, cadence=unit.cadence,
                                    status="ok", last_success_utc=now_utc(), last_attempt_utc=now_utc())
            store.log_run(unit.source_id, unit.unit_id, res.status, obs=(res.obs or 0),
                          dur_s=round(time.time() - t0, 1), note=res.error)
            results.append((unit.key, res.status))
        except TransientError as e:
            _record(store, unit, "transient_fail", err=str(e)[:300], dur=time.time() - t0)
            results.append((unit.key, "transient_fail"))
        except DefinitiveError as e:
            _record(store, unit, "partial", err=str(e)[:300], dur=time.time() - t0)
            results.append((unit.key, "partial"))
        except Exception as e:  # noqa: BLE001 — unexpected: treat as transient, surface, retry
            _record(store, unit, "transient_fail", err=("UNEXPECTED:" + repr(e))[:300], dur=time.time() - t0)
            results.append((unit.key, "error"))
        finally:
            store.release_lease(unit.key, owner=OWNER)
    if skipped_no_adapter:
        uniq = sorted(set(skipped_no_adapter))
        print(f"[orchestrator] {len(uniq)} source(s) skipped — strategy/adapter not built yet "
              f"(incremental rollout): {', '.join(uniq[:8])}{' ...' if len(uniq) > 8 else ''}", flush=True)
    return results


def _record(store, unit, status, err=None, dur=0.0):
    store.upsert_unit(unit.source_id, unit.unit_id, strategy=unit.strategy,
                      status=status, last_attempt_utc=now_utc(), last_error=err)
    store.log_run(unit.source_id, unit.unit_id, status, dur_s=round(dur, 1), note=err)
