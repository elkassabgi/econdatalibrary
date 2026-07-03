"""The orchestrator: due-check -> change-detect -> dispatch -> record state.

Drives every registry unit through its strategy, honoring leases (no double-runs),
the Transient/Definitive failure contract, and the first-pass protection (never
touch the 3 in-flight backfill jobs or their data). A unit is marked `ok` only
after its strategy reports a successful atomic publish.

Honesty additions per UPDATER_BUILD_PLAN.md §1.3/§5:
- the registry is validated against the MEASURED `config.EXPECTED_SOURCE_COUNT`
  (see updater/REGISTRY_RECONCILIATION.md) — a shrunken/bloated registry refuses
  to run rather than silently managing the wrong universe;
- a source with no runnable adapter gets an explicit PENDING line, and if the
  source is in the live tier (`live: true` in registry.yaml — a data flag, never
  a Python source list) the whole run FAILS: no silent skips inside the rollout
  perimeter (§5.3);
- contract step 5 (§5.7): after a successful merge the changed series' CSVs are
  re-derived in the same run; a CSV failure never crashes or rolls back the data
  publish — the run is demoted to `partial` and the series ids are recorded in
  the `csv_retry_queue` state table.
"""
from __future__ import annotations
import os
import time

from . import config
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


def _is_live(unit) -> bool:
    """Live tier = the rollout perimeter, carried as `live: true` on the registry
    entry (one source of truth in DATA — a hardcoded Python source list would be
    the whack-a-mole pattern reborn, §1.3)."""
    return bool((unit.config or {}).get("live"))


def _resolve_blob():
    """Blob handle for CSV PUTs when the caller didn't inject one — updater.blob
    owns backend selection (AQUEDUCT_BACKEND=local|r2, from_env())."""
    from . import blob as blob_mod  # lazy: only needed when a merge changed series
    return blob_mod.from_env()


def _derive_changed_csvs(unit, res, blob):
    """Contract step 5 — CSV/parquet coherence (§5.7): re-derive the CSV of every
    series whose parquet changed this run. Returns (failed_series_ids, error_note).

    The changed set is exactly `res.series_cursors` — the per-series freshness the
    fetcher measured from rows it actually merged (never inferred from schedules).
    ANY failure here (derive module missing, catalog unreadable, PUT exhausted its
    retries) must never crash or roll back the already-published parquet: the
    caller demotes the run to `partial` and queues the ids in csv_retry_queue."""
    changed = sorted((res.series_cursors or {}).keys())
    if not changed:
        if res.obs:
            # Coherence is UNMET, not merely unlogged: parquet changed but the
            # changed series are unknown, so their CSVs go stale. The caller
            # demotes the run to `partial` (nothing to queue — ids unknown);
            # the un-bumped vintage forces a re-fetch until the fetcher
            # reports cursors. An `ok` here would be a silent §5.7 violation.
            print(f"[orchestrator] WARNING {unit.key}: merged {res.obs} obs but reported no "
                  f"series_cursors — cannot re-derive CSVs for unknown series; the fetcher "
                  f"must report per-series cursors to satisfy CSV/parquet coherence (§5.7)",
                  flush=True)
            return [], (f"csv coherence unmet: fetcher reported no series_cursors for "
                        f"{res.obs} merged obs — CSVs not re-derived (§5.7)")
        return [], None
    try:
        # `changed` holds STORE series_keys; the derive/resolver layer needs
        # CATALOG series_ids. The key→id mapping is source-specific (e.g.
        # frankfurter stores 'EURARS' but catalogs 'frankfurter:EUR:ARS'), so:
        #   1. exact hit: '<source>:<key>' exists in the catalog → derive it;
        #   2. any unmapped keys → for small sources (≤ _DERIVE_ALL_CAP catalog
        #      ids) re-derive ALL of the source's ids — coherence guaranteed at
        #      trivial cost; larger sources demote to partial with the gap named
        #      (never a silent §5.7 violation).
        ids, unmapped = _catalog_ids_for(unit.source_id, changed)
        if not ids:
            return [], (f"csv coherence unmet: {len(unmapped)} changed series_keys "
                        f"have no catalog mapping for {unit.source_id} and the "
                        f"source exceeds the derive-all cap (§5.7)")
        from . import derive  # lazy: lands with the derive work-package; missing => partial
        out = derive.derive_and_put(ids, blob if blob is not None else _resolve_blob()) or {}
        failed = [str(s) for s in (out.get("failed") or [])]
        note = f"csv_derive failed {len(failed)}/{len(ids)} series" if failed else None
        if not note and unmapped:
            note = (f"csv coherence partial: {len(unmapped)} changed keys unmapped "
                    f"for {unit.source_id} (over derive-all cap)")
        return failed, note
    except Exception as e:  # noqa: BLE001 — CSV failure must NEVER sink the data publish
        return changed, (f"csv_derive crashed ({len(changed)} series queued): " + repr(e))[:300]


_DERIVE_ALL_CAP = 5000


def _catalog_ids_for(source_id: str, changed_keys):
    """Map changed store series_keys to catalog series_ids (see hook comment).
    Returns (ids_to_derive, unmapped_keys). Reads the catalog read-only from
    $ECONDL_CATALOG or <root>/data/catalog.db."""
    import sqlite3
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
    try:
        exact, unmapped = [], []
        for k in changed_keys:
            cand = f"{source_id}:{k}"
            row = con.execute("SELECT 1 FROM series WHERE series_id=?", (cand,)).fetchone()
            if row:
                exact.append(cand)
            else:
                unmapped.append(k)
        if not unmapped:
            return exact, []
        n_src = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?",
                            (source_id,)).fetchone()[0]
        if 0 < n_src <= _DERIVE_ALL_CAP:
            all_ids = [r[0] for r in con.execute(
                "SELECT series_id FROM series WHERE source_id=?", (source_id,))]
            return all_ids, []
        return exact, unmapped
    finally:
        con.close()


def run_once(sources=None, strategies=None, cadences=None, force=False, dry=False,
             store=None, blob=None):
    store = store or StateStore()
    # Validate the registry up front — fail loudly rather than silently run a
    # malformed/incomplete registry (the documented coverage gate). The expected
    # count is the MEASURED number from updater/REGISTRY_RECONCILIATION.md (§5.6).
    problems = registry.validate(registry.load(),
                                 expected_count=config.EXPECTED_SOURCE_COUNT)
    if problems:
        raise SystemExit("registry invalid (fix before running):\n  " + "\n  ".join(problems[:20]))
    units = registry.all_units()
    results = []
    pending_live, pending_other = [], []  # no-adapter sources, split by live tier
    for unit in units:
        if sources and unit.source_id not in sources:
            continue
        if strategies and unit.strategy not in strategies:
            continue
        if cadences and unit.cadence not in cadences:
            continue
        if _protected(unit):
            continue  # protected in-flight backfill (by source_id or output dir)
        try:
            runnable = _has_adapter(unit)
        except Exception as e:  # noqa: BLE001 — adapter module EXISTS but is broken at
            # import (e.g. it wraps a since-deleted legacy script). Neither a silent
            # skip nor a crash-the-world: one broken fetcher must not sink the other
            # ~129 sources' updates. Loud per-source line + stale-marked state, and a
            # run failure if the source is inside the live tier (§5.3).
            print(f"[orchestrator] BROKEN {unit.source_id} — adapter import failed: {e!r}",
                  flush=True)
            if not dry:
                _record(store, unit, "transient_fail",
                        err=("adapter import broken: " + repr(e))[:300])
            if _is_live(unit):
                pending_live.append(unit.source_id)  # live + unrunnable => run failure
            results.append((unit.key, "broken_adapter"))
            continue
        if not runnable:
            # never a silent skip: every no-adapter source gets a PENDING line below,
            # and a live-tier one fails the whole run (§5.3)
            (pending_live if _is_live(unit) else pending_other).append(unit.source_id)
            continue

        us = store.get_unit(unit.source_id, unit.unit_id)
        strat = get_strategy(unit.strategy)

        if not force and not strat.is_due(unit, us):
            continue

        try:
            vintage = strat.detect_change(unit, us)
        except TransientError as e:
            if not dry:  # a dry run reports; it never mutates state (T-3)
                _record(store, unit, "transient_fail", err=f"detect:{e}")
            results.append((unit.key, "transient_fail"))
            continue
        if not force and vintage is None:
            # Upstream unchanged. An EARNED no_change — the probe produced a
            # vintage token equal to the stored one (§5.2) — is RECORDED, so
            # checked_at/last_success advance honestly; otherwise a quiet but
            # healthy live source rots into RED-SLA and /v1/last-updates
            # checked_at freezes (T-6). An UNDETERMINABLE probe (e.g.
            # manual_vintage holding an edition, current vintage unknowable)
            # records nothing: we verified nothing, and laundering that into
            # no_change is exactly what §5.2 forbids.
            probe = getattr(strat, "_cur", None)
            stored_vintage = (us or {}).get("upstream_vintage")
            if (not dry and probe is not None and stored_vintage is not None
                    and probe == stored_vintage):
                ts = now_utc()
                store.upsert_unit(unit.source_id, unit.unit_id, strategy=unit.strategy,
                                  status="no_change", last_success_utc=ts,
                                  last_attempt_utc=ts)
                store.upsert_source(unit.source_id, strategy=unit.strategy,
                                    cadence=unit.cadence, status="ok",
                                    last_success_utc=ts, last_attempt_utc=ts)
                store.log_run(unit.source_id, unit.unit_id, "no_change", obs=0,
                              note=f"vintage probe matched stored ({str(probe)[:80]})")
                results.append((unit.key, "no_change"))
            continue

        if dry:
            results.append((unit.key, "due"))
            continue

        if not store.claim_lease(unit.key, owner=OWNER, ttl_s=_ttl(unit)):
            results.append((unit.key, "locked"))
            continue

        t0 = time.time()
        try:
            res = strat.run(unit, since=(us or {}).get("last_obs_date"))
            status = res.status
            ok = status in ("ok", "no_change")
            err_note = res.error
            # Contract step 5 (§5.7): a successful merge re-derives the changed
            # series' CSVs in the same run. A CSV failure demotes the run to
            # `partial` and queues the ids — the parquet publish stands, and the
            # un-bumped vintage below makes the next run re-check + re-derive.
            if status == "ok" and not dry:
                csv_failed, csv_err = _derive_changed_csvs(unit, res, blob)
                if csv_failed or csv_err:
                    # csv_err with no ids = coherence unmet with unknown series
                    # (no cursors reported): still `partial`, nothing queueable.
                    if csv_failed:
                        store.enqueue_csv_retry(unit.source_id, csv_failed, csv_err)
                    status, ok = "partial", False
                    err_note = "; ".join(x for x in (err_note, csv_err) if x)
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
                status=status,
                last_obs_date=new_last,
                obs_count=(res.obs if res.obs else (us or {}).get("obs_count")),
                last_success_utc=(now_utc() if ok else (us or {}).get("last_success_utc")),
                last_attempt_utc=now_utc(), last_error=err_note)
            if res.series_cursors:
                # per-series freshness stands even on a csv-partial: the parquet
                # holding these observations DID publish (§5.1 — freshness only
                # from fetched rows, which these are)
                store.put_series_cursors(unit.source_id, res.series_cursors)
            if ok:
                store.upsert_source(unit.source_id, strategy=unit.strategy, cadence=unit.cadence,
                                    status="ok", last_success_utc=now_utc(), last_attempt_utc=now_utc())
            store.log_run(unit.source_id, unit.unit_id, status, obs=(res.obs or 0),
                          dur_s=round(time.time() - t0, 1), note=err_note)
            results.append((unit.key, status))
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
    # Explicit PENDING line per no-adapter source — never an aggregate shrug (§5.3).
    live_set = sorted(set(pending_live))
    for sid in sorted(set(pending_other) | set(live_set)):
        tag = "LIVE-TIER, run fails" if sid in live_set else "not in live tier yet"
        print(f"[orchestrator] PENDING {sid} — no adapter built ({tag})", flush=True)
    if live_set:
        # A live source may never be silently skipped: fail the run loudly. Print
        # this run's per-unit results first (the CLI's summary won't get the chance),
        # so the log still shows what WAS processed before the failure.
        for k, s in results:
            print(f"  {s:16} {k}", flush=True)
        raise SystemExit(
            f"[orchestrator] RUN FAILURE: {len(live_set)} live-tier source(s) have no "
            f"runnable adapter: {', '.join(live_set)} — build the fetcher or remove "
            f"`live: true` from registry.yaml (no silent skips inside the rollout perimeter, §5.3)")
    return results


def _record(store, unit, status, err=None, dur=0.0):
    store.upsert_unit(unit.source_id, unit.unit_id, strategy=unit.strategy,
                      status=status, last_attempt_utc=now_utc(), last_error=err)
    store.log_run(unit.source_id, unit.unit_id, status, dur_s=round(dur, 1), note=err)
