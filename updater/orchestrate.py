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
from . import merge
from . import registry
from .state import StateStore, now_utc
from .strategies import get as get_strategy
from .strategies.fetchers import implemented as fetcher_implemented
from .strategies.fetchers._common import sane_since
from .errors import TransientError, DefinitiveError

# In-flight first-pass backfills — NEVER touched by updates. These are the
# clean_full/<dir> names the running jobs write to (cbs_nl, gus_dbw, and the
# DBnomics-ISTAT re-pull which writes clean_full/dbnomics/). We match on BOTH
# source_id and the unit's output directory so a registry source that maps onto
# one of these dirs can never slip through (the source_id alone was brittle).
# wid was pinned here on 2026-07-29 while its 2.47M-CSV derive ran — a CI re-fetch
# would have republished wid.parquet underneath it and left every remaining CSV derived
# from a superseded copy (the fao_oa failure: files present, dates identical, values
# stale). REMOVED the same day, once the derive completed and was verified
# (catalog 2,465,197 == R2 CSVs 2,465,197, missing 0).
#
# Leaving it pinned would have been its own bug, and a quiet one: wid is now the
# library's largest SERVED source, and a protected source is never attempted, so it
# would sit live and frozen forever while the health gate reported RED-UNRUN and
# nothing looked broken. A protection that outlives the operation it protects becomes
# the outage.
FIRSTPASS_DIRS = {"cbs_nl", "gus_dbw", "dbnomics"}

# A unit whose recent runs all finish inside this is "cheap" and rides the fast lane.
FAST_LANE_SECONDS = 120.0


def order_units(units, costs, staleness_key, fast_lane_seconds=FAST_LANE_SECONDS):
    """Run order: CHEAP BAND FIRST, staleness within the band.

    Module-level and pure so the scheduling rule can be tested directly. A test that
    re-implements the ordering proves only that the test agrees with itself — the property
    being asserted is about what the RUN does, so the run and the test must call the same
    function (R249: match the tool to the claim).

    `costs` maps source_id -> estimated seconds; a source ABSENT from it has never run and
    sorts into the cheap band, so it keeps absolute priority for its first turn.
    """
    def band(unit):
        est = costs.get(unit.source_id)
        return 0 if est is None or est < fast_lane_seconds else 1
    return sorted(units, key=lambda u: (band(u), staleness_key(u)))


def _protected(unit) -> bool:
    """Protected in-flight backfill. Announced, never silent — see below."""
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


def _clip_err(msg, limit: int = 1400) -> str:
    """Bound an error string WITHOUT silently defeating the offender list inside it.

    Errors were stored as `str(e)[:300]`. That cap is right in spirit — an unbounded error
    goes into state.db, which is pulled and pushed on EVERY run — but 300 chars is below the
    length of the messages the fetchers deliberately build. finalize() names the sub-units
    that failed precisely so a finding is actionable without a bisect (see Tally.structural_ids),
    and the cap was cutting that list off mid-token:

        hagstofa: 7/1096 sub-unit(s) returned 200 but parsed 0 rows ... [kosningar/.../KOS03190.px,
        kosningar/.../KOS03190a.px, manntal/2011/1manntalfjolsk/CEN01560.px, manntal/2011/1manntalf

    Four of seven offenders, the fourth unusable. The prefix alone is ~135 characters, so any
    source whose unit ids are paths loses most of its list. The naming fix and the truncation
    were each reasonable alone and cancelled each other out.

    Two changes: a limit that fits a real offender list, and truncation that ANNOUNCES itself
    and lands on a separator, so a reader can tell "these are the 4 that failed" from "these
    are 4 of 7 and the rest were cut". A silent clip reads as completeness.
    """
    s = str(msg)
    if len(s) <= limit:
        return s
    cut = s[:limit]
    i = cut.rfind(", ")
    if i > limit * 0.5:          # end on a whole element when one is close enough
        cut = cut[:i]
    return f"{cut} …[truncated, {len(s) - len(cut)} more chars]"


def _ttl(unit) -> int:
    return _TTL_BY_COST.get((unit.config or {}).get("refresh_cost"), 7200)


# HARD PER-SOURCE WALL CLOCK. AQUEDUCT_UNIT_TIMEOUT_MIN, 0 disables.
#
# The whole-run budget is checked BETWEEN units, so it cannot bound a unit that never returns.
# Measured 2026-07-31: ssb ran 2h31m inside one update() and took GitHub's 300-minute ceiling
# with it - the run was killed, "RUN BUDGET" never printed, and the graceful stop (finish what
# we started, push state, name what we skipped) never happened. Per-source Deadlines fix the
# fetchers that cooperate; only 18 of 107 live cloud sources have one, and a fetcher making a
# single blocking call (imf _direct's one ing.pull) has nothing to check a Deadline between.
#
# SIGALRM, not a thread. Injecting an exception across threads needs ctypes and cannot
# interrupt a blocking C call; SIGALRM is the standard tool and lands in the main thread as a
# normal Python exception, so the existing `except Exception` records it transient and the run
# CONTINUES to the next source. Interrupting is safe for data because merge_and_write publishes
# through write_table_atomic - a half-written store is not reachable.
#
# POSIX only. signal.setitimer does not exist on Windows, where this is a no-op by design: the
# ceiling being defended is GitHub's, and the workstation job runs with a 2,880-minute budget
# and no hard kill. Never silently: _unit_timeout says which platform it is on the first call.
_TIMEOUT_WARNED = False


class UnitTimeout(Exception):
    """One source exceeded its hard wall clock. Deliberately an Exception, not BaseException:
    it must be caught by the unit handler and demote THAT source, never abort the run."""


def _unit_timeout_min() -> float:
    try:
        return float(os.environ.get("AQUEDUCT_UNIT_TIMEOUT_MIN", "45"))
    except ValueError:
        return 45.0


class _unit_deadline:
    """Context manager arming SIGALRM for one unit; a no-op where unavailable."""

    def __init__(self, key: str, minutes: float):
        self.key, self.minutes = key, minutes
        self.armed = False

    def __enter__(self):
        global _TIMEOUT_WARNED
        if self.minutes <= 0:
            return self
        try:
            import signal
            if not hasattr(signal, "setitimer"):
                raise AttributeError("setitimer")

            def _fire(signum, frame):
                raise UnitTimeout(
                    f"{self.key} exceeded its {self.minutes:.0f}-minute hard limit and was "
                    f"interrupted; existing data untouched, re-queued for the next tick")

            self._prev = signal.signal(signal.SIGALRM, _fire)
            signal.setitimer(signal.ITIMER_REAL, self.minutes * 60.0)
            self.armed = True
            if not _TIMEOUT_WARNED:
                # Once per run, so the log PROVES the guard is active rather than asserting
                # it. This path cannot be exercised on the Windows workstation (no setitimer),
                # so the first CI run is its first real test and must say so out loud.
                print(f"[orchestrator] per-unit hard timeout ARMED at "
                      f"{self.minutes:.0f} min (SIGALRM)", flush=True)
                _TIMEOUT_WARNED = True
        except (ImportError, AttributeError, ValueError):
            if not _TIMEOUT_WARNED:
                print("[orchestrator] per-unit hard timeout UNAVAILABLE on this platform "
                      "(no signal.setitimer); relying on per-source Deadlines only", flush=True)
                _TIMEOUT_WARNED = True
        return self

    def __exit__(self, *exc):
        if self.armed:
            try:
                import signal
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, self._prev)
            except Exception:                                # noqa: BLE001
                pass
        return False


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


def _here() -> str:
    """Where this process is running: "cloud" on a GitHub runner, else "local".

    Overridable with AQUEDUCT_RUN_LOCATION for a workstation run that should behave as if it
    were CI (or the reverse) without editing the registry.
    """
    override = os.environ.get("AQUEDUCT_RUN_LOCATION", "").strip().lower()
    if override in ("cloud", "local"):
        return override
    return "cloud" if os.environ.get("GITHUB_ACTIONS", "").strip() else "local"


def _wrong_location(unit) -> bool:
    """True when this source is not permitted to execute HERE.

    Sources whose merge peak exceeds a 16 GB runner carry `run_location: local`. Until this
    check existed the label was inert: nothing in the updater or the workflow read it. Of the
    13 that carry it, 12 were kept out of CI only incidentally, by `live: false`; the one that
    was live went straight through. That is what killed the 2026-07-31 08:30 UTC run - `ons_uk`
    climbed from 2.3 GB to 15.8 GB with 151 MB left and the runner was destroyed at 104
    minutes, taking with it the state, freshness and D1 syncs of every source that had already
    succeeded. `always()` cannot rescue those steps: there is no machine left to run them on.

    "any" (the default) runs anywhere. A source is skipped only where it is known not to fit.
    """
    want = (unit.config or {}).get("run_location") or "any"
    return want != "any" and want != _here()


def _resolve_blob():
    """Blob handle for CSV PUTs when the caller didn't inject one — updater.blob
    owns backend selection (AQUEDUCT_BACKEND=local|r2, from_env())."""
    from . import blob as blob_mod  # lazy: only needed when a merge changed series
    return blob_mod.from_env()


def _record_for_catalog_sync(ids) -> None:
    """Append derived series ids for the post-run D1 catalog sync.

    Append-only and best-effort: this is a discoverability improvement, and it must
    never be able to sink a run that already published data correctly (§5 — a CSV
    problem never undoes a good parquet publish). The consumer
    (core/sync_catalog_d1.py) dedupes and truncates the file.
    """
    if not ids:
        return
    try:
        from . import config
        os.makedirs(config.STATE_DIR, exist_ok=True)
        with open(os.path.join(config.STATE_DIR, "pending_catalog_sync.txt"),
                  "a", encoding="utf-8") as fh:
            fh.write("".join(f"{s}\n" for s in ids))
    except Exception as e:  # noqa: BLE001 — never sink a good publish over this
        print(f"[orchestrator] WARNING: could not record {len(ids)} id(s) for the "
              f"D1 catalog sync ({e!r}); they stay hosted but undiscoverable until "
              f"the next `sync_catalog_d1.py --source <id>` reconcile", flush=True)


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
        if res.obs and _catalog_series_count(unit.source_id) == 0:
            # VACUOUS COHERENCE. A source with ZERO catalogued series has no per-series
            # CSVs, so there is nothing that can go stale and §5.7 is satisfied rather
            # than violated. gleif is the case: a REFERENCE TABLE (LEI golden copy) with
            # no series_key/obs_date at all, whose module docstring says plainly that
            # cursors "would be meaningless, not missing" and asks the sweep not to
            # "fix" it. Without this it merged 3,391,691 obs and demoted to `partial`
            # EVERY run — and because partial never sets last_success_utc, it could
            # never report success and sat permanently in the health gate's attention
            # list, crowding out real failures.
            #
            # This is deliberately a MEASURED fact, not a fetcher-declared opt-out: a
            # source cannot exempt itself by asserting anything, it only passes when the
            # catalogue provably holds nothing to re-derive. Any source with catalogued
            # series still gets the full check below.
            return [], None
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
            # NAME THE CAUSE YOU MEASURED, NOT THE ONE THAT SOUNDS RIGHT. This note used to
            # end "and the source exceeds the derive-all cap" unconditionally — the same
            # hardcoded-cause defect already fixed in the sibling branch below (R152), but
            # here it was worse than unverified: under the r2 backend `_catalog_ids_for`
            # returns before the cap is ever consulted, so the note named a condition the
            # code CANNOT have evaluated. On 2026-08-02 it was the note on 28 of 54 partial
            # sources, and it sent every reader — the digest email and me — after a cap that
            # was irrelevant. The real cause was that R2's coherence catalog held 4,605,291
            # of 10,853,209 series: noaa had 10 rows there against 3,135,873 locally, so of
            # course nothing mapped.
            #
            # Zero mapped ids has exactly two causes and they need opposite fixes, so the
            # note distinguishes them by MEASURING the catalogue rather than guessing:
            # no rows at all (not catalogued / purged / the reference is stale) versus rows
            # present but none matched (a grain or key-form mismatch).
            n_ids = None
            try:
                import sqlite3 as _sq
                _cat = (os.environ.get("ECONDL_CATALOG")
                        or os.path.join(config.ROOT, "data", "catalog.db"))
                with _sq.connect(f"file:{_cat}?mode=ro", uri=True) as _c:
                    n_ids = _c.execute(
                        "SELECT COUNT(*) FROM series WHERE source_id=?",
                        (unit.source_id,)).fetchone()[0]
            except Exception:                       # noqa: BLE001 — a note must never raise
                n_ids = None
            if n_ids is None:
                why = "catalog id count unavailable"
            elif n_ids == 0:
                why = ("the catalog this run read has NO rows for it — not catalogued, "
                       "purged, or the coherence catalog is stale")
            else:
                why = (f"the catalog this run read has {n_ids:,} rows for it but none "
                       f"matched — grain/key-form mismatch")
            return [], (f"csv coherence unmet: {len(unmapped)} changed series_keys "
                        f"have no catalog mapping for {unit.source_id}: {why} (§5.7)")
        from . import derive  # lazy: lands with the derive work-package; missing => partial
        out = derive.derive_and_put(ids, blob if blob is not None else _resolve_blob()) or {}
        failed = [str(s) for s in (out.get("failed") or [])]
        # A derived CSV is HOSTED but not yet DISCOVERABLE: nothing in the daily
        # pipeline pushed catalog rows to D1 (sync_state_d1 syncs freshness only,
        # by design), so a new series reached R2 and never appeared in /v1/catalog.
        # That silently stranded 31,259 series -- boe alone showed 21 of 30,674 in
        # the serving catalog while its fetcher had been live for weeks. Record what
        # we derived; the post-run catalog sync step upserts exactly these rows.
        _record_for_catalog_sync([s for s in ids if s not in set(failed)])
        # Name the failures, bounded. "failed 7/24" alone costs a bisect to act on,
        # which is why such notes sit unfixed for weeks (same reason Tally now carries
        # structural_ids). The count stays authoritative; the elision is explicit.
        note = None
        if failed:
            shown = ", ".join(failed[:5])
            more = f", +{len(failed) - 5} more" if len(failed) > 5 else ""
            note = f"csv_derive failed {len(failed)}/{len(ids)} series [{shown}{more}]"
        if not note and unmapped:
            # STATE ONLY WHAT WAS CHECKED. This used to append "(over derive-all cap)"
            # unconditionally — a hardcoded cause, never tested. riksbank emitted
            # "28 changed keys unmapped for riksbank (over derive-all cap)" while holding
            # 117 catalogue rows against a 5,000 cap, so the reason was impossible; the
            # note sent every reader (and the digest email) after the wrong explanation,
            # me included. A diagnostic that names its own cause without verifying it is
            # a plausible lie with a long half-life (ledger R152).
            n_ids = None
            try:
                import sqlite3 as _sq
                _cat = (os.environ.get("ECONDL_CATALOG")
                        or os.path.join(config.ROOT, "data", "catalog.db"))
                with _sq.connect(f"file:{_cat}?mode=ro", uri=True) as _c:
                    n_ids = _c.execute(
                        "SELECT COUNT(*) FROM series WHERE source_id=?",
                        (unit.source_id,)).fetchone()[0]
            except Exception:                       # noqa: BLE001 — a note must never raise
                n_ids = None
            if n_ids is None:
                why = "catalog id count unavailable"
            elif n_ids > _DERIVE_ALL_CAP:
                why = (f"source has {n_ids:,} catalog ids, over the "
                       f"{_DERIVE_ALL_CAP:,} derive-all cap")
            else:
                why = (f"source has {n_ids:,} catalog ids, UNDER the "
                       f"{_DERIVE_ALL_CAP:,} cap — cause is NOT the cap")
            # COVERAGE, NOT COHERENCE (2026-08-05). §5.7's claim is that SERVED CSVs
            # track the store. Reaching here means every changed key that HAS a catalog
            # row was mapped and derived without failure; the residue provably has no
            # catalog row to go stale (measured above, all three mapping rules tried).
            # The old "coherence partial" demotion punished partial catalogue coverage
            # HARDER than zero coverage (a source with 0 catalogued series passes
            # trivially at line ~300) and kept statfin/snb/unesco_*/who_sdg permanently
            # partial — never green, never vintage-bumped, gate red every day, which is
            # how gates stop being read (R244). Zero-mapped-with-rows (the key-form
            # mismatch class, defillama pre-fix) still demotes above; derive failures
            # and missing cursors still demote. The tail stays visible: this note is
            # persisted on the unit and printed here.
            note = (f"csv coverage note: {len(unmapped)} changed keys have no catalog "
                    f"row for {unit.source_id} ({why}) — served ids coherent")
            print(f"[orchestrator] {unit.source_id}: {note}", flush=True)
        return failed, note
    except Exception as e:  # noqa: BLE001 — CSV failure must NEVER sink the data publish
        return changed, (f"csv_derive crashed ({len(changed)} series queued): " + repr(e))[:300]


_DERIVE_ALL_CAP = 5000

# Max queued csv-retry ids attempted per source per run (the drain at the csv step).
# Bounded so a large parked backlog (insee_bdm: 43,354) cannot monopolise the derive
# budget that fresh changes need; the rest stays queued for later runs.
_CSV_RETRY_CAP = 20_000


def _flow_of(key: str) -> str:
    """FLOW-grain id for a series-grain PxWeb store key.

    The store key is `<flow>:<dim>=<value>:<dim>=<value>…`, so the obvious rule is "drop the
    `=`-bearing segments". That rule is WRONG whenever a dimension VALUE contains a colon:
    hagstofa stores NACE codes like `Atvinnugrein=K: 65`, which splits into `Atvinnugrein=K`
    (dropped, has `=`) and ` 65` (KEPT, has none), yielding the corrupt flow
    `…THJ11002.px: 65`. That silently left 658 hagstofa keys unmapped, and an unmapped key
    trips the _DERIVE_ALL_CAP fallback into re-deriving the source's entire catalog.

    Truncating at the table-id segment instead is immune to colons in values: measured on
    every one of those 658 keys, it maps 658/658. Sources whose table ids are not `*.px`
    (e.g. ssb's `SSB:A1Skog`) keep the `=` rule, verified unchanged.
    """
    parts = key.split(":")
    for i, p in enumerate(parts):
        if p.endswith(".px"):
            return ":".join(parts[:i + 1])
    return ":".join(p for p in parts if "=" not in p)


def _norm_id(s: str) -> str:
    """Identity of a series id ignoring punctuation and case.

    `frankfurter:EUR:USD` and `frankfurter:EURUSD` are the same series written two
    ways; comparing only the alphanumerics makes them equal without teaching this
    module anything about FX pairs.
    """
    return "".join(ch for ch in s if ch.isalnum()).lower()


def _catalog_series_count(source_id: str) -> int:
    """Does the CATALOGUE hold ANY series for this source? 1 = yes, 0 = none, -1 = unreadable.

    Used only to recognise vacuous CSV coherence: zero catalogued series means no per-series
    CSVs exist, so a cursorless publish cannot make anything stale. The one caller tests
    `== 0` and nothing else, so an exact count was never needed — and computing one was
    ruinously expensive.

    Returns -1 when the catalogue cannot be read. That is deliberately NOT zero: an unreadable
    catalogue must not be mistaken for "nothing to derive" and hand out a free pass to every
    source. -1 fails the `== 0` test, so an unreadable catalogue leaves the strict §5.7 path
    exactly as it was.

    WHY THE QUERY LOOKS LIKE THIS. It was:

        SELECT count(*) FROM series WHERE series_id LIKE 'src:%'

    and sqlite plans that as `SCAN series USING COVERING INDEX` — a FULL SCAN of the catalogue.
    Prefix-LIKE is only index-optimisable when case_sensitive_like is ON, and it is off by
    default, so the pattern cannot use the primary key. At 8.5 GB and 10.8M rows that is a full
    scan PER SOURCE, on a run that touches ~120 of them.

    Measured on the live catalogue 2026-08-03:
        LIKE 'noaa:%'  count   -> SCAN,   116.35 s
        >= 'noaa:' AND < 'noaa;' EXISTS -> SEARCH, 0.0001 s
    A range on the primary key is exact for a prefix (':' + 1 == ';'), uses the index, and
    LIMIT 1 stops at the first hit instead of walking 3.1M entries to count them.

    The name is kept so the caller and its comments still read correctly; the docstring above
    is now the contract.
    """
    import sqlite3
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    try:
        con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT 1 FROM series WHERE series_id >= ? AND series_id < ? LIMIT 1",
                (f"{source_id}:", f"{source_id};"),
            ).fetchone()
            return 1 if row else 0
        finally:
            con.close()
    except Exception:                                        # noqa: BLE001
        return -1


def _catalog_ids_for(source_id: str, changed_keys):
    """Map changed store series_keys to catalog series_ids (see hook comment).
    Returns (ids_to_derive, unmapped_keys). Reads the catalog read-only from
    $ECONDL_CATALOG or <root>/data/catalog.db."""
    import sqlite3
    cat = os.environ.get("ECONDL_CATALOG") or os.path.join(config.ROOT, "data", "catalog.db")
    con = sqlite3.connect(f"file:{cat}?mode=ro", uri=True)
    try:
        exact, unmapped = [], []
        seen = set()
        for k in changed_keys:
            cand = f"{source_id}:{k}"
            row = con.execute("SELECT 1 FROM series WHERE series_id=?", (cand,)).fetchone()
            if row:
                if cand not in seen:
                    seen.add(cand)
                    exact.append(cand)
                continue
            # FLOW-GRAIN fallback. The PxWeb family is catalogued at TABLE (flow) grain
            # while the store is at SERIES grain: the store key appends one `dim=value`
            # segment per dimension, so `<source>:<key>` can never equal the catalog id.
            #   store   LV:OSP_OD:...:ARA30.px:ContentsCode=...:Apmācības formas=0
            #   catalog stat_latvia:LV:OSP_OD:...:ARA30.px
            # Stripping the `=`-bearing segments recovers the flow id. Measured across all
            # nine PxWeb sources (stat_latvia, stat_estonia, ssb, bfs, dst, statfin,
            # hagstofa, stat_slovenia, scb): exact-match 0%, flow-match 100%. Without this
            # every one of them merged its rows and then demoted to `partial` with
            # "N changed series_keys have no catalog mapping" — stat_latvia's unmapped
            # count (1,952) equalled its catalog row count exactly, which is the tell that
            # the catalog was complete and only the GRAIN differed.
            flow = _flow_of(k)
            if flow != k:
                fcand = f"{source_id}:{flow}"
                if fcand in seen:
                    continue          # many series collapse onto one flow — derive it once
                if con.execute("SELECT 1 FROM series WHERE series_id=?",
                               (fcand,)).fetchone():
                    seen.add(fcand)
                    exact.append(fcand)
                    continue
            # SPLIT-PART expansion (2026-08-05, the census cycle). A table too large for
            # one CSV is catalogued as `<source>:<table>#<part>` rows with NO base id
            # (census: eits__m3#no/#yes, idb__1year#AD..., six composite trade splits) —
            # so a table-grain cursor exact-misses while every part's CSV genuinely goes
            # stale on change. A changed table conservatively re-derives ALL its parts.
            # `#` never appears in a non-split id's tail, and LIKE-wildcards in the key
            # are escaped so a key containing % or _ cannot over-match.
            esc = cand.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            parts = [r[0] for r in con.execute(
                r"SELECT series_id FROM series WHERE series_id LIKE ? ESCAPE '\'",
                (esc + "#%",))]
            if parts:
                for p in parts:
                    if p not in seen:
                        seen.add(p)
                        exact.append(p)
                continue
            unmapped.append(k)

        # PUNCTUATION-GRAIN fallback. frankfurter stores the key `EURUSD` while its
        # catalog id is `frankfurter:EUR:USD` — the same identity, differently
        # punctuated, so neither the exact nor the flow rule can bridge it. The cost
        # was not an error: all 46 CSVs existed in R2 and simply stopped being
        # REGENERATED, so `frankfurter:EUR:USD` served data to 2026-07-24 while the
        # store held 2026-07-27, drifting further every day while the source reported
        # only a vague `partial`.
        #
        # Matching on the alphanumerics alone bridges it without special-casing any
        # source. A collision would be far worse than a miss — it would rewrite one
        # series' CSV with another's data — so the index is built once per source and
        # a normalised form claimed by more than one catalog id is DISCARDED rather
        # than guessed.
        if unmapped:
            norm = {}
            for (cid,) in con.execute(
                    "SELECT series_id FROM series WHERE source_id=?", (source_id,)):
                n = _norm_id(cid)
                norm[n] = None if n in norm else cid      # None marks an ambiguity
            still = []
            for k in unmapped:
                hit = norm.get(_norm_id(f"{source_id}:{k}"))
                if hit and hit not in seen:
                    seen.add(hit)
                    exact.append(hit)
                elif not hit:
                    still.append(k)
            if len(still) != len(unmapped):
                print(f"[orchestrator] {source_id}: mapped "
                      f"{len(unmapped) - len(still)} key(s) to catalog ids by "
                      f"punctuation-insensitive match", flush=True)
            unmapped = still

        if not unmapped:
            return exact, []
        # DERIVE-ALL is only meaningful when the whole store is readable. Under the r2
        # backend it is not: blob.write_table_atomic keeps a local copy purely as a
        # scratch mirror for the same-run derive (blob.py), so $ECONDL_DATA on a runner
        # holds ONLY the files this run wrote. Asking for every id of the source then
        # fails for every flow whose file was not touched — measured on stat_estonia,
        # "csv_derive failed 949/3437", and on dst "1923/1963", each failure reading
        # "zero rows matched in N files". Those are not coverage gaps; they are requests
        # for data that was never on the machine.
        #
        # So under r2 we derive exactly the ids we could MAP (their files are, by
        # construction, the ones this run wrote) and surface the rest as an honest
        # unmapped list. Locally, where the full store is present, derive-all still runs
        # and still guarantees coherence for small sources.
        if config.BACKEND == "r2":
            return exact, unmapped
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

    # RECONCILE WHAT WAS ASKED FOR AGAINST WHAT EXISTS, before running anything. `--source`
    # is a promise the caller makes to a human ("running updater for 14 source(s)") and the
    # loop below silently drops any name that matches no unit — a typo, a renamed source, or
    # a registry entry that produces no units all look identical to a clean run that simply
    # had less to do. Fail fast and name them: the caller can fix a typo in seconds, whereas
    # a missing source is only noticed weeks later as unexplained staleness.
    if sources:
        have = {u.source_id for u in units}
        unknown = sorted(set(sources) - have)
        if unknown:
            raise SystemExit(
                f"[orchestrator] {len(unknown)} requested source(s) have no unit in the "
                f"registry: {', '.join(unknown)} — check the spelling, or the source's "
                f"registry entry. Refusing to run a partial set silently.")

    # STALEST FIRST, so a slow head of the alphabet cannot starve the tail forever.
    #
    # The loop used to walk `units` in registry order, which is effectively alphabetical, and
    # the whole-run budget then cut the SAME tail off every night. Measured on CI run
    # 30690884454 (2026-08-01): seven sources consumed 3.7 of the 4-hour budget — stat_slovenia
    # 2,700s, unesco_natmon 2,999s, unesco_sdg 2,912s, ssb 2,401s, ecb 2,108s, wikidata 770s,
    # comtrade 408s — and 29 sources were NOT ATTEMPTED. That is not a one-night miss: three of
    # them (imf_fsibsis_direct, imf_fsic_direct, imf_fsicdm_direct) have NO STATE AT ALL, live
    # and due and never once attempted by any run. Their fetchers work — all three return real
    # vintage tokens when called by hand — they simply never got a turn.
    #
    # Ordering by staleness makes starvation self-correcting: a source skipped tonight is the
    # stalest tomorrow, so it goes first. Never-run units sort ahead of everything, which is the
    # only ordering under which a brand-new source is guaranteed a first run.
    #
    # It does NOT reorder anything else — cadence, protection, location and budget checks all
    # still apply per unit, and an explicit --source list is unaffected because it is a filter,
    # not an order.
    # ...BUT STALENESS ALONE STILL STARVES THE CHEAP SOURCES, because it is blind to COST.
    #
    # Measured on the 2026-08-02 06:00 run (CI 30738981790) against the 106 live cloud sources:
    #
    #     68 sources cost < 2 min each   —    24.5 min for ALL of them together
    #     11 sources cost 2-10 min       —    40.7 min
    #     27 sources cost >= 10 min      — 1,031.4 min   (4.3x the whole 240-min budget)
    #
    # Under one staleness order the 27 expensive ones interleave with the rest, so the budget
    # is gone after 20 sources and 76 are NOT ATTEMPTED. Among the skipped: cnb, whose run
    # takes 4.9 SECONDS, and frankfurter at 5.6s. Both are daily FX feeds with a 2-day SLA,
    # both were last refreshed 2026-07-31, and both were RED-SLA in the gate — not because
    # anything about them is broken, but because a 5-second job kept queueing behind a
    # 400-minute one. A source cannot meet a 2-day SLA if its turn comes round every 5 days.
    #
    # So order by COST BAND first, staleness within the band. The cheap band drains in ~25 min
    # (10% of the budget) and refreshes two thirds of the fleet every single night; the
    # expensive band then gets ~215 of the 240 min instead of 240.
    #
    # THE COST TO THE EXPENSIVE SOURCES IS REAL AND SMALL: at ~38 min each they fit ~6 per run
    # today and ~5.6 after this change — they were never all running anyway, and they rotate by
    # staleness exactly as before. What changes is that their rotation no longer runs THROUGH
    # the cheap sources.
    #
    # Never-run units keep absolute priority: unknown cost sorts into the cheap band and ""
    # sorts first within it, so a brand-new source is still guaranteed a first run. That is
    # deliberate — a never-run source has no cost on record precisely because it has never had
    # a turn, and putting it last would be the starvation this whole ordering exists to undo.
    def _staleness(unit):
        st = store.get_unit(unit.source_id, unit.unit_id) or {}
        last = st.get("last_success_utc") or st.get("last_attempt_utc")
        return (last or "", unit.key)          # "" (never run) sorts first

    units = order_units(units, store.run_cost_estimate(), _staleness)

    results = []
    pending_live, pending_other = [], []  # no-adapter sources, split by live tier
    # Rollout perimeter enforced at EXECUTION (learned from CI run 28682266857,
    # SIGTERM'd at 1h47m): `live: true` alone only gated the no-adapter failure
    # mode, so a CI run with no --source filter executed EVERY due source with
    # an adapter — ~70 fetchers + a 6,500-CSV derive marathon on one 14 GB
    # runner. With AQUEDUCT_LIVE_ONLY=1 (set in updater-daily.yml), only
    # live-tier sources EXECUTE; the rest are counted and reported, never run.
    # An explicit --source request still runs a non-live source (manual
    # dispatch is how a source earns its delta proof before joining the tier).
    live_only = os.environ.get("AQUEDUCT_LIVE_ONLY", "").strip() in ("1", "true", "yes")
    not_in_rollout = []

    # Whole-RUN wall-clock budget. Sources run strictly serially, so a few slow
    # upstreams push the job into GitHub's hard 300-minute ceiling — and being killed
    # there is not merely "some sources missed": the run dies BEFORE push-state,
    # before the D1 syncs and before the digest, so every source that DID succeed
    # loses its state write too. One slow source costs the whole night.
    #
    # Stopping early is strictly better than being killed: we finish the units we
    # started, push state, and name what we skipped. Skipped units are untouched —
    # not due-marked, not vintage-advanced — so the next tick picks them up first.
    # Default 240 min leaves ~60 min of headroom under the 300-minute ceiling for
    # the post-run steps.
    try:
        run_budget_min = float(os.environ.get("AQUEDUCT_RUN_BUDGET_MIN", "240"))
    except ValueError:
        run_budget_min = 240.0
    run_deadline = time.time() + run_budget_min * 60.0 if run_budget_min > 0 else None
    budget_skipped = []
    protected_skipped = []
    wrong_location = []
    not_due = []

    for unit in units:
        if sources and unit.source_id not in sources:
            continue
        if strategies and unit.strategy not in strategies:
            continue
        if cadences and unit.cadence not in cadences:
            continue
        if live_only and not _is_live(unit) and not sources:
            not_in_rollout.append(unit.source_id)
            continue
        if _wrong_location(unit) and sources:
            # The explicit --source override is location-BLIND by design (the workstation
            # job depends on it) — but a manual cloud dispatch of a run_location: local
            # source runs KEYLESS there and writes false structural verdicts (census,
            # 2026-08-06: 45/45 'Missing Key' pages recorded as schema breaks, R362).
            # The override stands; it just cannot be silent any more.
            print(f"[orchestrator] WARNING {unit.key}: explicit --source OVERRIDES "
                  f"run_location={(unit.config or {}).get('run_location')} on "
                  f"{_here()} — a local-routed source's credentials are typically "
                  f"absent here and its verdicts belong to the other lineage", flush=True)
        if _wrong_location(unit) and not sources:
            # Announced and recorded, never silent (R101). An explicit --source still runs it,
            # so the workstation job and a manual proof are both unaffected.
            wrong_location.append(unit.source_id)
            print(f"[orchestrator] WRONG LOCATION {unit.key} — needs "
                  f"run_location={(unit.config or {}).get('run_location')}, running on "
                  f"{_here()}; not attempted this run", flush=True)
            continue
        if _protected(unit):
            # ANNOUNCED, not silent. This was the only `continue` in this loop that
            # printed nothing and recorded nothing, so a protected source produced no
            # state and surfaced downstream as RED-UNRUN ("built but never ran") with
            # nothing anywhere in the log to say why. Chasing exactly that on `wid`
            # cost an hour of auditing leases, adapter checks and due-checks before
            # the real cause turned up somewhere else entirely (R101). A deliberate
            # skip that leaves no trace is indistinguishable from a bug.
            protected_skipped.append(unit.source_id)
            print(f"[orchestrator] PROTECTED {unit.key} — in-flight backfill, not "
                  f"attempted this run (FIRSTPASS_DIRS)", flush=True)
            continue
        # AFTER the filters, deliberately: checked first it would count units that were
        # never in scope — a two-source dispatch reported "100 source(s) NOT ATTEMPTED".
        # A skip count is only meaningful over units that would otherwise have RUN.
        if (run_deadline is not None and time.time() > run_deadline
                and not (sources and len(sources) == 1)):
            # Single-source dispatches are deliberate manual proofs — never cap those,
            # or a proof run would report success having skipped the source under test.
            budget_skipped.append(unit.source_id)
            continue
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
                        err=_clip_err("adapter import broken: " + repr(e)))
            if _is_live(unit):
                pending_live.append(unit.source_id)  # live + unrunnable => run failure
            results.append((unit.key, "broken_adapter"))
            continue
        if not runnable:
            # ANNOUNCED INLINE, not only in the end-of-run summary. This branch claimed to
            # be "never a silent skip" on the strength of a PENDING line printed AFTER the
            # loop — which is no use at all while the run is still going, and no use ever
            # if the run is killed. noaa proved it: the 2026-08-01 workstation pass listed
            # 14 sources, jumped from `NOT DUE istat` straight to `>>> oecd`, and emitted
            # not one character about noaa in either stdout or stderr. I read that silence
            # as "the pass is working on noaa's files" and parked its re-key as blocked for
            # a source the run had never touched (R211).
            #
            # Every other `continue` in this loop prints where the reader is looking. This
            # one now does too; the summary line below still restates it.
            (pending_live if _is_live(unit) else pending_other).append(unit.source_id)
            print(f"[orchestrator] PENDING {unit.key} — no adapter built for "
                  f"strategy={unit.strategy}; not attempted", flush=True)
            continue

        us = store.get_unit(unit.source_id, unit.unit_id)
        strat = get_strategy(unit.strategy)

        if not force and not strat.is_due(unit, us):
            # ANNOUNCED, like every other deliberate skip in this loop. This was the last
            # silent `continue` here, and the codebase's own rule (R101) is that a skip
            # leaving no trace is indistinguishable from a bug. It also became load-bearing
            # the moment updater-heavy started failing a job that processed zero units: a
            # not-due source printed NOTHING, so a correct, cadence-respecting skip was
            # indistinguishable in the log from "this source has no fetcher at all", and the
            # guard would have failed the job for doing exactly the right thing. The heavy
            # cron does not pass --force, so that would have fired on the next quiet night.
            not_due.append(unit.source_id)
            print(f"[orchestrator] NOT DUE {unit.key} — cadence={unit.cadence}, "
                  f"last_success={(us or {}).get('last_success_utc') or 'never'}; "
                  f"skipped (use --force to override)", flush=True)
            continue

        # Announce the unit BEFORE any work. A run that is KILLED (OOM -> SIGKILL,
        # which GitHub renders as "cancelled") prints nothing afterwards, so without
        # this line the log cannot say WHICH source died. Batch 30312217406 burned
        # 49 minutes, peaked at 15,654 MB of a 16 GB runner, and named no culprit:
        # every source in the dispatch appeared exactly once, on the input line.
        # The orchestrator only ever printed on skips and at the end, so a long
        # single-source run was indistinguishable from a hung one.
        print(f"[orchestrator] >>> {unit.key} (strategy={unit.strategy}, "
              f"cadence={unit.cadence})", flush=True)
        t_unit = time.time()

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

        merge.impossible_reset()   # per-unit, so the count below is THIS source's
        t0 = time.time()
        try:
            # sane_since GUARDS THE CURSOR AT THE HAND-OFF, not in each fetcher. The stored
            # frontier is the furthest period the store holds, and for a source carrying
            # forecasts or projections that is decades ahead: abs 2046, un_wpp 2101,
            # bfs 2150. Handing that to a delta fetcher as "only fetch periods after this"
            # selects nothing, for ever, silently - and it is wrong whether the date is a
            # legitimate projection horizon or a corrupt sentinel, because new OBSERVED data
            # lands before it either way.
            #
            # sane_since already existed for exactly this, but only as something each fetcher
            # could remember to call. Measured 2026-07-31: 28 of 93 units carried a future
            # frontier, 12 of their fetchers took `since`, and only 4 guarded it. None is
            # actively frozen today - the other 8 happen not to filter on it - but that is
            # luck, and the next fetcher to start honouring `since` inherits the trap
            # invisibly. Guarding here makes it structurally impossible instead.
            #
            # None means "no usable lower bound", which every fetcher already handles: it is
            # what a first run passes.
            with _unit_deadline(unit.key, _unit_timeout_min()):
                res = strat.run(unit, since=sane_since((us or {}).get("last_obs_date")))
            status = res.status
            ok = status in ("ok", "no_change")
            err_note = res.error
            # Contract step 5 (§5.7): a successful merge re-derives the changed
            # series' CSVs in the same run. A CSV failure demotes the run to
            # `partial` and queues the ids — the parquet publish stands, and the
            # un-bumped vintage below makes the next run re-check + re-derive.
            if status == "ok" and not dry:
                csv_failed, csv_err = _derive_changed_csvs(unit, res, blob)
                # DRAIN THE RETRY QUEUE (2026-08-06). derive.py has promised since it
                # gained a wall-clock budget that ids not reached "come back in `failed`
                # ... so they are retried next run instead of lost" — but the queue was
                # WRITE-ONLY: csv_retries()/clear_csv_retries() had zero callers, so
                # every queued id was lost after all (insee_bdm alone parked 43,354 in
                # one outage-recovery run). Retries run AFTER the fresh changes (fresh
                # first — they are the run's purpose), bounded per run; successes are
                # cleared, refailures simply STAY QUEUED — they are deliberately NOT
                # merged into csv_failed, because demoting a run over OLD residue would
                # re-create the permanently-partial disease (R359) through this path.
                # Under the r2 backend a retried id derives only when its file is on
                # this runner (written by this run) — others fast-fail on the local
                # miss and wait for the run that next rewrites their file.
                _retry_rows = store.csv_retries(unit.source_id)
                if _retry_rows and not csv_err:
                    _retry_ids = [r["series_id"] for r in _retry_rows][:_CSV_RETRY_CAP]
                    from . import derive as _derive_mod
                    _out = _derive_mod.derive_and_put(
                        _retry_ids, blob if blob is not None else _resolve_blob()) or {}
                    _refailed = set(str(s) for s in (_out.get("failed") or []))
                    _cleared = [s for s in _retry_ids if s not in _refailed]
                    if _cleared:
                        store.clear_csv_retries(_cleared)
                        _record_for_catalog_sync(_cleared)
                    print(f"[orchestrator] {unit.source_id}: csv retry queue "
                          f"{len(_retry_rows):,} -> attempted {len(_retry_ids):,}, "
                          f"cleared {len(_cleared):,}, still queued {len(_refailed):,}",
                          flush=True)
                if csv_err and csv_err.startswith("csv coverage note:") and not csv_failed:
                    # Coverage, not coherence: every mapped (= served) changed id was
                    # re-derived; the residual keys have no catalog row to go stale.
                    # Keep the note visible on the unit, keep the run GREEN — a
                    # permanently-partial source is how gates stop being read (R244).
                    err_note = "; ".join(x for x in (err_note, csv_err) if x)
                    csv_err = None
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
        except UnitTimeout as e:
            # Its own branch so the log names the cap rather than reporting a generic
            # UNEXPECTED. Transient by design: nothing was corrupted, the source simply did
            # not finish, and it is re-queued.
            print(f"[orchestrator] TIMEOUT {unit.key} — {e}", flush=True)
            _record(store, unit, "transient_fail", err=_clip_err(e), dur=time.time() - t0)
            results.append((unit.key, "timeout"))
        except TransientError as e:
            _record(store, unit, "transient_fail", err=_clip_err(e), dur=time.time() - t0)
            results.append((unit.key, "transient_fail"))
        except DefinitiveError as e:
            _record(store, unit, "partial", err=_clip_err(e), dur=time.time() - t0)
            results.append((unit.key, "partial"))
        except Exception as e:  # noqa: BLE001 — unexpected: treat as transient, surface, retry
            _record(store, unit, "transient_fail", err=_clip_err("UNEXPECTED:" + repr(e)), dur=time.time() - t0)
            results.append((unit.key, "error"))
        finally:
            store.release_lease(unit.key, owner=OWNER)
            # Per-unit cost, printed even on failure. Peak RSS is what turns "the
            # runner OOMed" into "THIS source needs 15 GB" — the fact that decides
            # whether a source belongs in the shared nightly job or on its own
            # runner (updater-heavy.yml). Without it, every OOM investigation
            # restarts from zero.
            try:
                import resource                              # POSIX (CI runners)
                peak_mb = resource.getrusage(
                    resource.RUSAGE_SELF).ru_maxrss / 1024.0
                mem = f", peak_rss={peak_mb:,.0f}MB"
            except Exception:                                # noqa: BLE001
                mem = ""                                     # Windows: not available
            # IMPOSSIBLE DATES, aggregated per source. merge's guard has always printed one
            # line per affected FILE and returned a count nobody kept, so the evidence existed
            # only as scattered lines in a log nobody diffed — 273,980 rows across six sources
            # sat published at 2999-12-31..9999-12-31 while it printed on every run, and it
            # took a standalone audit to find them (R320). A per-source total sits next to the
            # cost line that IS read, and a number that grows is noticeable in a way a repeated
            # warning is not. Still does not block the publish; that judgement is unchanged.
            imp = merge.impossible_report()
            bad = ""
            if imp.get("rows"):
                worst = imp.get("worst") or ("?", "?")
                bad = (f", IMPOSSIBLE_DATES={imp['rows']:,} row(s) in {imp['files']} file(s)"
                       f" (worst {worst[1]} e.g. {str(worst[0])[:60]})")
            print(f"[orchestrator] <<< {unit.key} took "
                  f"{time.time() - t_unit:,.0f}s{mem}{bad}", flush=True)
    if not_due:
        # Restated in the summary so a reader who sees "0 unit(s) processed" can tell a
        # healthy cadence skip from a source that cannot run at all.
        print(f"[orchestrator] NOT DUE this tick ({len(not_due)}): "
              f"{', '.join(sorted(set(not_due)))} — their cadence has not elapsed. This is "
              f"normal; --force overrides it.", flush=True)
    if wrong_location:
        # Same reasoning as PROTECTED below: these WILL show as RED-UNRUN in the health gate,
        # and the reader needs the reason where they read the outcome. Deliberate routing, not
        # a failure - but not invisible either, or "we run everything nightly" quietly stops
        # being true for 13 sources.
        print(f"[orchestrator] WRONG LOCATION, not attempted on {_here()}: "
              f"{', '.join(sorted(set(wrong_location)))} — these carry run_location: local "
              f"because their merge peak exceeds a 16 GB runner. They update from the "
              f"workstation job; expect RED-UNRUN here until that lands.", flush=True)
    if protected_skipped:
        # Restated in the summary as well as inline, because the inline line scrolls
        # past in a long run and the health gate WILL show these as RED-UNRUN. A
        # reader looking at that red needs the reason in the same place they look for
        # the run's outcome, not buried thousands of lines up.
        print(f"[orchestrator] PROTECTED, not attempted this run: "
              f"{', '.join(sorted(set(protected_skipped)))} — in-flight backfill "
              f"(FIRSTPASS_DIRS). Expect RED-UNRUN/stale for these until it clears.",
              flush=True)
    if budget_skipped:
        # LOUD, and never mistakable for a clean run: a capped run that reported only
        # its successes would read as "everything current" while sources silently
        # aged. These were not attempted at all, so their state is untouched and the
        # next tick takes them first.
        print(f"[orchestrator] RUN BUDGET {run_budget_min:.0f} min SPENT — "
              f"{len(set(budget_skipped))} source(s) NOT ATTEMPTED this run: "
              f"{', '.join(sorted(set(budget_skipped))[:15])}"
              f"{' …' if len(set(budget_skipped)) > 15 else ''}", flush=True)
        print("[orchestrator] this run is INCOMPLETE by design — stopping early beats "
              "being killed at the 300-minute ceiling, which would also lose the state "
              "push, the D1 syncs and the digest for the sources that DID succeed.",
              flush=True)
    if not_in_rollout:
        # Honest disclosure, not a shrug: these sources are DUE-ELIGIBLE but the
        # rollout perimeter (AQUEDUCT_LIVE_ONLY) excludes them until their tier
        # ships with per-source delta proofs (plan §3). Count + names, one line.
        print(f"[orchestrator] rollout perimeter: {len(set(not_in_rollout))} non-live "
              f"source(s) not executed (AQUEDUCT_LIVE_ONLY=1): "
              f"{', '.join(sorted(set(not_in_rollout))[:12])}"
              f"{' …' if len(set(not_in_rollout)) > 12 else ''}", flush=True)
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
