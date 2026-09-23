"""S3 fetcher — Australian Bureau of Statistics (ABS) SDMX 2.1 REST. Keyless, CC-BY-4.0.

Layout (set by jobs/ingest_abs_full.py): ONE grouped parquet per DATAFLOW under
clean_full/abs/<FLOW>.parquet, schema (series_key, obs_date, value) — many series per
file. series_key is the dotted SDMX data key: every dimension code column strictly
between DATAFLOW (col 0) and TIME_PERIOD in the compact CSV, joined with '.'
(e.g. "4.161.TOT.TOTXE.Q"). The dedup key is therefore (series_key, obs_date).

DATE-TAIL (sdmx_delta). ABS SDMX 2.1 honors ?startPeriod on the data endpoint
(verified live: GET /rest/data/<FLOW>/all?format=csvfile&startPeriod=2024 returns only
periods >= 2024 across annual/quarterly/monthly flows). For each on-disk flow we:
  - read that flow's existing parquet max(obs_date) and request ONLY periods from the
    BOUNDARY YEAR forward (startPeriod=<year(max_obs)>). We re-fetch the boundary year
    (not year+1) so a same-year later period or an in-place REVISION of an already
    stored period is captured; merge dedups the overlap and never shrinks. Year
    granularity is used because ABS flows mix frequencies (A/Q/M/D) and a year
    startPeriod is the one form every frequency accepts.
  - REUSE jobs/ingest_abs_full enumeration + parse verbatim: list_dataflows() for the
    catalog, and collect() (which wraps stream_rows() with the 5x exponential-backoff
    retry on the ABS mid-body connection drops, locates TIME_PERIOD/OBS_VALUE by name,
    treats cols 1..ti-1 as the dotted key, parse_period -> period-END date,
    parse_value -> float). We do NOT re-discover endpoints/keys/parse logic.
  - merge ONLY via merge.merge_and_write(path, tbl, mode="merge",
    dedup_keys=("series_key","obs_date")); never write parquet here.

updatedAfter is NOT used: the ABS data endpoint frequently drops the body mid-stream
(ChunkedEncodingError / IncompleteRead — confirmed live on this run), and the registry
adapter.open_question explicitly leaves whether ABS honors updatedAfter unverified. The
per-flow startPeriod date-tail is the robust, complete delta; collect()'s retry loop
absorbs the flakiness.

HONEST-STATUS CONTRACT (Tally + finalize). Each dataflow is a sub-unit:
  added_unit(n_new) merge netted n_new rows for the flow.
  empty_unit()      200 but no rows newer than the boundary (a legitimately quiet
                    tail) — the normal steady state for most of ~1222 flows.
  transient_unit()  collect() raised (timeout / 5xx / 429 / network / mid-body drop
                    surviving the retry budget) — record and KEEP GOING; the whole run
                    becomes 'partial' so the orchestrator re-runs (no silent no_change).
  structural_unit() a flow that previously had rows now returns a 200 header whose
                    TIME_PERIOD/OBS_VALUE columns are GONE (collect() returns [] from an
                    unexpected shape) — a per-flow schema break.
A large all-empty/transient window (every flow quiet) is gated by finalize()'s
empty_window_floor; we pass <#flows>-1 per the S3 contract so a genuine wholesale
outage raises DefinitiveError while one healthy quiet flow does not falsely trip it.
existing data is always preserved by merge (never shrinks).
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import pyarrow.compute as pc

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (CURSOR_CAP, Deadline, RotationCycle, Tally, finalize, load_rotation,
                      rotate_after, save_rotation)

# Reuse the ingester verbatim: enumeration, streaming+retry, parse helpers.
import jobs.ingest_abs_full as ing

SOURCE = "abs"
DEDUP = ("series_key", "obs_date")

# WHY THIS SOURCE HAS A BUDGET AND A CURSOR CAP (2026-07-30).
#
# abs KILLED THE ENTIRE DAILY UPDATER, TWICE. It sorts first alphabetically, so it runs
# first; run 30523814247 climbed 1,211MB -> 15,700MB monotonically at ~299 MB/min for 48.5
# minutes and the 16 GB runner was destroyed with 288 MB free. The full log carried ONE
# orchestrator banner (abs/_all) and ZERO completions: no other source ever executed, and
# because the state push is skipped on a non-zero exit, every minute of that work was
# discarded. Batch 30312217406 did the identical thing earlier — 49 min, 15,654 MB peak —
# and the response then was to add the ">>>" banner so a future OOM could NAME its culprit
# (see orchestrate.py). That observability is what identified abs today; the memory fix is
# this block.
#
# GitHub renders a destroyed runner as "cancelled", not "failure", so the workflow's
# rc=137/143 OOM branch never fires: bash never lives to report an exit code. Nothing
# downstream can catch this. It has to be bounded HERE.
#
# Three unbounded terms, all now closed:
#   1. `cursors` accumulated one entry per SERIES across all ~1,222 flows with no cap —
#      precisely the risk CURSOR_CAP was introduced for (R175, ilostat's 30.8M series).
#      _common's cap docstring enumerates the sources checked against it; abs was never
#      one of them. abs holds 18 catalog ids, so _catalog_ids_for maps almost nothing and
#      reporting millions of cursors buys nothing while costing millions of state rows.
#   2. Per-flow Arrow buffers and collect()'s three parallel row-lists were dropped but
#      never returned to the OS; over 1,222 iterations the pool only grows.
#   3. No self-limit. AQUEDUCT_RUN_BUDGET_MIN is checked BETWEEN sources, so a source that
#      never RETURNS is unbounded by construction — the run budget cannot see it.
BUDGET_MIN = float(os.environ.get("AQUEDUCT_ABS_BUDGET_MIN", "35"))


def _flow_max_obs(path: str):
    """Max obs_date already on disk for a flow's parquet (a datetime.date) or None."""
    try:
        od = blob.read_table(path, columns=["obs_date"]).column("obs_date")
        if od.length() == 0:
            return None
        m = pc.max(od).as_py()
        if isinstance(m, dt.datetime):
            m = m.date()
        return m
    except Exception:
        return None


def _flow_start_param(max_obs):
    """startPeriod value (a year string) for the date-tail, INCLUSIVE of the boundary year.

    Re-fetching from the boundary year (not year+1) re-requests the latest stored
    period(s) so a same-year later observation OR an in-place revision is seen; merge
    dedups the overlap. None -> no max on disk (defensive; fetch full history)."""
    if max_obs is None:
        return None
    return str(max_obs.year)


def _build_table(keys, dates, vals):
    return pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })


# _series_maxes() was REMOVED here on 2026-07-30. It built a per-flow
# {series_key: max obs_date} dict in full before returning it, which on this source means
# millions of entries for a single census cross-tab — allocated before any cap could
# apply. The fold in update() now streams the same computation into the capped run-global
# `cursors`. Sibling fetchers keep their own copies; theirs are small sources.


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)

    # On-disk flows = the authoritative sub-unit set (one parquet per dataflow). We
    # date-tail every existing flow. (Brand-new dataflows added to the ABS catalog are
    # not back-discovered here — that's a re-ingest concern; this fetcher keeps the
    # existing ~1222 flows fresh, which is the S3 contract.) Enumeration goes through
    # blob so the flow set is visible under AQUEDUCT_BACKEND=r2 (the local store dir
    # is absent on a CI runner).
    pfiles = blob.list_parquets(out_dir)
    if not pfiles:
        raise DefinitiveError(f"no abs parquet files under {out_dir}")

    sess = ing.session()
    tally = Tally()
    total = 0
    last_obs = None
    cursors: dict[str, str] = {}   # series_key -> max obs_date written this run
    # '<FLOW>:<series_key>' -> changed date (merge-measured); None once the pass passed the cap
    changed: dict[str, str | None] | None = {}
    cursors_capped = False
    dl = Deadline(minutes=BUDGET_MIN)
    deferred = 0

    # ROTATE THE STARTING POINT, OR THE BUDGET STARVES THE TAIL FOREVER.
    # blob.list_parquets returns SORTED names, so a fixed budget over a fixed order works
    # the same prefix every run and flows past the cut-off are never reached — a quieter
    # outage than the OOM, and one that would sit behind a log line claiming they "drain
    # next tick". Resume after the last flow attempted, wrapping around.
    resume = load_rotation(out_dir)
    if resume:
        pfiles = rotate_after(pfiles, resume)
        print(f"[abs] resuming after {resume} ({len(pfiles)} flows, rotated)", flush=True)
    last_attempted = None
    # `ok` = EVERY flow attempted since the last ok (RotationCycle, 2026-09-23). A pass reaches
    # ~118 of 1,222 flows (daily run 35783253243: "118 sub-unit(s) attempted, none failed; 1104
    # deferred"), so every pass booked the rest deferred and abs read `partial` on every run - in
    # the daily gate's failure list with nothing failing. Now only flows not yet attempted THIS
    # CYCLE are booked, and the pass that completes the cycle reads ok.
    cycle = RotationCycle(out_dir, pfiles)
    owed_this_cycle = set(cycle.unvisited())

    for fn in pfiles:
        path = os.path.join(out_dir, fn)
        flow = fn[:-len(".parquet")]
        before = blob.row_count(path)
        if cycle.done(fn):
            # Visited this cycle: no work owed (review R1105 P1). Its rows still count: `obs` is
            # the store total and is served as obs_count (review AR-123 P5).
            total += before
            continue

        # Stop STARTING new flows once the budget is spent. Deferred flows go through
        # tally.deferred_unit(), NOT transient_unit(): nothing failed and nothing was even
        # attempted, so they must not inflate the failure count (R303 — abs once reported
        # "805/1222 sub-unit(s) transient-failed" when every named unit read "deferred
        # (budget 35 min)"). The unit vintage is still NOT advanced and the remainder drains
        # on the next tick, so nothing is silently skipped. Without this a single source can
        # consume the whole 300-minute job and every runner byte.
        if dl.spent():
            if fn in owed_this_cycle:          # attempted earlier this cycle: not owed
                deferred += 1
                tally.deferred_unit(f"{flow} deferred (budget {BUDGET_MIN:.0f} min)")
            total += before
            continue
        last_attempted = fn
        # Saved per flow, not only after the loop (R273): abs's fetch ran 43.9 min on 09-16 against
        # the 45-minute unit cap, and an end-of-loop save is what the kill destroys (R1105).
        save_rotation(out_dir, fn)
        cycle.begin(fn)                     # a raise or kill inside it counts (AR-127 P5)
        fails_before = cycle.failures(tally)

        max_obs = _flow_max_obs(path)
        start = _flow_start_param(max_obs)
        params = {"startPeriod": start} if start else None

        # --- fetch ONLY the date-tail (one sub-unit per flow), reusing collect() ---
        try:
            keys, dates, vals = ing.collect(sess, flow, "all", params=params)
        except Exception as exc:  # noqa: BLE001
            # collect() exhausts its 5x retry budget on the ABS mid-body drops / 5xx /
            # timeouts and re-raises (requests.* or urllib3 ProtocolError). Leave this
            # flow's existing data untouched, record transient, keep going. -> 'partial'.
            # NAMED: `exc` was bound and discarded, and abs has 1,222 sub-units — a bare count
            # at that scale cannot be acted on.
            tally.transient_unit(f"{flow}: {type(exc).__name__} — {str(exc)[:160]}")
            total += before
            mx = max_obs.isoformat() if max_obs else None
            # seed this flow's frontier so a transient flow can't hide behind the max:
            # carry forward each on-disk series cursor is too costly per-flow; record the
            # flow-level frontier under a flow sentinel key for visibility.
            if mx:
                cursors.setdefault(f"__flow__{flow}", mx)
            cycle.visit(fn, failed=True)     # stays owed (R1105 P2)
            continue

        if not keys:
            # collect() returns [] for (a) a 404 / genuinely empty flow, (b) a quiet
            # date-tail (no period >= boundary year — normal for most flows), OR (c) a
            # 200 header missing TIME_PERIOD/OBS_VALUE (unexpected shape). We can only
            # call (c) a structural break when the flow PREVIOUSLY had rows AND the
            # request was an unfiltered/boundary fetch — but an incremental quiet tail is
            # indistinguishable from (a)/(b), and treating "nothing newer" as structural
            # would false-positive on every quiet flow. So a date-tail empty is recorded
            # as a legitimate empty; true wholesale breaks surface via the all-empty
            # floor in finalize(). Existing data is preserved (no write).
            tally.empty_unit()
            total += before
            mx = max_obs.isoformat() if max_obs else None
            if mx and (last_obs is None or mx > last_obs):
                last_obs = mx
            cycle.visit(fn)                  # a quiet flow was looked at and is current: done
            continue

        tbl = _build_table(keys, dates, vals)

        # --- publish (atomic, dedup, never-shrink) ---
        try:
            # MERGE-MEASURED changed keys (like ssb #68). abs stores bare keys per flow file
            # ('1.10001.10.50.Q' in CPI.parquet) while the catalogue ids carry the flow
            # ('abs:CPI:1.10001.10.50.Q'), so the bare cursor keys could NEVER map: every run read
            # "csv coherence unmet: 50000 changed series_keys have no catalog mapping". Reported
            # as '<FLOW>:<key>' - the catalogue's form - and COMPLETE (the cap is this flow's own
            # fetched table, so it never refuses), which lets the declared `catalog_scope: subset`
            # exception judge a changed set that is not truncated (R497).
            n, md, ch = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP,
                                              report_changed_keys=True,
                                              changed_keys_cap=max(tbl.num_rows, 1))
        except DefinitiveError as e:
            # never-shrink / dropped-column / 0-row guard refused this flow's write.
            # Keep the old file, surface as transient so the run is 'partial' and retries
            # rather than silently dropping a flow's delta. The exception says WHICH guard
            # and with what numbers; it used to be discarded.
            tally.transient_unit(f"{flow}: write refused — {str(e)[:160]}")
            total += before
            cycle.visit(fn, failed=True)     # stays owed (R1105 P3)
            continue

        total += n
        # one merge per flow per pass and the key carries the flow, so each key is reported once.
        # BOUNDED run-wide (review R1115): census flows re-fetch whole cross-tabs (C21_G09_SAL:
        # 24,409,680 rows per visit, 148 flows >= 1M), so a pass where one changes wholesale must
        # not grow an unbounded dict on a 16 GB runner. Over the cap the pass reports None - the
        # cursor path - and says so, as statcan does: honesty about completeness is binary.
        if changed is not None:
            if len(changed) + len(ch) > merge.CHANGED_KEYS_CAP:
                print(f"[abs] {flow}: the changed-keys report would pass {merge.CHANGED_KEYS_CAP:,} "
                      f"keys ({len(changed):,} + {len(ch):,}) - this pass falls back to the cursor "
                      f"path (changed_keys=None)", flush=True)
                changed = None
            else:
                changed.update((f"{flow}:{k}", str(d) if d is not None else None)
                               for k, d in ch.items())
        del ch
        # NET-DELTA -> ADDED: a boundary-year re-fetch that RETURNED real rows but
        # nets 0 new after dedup is still a data-bearing (successful) sub-unit, not
        # empty. Count len(keys) (rows that actually flowed for the flow), mirroring
        # the SDMX/bcb siblings. Counting net delta would record a healthy quiet
        # steady state as empty and falsely trip the all-empty structural floor.
        # The true net-new delta remains reflected in total/obs and last_obs.
        tally.added_unit(len(keys))
        if md and (last_obs is None or md > last_obs):
            last_obs = md
        # per-series cursors for the rows we just fetched (the moved series), BOUNDED.
        # Once the cap is reached we stop ADDING new keys but keep advancing ones already
        # held, so the reported set stays coherent rather than truncating mid-flow. The
        # cap being hit is printed below — a silent bound is the defect, not the bound.
        # Folded straight from the row stream rather than via a per-flow
        # {series_key: max} dict. ABS holds 376,332,763 distinct series (measured
        # 2026-07-30 over the 976,632,535-row store), and the census cross-tabs are
        # ~1 observation per series, so an intermediate dict for one big flow is itself
        # millions of entries — allocated in full BEFORE any cap could apply.
        for k, d in zip(keys, dates):
            if d is None:
                continue
            v = d.isoformat()
            prev = cursors.get(k)
            if prev is None:
                if len(cursors) >= CURSOR_CAP:
                    cursors_capped = True
                    continue
                cursors[k] = v
            elif v > prev:
                cursors[k] = v

        # VISITED only once the flow's fetch finished without a failure (stat_latvia review R1103,
        # P2): a flow whose fetch failed stays owed, so the cycle cannot close without it.
        cycle.visit(fn, failed=cycle.failures(tally) > fails_before)
        # Return this flow's buffers to the OS before opening the next one. Dropping the
        # references is not enough: Arrow keeps freed blocks in its pool, so across 1,222
        # flows RSS only ever climbs.
        del tbl, keys, dates, vals
        pa.default_memory_pool().release_unused()

    # Both bounds are DISCLOSED. A cap that trims the reported changed-set, or a budget
    # that leaves flows unattempted, must say so in the log — otherwise the next reader
    # sees a clean run and assumes full coverage.
    # Record where to resume. Saved even on a COMPLETE pass: the last flow then sits at
    # the end of the sorted order, so the next run wraps to the top — the same code path
    # either way, and no branch that could silently stop rotating.
    if last_attempted:
        save_rotation(out_dir, last_attempted)
    cycle.close_if_complete(tally)

    if deferred:
        where = (f"the next run RESUMES AFTER {last_attempted} so they actually drain"
                 if last_attempted else
                 "NO flow was attempted at all — the bookmark is unchanged, so the next "
                 "run retries this same point (check the budget, not the rotation)")
        print(f"[abs] BUDGET {BUDGET_MIN:.0f} min spent after {dl.elapsed_min():.1f} min — "
              f"{deferred}/{len(pfiles)} flow(s) still owed this cycle; {where} "
              f"(run reports partial, vintage not advanced)", flush=True)
    if cursors_capped:
        print(f"[abs] cursor set hit the {CURSOR_CAP:,} cap — further changed series are "
              f"not individually reported", flush=True)

    # empty_window_floor = (#subunits) - 1 per the S3 contract: a genuine wholesale
    # outage (every one of ~1222 flows empty) raises DefinitiveError, while a single
    # healthy quiet flow among many that moved does not.
    res = finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors,
                   empty_window_floor=max(len(pfiles) - 1, 1))
    # complete: every merge this pass reported ({} = nothing changed); None = over the cap (cursors)
    res.changed_keys = changed
    return res
