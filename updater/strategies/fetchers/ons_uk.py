"""S5 bulk fetcher — ONS (UK Office for National Statistics) beta API. OGL-UK-3.0, no key.

One parquet per ONS dataset under clean_full/ons_uk/{dataset_id}.parquet, schema
(series_key, obs_date, value); series_key = colon-joined `code=value` pairs over the V4
DIMENSION columns only, TIME EXCLUDED (built by jobs.ingest_ons_uk.parse_dataset_csv_v4, which
this fetcher IMPORTS so the keys are byte-identical to disk — the duplication invariant).

The /v1/datasets catalog walk IS the machine manifest: each catalog item embeds
links.latest_version.id and last_updated, so ONE paginated walk both (a) gives every dataset's
vintage without a per-dataset GET and (b) surfaces brand-new datasets. Vintage token =
"{latest_version_id}|{last_updated}"; unchanged datasets are skipped entirely, changed and NEW
ones are downloaded, parsed and merged (dedup + never-shrink). The store currently holds only 42
of the catalog's ~337 datasets, so early runs will also BACKFILL the missing ones.

Store I/O via blob (R36); the vintage sidecar lives on the store, not the runner. Downloads run
across a small thread pool (R40) since a first run touches hundreds of datasets.

HISTORY — the three defects that kept this source from EVER reporting a success, and what
closed each. Kept in full because two of them were invisible in the logs by construction.

  1. THE KEY GRAIN WAS WRONG (closed 2026-08-03). parse_dataset_csv treated every column that
     was not the time or value column as a dimension, which swept in the observation-metadata
     columns (`CV`, `Data marking`) AND the time CODE column, so a key read
     `CV=14.0:calendar-years=2018:administrative-geography=...` — a quality statistic and the
     observation period baked into the series identity. Every row became its own one-point
     "series". MEASURED on live data: ageing-population-projections is 225,368 rows and
     225,368 DISTINCT keys under that parser, versus 8,668 under parse_dataset_csv_v4.
     Fixed by switching to the V4 parser, which reads the header's self-describing
     `v4_N , <N metadata cols> , <code,label> ...` grammar. Because that CHANGES the on-disk
     keys, writes are mode="replace" (R22) — a merge would leave both grains in the store.

     Do NOT instead collapse keys to a flow id the way the PxWeb sources do — every ons_uk
     segment is a `dim=value` pair, so stripping them yields a fragment of a label rather
     than a flow (measured: `' Manufacture of Wearing Apparel'`).

  2. THE TIME CODES WERE UNPARSEABLE (closed 2026-08-03). parse_ons_period knew '2022',
     '2022 Q1' and ISO dates, and none of `mmm-yy`, `yyyy-yy`, `two-year-intervals` or
     `yyyy-to-yyyy-yy` — the formats ONS actually ships. So the flagship datasets returned
     ZERO rows from multi-megabyte bodies: cpih01 0 of 4,000 sampled rows, likewise
     retail-sales-index, gdp-to-four-decimal-places, index-private-housing-rental-prices,
     wellbeing-local-authority and life-expectancy-by-local-authority. Ten of twelve datasets
     in one batch. Fixed by parse_ons_time_code(), which selects the grammar from the CODE
     COLUMN'S NAME because the values themselves collide ('2011-12' is a financial year, a
     two-year interval, or ISO December 2011, depending only on its column).

  3. THE BATCH WAS A TRUNCATION, NOT A BUDGET (closed 2026-08-03, R190). `todo[:MAX_PER_RUN]`
     took a fixed prefix of a stable catalog order. That self-drains only while datasets
     succeed — a dataset that cannot publish never advances its vintage, so it never leaves
     `todo` and holds its slot forever. With defect 2 live, 10 of the 12 slots were held by
     permanent non-publishers and the 297-dataset backlog drained at ~2 per run. Now rotated
     by catalog position, with the cursor persisted EVERY WAVE (R273 — the orchestrator's
     per-source cap kills the source, so end-of-function state is written only on runs that
     happen to finish early).

Defect 2 also explains the old note that "ons_uk HAS NO CATALOG ROWS AT ALL": a source that
parses zero rows from most of its datasets has little to catalogue. Cursors are emitted at
DATASET grain (see the comment at the merge site), not per series.

HONEST-STATUS: catalog walk failure -> TransientError (partial, retried, data kept). A
per-dataset download/parse failure -> transient_unit for that dataset only, and the reason is
now PRINTED — it used to be swallowed, which is why "1/12 sub-unit(s) transient-failed" was
undiagnosable. A changed dataset that parses to ZERO rows -> empty_unit and its vintage is NOT
advanced, so it retries; rotation stops that retry from starving the rest.
"""
from __future__ import annotations
import gc
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from ._common import cancellable_pool
from jobs import ingest_ons_uk as ig   # reuse catalog walk + THE key builder

SOURCE = "ons_uk"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"     # {dataset_id: "versionid|last_updated"}
ROTATION = "_rotation.json"         # {"after": "<last dataset id this run reached>"} — R190
# ONS's beta API rate-limits HARD: a first attempt at 5 workers x 60 datasets drew 41 HTTP 429s
# in 4 minutes (run 30133384687). R40 says parallelize many-request fetchers, but never past what
# the server tolerates — 2 workers plus a per-request pace keeps us under the limit.
# MEASURED: even 2 workers @1s still draws sustained HTTP 429 (local run 2026-07-25, 6+ min and
# still throttled). Each 429 costs up to 50s of backoff (5+10+15+20), so throttling — not
# parsing — is the entire cost. ONS tolerates roughly one request per ~1.5s, so go SERIAL and
# pace it; a small per-run batch keeps each tick short and drains the ~295-dataset backfill over
# consecutive days rather than wedging one run. (R40b: the server's tolerance is the ceiling.)
MAX_WORKERS = 1
# MEASURED 2026-07-25 against download.ons.gov.uk (Cloudflare-fronted): 429 arrives on the
# ~6th rapid request carrying `Retry-After: 10`, i.e. a sustainable rate near 0.5 req/s.
# 1.5s (0.67 req/s) was still slightly over the line, so pace at 2.0s and let get_csv_bytes
# honour Retry-After when we do get throttled.
REQUEST_PAUSE = 2.0
MAX_PER_RUN = 12
# Datasets held in memory at once. The batch is processed in waves of this size, with the
# executor, its futures and Arrow's pool all released between waves, so peak RSS tracks one
# wave rather than the whole batch. 3 keeps the worst case (ashe-table-5 at 122 MB
# compressed, which explodes in Arrow because every row repeats a 200+ char series_key)
# comfortably inside the 16 GB runner.
WAVE_SIZE = 3


def _vintage(item) -> str:
    links = item.get("links") or {}
    ver = (links.get("latest_version") or {}).get("id", "")
    return f"{ver}|{item.get('last_updated', '')}"


def _catalog(raise_transient: bool):
    try:
        items = ig.get_all_datasets()
    except Exception as e:
        if raise_transient:
            raise TransientError(f"ons_uk: catalog walk failed: {e}")
        return None
    if not items:
        if raise_transient:
            raise TransientError("ons_uk: catalog walk returned no datasets")
        return None
    return items


def current_vintage(unit) -> str | None:
    """Cheap probe: hash over every catalog dataset's (id, version|last_updated)."""
    items = _catalog(raise_transient=False)
    if not items:
        return None
    h = hashlib.sha256()
    for it in sorted(items, key=lambda x: str(x.get("id", ""))):
        h.update(f"{it.get('id','')}={_vintage(it)};".encode())
    return f"ons_uk:{h.hexdigest()[:16]}"


def _load_sidecar(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _load_rotation(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, ROTATION))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_rotation(out_dir, after) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, ROTATION),
                            json.dumps({"after": after}).encode("utf-8"))


def _fetch_one(ds_id, version_href=None):
    """Thread task: download + parse one dataset -> (ds_id, keys, dates, vals, err).

    `err` is None on success and a SHORT REASON on failure. It used to be absent: every
    exception, every unresolvable URL and every empty body collapsed to the same
    `(None, None, None)`, so the run could only ever say "1/12 sub-unit(s) transient-failed"
    with nothing naming which one or why. That is what made this source undiagnosable from
    its own logs for weeks — the failure erased its own cause.

    `version_href` comes straight from the catalog item we already walked
    (links.latest_version.href). Using it skips resolve_csv_url()'s 1-3 EXTRA sequential
    API round-trips per dataset — that redundancy, not the parsing, is what made this
    fetcher take many minutes for only 25 datasets. We fall back to resolving only when
    the catalog didn't carry the href.
    """
    try:
        url = None
        no_distribution = False
        if version_href:
            meta = ig.get_json(version_href)
            if meta:
                downloads = meta.get("downloads") or {}
                for dl in downloads.values():
                    href = dl.get("href", "")
                    if href.endswith(".csv") or "csv" in href.lower():
                        url = href
                        break
                if not url:
                    # The publisher can list a version as `state: published` and still offer
                    # NO distribution for it: `downloads` is {} and the /csv route 404s.
                    # MEASURED over all 337 catalog datasets 2026-08-03 — exactly 2 are like
                    # this (trade, TS058); 335 offer a download. Remembering it here lets the
                    # 404 below be reported as "nothing to fetch" instead of a failure.
                    no_distribution = not downloads
                    url = version_href.rstrip("/") + "/csv"
        if not url:
            url = ig.resolve_csv_url(ds_id)
        if not url:
            return ds_id, None, None, None, "no CSV url resolvable"
        content = ig.get_csv_bytes(url)
        if not content:
            if no_distribution:
                # NOT transient. Retrying cannot conjure a distribution the publisher does not
                # publish, and calling it a failure would hold the whole source at `partial`
                # forever (R231: partial never sets last_success_utc), permanently flagging
                # "investigate" for a condition with nothing to investigate. Same reasoning as
                # R303, which split budget deferrals out of `transient` for exactly this
                # reason. Empty list (not None) so update() routes it to empty_unit, and the
                # reason is printed either way.
                return ds_id, [], [], [], "publisher offers no distribution (downloads:{}, /csv 404)"
            return ds_id, None, None, None, f"empty body from {url[-60:]}"
        # V4 parser: TIME-FREE, code-based keys. See the module docstring — the old
        # parse_dataset_csv folded the time axis and the observation-metadata columns into
        # series_key, so every row became its own one-point "series".
        k, d, v = ig.parse_dataset_csv_v4(ds_id, content)
        time.sleep(REQUEST_PAUSE)
        return ds_id, k, d, v, None
    except Exception as e:
        return ds_id, None, None, None, f"{type(e).__name__}: {e}"


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    items = _catalog(raise_transient=True)
    sidecar = _load_sidecar(out_dir)

    # Which datasets actually need work: vintage moved, or we don't hold them yet.
    #
    # FIRST, drop what is not this product at all. ONS's catalog mixes TWO things under
    # /v1/datasets, and it says which is which in each item's `type`:
    #
    #     cantabular_flexible_table       161
    #     cantabular_multivariate_table   126     <- Census 2021 cross-tabulations
    #     (absent)                         39     <- the v4 time-series product
    #     filterable                       11     <- also v4
    #
    # The cantabular ones are Census cross-tabulations: `ltla` x `has_ever_worked` and the
    # like, with NO time dimension anywhere. They cannot become a time series, the v4 parser
    # correctly declines them, and the store has never held one.
    #
    # Before this filter they were still WORK: 287 of the 337 catalog entries were queued,
    # downloaded, parsed to zero rows and discarded — every run, forever, because a zero-row
    # dataset deliberately does not advance its vintage. That is 287 pointless downloads per
    # full rotation from a publisher that rate-limits hard, and — since `empty` sub-units hold
    # a source at `partial` — it also meant ons_uk could NEVER report success no matter how
    # well the other 50 did. Filtering on the publisher's own declaration fixes both.
    #
    # Deliberately keyed on the type ONS publishes, not on the `TS`/`RM` id prefixes, so a new
    # naming scheme cannot quietly re-admit them. Anything mislabelled still meets the v4
    # parser and lands in empty_unit, as before — this narrows the queue, it does not widen
    # what we accept.
    CANTABULAR = {"cantabular_flexible_table", "cantabular_multivariate_table"}
    n_before = len(items)
    items = [it for it in items if (it.get("type") or "") not in CANTABULAR]
    if n_before != len(items):
        print(f"[ons_uk] catalog {n_before} -> {len(items)} datasets "
              f"({n_before - len(items)} Cantabular census tabulations excluded: no time "
              f"dimension, not a time series)", flush=True)

    todo = []
    for it in items:
        ds_id = it.get("id")
        if not ds_id:
            continue
        path = os.path.join(out_dir, f"{ds_id}.parquet")
        cur_v = _vintage(it)
        if sidecar.get(ds_id) == cur_v and blob.exists(path):
            continue
        href = ((it.get('links') or {}).get('latest_version') or {}).get('href')
        todo.append((ds_id, cur_v, href))

    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    capped = len(todo) > MAX_PER_RUN

    # ROTATE the window (R190). `todo[:MAX_PER_RUN]` is a fixed prefix of a stable catalog
    # order, which is a TRUNCATION dressed as a budget. It self-drains only while every
    # dataset succeeds, because success advances the vintage and drops the dataset out of
    # `todo` — but a dataset that cannot publish never leaves, so it sits at the head
    # consuming a slot forever. MEASURED 2026-08-03: 10 of the 12 slots were held by
    # permanent non-publishers (7 unparseable time codes, 2 Census tables with no time axis
    # at all, 1 dead download), so 297 pending datasets drained at ~2 per run — roughly 143
    # runs, with those 10 re-downloading themselves every single day in the meantime.
    #
    # Rotating by CATALOG POSITION rather than by index into `todo`: `todo` shrinks as
    # datasets succeed, so a saved integer offset would point somewhere different next run.
    cat_pos = {it.get("id"): i for i, it in enumerate(items) if it.get("id")}
    rotation = _load_rotation(out_dir)
    after = rotation.get("after")
    if after in cat_pos and todo:
        k = cat_pos[after]
        n = len(cat_pos)
        todo = sorted(todo, key=lambda t: (cat_pos.get(t[0], 0) - k - 1) % n)
    batch = todo[:MAX_PER_RUN]

    # Process in small WAVES, each with its own executor and future map. A single
    # executor over the whole batch holds every Future alive until the loop ends, and a
    # Future keeps a hard reference to its result — so all 12 datasets' parsed Python
    # lists coexisted in memory. Python str objects carry ~49 B of overhead EACH, and
    # ons_uk keys run 200+ chars over millions of rows, so that retention is what walked
    # the runner from 1.2 GB to 15.6 GB of 15.99 GB and got the job OOM-killed (measured
    # 2026-07-25 via the CI memory sampler; earlier runs died invisibly because stdout was
    # buffered — see R47). Dropping the executor and its map every wave, then returning
    # Arrow's freed blocks to the OS, bounds peak memory at one wave instead of the batch.
    # Every dataset still gets processed — this changes only how many are held at once.
    for wave_start in range(0, len(batch), WAVE_SIZE):
        wave = batch[wave_start:wave_start + WAVE_SIZE]
        with cancellable_pool(MAX_WORKERS) as ex:
            futs = {ex.submit(_fetch_one, ds_id, href): (ds_id, v) for ds_id, v, href in wave}
            for fut in as_completed(futs):
                ds_id, cur_v = futs[fut]
                _id, keys, dates, vals, err = fut.result()
                if keys is None:
                    print(f"[ons_uk] {ds_id}: transient — {err}", flush=True)
                    # the reason was already in hand and never reached the note
                    tally.transient_unit(f"{ds_id}: {str(err)[:120]}")
                    continue
                if not keys:
                    # Real body, zero parseable rows — or nothing the publisher offers to
                    # fetch. NOT structural: finalize() raises on any structural unit, which
                    # would abort the whole source and block the other ~23 datasets from
                    # publishing (run 30133686534: 2/25 -> nothing merged). Empty + vintage
                    # deliberately NOT advanced, so it retries next tick; rotation stops that
                    # retry from starving the rest of the catalog.
                    print(f"[ons_uk] {ds_id}: no rows — {err or 'parsed 0 rows from a real body'}",
                          flush=True)
                    tally.empty_unit(
                        f"{ds_id}: {str(err) or 'parsed 0 rows from a real body'} "
                        f"(empty ON PURPOSE — structural would abort the source)")
                    continue
                tbl = pa.table({
                    "series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64()),
                })
                path = os.path.join(out_dir, f"{ds_id}.parquet")
                before = blob.row_count(path) if blob.exists(path) else 0
                try:
                    # REPLACE, not merge (R22). Two independent reasons, and either alone
                    # would be sufficient:
                    #   (a) This is a bulk_snapshot source. Every fetch downloads the WHOLE
                    #       version CSV, so the new table is already the complete dataset —
                    #       merging it into itself only costs a read.
                    #   (b) The key grain CHANGED with the v4 parser. Merging would leave the
                    #       old observation-level keys (`CV=14.0:calendar-years=2018:...`)
                    #       sitting alongside the new time-free ones forever, since dedup is
                    #       on (series_key, obs_date) and the keys no longer collide — the
                    #       store would carry both grains and serve both.
                    # merge_and_write still applies the never-shrink invariant in replace
                    # mode, so a truncated upstream pull cannot overwrite good data.
                    n, md = merge.merge_and_write(path, tbl, mode="replace", dedup_keys=DEDUP)
                except DefinitiveError as e:
                    print(f"[ons_uk] {ds_id}: guard refused the write — {e}", flush=True)
                    # isolate a guard trip to this dataset; the reason is already in hand
                    tally.transient_unit(
                        f"{ds_id}: write guard refused over {before:,} stored — {str(e)[:100]}")
                    continue
                published += n
                tally.added_unit(max(0, n - before))
                # CURSORS AT THE CATALOGUE'S GRAIN — ONE PER DATASET, not one per series.
                #
                # This used to fold a cursor for every distinct store key, and the store key
                # is observation-level (`CV=19.0:calendar-years=2019:...` — a coefficient of
                # variation and the time axis are both inside it, defect 1 in the docstring).
                # The result: 10,099,151 cursor rows for ons_uk in state.db, 74% of a file
                # that had grown from 217 MB to 9.44 GB and is pulled and pushed every CI run.
                #
                # And not one of them was USABLE. orchestrate._catalog_ids_for rebuilds a
                # catalog id as "<source>:" + key, and ons_uk's catalogue is DATASET-grain —
                # 42 rows like `ons_uk:ageing-population-estimates`. Ten million
                # observation-level keys mapped to exactly nothing, so the coherence gate
                # could never be satisfied and the source could never report success.
                # Verified: all 42 catalogue slugs are exactly the parquet names, so
                # "ons_uk:" + ds_id IS the catalog id.
                #
                # This also removes what the old comment called "the memory ceiling for this
                # source": _max_by_key over a multi-million-key column is precisely the
                # allocation that killed the 2026-08-01 workstation pass at wave 3 of 12
                # after 8h56m, taking six completed sources' state with it. A per-dataset max
                # is one value.
                #
                # NOTE this was independent of defect 1: the cursor GRAIN is a separate
                # contract from the store KEY grain, and both were broken. Defect 1 is now
                # closed too (v4 parser + mode="replace"), so the store keys are time-free
                # and this cursor is a per-dataset max over a genuine time series.
                if md:
                    iso = str(md)
                    if cursors.get(ds_id, "") < iso:
                        cursors[ds_id] = iso
                if md and (maxd is None or str(md) > str(maxd)):
                    maxd = md
                sidecar[ds_id] = cur_v           # advance ONLY after a clean publish
                # Drop this dataset's payload before starting the next one. Without the
                # explicit del, keys/dates/vals stay bound to the loop variables for the
                # whole wave while the next dataset is already being parsed.
                del tbl, keys, dates, vals
        # Executor and its future map are gone here; hand Arrow's freed blocks back to the
        # OS. Arrow caches released buffers in its pool by default, so RSS stays at the
        # high-water mark across waves and the runner sees no memory being returned.
        gc.collect()
        pa.default_memory_pool().release_unused()
        # PERSIST EVERY WAVE, not once at the end (R273). The orchestrator's per-source cap
        # KILLS the source rather than breaking out of this loop, so anything written only
        # after the loop is written only on the runs that happen to finish early. Both the
        # vintage sidecar and the rotation cursor are state that MUST survive an interrupted
        # run: without this, a capped run re-downloads the same datasets next tick and the
        # rotation never moves, which is the starvation this rotation exists to fix.
        _save_sidecar(out_dir, sidecar)
        _save_rotation(out_dir, wave[-1][0])
        print(f"[ons_uk] wave {wave_start // WAVE_SIZE + 1} done "
              f"({min(wave_start + WAVE_SIZE, len(batch))}/{len(batch)} datasets), "
              f"rotation after={wave[-1][0]}, "
              f"arrow pool {pa.total_allocated_bytes() / 1e6:.0f} MB", flush=True)

    # Deliberately NOT re-saving the sidecar or the rotation here. The per-wave saves above
    # are already the durable record, and a trailing save that recomputed either from
    # loop-local state is exactly how R311 undid its own fix 200 lines later.

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    res = finalize(tally, published, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors)
    if capped:
        # More datasets still owe work — do NOT let the strategy stamp a "fully current"
        # unit vintage, or the remaining backlog would be skipped next tick.
        res.new_vintage = None
    return res
