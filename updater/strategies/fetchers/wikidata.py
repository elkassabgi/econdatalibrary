"""S1 fetcher — Wikidata econ/finance ENTITY universe (reference data; CC0).

Wikidata is a knowledge graph, not a time series. Three grouped parquet "cubes" live
under clean_full/wikidata/, one row per entity keyed by `series_key` = the Wikidata QID:

    companies.parquet        one row per company with a ticker (P249) OR an ISIN (P946)
    stock_exchanges.parquet  one row per stock exchange (P31/P279* Q11691)
    currencies.parquet       one row per currency (P31/P279* Q8142)

Each cube has its OWN schema, so each merges into its own path with dedup_keys=("series_key",)
— there is no obs_date here (entities, not observations); merge unions new rows with the
published cube, new wins on an attribute edit, and the never-shrink guard refuses to let a
throttled WDQS run overwrite a complete cube with a smaller snapshot.

S1 (overwrite_if_changed): re-fetch the WHOLE entity set by REUSING jobs/ingest_wikidata.py's
SPARQL endpoint + COUNT probes + paged GROUP_CONCAT queries + row-shapers, build one pyarrow
table per cube, and publish ONLY via merge.merge_and_write. The cheap vintage is the three
SPARQL COUNT probes combined into one token (the registry's vintage_signal hint) — it moves
when the published per-cube totals move; None when WDQS can't be reached cheaply (the strategy
then fetches anyway, which is safe).

Honest status: a cube that pages 0 rows from a healthy endpoint with a published total > 0 is
structural; a persistent WDQS timeout/5xx/429 (the ingester's run_sparql exhausts its retries
and raises) is transient; a published total of 0 is a genuine empty.
"""
from __future__ import annotations

import importlib.util
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      save_rotation)

# Wall-clock cap for one wikidata run. WDQS paging is unbounded from our side — each
# cube walks a DISTINCT entity set page by page, so a slow or throttled WDQS turns
# that into an open-ended loop while every source queued behind it waits (sources run
# strictly serially). A cube skipped here keeps its existing rows counted and does NOT
# advance its vintage, so it is re-pulled next tick rather than silently dropped.
BUDGET_MIN = 25

SOURCE = "wikidata"
DEDUP = ("series_key",)

# Cube definitions: (cube name, parquet basename, COUNT-probe attr, page-query attr,
# row-shaper attr) — all resolved against jobs/ingest_wikidata.py so the SPARQL + parse
# logic is reused, never re-implemented here.
CUBES = [
    ("companies",       "companies.parquet",       "COUNT_COMPANIES",  "companies_page",  "shape_company"),
    ("stock_exchanges", "stock_exchanges.parquet", "COUNT_EXCHANGES",  "exchanges_page",  "shape_exchange"),
    ("currencies",      "currencies.parquet",      "COUNT_CURRENCIES", "currencies_page", "shape_currency"),
]


def _ingest_mod():
    """Load jobs/ingest_wikidata.py by path (standalone script, not a package) so we reuse
    its ENDPOINT, run_sparql/count, the paged GROUP_CONCAT queries, and the row-shapers."""
    path = os.path.join(config.JOBS_DIR, "ingest_wikidata.py")
    spec = importlib.util.spec_from_file_location("_ingest_wikidata", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def current_vintage(unit):
    """Cheap probe: the three SPARQL COUNT totals combined into one token.

    Moves iff a per-cube published total moves (the registry vintage_signal hint). Returns
    None — never raises — if WDQS can't be reached cheaply, so detection failing does not
    fail the run; update()'s real pull handles transients via the ingester's retry/backoff."""
    try:
        ing = _ingest_mod()
    except Exception:
        return None
    parts = []
    for name, _basename, count_attr, _page_attr, _shape_attr in CUBES:
        try:
            n = ing.count(getattr(ing, count_attr), f"vintage-{name}")
        except (requests.RequestException, RuntimeError, ValueError):
            return None  # transient WDQS hiccup -> undeterminable; strategy fetches anyway
        parts.append(f"{name[:2]}{n}")
    return "-".join(parts)


def _pull_cube_records(ing, page_fn, shape_fn, total, tag):
    """Page the DISTINCT entity set and shape each row, deduping by QID — the same loop as
    jobs/ingest_wikidata.pull_cube but WITHOUT writing parquet (merge_and_write publishes).

    Re-raises RuntimeError (run_sparql exhausted its retries) so update() tallies transient."""
    records: dict[str, dict] = {}
    offset = 0
    page_no = 0
    while True:
        page_no += 1
        rows = ing.run_sparql(page_fn(ing.PAGE, offset), f"{tag}#{page_no}")
        if not rows:
            break
        new = 0
        for rr in rows:
            d = {k: v.get("value") for k, v in rr.items()}
            rec = shape_fn(d)
            key = rec.get("series_key")
            if not key:
                continue
            if key not in records:
                records[key] = rec
                new += 1
        offset += ing.PAGE
        if offset >= total and new == 0:
            break
        if offset >= total + ing.PAGE:  # hard safety stop (matches the ingester)
            break
        import time
        time.sleep(ing.SLEEP_BETWEEN_PAGES)
    return list(records.values())


def _table_for(path, recs):
    """Build an all-string pyarrow table whose columns EXACTLY match the published cube's
    schema (merge refuses to drop a published column). Column order = existing schema if the
    cube already exists, else the shaped-record key order."""
    if blob.exists(path):
        cols = list(blob.read_table(path).schema.names)
    else:
        cols = list(recs[0].keys())
    return pa.table({c: pa.array([r.get(c) for r in recs], type=pa.string()) for c in cols})


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    try:
        ing = _ingest_mod()
    except Exception as e:
        # Can't even load the ingester -> nothing fetched; leave data untouched, transient.
        t = Tally()
        t.transient_unit()
        return finalize(t, 0, None, source=SOURCE)

    tally = Tally()
    total_rows = 0
    cursors: dict[str, str] = {}
    run_day = None  # set to the manifest-style UTC day once we have a healthy pull

    dl = Deadline(minutes=BUDGET_MIN)
    # ROTATE (2026-07-30, R190). CUBES is a module-level literal, so a budget over a fixed
    # order always starts at "companies"; if the first cubes exhaust the 25 minutes the
    # last one is never pulled and the log's "retries next tick" is false — it retries the
    # same two cubes forever. With only three cubes that is a third of the source silently
    # frozen. Resume after whichever cube we reached last.
    cubes = rotate_after(list(CUBES), load_rotation(out_dir), key=lambda c: c[0])
    last_started = None
    for name, basename, count_attr, page_fn_attr, shape_fn_attr in cubes:
        path = os.path.join(out_dir, basename)
        before = blob.row_count(path)

        if dl.spent():
            # Budget gone: stop STARTING cubes. Count what this cube already holds so
            # the reported obs still describes the whole source, and tally it transient
            # so the run reports `partial` and retries — never `ok`, which would claim
            # a completeness this run did not achieve.
            print(f"[wikidata] budget {BUDGET_MIN} min spent — {name} not pulled this "
                  f"run (keeping {before:,} existing rows); the next run RESUMES AFTER "
                  f"{last_started} so it is reached", flush=True)
            tally.deferred_unit(name)
            total_rows += before
            continue
        last_started = name

        # Cheap published total for this cube (also the per-cube transient/empty signal).
        try:
            total = ing.count(getattr(ing, count_attr), f"count-{name}")
        except (requests.RequestException, RuntimeError, ValueError) as e:
            tally.transient_unit(f"{name}: COUNT query failed — {type(e).__name__}")
            total_rows += before
            continue

        if total == 0:
            # WDQS healthy but the COUNT genuinely returned 0 entities for this cube.
            tally.empty_unit(f"{name}: COUNT returned 0 entities")
            total_rows += before
            continue

        try:
            recs = _pull_cube_records(ing, getattr(ing, page_fn_attr),
                                      getattr(ing, shape_fn_attr), total, name)
        except (requests.RequestException, RuntimeError, ValueError) as e:
            # run_sparql exhausted retries on a 5xx/429/timeout/network drop -> transient.
            tally.transient_unit(f"{name}: paging exhausted retries — {type(e).__name__}")
            total_rows += before
            continue

        if not recs:
            # 200 responses that paged 0 rows while the COUNT says total>0 -> structural break
            # (schema/predicate change), NOT a quiet day. finalize() raises DefinitiveError.
            tally.structural_unit(
                f"{name}: COUNT says {total:,} but paging returned 0 rows")
            total_rows += before
            continue

        tbl = _table_for(path, recs)

        # Publish ONLY via merge (atomic, dedup on QID, never-shrink @0.97). A throttled run
        # returning a partial cube can only ADD via union — it can never drop the published
        # rows, and the 0.97 floor refuses any net shrink, exactly the registry's open_question.
        n, _md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        new_rows = max(0, n - before)
        if new_rows:
            tally.added += new_rows
            tally.attempted += 1
        else:
            tally.empty_unit()
        total_rows += n

        import time
        run_day = time.strftime("%Y-%m-%d", time.gmtime())
        cursors[name] = run_day

    # Saved even after a COMPLETE pass: the bookmark is then the last cube in order and
    # the next run wraps to the first, through the same code path.
    if last_started:
        save_rotation(out_dir, last_started)

    return finalize(tally, total_rows, run_day, source=SOURCE,
                    series_cursors=cursors or None)
