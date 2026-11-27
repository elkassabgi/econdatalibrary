#!/usr/bin/env python3
"""stat_estonia D2 store repair: re-pull ONE table's whole history at the table's own grain.

WHAT IT REPAIRS (the D2 class, updater/strategies/fetchers/stat_estonia.py header, CHALLENGE B1). The bulk
ingest (jobs/ingest_stat_estonia.query_table) measured MAX_CELLS against the WHOLE history, so a table whose
whole history is over MAX_CELLS (100,000 cells) was stored AGGREGATED, even when one period in full detail
fits under it. The fetcher asks every request at the table's own grain (full detail when one period fits).
On a D1-shape table (a year axis and a month/quarter variable) the boundary request at the stored max then
returns thousands of rows under series the table never held, and the new-series growth bound WITHHOLDS them
on every visit: measured 2026-11-25, TT065.PX 13,189 rows and RRI07.PX 4,939 rows (analysis
F:/work/econ_finish/estonia_fetch/analysis/REPORT.md). Merging them at the boundary would mint one-observation
series with false start dates and a permanent mixed grain. The repair is the whole history at the table's
grain, merged ONCE, after a person has read the counts. That is this tool.

PER TABLE (the fetcher's own functions, nothing re-implemented):
  1. GET the PxWeb metadata (S._get_meta).
  2. Plan with S._plan_table(ing, variables, None, full_pull_grain="table"): every period, the table's grain,
     time-chunked so no POST carries more than MAX_CELLS cells.
  3. POST every chunk (S._post_data) and parse each answer with the ingester's parser
     (ing.parse_jsonstat2). ALL OR NOTHING: a failed or rejected chunk ends the table; nothing merges.
  4. Compare with the store (S._compare_subject, dates=None: the full-pull path).
  5. Print the receipt: fetched rows / keys, new series, held, dup, changed, added, same.

THE FULL-PULL PATH SKIPS THE GROWTH BOUNDS (S._compare_subject: the bounds apply only to rows dated at or
before a stored max, and a full pull passes none). So --apply must be told the count it will add:
  * --expect-new-series N must equal the new-series count measured in this run, for this table;
  * dup (repeated (series_key, obs_date) in the fetch or the stored table) > 0 refuses;
  * held (stored rows of the table the publisher did not return) > 0 refuses. After a held-0 repair the
    table's stored rows are EXACTLY the publisher's rows, so no old-grain key is left beside the new grain
    (R22: a grain change must not leave two grains in one table);
  * a table outside the D2 cell class (one period over MAX_CELLS, or a whole history that fits under it)
    refuses: a full pull would not change its grain.

--apply (ONE table per invocation): the compare is re-measured and the merge runs inside
core.catalog_path.write_session() - after T0 the machine-wide store writer lock, taken here or already held;
before T0 there is no lock, so run it only while updater-daily and updater-heavy are idle (econ-updater
30-serving-pipeline.md, Checklist B2 step 4). The merge is updater.merge.merge_and_write (never-shrink, the
floor set to exactly stored + added rows, as the fetcher sets it). Then two read-backs, two instruments:
  * the store: every publisher row is in the table with the same value, and the table holds no other row;
  * the served byte contract: the table's CSV is derived with core.derive_csv._series_csv_bytes (the bytes the
    served object must hold) from the store root, written to the receipt folder, parsed back and compared
    with the publisher rows.
THE SERVED OBJECT IS NOT WRITTEN BY THIS TOOL. The tool prints the next step: the reviewed publisher
(python -m core.derive_csv ... --only <ids file>) and the byte-compare of the served object with the local CSV.

--census: METADATA ONLY (GETs, never a data POST). It lists every D1-shape table (S._is_d1_shape: a year axis
and a sub-period variable) whose whole history is over MAX_CELLS while one period in full detail is under it
- the class above - with its cell counts. Tables in the same cell class that are NOT D1-shape are listed as
EXCLUDED (the boundary does not reach them, but a tail can: TO0053). A table whose metadata GET failed is
counted UNREACHABLE and the census exits 1: it is then a lower bound, not a census. GETs are spaced --rate
seconds (default 1.0, floor 0.25).

  python tools/stat_estonia_grain_repair.py --tables sotsiaalelu/tooturg/tootud/luhiajastatistika/TT065.PX
  python tools/stat_estonia_grain_repair.py --tables <path.PX> --apply --expect-new-series 15743
  python tools/stat_estonia_grain_repair.py --census --crawl            # walk the PxWeb tree (GETs)
  python tools/stat_estonia_grain_repair.py --census --tables <path.PX> ...

--store-root DIR: a clean_full-shaped root (DIR/stat_estonia/<subject>.parquet) - an F: copy, or a test
folder. Only with AQUEDUCT_BACKEND=local: under r2 a path is mapped to a bucket key, so a copy would write
the real bucket. Default: updater.config.DATA_ROOT, through updater.blob like the fetcher.
Exit: 0 done, 1 a fetch failed (nothing merged) or the census is incomplete, 2 refused (nothing written),
3 a read-back failed after the write.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import datetime as dt
import io
import json
import os
import sys
import time
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pyarrow as pa                                                   # noqa: E402
import pyarrow.compute as pc                                           # noqa: E402

from core import catalog_path, cutover                                 # noqa: E402
from updater import blob, config, merge                                # noqa: E402
from updater.errors import DefinitiveError, TransientError             # noqa: E402
from updater.strategies.fetchers import stat_estonia as S              # noqa: E402

SOURCE = S.SOURCE
WHAT = "tools/stat_estonia_grain_repair.py --apply"
DEFAULT_RATE = 1.0
MIN_RATE = 0.25
OK, FETCH_FAILED, REFUSED, DEFECT = 0, 1, 2, 3
DEFAULT_OUT = os.path.join(ROOT, "logs", "stat_estonia_grain_repair")


def _utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _say(msg: str) -> None:
    print(f"[grain-repair] {msg}", flush=True)


class FetchFailed(Exception):
    """A metadata GET or a data POST of the table failed: nothing of the table is used."""


# --------------------------------------------------------------------------------------------------------- #
# The table's cell class, from its metadata (shared by the repair and the census: one rule, R10).
# --------------------------------------------------------------------------------------------------------- #
def table_class(ing, variables) -> dict | None:
    """None when the table has no time dimension, else
    {"plan", "tcode", "n_periods", "per_period", "whole", "d1", "d2"}.

    per_period = cells of ONE period in full detail (the fetcher's _selection test), whole = the ingester's
    total_cells (every variable, the time axis included). d2 = per_period <= MAX_CELLS < whole: the ingester
    aggregated, the fetcher asks full detail. d1 = the fetcher's own _is_d1_shape (in the plan)."""
    plan = S._plan_table(ing, variables, None, full_pull_grain="table")
    if plan is None:
        return None
    tvars = [v for v in variables if v.get("code") == plan["tcode"]]
    if len(tvars) != 1:
        raise DefinitiveError(f"time variable {plan['tcode']!r} is not one variable of the metadata")
    n_periods = len(tvars[0].get("values") or [])
    per_period = 1
    for v in variables:
        if v is not tvars[0]:
            per_period *= max(len(v.get("values") or []), 1)
    whole = per_period * max(n_periods, 1)
    if plan["queries"] and plan["grain"] != ("full" if per_period <= ing.MAX_CELLS else "aggregate"):
        raise DefinitiveError(f"plan grain {plan['grain']!r} disagrees with {per_period:,} cells per period")
    return {"plan": plan, "tcode": plan["tcode"], "n_periods": n_periods, "per_period": per_period,
            "whole": whole, "d1": bool(plan["d1"]),
            "d2": per_period <= ing.MAX_CELLS < whole}


# --------------------------------------------------------------------------------------------------------- #
# Fetch one table: metadata, every chunk, all or nothing.
# --------------------------------------------------------------------------------------------------------- #
def fetch_table(ing, sess, path: str, rate: float) -> dict:
    url = f"{ing.BASE}/{path}"                     # no trailing slash (ingester note)
    try:
        meta = S._get_meta(sess, url)
    except TransientError as e:
        raise FetchFailed(f"metadata GET failed: {e}") from None
    time.sleep(rate)
    if not meta or not isinstance(meta, dict) or not meta.get("variables"):
        raise FetchFailed("metadata GET answered no table (404/400 or no variables)")
    variables = meta["variables"]
    cls = table_class(ing, variables)
    if cls is None:
        raise FetchFailed("the table has no time dimension")
    plan = cls["plan"]
    prefix = S._table_prefix(path)
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    rows: list = []
    empty_chunks = 0
    for qi, query in enumerate(plan["queries"]):
        body = {"query": query, "response": {"format": "json-stat2"}}
        try:
            resp = S._post_data(sess, url, body)
        except TransientError as e:
            raise FetchFailed(f"data POST {qi + 1} of {len(plan['queries'])} failed: {e}") from None
        time.sleep(rate)
        if not resp or not isinstance(resp, dict):
            raise FetchFailed(f"data POST {qi + 1} of {len(plan['queries'])} rejected (400/403)")
        got = ing.parse_jsonstat2(resp, prefix, meta_time_code)
        empty_chunks += int(not got)       # periods listed with no value yet; counted, held rows guard the rest
        rows.extend(got)
    if not rows:
        raise FetchFailed(f"{len(plan['queries'])} data POST(s) parsed 0 rows")
    # One row per (series_key, obs_date), the first kept, repeats COUNTED (as update() does).
    seen: dict = defaultdict(int)
    kept: list = []
    for key, d, v in rows:
        if isinstance(d, dt.datetime):
            d = d.date()
        seen[(key, d)] += 1
        if seen[(key, d)] == 1:
            kept.append((key, d, v))
    return {"path": path, "prefix": prefix, "cls": cls, "rows": kept, "n_parsed": len(rows),
            "dup_fetched": sum(c - 1 for c in seen.values()), "posts": len(plan["queries"]),
            "empty_chunks": empty_chunks}


# --------------------------------------------------------------------------------------------------------- #
# The store side.
# --------------------------------------------------------------------------------------------------------- #
def subject_path(sdir: str, path: str) -> str:
    subj = S._safe_subject(path.split("/")[0])
    if subj is None:
        raise ValueError(f"{path!r}: the subject token is not a safe file name")
    return os.path.join(sdir, f"{subj}.parquet")


def stored_table(spath: str, prefix: str) -> dict:
    """{(series_key, obs_date): value} of ONE table in its subject file, and its repeated pairs. One pass in
    batches, blob-routed (R36)."""
    out: dict = {}
    dup = 0
    if not blob.exists(spath):
        return {"rows": out, "dup": 0}
    for b in blob.iter_batches(spath, columns=["series_key", "obs_date", "value"]):
        bt = pa.Table.from_batches([b])
        keys = bt.column("series_key")
        if keys.type != pa.string():
            keys = pc.cast(keys, pa.string())
        mask = pc.fill_null(pc.equal(S._prefixes(keys), prefix), False)
        if not pc.any(mask).as_py():
            continue
        for k, d, v in zip(pc.filter(keys, mask).to_pylist(),
                           pc.filter(bt.column("obs_date"), mask).to_pylist(),
                           pc.filter(bt.column("value"), mask).to_pylist()):
            if isinstance(d, dt.datetime):
                d = d.date()
            if (k, d) in out:
                dup += 1
            out[(k, d)] = v
    return {"rows": out, "dup": dup}


def measure(got: dict, spath: str, max_cells: int) -> tuple[dict, object]:
    """The receipt of merging `got` into its subject file, and the rows that would merge (or None)."""
    prefix = got["prefix"]
    st = stored_table(spath, prefix)
    stored_keys = {k for k, _ in st["rows"]}
    stored_dates = [d for _, d in st["rows"]]
    fetched_keys = {k for k, _, _ in got["rows"]}
    new_keys = fetched_keys - stored_keys
    n_stored = len(st["rows"]) + st["dup"]
    fetched = {prefix: {"rows": got["rows"], "dates": None, "stored_max": None,
                        "dup_fetched": got["dup_fetched"], "tail_grain_changed": False}}
    stats, mtbl = S._compare_subject(spath, fetched, {prefix: n_stored})
    s = stats[prefix]
    cls = got["cls"]
    rec = {
        "table": got["path"], "prefix": prefix, "catalog_id": f"{SOURCE}:{prefix}", "subject_file": spath,
        "time_variable": cls["tcode"], "periods": cls["n_periods"], "cells_per_period": cls["per_period"],
        "cells_whole_history": cls["whole"], "max_cells": max_cells, "d1_shape": cls["d1"],
        "d2_class": cls["d2"], "grain": cls["plan"]["grain"], "posts": got["posts"],
        "empty_chunks": got["empty_chunks"],
        "fetched_rows": len(got["rows"]), "fetched_keys": len(fetched_keys),
        "stored_rows": n_stored, "stored_keys": len(stored_keys),
        "stored_max": max(stored_dates).isoformat() if stored_dates else None,
        "new_series": len(new_keys),
        "rows_under_new_series": sum(1 for k, _, _ in got["rows"] if k in new_keys),
        "stored_keys_not_fetched": len(stored_keys - fetched_keys),
        "held": s["held"], "dup_fetched": s["dup_fetched"], "dup_stored": s["dup_stored"],
        "changed": s["changed"], "added": s["added"],
        "same": len(got["rows"]) - s["changed"] - s["added"],
        "compare_refused": bool(s["refused"]),
        "withheld_new_series": s["withheld_new_series"], "withheld_revision": s["withheld_revision"],
        "merged_changed": s.get("merged_changed", 0), "merged_added": s.get("merged_added", 0),
    }
    return rec, mtbl


def print_receipt(rec: dict) -> None:
    _say(f"{rec['table']}: grain={rec['grain']} time={rec['time_variable']!r} periods={rec['periods']} "
         f"cells/period={rec['cells_per_period']:,} cells whole history={rec['cells_whole_history']:,} "
         f"(MAX_CELLS {rec['max_cells']:,}) d1_shape={rec['d1_shape']} d2_class={rec['d2_class']} "
         f"posts={rec['posts']} (with no value {rec['empty_chunks']})")
    _say(f"{rec['table']}: RECEIPT fetched rows={rec['fetched_rows']:,} keys={rec['fetched_keys']:,} | "
         f"new series={rec['new_series']:,} (rows under them {rec['rows_under_new_series']:,}) | "
         f"held={rec['held']:,} dup={rec['dup_fetched'] + rec['dup_stored']:,} "
         f"(fetched {rec['dup_fetched']:,}, stored {rec['dup_stored']:,}) | changed={rec['changed']:,} "
         f"added={rec['added']:,} same={rec['same']:,} | stored before: rows={rec['stored_rows']:,} "
         f"keys={rec['stored_keys']:,} max={rec['stored_max']} keys not fetched={rec['stored_keys_not_fetched']:,}")


def refusals(rec: dict, expect) -> list[str]:
    """Why --apply must not write this table. Empty = it may. ONE gate per condition."""
    why = []
    if expect is None:
        why.append("--expect-new-series N is required (the full-pull path skips the growth bounds; "
                   "N is the dry run's measured new-series count)")
    elif expect != rec["new_series"]:
        why.append(f"--expect-new-series {expect:,} != the {rec['new_series']:,} new series measured now")
    if rec["dup_fetched"] or rec["dup_stored"]:
        why.append(f"duplicate (series_key, obs_date): {rec['dup_fetched']:,} in the fetch, "
                   f"{rec['dup_stored']:,} in the stored table - the key is not a key here")
    if rec["held"]:
        why.append(f"held {rec['held']:,}: stored rows of the table the publisher did not return - a merge "
                   f"would leave them beside the new grain (R22); this needs a reviewed replace, not this tool")
    if not rec["d2_class"]:
        why.append(f"not the D2 class: {rec['cells_per_period']:,} cells per period, "
                   f"{rec['cells_whole_history']:,} whole history, MAX_CELLS {rec['max_cells']:,} - "
                   f"a full pull does not change this table's grain")
    return why


# --------------------------------------------------------------------------------------------------------- #
# Read-backs after the write: the store, then the served byte contract.
# --------------------------------------------------------------------------------------------------------- #
def read_back_store(spath: str, got: dict) -> list[str]:
    st = stored_table(spath, got["prefix"])
    want = {(k, d): v for k, d, v in got["rows"]}
    bad = []
    missing = sum(1 for kd in want if kd not in st["rows"])
    differ = sum(1 for kd, v in want.items() if kd in st["rows"] and st["rows"][kd] != v)
    extra = sum(1 for kd in st["rows"] if kd not in want)
    if missing or differ or extra or st["dup"]:
        bad.append(f"store read-back: {missing:,} publisher row(s) missing, {differ:,} with another value, "
                   f"{extra:,} stored row(s) the publisher did not return, {st['dup']:,} repeated")
    return bad


@contextlib.contextmanager
def _data_root(store_root: str):
    """The resolver's data root (ECONDL_DATA) for the derive, restored afterwards."""
    old = os.environ.get("ECONDL_DATA")
    os.environ["ECONDL_DATA"] = store_root
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("ECONDL_DATA", None)
        else:
            os.environ["ECONDL_DATA"] = old


def derive_and_read_back(store_root: str, got: dict, out_dir: str, stamp: str) -> tuple[list[str], str]:
    """Derive the table's CSV with the served byte contract, keep it, parse it back, compare it with the
    publisher rows. Returns (problems, csv path)."""
    from core import derive_csv                                        # noqa: PLC0415
    cid = f"{SOURCE}:{got['prefix']}"
    with _data_root(store_root):
        body = derive_csv._series_csv_bytes(cid)
    leaf = got["path"].rsplit("/", 1)[-1]
    out = os.path.join(out_dir, f"{stamp}_{leaf}.csv")
    with open(out, "wb") as fh:
        fh.write(body)
    rd = csv.reader(io.StringIO(body.decode("utf-8")))
    header = next(rd, None)
    bad = []
    if header != ["series_id", "obs_date", "value"]:
        bad.append(f"derived CSV header {header!r}")
        return bad, out
    want = {(k, d.isoformat()): v for k, d, v in got["rows"]}
    n = missing_bad = 0
    seen = set()
    for row in rd:
        n += 1
        if len(row) != 3:
            missing_bad += 1
            continue
        sid, d, v = row
        seen.add((sid, d))
        try:
            fv = float(v)
        except ValueError:
            missing_bad += 1
            continue
        if (sid, d) not in want or fv != want[(sid, d)]:
            missing_bad += 1
    absent = sum(1 for kd in want if kd not in seen)
    if n != len(want) or missing_bad or absent:
        bad.append(f"derived CSV read-back: {n:,} row(s) for {len(want):,} publisher rows, {missing_bad:,} "
                   f"not a publisher row (or another value), {absent:,} publisher row(s) absent")
    return bad, out


# --------------------------------------------------------------------------------------------------------- #
# The repair run.
# --------------------------------------------------------------------------------------------------------- #
def _write_receipt(out_dir: str, stamp: str, leaf: str, rec: dict) -> str:
    os.makedirs(out_dir, exist_ok=True)
    p = os.path.join(out_dir, f"{stamp}_{leaf}.json")
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=1, sort_keys=True, default=str)
    return p


def _is_store_of_record(store_root: str) -> bool:
    same = lambda a, b: os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))  # noqa: E731
    return same(store_root, config.DATA_ROOT)


def repair(a, sess, ing) -> int:
    store_root = a.store_root or config.DATA_ROOT
    sdir = os.path.join(store_root, SOURCE)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    os.makedirs(a.out_dir, exist_ok=True)
    _say(f"{'APPLY' if a.apply else 'DRY RUN (nothing is written to the store)'} at {_utc()}; store "
         f"{sdir}; backend {os.environ.get('AQUEDUCT_BACKEND', 'local')}; cut over (T0) {cutover.is_cut_over()}; "
         f"MAX_CELLS {ing.MAX_CELLS:,}; GET/POST spacing {a.rate:.2f} s")
    worst = OK
    for path in a.tables:
        leaf = path.rsplit("/", 1)[-1]
        try:
            spath = subject_path(sdir, path)
        except ValueError as e:
            _say(f"REFUSED {path}: {e}")
            worst = max(worst, REFUSED)
            continue
        try:
            got = fetch_table(ing, sess, path, a.rate)
        except FetchFailed as e:
            _say(f"FETCH FAILED {path}: {e} - nothing of the table is used")
            worst = max(worst, FETCH_FAILED)
            continue
        session = catalog_path.write_session() if a.apply else contextlib.nullcontext()
        with session:
            # Measured INSIDE the session: the counts --apply is gated on are the store it writes.
            rec, mtbl = measure(got, spath, ing.MAX_CELLS)
            rec["measured_utc"] = _utc()
            print_receipt(rec)
            why = refusals(rec, a.expect_new_series)
            if rec["compare_refused"] and not (rec["dup_fetched"] or rec["dup_stored"]):
                raise DefinitiveError(f"{path}: the compare refused the table with no repeated key")
            rec["apply_refusals"] = why
            if not a.apply:
                rec["mode"] = "dry-run"
                _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)}")
                # a dry run has no N to check; every OTHER gate is what --apply would meet
                blocking = [w for w in why if not w.startswith("--expect-new-series N is required")]
                for w in blocking:
                    _say(f"{path}: --apply would be REFUSED: {w}")
                if not blocking:
                    _say(f"{path}: apply with: python tools/stat_estonia_grain_repair.py --tables {path} "
                         f"--apply --expect-new-series {rec['new_series']}"
                         + (f" --store-root {a.store_root}" if a.store_root else ""))
                continue
            rec["mode"] = "apply"
            if why:
                for w in why:
                    _say(f"REFUSED {path}: {w}")
                rec["result"] = "refused"
                _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)} - nothing was written")
                worst = max(worst, REFUSED)
                continue
            if mtbl is None:
                _say(f"{path}: nothing to merge (every publisher row is already stored with its value)")
                rec["result"] = "nothing to merge"
                _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)}")
                continue
            before = blob.row_count(spath)
            exp_added = rec["merged_added"]
            floor_ratio = ((before + exp_added - 0.5) / before) if before else 0.97
            try:
                n, _md, ch = merge.merge_and_write(spath, mtbl, mode="merge", dedup_keys=S.DEDUP,
                                                   min_ratio=floor_ratio, report_changed_keys=True,
                                                   changed_keys_cap=max(mtbl.num_rows, 1))
            except (DefinitiveError, ValueError) as e:
                # merge_and_write refuses BEFORE it publishes: the stored file is unchanged
                _say(f"REFUSED {path}: the merge refused ({str(e)[:200]}); the stored file is unchanged")
                rec["result"] = "merge refused"
                _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)}")
                worst = max(worst, REFUSED)
                continue
            problems = []
            if n != before + exp_added:
                problems.append(f"merge: {before:,} stored + {exp_added:,} added != {n:,} after")
            if set(ch) != set(mtbl.column("series_key").to_pylist()):
                problems.append(f"merge: the changed-key report ({len(ch):,}) != the compare's changed+added "
                                f"keys")
            problems += read_back_store(spath, got)
            csv_bad, csv_path = derive_and_read_back(store_root, got, a.out_dir, stamp)
            problems += csv_bad
            rec.update(rows_before=before, rows_after=n, derived_csv=csv_path, read_back_problems=problems)
            ids = os.path.join(a.out_dir, f"{stamp}_{leaf}.ids.txt")
            with open(ids, "w", encoding="utf-8") as fh:
                fh.write(rec["catalog_id"] + "\n")
            if problems:
                for p in problems:
                    _say(f"READ-BACK FAILED {path}: {p}")
                rec["result"] = "read-back failed"
                _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)}")
                worst = max(worst, DEFECT)
                continue
            rec["result"] = "merged and read back"
            _say(f"{path}: MERGED {exp_added:,} added + {rec['merged_changed']:,} changed rows; subject file "
                 f"{before:,} -> {n:,} rows; store read-back OK ({len(got['rows']):,} rows); derived CSV "
                 f"read-back OK ({csv_path})")
            _say(f"receipt {_write_receipt(a.out_dir, stamp, leaf, rec)}")
            _say("NEXT 1 (not done here - the served object is unchanged): publish the table's CSV with "
                 f"python -m core.derive_csv --source {SOURCE} --only {ids} (and --bucket econ-data before T0), "
                 f"then byte-compare the served object with {csv_path}")
            _say(f"NEXT 2: the receipt that the hold is gone is the next pass that VISITS this table logging "
                 f"'{path}: ... withheld_new_series=0'")
    return worst


# --------------------------------------------------------------------------------------------------------- #
# The census: metadata GETs only.
# --------------------------------------------------------------------------------------------------------- #
def crawl(ing, sess, rate: float) -> tuple[list[dict], int]:
    """Walk the PxWeb tree with GETs (the ingester's crawl_catalog walk, without its cache file)."""
    tables, failed = [], 0
    queue = [""]
    while queue:
        p = queue.pop(0)
        url = f"{ing.BASE}/{p}" if p else ing.BASE
        try:
            items = S._get_meta(sess, url)
        except TransientError as e:
            failed += 1
            _say(f"crawl: GET {p or '(root)'} failed: {e}")
            items = None
        time.sleep(rate)
        if not isinstance(items, list):
            continue
        for it in items:
            child = f"{p}/{it.get('id', '')}".lstrip("/")
            if it.get("type", "l") == "t":
                tables.append({"path": child, "id": it.get("id", ""), "text": it.get("text", "")})
            elif it.get("type", "l") == "l":
                queue.append(child)
    return tables, failed


def census(a, sess, ing) -> int:
    os.makedirs(a.out_dir, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    crawl_failed = 0
    if a.tables:
        paths, origin = list(a.tables), "--tables"
    elif a.catalog:
        with open(a.catalog, encoding="utf-8") as fh:
            paths, origin = [t["path"] for t in json.load(fh)], a.catalog
    elif a.crawl:
        tabs, crawl_failed = crawl(ing, sess, a.rate)
        cp = os.path.join(a.out_dir, f"{stamp}_catalog_crawled.json")
        with open(cp, "w", encoding="utf-8") as fh:
            json.dump(tabs, fh)
        paths, origin = [t["path"] for t in tabs], f"crawl ({cp})"
    else:
        sdir = os.path.join(a.store_root or config.DATA_ROOT, SOURCE)
        raw = blob.read_bytes(os.path.join(sdir, "_catalog.json"))
        if raw is None:
            _say(f"REFUSED: no _catalog.json in {sdir}; give --tables, --catalog or --crawl")
            return REFUSED
        paths, origin = [t["path"] for t in json.loads(raw.decode("utf-8"))], f"{sdir}/_catalog.json"
    _say(f"CENSUS (metadata GETs only) at {_utc()}: {len(paths):,} table(s) from {origin}; MAX_CELLS "
         f"{ing.MAX_CELLS:,}; GET spacing {a.rate:.2f} s")
    rows, n = [], defaultdict(int)
    for i, path in enumerate(paths):
        try:
            meta = S._get_meta(sess, f"{ing.BASE}/{path}")
        except TransientError as e:
            n["unreachable"] += 1
            rows.append((path, "UNREACHABLE", "", "", "", "", str(e)[:120]))
            time.sleep(a.rate)
            continue
        time.sleep(a.rate)
        if not meta or not isinstance(meta, dict) or not meta.get("variables"):
            n["gone"] += 1
            rows.append((path, "GONE", "", "", "", "", "metadata GET answered no table"))
            continue
        try:
            cls = table_class(ing, meta["variables"])
        except Exception as e:                                         # noqa: BLE001 - one table, named
            n["unreachable"] += 1
            rows.append((path, "UNREACHABLE", "", "", "", "", f"metadata not classified: {type(e).__name__}: "
                                                              f"{str(e)[:100]}"))
            continue
        if cls is None:
            n["no_time"] += 1
            rows.append((path, "NO_TIME", "", "", "", "", ""))
            continue
        verdict = ("MEMBER" if cls["d2"] and cls["d1"] else "EXCLUDED_NOT_D1" if cls["d2"]
                   else "OUTSIDE")
        n[verdict.lower()] += 1
        rows.append((path, verdict, str(cls["d1"]), str(cls["n_periods"]), str(cls["per_period"]),
                     str(cls["whole"]), f"chunks={len(cls['plan']['queries'])}"))
        if (i + 1) % 250 == 0:
            _say(f"census: {i + 1:,} of {len(paths):,} table(s) read; members so far {n['member']:,}")
    tsv = os.path.join(a.out_dir, f"{stamp}_census.tsv")
    with open(tsv, "w", encoding="utf-8", newline="") as fh:
        fh.write("table\tverdict\td1_shape\tperiods\tcells_per_period\tcells_whole_history\tnote\n")
        for r in rows:
            fh.write("\t".join(r) + "\n")
    for r in rows:
        if r[1] in ("MEMBER", "EXCLUDED_NOT_D1"):
            _say(f"{r[1]:16} {r[0]}  periods={r[3]} cells/period={int(r[4]):,} whole={int(r[5]):,}")
    _say(f"CENSUS: {len(paths):,} table(s): MEMBER (D1-shape, per period <= MAX_CELLS < whole) {n['member']:,}; "
         f"EXCLUDED (same cells, not D1-shape) {n['excluded_not_d1']:,}; outside the cell class "
         f"{n['outside']:,}; no time dimension {n['no_time']:,}; gone {n['gone']:,}; UNREACHABLE "
         f"{n['unreachable']:,}; crawl GETs failed {crawl_failed:,}. TSV {tsv}")
    if n["unreachable"] or crawl_failed:
        _say("CENSUS INCOMPLETE: a metadata GET failed, so the counts above are a LOWER BOUND")
        return FETCH_FAILED
    return OK


# --------------------------------------------------------------------------------------------------------- #
def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0], allow_abbrev=False)
    ap.add_argument("--tables", nargs="+", metavar="PATH.PX", help="PxWeb table path(s), e.g. majandus/.../X.PX")
    ap.add_argument("--apply", action="store_true", help="merge ONE table (default: a dry run)")
    ap.add_argument("--expect-new-series", type=int, default=None, help="--apply: the dry run's new-series count")
    ap.add_argument("--store-root", default=None, help="a clean_full-shaped root (AQUEDUCT_BACKEND=local only)")
    ap.add_argument("--out-dir", default=DEFAULT_OUT, help="receipts, derived CSVs, census TSVs")
    ap.add_argument("--rate", type=float, default=DEFAULT_RATE, help=f"seconds between requests (>= {MIN_RATE})")
    ap.add_argument("--census", action="store_true", help="metadata-only census of the class (GETs only)")
    ap.add_argument("--catalog", default=None, help="--census: a _catalog.json-shaped file of tables")
    ap.add_argument("--crawl", action="store_true", help="--census: walk the PxWeb tree for the tables (GETs)")
    return ap


def main(argv=None, session=None) -> int:
    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except Exception:                                                  # noqa: BLE001
        pass
    a = _parser().parse_args(argv)
    if a.rate < MIN_RATE:
        _say(f"--rate {a.rate} is under the {MIN_RATE} s floor; using {MIN_RATE}")
        a.rate = MIN_RATE
    backend = os.environ.get("AQUEDUCT_BACKEND", "local").strip().lower()
    if a.store_root and backend != "local":
        _say(f"REFUSED: --store-root needs AQUEDUCT_BACKEND=local (it is {backend!r}): under r2 a store path is "
             f"mapped to a bucket key, so a copy would be written to the real bucket. Nothing was written.")
        return REFUSED
    if a.census:
        if a.apply or a.expect_new_series is not None:
            _say("REFUSED: --census reads metadata only; it takes no --apply. Nothing was written.")
            return REFUSED
        if sum(bool(x) for x in (a.tables, a.catalog, a.crawl)) > 1:
            _say("REFUSED: give at most one of --tables, --catalog, --crawl")
            return REFUSED
    else:
        if not a.tables:
            _say("REFUSED: give --tables (or --census)")
            return REFUSED
        bad = [p for p in a.tables if "/" not in p or not p.lower().endswith(".px")]
        if bad:
            _say(f"REFUSED: not a PxWeb table path: {bad}")
            return REFUSED
        if a.apply and len(a.tables) != 1:
            _say(f"REFUSED: --apply takes ONE table per invocation ({len(a.tables)} given). Nothing was written.")
            return REFUSED
        if a.apply and a.expect_new_series is None:
            _say("REFUSED: --apply needs --expect-new-series N (the dry run prints N; the full-pull path skips "
                 "the growth bounds). Nothing was written.")
            return REFUSED
        if a.apply:
            if not cutover.is_cut_over():
                _say("before T0 there is no store writer lock: run --apply only while updater-daily and "
                     "updater-heavy are idle (econ-updater Checklist B2 step 4)")
            elif _is_store_of_record(a.store_root or config.DATA_ROOT):
                try:
                    blob.refuse_unless_live_checkout(WHAT)
                except cutover.CutoverRefused as e:
                    _say(f"REFUSED: {e}. Nothing was written.")
                    return REFUSED
    ing = S._ingester()
    if session is None:
        import requests                                                # noqa: PLC0415
        session = requests.Session()
        session.headers.update(dict(S.UA, Connection="close"))
    try:
        return census(a, session, ing) if a.census else repair(a, session, ing)
    except cutover.CutoverRefused as e:
        _say(f"REFUSED: {e}")
        return REFUSED


if __name__ == "__main__":
    sys.exit(main())
