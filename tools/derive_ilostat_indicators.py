"""Derive ilostat CSVs at INDICATOR grain and put them in R2.

ilostat holds 388,161,420 observations across 1,947 indicator-frequency files, and a 40-file
sample measures 7,675,015 rows / 765,292 distinct series = 10.0 observations per series, implying
~38.7 million series storewide. Series grain is therefore not a product: it would mean 38.7M CSVs
averaging ten rows each, four times the entire library's current served series count for one
source. The indicator file is the unit, exactly as it is for istat (9.2 obs/series), cso, usda
and the PxWeb family.

WHAT WAS ACTUALLY WRONG. ilostat is not a dark store — it is served, live:true, and updating.
The catalogue simply exposes 80 series totalling 3,466 observations out of those 388 million.
The resolver reads <root>/ilostat/<flow>_A.parquet where root is data/clean_full, the SAME
directory the fetcher writes, so the served 80 are fresh; there are just 1,947 files' worth of
data nobody can reach. (data/clean/ilostat — 80 files, 3,466 rows — is a dead legacy artifact on
no serving path.)

SPLITTING IS EASIER HERE THAN FOR istat, and this exploits that rather than copying the harder
method. ilostat files carry REAL dimension columns — ref_area, sex, classif1, classif2, source —
so a split groups by a column instead of regex-extracting a value out of the key string. 192 of
the 1,947 files exceed the 500,000-row bound; the largest is EMP_TEMP_SEX_ECO_EDU_NB_Q at
4,668,988.

CARDINALITY IS NOT BALANCE, so every candidate is CHECKED by measuring its largest resulting
part, and pairs are tried when no single column divides the file (the istat derive refused three
flows holding 96.7M rows before it learned to cross two dimensions — R219).

THE ID FORM IS `ilostat:<stem>` or `ilostat:<stem>#<part>`, which COEXISTS with the older
`ilostat:<flow>:<classif1>:<geo>`. They are told apart by segment count — 2 versus 4 — and that
is a rule rather than a coincidence: all 1,947 file stems match [A-Za-z0-9_]+ exactly, verified,
so a stem can never introduce a third colon.
"""
from __future__ import annotations

import argparse
import csv
import glob
import io
import json
import os
import queue
import sys
import threading
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                        # noqa: E402

SOURCE = "ilostat"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
HEADER = ["series_id", "obs_date", "value"]
MAX_ROWS_DEFAULT = 500_000
# The real dimension columns, coarsest-looking first. `time` is excluded on purpose: splitting a
# time series by time is the one cut that makes every part useless on its own.
DIM_COLS = ("sex", "source", "classif1", "classif2", "ref_area")


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """(column, parts) for a file needing a split; (None, 1) if it fits; ("", 0) if refused.

    Candidates are measured, never assumed: a column with "enough" distinct values can still put
    most rows in one group, which produces one oversized object plus a pile of trivia.
    """
    if n_rows <= max_rows:
        return None, 1
    cols = [c for c in DIM_COLS if c in set(pq.read_schema(path).names)]
    card = {}
    for c in cols:
        try:
            card[c] = con.execute(
                f'select count(distinct "{c}") from read_parquet(\'{path}\')').fetchone()[0]
        except Exception:                                       # noqa: BLE001
            continue

    def largest(expr: str):
        return con.execute(
            f"select max(n) from (select count(*) n from read_parquet('{path}') "
            f"where value is not null and obs_date is not null group by {expr})").fetchone()[0]

    for c, d in sorted((c, d) for d, c in card.items() if 2 <= c <= 2000):
        try:
            if largest(f'"{d}"') <= max_rows:
                return d, c
        except Exception:                                       # noqa: BLE001
            continue
    # PAIRS. Cross two columns and the part count multiplies while each part shrinks. Ordered by
    # the product so the coarsest workable cross wins; capped so parts stay worth fetching.
    usable = sorted((c, d) for d, c in card.items() if c >= 2)
    for prod, d1, d2 in sorted(
            (c1 * c2, n1, n2) for i, (c1, n1) in enumerate(usable)
            for c2, n2 in usable[i + 1:] if 2 <= c1 * c2 <= 20_000):
        try:
            if largest(f'"{d1}", "{d2}"') > max_rows:
                continue
            # '~' joins the two values in the part label, so it must be absent from both
            # vocabularies or the label cannot be split back apart. Checked, not assumed.
            if con.execute(f"select count(*) from read_parquet('{path}') "
                           f"where \"{d1}\" like '%~%' or \"{d2}\" like '%~%'").fetchone()[0]:
                continue
            return f"{d1}+{d2}", prod
        except Exception:                                       # noqa: BLE001
            continue
    return "", 0


def part_expr(dim: str) -> str:
    """SQL for the part label. ONE definition, imported by the catalogue and the resolver — a
    split expression reimplemented elsewhere drifts into ids no object answers to."""
    if "+" in dim:
        d1, d2 = dim.split("+", 1)
        return f"\"{d1}\" || '~' || \"{d2}\""
    return f'"{dim}"'


def unit_id(stem: str, part: str | None = None) -> str:
    return f"{SOURCE}:{stem}" + (f"#{part}" if part else "")


def csv_key(prefix: str, sid: str) -> str:
    return f"{prefix}/{urllib.parse.quote(sid, safe='')}.csv"


def _rows_csv(rows) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for sid, d, v in rows:
        w.writerow([sid, d, v])
    return buf.getvalue().encode("utf-8")


def merged_refused(summary_path: str, key: str, examined, refused, full_run: bool):
    """(list, scope) for the summary's `refused` field, merging a scoped run into the record.

    A `--only` run knows the truth for the stems it examined and NOTHING about the rest, so
    replacing the whole list throws away a previous full run's verdicts -- which is what the
    cataloguers print as their own remediation command, so the guard destroys its own evidence
    every time an operator follows it (R843 addendum).

    Returns `scope` = "full" when the merged list is still a statement about the whole store:
    either this run was full, or a previous full-scoped record existed and we updated only the
    stems this run looked at. Otherwise "partial" -- and a reader must then treat the list as
    UNKNOWN rather than as empty.

    Deliberately NOT keyed off the summary's `scope` field, because that one describes the CAP's
    provenance and the two have opposite risk asymmetries: an unknown cap must never be adopted
    (R833), while a merged refusal list from a previous full run is sound.

    Fails SAFE, never silently: if the old summary cannot be read or carries no scope, the merge
    is skipped and the scope returned is "partial", so a reader is told it does not know.
    """
    mine = [{key: st, "rows": nr} for st, nr in refused]
    if full_run:
        return mine, "full"
    try:
        old = json.load(open(summary_path, encoding="utf-8"))
        prev = old.get("refused")
        prev_scope = old.get("refused_scope") or ("full" if old.get("scope") == "full" else None)
        if not isinstance(prev, list) or prev_scope != "full":
            return mine, "partial"
        kept = [r for r in prev if isinstance(r, dict) and r.get(key) not in set(examined)]
        return kept + mine, "full"
    except (OSError, ValueError, TypeError, AttributeError):
        return mine, "partial"

def run_scope(a) -> str:
    """Was this run evidence about the WHOLE store, or only part of it?

    THE SAME FUNCTION AS `derive_statcan_tables.run_scope`, deliberately duplicated.

    NOT because an import would fail -- a review checked and it would not: `from core import
    r2_util` is at MODULE level in all four derives, and every cataloguer already imports its
    derive at module level after `sys.path.insert(0, .../tools)`. The first version of this
    docstring claimed otherwise and was wrong. The real reason is coupling: importing this from
    `derive_statcan_tables` would make three unrelated sources fail to start if statcan's module
    did, and a shared `tools/_derive_scope.py` buys one definition at the price of a fifth file
    in every deployment path. `tests/test_derive_scope_all.py` holds all four to ONE behaviour
    table, which is the mechanism that makes duplication safe -- prose is not (CLAUDE.md).

    The summary is written unconditionally - by dry runs and by scoped runs too - and three
    cataloguers read it: for the `refused` list, and for statcan the `max_rows` cap it adopts as
    fact. A `--only` run over 11 of 2,442 flows must not read as a statement about the store
    (R843 addendum, and R832/R833 for what a wrong cap costs).

    `--dry-run` is checked FIRST: a dry run is not evidence whatever else was passed. `--limit`
    defaults to 0 and `--only` to "" - both falsy - so an unused flag reads as `full`, and an
    EXPLICIT `--limit 0` or `--only ""` also reads as full, which is correct: neither restricts
    anything. `getattr` throughout, because not every derive has every flag.

    Named rather than inlined in the summary dict so it can be tested without running a derive
    over a store (R840 - a branch nothing calls is a branch nothing tests).
    """
    if getattr(a, "dry_run", False):
        return "dry_run"
    if getattr(a, "only", None):
        return "only"
    if getattr(a, "limit", None):
        return "limit"
    return "full"

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--memory-limit", default="8GB")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT)
    ap.add_argument("--only", default="",
                    help="comma-separated stems; the split map is MERGED, not replaced, so a "
                         "targeted re-run cannot orphan the other indicators' parts")
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    n_store = len(files)          # BEFORE --only filters it. NOTE --limit narrows nothing here: it breaks
                                 # the LOOP, which is why `processed` exists below
    n_done = 0                    # what the loop ACTUALLY processed; --limit breaks it
    scan_failed = []              # a terminal disposition that had no key (R219)
    _examined_stems = []          # and WHICH ones - the merge drops only these
    if a.only:
        want = {s.strip() for s in a.only.split(",") if s.strip()}
        files = [f for f in files if os.path.splitext(os.path.basename(f))[0] in want]
        got = {os.path.splitext(os.path.basename(f))[0] for f in files}
        if got != want:
            raise SystemExit(f"--only names {len(want)}; {len(got)} exist under {STORE}. "
                             f"Missing: {', '.join(sorted(want - got))}")
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    print(f"{len(files)} indicator file(s); splitting any over {a.max_rows:,} rows", flush=True)

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)

    existing = set()
    s3 = None
    if not a.dry_run:
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            pref = f"{a.prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = s3.list_objects_v2(**kw)
                existing.update(o["Key"] for o in r.get("Contents", []))
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    q: queue.Queue = queue.Queue(maxsize=1000)
    counts = {"put": 0, "skip": 0, "err": 0}
    lock = threading.Lock()
    STOP = object()

    def worker():
        while True:
            item = q.get()
            if item is STOP:
                q.task_done()
                return
            key, body = item
            try:
                # GZIP AT REST (Ahmed, 2026-08-18), through the SHARED helper rather
                # than a fourth private copy: it also carries the magic-byte check
                # that R560 was written about - 188 objects shipped double-gzipped
                # and served as text/csv because one uploader lacked it. This tool
                # was still writing plain CSV while statcan gzipped and 284 of 502
                # objects in this very prefix already carried ContentEncoding: gzip.
                _body, _kw, _digest = r2_util.series_csv_put_args(body)
                s3.put_object(Bucket=a.bucket, Key=key, Body=_body, **_kw)
                with lock:
                    counts["put"] += 1
                    if counts["put"] % 500 == 0:
                        print(f"  put {counts['put']:,}", flush=True)
            except Exception as e:                              # noqa: BLE001
                with lock:
                    counts["err"] += 1
                    if counts["err"] <= 5:
                        print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)
            finally:
                q.task_done()

    threads = []
    if not a.dry_run:
        for _ in range(a.workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

    t0 = time.time()
    n_units = 0
    dropped_total = 0
    refused = []
    split_map: dict = {}
    for i, f in enumerate(files, 1):
        stem = os.path.splitext(os.path.basename(f))[0]
        # RECORDED HERE, AT THE TOP, because "examined" means "this run looked at it and
        # formed a verdict" - which includes REFUSED and SCAN FAILED. Recording it at the
        # BOTTOM put it after two `continue`s, so a refused stem was in `refused` and not
        # in `examined`; the merge then KEPT the previous entry for that stem and added
        # the new one, duplicating it and double-counting `refused_rows`.
        _examined_stems.append(stem)
        con = duckdb.connect()
        con.execute(f"SET memory_limit='{a.memory_limit}'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        n_rows = pq.ParquetFile(f).metadata.num_rows
        dim, n_parts = choose_split(con, f, n_rows, a.max_rows)
        if dim:
            split_map[stem] = {"dim": dim, "parts": n_parts, "rows": n_rows}
        if dim == "":
            refused.append((stem, n_rows))
            print(f"  [{i}/{len(files)}] {stem}: REFUSED — {n_rows:,} rows and no column pair "
                  f"divides it below {a.max_rows:,}; NOT emitted", flush=True)
            con.close()
            continue
        # `value` LAST in the ORDER BY so a collapsed duplicate is the MAXIMUM, deterministically.
        if dim:
            sel = f"{part_expr(dim)} AS part, series_key, obs_date, value"
            order = "part, series_key, obs_date, value"
        else:
            sel = "'' AS part, series_key, obs_date, value"
            order = "series_key, obs_date, value"
        try:
            cur = con.execute(f"""
                SELECT {sel} FROM read_parquet('{f}')
                WHERE value IS NOT NULL AND obs_date IS NOT NULL
                ORDER BY {order}""")
        except Exception as e:                                  # noqa: BLE001
            # EVERY TERMINAL DISPOSITION GETS A KEY (R219). This branch printed and moved
            # on, so the summary read `refused: [], errors: 0` while a file was dropped.
            scan_failed.append(stem)
            print(f"  [{i}/{len(files)}] {stem}: SCAN FAILED {type(e).__name__} "
                  f"{str(e)[:70]}", flush=True)
            con.close()
            continue

        cur_part, rows, last, dropped = None, [], None, 0

        def flush(part):
            nonlocal n_units
            if not rows:
                return
            sid = unit_id(stem, part or None)
            n_units += 1
            if a.dry_run:
                if n_units <= 3:
                    print(f"  would PUT {csv_key(a.prefix, sid)} ({len(rows):,} rows)")
                return
            key = csv_key(a.prefix, sid)
            if key in existing:
                with lock:
                    counts["skip"] += 1
                return
            q.put((key, _rows_csv(rows)))

        while True:
            batch = cur.fetchmany(200_000)
            if not batch:
                break
            for part, k, d, v in batch:
                if part != cur_part:
                    flush(cur_part)
                    cur_part, rows, last = part, [], None
                if (k, d) == last:
                    dropped += 1
                    rows[-1] = (k, d.isoformat(), v)
                    continue
                last = (k, d)
                rows.append((k, d.isoformat(), v))
        flush(cur_part)
        con.close()
        dropped_total += dropped
        if i % 100 == 0 or i == len(files) or dim:
            print(f"  [{i}/{len(files)}] {stem}{' split by ' + dim if dim else ''}: "
                  f"{n_units:,} units so far, {dropped_total:,} dup rows collapsed, "
                  f"{time.time()-t0:,.0f}s", flush=True)
        # WHAT THIS RUN ACTUALLY LOOKED AT. `--limit` breaks this loop; it does NOT filter
        # `files`, so `considered` (len(files)) would report the whole store for a run that
        # stopped after five. Against `store_files` that renders as 100% coverage of a 0.2%
        # run -- a summary contradicting its own `scope` tag, with the wrong half readable.
        n_done = i
        if a.limit and i >= a.limit:
            break

    if not a.dry_run:
        for _ in threads:
            q.put(STOP)
        q.join()

    dt = time.time() - t0
    print(f"\nunits: {n_units:,}   put {counts['put']:,}   skipped {counts['skip']:,}   "
          f"errors {counts['err']:,}   {dt:,.0f}s")
    print(f"duplicate (series_key, obs_date) rows collapsed: {dropped_total:,}")
    if refused:
        print(f"REFUSED (too large, no usable splitter) — {len(refused)}:")
        for st, nr in refused:
            print(f"   {st:48s} {nr:>12,} rows")

    smap = os.path.join(STORE, "_split_map.json")
    if a.dry_run:
        # A DRY RUN MUST NOT TOUCH THE STORE. Writing the map here would replace a complete map
        # with one describing whatever subset the dry run happened to look at — a rehearsal that
        # breaks the real thing.
        print(f"(dry run: split map NOT written; this run chose {len(split_map)} split(s))")
    else:
        out_map = split_map
        if a.only:
            try:
                out_map = json.load(open(smap, encoding="utf-8"))
            except (OSError, ValueError) as _e:
                # FAIL CLOSED ON A SCOPED RUN (R503). Starting from {} here writes a
                # map holding ONLY this run's stems, and every other split flow then
                # has no entry - the resolver raises "never split" for every part id
                # already catalogued. A transient read error must stop the run, not
                # silently narrow the map to what one --only run happened to touch.
                raise SystemExit(
                    f"REFUSING: --only was given but the existing split map at {smap} "
                    f"could not be read ({type(_e).__name__}: {_e}). Merging is "
                    f"impossible, and writing a fresh map would orphan every other "
                    f"split flow. Restore the map (a .20260907.bak sits beside it) "
                    f"and re-run.") from _e
            for st in {os.path.splitext(os.path.basename(x))[0] for x in files}:
                out_map.pop(st, None)
            out_map.update(split_map)
        with open(smap, "w", encoding="utf-8") as fh:
            json.dump(out_map, fh, indent=1, sort_keys=True)
        print(f"split map ({len(out_map):,} indicator(s)) -> {smap}")

    # EVERY TERMINAL DISPOSITION GETS A KEY (R219). A summary carrying only `errors` and
    # `skipped` reads as complete while refused units are on the floor; `considered` lets a
    # consumer check that the buckets add up.
    # MERGE, DO NOT REPLACE, when this run looked at only part of the store. The
    # cataloguers print `--only <ids>` as their own fix; without this, following that
    # instruction erases every other stem's verdict (R843 addendum).
    # WHAT THE RUN PROCESSED, not what it globbed. `--only` filters `files`, but
    # `--limit` does NOT - it breaks the loop - so using `files` here would drop
    # every stem in the store from the previous record while having examined five.
    _examined = _examined_stems
    _ref_list, _ref_scope = merged_refused(
        os.path.join(ROOT, "logs", "ilostat_indicators_summary.json"), "indicator", _examined, refused,
        run_scope(a) == "full")
    summary = os.path.join(ROOT, "logs", "ilostat_indicators_summary.json")
    json.dump({"considered": len(files), "units": n_units, "put": counts["put"],
               "skipped": counts["skip"], "errors": counts["err"],
               "refused": _ref_list,
               "refused_scope": _ref_scope,
               # SUMS THE MERGED LIST, not just this run's - `refused` above is merged,
               # and two keys describing one set must not disagree (R219).
               "refused_rows": sum(r.get("rows", 0) for r in _ref_list),
               "duplicates_collapsed": dropped_total, "seconds": round(dt),
               # THE SCOPE OF THE RUN, RECORDED WHERE ITS READER CAN SEE IT (R843
               # addendum). A cataloguer that reads `refused` from a `--only` summary
               # reads "nothing was refused" when the truth is "this run looked at part
               # of the store". `store_files` is the denominator that makes that
               # visible without any tag at all.
               "scope": run_scope(a),
               "dry_run": bool(a.dry_run),
               "store_files": n_store,
               # THE LENGTH OF WHAT WAS EXAMINED, not the loop index:
               # trailing refusals never reached the index assignment.
               "processed": len(_examined_stems),
               "scan_failed": scan_failed,
               "max_rows": int(a.max_rows)},
              open(summary, "w"), indent=1)
    print(f"summary -> {summary}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
