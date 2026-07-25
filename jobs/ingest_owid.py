#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of the ENTIRE Our World in Data grapher catalog.

Source: Our World in Data (CC BY 4.0).  license_id = cc-by-4.0.

ENUMERATION
-----------
OWID has no JSON "list every chart" endpoint, but it publishes a sitemap that
lists every public page including every grapher chart:
    https://ourworldindata.org/sitemap.xml
We extract every `https://ourworldindata.org/grapher/<slug>` URL from it -> the
full set of chart slugs (4,510 as of 2026-06). This is OWID's own authoritative
manifest of public charts.

PULL
----
For each slug the full long-format data is one CSV:
    https://ourworldindata.org/grapher/<slug>.csv?csvType=full&useColumnShortNames=true
CSV schema is tidy:  entity, code, <time>, <one-or-more value columns>
  * <time> is `year` (annual, the vast majority) OR `date` (YYYY-MM-DD daily).
  * helper columns we never treat as data: owid_region, owid_*  (region tags).
  * `code` is ISO-alpha3 for countries, an OWID code for aggregates (OWID_WRL,
    OWID_EU27, ...). Some rows have an empty code (e.g. "World" sub-aggregates);
    we then fall back to the `entity` label for the key.

LICENSE / CARVE-OUT (sources.yaml owid carve_out)
-------------------------------------------------
"Host OWID's own work. For upstream third-party series, exclude where the
license is not reservable." OWID enforces this at the source: a chart built on
non-redistributable third-party data returns HTTP 403 with body
  {"status":403,"error":"This chart contains non-redistributable data ..."}.
We DETECT that, EXCLUDE the chart, and count it under `non_redistributable`.
Everything OWID lets us download is CC BY 4.0 and reservable.

GROUPED STORAGE (anti-bloat)
----------------------------
ONE Parquet per chart slug ->  data/clean_full/owid/<slug>.parquet  with columns
    series_key (string), obs_date (date32), value (float64)
where series_key = "<slug>|<value_col>|<code-or-entity>".  All entities and all
value columns of a chart live inside that ONE file. Max ~4,510 files for the
whole source (one per chart) -- NOT one-file-per-series.

A per-source JSON summary (data/clean_full/owid/_ingest_summary.json) records
coverage. catalog.db is NOT touched here.

Usage:
  python jobs/ingest_owid.py --enumerate        # rebuild slug list from sitemap, print count
  python jobs/ingest_owid.py --dry 20           # download+parse 20 charts, no writes
  python jobs/ingest_owid.py                     # FULL run (all slugs), resumable
  python jobs/ingest_owid.py --workers 6         # tune concurrency (default 6)
  python jobs/ingest_owid.py --limit 200         # only first N slugs (testing)
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

SOURCE_ID = "owid"
LICENSE_ID = "cc-by-4.0"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
SITEMAP = "https://ourworldindata.org/sitemap.xml"
BASE = "https://ourworldindata.org/grapher"

RAW = os.path.join(ROOT, "data", "raw", "owid")
OUT = os.path.join(ROOT, "data", "clean_full", "owid")
SLUGS_FILE = os.path.join(RAW, "owid_slugs.txt")
SITEMAP_FILE = os.path.join(RAW, "owid_sitemap.xml")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

# columns that are never the chart's subject data
_NON_DATA = {"entity", "code", "year", "date", "day"}

SCHEMA = pa.schema([
    ("series_key", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
])

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def atomic_write_parquet(tbl, out_path, *, retries=8):
    """Write Parquet to a unique temp file then rename into place.

    On Windows an AV scanner / search-indexer can briefly hold a just-closed
    file, making os.replace raise PermissionError(WinError 32). Retry the rename
    with short backoff so one transient lock never aborts the whole crawl.
    The temp name includes the thread id so concurrent writers never collide.
    """
    tmp = f"{out_path}.{os.getpid()}.{threading.get_ident()}.part"
    pq.write_table(tbl, tmp, compression="zstd")
    last = None
    for attempt in range(retries):
        try:
            os.replace(tmp, out_path)
            return
        except PermissionError as e:  # WinError 32 (file in use)
            last = e
            time.sleep(0.25 * (attempt + 1))
    # last resort: copy bytes then drop temp
    try:
        with open(tmp, "rb") as fsrc, open(out_path, "wb") as fdst:
            fdst.write(fsrc.read())
        os.remove(tmp)
        return
    except Exception:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        raise last


# ---------------------------------------------------------------------------
# enumeration
# ---------------------------------------------------------------------------
def http_get(url, *, max_tries=5, timeout=180, expect_json=False):
    """GET with polite UA + exponential backoff. Returns requests.Response.

    Treats 403/404/410 as TERMINAL (not retryable) and returns the response so
    the caller can classify it (OWID uses 403 for non-redistributable charts).
    """
    last = None
    for attempt in range(max_tries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code in (403, 404, 410):
                return r  # terminal, let caller classify
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 2 * (2 ** attempt))
                time.sleep(wait)
                continue
            return r
        except requests.RequestException as e:
            last = str(e)
            time.sleep(min(60, 2 * (2 ** attempt)))
    raise RuntimeError(f"GET failed after {max_tries} tries: {url} ({last})")


def enumerate_slugs(force=False):
    """Extract every grapher slug from OWID's sitemap. Cache to disk."""
    if os.path.exists(SLUGS_FILE) and not force:
        slugs = [s for s in open(SLUGS_FILE, encoding="utf-8").read().split("\n") if s]
        return slugs
    r = http_get(SITEMAP)
    if r.status_code != 200:
        raise RuntimeError(f"sitemap fetch failed: HTTP {r.status_code}")
    open(SITEMAP_FILE, "w", encoding="utf-8").write(r.text)
    slugs = sorted(set(re.findall(r"/grapher/([^<\s?&#]+)", r.text)))
    open(SLUGS_FILE, "w", encoding="utf-8").write("\n".join(slugs))
    return slugs


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------
def parse_date(time_col, cell):
    """year -> Dec-31 of that year (annual convention, matches eurostat/faostat);
    date/day -> exact YYYY-MM-DD (OWID ships ISO date strings under both the
    `date` and `day` headers when csvType=full)."""
    s = (cell or "").strip()
    if not s:
        return None
    if time_col == "year":
        try:
            y = int(s)
        except ValueError:
            return None
        if 1000 <= y <= 3000:
            return dt.date(y, 12, 31)
        # some "year" charts use negative / BCE or huge values -> clamp out of range
        return None
    # date/day column -> ISO date
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def parse_chart_csv(text, slug):
    """Stream-parse one chart CSV into parallel column arrays.

    Returns (keys, dates, vals, n_value_cols, n_entities, time_col) or None if
    the CSV has no usable time/value columns.
    """
    rdr = csv.reader(io.StringIO(text))
    try:
        header = next(rdr)
    except StopIteration:
        return None
    idx = {h: i for i, h in enumerate(header)}
    # time dimension: prefer `year` (annual), then `date`/`day` (both carry ISO
    # date strings in the full CSV export). `day` is what OWID uses for many
    # high-frequency / AI-benchmark charts.
    if "year" in idx:
        time_col = "year"
    elif "date" in idx:
        time_col = "date"
    elif "day" in idx:
        time_col = "day"
    else:
        time_col = None
    if time_col is None or "entity" not in idx:
        return None
    ti = idx[time_col]
    ei = idx["entity"]
    ci = idx.get("code")
    # value columns = everything that isn't entity/code/time/day and isn't an
    # owid_* helper tag (owid_region etc.)
    value_cols = [h for h in header
                  if h not in _NON_DATA and not h.startswith("owid_")]
    if not value_cols:
        return None
    vidx = [(h, idx[h]) for h in value_cols]
    ncol = len(header)

    keys, dates, vals = [], [], []
    entities = set()
    for row in rdr:
        if len(row) < ncol:
            row = row + [""] * (ncol - len(row))
        od = parse_date(time_col, row[ti])
        if od is None:
            continue
        code = (row[ci].strip() if ci is not None else "") or row[ei].strip()
        if not code:
            continue
        entities.add(code)
        for h, j in vidx:
            cell = row[j].strip()
            if not cell:
                continue
            try:
                v = float(cell)
            except ValueError:
                continue  # non-numeric (text categories) -> skip
            keys.append(f"{slug}|{h}|{code}")
            dates.append(od)
            vals.append(v)
    return keys, dates, vals, len(value_cols), len(entities), time_col


# ---------------------------------------------------------------------------
# per-chart worker
# ---------------------------------------------------------------------------
def process_slug(slug, dry=False, skip_existing=True):
    out_path = os.path.join(OUT, slug + ".parquet")
    if skip_existing and not dry and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        # resume: read back its stats cheaply
        try:
            md = pq.read_metadata(out_path)
            return {"slug": slug, "status": "cached", "n_obs": md.num_rows}
        except Exception:
            pass  # corrupt -> re-fetch

    url = f"{BASE}/{slug}.csv?csvType=full&useColumnShortNames=true"
    try:
        r = http_get(url)
    except Exception as e:
        return {"slug": slug, "status": "error", "n_obs": 0, "detail": str(e)[:120]}

    if r.status_code == 403:
        # OWID's non-redistributable carve-out signal
        detail = ""
        try:
            detail = r.json().get("error", "")
        except Exception:
            detail = r.text[:120]
        return {"slug": slug, "status": "non_redistributable", "n_obs": 0, "detail": detail[:160]}
    if r.status_code in (404, 410):
        return {"slug": slug, "status": "missing", "n_obs": 0, "detail": f"HTTP {r.status_code}"}
    if r.status_code != 200:
        return {"slug": slug, "status": "error", "n_obs": 0, "detail": f"HTTP {r.status_code}"}

    parsed = parse_chart_csv(r.text, slug)
    if parsed is None:
        return {"slug": slug, "status": "no_data_cols", "n_obs": 0,
                "detail": "no usable time/value columns"}
    keys, dates, vals, n_vcols, n_ent, time_col = parsed
    if not keys:
        return {"slug": slug, "status": "empty", "n_obs": 0,
                "n_value_cols": n_vcols, "n_entities": n_ent, "time": time_col}

    n_series = len(set(keys))
    min_d, max_d = min(dates), max(dates)
    if not dry:
        tbl = pa.table({
            "series_key": pa.array(keys, type=pa.string()),
            "obs_date": pa.array(dates, type=pa.date32()),
            "value": pa.array(vals, type=pa.float64()),
        }, schema=SCHEMA)
        atomic_write_parquet(tbl, out_path)

    return {"slug": slug, "status": "ok", "n_obs": len(keys), "n_series": n_series,
            "n_value_cols": n_vcols, "n_entities": n_ent, "time": time_col,
            "start": str(min_d), "end": str(max_d)}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    args = sys.argv[1:]
    if "--enumerate" in args:
        slugs = enumerate_slugs(force=True)
        log(f"OWID grapher catalog: {len(slugs)} chart slugs (from sitemap)")
        return

    dry = "--dry" in args
    limit = None
    workers = 6
    if dry:
        try:
            limit = int(args[args.index("--dry") + 1])
        except (ValueError, IndexError):
            limit = 20
    if "--limit" in args:
        limit = int(args[args.index("--limit") + 1])
    if "--workers" in args:
        workers = max(1, min(6, int(args[args.index("--workers") + 1])))

    slugs = enumerate_slugs()
    total = len(slugs)
    log(f"OWID grapher catalog: {total} chart slugs published (sitemap manifest)")
    if limit:
        slugs = slugs[:limit] if not dry else slugs[:limit]
    log(f"{'DRY' if dry else 'FULL'} run: {len(slugs)} charts, workers={workers}")

    results = []
    t0 = time.time()
    done = 0
    grand_obs = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_slug, s, dry=dry): s for s in slugs}
        for fut in as_completed(futs):
            slug = futs[fut]
            try:
                res = fut.result()
            except Exception as e:  # one bad chart must never kill the crawl
                res = {"slug": slug, "status": "error", "n_obs": 0,
                       "detail": f"{type(e).__name__}: {e}"[:160]}
            results.append(res)
            grand_obs += res.get("n_obs", 0)
            done += 1
            if res["status"] not in ("ok", "cached"):
                log(f"  [{res['status']:18}] {res['slug'][:60]}  {res.get('detail','')[:80]}")
            if done % 200 == 0:
                el = time.time() - t0
                rate = done / el if el else 0
                log(f"  ...{done}/{len(slugs)} charts | {grand_obs:,} obs | "
                    f"{el/60:.1f} min | {rate:.1f} charts/s | "
                    f"eta {((len(slugs)-done)/rate/60) if rate else 0:.1f} min")

    # ---- summary ----
    by_status = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    ok = [r for r in results if r["status"] in ("ok", "cached")]
    n_obs = sum(r.get("n_obs", 0) for r in results)
    n_series = sum(r.get("n_series", 0) for r in results if "n_series" in r)
    files_written = sum(1 for r in results if r["status"] == "ok" and r.get("n_obs", 0) > 0)
    files_total = len([f for f in os.listdir(OUT) if f.endswith(".parquet")])

    summary = {
        "source_id": SOURCE_ID,
        "license_id": LICENSE_ID,
        "catalog_slugs_total": total,
        "charts_attempted": len(slugs),
        "status_breakdown": by_status,
        "charts_with_data": len(ok),
        "observations_written": n_obs,
        "series_written_approx": n_series,
        "parquet_files_written_this_run": files_written,
        "parquet_files_on_disk_total": files_total,
        "generated": dt.datetime.now().isoformat(timespec="seconds"),
        "results": sorted(results, key=lambda r: r["slug"]),
    }
    if not dry:
        with open(os.path.join(OUT, "_ingest_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

    el = time.time() - t0
    log(f"\n{'DRY' if dry else 'DONE'}: {len(slugs)} charts in {el/60:.1f} min")
    log(f"  status breakdown: {by_status}")
    log(f"  observations written: {n_obs:,}")
    log(f"  parquet files on disk: {files_total}")
    log(f"  charts with data: {len(ok)} / {len(slugs)} attempted "
        f"({len(ok)/len(slugs)*100:.1f}%)")


if __name__ == "__main__":
    main()
