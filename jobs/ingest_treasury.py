#!/usr/bin/env python3
"""Full-coverage grouped ingest of U.S. Treasury FiscalData.

Enumerates the ENTIRE FiscalData catalog (every dataset/endpoint discovered from the
FiscalData dataset pages -> data/_treasury_catalog_final.json) and pulls each endpoint
in full via the REST API (JSON, page[size]=10000, paginated to the last page).

GROUPED storage: ONE Parquet per ENDPOINT (the "cube") under
  data/clean_full/treasury/<slug>__<endpoint_leaf>.parquet
Each file holds the endpoint's FULL relational table (all original columns), plus:
  series_key : the endpoint path (e.g. "v2/accounting/od/debt_to_penny") -- the cube id
  obs_date   : date32, the parsed primary date field (record_date / reporting_date / ...)
All other source columns are preserved verbatim as strings (Treasury returns decimal
strings; keeping them avoids lossy float() of multi-trillion-dollar amounts). This makes
the written row count equal the source-published total-count, so coverage is verifiable.

Anti-bloat: one file per endpoint (~130 files for the whole source), NOT one-per-series.

License: us-public-domain (FiscalData is U.S. Government public domain).
Polite UA, retry/backoff, single-stream paging per endpoint (concurrency<=6 across
endpoints), bounded memory (rows flushed to the ParquetWriter in row-group batches).

Usage:
  python jobs/ingest_treasury.py --dry            # list catalog + planned files, no writes
  python jobs/ingest_treasury.py                  # full run (all endpoints)
  python jobs/ingest_treasury.py --workers 4      # endpoint-level concurrency (default 4)
  python jobs/ingest_treasury.py --only debt_to_penny   # substring filter on endpoint
"""
import datetime as dt
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT = os.path.join(ROOT, "data", "clean_full", "treasury")
CATALOG = os.path.join(ROOT, "data", "_treasury_catalog_final.json")
MANIFEST = os.path.join(OUT, "_manifest.jsonl")
ERRLOG = os.path.join(OUT, "_errors.log")

API = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
PAGE_SIZE = 10000          # API hard cap
FLUSH_ROWS = 100_000       # row-group batch (bounds memory on the 3.4M-row endpoint)
DATE_CANDS = ("record_date", "reporting_date", "effective_date", "date",
              "auction_date", "issue_date", "index_date")

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def errlog(msg):
    with _print_lock, open(ERRLOG, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def parse_date(s):
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y/%m/%d", "%m/%d/%Y", "%Y"):
        try:
            d = dt.datetime.strptime(s, fmt).date()
            return d
        except (ValueError, TypeError):
            continue
    return None


def safe_leaf(endpoint):
    """Stable filename: <slug>__<last path segment>."""
    leaf = endpoint.rstrip("/").split("/")[-1]
    return leaf


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json",
                      # Force a fresh TCP connection per request: the API drops
                      # keep-alive sockets under load, surfacing as RemoteDisconnected.
                      "Connection": "close"})
    return s


MAX_ATTEMPTS = 15   # ride out sustained API-contention bursts (shared API under load)
import random  # noqa: E402


def request(sess, url, params):
    """GET with retries. On ANY failure we mint a brand-new session (new TCP
    connection), since the dropped-connection errors come from stale keep-alive
    sockets while the shared API is under heavy concurrent load. Backoff is short
    with jitter so we cycle quickly through transient drops instead of stalling."""
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, params=params, timeout=90)
            if r.status_code in (429, 500, 502, 503, 504):
                last = f"http{r.status_code}"
                raise requests.HTTPError(last)
            r.raise_for_status()
            return r.json(), sess
        except (requests.RequestException, ValueError) as e:
            last = str(e)[:80]
            try:
                sess.close()
            except Exception:
                pass
            sess = session()  # fresh connection for the next attempt
            time.sleep(min(1.5 * (attempt + 1), 20) + random.uniform(0, 1.5))
    raise RuntimeError(f"request failed after retries: {url} params={params} -- {last}")


def pick_date_field(cols, hinted):
    if hinted and hinted in cols:
        return hinted
    for c in DATE_CANDS:
        if c in cols:
            return c
    # any *_date column
    for c in cols:
        if c.endswith("_date"):
            return c
    return None


def ingest_endpoint(endpoint, meta, dry=False):
    """Pull ALL pages of one endpoint; write ONE grouped Parquet. Returns (endpoint, n_written, expected)."""
    slug = meta["slug"]
    expected = meta.get("total") or 0
    date_field = meta.get("date_field")
    fname = f"{slug}__{safe_leaf(endpoint)}.parquet"
    fpath = os.path.join(OUT, fname)
    done_marker = fpath + ".done"

    # Resume: skip if already complete
    if os.path.exists(done_marker) and os.path.exists(fpath):
        try:
            have = pq.read_metadata(fpath).num_rows
            log(f"  SKIP {endpoint:60} (done, {have:,} rows)")
            return endpoint, have, expected
        except Exception:
            pass

    if dry:
        log(f"  PLAN {endpoint:60} -> {fname}  expect~{expected:,}")
        return endpoint, 0, expected

    sess = session()
    params_base = {"page[size]": str(PAGE_SIZE)}

    # If the catalog has no date-field hint, peek at page 1 (unsorted) to detect it from
    # the actual columns -- so obs_date is populated and pagination can sort stably.
    if not date_field:
        try:
            peek, sess = request(sess, f"{API}/{endpoint}",
                                 dict(params_base, **{"page[number]": "1"}))
            prow = (peek.get("data") or [{}])[0]
            date_field = pick_date_field(list(prow.keys()), None)
        except Exception:
            date_field = None

    sort_key = date_field if date_field else None
    if sort_key:
        params_base["sort"] = sort_key

    # ----- within-endpoint checkpointing (survives mid-pagination failures) -----
    # Each flush writes a numbered part-file; a .ckpt JSON records the next page to
    # fetch and the parts written so far. On re-run we resume from .ckpt instead of
    # restarting at page 1 (critical for the multi-hundred-page giant endpoints).
    parts_dir = os.path.join(OUT, "_parts")
    os.makedirs(parts_dir, exist_ok=True)
    base = f"{slug}__{safe_leaf(endpoint)}"
    ckpt_path = os.path.join(parts_dir, base + ".ckpt")

    start_page = 1
    parts = []
    n_written = 0
    if os.path.exists(ckpt_path):
        try:
            ck = json.load(open(ckpt_path, encoding="utf-8"))
            parts = [p for p in ck.get("parts", []) if os.path.exists(p)]
            n_written = sum(pq.read_metadata(p).num_rows for p in parts)
            start_page = ck.get("next_page", 1)
            if parts:
                log(f"  RESUME {endpoint:56} from page {start_page} ({n_written:,} rows kept)")
        except Exception:
            start_page, parts, n_written = 1, [], 0

    # Column order: union of catalog cols + any keys seen; locked on first part write.
    catalog_cols = list(meta.get("cols") or [])
    cols_order = ([date_field] if (date_field and date_field in catalog_cols) else []) + \
                 [c for c in catalog_cols if c != date_field]
    # if resuming, lock cols_order from an existing part's schema for consistency
    if parts:
        try:
            existing = pq.read_schema(parts[0]).names
            cols_order = [c for c in existing if c not in ("series_key", "obs_date")]
        except Exception:
            pass
    if not cols_order:
        cols_order = None

    def build_table(rows):
        nonlocal cols_order
        if cols_order is None:
            keyset = set()
            for r in rows:
                keyset.update(r.keys())
            others = sorted(k for k in keyset if k != date_field)
            cols_order = ([date_field] if date_field else []) + others
        n = len(rows)
        arrays = {"series_key": pa.array([endpoint] * n, type=pa.string())}
        if date_field:
            arrays["obs_date"] = pa.array([parse_date(r.get(date_field)) for r in rows],
                                          type=pa.date32())
        else:
            arrays["obs_date"] = pa.array([None] * n, type=pa.date32())
        for c in cols_order:
            arrays[c] = pa.array([(None if r.get(c) is None else str(r.get(c))) for r in rows],
                                 type=pa.string())
        return pa.table({k: arrays[k] for k in (["series_key", "obs_date"] + cols_order)})

    def write_part(rows, page_after):
        """Write a part file for `rows`, then atomically update the checkpoint."""
        nonlocal n_written
        if rows:
            idx = len(parts)
            ppath = os.path.join(parts_dir, f"{base}.{idx:05d}.parquet")
            pq.write_table(build_table(rows), ppath, compression="zstd")
            parts.append(ppath)
            n_written += len(rows)
        ck = {"endpoint": endpoint, "next_page": page_after, "parts": parts}
        tmpck = ckpt_path + ".tmp"
        json.dump(ck, open(tmpck, "w", encoding="utf-8"))
        os.replace(tmpck, ckpt_path)

    buf = []
    page = start_page
    total_pages = None
    try:
        while True:
            params = dict(params_base, **{"page[number]": str(page)})
            payload, sess = request(sess, f"{API}/{endpoint}", params)
            rows = payload.get("data", []) or []
            if total_pages is None:
                m = payload.get("meta", {}) or {}
                total_pages = m.get("total-pages", 1) or 1
            buf.extend(rows)
            last = (page >= total_pages or not rows)
            if len(buf) >= FLUSH_ROWS or last:
                write_part(buf, page + 1)
                buf = []
            if last:
                break
            page += 1
            time.sleep(0.12)  # polite between pages

        # concatenate parts -> final file
        if parts:
            schema = pq.read_schema(parts[0])
            writer = pq.ParquetWriter(fpath + ".tmp", schema, compression="zstd")
            for p in parts:
                writer.write_table(pq.read_table(p))
            writer.close()
            os.replace(fpath + ".tmp", fpath)
        else:
            empty = pa.table({"series_key": pa.array([], type=pa.string()),
                              "obs_date": pa.array([], type=pa.date32())})
            pq.write_table(empty, fpath, compression="zstd")

        # finalize: done marker, manifest, cleanup parts + ckpt
        open(done_marker, "w").write(str(n_written))
        with _print_lock, open(MANIFEST, "a", encoding="utf-8") as f:
            f.write(json.dumps({"endpoint": endpoint, "slug": slug, "file": fname,
                                "rows_written": n_written, "expected": expected,
                                "date_field": date_field}) + "\n")
        for p in parts:
            try:
                os.remove(p)
            except OSError:
                pass
        try:
            os.remove(ckpt_path)
        except OSError:
            pass
        status = "OK " if (expected == 0 or n_written >= expected) else "LOW"
        log(f"  {status} {endpoint:60} wrote {n_written:>12,} / expect {expected:>12,}")
        return endpoint, n_written, expected
    except Exception as e:
        # parts + ckpt are preserved on disk -> next run resumes from next_page
        errlog(f"{endpoint}: page~{page}: {e}")
        log(f"  ERR {endpoint:60} page~{page} (will resume) {str(e)[:60]}")
        return endpoint, -1, expected


def main():
    dry = "--dry" in sys.argv
    workers = 4
    if "--workers" in sys.argv:
        workers = int(sys.argv[sys.argv.index("--workers") + 1])
    only = None
    if "--only" in sys.argv:
        only = sys.argv[sys.argv.index("--only") + 1]

    os.makedirs(OUT, exist_ok=True)
    catalog = json.load(open(CATALOG, encoding="utf-8"))
    items = sorted(catalog.items())
    if only:
        items = [(e, m) for e, m in items if only in e]

    total_expected = sum((m.get("total") or 0) for _, m in items)
    log(f"{'DRY-RUN' if dry else 'FULL'}: {len(items)} endpoints, "
        f"source-published total = {total_expected:,} rows, workers={workers}")

    results = []
    if dry:
        for e, m in items:
            results.append(ingest_endpoint(e, m, dry=True))
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(ingest_endpoint, e, m, False): e for e, m in items}
            for fut in as_completed(futs):
                results.append(fut.result())

    n_ok = sum(1 for _, w, _ in results if w >= 0)
    n_rows = sum(w for _, w, _ in results if w > 0)
    n_err = sum(1 for _, w, _ in results if w < 0)
    log(f"\n{'DRY' if dry else 'DONE'}: {n_ok}/{len(items)} endpoints ok, "
        f"{n_err} errors, {n_rows:,} rows written "
        f"(source-published {total_expected:,}, "
        f"{100.0 * n_rows / total_expected if total_expected else 0:.2f}%)")


if __name__ == "__main__":
    main()
