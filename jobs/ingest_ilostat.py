#!/usr/bin/env python3
"""Full-coverage grouped ingest of ILOSTAT (International Labour Organization).

ILOSTAT exposes every indicator through the rplumber bulk API:
  - TOC of ALL indicators:  /metadata/toc/indicator   (1,947 indicator-frequency datasets)
  - bulk data per indicator: /data/indicator?id=<ID>&format=.parquet

Each "id" is an indicator+frequency code (e.g. SDG_0111_SEX_AGE_RT_A, the trailing
_A/_Q/_M is the frequency). A single bulk pull returns every country / sex /
classification / source combination for that indicator -- the full long-format
table whose row count equals the TOC's n.records (best_source=yes).

We write ONE grouped Parquet per indicator-id to data/clean_full/ilostat/<id>.parquet:
  columns: series_key, ref_area, source, sex, classif1, classif2,
           obs_date (date32), time (orig period string), value, obs_status
series_key = ilostat:<indicator>:<ref_area>:<sex>:<classif1>:<classif2>:<source>
(empty dimension segments collapse, so e.g. ilostat:SDG_0552_NOC_RT:AGO::::BA:13951).

license: cc-by-4.0 (configs/sources.yaml -> ilostat).

A run manifest (data/raw/ilostat/_manifest.csv) records per-id status + rows so the
run is idempotent (re-running skips completed ids) and verifiable against the TOC.

Usage:
  python jobs/ingest_ilostat.py --toc          # (re)download the indicator TOC only
  python jobs/ingest_ilostat.py --dry 5        # parse 5 ids, print, no writes
  python jobs/ingest_ilostat.py                # full run (all 1,947 ids)
  python jobs/ingest_ilostat.py --workers 6    # set concurrency (default 6)
"""
import csv
import datetime as dt
import io
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

RAW = os.path.join(ROOT, "data", "raw", "ilostat")
OUT = os.path.join(ROOT, "data", "clean_full", "ilostat")
TOC_PATH = os.path.join(RAW, "toc_indicator_en.csv")
MANIFEST = os.path.join(RAW, "_manifest.csv")

BASE = "https://rplumber.ilo.org"
TOC_URL = f"{BASE}/metadata/toc/indicator?lang=en&format=.csv"
HEADERS = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
LICENSE_ID = "cc-by-4.0"

FREQ_MAP = {"A": "A", "Q": "Q", "M": "M"}


def http_get(url, timeout, retries=5):
    """GET with exponential backoff. Returns the response or raises."""
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
            # 4xx other than 429 won't fix on retry
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise RuntimeError(f"{last} for {url}")
        except requests.RequestException as e:  # noqa: PERF203
            last = str(e)
        sleep = min(60, 2 ** attempt) + (attempt * 0.3)
        time.sleep(sleep)
    raise RuntimeError(f"giving up after {retries} tries ({last}) for {url}")


def download_toc():
    os.makedirs(RAW, exist_ok=True)
    r = http_get(TOC_URL, timeout=300)
    with open(TOC_PATH, "wb") as f:
        f.write(r.content)
    print(f"TOC saved: {len(r.content):,} bytes -> {TOC_PATH}", flush=True)


def read_toc():
    """Return list of dataset dicts from the TOC CSV."""
    if not os.path.exists(TOC_PATH):
        download_toc()
    rows = []
    with open(TOC_PATH, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def parse_period(p):
    """ILOSTAT time string -> first-of-period date. '2025', '2025Q1', '2025M01'."""
    p = (p or "").strip()
    try:
        if "Q" in p:
            y, q = p.split("Q")
            return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if "M" in p:
            y, m = p.split("M")
            return dt.date(int(y), int(m), 1)
        if len(p) == 4 and p.isdigit():
            return dt.date(int(p), 1, 1)
    except (ValueError, IndexError):
        return None
    return None


def _seg(v):
    return "" if v is None else str(v)


def build_table(raw_tbl, indicator_code):
    """Convert the API's long-format parquet into our grouped schema.

    Returns (pa.Table, n_rows, min_date, max_date, n_series) or (None, 0, ...)
    if nothing usable. Fully vectorized via pyarrow.compute (no per-row Python
    loop), so even the 4.6M-row indicators convert in ~1s.
    """
    n = raw_tbl.num_rows
    if n == 0:
        return None, 0, None, None, 0
    cols = set(raw_tbl.column_names)

    def col(name):
        if name in cols:
            return raw_tbl.column(name).cast(pa.string())
        return pa.nulls(n, pa.string())

    ref = col("ref_area")
    src = col("source")
    sex = col("sex")
    c1 = col("classif1")
    c2 = col("classif2")
    times = col("time")
    status = col("obs_status")
    if "indicator" in cols:
        ind = raw_tbl.column("indicator").cast(pa.string())
        # fill nulls with the catalog indicator code
        ind = pc.fill_null(ind, indicator_code)
    else:
        ind = pa.array([indicator_code] * n, pa.string())

    if "obs_value" in cols:
        vals = raw_tbl.column("obs_value").cast(pa.float64())
    else:
        vals = pa.nulls(n, pa.float64())

    # ---- vectorized date parse ------------------------------------------
    # year = first 4 chars; period type from char at index 4 (Q / M / none)
    ystr = pc.utf8_slice_codeunits(times, 0, 4)
    year = pc.cast(ystr, pa.int32())
    # quarter/month suffix after the marker char
    suffix = pc.utf8_slice_codeunits(times, 5, 99)  # "" for annual, "1" for Q1, "01" for M01
    suffix = pc.if_else(pc.equal(suffix, pa.scalar("")), pa.scalar(None, pa.string()), suffix)
    suff_n = pc.cast(suffix, pa.int32())  # null when annual
    marker = pc.utf8_slice_codeunits(times, 4, 5)  # "" (annual) / "Q" / "M"
    is_q = pc.equal(marker, pa.scalar("Q"))
    is_m = pc.equal(marker, pa.scalar("M"))
    # month = annual->1 ; Q->(q-1)*3+1 ; M->m
    q_month = pc.add(pc.multiply(pc.subtract(suff_n, 1), 3), 1)
    month = pc.if_else(is_q, q_month, pc.if_else(is_m, suff_n, pa.scalar(1, pa.int32())))
    # build "YYYY-MM-01" strings then cast to date32, invalid -> null
    yfmt = pc.utf8_lpad(pc.cast(year, pa.string()), 4, "0")
    mfmt = pc.utf8_lpad(pc.cast(month, pa.string()), 2, "0")
    datestr = pc.binary_join_element_wise(yfmt, mfmt, "01", "-")
    obs_date = pc.strptime(datestr, format="%Y-%m-%d", unit="s", error_is_null=True)
    obs_date = pc.cast(obs_date, pa.date32())

    # ---- vectorized series_key ------------------------------------------
    # ilostat:<indicator>:<ref>:<sex>:<classif1>:<classif2>:<source>
    e = pa.scalar("")
    parts = [
        pa.array(["ilostat"] * n, pa.string()),
        pc.fill_null(ind, e),
        pc.fill_null(ref, e),
        pc.fill_null(sex, e),
        pc.fill_null(c1, e),
        pc.fill_null(c2, e),
        pc.fill_null(src, e),
    ]
    series_key = pc.binary_join_element_wise(*parts, ":")

    full = pa.table({
        "series_key": series_key,
        "ref_area": ref,
        "source": src,
        "sex": sex,
        "classif1": c1,
        "classif2": c2,
        "obs_date": obs_date,
        "time": times,
        "value": vals,
        "obs_status": status,
    })
    # drop rows whose period failed to parse
    mask = pc.is_valid(full.column("obs_date"))
    if not pc.all(mask).as_py():
        full = full.filter(mask)
    rows = full.num_rows
    if rows == 0:
        return None, 0, None, None, 0

    mn = pc.min(full.column("obs_date")).as_py()
    mx = pc.max(full.column("obs_date")).as_py()
    nser = pc.count_distinct(full.column("series_key")).as_py()
    return full, rows, mn, mx, nser


# ---- manifest (thread-safe) -------------------------------------------------
_mlock = threading.Lock()


def load_manifest():
    done = {}
    if os.path.exists(MANIFEST):
        with open(MANIFEST, encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                done[row["id"]] = row
    return done


def append_manifest(rec):
    new = not os.path.exists(MANIFEST)
    with _mlock:
        with open(MANIFEST, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["id", "status", "rows", "n_series",
                                              "expected", "start", "end", "bytes", "secs"])
            if new:
                w.writeheader()
            w.writerow(rec)


def process_one(ds, dry=False):
    """Download + write one indicator id. Returns (id, rows, n_series, status)."""
    iid = ds["id"]
    indicator = ds.get("indicator") or iid
    try:
        expected = int(float(ds.get("n.records") or 0))
    except (ValueError, TypeError):
        expected = 0
    url = f"{BASE}/data/indicator?id={iid}&format=.parquet"
    out_path = os.path.join(OUT, iid + ".parquet")

    t0 = time.time()
    r = http_get(url, timeout=900)
    nbytes = len(r.content)
    if r.content[:4] != b"PAR1":
        # Not a parquet payload -> treat as empty/error
        head = r.content[:200]
        raise RuntimeError(f"non-parquet response ({nbytes}B): {head!r}")

    raw_tbl = pq.read_table(io.BytesIO(r.content))
    tbl, rows, mn, mx, nser = build_table(raw_tbl, indicator)
    secs = time.time() - t0

    if dry:
        return iid, rows, nser, expected, "dry", mn, mx, nbytes, secs

    if tbl is None or rows == 0:
        append_manifest({"id": iid, "status": "empty", "rows": 0, "n_series": 0,
                         "expected": expected, "start": "", "end": "",
                         "bytes": nbytes, "secs": round(secs, 1)})
        return iid, 0, 0, expected, "empty", mn, mx, nbytes, secs

    tmp = out_path + ".tmp"
    pq.write_table(tbl, tmp, compression="zstd")
    os.replace(tmp, out_path)
    status = "full" if rows >= expected else "short"
    append_manifest({"id": iid, "status": status, "rows": rows, "n_series": nser,
                     "expected": expected, "start": str(mn), "end": str(mx),
                     "bytes": nbytes, "secs": round(secs, 1)})
    return iid, rows, nser, expected, status, mn, mx, nbytes, secs


def main():
    args = sys.argv[1:]
    if "--toc" in args:
        download_toc()
        return

    dry = "--dry" in args
    limit = int(args[args.index("--dry") + 1]) if dry else None
    workers = int(args[args.index("--workers") + 1]) if "--workers" in args else 6
    workers = max(1, min(workers, 6))

    datasets = read_toc()
    total_expected = sum(int(float(d.get("n.records") or 0)) for d in datasets)
    print(f"ILOSTAT catalog: {len(datasets):,} indicator-frequency datasets "
          f"({len({d['indicator'] for d in datasets}):,} base indicators), "
          f"published n.records total = {total_expected:,}", flush=True)

    if dry:
        datasets = datasets[:limit]
        print(f"DRY-RUN: processing {len(datasets)} ids (no writes)", flush=True)
    else:
        os.makedirs(OUT, exist_ok=True)
        done = load_manifest()
        todo = []
        for d in datasets:
            iid = d["id"]
            rec = done.get(iid)
            # Re-do only if not previously completed (full/empty) AND file present.
            if rec and rec["status"] in ("full", "empty") and (
                rec["status"] == "empty" or os.path.exists(os.path.join(OUT, iid + ".parquet"))
            ):
                continue
            todo.append(d)
        skipped = len(datasets) - len(todo)
        print(f"FULL run: {len(todo):,} to fetch, {skipped:,} already done "
              f"(workers={workers})", flush=True)
        datasets = todo

    n_ok = n_obs = n_series = n_err = n_empty = 0
    done_ct = 0
    t_start = time.time()
    errors = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_one, d, dry): d for d in datasets}
        for fut in as_completed(futs):
            d = futs[fut]
            done_ct += 1
            try:
                iid, rows, nser, expected, status, mn, mx, nbytes, secs = fut.result()
            except Exception as e:  # noqa: BLE001
                n_err += 1
                errors.append((d["id"], str(e)[:160]))
                if not dry:
                    append_manifest({"id": d["id"], "status": "error", "rows": 0,
                                     "n_series": 0, "expected": d.get("n.records", ""),
                                     "start": "", "end": "", "bytes": 0, "secs": 0})
                print(f"  [{done_ct}/{len(datasets)}] ERROR {d['id']}: {str(e)[:120]}", flush=True)
                continue
            if status == "empty":
                n_empty += 1
            else:
                n_ok += 1
                n_obs += rows
                n_series += nser
            if dry or done_ct % 50 == 0 or status in ("short",):
                rate = n_obs / max(1e-9, time.time() - t_start)
                flag = "" if rows >= expected else f"  (SHORT exp={expected:,})"
                print(f"  [{done_ct}/{len(datasets)}] {iid:30} rows={rows:>9,} "
                      f"series={nser:>7,} {mn}..{mx} {nbytes/1e6:.1f}MB {secs:.1f}s"
                      f"  | cum_obs={n_obs:,} ({rate:,.0f}/s){flag}", flush=True)

    dur = time.time() - t_start
    print("=" * 70, flush=True)
    print(f"{'DRY' if dry else 'DONE'}: wrote {n_ok:,} datasets / {n_obs:,} observations "
          f"/ {n_series:,} series; empty={n_empty}, errors={n_err}; {dur/60:.1f} min", flush=True)
    if errors:
        print(f"First errors ({len(errors)} total):", flush=True)
        for iid, msg in errors[:20]:
            print(f"   {iid}: {msg}", flush=True)


if __name__ == "__main__":
    main()
