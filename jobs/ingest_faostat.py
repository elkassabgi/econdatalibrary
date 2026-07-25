#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of FAOSTAT bulk domains.

Source: FAO FAOSTAT (CC BY 4.0). license_id = cc-by-4.0.

Strategy
--------
FAOSTAT publishes a machine manifest listing EVERY domain + its bulk zip URL:
    https://bulks-faostat.fao.org/production/datasets_E.json
We download ALL domain bulk zips (the "(Normalized).zip" long-format files),
stream the single normalized CSV inside each, and write ONE grouped Parquet per
domain to data/clean_full/faostat/<DatasetCode>.parquet with columns:
    series_key, obs_date (date32), value (float64), flag (string)

Normalized FAOSTAT schema varies per domain, so the parser is header-driven:
  * Geography / dimension columns (Area, Item, Element, Indicator, Reporter,
    Partner, Survey, Currency, Cost Category, Institution, Food Group, ...) are
    every column that is NOT one of the reserved roles below.
  * TIME comes from `Year` (single year, or a range like "2014-2016" -> end yr).
    Some domains add `Months` (calendar month -> that month/year; non-calendar
    qualifiers like "Annual value", "Meher season" stay in the series key).
  * VALUE from `Value`; FLAG from `Flag` (data-quality letter).
  * `Year Code`, `Months Code`, the `(M49)`/`(CPC)`/`(FAO)`/`(ISO3)` code twins,
    `Note` and `Flag` are excluded from the series key to keep it stable & lean.

series_key = pipe-joined dimension values (codes preferred for stability, plus
the human label so the key is self-describing), e.g.
    "QCL|Area=2|Item=221|Element=5312|Almonds, in shell|Area harvested"
The grouped Parquet keeps the FULL dimensionality (nothing collapsed/dropped).

Memory is bounded: each domain is streamed row-by-row from the zip and flushed
to Parquet in 1,000,000-row batches, so even the 52M-row Trade matrix never
holds more than one batch in RAM.

Usage:
  python jobs/ingest_faostat.py --list          # enumerate catalog, no download
  python jobs/ingest_faostat.py --dry 3         # download+parse 3 small domains
  python jobs/ingest_faostat.py                 # FULL run (all 68 domains)
  python jobs/ingest_faostat.py --only QCL,TM   # specific domains
  python jobs/ingest_faostat.py --skip TM       # all except listed
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import re
import sys
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

MANIFEST = "https://bulks-faostat.fao.org/production/datasets_E.json"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
LICENSE_ID = "cc-by-4.0"
SOURCE_ID = "faostat"

RAW = os.path.join(ROOT, "data", "raw", "faostat")
OUT = os.path.join(ROOT, "data", "clean_full", "faostat")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

BATCH = 500_000            # rows per Parquet row-group flush (bounded memory)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# Column names that are NOT series dimensions (reserved roles / redundant codes).
_RESERVED_EXACT = {
    "year", "year code", "months code", "value", "flag", "note", "notes",
}
# Suffix patterns of "code twin" columns we drop from the key (keep the label).
_CODE_TWIN_RE = re.compile(r"\((m49|cpc|fao|iso3|iso2|sdg|fbs|gaul|hs)\)\s*$", re.I)


def http_get(url, *, stream=False, headers=None, max_tries=5):
    """GET with descriptive UA + exponential backoff."""
    h = dict(UA)
    if headers:
        h.update(headers)
    last = None
    for attempt in range(max_tries):
        try:
            r = requests.get(url, headers=h, stream=stream, timeout=600)
            if r.status_code == 200:
                return r
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 3 * (2 ** attempt))
                print(f"    {last} -> backoff {wait}s ({url[:70]})", flush=True)
                time.sleep(wait)
                continue
            r.raise_for_status()
        except requests.RequestException as e:  # noqa: PERF203
            last = str(e)
            wait = min(60, 3 * (2 ** attempt))
            print(f"    error {last[:80]} -> backoff {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {max_tries} tries: {url} ({last})")


def load_manifest():
    r = http_get(MANIFEST)
    data = r.json()
    ds = data["Datasets"]["Dataset"]
    return ds


def parse_year(year_s):
    """FAOSTAT Year: '1961', '2014-2016', '20142016', or embedded in survey code."""
    s = (year_s or "").strip()
    if not s:
        return None
    # ranges "2014-2016" or "2014-16" -> take the ending year
    m = re.match(r"^(\d{4})\s*[-/]\s*(\d{2,4})$", s)
    if m:
        end = m.group(2)
        if len(end) == 2:
            end = m.group(1)[:2] + end
        try:
            return int(end)
        except ValueError:
            return None
    # plain 4-digit
    if s.isdigit() and len(s) == 4:
        return int(s)
    # 8-digit concatenated range "20142016"
    if s.isdigit() and len(s) == 8:
        try:
            return int(s[4:])
        except ValueError:
            return None
    # otherwise grab the last 4-digit run (e.g. survey "076_2014")
    runs = re.findall(r"\d{4}", s)
    if runs:
        try:
            return int(runs[-1])
        except ValueError:
            return None
    return None


def build_date(year, month_label):
    """Return a date32-compatible date, or None."""
    yr = parse_year(year)
    if yr is None or yr < 1000 or yr > 3000:
        return None
    if month_label:
        mo = MONTHS.get(month_label.strip().lower())
        if mo:
            return dt.date(yr, mo, 1)
    return dt.date(yr, 12, 31)  # annual convention (matches eurostat 4-digit rule)


def parse_value(v):
    s = (v or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def plan_columns(header):
    """Map header -> roles. Returns (role dict, dim_indices, dim_names).

    Time role priority:
      1. a column literally named "Year"
      2. any other column ending in "year" (e.g. "Census Year") -- its label form
      3. "Year Code"
      4. survey domains (no year col at all): extract the year from "Survey Code"
         (or "Survey") at parse time -> role['survey'].
    """
    lower = [h.strip().lower() for h in header]
    role = {}
    for i, h in enumerate(lower):
        if h == "year":
            role["year"] = i
        elif h.endswith(" year") and "year" not in role:   # "Census Year"
            role.setdefault("year_named", i)
        elif h == "year code" and "year_code" not in role:
            role.setdefault("year_code", i)
        if h == "months":
            role["months"] = i
        elif h == "value":
            role["value"] = i
        elif h == "flag":
            role["flag"] = i
    # resolve the time column
    if "year" not in role:
        if "year_named" in role:
            role["year"] = role["year_named"]
        elif "year_code" in role:
            role["year"] = role["year_code"]
    # survey-type domains (no year column) -> derive year from Survey Code/Survey
    if "year" not in role:
        for i, h in enumerate(lower):
            if h == "survey code":
                role["survey"] = i
                break
        if "survey" not in role:
            for i, h in enumerate(lower):
                if h == "survey":
                    role["survey"] = i
                    break

    dim_idx, dim_names = [], []
    for i, h in enumerate(lower):
        if h in _RESERVED_EXACT:
            continue
        # the resolved year column (if it is a NAMED year like "Census Year"
        # or a "Year Code") is consumed for the date and dropped from the key.
        if i == role.get("year"):
            continue
        if i == role.get("year_code") and i != role.get("year"):
            continue
        if i == role.get("months") and h == "months":
            # 'Months' label is consumed for date; non-calendar qualifiers are
            # re-appended to the key per-row in the main loop.
            continue
        if _CODE_TWIN_RE.search(h):
            continue  # drop redundant (M49)/(CPC)/... code twin from the key
        dim_idx.append(i)
        dim_names.append(header[i].strip())
    return role, dim_idx, dim_names


def stream_csv_rows(zf, member):
    """Yield raw CSV rows (list[str]) streaming from a zip member, low memory."""
    with zf.open(member, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
        rdr = csv.reader(text)
        for row in rdr:
            yield row


def open_zip_member(zip_path):
    """Open the zip FROM DISK (not in-memory) so giant archives stay off the heap."""
    zf = zipfile.ZipFile(zip_path)
    members = zf.namelist()
    norm = [m for m in members if m.lower().endswith("(normalized).csv")]
    if not norm:
        norm = [m for m in members if m.lower().endswith(".csv")
                and "elements" not in m.lower() and "flags" not in m.lower()
                and "items" not in m.lower() and "areacodes" not in m.lower()
                and "symbols" not in m.lower()]
    if not norm:
        norm = [m for m in members if m.lower().endswith(".csv")]
    return zf, norm[0]


def ingest_domain(d, dry=False):
    code = d["DatasetCode"]
    url = d["FileLocation"]
    name = d["DatasetName"]
    published_rows = int(d.get("FileRows") or 0)
    raw_zip = os.path.join(RAW, code + ".zip")

    # ---- download (stream straight to disk; never hold the archive in RAM) ----
    if os.path.exists(raw_zip) and os.path.getsize(raw_zip) > 0:
        cached = True
    else:
        r = http_get(url, stream=True)
        tmp = raw_zip + ".part"
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
        os.replace(tmp, raw_zip)
        cached = False

    zf, member = open_zip_member(raw_zip)   # opened from disk, streamed below

    # ---- parse header ----
    rows_iter = stream_csv_rows(zf, member)
    header = next(rows_iter)
    role, dim_idx, dim_names = plan_columns(header)
    if "value" not in role or ("year" not in role and "survey" not in role):
        print(f"  [{code}] WARN missing value/time role; header={header}", flush=True)
    vi = role.get("value")
    yi = role.get("year")
    si = role.get("survey")     # fallback time source for survey domains
    mi = role.get("months")
    fi = role.get("flag")

    # ---- stream rows -> Parquet batches ----
    out_path = os.path.join(OUT, code + ".parquet")
    writer = None
    schema = pa.schema([
        ("series_key", pa.string()),
        ("obs_date", pa.date32()),
        ("value", pa.float64()),
        ("flag", pa.string()),
    ])
    keys, dates, vals, flags = [], [], [], []
    n_obs = 0
    # Count distinct series with a set of 64-bit hashes, NOT full strings:
    # a giant domain (e.g. Trade matrix) can have tens of millions of distinct
    # keys; storing them as strings would cost multiple GB and risks an OOM.
    series_hashes = set()
    min_d = max_d = None
    ncol = len(header)

    def flush():
        nonlocal writer, keys, dates, vals, flags
        if not keys:
            return
        tbl = pa.table({
            "series_key": pa.array(keys, type=pa.string()),
            "obs_date": pa.array(dates, type=pa.date32()),
            "value": pa.array(vals, type=pa.float64()),
            "flag": pa.array(flags, type=pa.string()),
        }, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(out_path, schema, compression="zstd")
        writer.write_table(tbl)
        keys.clear(); dates.clear(); vals.clear(); flags.clear()

    for row in rows_iter:
        if len(row) < ncol:
            row = row + [""] * (ncol - len(row))
        val = parse_value(row[vi]) if vi is not None else None
        if val is None:
            continue
        month_lbl = row[mi] if mi is not None else ""
        if yi is not None:
            year_src = row[yi]
        elif si is not None:
            year_src = row[si]      # survey domains: parse year out of Survey Code
        else:
            year_src = ""
        od = build_date(year_src, month_lbl)
        if od is None:
            continue
        # series key from dimension columns (+ months qualifier if non-calendar)
        parts = []
        for j in dim_idx:
            cell = row[j].strip().lstrip("'")
            if cell:
                parts.append(cell)
        if mi is not None and month_lbl and month_lbl.strip().lower() not in MONTHS:
            parts.append(month_lbl.strip())  # e.g. "Annual value", "Meher season"
        sk = code + "|" + "|".join(parts)
        flag = row[fi].strip() if (fi is not None and fi < len(row)) else ""

        keys.append(sk); dates.append(od); vals.append(val); flags.append(flag)
        series_hashes.add(hash(sk) & 0xFFFFFFFFFFFFFFFF)  # bounded 8-byte footprint
        n_obs += 1
        if min_d is None or od < min_d:
            min_d = od
        if max_d is None or od > max_d:
            max_d = od
        if len(keys) >= BATCH:
            if dry:
                keys.clear(); dates.clear(); vals.clear(); flags.clear()
            else:
                flush()

    if dry:
        keys.clear(); dates.clear(); vals.clear(); flags.clear()
    else:
        flush()
        if writer is not None:
            writer.close()
        elif n_obs == 0:
            # write empty file so coverage is explicit
            pq.write_table(pa.table({c: [] for c in
                            ["series_key", "value"]} | {}, schema=schema), out_path)

    zf.close()
    n_series = len(series_hashes)
    parsed_pct = (n_obs / published_rows * 100) if published_rows else 0
    print(f"  [{code:6}] {('cache' if cached else 'dl   ')} "
          f"obs={n_obs:>11,} series={n_series:>9,} "
          f"pub_rows={published_rows:>11,} ({parsed_pct:5.1f}%) "
          f"{name[:42]}", flush=True)
    return {
        "code": code, "name": name, "n_obs": n_obs,
        "n_series": n_series, "published_rows": published_rows,
        "start": str(min_d) if min_d else None,
        "end": str(max_d) if max_d else None,
    }


def main():
    args = sys.argv[1:]
    list_only = "--list" in args
    dry = "--dry" in args
    limit = None
    only = skip = None
    if dry:
        try:
            limit = int(args[args.index("--dry") + 1])
        except (ValueError, IndexError):
            limit = 3
    if "--only" in args:
        only = set(args[args.index("--only") + 1].split(","))
    if "--skip" in args:
        skip = set(args[args.index("--skip") + 1].split(","))

    ds = load_manifest()
    total_rows = sum(int(d.get("FileRows") or 0) for d in ds)
    print(f"FAOSTAT manifest: {len(ds)} domains, "
          f"{total_rows:,} published rows total", flush=True)

    if list_only:
        for d in sorted(ds, key=lambda x: -(int(x.get("FileRows") or 0))):
            print(f"  {d['DatasetCode']:6} {int(d.get('FileRows') or 0):>11,} "
                  f"{d.get('FileSize',''):>9}  {d['DatasetName'][:55]}")
        return

    targets = ds
    if only:
        targets = [d for d in ds if d["DatasetCode"] in only]
    if skip:
        targets = [d for d in targets if d["DatasetCode"] not in skip]
    if dry:
        # pick the SMALLEST domains for a fast dry run
        targets = sorted(targets, key=lambda x: int(x.get("FileRows") or 0))[:limit]

    print(f"{'DRY' if dry else 'FULL'} run: {len(targets)} domains", flush=True)
    results = []
    grand_obs = grand_series = 0
    t0 = time.time()
    for i, d in enumerate(targets, 1):
        try:
            res = ingest_domain(d, dry=dry)
            results.append(res)
            grand_obs += res["n_obs"]
            grand_series += res["n_series"]
        except Exception as e:  # noqa: BLE001
            print(f"  [{d['DatasetCode']}] FAILED: {e}", flush=True)
            results.append({"code": d["DatasetCode"], "name": d["DatasetName"],
                            "n_obs": 0, "n_series": 0,
                            "published_rows": int(d.get("FileRows") or 0),
                            "error": str(e)})
        if i % 5 == 0:
            el = time.time() - t0
            print(f"  ...{i}/{len(targets)} domains, {grand_obs:,} obs, "
                  f"{el/60:.1f} min", flush=True)

    # ---- summary manifest (JSON sidecar, NOT the catalog.db) ----
    pub_total = sum(r["published_rows"] for r in results)
    summary = {
        "source_id": SOURCE_ID,
        "license_id": LICENSE_ID,
        "domains_total_in_manifest": len(ds),
        "domains_processed": len(results),
        "published_rows_processed": pub_total,
        "observations_written": grand_obs,
        "series_written": grand_series,
        "files_written": sum(1 for r in results if r["n_obs"] > 0),
        "per_domain": results,
    }
    if not dry:
        with open(os.path.join(OUT, "_ingest_summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    el = time.time() - t0
    print(f"\n{'DRY' if dry else 'DONE'}: {len(results)} domains | "
          f"{grand_obs:,} observations | {grand_series:,} series | "
          f"{el/60:.1f} min", flush=True)
    print(f"published rows across processed domains: {pub_total:,} | "
          f"parse ratio: {grand_obs/pub_total*100:.1f}%", flush=True)


if __name__ == "__main__":
    main()
