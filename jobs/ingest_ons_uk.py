#!/usr/bin/env python3
"""ONS UK (Office for National Statistics) full ingest.

License: Open Government Licence v3.0 (OGL)
Source: ONS API — https://api.beta.ons.gov.uk/v1/
No API key required.

Strategy:
  * List all datasets from /v1/datasets
  * For each dataset: fetch the latest version CSV or observations
  * One Parquet per dataset; fully resumable

Run: python jobs/ingest_ons_uk.py
     python jobs/ingest_ons_uk.py --only cpih01,lfst01
"""
from __future__ import annotations
import csv, datetime as dt, io, os, sys, time
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "ons_uk")
BASE = "https://api.beta.ons.gov.uk/v1"
# ONS bot policy (https://developer.ons.gov.uk/bots/) MANDATES this User-Agent shape:
#   botName/Version (organisation-name +http://organisation-site/)
# and explicitly forbids personal identifying information or personal emails in it — our
# previous UA embedded an email address and did not match the format at all.
# Their published limits: 120 req/10s (site+API), 200 req/min, and 15 req/10s for
# "high demand site assets" (the CSV downloads). Exceeding them returns 429 + Retry-After,
# and — the part that actually bit us — "If this is not respected our algorithms may impose
# a block to our services for up to 1 hour." That block is what made CI runs hang and die.
UA   = {"User-Agent": "EconDataLibrary/1.0 (Elkassabgi Data Library +https://econdatalibrary.com)",
        "Accept": "application/json"}
RATE = 0.7          # <= 15 req/10s, the tightest ONS tier


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def retry_after_seconds(resp, default: int = 10) -> int:
    """Seconds to wait per the server's Retry-After header.

    Handles BOTH forms the spec allows (RFC 9110 / MDN): <delay-seconds> and <http-date>.
    ONS publishes (developer.ons.gov.uk/bots) that ignoring this header can earn a block of
    "up to 1 hour" — which is exactly what kept killing CI runs. Clamped to [1, 120] so a
    hostile or bogus value can't park a job for an hour.
    """
    raw = (resp.headers.get("Retry-After") or "").strip()
    wait = None
    if raw.isdigit():
        wait = int(raw)
    elif raw:
        try:
            from email.utils import parsedate_to_datetime
            import datetime as _dt
            when = parsedate_to_datetime(raw)
            if when.tzinfo is None:
                when = when.replace(tzinfo=_dt.timezone.utc)
            wait = int((when - _dt.datetime.now(_dt.timezone.utc)).total_seconds())
        except Exception:
            wait = None
    if wait is None:
        wait = default
    return min(max(wait, 1), 120) + 1


def get_json(url: str, retries: int = 4) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=120)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                # Was a blind 60s sleep that ignored the header — 4 retries meant up to 4
                # MINUTES of silence per call and kept us inside ONS's cooldown, renewing
                # the block. The API host throttles too, not just the CSV host.
                w = retry_after_seconds(r)
                log(f"  API 429 — honouring Retry-After: sleeping {w}s (attempt {attempt+1})")
                time.sleep(w); continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_csv_bytes(url: str, retries: int = 4) -> bytes | None:
    """Download one dataset CSV, HONOURING the server's Retry-After on 429.

    download.ons.gov.uk sits behind Cloudflare and rate-limits at roughly 5 requests per
    burst, returning `429` WITH `Retry-After: 10` (verified 2026-07-25). The previous fixed
    5/10/15/20s backoff ignored that header, so we kept retrying inside the cooldown window
    and keeping ourselves throttled — and Cloudflare escalates repeat offenders, much harder
    for datacentre IPs (i.e. CI runners) than for a home connection. Same class of bug as
    pypa/pip#11006: a client that does not respect Retry-After on 429.
    """
    hdrs = {**UA, "Accept": "text/csv,application/csv,text/plain"}
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=hdrs, timeout=300, stream=True)
            if r.status_code == 200:
                return r.content
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                wait = retry_after_seconds(r, default=10)
                log(f"  CSV 429 — honouring Retry-After: sleeping {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue
            log(f"  CSV HTTP {r.status_code} attempt {attempt+1}")
        except Exception as e:
            log(f"  CSV ERR attempt {attempt+1}: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_ons_period(s: str) -> dt.date | None:
    """Parse ONS period codes: '2022', '2022 Q1', '2022 Jan', 'Dec 2022', etc."""
    s = (s or "").strip()
    if not s:
        return None
    try:
        # Pure year
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
        # YYYY Qn
        if len(s) == 7 and s[5] == "Q":
            q = int(s[6])
            return dt.date(int(s[:4]), (q-1)*3+1, 1)
        # 'YYYY Q1' with space
        parts = s.split()
        if len(parts) == 2:
            yr_str, second = parts
            if yr_str.isdigit() and second.startswith("Q"):
                q = int(second[1])
                return dt.date(int(yr_str), (q-1)*3+1, 1)
            months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
                      "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
            if yr_str.isdigit() and second.lower()[:3] in months:
                return dt.date(int(yr_str), months[second.lower()[:3]], 1)
            if second.isdigit() and yr_str.lower()[:3] in months:
                return dt.date(int(second), months[yr_str.lower()[:3]], 1)
        # ISO date
        if len(s) == 10 and s[4] == "-":
            return dt.date.fromisoformat(s[:10])
        # YYYYMM
        if len(s) == 7 and s[4] == "M":
            return dt.date(int(s[:4]), int(s[5:7]), 1)
    except Exception:
        pass
    return None


def get_all_datasets() -> list[dict]:
    """Get all datasets from the ONS catalog (paginated)."""
    results = []
    offset = 0
    limit  = 1000
    while True:
        url = f"{BASE}/datasets?offset={offset}&limit={limit}"
        data = get_json(url)
        if not data:
            break
        items = data.get("items", [])
        results.extend(items)
        total = int(data.get("total_count", data.get("count", 0)))
        offset += len(items)
        if not items or offset >= total:
            break
        time.sleep(RATE)
    return results


def parse_dataset_csv(dataset_id: str, content: bytes) -> tuple[list, list, list]:
    """Parse one ONS dataset CSV -> (series_keys, obs_dates, values).

    THE canonical series_key builder for this source: colon-joined `dim=value` pairs over every
    column that is not the time/value column and does not contain 'uri', falling back to the
    dataset_id when a row carries no dimensions. Extracted from ingest_dataset so the updater
    fetcher can import it and emit byte-identical keys (the duplication invariant) instead of
    re-deriving the logic. Returns three empty lists on any parse failure.
    """
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return [], [], []
        cols = [c.lower() for c in reader.fieldnames]

        # Find time and value columns
        time_col = next((reader.fieldnames[i] for i, c in enumerate(cols)
                         if c in ("time_period", "time", "period", "year", "date")), None)
        val_col = next((reader.fieldnames[i] for i, c in enumerate(cols)
                        if c in ("observation", "value", "obs_value", "v4_0")), None)
        if not time_col or not val_col:
            # ONS V4 format: v4_0, v4_1, v4_2, v4_3 etc.
            v4_col = next((reader.fieldnames[i] for i, c in enumerate(cols)
                           if c.startswith("v4_") and c[3:].isdigit()), None)
            if v4_col:
                val_col = v4_col
                time_col = reader.fieldnames[cols.index("time")] if "time" in cols else (
                    reader.fieldnames[cols.index("time_period")] if "time_period" in cols else None)

        if not time_col or not val_col:
            log(f"  {dataset_id}: cannot find time/value cols: {reader.fieldnames[:8]}")
            return [], [], []

        skip_cols = {time_col, val_col}
        dim_cols = [c for c in reader.fieldnames if c not in skip_cols and "uri" not in c.lower()]

        all_keys, all_dates, all_vals = [], [], []
        for row in reader:
            raw_v = row.get(val_col, "")
            if raw_v in ("", "nan", "*", "...", "z", "c", "n/a", None):
                continue
            try:
                v = float(str(raw_v).replace(",", ""))
            except ValueError:
                continue
            d = parse_ons_period(row.get(time_col, ""))
            if d is None:
                continue
            key_parts = [f"{c}={row.get(c,'')}" for c in dim_cols if row.get(c, "")]
            all_keys.append(":".join(key_parts) or dataset_id)
            all_dates.append(d)
            all_vals.append(v)
        return all_keys, all_dates, all_vals
    except Exception as e:  # noqa: BLE001
        log(f"  {dataset_id}: parse error: {e}")
        return [], [], []


def resolve_csv_url(dataset_id: str) -> str | None:
    """Resolve a dataset's latest-version CSV download URL (shared by ingest + updater)."""
    meta = get_json(f"{BASE}/datasets/{dataset_id}")
    if not meta:
        return None
    edition_url = (meta.get("links", {}).get("latest_version", {}).get("href", ""))
    if not edition_url:
        editions = get_json(f"{BASE}/datasets/{dataset_id}/editions")
        if not editions or not editions.get("items"):
            return None
        latest_ed = editions["items"][0].get("id", "")
        versions = get_json(f"{BASE}/datasets/{dataset_id}/editions/{latest_ed}/versions")
        if not versions or not versions.get("items"):
            return None
        version_num = versions["items"][0].get("version", 1)
        edition_url = f"{BASE}/datasets/{dataset_id}/editions/{latest_ed}/versions/{version_num}"
    ver_meta = get_json(edition_url)
    if not ver_meta:
        return None
    for download in ver_meta.get("downloads", {}).values():
        href = download.get("href", "")
        if href.endswith(".csv") or "csv" in href.lower():
            return href
    return edition_url.rstrip("/") + "/csv"


def ingest_dataset(dataset_id: str, title: str, out_dir: str) -> int:
    """Download latest version of a dataset as CSV. Returns obs count."""
    out_path = os.path.join(out_dir, f"{dataset_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip {dataset_id} ({n:,} rows)")
        return n

    csv_url = resolve_csv_url(dataset_id)
    if not csv_url:
        return 0

    content = get_csv_bytes(csv_url)
    if not content:
        return 0

    all_keys, all_dates, all_vals = parse_dataset_csv(dataset_id, content)
    if not all_vals:
        return 0

    tbl = pa.table({
        "series_key": pa.array(all_keys,  pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals,  pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {dataset_id}: DONE {n:,} obs  [{title[:50]}]")
    return n


def main():
    os.makedirs(OUT, exist_ok=True)
    only_ids: set[str] = set()
    for a in sys.argv[1:]:
        if a.startswith("--only"):
            only_ids = set(a.split("=", 1)[-1].split(",")) if "=" in a else set()
        elif not a.startswith("-"):
            only_ids.add(a)

    log("Fetching ONS UK dataset catalog...")
    datasets = get_all_datasets()
    log(f"Found {len(datasets)} datasets")

    if only_ids:
        datasets = [d for d in datasets if d.get("id") in only_ids]
        log(f"Filtered to {len(datasets)} datasets")

    total = 0
    for i, ds in enumerate(datasets, 1):
        did   = ds.get("id", "")
        title = ds.get("title", "") or ds.get("description", "")[:60]
        if not did:
            continue
        log(f"[{i}/{len(datasets)}] {did}: {title[:60]}")
        total += ingest_dataset(did, title, OUT)
        time.sleep(RATE)

    log(f"DONE: {total:,} total ONS UK observations")


if __name__ == "__main__":
    main()
