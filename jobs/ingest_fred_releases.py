#!/usr/bin/env python3
"""Full FRED public-domain release crawl using the fred/v2/release/observations API.

Enumerates ALL FRED releases, filters to those with public-domain series only
(copyright_id = "public domain: citation requested"), and pulls every observation
using the cursor-paginated v2 bulk endpoint (500k obs per page).

This gets data not available from direct connectors: regional Fed data, specialty
finance series, some international, etc. Series flagged as Copyright (e.g.
Case-Shiller) are NEVER stored -- the copyright_id field enforces this per-series.

License: US public domain for qualifying series (copyright_id checked per-series).
Auth: Authorization: Bearer <FRED_API_KEY> header (v2 endpoint format).

Run: python jobs/ingest_fred_releases.py [--list] [--release_id 52]
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
from core.config import require  # noqa: E402

OUT = os.path.join(ROOT, "data", "clean_full", "fred")
MANIFEST = os.path.join(OUT, "_manifest.json")
BASE = "https://api.stlouisfed.org/fred"
V2_BASE = "https://api.stlouisfed.org/fred/v2"

# Only store series with these copyright flags
PUBLIC_DOMAIN = {"public domain: citation requested", "public domain"}

# Known copyrighted series patterns to double-exclude
COPYRIGHT_BLOCKLIST = {"copyright", "proprietary", "s&p", "case-shiller",
                       "ice", "baml", "ml bond"}

SCHEMA = pa.schema([
    ("series_id",  pa.string()),
    ("title",      pa.string()),
    ("frequency",  pa.string()),
    ("units",      pa.string()),
    ("obs_date",   pa.date32()),
    ("value",      pa.float64()),
])


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def headers(key):
    return {"Authorization": f"Bearer {key}",
            "User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def get_releases(key):
    """List all FRED releases."""
    all_rels = []
    offset = 0
    while True:
        r = requests.get(f"{BASE}/releases",
                         params={"api_key": key, "file_type": "json",
                                 "limit": 1000, "offset": offset},
                         headers={"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"},
                         timeout=30)
        r.raise_for_status()
        d = r.json()
        rels = d.get("releases", [])
        all_rels.extend(rels)
        if len(all_rels) >= int(d.get("count", 0)):
            break
        offset += 1000
        time.sleep(0.2)
    return all_rels


def is_blocked(title_lower, notes_lower):
    return any(b in title_lower or b in notes_lower
               for b in COPYRIGHT_BLOCKLIST)


def pull_release(key, release_id, release_name):
    """Pull all public-domain obs for one release via v2 cursor pagination.
    Returns total obs written."""
    out_path = os.path.join(OUT, f"release_{release_id:05d}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  rel {release_id} skip (exists, {n:,} rows)")
        return n, True

    writer = None
    total = 0
    skipped_copyright = 0
    cursor = None
    page = 0

    while True:
        params = {"release_id": release_id, "format": "json", "limit": 500000}
        if cursor:
            params["next_cursor"] = cursor
        try:
            r = requests.get(f"{V2_BASE}/release/observations",
                             params=params, headers=headers(key), timeout=300)
            if r.status_code == 404:
                return 0, False
            r.raise_for_status()
            d = r.json()
        except Exception as e:
            log(f"  rel {release_id} page {page} ERR: {e}")
            break

        series_list = d.get("series", [])
        page += 1

        # Build batch arrays
        sids, titles, freqs, units_list, dates, vals = [], [], [], [], [], []

        for s in series_list:
            cid = (s.get("copyright_id") or "").lower()
            title = s.get("title", "")
            notes = s.get("notes", "")
            # Skip copyrighted series
            if cid not in PUBLIC_DOMAIN or is_blocked(title.lower(), notes.lower()):
                skipped_copyright += len(s.get("observations", []))
                continue
            sid = s["series_id"]
            freq = s.get("frequency", "")
            unit = s.get("units", "")
            for obs in s.get("observations", []):
                v = obs.get("value", "")
                if v in (".", "", None):
                    continue
                try:
                    fv = float(v)
                except ValueError:
                    continue
                try:
                    od = dt.date.fromisoformat(obs["date"])
                except (ValueError, KeyError):
                    continue
                sids.append(sid); titles.append(title); freqs.append(freq)
                units_list.append(unit); dates.append(od); vals.append(fv)

        if sids:
            batch = pa.record_batch([
                pa.array(sids, pa.string()),
                pa.array(titles, pa.string()),
                pa.array(freqs, pa.string()),
                pa.array(units_list, pa.string()),
                pa.array(dates, pa.date32()),
                pa.array(vals, pa.float64()),
            ], schema=SCHEMA)
            if writer is None:
                tmp = out_path + ".tmp"
                writer = pq.ParquetWriter(tmp, SCHEMA, compression="zstd")
            writer.write_batch(batch)
            total += len(sids)

        has_more = d.get("has_more", False)
        cursor = d.get("next_cursor")
        if not has_more or not cursor:
            break
        time.sleep(0.1)

    if writer:
        writer.close()
        os.replace(out_path + ".tmp", out_path)

    log(f"  rel {release_id} '{release_name[:40]}': {total:,} obs "
        f"({skipped_copyright:,} skipped copyright)")
    return total, total > 0


def main():
    os.makedirs(OUT, exist_ok=True)
    key = require("FRED_API_KEY")
    args = sys.argv[1:]

    if "--list" in args:
        releases = get_releases(key)
        log(f"FRED releases ({len(releases)}):")
        for rel in releases:
            print(f"  {rel['id']:5d} {rel['name'][:60]}")
        return

    if "--release_id" in args:
        rid = int(args[args.index("--release_id") + 1])
        releases = [{"id": rid, "name": f"Release {rid}"}]
    else:
        log("Enumerating all FRED releases...")
        releases = get_releases(key)
        log(f"Total: {len(releases)} releases")

    # Load manifest for resume
    done_ids = set()
    if os.path.exists(MANIFEST):
        try:
            done_ids = set(json.load(open(MANIFEST)).get("done_release_ids", []))
        except Exception:
            pass

    grand_total = 0
    done_list = list(done_ids)
    failed = []

    for rel in releases:
        rid = rel["id"]
        name = rel.get("name", "")
        if rid in done_ids:
            continue
        n, ok = pull_release(key, rid, name)
        grand_total += n
        if ok or n == 0:
            done_ids.add(rid)
            done_list.append(rid)
        else:
            failed.append(rid)
        # Save manifest after each release (resume-safe)
        with open(MANIFEST, "w") as f:
            json.dump({"done_release_ids": list(done_ids),
                       "failed": failed,
                       "total_obs": grand_total}, f)
        time.sleep(0.3)

    log(f"DONE: {grand_total:,} public-domain obs across {len(done_ids)} releases")
    if failed:
        log(f"Failed releases: {failed}")


if __name__ == "__main__":
    main()
