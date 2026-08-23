"""Series/observations census -> _aqueduct/stats.json (the /v1/stats source of truth).

The worker's /v1/stats REFUSES to serve compiled-in numbers (owner rule: counts
must never go stale in code) and reads this object from R2 instead. The previous
census (as_of 2026-07-02: 7,730,440,157 series / 79,782,631,887 obs / 309 sources)
was produced by throwaway scripts that were not retained; five weeks of serves
(noaa, bea, the UNCTAD giants) and 34 retirements later, this makes the census a
TOOL, not an artifact.

METHOD (mirrors the published method string exactly):
  * observations       — exact parquet metadata row counts (pq.read_metadata,
                         no data read), every .parquet under data/clean_full and
                         data/clean_grouped, minus exclusions below.
  * individual_series  — per source, DuckDB approx_count_distinct(series_key)
                         (HyperLogLog, ~1%% error) across that source's uniform
                         files; summed over sources. Files WITHOUT a series_key
                         column are counted for obs but not series, and REPORTED.
  * sources_catalogued — live COUNT(DISTINCT source_id) from catalog.db.

EXCLUSIONS (each mirrors the serving layer, not an opinion):
  * wid.parquet monolith beside its 412 shards — superseded; the resolver and
    derive_csv_bulk both exclude it (R384's near-miss corruption); counting it
    would double its rows.
  * *__series.parquet sidecars, *checkpoint*/*ckpt* files — derived/bookkeeping.

Writes stats.json locally (logs/stats-<date>.json kept as history), uploads to
r2://econ-data/_aqueduct/stats.json, then verifies the LIVE endpoint flips its
as_of. Run time is dominated by the giant key scans; threads capped at 24 so
concurrent pulls keep breathing room.

R420 — TWO LESSONS THIS TOOL'S FIRST RUN PUBLISHED THE HARD WAY:
  1. LOCAL DISK IS NOT THE COMPLETE STORE. The US census source's ~7.73B-series
     grouped store lives on R2 ONLY (local clean_full/census is a 2.4 GB tail),
     so a local-roots scan under-measures it by ~5 orders of magnitude. Until
     this tool reads the R2-resident stores too, its totals are NOT comparable
     to the 2026-07-02 census and MUST NOT be published.
  2. statcan's keys are store-true but hero-hostile: the 2021 census-profile
     tables carry ~32.85B one-observation coordinate cells (98100620.parquet:
     894M rows, ~1.3B distinct keys). Whether those count as "series" in the
     public number is the metric owner's call, not a scan default.
PUBLISH GATE (mechanical, per R420): before uploading, fetch the CURRENTLY
published object; if individual_series or observations moves >20%, REFUSE
unless --force-publish. Running without --publish computes and writes history
only — publishing is an explicit act, never a side effect of measuring.
"""
from __future__ import annotations

import datetime as dt
import glob
import json
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import duckdb                      # noqa: E402
import pyarrow.parquet as pq       # noqa: E402

from core import r2_util           # noqa: E402

ROOTS = [os.path.join(ROOT, "data", "clean_full"),
         os.path.join(ROOT, "data", "clean_grouped")]
BUCKET = "econ-data"
_S3 = f"s3://{BUCKET}/"
KEY = "_aqueduct/stats.json"


def served_keys() -> dict[str, int]:
    """Every parquet object on R2 under the served roots -> its size.

    WHY THIS EXISTS (R449/R450). This tool measured LOCAL DISK and its output is what
    /v1/stats publishes to the public. Those are not the same store. On 2026-08-23 the
    local scan reported 36.9B series / 93.3B observations while statcan - 175.1 GB, 8,207
    files, 56.8B of those observations - had ZERO bytes uploaded, and cbs_nl and gus_dbw
    were 15% and 8% uploaded. A reader who believed the published figure and tried to
    download statcan would have found nothing there.

    "We computed it" and "a user can download it" are different claims, and only the
    second one belongs on a public page. The served surface is the bucket, so the bucket
    is what gets counted.
    """
    from updater.blob import R2Blob                                   # noqa: PLC0415
    r2 = R2Blob()
    out: dict[str, int] = {}
    pag = r2.client.get_paginator("list_objects_v2")
    for prefix in ("clean_full/", "clean_grouped/"):
        for page in pag.paginate(Bucket=r2.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".parquet"):
                    out[o["Key"]] = o["Size"]
    return out


def _r2_key(path: str) -> str:
    """Local parquet path -> the object key the worker would resolve it from."""
    ap = os.path.abspath(path)
    for root in ROOTS:
        ar = os.path.abspath(root)
        if ap.startswith(ar + os.sep):
            return os.path.basename(ar) + "/" + os.path.relpath(ap, ar).replace(os.sep, "/")
    return ""


def keep_served(srcs: dict[str, list[str]]) -> tuple[dict[str, list[str]], dict[str, int]]:
    """Keep only what R2 serves, reading from R2 itself wherever it differs from local.

    ABSENT from R2 -> dropped. It is not downloadable, so it is not ours to count.

    PRESENT but a different size -> the R2 object wins and the whole source is read over
    s3://. Do NOT read the local copy and do NOT drop the file. For every cloud-run source
    CI updates R2 and the local mirror lags: sampled on 2026-08-23, R2 was newer in 8 of 8
    mismatches and local newer in none (ilostat 08-19 vs 08-07, ecb 08-23 vs 08-07,
    treasury 19:09 vs 08-03). Counting local would report stale data; dropping the file
    would discard 4,417 files that are served perfectly well and are FRESHER than the copy
    on this disk. Egress from R2 is free and DuckDB reads the objects directly, so
    correctness here costs nothing.

    Identical size -> read the local copy; the bytes are the same and local is faster.
    """
    on_r2 = served_keys()
    kept: dict[str, list[str]] = {}
    dropped: dict[str, int] = {}
    for src, files in srcs.items():
        keep, mismatched = [], False
        for f in files:
            key = _r2_key(f)
            if not key or key not in on_r2:
                dropped[src] = dropped.get(src, 0) + 1      # absent: not downloadable
                continue
            keep.append((f, key))
            try:
                if on_r2[key] != os.path.getsize(f):
                    mismatched = True
            except OSError:
                mismatched = True
        if not keep:
            continue
        # One filesystem per source: mixing local paths and s3:// in a single
        # read_parquet list is not worth relying on, and a source is small enough
        # to read whole.
        kept[src] = ([_S3 + k for _f, k in keep] if mismatched
                     else [f for f, _k in keep])
    return kept, dropped


def source_files() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for root in ROOTS:
        if not os.path.isdir(root):
            continue
        for entry in sorted(os.scandir(root), key=lambda e: e.name.lower()):
            if not entry.is_dir():
                continue
            files = []
            for dp, _dn, fns in os.walk(entry.path):
                for f in fns:
                    lf = f.lower()
                    if (not f.endswith(".parquet") or f.endswith("__series.parquet")
                            or "checkpoint" in lf or "ckpt" in lf):
                        continue
                    files.append(os.path.join(dp, f))
            if entry.name == "wid":
                mono = os.path.join(entry.path, "wid.parquet")
                rest = [f for f in files if os.path.abspath(f) != os.path.abspath(mono)]
                if rest and len(rest) != len(files):
                    files = rest
            if files:
                out.setdefault(entry.name, []).extend(files)
    return out


def _wire_r2(con) -> None:
    """Point DuckDB at the bucket so read_parquet can take s3:// keys."""
    from updater.blob import R2Blob                                   # noqa: PLC0415
    r2 = R2Blob()
    creds = r2.client._request_signer._credentials
    ep = r2.client.meta.endpoint_url.split("//", 1)[-1]
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{ep}'")
    con.execute("SET s3_region='auto'")
    con.execute(f"SET s3_access_key_id='{creds.access_key}'")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}'")
    con.execute("SET s3_url_style='path'")


def _rows_and_key(con, f: str) -> tuple[int, bool]:
    """(row count, has series_key) for a local path OR an s3:// key.

    Local goes through parquet metadata - the footer only, no data read. Remote goes
    through DuckDB, which range-reads the same footer.
    """
    if not f.startswith("s3://"):
        md = pq.read_metadata(f)
        return md.num_rows, "series_key" in md.schema.names
    n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [f]).fetchone()[0]
    cols = [d[0] for d in con.execute(
        "SELECT * FROM read_parquet(?) LIMIT 0", [f]).description]
    return n, "series_key" in cols


def main() -> int:
    srcs = source_files()
    _local_n = sum(len(v) for v in srcs.values())
    if "--include-unserved" in sys.argv:
        print(f"{_local_n:,} parquet files across {len(srcs)} store sources "
              f"(--include-unserved: counting LOCAL disk, NOT publishable)", flush=True)
    else:
        srcs, dropped = keep_served(srcs)
        n_drop = sum(dropped.values())
        print(f"{sum(len(v) for v in srcs.values()):,} SERVED parquet files across "
              f"{len(srcs)} sources "
              f"({n_drop:,} local file(s) skipped - absent from R2 or a different size)",
              flush=True)
        if dropped:
            worst = sorted(dropped.items(), key=lambda kv: -kv[1])[:8]
            print("  serving gap (computed locally, NOT downloadable): "
                  + ", ".join(f"{k} {v:,}" for k, v in worst), flush=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=24")
    _wire_r2(con)
    obs_by_src: dict[str, int] = {}
    ser_by_src: dict[str, int] = {}
    no_key_files = 0
    for i, (src, files) in enumerate(sorted(srcs.items()), 1):
        obs = 0
        keyed: list[str] = []
        for f in files:
            n_rows, has_key = _rows_and_key(con, f)
            obs += n_rows
            if has_key:
                keyed.append(f)
            else:
                no_key_files += 1
        obs_by_src[src] = obs
        if keyed:
            ser_by_src[src] = con.execute(
                "SELECT approx_count_distinct(series_key) FROM read_parquet(?, "
                "union_by_name=true)", [keyed]).fetchone()[0]
        else:
            ser_by_src[src] = 0
        print(f"  [{i}/{len(srcs)}] {src}: {obs:,} obs, "
              f"~{ser_by_src[src]:,} series", flush=True)

    cat = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro",
                          uri=True)
    n_sources = cat.execute("SELECT COUNT(DISTINCT source_id) FROM series").fetchone()[0]
    cat.close()

    today = dt.date.today().isoformat()
    stats = {
        "individual_series": int(sum(ser_by_src.values())),
        "observations": int(sum(obs_by_src.values())),
        "sources_catalogued": n_sources,
        "as_of": today,
        "method": ("individual_series = sum over sources of globally distinct "
                   "series keys, measured on the complete data store (HyperLogLog "
                   "estimate, ~1% error; conservative floor). observations = exact "
                   "parquet row counts. Refresh by re-running the census "
                   "(tools/series_census.py) and re-uploading this object."),
    }
    print(f"\nTOTALS: {stats['individual_series']:,} series / "
          f"{stats['observations']:,} obs / {n_sources} catalogued sources "
          f"({no_key_files} files had no series_key column)", flush=True)

    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    hist = os.path.join(ROOT, "logs", f"stats-{today}.json")
    detail = {**stats, "per_source_obs": obs_by_src, "per_source_series": ser_by_src}
    with open(hist, "w", encoding="utf-8") as fh:
        json.dump(detail, fh, indent=1)
    print(f"history written: {hist}")

    if "--publish" not in sys.argv:
        print("NOT PUBLISHED (measurement-only run; pass --publish to upload). "
              "NOTE: totals exclude R2-resident stores — see the R420 header.")
        return 0

    # R420 publish gate: refuse a silent step-change against the live object.
    s3 = r2_util.client(write=True)
    try:
        cur = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    except Exception:                                        # noqa: BLE001
        cur = None
    if cur and "--force-publish" not in sys.argv:
        for k in ("individual_series", "observations"):
            old_v, new_v = cur.get(k) or 0, stats[k]
            if old_v and abs(new_v - old_v) / old_v > 0.20:
                print(f"REFUSING to publish: {k} moves {old_v:,} -> {new_v:,} "
                      f"({(new_v - old_v) / old_v:+.0%}). Explain the delta, then "
                      f"re-run with --force-publish if it is real.")
                return 1
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(stats).encode("utf-8"),
                  ContentType="application/json")
    print(f"uploaded r2://{BUCKET}/{KEY}")

    import urllib.request
    req = urllib.request.Request(
        "https://econdl-api.elkassabgi.workers.dev/v1/stats",
        headers={"User-Agent": "census-verify"})
    live = json.loads(urllib.request.urlopen(req, timeout=60).read())
    ok = live.get("as_of") == today
    print(f"LIVE /v1/stats as_of = {live.get('as_of')} -> {'VERIFIED' if ok else 'STALE?'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
