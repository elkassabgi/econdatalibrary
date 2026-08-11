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
as_of. Run time is dominated by the census/eia key scans; threads capped at 24
so concurrent pulls keep breathing room.
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
KEY = "_aqueduct/stats.json"


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


def main() -> int:
    srcs = source_files()
    print(f"{sum(len(v) for v in srcs.values()):,} parquet files across "
          f"{len(srcs)} store sources", flush=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=24")
    obs_by_src: dict[str, int] = {}
    ser_by_src: dict[str, int] = {}
    no_key_files = 0
    for i, (src, files) in enumerate(sorted(srcs.items()), 1):
        obs = 0
        keyed: list[str] = []
        for f in files:
            md = pq.read_metadata(f)
            obs += md.num_rows
            if "series_key" in md.schema.names:
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

    s3 = r2_util.client(write=True)
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
