#!/usr/bin/env python3
"""Enumerate the ENTIRE BLS flat-file catalog (download.bls.gov/pub/time.series/).

Crawls the top-level index for every survey folder, then each survey folder for
its files. Produces data/raw/bls/_manifest.json:

  { "<survey>": {
        "all":  {filename: size_bytes, ...},     # every file in the folder
        "data": ["<survey>.data.N.*", ...],       # observation files only
        "has_series": bool                        # a <survey>.series file exists
    }, ... }

BLS blocks generic UAs -> use the project UA. Retry/backoff on transient errors.
This is the authoritative catalog enumeration that feeds ingest_bls.py and the
coverage/honesty accounting (source_published_total = sum of .series line counts).
"""
from __future__ import annotations
import json
import os
import re
import sys
import time

import requests

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "bls")
BASE = "https://download.bls.gov/pub/time.series"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

# top-level folders that are NOT survey databases
NON_SURVEY = {"compressed", "sdmx"}

os.makedirs(RAW, exist_ok=True)
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

# IIS directory row: "<date> <time>  <size> <A HREF="...">name</A>"
ROW = re.compile(r'(\d+)\s+<A HREF="[^"]*">([^<]+)</A>', re.I)
DIRROW = re.compile(r'<A HREF="/pub/time\.series/([a-z0-9_]+)/">', re.I)


def get(url: str) -> str:
    last = None
    for attempt in range(6):
        try:
            r = SESSION.get(url, timeout=120)
            r.raise_for_status()
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(60, 3 * (attempt + 1) ** 2))
    raise RuntimeError(f"GET failed {url}: {last}")


def list_surveys() -> list[str]:
    html = get(BASE + "/")
    found = sorted(set(DIRROW.findall(html)))
    return [s for s in found if s not in NON_SURVEY]


def list_folder(survey: str) -> dict:
    html = get(f"{BASE}/{survey}/")
    files = {}
    for size, name in ROW.findall(html):
        name = name.strip()
        if name and not name.startswith("["):
            files[name] = int(size)
    data = sorted(n for n in files if re.search(rf"^{re.escape(survey)}\.data\.", n))
    has_series = f"{survey}.series" in files
    return {"all": files, "data": data, "has_series": has_series}


def main():
    surveys = list_surveys()
    print(f"survey folders (excluding {sorted(NON_SURVEY)}): {len(surveys)}", flush=True)
    manifest = {}
    for i, s in enumerate(surveys, 1):
        info = list_folder(s)
        manifest[s] = info
        print(f"  [{i:>2}/{len(surveys)}] {s:6} files={len(info['all']):>3} "
              f"data={len(info['data']):>3} series={'Y' if info['has_series'] else 'n'}",
              flush=True)
    out = os.path.join(RAW, "_manifest.json")
    json.dump(manifest, open(out, "w"), indent=2)
    tot_data = sum(len(v["data"]) for v in manifest.values())
    tot_series = sum(1 for v in manifest.values() if v["has_series"])
    tot_files = sum(len(v["all"]) for v in manifest.values())
    print("=" * 60, flush=True)
    print(f"WROTE {out}", flush=True)
    print(f"surveys={len(manifest)} data_files={tot_data} "
          f"surveys_with_series={tot_series} total_files={tot_files}", flush=True)


if __name__ == "__main__":
    main()
