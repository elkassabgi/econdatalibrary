#!/usr/bin/env python3
"""Enumerate the ENTIRE Bank of England IADB series catalogue.

Strategy (provably complete): the database's "Combined A to Z" page lists every
category VALUE across all facets (INSTRUMENTS=6, SECTOR=5, COUNTRY=9, facet 4).
Every series is tagged with at least an instrument and a sector, so the UNION of
the series codes found by drilling into ALL category pages == the full catalogue.

Each category page (FromShowColumns.asp) renders 150 series per page; we follow
ShadowPage pagination until TotalNumResults codes are collected. Series codes are
read from the `SeriesCodes=<CODE>` download links the page emits for every series.

Output: data/raw/boe/_series_codes.json  -> {"codes":[...], "by_category":{...}, ...}
Resumable: per-category results cached in data/raw/boe/_cat_cache/<id>.json

Usage:
  python jobs/_boe_enumerate.py --facet 6        # only INSTRUMENTS facet
  python jobs/_boe_enumerate.py                  # ALL facets (complete union)
  python jobs/_boe_enumerate.py --workers 6
"""
import json
import os
import re
import sys
import threading
import time
import html as htmlmod
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
RAW = os.path.join(ROOT, "data", "raw", "boe")
CATCACHE = os.path.join(RAW, "_cat_cache")
CATS_JSON = os.path.join(RAW, "_categories.json")
OUT_JSON = os.path.join(RAW, "_series_codes.json")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
BASE = "https://www.bankofengland.co.uk/boeapps/database"
AZ_URL = (BASE + "/CategoryIndex.asp?Travel=NIxAZx&CategId=allcats"
          "&CategName=Combined%20A%20to%20Z")
FSC = BASE + "/FromShowColumns.asp"

_print_lock = threading.Lock()


def log(m):
    with _print_lock:
        print(m, flush=True)


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s


def get_retry(s, url, params=None, tries=5):
    for i in range(tries):
        try:
            r = s.get(url, params=params, timeout=120)
            if r.status_code == 200:
                return r
            # BoE returns 500 for category values containing < or > (the classic-ASP
            # page can't decode %3C/%3E). That is PERMANENT, not transient -> fail fast.
            if r.status_code == 500:
                return r
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 * (i + 1) + 1)
                continue
            r.raise_for_status()
        except requests.RequestException:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1) + 1)
    return None


def parse_codes(text):
    """All distinct SeriesCodes=<code> tokens on the page (handles comma lists)."""
    out = set()
    for grp in re.findall(r"SeriesCodes=([A-Z0-9,]+)", text):
        for c in grp.split(","):
            if c:
                out.add(c)
    return out


def parse_total(text):
    m = re.search(r"TotalNumResults[\"'>\s=]+(\d+)", text)
    return int(m.group(1)) if m else None


def parse_anp(text):
    m = re.search(r"ActualResNumPerPage[\"'>\s=]+([0-9X]+)", text)
    return m.group(1) if m else None


def discover_categories(s):
    """Return list of {NewMeaningId, CategId, Highlight}; cache to _categories.json."""
    log("Fetching Combined A to Z category index ...")
    r = get_retry(s, AZ_URL)
    text = r.text
    cats = {}
    for m in re.finditer(r"href=[\"'](FromShowColumns\.asp\?[^\"']*)[\"']", text, re.I):
        href = htmlmod.unescape(m.group(1))
        nm = re.search(r"NewMeaningId=([^&]+)", href)
        cid = re.search(r"CategId=([^&]+)", href)
        hv = re.search(r"HighlightCatValueDisplay=([^&]*)", href)
        if nm and cid:
            key = (cid.group(1), nm.group(1))
            cats[key] = {
                "NewMeaningId": nm.group(1),
                "CategId": cid.group(1),
                "Highlight": htmlmod.unescape(hv.group(1)) if hv else "",
            }
    cats = list(cats.values())
    json.dump(cats, open(CATS_JSON, "w", encoding="utf-8"), indent=0)
    from collections import Counter
    log(f"Categories: {len(cats)} unique  by_facet={dict(Counter(c['CategId'] for c in cats))}")
    return cats


def cat_cache_path(c):
    safe = re.sub(r"[^A-Za-z0-9_]", "_", f"{c['CategId']}__{c['NewMeaningId']}")[:120]
    return os.path.join(CATCACHE, safe + ".json")


def crawl_category(c):
    """Return set of series codes for one category, paginating fully. Cached."""
    cp = cat_cache_path(c)
    if os.path.exists(cp):
        try:
            d = json.load(open(cp, encoding="utf-8"))
            return set(d["codes"]), d.get("total"), True
        except Exception:
            pass
    s = session()
    params = {
        "Travel": "NIxAZxI1x",
        "FromCategoryList": "Yes",
        "NewMeaningId": c["NewMeaningId"],
        "CategID": c["CategId"],
        "HighlightCatValueDisplay": c["Highlight"],
        "ShadowPage": "1",
    }
    r = get_retry(s, FSC, params=params)
    codes = parse_codes(r.text)
    total = parse_total(r.text)
    anp = parse_anp(r.text)
    page = 1
    # paginate until we have `total` codes (or pages stop yielding new ones)
    while total is not None and len(codes) < total and page < 200:
        page += 1
        p2 = dict(params)
        p2["ShadowPage"] = str(page)
        if anp:
            p2["ActualResNumPerPage"] = anp
        r2 = get_retry(s, FSC, params=p2)
        new = parse_codes(r2.text)
        before = len(codes)
        codes |= new
        anp = parse_anp(r2.text) or anp
        if len(codes) == before:
            break  # no progress -> stop (defensive)
    d = {"category": c, "total": total, "n_codes": len(codes), "codes": sorted(codes)}
    json.dump(d, open(cp, "w", encoding="utf-8"))
    return codes, total, False


def main():
    argv = sys.argv[1:]
    workers = int(argv[argv.index("--workers") + 1]) if "--workers" in argv else 6
    workers = max(1, min(workers, 6))
    facet = argv[argv.index("--facet") + 1] if "--facet" in argv else None

    os.makedirs(CATCACHE, exist_ok=True)
    s = session()
    if os.path.exists(CATS_JSON):
        cats = json.load(open(CATS_JSON, encoding="utf-8"))
        log(f"Loaded {len(cats)} categories from cache")
    else:
        cats = discover_categories(s)
    if facet:
        cats = [c for c in cats if c["CategId"] == facet]
        log(f"Filtered to facet {facet}: {len(cats)} categories")

    all_codes = set()
    by_cat = {}
    totals = {}
    t0 = time.time()
    done = 0
    cached_hits = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(crawl_category, c): c for c in cats}
        for fut in as_completed(futs):
            c = futs[fut]
            done += 1
            try:
                codes, total, was_cached = fut.result()
                if was_cached:
                    cached_hits += 1
                all_codes |= codes
                key = f"{c['CategId']}:{c['NewMeaningId']}"
                by_cat[key] = len(codes)
                totals[key] = total
            except Exception as e:  # noqa: BLE001
                log(f"  ERROR cat {c['CategId']}:{c['NewMeaningId']} ({c['Highlight'][:40]}): {type(e).__name__}: {e}")
            if done % 50 == 0:
                rate = done / max(time.time() - t0, 1e-9)
                eta = (len(cats) - done) / max(rate, 1e-9) / 60
                log(f"  [{done}/{len(cats)}] union_codes={len(all_codes):,} "
                    f"{rate:.1f} cat/s ETA {eta:.1f}m (cached {cached_hits})")

    out = {
        "n_codes": len(all_codes),
        "codes": sorted(all_codes),
        "n_categories": len(cats),
        "facet": facet or "all",
        "by_category_count": by_cat,
        "category_totals": totals,
    }
    json.dump(out, open(OUT_JSON, "w", encoding="utf-8"))
    log("=" * 60)
    log(f"DONE: {len(all_codes):,} distinct series codes across {len(cats)} categories")
    log(f"  written: {OUT_JSON}")


if __name__ == "__main__":
    main()
