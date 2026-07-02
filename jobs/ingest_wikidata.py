#!/usr/bin/env python3
"""FULL-coverage ingest of the Wikidata econ/finance ENTITY set (reference data; CC0).

Wikidata is a knowledge graph, not a time series. We pull the ENTIRE econ/finance
entity universe and store it as GROUPED Parquet -- one file per "cube", each holding
every entity with a `series_key` column (the Wikidata QID):

  data/clean_full/wikidata/
    companies.parquet     one row per company that has a stock ticker (P249) OR an ISIN (P946)
    stock_exchanges.parquet  one row per stock exchange (P31/P279* Q11691)
    currencies.parquet    one row per currency (P31/P279* Q8142)
    _manifest.json        per-cube row counts + the SPARQL-published total it targets

Multi-valued attributes (tickers, ISINs, exchanges, industries, ...) are aggregated
per entity with SPARQL GROUP_CONCAT (separator '|') so each result row == one entity;
we paginate the DISTINCT entity set with LIMIT/OFFSET over a stable ORDER BY ?x to
pull ALL of them (not a 250-row sample). License = cc0 (the reservable id in
configs/sources.yaml). Polite UA, retry/backoff, single-stream paging (well under the
concurrency<=6 cap; WDQS rate-limits aggressive parallelism anyway).

Usage:
  python jobs/ingest_wikidata.py --probe       # print SPARQL totals, no writes
  python jobs/ingest_wikidata.py --dry         # pull 1 small page per cube, print, no writes
  python jobs/ingest_wikidata.py               # FULL run -> grouped Parquet
"""
from __future__ import annotations

import json
import os
import sys
import time

import requests
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
OUT = os.path.join(ROOT, "data", "clean_full", "wikidata")

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "Econ-Fin Data Library admin@hfdatalibrary.com"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}

PAGE = 1000          # distinct entities per page (WDQS handles this in a few seconds)
MAX_RETRIES = 6
SLEEP_BETWEEN_PAGES = 1.0   # be polite to WDQS between pages


# --------------------------------------------------------------------------- #
# HTTP                                                                          #
# --------------------------------------------------------------------------- #
def run_sparql(query: str, tag: str, timeout: int = 300) -> list[dict]:
    last = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(ENDPOINT, params={"query": query, "format": "json"},
                             headers=HEADERS, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(120, 10 * (attempt + 1))
                ra = r.headers.get("Retry-After")
                if ra and ra.isdigit():
                    wait = min(180, int(ra))
                print(f"  [{tag}] HTTP {r.status_code} -> backoff {wait}s (try {attempt+1}/{MAX_RETRIES})", flush=True)
                time.sleep(wait)
                last = f"HTTP {r.status_code}"
                continue
            r.raise_for_status()
            return r.json()["results"]["bindings"]
        except (requests.RequestException, ValueError) as e:
            last = str(e)
            wait = min(120, 10 * (attempt + 1))
            print(f"  [{tag}] error {e} -> retry {wait}s (try {attempt+1}/{MAX_RETRIES})", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"[{tag}] SPARQL failed after {MAX_RETRIES} tries: {last}")


def count(query: str, tag: str) -> int:
    b = run_sparql(query, tag)
    return int(b[0]["c"]["value"]) if b else 0


def qid_of(uri: str | None) -> str | None:
    return uri.rsplit("/", 1)[-1] if uri else None


# --------------------------------------------------------------------------- #
# SPARQL: total-count probes                                                    #
# --------------------------------------------------------------------------- #
COUNT_COMPANIES = """
SELECT (COUNT(DISTINCT ?x) AS ?c) WHERE {
  { ?x wdt:P946 ?isin . } UNION { ?x p:P414 ?s . ?s pq:P249 ?t . } UNION { ?x wdt:P249 ?d . }
}"""
COUNT_EXCHANGES = "SELECT (COUNT(DISTINCT ?x) AS ?c) WHERE { ?x wdt:P31/wdt:P279* wd:Q11691 . }"
COUNT_CURRENCIES = "SELECT (COUNT(DISTINCT ?x) AS ?c) WHERE { ?x wdt:P31/wdt:P279* wd:Q8142 . }"


# --------------------------------------------------------------------------- #
# SPARQL: paged data pulls (each result row == one entity via GROUP_CONCAT)     #
# --------------------------------------------------------------------------- #
def companies_page(limit: int, offset: int) -> str:
    # Identifiers carried: ISIN P946, ticker (qualifier pq:P249 on p:P414, plus direct
    # wdt:P249), LEI P1278, SEC CIK P5531, PermID P3347, OpenCorporates P1320,
    # Crunchbase P2088. Plus exchanges (P414), industry P452, country P17,
    # inception P571, official website P856.
    return f"""
SELECT ?x ?xLabel ?countryLabel
  (GROUP_CONCAT(DISTINCT ?isin;   SEPARATOR="|") AS ?isins)
  (GROUP_CONCAT(DISTINCT ?qtick;  SEPARATOR="|") AS ?qtickers)
  (GROUP_CONCAT(DISTINCT ?dtick;  SEPARATOR="|") AS ?dtickers)
  (GROUP_CONCAT(DISTINCT ?exLabel;SEPARATOR="|") AS ?exchanges)
  (GROUP_CONCAT(DISTINCT ?indLabel;SEPARATOR="|") AS ?industries)
  (GROUP_CONCAT(DISTINCT ?lei;    SEPARATOR="|") AS ?leis)
  (GROUP_CONCAT(DISTINCT ?cik;    SEPARATOR="|") AS ?ciks)
  (GROUP_CONCAT(DISTINCT ?permid; SEPARATOR="|") AS ?permids)
  (GROUP_CONCAT(DISTINCT ?occ;    SEPARATOR="|") AS ?opencorporates)
  (GROUP_CONCAT(DISTINCT ?cb;     SEPARATOR="|") AS ?crunchbase)
  (SAMPLE(?inception) AS ?inc)
  (SAMPLE(?website)   AS ?site)
WHERE {{
  {{
    SELECT DISTINCT ?x WHERE {{
      {{ ?x wdt:P946 ?i0 . }} UNION {{ ?x p:P414 ?s0 . ?s0 pq:P249 ?t0 . }} UNION {{ ?x wdt:P249 ?d0 . }}
    }}
    ORDER BY ?x LIMIT {limit} OFFSET {offset}
  }}
  OPTIONAL {{ ?x wdt:P946  ?isin . }}
  OPTIONAL {{ ?x p:P414 ?st . ?st pq:P249 ?qtick . }}
  OPTIONAL {{ ?x wdt:P249  ?dtick . }}
  OPTIONAL {{ ?x p:P414 ?st2 . ?st2 ps:P414 ?ex . ?ex rdfs:label ?exLabel . FILTER(LANG(?exLabel)="en") }}
  OPTIONAL {{ ?x wdt:P452  ?ind . ?ind rdfs:label ?indLabel . FILTER(LANG(?indLabel)="en") }}
  OPTIONAL {{ ?x wdt:P1278 ?lei . }}
  OPTIONAL {{ ?x wdt:P5531 ?cik . }}
  OPTIONAL {{ ?x wdt:P3347 ?permid . }}
  OPTIONAL {{ ?x wdt:P1320 ?occ . }}
  OPTIONAL {{ ?x wdt:P2088 ?cb . }}
  OPTIONAL {{ ?x wdt:P571  ?inception . }}
  OPTIONAL {{ ?x wdt:P856  ?website . }}
  OPTIONAL {{ ?x wdt:P17   ?country . ?country rdfs:label ?countryLabel . FILTER(LANG(?countryLabel)="en") }}
  OPTIONAL {{ ?x rdfs:label ?xLabel . FILTER(LANG(?xLabel)="en") }}
}}
GROUP BY ?x ?xLabel ?countryLabel
ORDER BY ?x
"""


def exchanges_page(limit: int, offset: int) -> str:
    # MIC market code = P7534 (verified -- NOT P1656). short name/acronym P1813,
    # inception P571, official website P856, country P17.
    return f"""
SELECT ?x ?xLabel ?countryLabel
  (GROUP_CONCAT(DISTINCT ?mic; SEPARATOR="|") AS ?mics)
  (GROUP_CONCAT(DISTINCT ?acr; SEPARATOR="|") AS ?acronyms)
  (SAMPLE(?inception) AS ?inc)
  (SAMPLE(?website)   AS ?site)
WHERE {{
  {{
    SELECT DISTINCT ?x WHERE {{ ?x wdt:P31/wdt:P279* wd:Q11691 . }}
    ORDER BY ?x LIMIT {limit} OFFSET {offset}
  }}
  OPTIONAL {{ ?x wdt:P7534 ?mic . }}
  OPTIONAL {{ ?x wdt:P1813 ?acr . FILTER(LANG(?acr)="en" || LANG(?acr)="") }}
  OPTIONAL {{ ?x wdt:P571  ?inception . }}
  OPTIONAL {{ ?x wdt:P856  ?website . }}
  OPTIONAL {{ ?x wdt:P17   ?country . ?country rdfs:label ?countryLabel . FILTER(LANG(?countryLabel)="en") }}
  OPTIONAL {{ ?x rdfs:label ?xLabel . FILTER(LANG(?xLabel)="en") }}
}}
GROUP BY ?x ?xLabel ?countryLabel
ORDER BY ?x
"""


def currencies_page(limit: int, offset: int) -> str:
    # ISO 4217 alpha = P498, ISO 4217 numeric = P499, currency symbol = P489,
    # country (where used) P17, inception P571.
    return f"""
SELECT ?x ?xLabel
  (GROUP_CONCAT(DISTINCT ?iso; SEPARATOR="|") AS ?iso4217)
  (GROUP_CONCAT(DISTINCT ?num; SEPARATOR="|") AS ?iso4217num)
  (GROUP_CONCAT(DISTINCT ?sym; SEPARATOR="|") AS ?symbols)
  (GROUP_CONCAT(DISTINCT ?countryLabel; SEPARATOR="|") AS ?countries)
  (SAMPLE(?inception) AS ?inc)
WHERE {{
  {{
    SELECT DISTINCT ?x WHERE {{ ?x wdt:P31/wdt:P279* wd:Q8142 . }}
    ORDER BY ?x LIMIT {limit} OFFSET {offset}
  }}
  OPTIONAL {{ ?x wdt:P498 ?iso . }}
  OPTIONAL {{ ?x wdt:P499 ?num . }}
  OPTIONAL {{ ?x wdt:P489 ?symN . ?symN rdfs:label ?sym . FILTER(LANG(?sym)="en") }}
  OPTIONAL {{ ?x wdt:P571 ?inception . }}
  OPTIONAL {{ ?x wdt:P17  ?country . ?country rdfs:label ?countryLabel . FILTER(LANG(?countryLabel)="en") }}
  OPTIONAL {{ ?x rdfs:label ?xLabel . FILTER(LANG(?xLabel)="en") }}
}}
GROUP BY ?x ?xLabel
ORDER BY ?x
"""


# --------------------------------------------------------------------------- #
# Row-shaping helpers (collapse the GROUP_CONCAT row -> clean dict)             #
# --------------------------------------------------------------------------- #
def _split(val: str | None) -> list[str]:
    if not val:
        return []
    return [s for s in (p.strip() for p in val.split("|")) if s]


def _label(val: str | None) -> str | None:
    """Drop the bare-QID echo the label service returns when no English label exists."""
    if not val:
        return None
    s = val.strip()
    if not s:
        return None
    if s[0] in "QPL" and s[1:].isdigit():
        return None
    return s


def shape_company(d: dict) -> dict:
    qid = qid_of(d.get("x"))
    tickers = sorted(set(_split(d.get("qtickers")) + _split(d.get("dtickers"))))
    return {
        "series_key": qid,
        "qid": qid,
        "wikidata_url": d.get("x"),
        "name": _label(d.get("xLabel")) or qid,
        "tickers": "|".join(tickers),
        "primary_ticker": tickers[0] if tickers else None,
        "isins": "|".join(sorted(set(_split(d.get("isins"))))),
        "exchanges": "|".join(sorted(set(_split(d.get("exchanges"))))),
        "industries": "|".join(sorted(set(_split(d.get("industries"))))),
        "country": _label(d.get("countryLabel")),
        "lei": "|".join(sorted(set(_split(d.get("leis"))))),
        "sec_cik": "|".join(sorted(set(_split(d.get("ciks"))))),
        "permid": "|".join(sorted(set(_split(d.get("permids"))))),
        "opencorporates": "|".join(sorted(set(_split(d.get("opencorporates"))))),
        "crunchbase": "|".join(sorted(set(_split(d.get("crunchbase"))))),
        "inception": (d.get("inc") or None),
        "website": d.get("site") or None,
    }


def shape_exchange(d: dict) -> dict:
    qid = qid_of(d.get("x"))
    return {
        "series_key": qid,
        "qid": qid,
        "wikidata_url": d.get("x"),
        "name": _label(d.get("xLabel")) or qid,
        "mic": "|".join(sorted(set(_split(d.get("mics"))))),
        "acronyms": "|".join(sorted(set(_split(d.get("acronyms"))))),
        "country": _label(d.get("countryLabel")),
        "inception": d.get("inc") or None,
        "website": d.get("site") or None,
    }


def shape_currency(d: dict) -> dict:
    qid = qid_of(d.get("x"))
    return {
        "series_key": qid,
        "qid": qid,
        "wikidata_url": d.get("x"),
        "name": _label(d.get("xLabel")) or qid,
        "iso4217": "|".join(sorted(set(_split(d.get("iso4217"))))),
        "iso4217_numeric": "|".join(sorted(set(_split(d.get("iso4217num"))))),
        "symbols": "|".join(sorted(set(_split(d.get("symbols"))))),
        "countries": "|".join(sorted(set(_split(d.get("countries"))))),
        "inception": d.get("inc") or None,
    }


# --------------------------------------------------------------------------- #
# Paged pull -> one row per entity (dedupe by QID across the bounded page)      #
# --------------------------------------------------------------------------- #
def pull_cube(name: str, page_fn, shape_fn, total: int, dry: bool) -> dict:
    print(f"\n=== {name}: target total = {total:,} ===", flush=True)
    records: dict[str, dict] = {}
    offset = 0
    page_no = 0
    pages_to_do = 1 if dry else None
    while True:
        page_no += 1
        rows = run_sparql(page_fn(PAGE, offset), f"{name}#{page_no}")
        if not rows:
            print(f"  page {page_no} (offset {offset}): 0 rows -> done", flush=True)
            break
        new = 0
        for rr in rows:
            d = {k: v.get("value") for k, v in rr.items()}
            rec = shape_fn(d)
            key = rec["series_key"]
            if not key:
                continue
            if key not in records:
                records[key] = rec
                new += 1
        print(f"  page {page_no} (offset {offset}): {len(rows)} rows, +{new} new, total {len(records):,}/{total:,}",
              flush=True)
        offset += PAGE
        if pages_to_do and page_no >= pages_to_do:
            print(f"  [dry] stopping after {pages_to_do} page", flush=True)
            break
        # Stop when we've paged past the published distinct total AND the last page
        # added nothing new (guards against off-by-one at the tail).
        if offset >= total and new == 0:
            break
        if offset >= total + PAGE:   # hard safety stop
            break
        time.sleep(SLEEP_BETWEEN_PAGES)

    recs = list(records.values())
    print(f"  {name}: collected {len(recs):,} distinct entities", flush=True)
    if dry:
        for r in recs[:3]:
            print("   sample:", json.dumps(r, ensure_ascii=False)[:400], flush=True)
        return {"name": name, "rows": len(recs), "target_total": total, "written": False}

    os.makedirs(OUT, exist_ok=True)
    cols = list(recs[0].keys())
    table = pa.table({c: pa.array([r.get(c) for r in recs], type=pa.string()) for c in cols})
    path = os.path.join(OUT, f"{name}.parquet")
    pq.write_table(table, path)
    print(f"  WROTE {path}  ({len(recs):,} rows, {len(cols)} cols)", flush=True)
    return {"name": name, "rows": len(recs), "target_total": total, "written": True,
            "path": os.path.basename(path)}


# --------------------------------------------------------------------------- #
def main():
    probe = "--probe" in sys.argv
    dry = "--dry" in sys.argv

    print("Probing SPARQL published totals ...", flush=True)
    t_companies = count(COUNT_COMPANIES, "count-companies")
    t_exchanges = count(COUNT_EXCHANGES, "count-exchanges")
    t_currencies = count(COUNT_CURRENCIES, "count-currencies")
    grand = t_companies + t_exchanges + t_currencies
    print(f"  companies (ticker P249 OR ISIN P946): {t_companies:,}", flush=True)
    print(f"  stock exchanges (P31/P279* Q11691):   {t_exchanges:,}", flush=True)
    print(f"  currencies (P31/P279* Q8142):         {t_currencies:,}", flush=True)
    print(f"  GRAND TOTAL entities:                 {grand:,}", flush=True)
    if probe:
        return

    cubes = [
        pull_cube("companies", companies_page, shape_company, t_companies, dry),
        pull_cube("stock_exchanges", exchanges_page, shape_exchange, t_exchanges, dry),
        pull_cube("currencies", currencies_page, shape_currency, t_currencies, dry),
    ]

    if not dry:
        manifest = {
            "source_id": "wikidata",
            "license": "cc0",
            "attribution": "Source: Wikidata (CC0 1.0 public domain dedication)",
            "homepage": "https://www.wikidata.org",
            "endpoint": ENDPOINT,
            "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "published_totals": {
                "companies": t_companies,
                "stock_exchanges": t_exchanges,
                "currencies": t_currencies,
                "grand_total": grand,
            },
            "cubes": cubes,
            "written_total": sum(c["rows"] for c in cubes),
        }
        with open(os.path.join(OUT, "_manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        print(f"\nWROTE manifest. Written total = {manifest['written_total']:,} / published {grand:,}", flush=True)

    print("\nDONE.", flush=True)


if __name__ == "__main__":
    main()
