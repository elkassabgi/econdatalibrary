#!/usr/bin/env python3
"""FULL-COVERAGE ingest of the World Bank Sovereign ESG database (API source=75).

Catalog = ALL 71 ESG indicators (enumerated live from
  https://api.worldbank.org/v2/source/75/indicator ).

For each indicator we pull EVERY economy x year via
  https://api.worldbank.org/v2/country/all/indicator/<IND>?source=75
in a single large page (per_page=20000; the largest indicator has ~15k rows).

GROUPED storage (mirrors jobs/ingest_worldbank_wdi.py): ONE Parquet per indicator,
all economies inside -> columns (country, obs_date, value). 71 indicators => 71
Parquet files for the whole source. No per-series files.

Economy code fix: when queried with source=75 the API returns BLANK
country.id / countryiso3code for the ~34 aggregate economies (World, Euro area,
Sub-Saharan Africa, ...). We repair this with a name->code map built from the
canonical /v2/country reference list (keys stripped of trailing spaces), so every
row gets a proper WDI code (WLD, EMU, SSF, LCN, ...) exactly like the WDI bulk.

License: cc-by-4.0 (reservable id from configs/sources.yaml -> worldbank_esg).

Does NOT touch data/catalog.db or data/clean/ (per task constraints).

Run:
  python jobs/ingest_worldbank_esg.py --dry      # enumerate + 2 indicators, no writes
  python jobs/ingest_worldbank_esg.py            # full run
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)

SOURCE_ID = "worldbank_esg"
LICENSE_ID = "cc-by-4.0"
WB_SOURCE = "75"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"

RAW = os.path.join(ROOT, "data", "raw", SOURCE_ID)
OUT = os.path.join(ROOT, "data", "clean_full", SOURCE_ID)
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

API = "https://api.worldbank.org/v2"


def get_json(url: str, tries: int = 6):
    """GET with polite UA + exponential backoff. Returns parsed JSON."""
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            # Only 429 and 5xx are worth retrying. A 4xx like 400/404 is the server
            # telling us the REQUEST is wrong — repeating it verbatim cannot help, and
            # the cost is brutal here: 6 tries x 120 s timeout plus ~61 s of backoff is
            # ~13 min PER URL across ~71 indicators. Observed for real: a local run sat
            # on worldbank_esg for 39 minutes emitting
            # "retry 1/6 after 1s (HTTP Error 400: Bad Request)" before being killed,
            # and orchestrate.py runs sources serially so that stalls the whole job.
            if e.code != 429 and 400 <= e.code < 500:
                raise RuntimeError(f"GET {url} failed permanently: HTTP {e.code} {e.reason}")
            last = e
            wait = min(2 ** attempt, 30)
            print(f"    retry {attempt + 1}/{tries} after {wait}s ({e})", flush=True)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last = e
            wait = min(2 ** attempt, 30)
            print(f"    retry {attempt + 1}/{tries} after {wait}s ({e})", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"GET failed after {tries} tries: {url} ({last})")


def safe(s: str) -> str:
    return s.replace(":", "_").replace("/", "_").replace("\\", "_")


def build_code_map() -> dict[str, str]:
    """name(stripped) -> WDI code, from the canonical country reference list."""
    url = f"{API}/country?format=json&per_page=400"
    payload = get_json(url)
    ref = payload[1]
    m: dict[str, str] = {}
    for r in ref:
        m[r["name"].strip()] = r["id"]
    return m


def enumerate_indicators() -> list[dict]:
    """ALL ESG indicators (source=75). Prints the published total."""
    url = f"{API}/source/{WB_SOURCE}/indicator?format=json&per_page=1000"
    payload = get_json(url)
    meta, inds = payload[0], payload[1]
    total = meta.get("total")
    print(f"[{SOURCE_ID}] ESG catalog: published total = {total}; returned = {len(inds)}", flush=True)
    if total is not None and len(inds) != int(total):
        # paginate defensively if ever > 1000
        pages = int(meta.get("pages", 1))
        for p in range(2, pages + 1):
            more = get_json(f"{API}/source/{WB_SOURCE}/indicator?format=json&per_page=1000&page={p}")
            inds.extend(more[1])
        print(f"[{SOURCE_ID}] after pagination: {len(inds)} indicators", flush=True)
    return inds


def fetch_indicator_rows(ind_id: str) -> tuple[list[dict], dict]:
    """All economy x year rows for one indicator (source=75). Returns (rows, meta).
    Paginates if the single big page ever splits."""
    base = f"{API}/country/all/indicator/{ind_id}?source={WB_SOURCE}&format=json&per_page=20000"
    payload = get_json(base)
    meta, rows = payload[0], (payload[1] or [])
    pages = int(meta.get("pages", 1))
    for p in range(2, pages + 1):
        more = get_json(base + f"&page={p}")
        rows.extend(more[1] or [])
    return rows, meta


def year_to_date(y: str):
    try:
        return dt.date(int(y), 12, 31)
    except (ValueError, TypeError):
        return None


def main() -> int:
    dry = "--dry" in sys.argv
    code_map = build_code_map()
    print(f"[{SOURCE_ID}] code map: {len(code_map)} economy names", flush=True)

    indicators = enumerate_indicators()
    if dry:
        indicators = indicators[:2]
        print(f"[{SOURCE_ID}] DRY-RUN: processing {len(indicators)} indicators, no writes", flush=True)

    n_ind = 0
    n_obs = 0
    n_unmapped = 0
    manifest = []
    last_updated = None

    for ind in indicators:
        ind_id = ind["id"]
        ind_name = ind.get("name") or ind_id
        rows, meta = fetch_indicator_rows(ind_id)
        if meta.get("lastupdated"):
            last_updated = meta["lastupdated"]

        # persist raw json for re-verification / provenance
        if not dry:
            with open(os.path.join(RAW, safe(ind_id) + ".json"), "w", encoding="utf-8") as fh:
                json.dump(rows, fh)

        countries, dates, vals = [], [], []
        for r in rows:
            v = r.get("value")
            if v is None:
                continue
            od = year_to_date(r.get("date"))
            if od is None:
                continue
            code = r.get("countryiso3code") or ""
            if not code:
                nm = (r.get("country") or {}).get("value", "").strip()
                code = code_map.get(nm, "")
                if not code:
                    n_unmapped += 1
                    code = nm or "UNKNOWN"  # never drop an obs; keep with name as last resort
            countries.append(code)
            dates.append(od)
            try:
                vals.append(float(v))
            except (TypeError, ValueError):
                # value present but not numeric -> drop this single cell
                countries.pop(); dates.pop()
                continue

        total_rows = int(meta.get("total", len(rows)))
        if dry:
            uniq = len(set(countries))
            print(f"  {ind_id:20} api_total={total_rows:>7,} nonnull={len(vals):>7,} "
                  f"economies={uniq:>4} sample=({countries[0] if countries else '-'},"
                  f"{dates[0] if dates else '-'},{vals[0] if vals else '-'})", flush=True)
        else:
            if vals:
                tbl = pa.table({
                    "country": countries,
                    "obs_date": pa.array(dates, type=pa.date32()),
                    "value": vals,
                })
                pq.write_table(tbl, os.path.join(OUT, safe(ind_id) + ".parquet"))
            manifest.append({
                "indicator": ind_id, "name": ind_name,
                "api_total_rows": total_rows, "nonnull_obs": len(vals),
                "economies": len(set(countries)),
                "year_min": str(min(dates)) if dates else None,
                "year_max": str(max(dates)) if dates else None,
            })
        n_ind += 1
        n_obs += len(vals)
        if n_ind % 10 == 0:
            print(f"  ... {n_ind}/{len(indicators)} indicators, {n_obs:,} obs", flush=True)

    if not dry:
        with open(os.path.join(OUT, "_manifest.json"), "w", encoding="utf-8") as fh:
            json.dump({
                "source_id": SOURCE_ID, "license": LICENSE_ID,
                "wb_source": WB_SOURCE, "lastupdated": last_updated,
                "n_indicators": n_ind, "n_observations": n_obs,
                "indicators": manifest,
            }, fh, indent=2)

    print(f"[{SOURCE_ID}] {'DRY' if dry else 'DONE'}: {n_ind} indicators / {n_obs:,} observations "
          f"(unmapped economy codes: {n_unmapped}; lastupdated={last_updated})", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
