#!/usr/bin/env python3
"""CSO Ireland (Central Statistics Office) — PxStat full ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://data.cso.ie / https://ws.cso.ie/public/api.restful/
No API key required.

Coverage: ~10,000+ tables across 9 themes:
  * Census (population by age, sex, nationality, housing, etc.)
  * Economy (national accounts, trade, finance, prices)
  * Labour Market and Earnings
  * People and Society (health, housing, crime, education)
  * Business Sectors (agriculture, industry, transport, tourism)
  * Environment (energy, climate, ecosystem)
  * Other Public Sector Bodies (Central Bank, Dept of Health, etc.)
  * High Value Datasets
  * Themed Publications

Run: python jobs/ingest_cso_ireland.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
from collections import defaultdict
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
OUT  = os.path.join(ROOT, "data", "clean_full", "cso")
JSONRPC_URL = "https://ws.cso.ie/public/api.jsonrpc"
REST_BASE   = "https://ws.cso.ie/public/api.restful"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Content-Type": "application/json"}
RATE = 0.4
CATALOG_FILE = os.path.join(OUT, "_catalog.json")


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def jsonrpc(method: str, params: dict, retries: int = 3) -> list | dict | None:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    for attempt in range(retries):
        try:
            r = requests.post(JSONRPC_URL, json=body, headers=UA, timeout=60)
            if r.status_code == 200:
                d = r.json()
                if d.get("error"):
                    log(f"  JSON-RPC error: {d['error']}")
                    return None
                return d.get("result")
            if r.status_code == 429:
                log("  429, sleeping 30s"); time.sleep(30); continue
            log(f"  JSON-RPC HTTP {r.status_code}")
        except Exception as e:
            log(f"  JSON-RPC ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def get_json(url: str, retries: int = 3) -> dict | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers={"User-Agent": UA["User-Agent"]}, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log("  429, sleeping 30s"); time.sleep(30); continue
            log(f"  GET HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            log(f"  GET ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def build_catalog() -> list[dict]:
    """Get all matrix (table) codes via a single Search call (13,000+ tables)."""
    if os.path.exists(CATALOG_FILE):
        with open(CATALOG_FILE) as f:
            cat = json.load(f)
        log(f"Loaded catalog: {len(cat)} tables")
        return cat

    log("Fetching CSO Ireland full table catalog via Search...")
    matrices = jsonrpc("PxStat.System.Navigation.Navigation_API.Search", {"LngIsoCode": "en"})
    if not matrices:
        log("Failed to get catalog")
        return []

    catalog = []
    for m in matrices:
        mtr = m.get("MtrCode", "")
        if not mtr:
            continue
        catalog.append({
            "MtrCode":  mtr,
            "MtrTitle": m.get("MtrTitle", ""),
            "SbjCode":  m.get("SbjCode", 0),
            "SbjValue": m.get("SbjValue", ""),
            "ThmCode":  m.get("ThmCode", 0),
            "ThmValue": m.get("ThmValue", ""),
            "PrcCode":  m.get("PrcCode", ""),
            "FrqCode":  m.get("FrqCode", ""),
            "Archived": m.get("RlsArchiveFlag", False),
        })

    log(f"Catalog complete: {len(catalog)} tables ({sum(1 for c in catalog if not c['Archived'])} current)")
    os.makedirs(OUT, exist_ok=True)
    with open(CATALOG_FILE, "w") as f:
        json.dump(catalog, f)
    return catalog


def parse_date(s: str) -> dt.date | None:
    s = (s or "").strip()
    try:
        # Annual: "2022", "2022A1"
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        m = re.match(r"^(\d{4})A1$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 12, 31)
        # Monthly: "2022M01"
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        # Quarterly: "2022Q1"
        m = re.match(r"^(\d{4})Q(\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        # Half-year: "2022H1", "2022S1"
        m = re.match(r"^(\d{4})[HS](\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        # Weekly: "2022W01"
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        # TLIST style: "TLIST(A1)" is the dimension name, values are years
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
        # DAILY, TLIST(D1): "2010M01D01". MEASURED 2026-08-03 — MTD05 ("Precipitation Amount")
        # and MTH05 each publish ~6,025 of these. Both return HTTP 200 with real bodies (MTD05
        # is 557,685 bytes) and BOTH PARSED TO ZERO ROWS, because this grammar stopped at
        # monthly. The fetcher then filed them as "fetch_table returned no rows (network failure
        # after retries...)" — so a pure parser gap was reported as upstream weather, for every
        # daily matrix CSO publishes.
        m = re.match(r"^(\d{4})M(\d{2})D(\d{2})$", s, re.IGNORECASE)
        if m:
            mo, dy = int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12 and 1 <= dy <= 31:
                return dt.date(int(m.group(1)), mo, dy)   # ValueError -> None for 02-30 etc.
            return None
        # SPLIT / ACADEMIC YEAR: "2003-2004" (EDA21, "Average Class Size in Mainstream National
        # Schools"; 22 codes, all dropped). Dated to the year the period BEGINS, matching bare
        # YYYY -> that year's 31 Dec above.
        #
        # Second year must be first + 1. parse_date also feeds is_time_dim's value-driven
        # fallback, so a loose ^\d{4}-\d{4}$ would let a range label ("1990-2000") read as a
        # date and could promote a classification axis to the time axis — the swapped-axis
        # defect that cost 290 matrices and 754,780 rows to repair (R288).
        m = re.match(r"^(\d{4})-(\d{4})$", s)
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2))
            if y2 == y1 + 1:
                return dt.date(y1, 12, 31)
            return None
    except (ValueError, TypeError):
        pass
    return None


def is_time_dim(dim_id: str, values: list[str]) -> bool:
    did = dim_id.upper()
    # CSO uses TLIST(A1), TLIST(M1), TLIST(Q1), etc.
    if did.startswith("TLIST"):
        return True
    if did in ("TIME", "YEAR", "PERIOD", "TID"):
        return True
    if values:
        # THE 4-DIGIT PREFIX MUST BE A PLAUSIBLE YEAR, not merely four digits. CSO
        # classification dimensions use numeric sentinel codes — 3001/3002 for category splits,
        # 9998/9999 for "not stated"/"all" — and `^\d{4}...$` alone accepts every one of them,
        # so a classification axis could pass as time and have its codes parsed as years. That
        # produced 434,408 rows dated beyond 2100 in the live store (272,445 in Census 2016
        # alone, at 9998-12-31), and it is the same hole ingest_pxweb.py's is_time_dim already
        # closes: "its values parse ... to a SANE year".
        #
        # The bound is 1800..2100, not current_year+2 as pxweb uses: detection must not reject
        # a genuine projection axis, and legitimate long horizons exist in this fleet (un_wpp
        # reaches 2101, gapminder and owid 2100). 1800..2100 excludes every sentinel observed
        # while leaving real data alone.
        sample = values[:5]
        yr_count = 0
        for v in sample:
            m = re.match(r"^(\d{4})[MQHSAW]?\d*$", str(v).strip())
            if m and 1800 <= int(m.group(1)) <= 2100:
                yr_count += 1
        return yr_count >= len(sample) * 0.6
    return False


def parse_jsonstat2(data: dict, prefix: str) -> list[tuple[str, dt.date, float]]:
    results = []
    try:
        dim_ids   = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims      = data.get("dimension", {})
        values    = data.get("value", [])
        if not dim_ids or not values:
            return results

        time_dim_idx = None
        dim_codes = []
        dim_labels = []
        for i, did in enumerate(dim_ids):
            cat = dims.get(did, {}).get("category", {})
            cat_idx = cat.get("index", {})
            if isinstance(cat_idx, dict):
                size = dim_sizes[i] if i < len(dim_sizes) else max(cat_idx.values(), default=-1) + 1
                pos_to_code = [""] * size
                for code, pos in cat_idx.items():
                    if pos < len(pos_to_code):
                        pos_to_code[pos] = code
            elif isinstance(cat_idx, list):
                pos_to_code = list(cat_idx)
            else:
                pos_to_code = []
            dim_codes.append(pos_to_code)
            # Some tables index the time axis POSITIONALLY ("0","1","2"…) and carry the real
            # period only in the category label. A code-only date lookup then parses nothing,
            # every observation is skipped, and a good 200 yields zero rows — which the fetcher
            # reports as a structural break. Keep the labels as a date fallback; the KEY still
            # uses codes, so no existing series_key changes.
            lab = cat.get("label", {})
            dim_labels.append([lab.get(c, "") if isinstance(lab, dict) else ""
                               for c in pos_to_code])
        # TIME DIMENSION: AUTHORITATIVE FIRST, HEURISTIC ONLY AS A LAST RESORT.
        #
        # This used to be `if time_dim_idx is None and is_time_dim(...)` inside the loop above —
        # FIRST MATCH WINS. is_time_dim answers True for any dimension where >=60% of a
        # FIVE-VALUE sample matches ^\d{4}[MQHSAW]?\d*$, and CSO classification dimensions are
        # full of numeric sentinel codes (3001, 9998, 9999 for "not stated"/"all"). So a
        # classification axis that merely appeared BEFORE the real TLIST axis was taken as time,
        # and its codes were parsed as years.
        #
        # Measured on the live store 2026-08-03: 434,408 of cso's 48,960,271 rows (0.887%),
        # across 11 files, carry an obs_date beyond the year 2100 — 272,445 in
        # 10_Census_2016.parquet alone, dated 9998-12-31. The keys show the mechanism plainly:
        #   CSO:B0726:...C02750V03319A=3001:...        -> 3001-12-31
        #   CSO:VSA10:TLIST(A1)=2019:STATISTIC=...     -> 2452-12-31
        # In the second, TLIST(A1)=2019 is sitting in the KEY, which is where a dimension goes
        # when it was NOT chosen as the time axis: the real year was right there and lost to an
        # earlier numeric dimension.
        #
        # Two passes fix it without touching is_time_dim's meaning. An explicitly named time
        # axis (TLIST*, TIME/YEAR/PERIOD/TID, or JSON-stat's own role.time) is authoritative and
        # cannot be outvoted by a coincidence of digits; the sample heuristic still runs, but
        # only for tables that name nothing. This is the same value-first principle the other
        # eight PxWeb ingesters already use via core/pxweb.resolve_time_dim.
        role_time = set((data.get("role") or {}).get("time") or [])
        for i, did in enumerate(dim_ids):
            u = did.upper()
            if did in role_time or u.startswith("TLIST") or u in ("TIME", "YEAR", "PERIOD", "TID"):
                time_dim_idx = i
                break
        if time_dim_idx is None:
            for i, did in enumerate(dim_ids):
                if is_time_dim(did, dim_codes[i]):
                    time_dim_idx = i
                    break

        if time_dim_idx is None:
            return results

        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        for flat_idx, raw_v in enumerate(values):
            if raw_v is None:
                continue
            try:
                v = float(raw_v)
                if v != v:
                    continue
            except (ValueError, TypeError):
                continue

            remainder = flat_idx
            dim_indices = []
            for stride in strides:
                dim_indices.append(remainder // stride)
                remainder %= stride

            t_pos = dim_indices[time_dim_idx]
            t_codes = dim_codes[time_dim_idx]
            if t_pos >= len(t_codes):
                continue
            obs_date = parse_date(t_codes[t_pos])
            if obs_date is None:
                # positional-index time codes: the period is in the label (see above)
                t_labels = dim_labels[time_dim_idx] if time_dim_idx < len(dim_labels) else []
                if t_pos < len(t_labels):
                    obs_date = parse_date(t_labels[t_pos])
            if obs_date is None:
                continue

            key_parts = [prefix]
            for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
                if i == time_dim_idx:
                    continue
                codes_for_dim = dim_codes[i]
                code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
                key_parts.append(f"{did}={code_val}")

            results.append((":".join(key_parts), obs_date, v))
    except Exception as e:
        log(f"  JSON-stat2 parse error: {e}")
    return results


def fetch_table(mtr_code: str) -> list[tuple[str, dt.date, float]]:
    url = f"{REST_BASE}/PxStat.Data.Cube_API.ReadDataset/{mtr_code}/JSON-stat/2.0/en"
    data = get_json(url)
    time.sleep(RATE)
    if not data:
        return []
    prefix = f"CSO:{mtr_code}"
    return parse_jsonstat2(data, prefix)


def main():
    os.makedirs(OUT, exist_ok=True)

    catalog = build_catalog()
    log(f"Processing {len(catalog)} CSO Ireland tables")

    # Group by subject
    by_subject: dict[str, list] = defaultdict(list)
    for t in catalog:
        sbj = f"{t['SbjCode']}_{t['SbjValue'][:30].replace(' ','_').replace('/','_')}"
        by_subject[sbj].append(t)

    log(f"Found {len(by_subject)} subjects")
    total_obs = 0

    for sbj_key in sorted(by_subject.keys(), key=lambda x: int(x.split("_")[0])):
        tables = by_subject[sbj_key]
        out_path = os.path.join(OUT, f"{sbj_key[:50]}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {sbj_key[:40]}: {n:,} rows")
            total_obs += n
            continue

        log(f"  Subject {sbj_key[:40]}: {len(tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(tables):
            mtr = t["MtrCode"]
            if not mtr:
                continue
            try:
                rows = fetch_table(mtr)
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        all_keys.append(key)
                        all_dates.append(d)
                        all_vals.append(v)
                        n += 1
                if n > 0:
                    log(f"    [{i+1}/{len(tables)}] {mtr}: {n:,} obs")
            except Exception as e:
                log(f"    [{i+1}] {mtr} ERR: {e}")

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {sbj_key[:40]}: {n:,} obs saved")
            total_obs += n
        else:
            log(f"  {sbj_key[:40]}: 0 obs")

    log(f"DONE: {total_obs:,} total CSO Ireland observations")


if __name__ == "__main__":
    main()
