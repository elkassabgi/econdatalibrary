#!/usr/bin/env python3
"""Asian Development Bank — KIDB (Key Indicators Database), SDMX 3.0 API.

License: ADB open data terms (attribution)
Source: https://kidb.adb.org/
No API key required.

Coverage: 62 dataflows (EO_* economic outlook / national accounts, PPL_* people,
MFP_* money-finance-prices, GG_* government, EGELC_* energy, ENV_*, ...),
annual (plus Q/M where annual is empty), all ADB member economies.

Endpoints:
  * Dataflows:  GET /v4/sdmx/structure/dataflow/all/all/        (SDMX 3.0 XML)
  * Indicators: GET /dataflow/indicators/{FLOW_ID}              (JSON list)
  * Data:       GET /v4/sdmx/data/ADB,{FLOW}/A.{IND}.?format=sdmx-csv
        Full-flow dumps (A..) return 504 — per-indicator with a trailing dot
        (= all economies) is the granularity that works. On 422/504 falls back
        to per-economy iteration using economy codes already harvested; if none
        are known yet, logs and skips. Tries frequency A first; if a whole flow
        yields zero observations, retries once with Q, then M.

Output: one parquet per dataflow in data/clean_full/adb/, long format
        {series_key, obs_date, value}; resumable (skips existing parquets,
        flags empty flows under _empty/).
series_key: ADB:{FLOW}:{INDICATOR}:{ECONOMY_CODE}
obs_date:   2023 → Dec 31; 2023-Q1 → quarter start; 2023-01 → month start.

Run: python jobs/ingest_adb_kidb.py
"""
from __future__ import annotations
import csv, datetime as dt, io, json, os, re, time
import xml.etree.ElementTree as ET
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "adb")
EMPTY_DIR = os.path.join(OUT, "_empty")
BASE = "https://kidb.adb.org/api"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 1.0
FLOWS_FILE = os.path.join(OUT, "_dataflows.json")
ECON_FILE  = os.path.join(OUT, "_economies.json")


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def fetch(url: str, retries: int = 3, timeout: int = 240) -> requests.Response | None:
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=timeout)
            last = r
            if r.status_code == 200:
                return r
            if r.status_code in (400, 404, 422):
                return r                      # caller decides; no point retrying
            if r.status_code == 429:
                log("  429 throttle, sleeping 60s")
                time.sleep(60)
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(5 * (attempt + 1))
                continue
            log(f"  HTTP {r.status_code}: {url[-90:]}")
        except Exception as e:
            log(f"  ERR: {e} on {url[-90:]}")
            time.sleep(5 * (attempt + 1))
    return last


# ---------------------------------------------------------------- structures
def get_dataflows() -> list[tuple[str, str]]:
    if os.path.exists(FLOWS_FILE):
        with open(FLOWS_FILE, encoding="utf-8") as f:
            flows = [tuple(x) for x in json.load(f)]
        log(f"Loaded dataflow list: {len(flows)} flows")
        return flows
    r = fetch(f"{BASE}/v4/sdmx/structure/dataflow/all/all/")
    if r is None or r.status_code != 200:
        log(f"FATAL: dataflow list HTTP {r.status_code if r else 'EXC'}")
        return []
    flows = []
    try:
        root = ET.fromstring(r.content)
        for el in root.iter():
            if el.tag.split("}")[-1] == "Dataflow" and el.attrib.get("id"):
                flows.append((el.attrib["id"], el.attrib.get("agencyID", "ADB")))
    except ET.ParseError as e:
        log(f"FATAL: dataflow XML parse error: {e}")
        return []
    flows = sorted(set(flows))
    os.makedirs(OUT, exist_ok=True)
    with open(FLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(flows, f)
    log(f"Dataflows: {len(flows)}")
    return flows


def get_indicators(flow: str) -> list[str]:
    r = fetch(f"{BASE}/dataflow/indicators/{flow}")
    time.sleep(RATE)
    if r is None or r.status_code != 200:
        log(f"  indicators/{flow}: HTTP {r.status_code if r else 'EXC'}")
        return []
    try:
        j = r.json()
    except ValueError:
        log(f"  indicators/{flow}: not JSON")
        return []
    # extract code-like fields defensively: prefer 'code', then 'indicatorCode', then 'id'
    found: dict[str, list[str]] = {"code": [], "indicatorCode": [], "id": []}

    def walk(o):
        if isinstance(o, dict):
            for k in ("code", "indicatorCode", "id"):
                v = o.get(k)
                if isinstance(v, str) and v.strip():
                    found[k].append(v.strip())
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(j)
    codes = found["code"] or found["indicatorCode"] or found["id"]
    out, seen = [], set()
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ---------------------------------------------------------------- data
def parse_tp(s: str) -> dt.date | None:
    t = (s or "").strip()
    try:
        m = re.fullmatch(r"(\d{4})", t)
        if m:
            return dt.date(int(m.group(1)), 12, 31)
        m = re.fullmatch(r"(\d{4})-?Q([1-4])", t, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1, 1)
        m = re.fullmatch(r"(\d{4})[-M](\d{1,2})", t, re.IGNORECASE)
        if m and 1 <= int(m.group(2)) <= 12:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", t)
        if m:
            return dt.date.fromisoformat(t)
    except ValueError:
        return None
    return None


def parse_sdmx_csv(text: str, flow: str, fallback_ind: str,
                   economies: set[str]) -> list[tuple[str, dt.date, float]]:
    out = []
    try:
        rdr = csv.DictReader(io.StringIO(text))
        if not rdr.fieldnames:
            return out
        cols = {c.upper().strip(): c for c in rdr.fieldnames if c}
        c_tp  = cols.get("TIME_PERIOD")
        c_val = cols.get("OBS_VALUE") or cols.get("VALUE")
        c_ind = cols.get("INDICATOR") or cols.get("INDICATOR_CODE") or cols.get("SERIES")
        c_eco = (cols.get("ECONOMY_CODE") or cols.get("ECONOMY") or
                 cols.get("REF_AREA") or cols.get("COUNTRY"))
        if not c_tp or not c_val:
            return out
        for row in rdr:
            d = parse_tp(row.get(c_tp, ""))
            if d is None:
                continue
            raw = (row.get(c_val) or "").strip()
            if not raw:
                continue
            try:
                v = float(raw)
            except ValueError:
                continue
            if v != v:
                continue
            ind = (row.get(c_ind) or "").strip() if c_ind else ""
            eco = (row.get(c_eco) or "").strip() if c_eco else ""
            if eco:
                economies.add(eco)
            out.append((f"ADB:{flow}:{ind or fallback_ind}:{eco}", d, v))
    except Exception as e:
        log(f"    CSV parse error ({flow}/{fallback_ind}): {e}")
    return out


def harvest_indicator(agency: str, flow: str, freq: str, code: str,
                      economies: set[str]) -> list[tuple[str, dt.date, float]]:
    url = f"{BASE}/v4/sdmx/data/{agency},{flow}/{freq}.{code}.?format=sdmx-csv"
    r = fetch(url, retries=2)
    time.sleep(RATE)
    if r is not None and r.status_code == 200:
        return parse_sdmx_csv(r.text, flow, code, economies)
    sc = r.status_code if r is not None else "EXC"
    if sc == 404:
        return []
    if sc in (422, 500, 504, "EXC") and economies:
        log(f"    {code}: HTTP {sc} → per-economy fallback ({len(economies)} economies)")
        rows = []
        for eco in sorted(economies):
            r2 = fetch(f"{BASE}/v4/sdmx/data/{agency},{flow}/{freq}.{code}.{eco}?format=sdmx-csv",
                       retries=1, timeout=120)
            time.sleep(RATE)
            if r2 is not None and r2.status_code == 200:
                rows.extend(parse_sdmx_csv(r2.text, flow, code, economies))
        return rows
    log(f"    {code}: HTTP {sc}, skipped")
    return []


def main():
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(EMPTY_DIR, exist_ok=True)

    economies: set[str] = set()
    if os.path.exists(ECON_FILE):
        try:
            with open(ECON_FILE, encoding="utf-8") as f:
                economies = set(json.load(f))
        except Exception:
            pass

    flows = get_dataflows()
    if not flows:
        return
    log(f"Processing {len(flows)} ADB KIDB dataflows")

    total_obs = 0
    for fi, (flow, agency) in enumerate(flows):
        out_path = os.path.join(OUT, f"{flow}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"[{fi+1}/{len(flows)}] Skip {flow}: {n:,} rows")
            total_obs += n
            continue
        empty_flag = os.path.join(EMPTY_DIR, f"{flow}.txt")
        if os.path.exists(empty_flag):
            log(f"[{fi+1}/{len(flows)}] Skip {flow}: flagged empty")
            continue

        codes = get_indicators(flow)
        if not codes:
            log(f"[{fi+1}/{len(flows)}] {flow}: no indicators found, flagging empty")
            with open(empty_flag, "w") as f:
                f.write("no indicators\n")
            continue
        log(f"[{fi+1}/{len(flows)}] {flow} ({agency}): {len(codes)} indicators")

        rows_all: list[tuple] = []
        seen: set[tuple] = set()
        used_freq = None
        for freq in ("A", "Q", "M"):
            for ci, code in enumerate(codes):
                try:
                    rows = harvest_indicator(agency, flow, freq, code, economies)
                except Exception as e:
                    log(f"    {code} ERR: {e}")
                    continue
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        rows_all.append((key, d, v))
                        n += 1
                if n:
                    log(f"    [{ci+1}/{len(codes)}] {freq}.{code}: {n:,} obs")
            if rows_all:
                used_freq = freq
                break
            log(f"  {flow}: 0 obs at freq {freq}" +
                (", trying next frequency" if freq != "M" else ""))

        try:
            with open(ECON_FILE + ".tmp", "w", encoding="utf-8") as f:
                json.dump(sorted(economies), f)
            os.replace(ECON_FILE + ".tmp", ECON_FILE)
        except Exception:
            pass

        if rows_all:
            tbl = pa.table({
                "series_key": pa.array([r[0] for r in rows_all], pa.string()),
                "obs_date":   pa.array([r[1] for r in rows_all], pa.date32()),
                "value":      pa.array([r[2] for r in rows_all], pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {flow}: {n:,} obs saved (freq {used_freq})")
            total_obs += n
        else:
            with open(empty_flag, "w") as f:
                f.write("0 observations at A, Q, M\n")
            log(f"  {flow}: 0 obs at all frequencies, flagged")

    log(f"DONE: {total_obs:,} total ADB KIDB observations")


if __name__ == "__main__":
    main()
