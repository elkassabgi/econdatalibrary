#!/usr/bin/env python3
"""Deutsche Bundesbank SDMX REST connector — keyless open data.

License: DL-DE-BY-2.0
Source: https://api.statistiken.bundesbank.de/rest/data/{flowID}
No API key required.

Available flows discovered via probe (catalog endpoint not supported):
  BBEX3  — Exchange rates (EUR vs. all currencies) ~2M obs
  BBNZ1  — Securities statistics
  BBDP1  — Balance of payments
  BBSIS  — Stock market indices
  BBFI1  — Public finance / government statistics I
  BBFI3  — Public finance / government statistics III
  BBBP1  — Banking system / bank position statistics

Note: API ignores startPeriod/endPeriod for XML format — returns full history.
      Responses can be 1+ GB. Uses streaming iterparse for memory efficiency.

Run: python jobs/ingest_bundesbank.py
"""
from __future__ import annotations
import datetime as dt, os, time, io
import xml.etree.ElementTree as ET
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
OUT  = os.path.join(ROOT, "data", "clean_full", "bundesbank")
BASE = "https://api.statistiken.bundesbank.de/rest/data"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
        "Accept": "application/xml"}
RATE = 2.0  # seconds between requests (be polite)

# XML namespaces
NS_GENERIC = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
NS_MESSAGE = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"

# Flows confirmed working via HTTP probe
FLOWS = [
    "BBEX3",   # Exchange rates (~2M obs, ~1.3 GB XML)
    "BBNZ1",   # Securities statistics
    "BBDP1",   # Balance of payments
    "BBSIS",   # Stock market indices
    "BBFI1",   # Public finance I
    "BBFI3",   # Public finance III
    "BBBP1",   # Banking system
]

LOG_FILE = os.path.join(OUT, "_bundesbank_log.txt")


def log(m):
    msg = f"[{time.strftime('%H:%M:%S')}] {m}"
    print(msg, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def parse_bbk_date(s: str) -> dt.date | None:
    """Parse Bundesbank time period to date."""
    s = s.strip()
    try:
        if len(s) == 4 and s.isdigit():                              # Annual: 2023
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-" and s[5:].isdigit():         # Monthly: 2023-01
            return dt.date(int(s[:4]), int(s[5:7]), 1)
        if len(s) == 7 and s[4] == "-" and s[5] == "Q":             # Quarterly: 2023-Q1
            yr, q = int(s[:4]), int(s[6])
            return dt.date(yr, (q - 1) * 3 + 1, 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":            # Daily: 2023-01-15
            return dt.date.fromisoformat(s)
        if len(s) == 8 and s[4] == "-" and s[5] == "W":             # Weekly: 2023-W01
            return dt.date.fromisocalendar(int(s[:4]), int(s[6:8]), 1)
    except (ValueError, IndexError):
        pass
    return None


def download_flow(flow_id: str) -> list[tuple[str, dt.date, float]]:
    """Stream-download and iterparse SDMX Generic XML for one flow."""
    url = f"{BASE}/{flow_id}"
    log(f"  Fetching {flow_id} ...")

    for attempt in range(3):
        try:
            r = requests.get(url, headers=UA, timeout=600, stream=True)
            if r.status_code == 200:
                break
            if r.status_code in (404, 400):
                log(f"  {flow_id}: HTTP {r.status_code} — skipping")
                return []
            log(f"  {flow_id}: HTTP {r.status_code} attempt {attempt+1}")
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            log(f"  {flow_id}: ERR attempt {attempt+1}: {e}")
            time.sleep(10 * (attempt + 1))
    else:
        return []

    # Stream-collect into bytes buffer
    chunks = []
    bytes_read = 0
    for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
        chunks.append(chunk)
        bytes_read += len(chunk)
        if bytes_read % (100 * 1024 * 1024) < 1024 * 1024:
            log(f"    {flow_id}: {bytes_read // (1024*1024)} MB downloaded...")
    xml_bytes = b"".join(chunks)
    log(f"  {flow_id}: downloaded {len(xml_bytes) // (1024*1024)} MB, parsing...")

    # Parse using iterparse (memory-efficient)
    results: list[tuple[str, dt.date, float]] = []
    tag_series    = f"{{{NS_GENERIC}}}Series"
    tag_serieskey = f"{{{NS_GENERIC}}}SeriesKey"
    tag_value     = f"{{{NS_GENERIC}}}Value"
    tag_obs       = f"{{{NS_GENERIC}}}Obs"
    tag_obsdim    = f"{{{NS_GENERIC}}}ObsDimension"
    tag_obsval    = f"{{{NS_GENERIC}}}ObsValue"

    current_key: str = ""
    current_obs_date: dt.date | None = None

    try:
        for event, elem in ET.iterparse(io.BytesIO(xml_bytes), events=("start", "end")):
            if event == "start":
                if elem.tag == tag_series:
                    current_key = ""
                elif elem.tag == tag_obs:
                    current_obs_date = None
                elif elem.tag == tag_obsdim:
                    d = parse_bbk_date(elem.get("value", ""))
                    if d:
                        current_obs_date = d
                elif elem.tag == tag_obsval:
                    if current_obs_date:
                        try:
                            v = float(elem.get("value", "nan"))
                            results.append((f"BBK:{flow_id}:{current_key}", current_obs_date, v))
                        except ValueError:
                            pass
                elif elem.tag == tag_value and current_key == "":
                    # Inside SeriesKey — build the key string
                    pass  # handled in 'end' event for value

            elif event == "end":
                if elem.tag == tag_series:
                    elem.clear()  # free memory
                elif elem.tag == tag_serieskey:
                    # Build key from all child Value elements
                    kv_parts = [
                        f"{v.get('id', '')}={v.get('value', '')}"
                        for v in elem
                        if v.tag == tag_value
                    ]
                    current_key = ":".join(kv_parts)
    except ET.ParseError as e:
        log(f"  {flow_id}: XML parse error: {e}")

    log(f"  {flow_id}: parsed {len(results):,} observations")
    return results


def main():
    os.makedirs(OUT, exist_ok=True)

    # Delete old small parquets (from broken regex run) and re-download
    for flow_id in FLOWS:
        fp = os.path.join(OUT, f"{flow_id}.parquet")
        if os.path.exists(fp):
            try:
                m = pq.read_metadata(fp)
                if m.num_rows < 1000:  # clearly broken (regex got only 10-330 rows)
                    os.remove(fp)
                    log(f"  Removed stub {flow_id}.parquet ({m.num_rows} rows)")
            except Exception:
                os.remove(fp)

    done = {f[:-8] for f in os.listdir(OUT) if f.endswith(".parquet")}
    todo = [f for f in FLOWS if f not in done]
    log(f"Bundesbank: {len(todo)}/{len(FLOWS)} flows to fetch ({len(done)} done)")

    total = 0
    for i, flow_id in enumerate(todo, 1):
        log(f"[{i}/{len(todo)}] {flow_id}")
        rows = download_flow(flow_id)
        if rows:
            keys  = [r[0] for r in rows]
            dates = [r[1] for r in rows]
            vals  = [r[2] for r in rows]
            tbl = pa.table({
                "series_key": pa.array(keys,  pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals,  pa.float64()),
            })
            out_path = os.path.join(OUT, f"{flow_id}.parquet")
            pq.write_table(tbl, out_path, compression="zstd")
            total += len(rows)
            log(f"  {flow_id}: saved {len(rows):,} obs → {out_path}")
        time.sleep(RATE)

    log(f"DONE: {total:,} total Bundesbank observations across {len(todo)} flows")


if __name__ == "__main__":
    main()
