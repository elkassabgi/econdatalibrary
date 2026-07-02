#!/usr/bin/env python3
"""Generic SDMX 2.1 REST connector for national statistical offices / central banks.

All providers are keyless open data. Tries SDMX-CSV first (fast), falls back to
SDMX generic XML (slower but universal). One Parquet per dataflow; fully resumable.

Providers:
  istat      — ISTAT Italy      https://esploradati.istat.it/SDMXWS/rest/
  bundesbank — Bundesbank DE    https://api.statistiken.bundesbank.de/rest/
  norgesbank — Norges Bank NO   https://data.norges-bank.no/api/
  ksh        — KSH Hungary      https://sdmx.ksh.hu/sdmx/rest/
  stats_nz   — Stats NZ         https://api.stats.govt.nz/sdmx/v1/

Run: python jobs/ingest_sdmx_nso.py <provider>
     python jobs/ingest_sdmx_nso.py <provider> --list        # print flows, no download
     python jobs/ingest_sdmx_nso.py <provider> --only ID1,ID2
"""
from __future__ import annotations
import csv, datetime as dt, io, os, sys, time, xml.etree.ElementTree as ET
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept-Encoding": "gzip, deflate"}

NS = {
    "mes":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "gen":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}

PROVIDERS = {
    "istat": {
        # sdmx.istat.it = ISTAT's older NSI host (same SDMX 2.1 REST dialect).
        # Switched 2026-06-10: esploradati.istat.it has been timing out for
        # days; the old host answers in ~3s (endpoint used by the istatapi
        # library, github.com/Attol8/istatapi). Serves 1,020 dataflows.
        "base":    "https://sdmx.istat.it/SDMXWS/rest/",
        "agency":  "IT1",
        "name":    "ISTAT Italy",
        "license": "CC BY 3.0 IT",
        "rate":    1.5,    # seconds between requests
        "timeout": 300,
    },
    "istat_esploradati": {
        # ISTAT's NEW portal (4,874 flows incl. granular DF_* datasets beyond
        # the 509 classical flows on sdmx.istat.it). Shares out_key "istat" so
        # already-downloaded flows are skipped. Chronically slow/flaky host —
        # recovered 2026-06-12 after multi-day outage.
        "base":    "https://esploradati.istat.it/SDMXWS/rest/",
        "agency":  "IT1",
        "name":    "ISTAT Italy (esploradati complementary sweep)",
        "license": "CC BY 3.0 IT",
        "rate":    1.5,
        "timeout": 300,
        "out_key": "istat",
    },
    "unicef": {
        # Added 2026-06-11 (Ahmed's lead): UNICEF Indicator Data Warehouse.
        # Multi-agency registry — flows carry their own agencyID (CD2030,
        # ECARO, ...); catalog enumerated via dataflow/all. Verified live:
        # data/CD2030,CDCOV/ -> 200, 11.2 MB SDMX-CSV.
        # Docs: https://data.unicef.org/sdmx-api-documentation/
        "base":    "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/",
        "agency":  "all",
        "name":    "UNICEF Data Warehouse",
        "license": "CC BY 3.0 IGO (some flows CC BY-NC per docs)",
        "rate":    1.0,
        "timeout": 300,
    },
    "bundesbank": {
        "base":    "https://www.bundesbank.de/sdmx-dl/v3/",  # sdmx-dl endpoint (used by R sdmx1 pkg)
        "agency":  "BBK",
        "name":    "Deutsche Bundesbank",
        "license": "DL-DE-BY-2.0",
        "rate":    2.0,
        "timeout": 300,
    },
    "idb": {
        "base":    "https://data.iadb.org/api/",
        "agency":  "IDB",
        "name":    "Inter-American Development Bank",
        "license": "CC BY",
        "rate":    1.5,
        "timeout": 300,
    },
    "adb": {
        "base":    "https://kidb.adb.org/api/v1/",
        "agency":  "ADB",
        "name":    "Asian Development Bank",
        "license": "CC BY 4.0",
        "rate":    2.0,
        "timeout": 300,
    },
    "norgesbank": {
        "base":    "https://data.norges-bank.no/api/",
        "agency":  "NB",
        "name":    "Norges Bank",
        "license": "CC BY 4.0",
        "rate":    1.0,
        "timeout": 180,
    },
    "ksh": {
        "base":    "https://sdmx.ksh.hu/sdmx/rest/",
        "agency":  "HU1",
        "name":    "KSH Hungary",
        "license": "CC BY 4.0",
        "rate":    2.0,
        "timeout": 300,
    },
    "stats_nz": {
        "base":    "https://api.stats.govt.nz/sdmx/v1/",
        "agency":  "NZ1",
        "name":    "Stats NZ",
        "license": "CC BY 4.0",
        "rate":    1.0,
        "timeout": 300,
    },
    "gus": {
        "base":    "https://sdmx.stat.gov.pl/SdmxRestWs/rest/",
        "agency":  "PL1",
        "name":    "GUS Statistics Poland",
        "license": "CC BY 4.0",
        "rate":    2.0,
        "timeout": 300,
    },
    "stat_austria": {
        "base":    "https://data.statistik.gv.at/ogd/sdmx/",
        "agency":  "STAT",
        "name":    "Statistics Austria",
        "license": "CC BY 4.0",
        "rate":    2.0,
        "timeout": 300,
    },
    "insee_sdmx": {
        "base":    "https://bdm.insee.fr/series/sdmx/",
        "agency":  "FR1",
        "name":    "INSEE France (SDMX)",
        "license": "Open Government France",
        "rate":    1.5,
        "timeout": 300,
    },
    "bis": {
        "base":    "https://stats.bis.org/api/v1/",
        "agency":  "BIS",
        "name":    "Bank for International Settlements",
        "license": "CC BY-NC 4.0",
        "rate":    2.0,
        "timeout": 600,
    },
    "ilo": {
        "base":    "https://sdmx.ilo.org/rest/",
        "agency":  "ILO",
        "name":    "International Labour Organization",
        "license": "CC BY 4.0",
        "rate":    1.5,
        "timeout": 600,
        "csv_accept": "text/csv",
        "xml_accept": "application/xml",
    },
    "eurostat": {
        "base":        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/",
        "agency":      "ESTAT",
        "name":        "Eurostat",
        "license":     "CC BY 4.0",
        "rate":        1.0,
        "timeout":     600,
        # Eurostat uses ?format=SDMX-CSV param, not Accept header
        "data_params":        "?format=SDMX-CSV",
        "csv_accept":         "text/html,*/*",   # neutral - Eurostat ignores Accept
        "xml_accept":         "text/html,*/*",
    },
    "ecb_sdmx": {
        "base":           "https://data-api.ecb.europa.eu/service/",
        "agency":         "ECB",
        "name":           "European Central Bank",
        "license":        "CC BY 4.0",
        "rate":           1.0,
        "timeout":        600,
        # ECB requires at least one query param; trailing slash → 404
        # ECB doesn't accept sdmx mime types, use text/csv
        "data_params":        "?startPeriod=1900-01-01",
        "no_trailing_slash":  True,
        "csv_accept":         "text/csv",
        "xml_accept":         "application/xml",
    },
}


def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def parse_sdmx_period(s: str) -> dt.date | None:
    """Convert SDMX TIME_PERIOD codes to date.
    Annual YYYY → Dec 31; Monthly YYYY-MM → 1st; Quarterly → first month;
    Semi-annual S1/S2; Weekly W; Daily ISO.
    """
    s = (s or "").strip()
    try:
        if len(s) == 4 and s.isdigit():
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] == "-":
            if s[5] == "Q":  # YYYY-Q1
                m = (int(s[6]) - 1) * 3 + 1
                return dt.date(int(s[:4]), m, 1)
            if s[5] == "S":  # YYYY-S1
                m = 1 if s[6] == "1" else 7
                return dt.date(int(s[:4]), m, 1)
            if s[5:].isdigit():  # YYYY-MM
                return dt.date(int(s[:4]), int(s[5:]), 1)
        if len(s) == 8 and s[4] == "-" and s[6] == "W":  # YYYY-Www
            return dt.date.fromisocalendar(int(s[:4]), int(s[7:]), 1)
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return dt.date.fromisoformat(s)
        # year only with extra chars e.g. "2022-A1"
        if "-A" in s:
            return dt.date(int(s[:4]), 12, 31)
    except Exception:
        pass
    return None


# ──────────────────────────────── HTTP ──────────────────────────────────────

def http_get(url: str, accept: str, timeout: int, retries: int = 4) -> bytes | None:
    hdrs = {**UA, "Accept": accept}
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=hdrs, timeout=timeout)
            if r.status_code == 200:
                return r.content
            if r.status_code in (400, 404, 413):
                log(f"  HTTP {r.status_code}: {url[-80:]}")
                return None
            if r.status_code == 429:
                log(f"  429 rate limit, sleeping 60s")
                time.sleep(60)
                continue
            log(f"  HTTP {r.status_code} attempt {attempt+1}: {url[-80:]}")
        except Exception as e:
            log(f"  ERR attempt {attempt+1}: {e}")
        time.sleep(10 * (attempt + 1))
    return None


# ──────────────────────────────── Dataflow catalog ─────────────────────────

def get_dataflows(base: str, agency: str, timeout: int) -> list[dict]:
    """Enumerate all dataflows for this agency via SDMX Structure message."""
    url = f"{base}dataflow/{agency}"
    accept = "application/vnd.sdmx.structure+xml;version=2.1"
    raw = http_get(url, accept, timeout)
    if not raw:
        # try without agency
        url2 = f"{base}dataflow/all"
        raw = http_get(url2, accept, timeout)
    if not raw:
        return []
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        log(f"  XML parse error on dataflows: {e}")
        return []
    flows = []
    for f in root.findall(".//str:Dataflow", NS):
        fid = f.get("id", "")
        ag  = f.get("agencyID", agency)
        ver = f.get("version", "1.0")
        nm  = f.find("com:Name", NS)
        flows.append({
            "id": fid, "agency": ag, "version": ver,
            "name": nm.text if nm is not None else fid,
        })
    return flows


# ──────────────────────────────── CSV parsing ─────────────────────────────

def _build_series_key(row: dict, skip_cols: set[str]) -> str:
    """Build series key from all dimension columns (excludes time/obs/dataflow)."""
    return ":".join(f"{k}={v}" for k, v in row.items()
                    if k not in skip_cols and v)


def parse_sdmx_csv(content: bytes) -> tuple[list, list, list]:
    """Parse SDMX-CSV; return (keys, dates, values) lists."""
    keys, dates, vals = [], [], []
    try:
        text = content.decode("utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return [], [], []
        cols = [c.upper() for c in reader.fieldnames]
        # locate time and obs columns
        time_col  = next((reader.fieldnames[i] for i, c in enumerate(cols)
                          if c in ("TIME_PERIOD", "TIME", "PERIOD", "DATE")), None)
        obs_col   = next((reader.fieldnames[i] for i, c in enumerate(cols)
                          if c in ("OBS_VALUE", "VALUE", "OBSVALUE")), None)
        # series key: prefer pre-built KEY/DATAFLOW col
        key_col   = next((reader.fieldnames[i] for i, c in enumerate(cols)
                          if c in ("KEY", "SERIES_KEY")), None)
        if not time_col or not obs_col:
            return [], [], []
        skip = {time_col, obs_col, "DATAFLOW", "STRUCTURE", "STRUCTURE_ID",
                "ACTION", "LOCALE", "COMMENT", "OBS_STATUS", "OBS_CONF",
                "OBS_PRE_BREAK", "OBS_COM", "TIME_FORMAT", "LAST_UPDATE",
                "COLLECTION", "TITLE", "UNIT_MEASURE", "UNIT_MULT",
                "DECIMALS", "SOURCE_AGENCY"}
        for row in reader:
            raw_v = row.get(obs_col, "")
            if not raw_v or raw_v in ("", "NaN", "nan", "NA", "N/A", ".", "..."):
                continue
            try:
                v = float(raw_v)
            except ValueError:
                continue
            d = parse_sdmx_period(row.get(time_col, ""))
            if d is None:
                continue
            if key_col and row.get(key_col):
                k = row[key_col]
            else:
                k = _build_series_key(row, skip)
            keys.append(k); dates.append(d); vals.append(v)
    except Exception as e:
        log(f"  CSV parse error: {e}")
    return keys, dates, vals


# ──────────────────────────────── XML parsing ─────────────────────────────

def parse_sdmx_xml(content: bytes) -> tuple[list, list, list]:
    """Parse SDMX 2.1 generic or compact XML data message.
    Returns (keys, dates, values).
    """
    keys, dates, vals = [], [], []
    # ElementTree overflows on >2GB documents (OverflowError) and giant trees
    # can exhaust RAM — cap the fallback at 800MB; oversized flows are logged
    # and skipped (revisit with dimension-sliced queries if needed).
    if len(content) > 800_000_000:
        log(f"  XML too large to parse safely ({len(content)/1e6:.0f} MB), skipping")
        return [], [], []
    try:
        root = ET.fromstring(content)
    except (ET.ParseError, OverflowError, MemoryError, ValueError) as e:
        log(f"  XML parse error: {e}")
        return [], [], []

    # Generic data format
    gen_pfx = "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}"
    for series in root.findall(f".//{gen_pfx}Series"):
        # Build key from SeriesKey values
        key_parts = []
        for v in series.findall(f"{gen_pfx}SeriesKey/{gen_pfx}Value"):
            key_parts.append(f"{v.get('id')}={v.get('value', '')}")
        series_key = ":".join(key_parts)
        for obs in series.findall(f"{gen_pfx}Obs"):
            tdim = obs.find(f"{gen_pfx}ObsDimension")
            tval_el = obs.find(f"{gen_pfx}ObsValue")
            if tdim is None or tval_el is None:
                continue
            d = parse_sdmx_period(tdim.get("value", ""))
            if d is None:
                continue
            raw_v = tval_el.get("value", "")
            try:
                v = float(raw_v)
            except ValueError:
                continue
            keys.append(series_key); dates.append(d); vals.append(v)

    if not keys:
        # StructureSpecific / compact format: all dims in <Obs> attributes
        for obs in root.iter():
            if "Obs" not in obs.tag:
                continue
            attrs = obs.attrib
            time_val = (attrs.get("TIME_PERIOD") or attrs.get("TIME") or
                        attrs.get("PERIOD") or attrs.get("Date", ""))
            obs_val  = (attrs.get("OBS_VALUE") or attrs.get("VALUE") or
                        attrs.get("ObsValue", ""))
            if not time_val or not obs_val:
                continue
            d = parse_sdmx_period(time_val)
            if d is None:
                continue
            try:
                v = float(obs_val)
            except ValueError:
                continue
            skip_attr = {"OBS_VALUE", "VALUE", "ObsValue", "TIME_PERIOD",
                         "TIME", "PERIOD", "Date", "OBS_STATUS", "OBS_CONF",
                         "OBS_COM", "LAST_UPDATE", "UNIT_MEASURE"}
            k = ":".join(f"{kk}={vv}" for kk, vv in attrs.items()
                         if kk not in skip_attr and vv)
            keys.append(k); dates.append(d); vals.append(v)
    return keys, dates, vals


# ──────────────────────────────── Per-flow download ───────────────────────

def ingest_flow(base: str, agency: str, flow: dict,
                out_dir: str, rate: float, timeout: int,
                data_params: str = "", no_trailing_slash: bool = False,
                csv_accept: str = "", xml_accept: str = "") -> int:
    """Download one SDMX dataflow and save as Parquet. Returns obs count."""
    flow_id  = flow["id"]
    out_path = os.path.join(out_dir, f"{flow_id}.parquet")
    if os.path.exists(out_path):
        n = pq.read_metadata(out_path).num_rows
        log(f"  skip {flow_id} ({n:,} rows)")
        return n

    # Try CSV format first
    slug = f"{agency},{flow_id}" if agency else flow_id
    trail = "" if no_trailing_slash else "/"
    data_url = f"{base}data/{slug}{trail}{data_params}"
    _csv_accept = csv_accept or "application/vnd.sdmx.data+csv;version=1.0.0"
    content = http_get(data_url, _csv_accept, timeout)
    keys, dates, obs_vals = [], [], []
    if content:
        # Detect if response is XML (fallback from server) or CSV
        head = content[:100].lstrip()
        if head.startswith(b"<") or head.startswith(b"<?xml"):
            keys, dates, obs_vals = parse_sdmx_xml(content)
        else:
            keys, dates, obs_vals = parse_sdmx_csv(content)

    if not obs_vals:
        # Fallback: try generic XML
        _xml_accept = xml_accept or "application/vnd.sdmx.genericdata+xml;version=2.1"
        content_xml = http_get(data_url, _xml_accept, timeout)
        if content_xml:
            keys, dates, obs_vals = parse_sdmx_xml(content_xml)

    if not obs_vals:
        log(f"  {flow_id}: 0 obs")
        time.sleep(rate)
        return 0

    # Defensive: truncate to shortest array to guard against parser edge-case mismatches
    min_len = min(len(keys), len(dates), len(obs_vals))
    if min_len < max(len(keys), len(dates), len(obs_vals)):
        log(f"  {flow_id}: WARNING array length mismatch keys={len(keys)} dates={len(dates)} vals={len(obs_vals)}, truncating to {min_len}")
        keys     = keys[:min_len]
        dates    = dates[:min_len]
        obs_vals = obs_vals[:min_len]

    tbl = pa.table({
        "series_key": pa.array(keys,     pa.string()),
        "obs_date":   pa.array(dates,    pa.date32()),
        "value":      pa.array(obs_vals, pa.float64()),
    })
    pq.write_table(tbl, out_path, compression="zstd")
    n = pq.read_metadata(out_path).num_rows
    log(f"  {flow_id}: {n:,} obs  [{flow.get('name','')[:60]}]")
    time.sleep(rate)
    return n


# ──────────────────────────────── Main ────────────────────────────────────

def main():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__); sys.exit(0)

    provider_key = args[0].lower()
    if provider_key not in PROVIDERS:
        print(f"Unknown provider: {provider_key}"); print("Available:", list(PROVIDERS)); sys.exit(1)

    only_ids   = set()
    list_only  = False
    for a in args[1:]:
        if a == "--list": list_only = True
        elif a.startswith("--only"):
            ids = a.split("=", 1)[-1] if "=" in a else (args[args.index(a)+1] if args.index(a)+1 < len(args) else "")
            only_ids = set(ids.split(","))

    cfg     = PROVIDERS[provider_key]
    base    = cfg["base"]
    agency  = cfg["agency"]
    rate    = cfg["rate"]
    timeout = cfg["timeout"]
    data_params       = cfg.get("data_params", "")
    no_trailing_slash = cfg.get("no_trailing_slash", False)
    # out_key lets two provider entries share one output dir (e.g. the two
    # ISTAT hosts) so the skip-existing logic dedupes across them.
    out_dir = os.path.join(ROOT, "data", "clean_full", cfg.get("out_key", provider_key))
    os.makedirs(out_dir, exist_ok=True)

    log(f"Provider: {cfg['name']} | License: {cfg['license']}")
    log("Fetching dataflow catalog...")
    flows = get_dataflows(base, agency, timeout)
    log(f"Found {len(flows)} dataflows")

    if list_only:
        for f in flows:
            print(f"  {f['id']:40s}  {f['name']}")
        return

    if only_ids:
        flows = [f for f in flows if f["id"] in only_ids]
        log(f"Filtered to {len(flows)} flows")

    # Deferred flows (env SDMX_SKIP_FLOWS="CSEC,..."): oversized datasets that
    # need a dedicated dimension-sliced pass — skipped here, tracked in the
    # gap list, NOT dropped.
    skip_ids = set(filter(None, os.environ.get("SDMX_SKIP_FLOWS", "").split(",")))
    if skip_ids:
        n0 = len(flows)
        flows = [f for f in flows if f["id"] not in skip_ids]
        log(f"Deferring {n0 - len(flows)} flow(s) via SDMX_SKIP_FLOWS: {sorted(skip_ids)}")

    csv_accept = cfg.get("csv_accept", "")
    xml_accept = cfg.get("xml_accept", "")

    total = 0
    for i, flow in enumerate(flows, 1):
        log(f"[{i}/{len(flows)}] {flow['id']}")
        # Use the flow's own agencyID (multi-agency registries like UNICEF);
        # falls back to the provider-level agency for single-agency hosts.
        total += ingest_flow(base, flow.get("agency") or agency, flow, out_dir, rate, timeout,
                             data_params=data_params,
                             no_trailing_slash=no_trailing_slash,
                             csv_accept=csv_accept,
                             xml_accept=xml_accept)

    log(f"DONE: {total:,} observations from {cfg['name']}")


if __name__ == "__main__":
    main()
