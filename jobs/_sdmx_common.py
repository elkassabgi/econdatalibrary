"""Shared SDMX 2.1 parsing helpers.

Extracted 2026-09-08 from the ingester that used to own them, so that the sources which
reuse them keep working after that ingester was removed. The parsing logic is byte-for-byte
what produced the existing rows: period parsing, the SDMX-CSV reader and the SDMX-ML reader,
plus the polite user agent and the XML namespace map.

Import these rather than copying them -- two divergent copies of a period parser is how a
source silently changes its own series keys.
"""
import csv
import io
import re
import xml.etree.ElementTree as ET


NS = {
    "mes":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "gen":  "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}


UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept-Encoding": "gzip, deflate"}


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