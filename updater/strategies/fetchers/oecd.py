"""S4 fetcher — OECD (one of the four named giants, ~1,413 dataflows, ~6.0B obs, ~57GB).

Change-feed: re-enumerate the SDMX dataflow catalogue
  https://sdmx.oecd.org/public/rest/dataflow/all/all/latest
Each dataflow carries a VERSION (e.g. "1.4"); the per-flow version is the vintage.
Re-pull only flows whose version moved (plus new flows, plus any flow whose last
run was partial/failed/empty/absent — OECD's resume-skip would otherwise freeze a
flow that failed its REF_AREA/COUNTERPART split forever).

Per changed flow: incremental SDMX-CSV pull
  <root>/rest/data/<AGENCY>,<ID>,<VER>/all?dimensionAtObservation=AllDimensions
       &startPeriod=<last_obs year>
merged into clean_full/oecd/<AGENCY>__<ID>.parquet
(series_key, obs_date, value, obs_status, time_raw).

series_key is ALREADY STABLE: it is the dimension columns strictly between DATAFLOW
and TIME_PERIOD, joined with '.', exactly as the existing parquet stores it. OECD's
LAST_UPDATE column sits AFTER TIME_PERIOD (an attribute), so it never entered the
key — no re-key needed for OECD (unlike eurostat).

Rate: no key (CC BY 4.0) but Cloudflare + an app-level download quota: HTTP 429
"exceeded number of requests" -> long cooldown; default a polite global interval.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import xml.etree.ElementTree as ET

import pyarrow as pa

from ..base import Result
from ...errors import DefinitiveError
from . import _giant
from ._giant import http_get

DATAFLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/all/all/latest"
DEFAULT_ROOT = "https://sdmx.oecd.org/public"
CSV_ACCEPT = "application/vnd.sdmx.data+csv; charset=utf-8"
XML_ACCEPT = "application/vnd.sdmx.structure+xml;version=2.1"
RATE = 4.0       # OECD_MIN_INTERVAL default; quota-friendly
TIMEOUT = 900

NS = {
    "m": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "s": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "c": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
}

# columns that are never part of the OECD series key (mirror jobs/ingest_oecd.py NON_KEY)
_NON_KEY = {"TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "UNIT_MULT", "DECIMALS",
            "BASE_PER", "DATAFLOW"}


def fetch_catalog() -> dict:
    """Return {flow_key: {"vintage": ver, "filename": <AGENCY>__<ID>.parquet,
    "agency","id","version","root"}}. flow_key == filename stem so it matches the
    on-disk parquet 1:1 (the change-feed identity)."""
    raw = http_get(DATAFLOW_URL, XML_ACCEPT, 180, rate=RATE)
    if raw is None:
        return {}
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as e:
        raise DefinitiveError(f"oecd dataflow catalogue XML parse error: {e}")
    out = {}
    for df in root.iter("{%s}Dataflow" % NS["s"]):
        fid = df.get("id", "")
        ag = df.get("agencyID", "")
        ver = df.get("version", "1.0")
        if not fid or not ag:
            continue
        stem = (ag + "__" + fid).replace("/", "_").replace(":", "_")
        out[stem] = {"vintage": ver, "filename": stem + ".parquet",
                     "agency": ag, "id": fid, "version": ver, "root": DEFAULT_ROOT}
    return out


def _parse_period(p: str):
    """OECD SDMX TIME_PERIOD -> date (annual YYYY -> Dec 31, matches existing parquet)."""
    p = (p or "").strip()
    if not p:
        return None
    try:
        if len(p) == 4 and p.isdigit():
            return _dt.date(int(p), 12, 31)
        if len(p) == 7 and p[4] == "-":
            if p[5] == "Q":
                return _dt.date(int(p[:4]), (int(p[6]) - 1) * 3 + 1, 1)
            if p[5] == "S":
                return _dt.date(int(p[:4]), 1 if p[6] == "1" else 7, 1)
            if p[5:].isdigit():
                return _dt.date(int(p[:4]), int(p[5:]), 1)
        if len(p) == 8 and p[4] == "-" and p[6] == "W":
            return _dt.date.fromisocalendar(int(p[:4]), int(p[7:]), 1)
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            return _dt.date.fromisoformat(p)
    except Exception:
        return None
    return None


def _parse_csv(content: bytes):
    """Parse OECD SDMX-CSV (dimensionAtObservation=AllDimensions).
    series_key = columns strictly between DATAFLOW(0) and TIME_PERIOD, joined '.'.
    Returns (keys, dates, vals, statuses, raws) or (None,...) on a structural body."""
    text = content.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return None, None, None, None, None
    try:
        ti = header.index("TIME_PERIOD")
        vi = header.index("OBS_VALUE")
    except ValueError:
        return None, None, None, None, None
    si = header.index("OBS_STATUS") if "OBS_STATUS" in header else -1
    dim_idx = [i for i in range(1, ti)]  # between DATAFLOW(0) and TIME_PERIOD
    keys, dates, vals, statuses, raws = [], [], [], [], []
    for row in reader:
        if len(row) <= max(ti, vi):
            continue
        raw_v = row[vi].strip()
        if not raw_v or raw_v in ("NaN", "nan", "NA", "N/A", ".", "...", ":"):
            continue
        try:
            v = float(raw_v)
        except ValueError:
            continue
        praw = row[ti].strip()
        d = _parse_period(praw)
        if d is None:
            continue
        keys.append(".".join(row[i] for i in dim_idx))
        dates.append(d)
        vals.append(v)
        statuses.append(row[si] if 0 <= si < len(row) else "")
        raws.append(praw)
    return keys, dates, vals, statuses, raws


def _since_param(since: str | None) -> str:
    if not since:
        return ""
    try:
        d = since if isinstance(since, _dt.date) else _dt.date.fromisoformat(str(since)[:10])
    except Exception:
        return ""
    return f"&startPeriod={d.year:04d}"


def fetch_flow(flow_id, meta, since, session):
    url = (f"{meta['root']}/rest/data/"
           f"{meta['agency']},{meta['id']},{meta['version']}/all"
           f"?dimensionAtObservation=AllDimensions" + _since_param(since))
    content = http_get(url, CSV_ACCEPT, TIMEOUT, rate=RATE, session=session)
    if content is None:
        # Hard failure on /all. The full ingest falls back to REF_AREA/COUNTERPART
        # splitting for giant flows; here we mark transient so the flow is reselected
        # (a split-fetch path can be added without changing the contract).
        return None, "transient"
    head = content[:64].lstrip()
    if head.startswith(b"<"):
        # XML body where CSV expected -> structural error doc.
        return None, "structural"
    keys, dates, vals, statuses, raws = _parse_csv(content)
    if keys is None:
        return (None, "structural") if len(content) > 200 else (None, "empty")
    if not keys:
        return None, "empty"
    table = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
        "obs_status": pa.array(statuses, pa.string()),
        "time_raw": pa.array(raws, pa.string()),
    })
    return table, "ok"


def update(unit, since) -> Result:
    return _giant.run_giant(
        unit, source="oecd",
        fetch_catalog=fetch_catalog, fetch_flow=fetch_flow,
        csv_accept=CSV_ACCEPT, rate=RATE, timeout=TIMEOUT)


def current_vintage(unit) -> str | None:
    cat = fetch_catalog()
    return _giant._catalog_token(cat) if cat else None
