"""S1 fetcher — CBOE volatility indexes (VIX, SKEW, VVIX, VXN, RVX, ... ~20 CSVs).

Free for informational use (CBOE terms), no API key. The CDN serves only
full-history *_History.csv per index (no date filter), but the whole dataset is
tiny (~888KB parquet), so the right move for this daily source is to re-fetch the
whole table and MERGE (dedup series_key+obs_date, new wins on revision, never
shrink). Single grouped parquet clean_full/cboe/cboe.parquet, schema
(series_key, obs_date, value).

current_vintage is a cheap HEAD on the flagship VIX_History.csv (ETag /
Last-Modified) per the registry vintage_signal — VIX updates every trading day, so
its header moving is a reliable proxy for "new data today". Each index is its own
sub-unit: a transient CDN failure on one index is tallied transient (status
partial, re-run next tick) instead of silently dropping that index from the build;
a 200 that parses 0 rows from a real body is structural. The whole-table merge
under merge_and_write keeps every index that DID succeed.
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "cboe"
DEDUP = ("series_key", "obs_date")
BASE = "https://cdn.cboe.com/api/global/us_indices/daily_prices"

# (filename, series_name) — mirrors jobs/ingest_cboe.py. Multi-column indices emit
# series_key f"{name}_{COL}" (e.g. VIX_CLOSE); single-value indices emit the bare name.
INDICES = [
    ("VIX_History.csv",    "VIX"),
    ("SKEW_History.csv",   "SKEW"),
    ("VVIX_History.csv",   "VVIX"),
    ("VXN_History.csv",    "VXN"),
    ("RVX_History.csv",    "RVX"),
    ("VXD_History.csv",    "VXD"),
    ("VIX9D_History.csv",  "VIX9D"),
    ("VIX3M_History.csv",  "VIX3M"),
    ("VIX6M_History.csv",  "VIX6M"),
    ("VIX1Y_History.csv",  "VIX1Y"),
    ("GVZ_History.csv",    "GVZ"),
    ("OVX_History.csv",    "OVX"),
    ("EUVIX_History.csv",  "EUVIX"),
    ("JYVIX_History.csv",  "JYVIX"),
    ("BPVIX_History.csv",  "BPVIX"),
    ("VXAPL_History.csv",  "VXAPL"),
    ("VXGOG_History.csv",  "VXGOG"),
    ("VXGS_History.csv",   "VXGS"),
    ("VXIBM_History.csv",  "VXIBM"),
    ("VXAZN_History.csv",  "VXAZN"),
]

# Cheap vintage probe: the flagship index updates every trading day.
VINTAGE_URL = f"{BASE}/VIX_History.csv"

_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def current_vintage(unit):
    """Cheap HEAD on VIX_History.csv — ETag/Last-Modified moves iff new data.
    Returns None if undeterminable (strategy then fetches anyway, which is safe)."""
    for attempt in range(3):
        try:
            r = requests.head(VINTAGE_URL, headers=UA, timeout=60, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError):
            return None
        if r.status_code in _TRANSIENT_HTTP:
            return None
        if r.status_code != 200:
            return None
        h = r.headers
        return h.get("ETag") or h.get("Last-Modified") or h.get("Content-Length")
    return None


def _parse_date(date_str: str):
    s = (date_str or "").strip()
    if not s:
        return None
    try:
        if "/" in s:
            parts = s.split("/")
            if len(parts) != 3:
                return None
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            return dt.date(y, m, d)
        return dt.date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _parse_cboe_csv(content: bytes, series_name: str):
    """Parse one CBOE index history CSV -> [(date, series_key, value)]. Mirrors
    jobs/ingest_cboe.py: bare name for single-value, f"{name}_{COL}" otherwise."""
    out = []
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    headers = reader.fieldnames or []
    date_col = next((h for h in headers if h.strip().upper() in ("DATE", "TRADE DATE")), None)
    if date_col is None:
        return out
    val_cols = [h for h in headers if h != date_col and h.strip()]
    multi = len(val_cols) > 1
    for row in reader:
        od = _parse_date(row.get(date_col, ""))
        if od is None:
            continue
        for col in val_cols:
            v_raw = (row.get(col, "") or "").strip()
            if not v_raw or v_raw.upper() in ("N/A", "NA"):
                continue
            try:
                v = float(v_raw.replace(",", ""))
            except (ValueError, TypeError):
                continue
            if v != v:  # NaN
                continue
            col_norm = col.strip().upper().replace(" ", "_")
            key = f"{series_name}_{col_norm}" if multi else series_name
            out.append((od, key, v))
    return out


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "cboe.parquet")
    before = blob.row_count(path)
    tally = Tally()

    keys, dates, vals = [], [], []
    seen = set()

    for filename, series_name in INDICES:
        url = f"{BASE}/{filename}"
        try:
            r = requests.get(url, headers=UA, timeout=60)
        except (requests.Timeout, requests.ConnectionError):
            tally.transient_unit()
            continue
        if r.status_code in _TRANSIENT_HTTP:
            tally.transient_unit()
            continue
        if r.status_code == 404:
            # one missing index is not the whole source failing; record empty
            tally.empty_unit()
            continue
        if r.status_code != 200:
            tally.structural_unit()
            continue
        if len(r.content) < 100:
            tally.empty_unit()
            continue
        rows = _parse_cboe_csv(r.content, series_name)
        if not rows:
            tally.structural_unit()  # 200 with a real body but parsed nothing
            continue
        n = 0
        for od, key, v in rows:
            tok = (key, od)
            if tok in seen:
                continue
            seen.add(tok)
            keys.append(key)
            dates.append(od)
            vals.append(v)
            n += 1
        # added accounting is done after merge (these are total rows, not just NEW);
        # mark this sub-unit as having produced data so the empty-window guard is correct.
        if n:
            tally.attempted += 1
        else:
            tally.empty_unit()

    if not vals:
        # nothing parsed at all from any index -> finalize will raise/partial honestly
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    # Attribute the net new rows to the tally so status is ok/no_change honestly.
    tally.added = max(0, n - before)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(keys, dates))
