"""S5 bulk fetcher — Reserve Bank of Australia statistical tables (~224 CSVs → one parquet).

RBA copyright, free for non-commercial research with attribution. Single parquet
clean_full/rba/rba.parquet, schema (series_key, obs_date, value); series_key = the RBA Series ID
(ARBALNOIW, …) or "<file>:col<n>" for unlabelled columns. The RBA CSVs have NO server-side date
filter, so a row-level delta is impossible — but each CSV is served with a Last-Modified header and
the site honours conditional GETs (verified: If-Modified-Since → 304, 0 bytes when unchanged).

So the refresh is a per-file conditional GET: scrape the tables index for the CSV links, then GET each
with If-Modified-Since = its stored Last-Modified. A 304 skips the file with no body (cheap, not
counted — it is up to date); a 200 is parsed and its rows merged (dedup series_key+obs_date,
never-shrink). Per-file Last-Modified lives in a blob-routed sidecar (R36: it must survive on the
STORE, not the ephemeral CI runner, or every run re-downloads all 224 files).

HONEST-STATUS: a file whose GET fails after retries (timeout/5xx) -> transient_unit (kept, retried);
a changed 200 that parses to rows -> added_unit; a changed 200 that parses to ZERO rows -> empty_unit
and its Last-Modified is NOT advanced (so a parser break re-surfaces rather than being sealed in).
An index-page fetch failure -> TransientError (whole run partial; nothing silently no_change).
"""
from __future__ import annotations
import csv
import datetime as dt
import io
import os
import re
import time

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "rba"
BASE = "https://www.rba.gov.au"
INDEX = "/statistics/tables/"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com",
      "Accept": "text/html,text/csv,application/csv,*/*"}
PARQUET = "rba.parquet"
SIDECAR = "_lastmod.json"          # {csv_path: Last-Modified} — blob-routed
DEDUP = ("series_key", "obs_date")
RATE = 0.4
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def _load_sidecar(out_dir):
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        import json
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data):
    import json
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, sort_keys=True).encode("utf-8"))


def _parse_date(s):
    s = (s or "").strip()
    try:
        if len(s) == 11 and s[2] == "-":            # DD-Mon-YYYY
            return dt.datetime.strptime(s, "%d-%b-%Y").date()
        if len(s) == 10 and s[2] == "/" and s[5] == "/":   # DD/MM/YYYY
            return dt.datetime.strptime(s, "%d/%m/%Y").date()
        if len(s) == 10 and s[4] == "-":            # YYYY-MM-DD
            return dt.date.fromisoformat(s)
        if len(s) == 4 and s.isdigit():             # YYYY
            return dt.date(int(s), 12, 31)
        if len(s) == 7 and s[4] in ("-", "/"):      # YYYY-MM
            return dt.date(int(s[:4]), int(s[5:7]), 1)
    except (ValueError, TypeError):
        pass
    return None


def _parse_csv(content, csv_path):
    """RBA CSV → (keys, dates, vals). Same layout the ingester consumes (metadata rows, a
    'Series ID' row, then DATE,val,val,… rows)."""
    keys, dates, vals = [], [], []
    text = content.decode("utf-8-sig", errors="replace")
    lines = text.splitlines()
    if len(lines) < 11:
        return keys, dates, vals
    sid_row = data_start = None
    for idx, line in enumerate(lines):
        if line.lower().startswith("series id,"):
            sid_row, data_start = idx, idx + 1
            break
    if sid_row is None:
        for idx in range(5, min(20, len(lines))):
            parts = lines[idx].split(",")
            if parts and _parse_date(parts[0].strip()):
                sid_row, data_start = idx - 1, idx
                break
    if sid_row is None or data_start is None:
        return keys, dates, vals
    rows = list(csv.reader(io.StringIO("\n".join(lines))))
    if sid_row >= len(rows):
        return keys, dates, vals
    series_ids = rows[sid_row][1:]
    prefix = os.path.basename(csv_path).replace(".csv", "")
    for row in rows[data_start:]:
        if not row:
            continue
        d = _parse_date(row[0].strip())
        if d is None:
            continue
        for ci, raw in enumerate(row[1:]):
            raw = raw.strip()
            if not raw or raw in ("", "..", "N/A", "n/a", "na", "—"):
                continue
            try:
                v = float(raw.replace(",", ""))
            except ValueError:
                continue
            sid = (series_ids[ci].strip()
                   if ci < len(series_ids) and series_ids[ci].strip()
                   else f"{prefix}:col{ci + 1}")
            keys.append(sid); dates.append(d); vals.append(v)
    return keys, dates, vals


def _get(sess, url, since_lm, tries=4):
    """Conditional GET. Returns ('not_modified',None,None) | ('ok',bytes,last_modified) |
    ('gone',None,None). Exhausted retries on a retryable fault -> TransientError."""
    headers = dict(UA)
    if since_lm:
        headers["If-Modified-Since"] = since_lm
    for a in range(tries):
        try:
            r = sess.get(url, headers=headers, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"rba: {e}")
            time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code == 304:
            return "not_modified", None, None
        if r.status_code == 200:
            return "ok", r.content, r.headers.get("Last-Modified")
        if r.status_code in (400, 404):
            return "gone", None, None
        if r.status_code in _TRANSIENT_HTTP:
            if a == tries - 1:
                raise TransientError(f"rba HTTP {r.status_code}")
            time.sleep(60 if r.status_code == 429 else min(5 * (a + 1), 30)); continue
        return "gone", None, None
    raise TransientError("rba: retry budget exhausted")


def _links(sess):
    _, content, _ = _get(sess, BASE + INDEX, None)   # raises TransientError on failure
    if not content:
        raise TransientError("rba: empty index page")
    html = content.decode("utf-8", errors="replace")
    return sorted(set(re.findall(r'href="(/statistics/tables/csv/[^"]+\.csv)"', html)))


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, PARQUET)
    sess = requests.Session()

    links = _links(sess)
    if not links:
        raise TransientError("rba: index returned no CSV links")

    sidecar = _load_sidecar(out_dir)
    tally = Tally()
    keys, dates, vals = [], [], []
    new_lm = {}

    for link in links:
        try:
            status, content, lm = _get(sess, BASE + link, sidecar.get(link))
        except TransientError:
            tally.transient_unit()
            time.sleep(RATE); continue
        if status == "not_modified":
            time.sleep(RATE); continue    # up to date — skipped, not counted (faostat semantics)
        if status == "gone":
            tally.empty_unit()
            time.sleep(RATE); continue
        k, d, v = _parse_csv(content, link)
        if not k:
            tally.empty_unit()            # changed file, no parseable rows: do NOT advance Last-Modified
            time.sleep(RATE); continue
        keys.extend(k); dates.extend(d); vals.extend(v)
        tally.added_unit(len(k))
        new_lm[link] = lm                 # advance only on a clean parse
        time.sleep(RATE)

    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except Exception:
            # a merge guard (e.g. never-shrink) tripped: keep data + DON'T advance any sidecar
            # Last-Modified, surface as partial so the files are re-evaluated next tick.
            tally.transient_unit()
            return finalize(tally, blob.row_count(path) if blob.exists(path) else 0, since or None,
                            source=SOURCE)
        total = n
        cursors = _series_maxes(tbl)
        last_obs = md
        # persist the advanced Last-Modified ONLY after the merge succeeded.
        sidecar.update({k: v for k, v in new_lm.items() if v})
        _save_sidecar(out_dir, sidecar)
    else:
        total = blob.row_count(path) if blob.exists(path) else 0
        cursors = {}
        last_obs = since or None

    return finalize(tally, total, last_obs, source=SOURCE, series_cursors=cursors)


def _series_maxes(tbl):
    out = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}
