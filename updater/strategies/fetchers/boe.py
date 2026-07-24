"""S2 fetcher — Bank of England IADB (Interactive statistical DataBase): FX rates, yields, monetary
and banking statistics. OGL-UK-3.0, no key.

Storage (matches the ingester): ONE parquet per 3-character series-code PREFIX (XUD, IUM, CFM, RPM…),
~38 files, each dense with many series. Schema (series_key, obs_date, value); series_key = the BoE code
(e.g. XUDLUSS). The IADB CSV export returns a wide table for up to ~50 codes at a time and honours a
server-side Datefrom/Dateto — a REAL date filter — so this is a true date-tail:

  per prefix file → read its distinct codes + stored max obs_date (sane, guarded vs corrupt far-future),
  request the codes in batches of 50 over [stored_max - LOOKBACK .. today] only, parse the wide CSV,
  merge (dedup series_key+obs_date, never-shrink) back into that prefix's parquet.

CI-safe: the code universe is the distinct series_key already in each parquet (read via blob), not the
local enumerate sidecar — so refreshing existing series needs no raw-path file (R36). Genuinely-new BoE
codes are a re-enumeration concern handled separately, not here.

HONEST-STATUS: a batch whose CSV fails after retries (5xx / HTML error body / timeout) -> transient_unit
(kept, retried); a batch that parses to no obs in the window -> empty_unit; parsed obs -> added_unit.
Nothing is silently reported no_change on a transport failure.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "boe"
CSV_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
BATCH = 50
LOOKBACK_DAYS = 120          # re-pull a trailing window to absorb BoE back-revisions
EPOCH = dt.date(1963, 1, 1)  # the IADB epoch; dates before this are rejected
MON = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTHS = {m: i for i, m in enumerate(MON, 1)}


def _fmt(d: dt.date) -> str:
    return f"{d.day:02d}/{MON[d.month - 1]}/{d.year}"


def _parse_date(s):
    s = s.strip().strip('"')
    if not s:
        return None
    parts = s.split()
    try:
        if len(parts) == 3:
            return dt.date(int(parts[2]), MONTHS[parts[1]], int(parts[0]))
        if len(parts) == 2:
            return dt.date(int(parts[1]), MONTHS[parts[0]], 1)
        if len(parts) == 1 and parts[0].isdigit():
            return dt.date(int(parts[0]), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def _parse_value(c):
    c = c.strip().strip('"')
    if not c:
        return None
    try:
        return float(c)
    except ValueError:
        return None


def _split_csv(line):
    if '"' not in line:
        return line.split(",")
    out, cur, q = [], [], False
    for ch in line:
        if ch == '"':
            q = not q
        elif ch == "," and not q:
            out.append("".join(cur)); cur = []
        else:
            cur.append(ch)
    out.append("".join(cur))
    return out


def _parse_csv(text):
    """Parse the IADB wide CSV → list[(code, obs_date, value)]. Raises ValueError on a bad header."""
    lines = text.splitlines()
    n = len(lines)
    if not lines or not lines[0].startswith("SERIES"):
        raise ValueError("unexpected CSV header")
    i = 1
    while i < n and lines[i].strip() != "":     # skip the SERIES/DESCRIPTION block
        i += 1
    while i < n and not lines[i].startswith("DATE,"):
        i += 1
    if i >= n:
        return []
    cols = _split_csv(lines[i])[1:]
    i += 1
    obs = []
    while i < n:
        row = lines[i]; i += 1
        if not row.strip():
            continue
        cells = _split_csv(row)
        od = _parse_date(cells[0])
        if od is None:
            continue
        for j, code in enumerate(cols, start=1):
            if j >= len(cells):
                break
            v = _parse_value(cells[j])
            if v is None:
                continue
            obs.append((code, od, v))
    return obs


def _fetch_csv(sess, codes, datefrom, dateto, tries=5):
    """Wide CSV for a batch of codes over [datefrom..dateto]. Exhausted retries -> TransientError."""
    params = {"csv.x": "yes", "SeriesCodes": ",".join(codes), "Datefrom": datefrom,
              "Dateto": dateto, "CSVF": "TT", "UsingCodes": "Y", "VPD": "Y", "VFD": "N"}
    for i in range(tries):
        try:
            r = sess.get(CSV_URL, params=params, timeout=300)
        except (requests.Timeout, requests.ConnectionError) as e:
            if i == tries - 1:
                raise TransientError(f"boe: {e}")
            time.sleep(2 * (i + 1) + 1); continue
        if r.status_code == 200 and r.text.lstrip().startswith("SERIES"):
            return r.text
        # a 200 with an HTML error/empty body, or a 5xx/429 -> retry
        if r.status_code in (200, 429, 500, 502, 503, 504):
            if i == tries - 1:
                raise TransientError(f"boe: non-CSV/{r.status_code} body after retries")
            time.sleep(2 * (i + 1) + 1); continue
        raise TransientError(f"boe HTTP {r.status_code}")
    raise TransientError("boe: retry budget exhausted")


def _codes_and_max(path):
    """Distinct BoE codes + sane max obs_date (<= today) for one prefix parquet."""
    if not blob.exists(path):
        return [], None
    t = blob.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0:
        return [], None
    codes = sorted({c for c in t.column("series_key").to_pylist() if c})
    md = pc.max(t.column("obs_date")).as_py()
    if isinstance(md, dt.date) and md > dt.date.today():
        # ignore a corrupt far-future stamp when choosing the refetch window
        od = t.column("obs_date").to_pylist()
        today = dt.date.today()
        sane = [d for d in od if isinstance(d, dt.date) and d <= today]
        md = max(sane) if sane else None
    return codes, (md if isinstance(md, dt.date) else None)


def _chunk(lst, k):
    for i in range(0, len(lst), k):
        yield lst[i:i + k]


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    prefixes = blob.list_parquets(out_dir)
    sess = requests.Session()
    sess.headers.update(UA)
    tally = Tally()
    dateto = _fmt(dt.date.today())
    grand_total = 0
    grand_max: dt.date | None = None
    cursors: dict[str, str] = {}

    for pf in prefixes:
        path = os.path.join(out_dir, pf)
        codes, smax = _codes_and_max(path)
        if not codes:
            grand_total += blob.row_count(path)
            tally.empty_unit()
            continue
        start = max(EPOCH, (smax - dt.timedelta(days=LOOKBACK_DAYS)) if smax else EPOCH)
        datefrom = _fmt(start)

        keys, dates, vals = [], [], []
        for batch in _chunk(codes, BATCH):
            try:
                text = _fetch_csv(sess, batch, datefrom, dateto)
            except TransientError:
                tally.transient_unit()
                time.sleep(1.0)
                continue
            try:
                obs = _parse_csv(text)
            except ValueError:
                tally.transient_unit()   # a malformed body is a transport hiccup, not real no-data
                continue
            if not obs:
                tally.empty_unit()
                time.sleep(0.5)
                continue
            for code, od, v in obs:
                keys.append(code); dates.append(od); vals.append(v)
            tally.added_unit(len(obs))
            time.sleep(0.5)

        if keys:
            tbl = pa.table({
                "series_key": pa.array(keys, pa.string()),
                "obs_date": pa.array(dates, pa.date32()),
                "value": pa.array(vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
            grand_total += n
            for k, d in zip(keys, dates):
                iso = d.isoformat()
                if k not in cursors or iso > cursors[k]:
                    cursors[k] = iso
                if isinstance(d, dt.date) and d <= dt.date.today() and (grand_max is None or d > grand_max):
                    grand_max = d
        else:
            grand_total += blob.row_count(path)

    last_obs = grand_max.isoformat() if grand_max else (since or None)
    return finalize(tally, grand_total, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=len(prefixes) + 1)
