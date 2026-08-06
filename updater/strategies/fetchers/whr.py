"""S1 fetcher — World Happiness Report Figure 2.1, DIRECT from worldhappiness.report.

PROVENANCE IS THE WHOLE POINT OF THIS REWRITE (R215 / tools/catalog_whr.py's blocker).
The previous module fell through to OWID's grapher CSV while citing WHR/Gallup — data
obtained from a third party under a different licence, which held publication. This
version fetches ONLY the publisher's own Figure 2.1 workbook and has NO fallback: if
the primary is unreachable the run fails honestly (transient), it never silently
substitutes an aggregator.

SCOPE IS THE GRANT'S SCOPE. Gallup/WHR's written permission (2026-07-09, recorded in
DATABASE_LICENSES_VERBATIM.md as "CLEARED by WRITTEN PERMISSION (scoped/conditional)")
covers exactly the Figure 2.1 data the site offers for free download: 3-year life
evaluations, their 95% CI whiskers, and the six explanatory-factor contributions.
This fetcher ingests exactly that workbook and nothing else. Serving anything beyond
it would exceed the grant.

DISCOVERY: parse https://www.worldhappiness.report/data-sharing/ for
files.worldhappiness.report/WHR<NN>_Data_Figure_2.1*.xls[x] links and take the newest
edition (R78: watch the LISTING — editions get new filenames yearly, and WHR25 shipped
as "...2.1v3.xlsx", so a pinned URL or a guessed filename pattern goes stale). Verified
headless 2026-08-06: the listing 200s with our UA, and the WHR26 file serves a REAL
ETag + Last-Modified (unlike OWID's cache-fill Last-Modified, M-20260730-91) — vintage
= edition + ETag/Content-Length, never a timestamp.

KEYS: FIG21:<measure>:<geo> where geo is ISO3 (pycountry) or a deterministic slug for
unresolvable names. DELIBERATELY DISJOINT from the legacy OWID-era store keys
("WHR:Self-reported life satisfaction:AFG" — 2,270 rows still in whr.parquet) so the
two provenances can never mix under one served id; the legacy rows stay uncatalogued
and unserved until the pending purge removes them. New data lands in its own
whr_fig21.parquet.

HONEST-STATUS: listing/primary unreachable -> TransientError (no fallback); listing
200 with zero Figure-2.1 links, unrecognized workbook header, or 0 parsed obs ->
structural (the page/file was restructured, retrying will not help).
"""
from __future__ import annotations
import datetime as dt
import io
import os
import re
import unicodedata

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from ._vintage import UA

SOURCE = "whr"
DEDUP = ("series_key", "obs_date")
LISTING = "https://www.worldhappiness.report/data-sharing/"
LINK_RE = re.compile(
    r"https://files\.worldhappiness\.report/WHR(\d+)_Data_Figure_2\.1[^\"']*\.xlsx?")

# Column header (exact, from the WHR26 workbook) -> stable measure code. If WHR
# renames every header that is structural; a partial rename still parses the rest.
MEASURES = {
    "Rank": "rank",
    "Life evaluation (3-year average)": "ladder",
    "Lower whisker": "whisker_low",
    "Upper whisker": "whisker_high",
    "Explained by: Log GDP per capita": "gdp",
    "Explained by: Social support": "social_support",
    "Explained by: Healthy life expectancy": "healthy_life",
    "Explained by: Freedom to make life choices": "freedom",
    "Explained by: Generosity": "generosity",
    "Explained by: Perceptions of corruption": "corruption",
    "Dystopia + residual": "dystopia_residual",
}


def _newest_link(sess) -> "tuple[int, str]":
    r = sess.get(LISTING, timeout=90)
    r.raise_for_status()
    hits = {int(m.group(1)): m.group(0) for m in LINK_RE.finditer(r.text)}
    if not hits:
        # A 200 listing with zero Figure-2.1 links is a structural page change,
        # not an outage (R78: an empty listing is structural).
        raise ValueError("data-sharing page has no Figure 2.1 links (page restructured?)")
    ed = max(hits)
    return ed, hits[ed]


def _geo_code(name: str) -> str:
    try:
        import pycountry
        return pycountry.countries.lookup(name).alpha_3
    except Exception:                                        # noqa: BLE001
        slug = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
        return re.sub(r"[^A-Za-z0-9]+", "_", slug).strip("_").upper()


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def current_vintage(unit):
    """Edition + the file's own validators. files.worldhappiness.report serves a real
    ETag and Content-Length (measured 2026-08-06); the edition number alone would miss
    a re-issued file (WHR25 went through a v3 re-issue, so re-issues do happen)."""
    try:
        sess = requests.Session(); sess.headers.update(UA)
        ed, url = _newest_link(sess)
        h = sess.head(url, timeout=60, allow_redirects=True)
        return f"WHR{ed}:{h.headers.get('ETag', '')}:{h.headers.get('Content-Length', '')}"
    except Exception:                                        # noqa: BLE001
        return None                                          # undeterminable -> cadence decides


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "whr_fig21.parquet")
    before = blob.row_count(path) if blob.exists(path) else 0
    tally = Tally()
    sess = requests.Session(); sess.headers.update(UA)

    try:
        ed, url = _newest_link(sess)
        r = sess.get(url, timeout=180)
        r.raise_for_status()
        body = r.content
    except ValueError as e:
        tally.structural_unit(str(e))
        return finalize(tally, before, None, source=SOURCE)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"whr: primary fetch failed (NO fallback by design): {e!r}") from e

    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(body), read_only=True, data_only=True)
    ws = wb.worksheets[0]
    rows_iter = ws.iter_rows(values_only=True)
    header = [str(c).strip() if c is not None else "" for c in next(rows_iter)]
    col = {h: i for i, h in enumerate(header)}
    if all(h not in col for h in MEASURES):
        tally.structural_unit(f"workbook header unrecognized: {header[:6]}")
        return finalize(tally, before, None, source=SOURCE)
    yr_i, name_i = col.get("Year"), col.get("Country name")
    if yr_i is None or name_i is None:
        tally.structural_unit(f"workbook lacks Year/Country name columns: {header[:6]}")
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    for row in rows_iter:
        if row is None or yr_i >= len(row) or name_i >= len(row) \
                or row[yr_i] is None or row[name_i] is None:
            continue
        try:
            year = int(float(row[yr_i]))
        except (TypeError, ValueError):
            continue
        geo = _geo_code(str(row[name_i]).strip())
        stamp = dt.date(year, 12, 31)
        for h, code in MEASURES.items():
            i = col.get(h)
            if i is None or i >= len(row) or row[i] is None:
                continue
            try:
                v = float(row[i])
            except (TypeError, ValueError):
                continue
            keys.append(f"FIG21:{code}:{geo}")
            dates.append(stamp)
            vals.append(v)

    if not keys:
        tally.structural_unit("workbook parsed to 0 observations")
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, n - before), "fig21")
    print(f"[whr] WHR{ed}: parsed {len(keys):,} obs / {len(set(keys)):,} series from the "
          f"primary workbook ({url}); store now {n:,} rows", flush=True)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
