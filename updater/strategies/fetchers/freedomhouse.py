"""S1 fetcher — Freedom House, Freedom in the World (FIW), annual 1973-present.

CC-BY 4.0. Single grouped parquet clean_full/freedomhouse/freedomhouse.parquet,
schema (series_key, obs_date, value) where series_key is
'<metric>:<country>' with metric in {political_rights, civil_liberties,
freedom_status} and obs_date is the Dec-31 of each rated year. FH re-publishes a
wide XLSX each Feb/Mar (one new year column) and silently revises older years, so
we re-fetch the WHOLE workbook and MERGE (dedup series_key+obs_date, new wins on
revision, never-shrink). One sub-unit (the workbook); a 200 that parses 0 rows
from a real body is structural.

vintage_signal (registry): HEAD Last-Modified / Content-Length / ETag on the
current year-stamped XLSX. The hardcoded 2025-03 / 2023-02 paths in
jobs/ingest_freedomhouse.py are now 404; the live current edition is the FIW-2025
file under the 2025-02 directory (data column runs through year 2025), with the
2024-02 file as a known-good fallback. Each new Feb/Mar edition lands at a new
year-stamped path that is not auto-discovered (see registry open_question).
"""
from __future__ import annotations
import io
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

# Reuse the existing ingester's parse logic (single source of truth). `jobs` is a
# repo-root sibling of `updater`, not a subpackage, so import it absolutely; if the
# repo root isn't on sys.path, load the module directly by file path.
try:
    from jobs import ingest_freedomhouse as ING  # type: ignore
except Exception:  # pragma: no cover - import-path fallback
    import importlib.util as _ilu

    _JOB = os.path.join(config.ROOT, "jobs", "ingest_freedomhouse.py")
    _spec = _ilu.spec_from_file_location("ingest_freedomhouse", _JOB)
    ING = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(ING)

SOURCE = "freedomhouse"
DEDUP = ("series_key", "obs_date")

# Live, newest-first. The ingester's first/last entries (2025-03, 2023-02) 404;
# the real current edition (FIW 2025, data through year 2025) lives in 2025-02,
# and the 2024-02 file is a still-served fallback. We probe/fetch in this order
# and union every URL that succeeds, then MERGE (dedup handles overlap).
URLS = [
    "https://freedomhouse.org/sites/default/files/2025-02/Country_and_Territory_Ratings_and_Statuses_FIW_1973-2024.xlsx",
    "https://freedomhouse.org/sites/default/files/2024-02/Country_and_Territory_Ratings_and_Statuses_FIW_1973-2024.xlsx",
]


def current_vintage(unit):
    """Cheap HEAD probe (ETag/Last-Modified/Content-Length) on the live edition.
    Changes iff FH re-publishes the XLSX. Returns None if no URL exposes a token
    (the strategy then fetches anyway — safe, merge dedups + never-shrinks)."""
    for url in URLS:
        v = http_vintage(url)
        if v is not None:
            return v
    return None


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


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "freedomhouse.parquet")
    before = blob.row_count(path)
    tally = Tally()

    # Re-fetch the whole table from every live URL; union (dedup on (key,date)).
    keys, dates, vals = [], [], []
    seen: set = set()
    got_200 = False        # at least one URL returned a real XLSX body
    transient = False      # at least one URL transient-failed (and none of the
    #                        successful ones produced rows -> report partial)

    for url in URLS:
        try:
            r = requests.get(url, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError):
            transient = True
            continue
        if r.status_code in (429, 500, 502, 503, 504):
            transient = True
            continue
        if r.status_code != 200 or len(r.content) <= 5000:
            # 404 / moved / truncated CDN path — not a transient; try the next URL.
            continue
        got_200 = True
        for key, d, v in ING.parse_fiw_xlsx(r.content):
            tok = (key, d)
            if tok not in seen:
                seen.add(tok)
                keys.append(key)
                dates.append(d)
                vals.append(v)
        # Newest edition is first; once it parses real rows we have the full
        # current table (older fallbacks only re-add already-seen rows).
        if keys:
            break

    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date":   pa.array(dates, pa.date32()),
            "value":      pa.array(vals, pa.float64()),
        })
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        tally.added_unit(max(0, n - before))
        return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))

    # Reached only if no URL yielded rows.
    if got_200:
        # A 200 with a real body that parsed 0 rows -> schema/structural break.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)
    if transient:
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)
    # Every URL 404'd / moved with no transient -> the hardcoded paths are stale.
    # Surface as structural so the orchestrator does NOT stamp last_success and a
    # human can supply the new year-stamped path.
    tally.structural_unit()
    return finalize(tally, before, None, source=SOURCE)
