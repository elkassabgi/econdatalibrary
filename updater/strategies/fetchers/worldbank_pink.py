"""S5 bulk fetcher — World Bank "Pink Sheet" commodity prices (monthly + annual workbooks → 6 parquets).

CC BY 4.0, no key. Two XLSX workbooks (CMO-Historical-Data-Monthly.xlsx, ...-Annual.xlsx) each hold
several sheets; the ingester splits them into 6 grouped parquets clean_full/worldbank_pink/{freq}_{variant}.parquet
(m_price, m_index, a_price, a_index_nominal, a_price_real, a_index_real), schema (series_key, obs_date, value),
series_key = "{freq.lower()}:{variant}:{slug}".

No server-side date filter (whole-workbook only), but both workbooks are served with Last-Modified and honour
conditional GET (verified: If-Modified-Since = stored LM -> 304; an older date -> 200 full). So the refresh is a
per-WORKBOOK conditional GET against a blob-routed Last-Modified sidecar: 304 -> skip that workbook's sheets
(cheap); 200 -> parse its sheets and merge each into its parquet (dedup + never-shrink). The parse REUSES the
production functions jobs.ingest_worldbank_pink.{resolve_urls,SHEETS,parse_sheet} so series_key is byte-identical
to disk (duplication invariant). Store I/O via blob (R36); the ETag is absent so the gate is Last-Modified only.

HONEST-STATUS: a workbook GET failing after retries -> transient_unit (kept, retried); a corrupt/unopenable xlsx
-> structural_unit; a 200 sheet that parses to rows -> added_unit; sidecar LM advanced only after a clean merge.
"""
from __future__ import annotations
import io
import os
import time

import openpyxl
import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_worldbank_pink as ig   # reuse resolve_urls / SHEETS / parse_sheet

SOURCE = "worldbank_pink"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
SIDECAR = "_lastmod.json"          # {workbook_key: Last-Modified}
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


def _cond_get(sess, url, since_lm, tries=4):
    """Conditional GET. ('not_modified',None,None) | ('ok',bytes,last_modified) | ('gone',None,None).
    Exhausted retries on a retryable fault -> TransientError."""
    headers = dict(UA)
    if since_lm:
        headers["If-Modified-Since"] = since_lm
    for a in range(tries):
        try:
            r = sess.get(url, headers=headers, timeout=180)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"worldbank_pink: {e}")
            time.sleep(min(5 * (a + 1), 30)); continue
        if r.status_code == 304:
            return "not_modified", None, None
        if r.status_code == 200:
            return "ok", r.content, r.headers.get("Last-Modified")
        if r.status_code in (400, 404):
            return "gone", None, None
        if r.status_code in _TRANSIENT_HTTP:
            if a == tries - 1:
                raise TransientError(f"worldbank_pink HTTP {r.status_code}")
            time.sleep(min(5 * (a + 1), 30)); continue
        return "gone", None, None
    raise TransientError("worldbank_pink: retry budget exhausted")


def _total_rows(out_dir):
    return sum(blob.row_count(os.path.join(out_dir, f)) for f in blob.list_parquets(out_dir))


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()
    tally = Tally()

    try:
        urls = ig.resolve_urls()          # {'monthly': url, 'annual': url}; falls back internally
    except Exception as e:
        raise TransientError(f"worldbank_pink: resolve_urls {e}")

    sidecar = _load_sidecar(out_dir)
    wbs = {}          # workbook_key -> loaded workbook (only changed ones)
    new_lm = {}
    for wk, url in urls.items():
        try:
            status, content, lm = _cond_get(sess, url, sidecar.get(wk))
        except TransientError as e:
            tally.transient_unit(f"{wk}: conditional GET failed — {str(e)[:120]}")
            continue
        if status == "not_modified":
            continue
        if status == "gone":
            tally.empty_unit(f"{wk}: workbook gone upstream")
            continue
        try:
            wbs[wk] = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            new_lm[wk] = lm
        except Exception as e:  # noqa: BLE001
            # a 200 that is not a valid xlsx
            tally.structural_unit(f"{wk}: 200 but not a valid xlsx — {type(e).__name__}")

    if not wbs:
        return finalize(tally, _total_rows(out_dir), since or None, source=SOURCE)

    total = 0
    cursors = {}
    maxd = None
    merged_wb = set()
    for spec in ig.SHEETS:
        wb_key, sheet = spec[0], spec[1]
        freq, variant = spec[2], spec[3]
        if wb_key not in wbs:
            continue
        wb = wbs[wb_key]
        if sheet not in wb.sheetnames:
            tally.structural_unit(f"{wb_key}: sheet {sheet!r} is missing from the workbook")
            continue
        _, slugs, dates, vals, _, _ = ig.parse_sheet(wb[sheet], spec)
        if not slugs:
            tally.empty_unit()
            continue
        ns = f"{freq.lower()}:{variant}:"
        keys = [ns + s for s in slugs]
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array([float(v) for v in vals], pa.float64()),
        })
        path = os.path.join(out_dir, f"{freq.lower()}_{variant}.parquet")
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        total += n
        tally.added_unit(len(keys))
        for k, d in zip(keys, dates):
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
            if maxd is None or d > maxd:
                maxd = d
        merged_wb.add(wb_key)

    # advance Last-Modified only for workbooks whose sheets merged cleanly
    sidecar.update({wk: new_lm[wk] for wk in merged_wb if new_lm.get(wk)})
    _save_sidecar(out_dir, sidecar)

    last_obs = maxd.isoformat() if maxd else (since or None)
    return finalize(tally, total or _total_rows(out_dir), last_obs, source=SOURCE, series_cursors=cursors)
