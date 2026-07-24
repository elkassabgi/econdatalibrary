"""S5 bulk fetcher — UN World Population Prospects (WPP 2024). Two fixed CSV.GZ bulk files.

Layout: 2 parquets under clean_full/un_wpp/ — indicators_medium.parquet (4.6M rows) and
indicators_other.parquet (23.3M rows); each is a full 1950-2100 snapshot. Schema
(series_key, obs_date, value); series_key = "WPP:{Indicator}:{Variant}:{Location}", built by
jobs.ingest_un_wpp.parse_wpp_csv which this fetcher imports so keys match disk byte-for-byte.

Why conditional-GET and not date-tail: obs_date runs to 2101 (projections), so max(obs_date) can
never be a staleness signal, and no server-side date/since parameter exists on these bulk files.
The vintage lives entirely in the HTTP validators — and the server honours them properly
(verified: If-Modified-Since=stored LM -> 304 no body; If-None-Match=stored ETag -> 304;
an older date -> 200 full body). Only 2 fixed URLs, so unlike rba there is no index to scrape.

Steady state is therefore ~free: two conditional GETs that 304 (the current files are the
Dec-2024 release). The expensive parse only runs when the UN actually republishes a revision.

Store I/O via blob (R36); the Last-Modified/ETag sidecar lives on the store. Each file is merged
independently so peak memory stays one file, not both.

HONEST-STATUS: a fetch failing after retries -> transient_unit (kept, retried). A changed 200 that
parses to zero rows -> structural_unit with its validator NOT advanced, so the break re-surfaces.
Cursors emitted for merged series (R41).
"""
from __future__ import annotations
import json
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_un_wpp as ig   # reuse FILES + THE csv parser / key builder

SOURCE = "un_wpp"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_validators.json"       # {label: {"lm": Last-Modified, "etag": ETag}}
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
_TRANSIENT_HTTP = (429, 500, 502, 503, 504)


def _load_sidecar(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save_sidecar(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _targets():
    """[(label, url, out_name), ...] from the ingester's FILES map.

    ig.FILES values are ALREADY absolute URLs (they embed BASE), and the on-disk name is
    exactly f"{label}.parquet" — mirroring ingest_un_wpp.main(). Getting either wrong would
    write to a brand-new file instead of merging into the existing one.
    """
    return [(label, url, f"{label}.parquet") for label, url in ig.FILES.items()]


def _cond_get(url, stored, tries=4):
    """Conditional GET honouring BOTH validators. Returns
    ('not_modified',None,None,None) | ('ok',bytes,lm,etag) | ('gone',...)."""
    headers = dict(UA)
    if stored.get("lm"):
        headers["If-Modified-Since"] = stored["lm"]
    if stored.get("etag"):
        headers["If-None-Match"] = stored["etag"]
    for a in range(tries):
        try:
            r = requests.get(url, headers=headers, timeout=600)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"un_wpp: {e}")
            continue
        if r.status_code == 304:
            return "not_modified", None, None, None
        if r.status_code == 200:
            return "ok", r.content, r.headers.get("Last-Modified"), r.headers.get("ETag")
        if r.status_code in (400, 404):
            return "gone", None, None, None
        if r.status_code in _TRANSIENT_HTTP:
            if a == tries - 1:
                raise TransientError(f"un_wpp HTTP {r.status_code}")
            continue
        return "gone", None, None, None
    raise TransientError("un_wpp: retry budget exhausted")


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sidecar = _load_sidecar(out_dir)

    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0

    for label, url, out_name in _targets():
        stored = sidecar.get(label) or {}
        try:
            status, content, lm, etag = _cond_get(url, stored)
        except TransientError:
            tally.transient_unit()
            continue
        if status == "not_modified":
            continue                       # current -> skipped, not counted
        if status == "gone":
            tally.empty_unit()
            continue

        try:
            keys, dates, vals = ig.parse_wpp_csv(content, label)
        except Exception:
            tally.transient_unit()
            continue
        if not keys:
            tally.structural_unit()        # real body, zero rows: don't advance the validator
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        path = os.path.join(out_dir, out_name)
        before = blob.row_count(path) if blob.exists(path) else 0
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            tally.transient_unit()
            continue
        published += n
        tally.added_unit(max(0, n - before))
        for k, d in zip(keys, dates):
            iso = d.isoformat()
            if k not in cursors or iso > cursors[k]:
                cursors[k] = iso
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        sidecar[label] = {"lm": lm or stored.get("lm"), "etag": etag or stored.get("etag")}

        # free the parsed arrays before the next (much larger) file
        del keys, dates, vals, tbl, content

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    return finalize(tally, published, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors)
