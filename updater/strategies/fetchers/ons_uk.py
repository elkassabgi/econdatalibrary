"""S5 bulk fetcher — ONS (UK Office for National Statistics) beta API. OGL-UK-3.0, no key.

One parquet per ONS dataset under clean_full/ons_uk/{dataset_id}.parquet, schema
(series_key, obs_date, value); series_key = colon-joined `dim=value` pairs over the CSV's
dimension columns (built by jobs.ingest_ons_uk.parse_dataset_csv, which this fetcher IMPORTS so
the keys are byte-identical to disk — the duplication invariant).

The /v1/datasets catalog walk IS the machine manifest: each catalog item embeds
links.latest_version.id and last_updated, so ONE paginated walk both (a) gives every dataset's
vintage without a per-dataset GET and (b) surfaces brand-new datasets. Vintage token =
"{latest_version_id}|{last_updated}"; unchanged datasets are skipped entirely, changed and NEW
ones are downloaded, parsed and merged (dedup + never-shrink). The store currently holds only 42
of the catalog's ~337 datasets, so early runs will also BACKFILL the missing ones.

Store I/O via blob (R36); the vintage sidecar lives on the store, not the runner. Downloads run
across a small thread pool (R40) since a first run touches hundreds of datasets.

HONEST-STATUS: catalog walk failure -> TransientError (partial, retried, data kept). A per-dataset
download/parse failure -> transient_unit for that dataset only. A changed dataset that parses to
ZERO rows -> structural_unit and its vintage is NOT advanced. Cursors emitted for every merged
series (R41).
"""
from __future__ import annotations
import hashlib
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_ons_uk as ig   # reuse catalog walk + THE key builder

SOURCE = "ons_uk"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"     # {dataset_id: "versionid|last_updated"}
# ONS's beta API rate-limits HARD: a first attempt at 5 workers x 60 datasets drew 41 HTTP 429s
# in 4 minutes (run 30133384687). R40 says parallelize many-request fetchers, but never past what
# the server tolerates — 2 workers plus a per-request pace keeps us under the limit.
# MEASURED: even 2 workers @1s still draws sustained HTTP 429 (local run 2026-07-25, 6+ min and
# still throttled). Each 429 costs up to 50s of backoff (5+10+15+20), so throttling — not
# parsing — is the entire cost. ONS tolerates roughly one request per ~1.5s, so go SERIAL and
# pace it; a small per-run batch keeps each tick short and drains the ~295-dataset backfill over
# consecutive days rather than wedging one run. (R40b: the server's tolerance is the ceiling.)
MAX_WORKERS = 1
# MEASURED 2026-07-25 against download.ons.gov.uk (Cloudflare-fronted): 429 arrives on the
# ~6th rapid request carrying `Retry-After: 10`, i.e. a sustainable rate near 0.5 req/s.
# 1.5s (0.67 req/s) was still slightly over the line, so pace at 2.0s and let get_csv_bytes
# honour Retry-After when we do get throttled.
REQUEST_PAUSE = 2.0
MAX_PER_RUN = 12


def _vintage(item) -> str:
    links = item.get("links") or {}
    ver = (links.get("latest_version") or {}).get("id", "")
    return f"{ver}|{item.get('last_updated', '')}"


def _catalog(raise_transient: bool):
    try:
        items = ig.get_all_datasets()
    except Exception as e:
        if raise_transient:
            raise TransientError(f"ons_uk: catalog walk failed: {e}")
        return None
    if not items:
        if raise_transient:
            raise TransientError("ons_uk: catalog walk returned no datasets")
        return None
    return items


def current_vintage(unit) -> str | None:
    """Cheap probe: hash over every catalog dataset's (id, version|last_updated)."""
    items = _catalog(raise_transient=False)
    if not items:
        return None
    h = hashlib.sha256()
    for it in sorted(items, key=lambda x: str(x.get("id", ""))):
        h.update(f"{it.get('id','')}={_vintage(it)};".encode())
    return f"ons_uk:{h.hexdigest()[:16]}"


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


def _fetch_one(ds_id, version_href=None):
    """Thread task: download + parse one dataset -> (ds_id, keys, dates, vals), or
    (ds_id, None, None, None) on a transport/parse failure.

    `version_href` comes straight from the catalog item we already walked
    (links.latest_version.href). Using it skips resolve_csv_url()'s 1-3 EXTRA sequential
    API round-trips per dataset — that redundancy, not the parsing, is what made this
    fetcher take many minutes for only 25 datasets. We fall back to resolving only when
    the catalog didn't carry the href.
    """
    try:
        url = None
        if version_href:
            meta = ig.get_json(version_href)
            if meta:
                for dl in (meta.get("downloads") or {}).values():
                    href = dl.get("href", "")
                    if href.endswith(".csv") or "csv" in href.lower():
                        url = href
                        break
                if not url:
                    url = version_href.rstrip("/") + "/csv"
        if not url:
            url = ig.resolve_csv_url(ds_id)
        if not url:
            return ds_id, None, None, None
        content = ig.get_csv_bytes(url)
        if not content:
            return ds_id, None, None, None
        k, d, v = ig.parse_dataset_csv(ds_id, content)
        time.sleep(REQUEST_PAUSE)
        return ds_id, k, d, v
    except Exception:
        return ds_id, None, None, None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    items = _catalog(raise_transient=True)
    sidecar = _load_sidecar(out_dir)

    # Which datasets actually need work: vintage moved, or we don't hold them yet.
    todo = []
    for it in items:
        ds_id = it.get("id")
        if not ds_id:
            continue
        path = os.path.join(out_dir, f"{ds_id}.parquet")
        cur_v = _vintage(it)
        if sidecar.get(ds_id) == cur_v and blob.exists(path):
            continue
        href = ((it.get('links') or {}).get('latest_version') or {}).get('href')
        todo.append((ds_id, cur_v, href))

    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    capped = len(todo) > MAX_PER_RUN
    batch = todo[:MAX_PER_RUN]

    if batch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_fetch_one, ds_id, href): (ds_id, v) for ds_id, v, href in batch}
            for fut in as_completed(futs):
                ds_id, cur_v = futs[fut]
                _id, keys, dates, vals = fut.result()
                if keys is None:
                    tally.transient_unit()
                    continue
                if not keys:
                    # Real body, zero parseable rows. NOT structural: finalize() raises on any
                    # structural unit, which would abort the whole source and block the other
                    # ~23 datasets from publishing (run 30133686534: 2/25 -> nothing merged).
                    # Empty + vintage deliberately NOT advanced, so it retries next tick.
                    tally.empty_unit()
                    continue
                tbl = pa.table({
                    "series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64()),
                })
                path = os.path.join(out_dir, f"{ds_id}.parquet")
                before = blob.row_count(path) if blob.exists(path) else 0
                try:
                    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
                except DefinitiveError:
                    tally.transient_unit()       # isolate a guard trip to this dataset
                    continue
                published += n
                tally.added_unit(max(0, n - before))
                for k, d in zip(keys, dates):
                    iso = d.isoformat()
                    if k not in cursors or iso > cursors[k]:
                        cursors[k] = iso
                if md and (maxd is None or str(md) > str(maxd)):
                    maxd = md
                sidecar[ds_id] = cur_v           # advance ONLY after a clean publish

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    res = finalize(tally, published, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors)
    if capped:
        # More datasets still owe work — do NOT let the strategy stamp a "fully current"
        # unit vintage, or the remaining backlog would be skipped next tick.
        res.new_vintage = None
    return res
