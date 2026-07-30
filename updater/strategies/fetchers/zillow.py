"""S1 bulk fetcher — Zillow Research public CSVs (files.zillowstatic.com, no key).

Zillow publishes ~208 wide CSVs (ZHVI, ZORI, inventory, days-to-pending, …), one per
metric x geography, discovered from the `var data` object embedded in the research page.

DISCOVERY IS LIVE, NOT PINNED. `ig.refresh_catalog()` re-scrapes the research page every run,
so a metric or geography Zillow ADDS is picked up instead of being invisible forever. The
frozen-list failure (stats_nz, R159) is the reason this calls refresh rather than
`load_catalog()`; the cached `_zillow_files.json` is only the fallback when the page is
unreachable, where a stale list beats no run at all.

THE CACHE TRAP, which the registry's own adapter note flags and which would have silently
neutered this fetcher: `ig.fetch_csv_text(sess, url, cache_raw=True)` RETURNS THE CACHED RAW
COPY when one exists on disk. Wired that way on a schedule, every run would re-parse the same
bytes from `data/raw/zillow/` forever and report success. So `cache_raw=False` is passed
explicitly, and this comment exists so nobody "optimises" it back.

VINTAGE — MEASURED, NOT ASSUMED (the R164 rule). Zillow's CDN serves stable content-derived
ETags: two HEADs 25s apart on three files returned identical ETag, Last-Modified and
Content-Length (e.g. Metro_zhvi ETag "fcde3e8e9a6fca03f6f3d30a53c663e5", LM Thu 16 Jul 2026
15:18:24, CL 4,444,528). Unlike federalreserve.gov (Last-Modified regenerated per request) and
data.bis.org (ETag flapping across replicas), an http_vintage gate is genuinely correct here.
Per-file validators live in a sidecar on the STORE, so an unchanged CSV costs one HEAD.

PUBLISHING. `ig.write_cube` writes `<dataset>.parquet` + `<dataset>__series.parquet` with a
raw `pq.write_table`, which never reaches R2 — only blob does (R36). ig.OUT is the same
directory as config.source_dir, so the bytes are already at their store path and
`blob.publish_file` streams them up without re-reading the table.

HONEST-STATUS: discovery failure with no cached fallback -> TransientError. A per-file
download/parse failure -> transient_unit for that file only, so one bad CSV cannot stop the
other 207. A file that parses to ZERO observations -> structural_unit with its validator NOT
recorded, so a parser break resurfaces next run instead of being sealed in.
"""
from __future__ import annotations
import hashlib
import json
import os

import requests

from ... import config, blob
from ...errors import TransientError
from ..base import Result
from ._common import Deadline, Tally, finalize
from ._vintage import http_vintage
from jobs import ingest_zillow as ig     # live discovery + the production parser/writer

SOURCE = "zillow"
SIDECAR = "_file_vintages.json"
BUDGET_MIN = 20
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def _session():
    s = requests.Session()
    s.headers.update(UA)
    return s


def _catalog(sess):
    """Live file records, falling back to the cached list only if the page is unreachable."""
    try:
        recs = ig.refresh_catalog(sess)
        if recs:
            return recs
    except Exception:                                        # noqa: BLE001
        pass
    try:
        return ig.load_catalog()
    except Exception:                                        # noqa: BLE001
        return []


def current_vintage(unit):
    """Hash of the discovered URL set + each CSV's HTTP validator.

    Moves when Zillow republishes ANY file or adds a new one — both are real changes. Costs
    ~208 HEADs, which on a monthly cadence is nothing.
    """
    sess = _session()
    recs = _catalog(sess)
    if not recs:
        return None
    h = hashlib.sha256()
    for rec in sorted(recs, key=lambda r: r["url"]):
        url = rec["url"]
        try:
            v = http_vintage(url, session=sess) or ""
        except Exception:                                    # noqa: BLE001
            v = ""
        h.update(f"{url}={v};".encode())
    return f"zillow:{len(recs)}:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def _publish(out_dir, dataset) -> int:
    """PUT the parquet(s) write_cube just wrote. Row count from the footer, not a full read."""
    import pyarrow.parquet as pq
    total = 0
    for suffix in (f"{dataset}.parquet", f"{dataset}__series.parquet"):
        local = os.path.join(ig.OUT, suffix)
        if not os.path.exists(local):
            continue
        blob.publish_file(os.path.join(out_dir, suffix))
        if suffix == f"{dataset}.parquet":
            total = pq.ParquetFile(local).metadata.num_rows
    return total


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(ig.OUT, exist_ok=True)
    sess = _session()

    recs = _catalog(sess)
    if not recs:
        raise TransientError("zillow: research-page discovery returned nothing and no cache")

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    dl = Deadline(minutes=BUDGET_MIN)

    for rec in sorted(recs, key=lambda r: r["url"]):
        url = rec["url"]
        dataset = ig._dataset_name(url)
        try:
            cur_v = http_vintage(url, session=sess) or ""
        except Exception:                                    # noqa: BLE001
            cur_v = ""                                       # unknown -> treat as changed
        stored = os.path.join(out_dir, f"{dataset}.parquet")
        if cur_v and sidecar.get(dataset) == cur_v and blob.exists(stored):
            unchanged += 1
            continue

        if dl.spent():
            print(f"[zillow] budget {BUDGET_MIN} min spent — {dataset} deferred", flush=True)
            tally.transient_unit(dataset)
            continue

        try:
            # cache_raw=False is LOAD-BEARING: True returns the cached copy and the source
            # would never advance again. See the module docstring.
            text = ig.fetch_csv_text(sess, url, cache_raw=False)
            obs_rows, series_rows = ig.parse_csv(text, url)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(dataset)
            continue

        if not obs_rows:
            # Downloaded fine, parsed to nothing — a real break. Validator NOT recorded.
            tally.structural_unit(f"{dataset}: CSV parsed to 0 observations")
            continue

        try:
            ig.write_cube(dataset, obs_rows, series_rows, rec)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(dataset)
            continue

        published += _publish(out_dir, dataset)
        tally.added_unit(len(obs_rows), dataset)
        if cur_v:
            sidecar[dataset] = cur_v                         # record ONLY after publishing

    if unchanged:
        print(f"[zillow] {unchanged}/{len(recs)} file(s) unchanged — skipped", flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, since or None, source=SOURCE)
