"""S5 bulk fetcher — Our World in Data grapher charts (CC BY 4.0, no key).

One parquet per chart slug under clean_full/owid/<slug>.parquet (3,786 on R2), schema
(series_key, obs_date, value); series_key = "<slug>|<value_col>|<code-or-entity>", built by
jobs.ingest_owid.parse_chart_csv which this fetcher IMPORTS so keys match disk byte-for-byte.

Manifest = https://ourworldindata.org/sitemap.xml: every /grapher/ URL carries a per-slug
<lastmod>, i.e. a faostat-style per-dataset vintage (4,541 slugs; only ~24% move in any 45-day
window, so most of the catalog is skipped every tick). HTTP Last-Modified is NOT usable here —
it is Cloudflare cache-generation time and resets to "now" on every regeneration — so the sitemap
<lastmod> is the gate, stored in a blob-routed sidecar. New slugs surface from the same fetch.

OWID returns **403 for non-redistributable charts** (third-party data). Those are skipped and
their vintage is deliberately NOT advanced, so a chart that later becomes redistributable is
picked up rather than permanently sealed out.

Store I/O via blob only (R36) — the first-pass ingester is full of raw local-path traps
(os.path.exists / pq.read_metadata / os.listdir on OUT) that must NOT be copied here.

HONEST-STATUS: sitemap failure -> TransientError (partial, retried, data kept). Per-slug 5xx/429/
network -> transient_unit. 403 non-redistributable / 404 missing -> empty_unit (no vintage bump).
A 200 that parses to nothing -> structural_unit. Cursors emitted for merged series (R41).
"""
from __future__ import annotations
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize
from jobs import ingest_owid as ig   # reuse http_get + THE chart parser

SOURCE = "owid"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_sitemap_vintages.json"   # {slug: lastmod}
MAX_WORKERS = 6
# Bound a single CI run; the remainder drains over consecutive ticks (the unit vintage is
# suppressed while a backlog remains, so nothing is skipped).
MAX_PER_RUN = 150

_URL_BLOCK = re.compile(r"<url>(.*?)</url>", re.S)
_LOC = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>")
_LASTMOD = re.compile(r"<lastmod>\s*([^<\s]+)\s*</lastmod>")
_SLUG = re.compile(r"/grapher/([^<\s?&#/]+)")


def _sitemap(raise_transient: bool):
    """Return {slug: lastmod} from OWID's sitemap, or None/raise on failure."""
    try:
        r = ig.http_get(ig.SITEMAP)
    except Exception as e:
        if raise_transient:
            raise TransientError(f"owid: sitemap fetch failed: {e}")
        return None
    if getattr(r, "status_code", 0) != 200:
        if raise_transient:
            raise TransientError(f"owid: sitemap HTTP {getattr(r, 'status_code', '?')}")
        return None
    out = {}
    for block in _URL_BLOCK.findall(r.text):
        loc = _LOC.search(block)
        if not loc:
            continue
        m = _SLUG.search(loc.group(1))
        if not m:
            continue
        lm = _LASTMOD.search(block)
        out[m.group(1)] = lm.group(1) if lm else ""
    if not out:
        if raise_transient:
            raise TransientError("owid: sitemap had no /grapher/ slugs")
        return None
    return out


def current_vintage(unit) -> str | None:
    """Cheap probe: hash over every slug's sitemap <lastmod>."""
    sm = _sitemap(raise_transient=False)
    if not sm:
        return None
    h = hashlib.sha256()
    for slug in sorted(sm):
        h.update(f"{slug}={sm[slug]};".encode())
    return f"owid:{h.hexdigest()[:16]}"


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
                            json.dumps(data, sort_keys=True).encode("utf-8"))


def _fetch_slug(slug):
    """Thread task -> (slug, status, parsed). status in ok|empty|transient|structural."""
    url = f"{ig.BASE}/{slug}.csv?csvType=full&useColumnShortNames=true"
    try:
        r = ig.http_get(url)
    except Exception:
        return slug, "transient", None
    sc = getattr(r, "status_code", 0)
    if sc == 403:
        return slug, "empty", None          # non-redistributable carve-out; do NOT seal it in
    if sc in (404, 410):
        return slug, "empty", None
    if sc != 200:
        return slug, "transient", None
    try:
        parsed = ig.parse_chart_csv(r.text, slug)
    except Exception:
        return slug, "transient", None
    if not parsed or not parsed[0]:
        return slug, "structural", None
    return slug, "ok", parsed


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    sm = _sitemap(raise_transient=True)
    sidecar = _load_sidecar(out_dir)

    todo = []
    for slug, lastmod in sm.items():
        path = os.path.join(out_dir, f"{slug}.parquet")
        if sidecar.get(slug) == lastmod and blob.exists(path):
            continue                        # unchanged and held -> skip entirely
        todo.append((slug, lastmod))
    todo.sort()

    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    capped = len(todo) > MAX_PER_RUN
    batch = todo[:MAX_PER_RUN]

    if batch:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(_fetch_slug, s): (s, lm) for s, lm in batch}
            for fut in as_completed(futs):
                slug, lastmod = futs[fut]
                _s, status, parsed = fut.result()
                if status == "transient":
                    tally.transient_unit(); continue
                if status == "empty":
                    tally.empty_unit(); continue
                if status == "structural":
                    tally.structural_unit(); continue

                keys, dates, vals = parsed[0], parsed[1], parsed[2]
                tbl = pa.table({
                    "series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64()),
                })
                path = os.path.join(out_dir, f"{slug}.parquet")
                before = blob.row_count(path) if blob.exists(path) else 0
                try:
                    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
                except DefinitiveError:
                    tally.transient_unit(); continue
                published += n
                tally.added_unit(max(0, n - before))
                for k, d in zip(keys, dates):
                    iso = d.isoformat()
                    if k not in cursors or iso > cursors[k]:
                        cursors[k] = iso
                if md and (maxd is None or str(md) > str(maxd)):
                    maxd = md
                sidecar[slug] = lastmod      # advance ONLY after a clean publish

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    res = finalize(tally, published, maxd or (since or None), source=SOURCE,
                   series_cursors=cursors)
    if capped:
        res.new_vintage = None               # backlog remains -> don't claim fully current
    return res
