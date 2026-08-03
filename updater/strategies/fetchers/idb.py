"""S5 bulk fetcher — IDB (Inter-American Development Bank) Open Data, CKAN. CC-BY 4.0, no key.

One parquet per CKAN RESOURCE: clean_full/idb/{pkg_slug}__{resource_id[:8]}.parquet (553 on R2),
schema (series_key, obs_date, value); series_key = "IDB:" + ":".join([slug, indicator, country])
built by jobs.ingest_idb.rows_to_long — imported here so keys match disk byte-for-byte.

Manifest = CKAN `current_package_list_with_resources` (paginated, offset honored): every resource
carries `last_modified` (verified live: 2,602/2,607 populated, newest = today, proving the signal
advances on real updates). There is NO server-side date filter (datastore_search `since` -> HTTP 409)
and NO HTTP conditional-GET (no Last-Modified/ETag on the API responses), so the CKAN per-resource
last_modified IS the gate, stored in a blob-routed sidecar.

Steady-state cost is low by design: almost all resources cluster at one 2025-03 bulk load, so only
actively-maintained sets (idb-projects-dataset, procurement, sanctioned-firms) move on a given tick.

Rate: the CKAN endpoint is shared and rate-limited; the ingester's proven pacing (RATE=0.3s between
requests, 60s sleep on 429) is reused via its get_json, and this fetcher stays SERIAL (R40b — never
exceed what the server tolerates; parallelism here would just draw 429s).

HONEST-STATUS: manifest failure -> TransientError. A per-resource fetch failure -> transient_unit.
A resource over the row cap, or one that yields no date+value pattern -> empty_unit (its vintage
still advances; a real change moves last_modified and we re-examine). Cursors emitted (R41).
"""
from __future__ import annotations
import hashlib
import json
import os

import pyarrow as pa

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Deadline, Tally, finalize
from jobs import ingest_idb as ig   # reuse CKAN client + THE row->long key builder

# MAX_PER_RUN already bounds how MANY resources a run touches, but not how LONG they
# take: one large CKAN datastore paged at CKAN's own speed can eat the whole run on
# its own. Count caps and time caps are not substitutes for each other.
BUDGET_MIN = 20

SOURCE = "idb"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"     # {resource_id: last_modified}
PKG_PAGE = 100
MAX_PER_RUN = 40                    # bound a run; backlog drains over ticks


def _manifest(raise_transient: bool):
    """Walk current_package_list_with_resources -> [(pkg_slug, resource_dict), ...]."""
    out = []
    offset = 0
    while True:
        j = ig.get_json(f"{ig.BASE}/current_package_list_with_resources",
                        params={"limit": PKG_PAGE, "offset": offset})
        if not j:
            if not out and raise_transient:
                raise TransientError("idb: CKAN package list unreachable")
            break
        pkgs = j.get("result") or []
        if not pkgs:
            break
        for pkg in pkgs:
            slug = pkg.get("name", "")
            for res in pkg.get("resources", []) or []:
                if res.get("datastore_active"):
                    out.append((slug, res))
        if len(pkgs) < PKG_PAGE:
            break
        offset += PKG_PAGE
    if not out and raise_transient:
        raise TransientError("idb: CKAN manifest returned no datastore resources")
    return out


def current_vintage(unit) -> str | None:
    try:
        res = _manifest(raise_transient=False)
    except Exception:
        return None
    if not res:
        return None
    h = hashlib.sha256()
    for _slug, r in sorted(res, key=lambda x: str(x[1].get("id", ""))):
        h.update(f"{r.get('id','')}={r.get('last_modified','')};".encode())
    return f"idb:{h.hexdigest()[:16]}"


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


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    resources = _manifest(raise_transient=True)
    sidecar = _load_sidecar(out_dir)

    todo = []
    for slug, res in resources:
        rid = res.get("id", "")
        if not rid:
            continue
        lm = str(res.get("last_modified") or "")
        path = os.path.join(out_dir, f"{slug}__{rid[:8]}.parquet")
        if sidecar.get(rid) == lm and blob.exists(path):
            continue
        todo.append((slug, res, rid, lm))
    todo.sort(key=lambda x: x[2])

    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    capped = len(todo) > MAX_PER_RUN
    dl = Deadline(minutes=BUDGET_MIN)

    for slug, res, rid, lm in todo[:MAX_PER_RUN]:
        rname = (res.get("name") or res.get("description") or slug)[:40]
        if dl.spent():
            # Time budget gone even though the COUNT cap had room. Mark the run capped
            # so it reports partial and the remainder drains next tick.
            print(f"[idb] budget {BUDGET_MIN} min spent — {slug} not pulled this run; "
                  f"retries next tick", flush=True)
            tally.deferred_unit(slug)
            capped = True
            continue
        j0 = ig.get_json(f"{ig.BASE}/datastore_search", params={"resource_id": rid, "limit": 0})
        if not j0:
            tally.transient_unit()
            continue
        r0 = j0.get("result", {})
        total = r0.get("total", 0)
        fields = r0.get("fields", [])
        if not total or total > ig.MAX_RESOURCE_ROWS:
            tally.empty_unit()            # empty or over the cap: examined, advance the vintage
            sidecar[rid] = lm
            continue

        rows = ig.fetch_resource_all_rows(rid, total)
        if not rows:
            tally.transient_unit()
            continue

        keys, dates, vals = ig.rows_to_long(rows, slug, rname, fields)
        if not vals:
            tally.empty_unit()            # no date+value pattern in this resource
            sidecar[rid] = lm
            continue

        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        path = os.path.join(out_dir, f"{slug}__{rid[:8]}.parquet")
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
        sidecar[rid] = lm                 # advance ONLY after a clean publish

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    res_out = finalize(tally, published, maxd or (since or None), source=SOURCE,
                       series_cursors=cursors)
    if capped:
        res_out.new_vintage = None
    return res_out
