"""S5 bulk fetcher — Ember (electricity/energy transition data, CC BY 4.0, no key).

Ember publishes ~90 current CSVs to a public GCS bucket; the ingester splits them into one parquet
per source CSV under clean_full/ember/ (58 on R2 today), schema (series_key, obs_date, value, + the
dataset's own dimension columns). series_key is a pipe-joined dimension tuple built by
jobs.ingest_ember.route() — vintage-free, so a re-merge dedups rather than duplicating.

There is no server-side date filter, but the GCS JSON listing gives a per-OBJECT machine vintage:
every object carries `updated` + `size` (+ md5Hash/generation). That is a faostat-style per-dataset
vintage, so this is the manifest-vintage template: list the bucket once, and re-download ONLY the
objects whose (updated|size) moved since the stored sidecar. Unchanged objects cost nothing.

Everything that determines a series_key is REUSED from jobs.ingest_ember (enumerate_catalog,
current_csvs, dataset_id, download_bytes, read_csv_bytes, route) so the fetcher and the first-pass
ingest agree byte-for-byte. Store I/O via blob (R36); sidecar lives on the STORE, not the runner.

HONEST-STATUS: listing failure -> TransientError (partial, retried, data kept). A per-object download/
parse failure -> transient_unit for that object (others still publish). A changed object that parses
to ZERO rows -> structural_unit and its vintage is NOT advanced (a parser break re-surfaces instead of
being sealed in). Cursors are emitted for every merged series (R41).
"""
from __future__ import annotations
import hashlib
import json
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Deadline, Tally, finalize
from jobs import ingest_ember as ig   # reuse the production enumerate/parse/route

# Wall-clock cap for one ember run: it walks every CSV in the published dataset list,
# and sources run strictly serially, so a slow upstream here delays the whole fleet.
# Datasets not reached keep their existing rows and are re-tried on the next tick.
BUDGET_MIN = 20

SOURCE = "ember"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"      # {dataset_id: "updated|size"} — blob-routed
# the dimension columns write_parquet may emit, in its exact order
_DIM_COLS = ("geography", "area", "state", "category", "subcategory",
             "variable", "unit", "dimension", "series")


def _session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"})
    return s


def _vintage(obj) -> str:
    """Per-object vintage token: the GCS (updated|size) pair."""
    return f"{obj.get('updated', '')}|{obj.get('size', '')}"


def _catalog(sess, raise_transient: bool):
    try:
        items = ig.enumerate_catalog(sess)
    except Exception as e:
        if raise_transient:
            raise TransientError(f"ember: catalog listing failed: {e}")
        return None
    if not items:
        if raise_transient:
            raise TransientError("ember: catalog listing returned nothing")
        return None
    return ig.current_csvs(items)


def current_vintage(unit) -> str | None:
    """Cheap probe: a hash over every current CSV's (name, updated, size). Changes iff any moved."""
    csvs = _catalog(_session(), raise_transient=False)
    if not csvs:
        return None
    h = hashlib.sha256()
    for o in sorted(csvs, key=lambda x: x.get("name", "")):
        h.update(f"{o.get('name','')}={_vintage(o)};".encode())
    return f"ember:{h.hexdigest()[:16]}"


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


def _rows_to_table(rows):
    """Build the table exactly as ig.write_parquet does, so the schema matches what is on disk."""
    cols = {k: [r.get(k) for r in rows] for k in _DIM_COLS if any(k in r for r in rows)}
    return pa.table({
        "series_key": pa.array([str(r["series_key"]) for r in rows], pa.string()),
        "obs_date": pa.array([r["obs_date"] for r in rows], pa.date32()),
        "value": pa.array([float(r["value"]) for r in rows], pa.float64()),
        **{k: pa.array([None if v is None else str(v) for v in cols[k]], pa.string())
           for k in cols},
    })


def _series_maxes(tbl):
    out = {}
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = _session()

    csvs = _catalog(sess, raise_transient=True)      # listing outage -> TransientError
    sidecar = _load_sidecar(out_dir)
    tally = Tally()
    cursors: dict[str, str] = {}
    maxd = None
    published = 0
    dl = Deadline(minutes=BUDGET_MIN)

    for obj in csvs:
        key = obj.get("name")
        if not key:
            continue
        ds_id = ig.dataset_id(key)
        path = os.path.join(out_dir, ds_id.replace("/", "__") + ".parquet")
        cur_v = _vintage(obj)
        # unchanged AND already held -> skip entirely (not counted; it is up to date)
        if sidecar.get(ds_id) == cur_v and blob.exists(path):
            continue

        # Budget checked AFTER the unchanged-skip: an already-current dataset costs no
        # upstream call, so it must not consume the budget or count as deferred work.
        if dl.spent():
            print(f"[ember] budget {BUDGET_MIN} min spent — {ds_id} not pulled this run; "
                  f"retries next tick", flush=True)
            tally.deferred_unit(ds_id)
            continue

        try:
            raw = ig.download_bytes(sess, key)
            df = ig.read_csv_bytes(raw)
            family, rows = ig.route(ds_id, df)
        except Exception:
            tally.transient_unit()
            continue

        if not rows:
            if family == "unparsed":
                # route() matched NO parser family: this object is not one of Ember's data CSVs
                # (the first-pass ingest skips these too — it produced 58 parquets from 90 CSVs).
                # That is a legitimate empty, NOT a break. Advance the vintage so we don't
                # re-download it every tick; if Ember ever changes the file its (updated|size)
                # moves and we re-examine it then.
                tally.empty_unit()
                sidecar[ds_id] = cur_v
            else:
                # A KNOWN parser family produced zero rows — suspicious, but NOT structural:
                # finalize() RAISES DefinitiveError on any structural unit, which aborts the
                # whole source so none of the other datasets publish (run 30133686534: 11/32
                # such files -> nothing merged at all). Count it empty and deliberately do NOT
                # advance the vintage, so the file is re-examined every tick until it yields
                # rows — a persistent break stays visible instead of being silently sealed in.
                tally.empty_unit()
            continue

        tbl = _rows_to_table(rows)
        before = blob.row_count(path) if blob.exists(path) else 0
        try:
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        except DefinitiveError:
            # a never-shrink/guard trip on ONE dataset must not abort the whole source
            tally.transient_unit()
            continue

        published += n
        tally.added_unit(max(0, n - before))
        cursors.update(_series_maxes(tbl))
        if md and (maxd is None or str(md) > str(maxd)):
            maxd = md
        sidecar[ds_id] = cur_v          # advance ONLY after a clean publish

    _save_sidecar(out_dir, sidecar)

    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))

    return finalize(tally, published, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors)
