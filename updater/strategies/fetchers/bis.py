"""S1 bulk fetcher — BIS Consolidated & Locational Banking Statistics (data.bis.org, no key).

BIS publishes two flat-CSV bulk zips at STABLE urls (no date or version in the path):
    CBS  https://data.bis.org/static/bulk/WS_CBS_PUB_csv_flat.zip
    LBS  https://data.bis.org/static/bulk/WS_LBS_D_PUB_csv_flat.zip
so the honest change signal is each object's own HTTP validator (ETag / Last-Modified /
Content-Length) — re-download only when it moves.

WHY THE INGEST COULD NOT SIMPLY BE WRAPPED. jobs/ingest_bis_cbs_lbs.py is backfill-shaped:
`download()` returns early when the zip is already on disk and `ingest_zip()` returns early
when the parquet already exists. Both are right for a one-off load and inert as an updater —
scheduled, they would do nothing forever while reporting success. The parse was also inline in
`ingest_zip`, writing straight to a ParquetWriter, so there was nothing callable to reuse.

Rather than copy the parser (which breaks the duplication invariant the moment either side is
edited), the parse was EXTRACTED into `ingest_bis_cbs_lbs.iter_rows()` — a pure generator
yielding (series_key, obs_date, value, freq) with no skip rules, no writer and no logging
policy. The ingest now delegates to it too, so there is exactly ONE parse and the fetcher and
the first-pass ingest cannot drift apart.

MEMORY. The CBS/LBS flat CSVs are tens of millions of rows, so rows are accumulated in
BATCH-sized chunks and merged incrementally rather than materialised whole.

HONEST-STATUS: a HEAD/validator failure -> the zip is treated as changed (fetch anyway, which
merge dedups) rather than silently skipped. A download or parse failure -> transient_unit for
that zip only, so CBS failing cannot stop LBS publishing. A zip that parses to ZERO rows ->
structural_unit and its vintage is NOT advanced, so a parser break resurfaces next run instead
of being sealed in.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import tempfile

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, finalize, merge_cursors
from ._vintage import http_vintage
from jobs import ingest_bis_cbs_lbs as ig      # BULK urls + the EXTRACTED pure parser

SOURCE = "bis"
DEDUP = ("series_key", "obs_date")
SIDECAR = "_bulk_vintages.json"
BUDGET_MIN = 25
# Backstop for the size-only gate: re-pull a zip whose Content-Length has not moved in this
# many days, so a same-byte-count revision cannot hide forever.
MAX_AGE_DAYS = 30
# RAISED from 500,000 on 2026-07-30, after bis aborted the whole daily run.
#
# Every merge_and_write re-reads and rewrites the ENTIRE existing parquet, so this constant
# sets how many times that happens. LBS.parquet holds 36,379,671 rows, so 500,000 meant 73
# merge cycles over a table growing toward 36M — the quadratic shape the ledger calls R169,
# and the exact trap cepii_gravity's docstring already spells out (it uses 20M for this
# reason; bis never got the same treatment). Memory went 1,516MB -> 15,700MB in under seven
# minutes and Arrow aborted the process: std::length_error: vector::_M_default_append,
# exit 134 (SIGABRT), taking the run down after only 3 sources had completed.
#
# 4M keeps the Python row buffer near 1 GB (these are 4-tuples, not cepii's 3-tuples) while
# cutting LBS from 73 merges to 10. Note this buys TOTAL WORK and pool churn, not a lower
# ceiling: the per-merge peak is set by the size of the existing file, not by the batch.
# The ceiling is what merge_and_write's new pool release addresses.
BATCH = 4_000_000
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}


def _validator(url, sess) -> "str | None":
    """Content-Length-first validator for the BIS bulk zips. NOT http_vintage — MEASURED.

    data.bis.org is served by several origin replicas whose copies of the same file carry
    DIFFERENT mtimes, so the ETag and Last-Modified FLAP between requests while the bytes are
    unchanged. Two HEADs 15s apart on WS_LBS_D_PUB_csv_flat.zip:

        ETag W/"153f3e65-19f89d4b74e"  LM Wed, 22 Jul 2026 12:37:26 GMT  CL 356466277
        ETag W/"153f3e65-19f88bb24fb"  LM Wed, 22 Jul 2026 07:29:53 GMT  CL 356466277

    Last-Modified went BACKWARDS by five hours, which no real republish can do, and
    Content-Length was identical. These are Apache-style ETags, `"<size-hex>-<mtime-hex>"`:
    0x153f3e65 = 356,466,277 = exactly Content-Length, so only the mtime half moves. Since
    http_vintage prefers ETag > Last-Modified > Content-Length, it picks the one field that
    flaps, and the gate would report "changed" on nearly every run — re-downloading and
    re-parsing 440 MB daily while looking like a working cache (the fed_board defect, R164).

    So the size is the honest signal. It is WEAKER than a hash — a revision that preserves the
    byte count would be invisible — which is why update() also force-refreshes on MAX_AGE_DAYS.
    """
    try:
        r = sess.head(url, headers=UA, timeout=60, allow_redirects=True)
    except Exception:                                        # noqa: BLE001
        return None
    cl = r.headers.get("Content-Length")
    if cl:
        return f"len={cl}"
    return http_vintage(url, session=sess)                   # no CL -> best available


def current_vintage(unit) -> "str | None":
    """Hash of both bulk objects' size validators. Moves iff BIS republishes either zip."""
    sess = requests.Session()
    parts = []
    for name, url in sorted(ig.BULK.items()):
        v = _validator(url, sess)
        if not v:
            return None                                      # unknown -> let cadence decide
        parts.append(f"{name}={v}")
    h = hashlib.sha256(";".join(parts).encode()).hexdigest()[:16]
    return f"bis:{h}"


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


def _download(sess, url, dest) -> None:
    """Straight download — deliberately NOT ig.download(), which skips when a file exists."""
    with sess.get(url, headers=UA, stream=True, timeout=1800) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    fh.write(chunk)


def _older_than(iso, days) -> bool:
    """True when `iso` is absent or older than `days` — the size gate's staleness backstop."""
    if not iso:
        return True
    try:
        return (dt.date.today() - dt.date.fromisoformat(iso)).days >= days
    except ValueError:
        return True


def _table(rows):
    return pa.table({
        "series_key": pa.array([r[0] for r in rows], pa.string()),
        "obs_date": pa.array([r[1] for r in rows], pa.date32()),
        "value": pa.array([r[2] for r in rows], pa.float64()),
        "freq": pa.array([r[3] for r in rows], pa.string()),
    })


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = requests.Session()
    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    maxd = None
    cursors: dict = {}          # §5.7 changed-series set, per republished bulk zip
    dl = Deadline(minutes=BUDGET_MIN)

    for name, url in sorted(ig.BULK.items()):
        path = os.path.join(out_dir, f"{name}.parquet")
        cur_v = _validator(url, sess) or ""
        stale = _older_than(sidecar.get(f"{name}__pulled"), MAX_AGE_DAYS)
        if cur_v and sidecar.get(name) == cur_v and blob.exists(path) and not stale:
            continue                                         # already current, costs nothing
        if stale and cur_v and sidecar.get(name) == cur_v:
            # Size unchanged for MAX_AGE_DAYS. Because the size gate cannot see a same-byte-count
            # revision, re-pull anyway rather than trust it indefinitely. merge dedups, so the
            # only cost is bandwidth.
            print(f"[bis] {name}: size unchanged {MAX_AGE_DAYS}d — forced re-pull", flush=True)

        if dl.spent():
            print(f"[bis] budget {BUDGET_MIN} min spent — {name} deferred to next run",
                  flush=True)
            tally.deferred_unit(name)
            continue

        tmp = os.path.join(tempfile.gettempdir(), f"bis_{name}.zip")
        try:
            _download(sess, url, tmp)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(name)
            continue

        n_rows = 0
        before = blob.row_count(path) if blob.exists(path) else 0
        buf = []
        try:
            for row in ig.iter_rows(name, tmp):
                buf.append(row)
                if len(buf) >= BATCH:
                    n, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                                  dedup_keys=DEDUP)
                    n_rows, buf = n, []
                    if md and (maxd is None or str(md) > str(maxd)):
                        maxd = md
            if buf:
                n, md = merge.merge_and_write(path, _table(buf), mode="merge",
                                              dedup_keys=DEDUP)
                n_rows = n
                if md and (maxd is None or str(md) > str(maxd)):
                    maxd = md
        except DefinitiveError:
            tally.transient_unit(name)                       # one zip must not sink the source
            continue
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(name)
            continue
        finally:
            try:
                os.remove(tmp)
            except OSError:
                pass

        if not n_rows:
            # Parsed to nothing from a zip that downloaded fine — a real break. Do NOT
            # advance the vintage, so it resurfaces instead of being sealed in.
            tally.structural_unit(f"{name}: zip parsed to 0 rows")
            continue

        published += n_rows
        merge_cursors(cursors, path)
        tally.added_unit(max(0, n_rows - before), name)
        if cur_v:
            sidecar[name] = cur_v                            # advance ONLY after a clean publish
            sidecar[f"{name}__pulled"] = dt.date.today().isoformat()

    if len(cursors) >= CURSOR_CAP:
        print(f"[bis] cursor set hit the {CURSOR_CAP:,} cap — further changed series are not "
              f"individually reported; the orchestrator's derive-all path covers small "
              f"catalogs", flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
