"""S2 date-tail fetcher — FDIC BankFind quarterly call reports (US public domain, no key).

Scope: `financials.parquet` ONLY (19.9M rows, LONG format: series_key/obs_date/value, series_key =
"CERT={cert}:{field}"). The source's other four parquets — institutions / history / failures /
summary — are WIDE full snapshots with no obs_date column, so they are not date-extendable and are
deliberately left alone here rather than being half-handled.

The REPDTE range filter is genuinely honoured server-side (verified: unfiltered meta.total =
1,678,302; REPDTE:[20260101 TO 20261231] -> 4,352; [20250101 TO 20261231] -> 22,245), so this is a
real date-tail: read the stored max obs_date, request [max - lookback .. today], merge (dedup on
series_key+obs_date, never-shrink). The lookback re-pulls the boundary quarter because call reports
get AMENDED after publication — dedup lets the revised values win.

NOTE ON FRESHNESS: on-disk max is 2026-03-31 and Q2-2026 (June 30) currently returns 0 rows — the
source has not published the next quarter yet. So `no_change` here is the CORRECT, honest answer,
not staleness. There is no usable HTTP validator (ETag is a per-response body hash, no
Last-Modified), which is why the date filter — not a conditional GET — is the mechanism.

Reuses jobs.ingest_fdic.{FINANCIAL_FIELDS, fetch_all_pages, parse_date} so series_key matches disk
byte-for-byte. Store I/O via blob (R36).

HONEST-STATUS: a page failing after retries -> transient (data kept, retried). A real 200 with no
rows in the window -> no_change via an empty tally. Cursors emitted for merged series (R41).
"""
from __future__ import annotations
import datetime as dt
import os

import pyarrow as pa
import pyarrow.compute as pc

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, sane_since
from jobs import ingest_fdic as ig

SOURCE = "fdic"
PARQUET = "financials.parquet"
DEDUP = ("series_key", "obs_date")
LOOKBACK_DAYS = 200            # >= two quarters: call reports are amended after publication


def _stored_max(path) -> dt.date | None:
    if not blob.exists(path):
        return None
    t = blob.read_table(path, columns=["obs_date"])
    if t.num_rows == 0:
        return None
    md = pc.max(t.column("obs_date")).as_py()
    md = sane_since(md) if md is not None else None
    return md if isinstance(md, dt.date) else None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    path = os.path.join(out_dir, PARQUET)
    if not blob.exists(path):
        raise DefinitiveError(f"fdic: {PARQUET} absent from the store")

    tally = Tally()
    before = blob.row_count(path)
    last = _stored_max(path)
    today = dt.date.today()
    start = (last - dt.timedelta(days=LOOKBACK_DAYS)) if last else dt.date(1986, 1, 1)
    filt = f"REPDTE:[{start.strftime('%Y%m%d')} TO {today.strftime('%Y%m%d')}]"

    try:
        rows = ig.fetch_all_pages("financials", ig.FINANCIAL_FIELDS,
                                  filters=filt, sort_by="CERT")
    except Exception as e:
        raise TransientError(f"fdic: window fetch failed: {e}")

    keys, dates, vals = [], [], []
    for rec in rows:
        rec = rec.get("data", rec)           # the API wraps each record in {"data": {...}}
        cert = str(rec.get("CERT", ""))
        d = ig.parse_date(str(rec.get("REPDTE", "")))
        if not d or not cert:
            continue
        for field in ig.FINANCIAL_FIELDS:
            if field in ("REPDTE", "CERT"):
                continue
            raw = rec.get(field, None)
            if raw is None or raw == "":
                continue
            try:
                v = float(raw)
            except (TypeError, ValueError):
                continue
            keys.append(f"CERT={cert}:{field}")
            dates.append(d)
            vals.append(v)

    if not keys:
        # No observations in the window. For FDIC this is the normal state between quarterly
        # publications (Q2 not out yet), so it is an honest no_change, not a break.
        tally.empty_unit()
        return finalize(tally, before, (last.isoformat() if last else (since or None)),
                        source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    try:
        n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    except DefinitiveError:
        tally.transient_unit()
        return finalize(tally, before, (last.isoformat() if last else (since or None)),
                        source=SOURCE)

    tally.added_unit(max(0, n - before))
    cursors = {}
    for k, d in zip(keys, dates):
        iso = d.isoformat()
        if k not in cursors or iso > cursors[k]:
            cursors[k] = iso
    return finalize(tally, n, md, source=SOURCE, series_cursors=cursors)
