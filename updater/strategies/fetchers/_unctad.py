"""Shared machinery for the UNCTADstat successor family (#70) — one implementation,
thin per-source modules.

The publisher's own documented data API (contract proven live 2026-08-07): vintage is
KEYLESS — `GET /api/reportMetadata/{DS}/en` returns {version, lastUpdated}, the
publisher's own statement of the current release — and observations are keyed
(ClientId/ClientSecret via CI secrets or .env) through
`POST https://unctadstat-user-api.unctad.org/{DS}/cur/Facts`.

Whole-dataset snapshot: re-pull ONLY when the vintage token moves, MERGE with dedup
(series_key, obs_date) so a revision updates in place and never-shrink holds. ALL parse
and key logic lives in jobs/ingest_unctad_ds.py and is called, never copied — the
insee_bdm/eurostat parity lesson (R-ledger 2026-08-08): a fetcher that re-types its
ingester's logic drifts silently.

HONEST-STATUS: metadata endpoint unreachable -> transient (no vintage claim). Facts
unreachable/HTTP-4xx -> transient_unit, data kept, retried next tick. A 200 that parses
ZERO rows, or a layout the generic job refuses -> structural_unit and the vintage is NOT
recorded, so a contract break resurfaces every run instead of being sealed in.

Per-source module (see unctad_trademerchtotal.py):

    from ._unctad import make
    current_vintage, update = make("US.TradeMerchTotal", "unctad_trademerchtotal")
"""
from __future__ import annotations

import os

import pyarrow as pa

from ... import blob, config, merge
from ..base import Result
from ._common import Tally, finalize

DEDUP = ("series_key", "obs_date")


def _job():
    # jobs/ is imported lazily so a broken jobs tree cannot break fetcher discovery.
    import importlib
    return importlib.import_module("jobs.ingest_unctad_ds")


def make(ds: str, source: str):
    """Return (current_vintage, update) bound to one UNCTADstat dataset."""

    def current_vintage(unit):
        j = _job()
        meta = j.report_metadata(ds)
        return f"{meta.get('version')}|{meta.get('lastUpdated')}"

    def update(unit, since) -> Result:
        j = _job()
        out_dir = config.source_dir(source)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"{source}.parquet")
        before = blob.row_count(path)
        tally = Tally()

        try:
            cid, key = j.creds()
            meta = j.report_metadata(ds)
            rows_k, rows_d, rows_v = j.pull_rows(ds, cid, key, meta)
        except j.UnsupportedLayout:
            tally.structural_unit()
            return finalize(tally, before, None, source=source)
        except Exception:                                # noqa: BLE001 — network/contract
            tally.transient_unit()
            return finalize(tally, before, None, source=source)

        if not rows_k:
            # A reachable API that yields zero parseable observations is a CONTRACT
            # change, not an empty release — do not record the vintage.
            tally.structural_unit()
            return finalize(tally, before, None, source=source)

        tbl = pa.table({"series_key": pa.array(rows_k, pa.string()),
                        "obs_date": pa.array(rows_d, pa.date32()),
                        "value": pa.array(rows_v, pa.float64())})
        merge.merge_and_write(path, tbl, mode="merge", dedup_keys=list(DEDUP))
        after = blob.row_count(path)
        tally.added_unit(max(0, after - before))
        cursors = {}
        for k, d in zip(rows_k, rows_d):
            if k not in cursors or d > cursors[k]:
                cursors[k] = d
        # finalize's THIRD positional is last_obs (a date); cursors go in the KEYWORD —
        # the dict passed positionally bound into state.db's last_obs_date column
        # ("type 'dict' is not supported"), caught by the first real orchestrator run.
        return finalize(tally, after, max(rows_d), source=source,
                        series_cursors={k: v.isoformat() for k, v in cursors.items()})

    return current_vintage, update
