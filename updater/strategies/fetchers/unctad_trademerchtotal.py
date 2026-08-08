"""S1 bulk fetcher — UNCTADstat US.TradeMerchTotal (first successor source, #70).

The publisher's own documented data API (contract proven live 2026-08-07):
vintage is KEYLESS — `GET /api/reportMetadata/US.TradeMerchTotal/en` returns
{version, lastUpdated}, the publisher's own statement of the current release —
and observations are keyed (ClientId/ClientSecret from .env / CI secrets) via
`POST https://unctadstat-user-api.unctad.org/US.TradeMerchTotal/cur/Facts`.

Whole-dataset snapshot (81,760 obs / 1,220 series measured at first ingest — two
measure groups, ~48k cells each): re-pull ONLY when the vintage token moves, MERGE
with dedup (series_key, obs_date) so a revision updates in place and never-shrink
holds. Parse + key scheme are IMPORTED from jobs/ingest_unctad_ds.py — the
insee_bdm/eurostat parity lesson (R-ledger 2026-08-08): a fetcher that re-types its
ingester's logic drifts, so this one refuses to own a copy.

HONEST-STATUS: metadata endpoint unreachable -> transient (no vintage claim).
Facts unreachable/HTTP-4xx -> transient_unit, data kept, retried next tick. A 200
that parses ZERO rows -> structural_unit and the vintage is NOT recorded, so a
contract break resurfaces every run instead of being sealed in.
"""
from __future__ import annotations

import os

import pyarrow as pa

from ... import blob, config, merge
from ..base import Result
from ._common import Tally, finalize

SOURCE = "unctad_trademerchtotal"
DS = "US.TradeMerchTotal"
DEDUP = ("series_key", "obs_date")


def _job():
    # jobs/ is imported lazily so a broken jobs tree cannot break fetcher discovery.
    import importlib
    return importlib.import_module("jobs.ingest_unctad_ds")


def current_vintage(unit):
    j = _job()
    meta = j.report_metadata(DS)
    return f"{meta.get('version')}|{meta.get('lastUpdated')}"


def update(unit, since) -> Result:
    j = _job()
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{SOURCE}.parquet")
    before = blob.row_count(path)
    tally = Tally()

    try:
        cid, key = j.creds()
        meta = j.report_metadata(DS)
        defaults = meta["defaults"]
        dims = [d for axe in ("rowAxe", "colAxe", "pageAxe")
                for d in defaults.get(axe) or []]
        key_dims = [d for d in dims if not d.get("isTime")]
        tdim = [d for d in dims if d.get("isTime")]
        if len(tdim) != 1:
            tally.structural_unit()
            return finalize(tally, before, None, source=SOURCE)
        tfield = tdim[0]["field"]
        kfields = [d["field"] for d in key_dims]
        measures = [m["code"] for g in defaults.get("observations") or []
                    for m in g.get("measures", []) if m.get("magnitude") == 1]

        import csv as _csv
        import io as _io
        rows_k, rows_d, rows_v = [], [], []
        for mcode in measures:
            select = (", ".join(f"{f}/Code" for f in kfields) +
                      f", {tfield}, M{mcode}/Value")
            text = j.facts_csv(DS, select, cid, key)
            for rec in _csv.DictReader(_io.StringIO(text)):
                vals = [rec.get(f"{f}_Code", "") for f in kfields]
                tv = rec.get(tfield) or rec.get(f"{tfield}_Code", "")
                vv = rec.get(f"M{mcode}_Value", "")
                d = j.parse_time(tv, tfield.lower() == "year")
                if d is None or vv in ("", None):
                    continue
                try:
                    v = float(vv)
                except ValueError:
                    continue
                rows_k.append(".".join(vals + [f"M{mcode}"]))
                rows_d.append(d)
                rows_v.append(v)
    except Exception:                                    # noqa: BLE001 — network/contract
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    if not rows_k:
        # A reachable API that yields zero parseable observations is a CONTRACT change,
        # not an empty release — do not record the vintage.
        tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

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
    # finalize's THIRD positional is last_obs (a date); cursors go in the KEYWORD.
    # Passing the dict positionally bound it into state.db's last_obs_date column
    # ("type 'dict' is not supported") — caught by the first real orchestrator run.
    return finalize(tally, after, max(rows_d), source=SOURCE,
                    series_cursors={k: v.isoformat() for k, v in cursors.items()})
