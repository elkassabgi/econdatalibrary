"""S2 fetcher — UN Comtrade annual merchandise totals (comtradeapi.un.org, no key).

!! NOT PROMOTED TO live — BLOCKED ON A DATA REPAIR (found 2026-07-30). Do not set live:true
   until the key is fixed; this module is committed so the analysis is not lost.

   THE PUBLISHED STORE IS UNDER-KEYED. comtrade.parquet holds 24,086 rows but only 4,154
   distinct (series_key, obs_date) pairs, and 1,240 of those pairs carry CONFLICTING values —
   e.g. `import_total:72` at 2014-12-31 appears as 1,603,998,886.636 AND 2,729,735,494.827 AND
   4,816,420,248.446. The ingest keys on `{flow}:{reporter}` only, so whatever dimension
   actually separates those records (mode of transport / customs / mos code) is dropped, and
   several genuinely different observations collapse onto one id. That is the vdem-vparty and
   unsdg defect again.
   Consequence: ANY merge is wrong. Deduping on (series_key, obs_date) discards real values
   (24,086 -> 4,154, proven by merging a single row), and not deduping leaves the ambiguity in
   place. merge_and_write's never-shrink guard correctly REFUSED, which is how this surfaced.
   Fix the key first (carry the missing dimension, as bls does with its 3-column identity),
   re-ingest, then promote.

   ALSO MEASURED: the `public/v1/preview` tier caps a response at 500 records and returned only
   period 2025, so a full re-fetch cannot reproduce the stored 2014-2025 history anyway. The
   docstring below originally claimed this endpoint "returns whole annual histories" — that was
   my assumption, and the probe evidence (5 reporters -> 1 record) contradicted it.

Two phases, matching the published id space exactly (713 catalogued series):
    import_total:<reporter>              115
    export_total:<reporter>              134
    import_bilateral:<reporter>:<partner> 235
    export_bilateral:<reporter>:<partner> 229

NO KEY IS NEEDED — this is the `public/v1/preview` endpoint, which serves annual HS totals
without a subscription. The rate limit is real though, so `ig.RATE` (2s) is honoured between
calls and 429s back off 60s x attempt inside `ig.fetch_totals`.

WHY main() COULD NOT BE WRAPPED. `jobs/ingest_comtrade.py:main()` builds `done_combos` from the
series_keys already in the parquet and skips every combo it finds:

    todo_reporters = [r for r in REPORTERS if f"{flow_label}:{r}" not in done_combos]

Correct for a resumable backfill, inert as an updater: a reporter already on disk is never
looked at again, so Comtrade can publish another year for it and we would never fetch it.
Scheduled, that would refresh only BRAND-NEW reporters while asserting the rest were current.

So this re-fetches every combo and lets `merge_and_write` dedup on (series_key, obs_date):
existing series extend, and the only cost is bandwidth. `fetch_totals`, `fetch_bilateral_totals` and `parse_record` are reused
verbatim, and MAJOR / MAJOR_PARTNERS were LIFTED out of main() to module scope so the two sides
cannot drift (R33).

VINTAGE: none. The preview endpoint exposes no catalogue, no per-dataset timestamp and no
useful HTTP validator, so `current_vintage` returns None and the strategy falls back to the
cadence — which is the documented, safe behaviour (a fetch happens; merge dedups). Returning a
fabricated token would be worse: it would either never match (re-pull forever, the fed_board
defect) or always match (freeze the source).

HONEST-STATUS: a batch that fails -> transient_unit for that batch only. Zero usable rows
across the WHOLE run -> TransientError, so existing data is kept and the run is partial rather
than reporting a hollow success.
"""
from __future__ import annotations
import os
import time

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Deadline, Tally, finalize
from jobs import ingest_comtrade as ig     # REPORTERS/MAJOR + the production fetch+parse

SOURCE = "comtrade"
DEDUP = ("series_key", "obs_date")
BUDGET_MIN = 20


def current_vintage(unit):
    """None by design — see the module docstring. Cadence-gated, never a fabricated token."""
    return None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "comtrade.parquet")

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    keys, dates, vals = [], [], []
    cursors: dict = {}

    def _take(rows_key, rec):
        got = ig.parse_record(rec, rows_key)
        if not got:
            return
        k, d, v = got
        keys.append(k)
        dates.append(d)
        vals.append(v)
        iso = d.isoformat()
        if cursors.get(k, "") < iso:
            cursors[k] = iso

    # ---- Phase 1: totals, every reporter re-fetched (no done_combos skip) --------
    for flow, label in {"M": "import_total", "X": "export_total"}.items():
        reporters = list(ig.REPORTERS)
        batches = [reporters[i:i + ig.BATCH] for i in range(0, len(reporters), ig.BATCH)]
        for batch in batches:
            if dl.spent():
                tally.transient_unit(f"{label}: budget — {len(batch)} reporters deferred")
                continue
            try:
                recs = ig.fetch_totals(batch, flow)
            except Exception:                                # noqa: BLE001
                tally.transient_unit(f"{label}:batch")
                continue
            for rec in recs:
                _take(f"{label}:{rec.get('reporterCode')}", rec)
            if recs:
                tally.added_unit(len(recs), label)
            time.sleep(ig.RATE)

    # ---- Phase 2: bilateral, MAJOR x MAJOR_PARTNERS ------------------------------
    for flow, label in {"M": "import_bilateral", "X": "export_bilateral"}.items():
        for reporter in ig.MAJOR:
            if dl.spent():
                tally.transient_unit(f"{label}:{reporter} deferred (budget)")
                continue
            try:
                recs = ig.fetch_bilateral_totals(reporter, list(ig.MAJOR_PARTNERS), flow)
            except Exception:                                # noqa: BLE001
                tally.transient_unit(f"{label}:{reporter}")
                continue
            for rec in recs:
                _take(f"{label}:{reporter}:{rec.get('partnerCode')}", rec)
            if recs:
                tally.added_unit(len(recs), f"{label}:{reporter}")
            time.sleep(ig.RATE)

    if not keys:
        # Nothing usable from the ENTIRE run. Keep what we have and report partial rather
        # than writing an empty table or claiming no_change.
        raise TransientError("comtrade: no usable observations from any phase this run")

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    before = blob.row_count(path) if blob.exists(path) else 0
    total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    print(f"[comtrade] {len(keys):,} obs across {len(cursors):,} series; "
          f"store {before:,} -> {total:,}", flush=True)
    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
