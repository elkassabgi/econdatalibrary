"""S2 fetcher - UN Comtrade annual merchandise totals (comtradeapi.un.org, subscription key).

REPAIRED 2026-07-31, previously blocked. The store was under-keyed: 24,086 rows collapsing to
4,154 distinct (series_key, obs_date) pairs, 19,184 of those rows in conflict - e.g.
`import_total:72` at 2014-12-31 held four different values, the largest being the sum of the
other three. Two independent causes, both now fixed in `jobs/ingest_comtrade.py`:

  1. THREE DIMENSIONS WERE DROPPED. The API returns the total alongside its breakdowns by
     motCode (mode of transport), customsCode (customs procedure) and partner2Code (secondary
     partner), and the ingest filed them all under one id. The published id means the TOTAL, so
     the repair is a FILTER on the aggregate triple (0 / C00 / 0), not a re-key - all 713
     published series ids are unchanged and no download URL broke. Verified on 8 series across
     both flows, totals and bilateral: the triple yields exactly one record per series per year.
  2. THE RESUME PATH DUPLICATED. main() seeded three parallel lists from the parquet and
     appended re-fetched rows with no dedup, so every re-run multiplied the rows it touched.
     It now accumulates into a dict keyed by (series_key, obs_date) and refuses to write if any
     duplicate survives.

Two phases, matching the published id space exactly (713 catalogued series):
    import_total:<reporter>              115
    export_total:<reporter>              134
    import_bilateral:<reporter>:<partner> 235
    export_bilateral:<reporter>:<partner> 229

A SUBSCRIPTION KEY IS REQUIRED (COMTRADE_API_KEY in .env, sent as Ocp-Apim-Subscription-Key).
The old `public/v1/preview` endpoint needed none but caps a response at 500 records and returned
only the latest period, so it could not carry the 2014-2025 history at all.

The three dimensions are passed as SERVER-SIDE query params, which is what makes this safe to
schedule: one bilateral pair drops from 16,712 rows to 12, byte-identical values. That matters
because the API silently truncates any response at 100,000 records - and it truncates the TAIL,
so the years lost are the most recent ones. Measured: a 15-partner batch lost 38 aggregate
year-rows, every one of them 2022-2025. `ig._get` refuses any response at or above that cap and
returns None rather than [], so a truncated or throttled call can never be read as "no data".
RATE is 6s; the subscription tier returns 429 well inside its documented allowance.

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
    # SAY WHY IT FAILED. This fetcher aborts the whole run with "no usable observations from any
    # phase", which names the SYMPTOM and nothing else — and every branch that could explain it
    # (exception, None-response, budget) was silent. It has been failing on CI while the very
    # same call succeeds from the workstation: probed 2026-08-01, ig.fetch_totals returned 208
    # records on the first try, and the COMTRADE_API_KEY secret IS set in CI (since 2026-07-05),
    # so a missing key is ruled out. The live hypotheses are an expired/invalid subscription or
    # 429 throttling of shared CI egress — and they are indistinguishable from the log as it
    # stands. These lines make the next CI run answer the question instead of restating it.
    n_none = n_exc = 0
    for flow, label in {"M": "import_total", "X": "export_total"}.items():
        reporters = list(ig.REPORTERS)
        batches = [reporters[i:i + ig.BATCH] for i in range(0, len(reporters), ig.BATCH)]
        print(f"[{SOURCE}] {label}: {len(reporters)} reporter(s) in {len(batches)} batch(es), "
              f"rate {ig.RATE}s", flush=True)
        for batch in batches:
            if dl.spent():
                tally.deferred_unit(f"{label}: budget — {len(batch)} reporters deferred")
                print(f"[{SOURCE}] {label}: BUDGET SPENT, {len(batch)} reporter(s) deferred",
                      flush=True)
                continue
            try:
                recs = ig.fetch_totals(batch, flow)
            except Exception as e:                           # noqa: BLE001
                tally.transient_unit(f"{label}:batch")
                n_exc += 1
                if n_exc <= 5:
                    print(f"[{SOURCE}] {label}: batch RAISED {type(e).__name__}: {str(e)[:110]}",
                          flush=True)
                continue
            if recs is None:
                # None means throttled, errored, or CAP-TRUNCATED - never "no data".
                # Treating it as [] would silently publish a short run as a success.
                tally.transient_unit(f"{label}:batch (no usable response)")
                n_none += 1
                if n_none <= 5:
                    print(f"[{SOURCE}] {label}: batch returned None (throttled, errored or "
                          f"cap-truncated) — reporters {batch[0]}..{batch[-1]}", flush=True)
                time.sleep(ig.RATE)
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
                tally.deferred_unit(f"{label}:{reporter} deferred (budget)")
                continue
            try:
                recs = ig.fetch_bilateral_totals(reporter, list(ig.MAJOR_PARTNERS), flow)
            except Exception:                                # noqa: BLE001
                tally.transient_unit(f"{label}:{reporter}")
                continue
            if recs is None:
                tally.transient_unit(f"{label}:{reporter} (no usable response)")
                time.sleep(ig.RATE)
                continue
            for rec in recs:
                _take(f"{label}:{reporter}:{rec.get('partnerCode')}", rec)
            if recs:
                tally.added_unit(len(recs), f"{label}:{reporter}")
            time.sleep(ig.RATE)

    if not keys:
        # Nothing usable from the ENTIRE run. Keep what we have and report partial rather
        # than writing an empty table or claiming no_change. The counts go IN THE MESSAGE:
        # "no usable observations" states the symptom, and the next reader needs the cause.
        raise TransientError(
            f"comtrade: no usable observations from any phase this run — "
            f"{n_none} batch(es) returned None (throttled/errored/cap-truncated), "
            f"{n_exc} raised. The same call succeeds from the workstation, so suspect the "
            f"subscription key's validity or 429s against shared CI egress, not the code.")

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
