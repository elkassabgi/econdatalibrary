"""S2 date-tail fetcher — Bank of Canada Valet (bankofcanada.ca/valet, no key).

12,862 published series, 2,732,162 observations. There is no ingest script for this source at
all — it was never in the registry under any name, so it had no updater path and could not
refresh. An unregistered source is not "failing"; the orchestrator simply never iterates it.

IDS NEED NO CROSSWALK, verified as a FULL set comparison rather than a sample: Valet's own
series names ARE our series_keys (`A.BCPI`, `A.BCNE`, `A.ENER`). Valet lists 15,906 series
against our 12,862 published ids — **12,862 reproduced exactly, 0 of ours missing upstream**,
and 3,044 Valet series we do not yet carry (new coverage, deliberately not added here: that is
a cataloguing decision, not an update).

WHY NOT DBnomics. Its BOC provider was last indexed 2025-02-15, and — the sharper reason — a
matching provider NAME is not provenance (R171). These ids are Valet's, so Valet is the source.

DATE-TAIL, BATCHED. Valet accepts many series in one call and returns a WIDE row per date:
    {"observations": [{"d": "2024-01-01", "A.BCPI": {"v": "123.4"}, "A.BCNE": {...}}, …]}
so each request covers BATCH series at once. `start_date` is the MINIMUM stored max(obs_date)
across the batch, which re-fetches a little history for the batch's freshest members —
cheaper than one call per series (12,862 calls) and harmless, because merge_and_write dedups
on (series_key, obs_date). Series we have never seen fall back to FIRST_START.

VINTAGE. A hash of Valet's series-name list. That moves when BoC adds or retires a series,
which is a real change worth a run — but it deliberately does NOT try to encode "some series
got new observations", because Valet exposes no catalogue-level timestamp for that. A token
that cannot see the common case would be worse than one that only sees additions: the strategy
falls back to the cadence, and the date-tail keeps each call small. Never a fabricated token
(the fed_board defect: a gate that can never match re-pulls everything forever).

HONEST-STATUS: the series list unreachable -> TransientError (partial, retried, data kept). A
failed batch -> transient_unit for that batch only. Zero usable observations across the WHOLE
run -> handled by finalize as no-change rather than an empty write, because for a date-tail on
mostly-annual data "nothing new today" is the normal steady state, not a break.
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import time
import urllib.error
import urllib.request

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Deadline, Tally, _max_by_key, finalize

SOURCE = "boc"
DEDUP = ("series_key", "obs_date")
BASE = "https://www.bankofcanada.ca/valet"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
BATCH = 25
RATE = 0.2
BUDGET_MIN = 20
FIRST_START = "1900-01-01"      # only used for a series we have never stored


def _get(url, tries=3):
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, headers=UA), timeout=120) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return None                                  # a bad batch, not a transient
            last = e
        except Exception as e:                               # noqa: BLE001
            last = e
        if attempt < tries - 1:
            time.sleep(2 ** attempt)
    raise TransientError(f"boc: GET failed {url[:90]} :: {last!r}")


def _series_names():
    d = _get(f"{BASE}/lists/series/json")
    return sorted((d or {}).get("series") or {})


def current_vintage(unit):
    """Hash of Valet's series-name list. Moves on additions/retirements, not on new obs."""
    try:
        names = _series_names()
    except Exception:                                        # noqa: BLE001
        return None
    if not names:
        return None
    h = hashlib.sha256()
    for n in names:
        h.update(f"{n};".encode())
    return f"boc:{len(names)}:{h.hexdigest()[:16]}"


def _stored_maxes(path) -> dict:
    """{series_key: max obs_date ISO} from what we already hold."""
    if not blob.exists(path):
        return {}
    tbl = blob.read_table(path, columns=["series_key", "obs_date"])
        # _max_by_key, NOT group_by. Arrow indexes string data with int32 offsets; past 2 GiB in one
    # column group_by dereferences past the overflowed offsets and KILLS THE PROCESS
    # (0xC0000005 / SIGABRT) - it does not raise, so no try/except catches it. ons_uk died that
    # way on 2026-08-01 after 8h56m. merge.py documented it; the fetchers never got the memo.
    # _max_by_key ALREADY returns ISO STRINGS. Calling .isoformat() on them raised
    # `'str' object has no attribute 'isoformat'` — which is exactly the note on boc's last
    # recorded run, and why this source has never once reported a success.
    agg_map = _max_by_key(tbl)
    return {k: d for k, d in agg_map.items() if k and d}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "boc.parquet")

    names = _series_names()
    if not names:
        raise TransientError("boc: Valet series list returned nothing")

    stored = _stored_maxes(path)
    # Only refresh what we already publish. Valet's extra 3,044 series are NEW COVERAGE and
    # adding them here would silently widen the published id space without cataloguing them.
    todo = [n for n in names if n in stored] or names

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    keys, dates, vals = [], [], []
    cursors: dict = {}

    for i in range(0, len(todo), BATCH):
        batch = todo[i:i + BATCH]
        if dl.spent():
            tally.deferred_unit(f"{len(batch)} series deferred (budget)")
            continue
        start = min((stored.get(n) or FIRST_START) for n in batch)
        url = (f"{BASE}/observations/{','.join(batch)}/json?start_date={start}")
        try:
            d = _get(url)
        except TransientError:
            tally.transient_unit(f"batch@{i}")
            time.sleep(RATE)
            continue
        time.sleep(RATE)
        if not d:
            tally.transient_unit(f"batch@{i}")
            continue

        got = 0
        for row in (d.get("observations") or []):
            ds = row.get("d")
            if not ds:
                continue
            try:
                day = dt.date.fromisoformat(ds)
            except ValueError:
                continue
            for name in batch:
                cell = row.get(name)
                if not isinstance(cell, dict):
                    continue
                v = cell.get("v")
                if v in (None, ""):
                    continue
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                keys.append(name)
                dates.append(day)
                vals.append(fv)
                got += 1
                iso = day.isoformat()
                if cursors.get(name, "") < iso:
                    cursors[name] = iso
        if got:
            tally.added_unit(got, f"batch@{i}")

    total = blob.row_count(path) if blob.exists(path) else 0
    maxd = None
    if keys:
        tbl = pa.table({
            "series_key": pa.array(keys, pa.string()),
            "obs_date": pa.array(dates, pa.date32()),
            "value": pa.array(vals, pa.float64()),
        })
        before = total
        total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
        print(f"[boc] {len(keys):,} obs across {len(cursors):,} series; "
              f"store {before:,} -> {total:,}", flush=True)
    else:
        print("[boc] no new observations in the date-tail window", flush=True)

    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
