"""S2 fetcher - U.S. Bureau of Economic Analysis, date-tail over the NIPA-family datasets.

240 series are published under `bea:<SeriesCode>:<freq>` and resolve today (verified live:
bea:A191RC:Q returns 317 observations through 2026-01-01), but nothing refreshes them. The
published rows sit in the OLD data/clean/ tier as 240 one-series-per-file parquets whose
schema is (obs_date, value, version) - the series identity is in the FILENAME
(bea__A191RC__Q.parquet), not in a column - while config.source_dir() points at clean_full/,
which is empty.

THE KEY SHAPE ALREADY MATCHES, which is what makes this safe. jobs/ingest_bea_full.py builds
`sk.append(f"{code}:{fr}")` - SeriesCode:frequency - and that is byte-identical to the
published ids. So this fetcher REUSES that ingest rather than reimplementing it, exactly as
the comtrade fetcher reuses jobs/ingest_comtrade: same rate limiter (BEA caps 100 req/min and
100 MB/min; the ingest holds itself to 85 and 70 across its workers), same parsing, same
completeness handling. Reimplementing would fork the key shape sooner or later, and a forked
key shape does not fail - it silently publishes a second copy of every series.

WHY A YEAR WINDOW, NOT A FULL RE-PULL. BEA exposes Year=<from>..<to> on GetData, and the
full enumeration is 12 datasets over 591 tables against a 100 req/min ceiling. Re-pulling
everything every night would spend hours to collect revisions that only ever touch the last
few years, so each run asks for a trailing window from the stored frontier and merges. The
lookback absorbs the annual back-revisions BEA publishes against prior years.

MIGRATION IS PART OF THE FETCH, not a separate chore. On the first run the 240 legacy files
are consolidated into the modern store first, with the series key recovered from each
filename; the pull then EXTENDS them. Without that step the first successful pull would
create a store containing only whatever the window returned, and 240 published series would
lose their history - a migration that drops history is a deletion wearing a different name.

HONEST-STATUS: one dataset is one sub-unit. A failure there is transient (existing data kept,
retried next tick). Zero usable rows across the WHOLE run -> TransientError rather than a
hollow success. BEA_API_KEY absent -> TransientError, never a silent no-op, because a source
that quietly does nothing looks identical to one that is up to date.
"""
from __future__ import annotations

import datetime as dt
import glob
import os
import re

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, cursors_from_table, finalize, merge_cursor_map

SOURCE = "bea"
DEDUP = ("series_key", "obs_date")
BUDGET_MIN = float(os.environ.get("AQUEDUCT_BEA_BUDGET_MIN", "35"))
# Years of overlap re-requested each run. BEA revises prior years on a normal schedule, so a
# window that starts exactly at the stored frontier would never see those corrections.
LOOKBACK_YEARS = 3
# bea__<SeriesCode>__<freq>.parquet  ->  ("A191RC", "Q")
_LEGACY = re.compile(r"^bea__(?P<code>.+)__(?P<freq>[AQM])\.parquet$")


def current_vintage(unit):
    """None by design: BEA publishes no library-wide vintage or last-modified feed, so the
    cadence gates the fetch and merge dedup makes a re-pull harmless. A fabricated token would
    either freeze the source or make it re-pull for ever."""
    return None


def _migrate_legacy(path: str) -> int:
    """Consolidate the 240 one-series-per-file legacy parquets. Returns rows seeded.

    The legacy files carry (obs_date, value, version) and hold the series identity in their
    NAME, so the key has to be reconstructed - reading them as-is would produce a table with
    no series_key at all. Runs only when the modern store is absent.
    """
    if blob.exists(path):
        return 0
    legacy_dir = os.path.normpath(os.path.join(config.DATA_ROOT, "..", "clean", SOURCE))
    if not os.path.isdir(legacy_dir):
        return 0
    import pyarrow.parquet as pq
    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []
    skipped = []
    for f in sorted(glob.glob(os.path.join(legacy_dir, "*.parquet"))):
        m = _LEGACY.match(os.path.basename(f))
        if not m:
            skipped.append(os.path.basename(f))
            continue
        key = f"{m.group('code')}:{m.group('freq')}"
        try:
            t = pq.read_table(f, columns=["obs_date", "value"])
        except Exception as e:                               # noqa: BLE001
            skipped.append(f"{os.path.basename(f)} ({type(e).__name__})")
            continue
        for d, v in zip(t.column("obs_date").to_pylist(), t.column("value").to_pylist()):
            if d is None or v is None:
                continue
            keys.append(key)
            dates.append(d)
            vals.append(float(v))
    if skipped:
        # Named, never a silent drop: each one is a published series that would lose its
        # history, and the filename is the only place its identity exists.
        print(f"[{SOURCE}] MIGRATION: {len(skipped)} legacy file(s) not understood and NOT "
              f"carried over: {skipped[:6]}{' ...' if len(skipped) > 6 else ''}", flush=True)
    if not keys:
        return 0
    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    n, _ = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    print(f"[{SOURCE}] MIGRATED {len(set(keys)):,} legacy series ({len(keys):,} obs) out of "
          f"the clean/ tier -> {n:,} rows; the pull now extends this", flush=True)
    return n


def _stored_frontier(path: str) -> dt.date | None:
    if not blob.exists(path):
        return None
    try:
        return merge._max_obs_date(blob.read_table(path, columns=["obs_date"]))
    except Exception:                                        # noqa: BLE001
        return None


def update(unit, since) -> Result:
    if not os.environ.get("BEA_API_KEY", "").strip():
        # Loud, never a quiet no-op: a source that silently does nothing is indistinguishable
        # from one that is up to date, which is how staleness hides.
        raise TransientError(
            f"{SOURCE}: BEA_API_KEY is not set, so nothing can be fetched. Existing data kept.")

    from jobs import ingest_bea_full as ig                   # rate limiter + parser + keys

    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{SOURCE}.parquet")

    _migrate_legacy(path)

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    frontier = _stored_frontier(path)
    start_year = (frontier.year - LOOKBACK_YEARS) if frontier else 1929
    end_year = dt.date.today().year + 1
    years = f"{start_year},{end_year}" if start_year != end_year else str(start_year)
    print(f"[{SOURCE}] stored frontier {frontier or 'none'}; requesting years "
          f"{start_year}-{end_year}", flush=True)

    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []

    try:
        meta = ig.load_manifest()
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{SOURCE}: BEA parameter manifest unavailable: {e!r}") from e

    for dataset, freqs in (("NIPA", ("A", "Q", "M")), ("NIUnderlyingDetail", ("A", "Q", "M"))):
        try:
            tables = ig._keys(meta["param_values"][dataset]["TableName"])
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(f"{dataset}: no table list in the manifest")
            continue
        for table in tables:
            if dl.spent():
                tally.transient_unit(f"{dataset}:{table} deferred (budget "
                                     f"{BUDGET_MIN:.0f} min)")
                continue
            got = 0
            for fr in freqs:
                try:
                    rows = ig.call(datasetname=dataset, TableName=table, Frequency=fr,
                                   Year=years)
                except Exception:                            # noqa: BLE001
                    tally.transient_unit(f"{dataset}:{table}:{fr}")
                    continue
                for row in rows or ():
                    code = row.get("SeriesCode")
                    od = ig.pdate(row.get("TimePeriod"))
                    val = ig.pval(row.get("DataValue"))
                    if not code or od is None or val is None:
                        continue
                    keys.append(f"{code}:{fr}")              # SAME shape as the published ids
                    dates.append(od)
                    vals.append(val)
                    got += 1
            if got:
                tally.added_unit(got, f"{dataset}:{table}")
            else:
                tally.empty_unit(f"{dataset}:{table}")

    if not keys:
        raise TransientError(
            f"{SOURCE}: no usable observations from any dataset this run; existing data kept")

    tbl = pa.table({"series_key": pa.array(keys, pa.string()),
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": pa.array(vals, pa.float64())})
    before = blob.row_count(path) if blob.exists(path) else 0
    total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    cursors: dict[str, str] = {}
    merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP), cap=CURSOR_CAP)

    print(f"[{SOURCE}] {len(keys):,} obs across {len(set(keys)):,} series; "
          f"store {before:,} -> {total:,}", flush=True)
    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
