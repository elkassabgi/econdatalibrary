"""S2 fetcher - legacy `worldbank` ids, kept fresh from the store that already updates.

WHY THIS DOES NOT CALL THE WORLD BANK API. 692 series are published under `worldbank:<code>`
(e.g. worldbank:NY.GDP.MKTP.CD:AFE) and resolve today, but nothing refreshes them: no registry
entry, no fetcher, and 692 one-series-per-file parquets under the legacy data/clean/ tier.

Measured 2026-08-01: 684 of those 692 (98.8%) ALREADY EXIST in
clean_full/worldbank_wdi/worldbank_wdi.parquet as `WDI:<same code>` - 289,303 series, 8.9M
rows, fetched daily by the live worldbank_wdi fetcher from api.worldbank.org. The two sources
are the same World Bank data under two key prefixes.

So the honest fix is a PROJECTION, not a second crawler. Pulling the same indicators from the
same API twice would double the upstream load, double the failure surface, and let the two
copies drift apart - and drift between two copies of one dataset is the defect this library
keeps finding elsewhere. This fetcher reads the wdi store and re-emits exactly the 692 keys
the catalogue publishes, stripping the `WDI:` prefix.

The alternative - retiring the legacy ids - was rejected for the reason jobs/ingest_imf_direct
gives for the imf_<flow> sources: breaking thousands of live series ids to buy freshness is a
bad trade when the ids cost nothing to keep.

THE EIGHT AGGREGATES (XD, XM, XN, XT - income groups) are now covered. The wdi fetcher
resolves those codes to their 3-char form via the /v2/country reference list, so they were
present under a different code rather than absent, and the direct key match missed them: they
were reported `missing` every run and NEVER refreshed, frozen at whatever we last held while
their wdi twins moved on. WDI_GEO_TO_LEGACY closes it, and this docstring's own promise -
"whoever adds the mapping will see the count go to zero" - is the test that proves it
(tests/test_worldbank_geo_alias.py). A non-empty `missing` list now means a NEW gap.

VINTAGE: none of its own. This source is downstream of worldbank_wdi, so it re-projects
whenever its cadence says to and merge dedup makes a re-run harmless. A fabricated token would
either freeze it or make it re-pull for ever.

HONEST-STATUS: if the upstream store is missing or unreadable -> TransientError, existing data
kept. If it is readable but yields none of our 692 keys -> structural, because that means the
wdi key shape moved under us and a silent 0-row publish would be the worst outcome.
"""
from __future__ import annotations

import datetime as dt
import os

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Tally, cursors_from_table, finalize, merge_cursor_map

SOURCE = "worldbank"
UPSTREAM = "worldbank_wdi"
DEDUP = ("series_key", "obs_date")
PREFIX = "WDI:"

# THE MAPPING THIS FILE'S OWN DOCSTRING ASKED FOR. Eight published `worldbank` ids end in a
# 2-char income-group code; `worldbank_wdi` carries the same economies under the publisher's
# 3-char form, so the direct key match misses and those eight were reported `missing` every
# run and never refreshed — frozen at whatever we last held while their wdi twins move on.
#
# MEASURED before it was written, not asserted (R504, R509):
#   (a) the publisher's own /v2/country reference list (295 entries, fetched 2026-08-30):
#       iso2Code XD -> id HIC "High income", XM -> LIC, XN -> LMC, XT -> UMC;
#   (b) in the store update() ACTUALLY OPENS -- clean_full/worldbank_wdi/worldbank_wdi.parquet,
#       8,973,662 rows, column `series_key` -- HIC/LIC/LMC/UMC carry 35,008 / 31,774 / 37,091 /
#       34,742 rows and XD/XM/XN/XT carry ZERO. (My first version of this comment cited the
#       GROUPED tier's counts, a different store with a `country` column that this function
#       never reads: the right answer from the wrong instrument, caught in review.);
#   (b2) and the defect is PRESENT, not prospective: clean_full/worldbank/worldbank.parquet
#       holds 684 series and ZERO with a 2-char geo, so the 8 are absent from the store this
#       fetcher merges into and survive only in the frozen data/clean/worldbank/ tier that
#       nothing writes. `_migrate_legacy` returns early when the target exists, so the
#       migration whose docstring exists to stop these "silently vanishing" has already
#       failed to carry them. The alias INSERTS them rather than overwriting anything;
#   (c) enumerating all 263 distinct geos across the 692 published legacy ids, exactly 8 use
#       a 2-char code and they use exactly these 4 — so this map is the whole class, not a
#       sample of it. The publisher derives 18 such codes; the other 14 name no legacy id.
#
# Direction matters: keys here are the WDI (3-char) spelling and values the LEGACY (2-char)
# one, because the loop below is translating an upstream key INTO a published id.
WDI_GEO_TO_LEGACY = {"HIC": "XD", "LIC": "XM", "LMC": "XN", "UMC": "XT"}


def _legacy_alias(legacy_key: str) -> str | None:
    """`NY.GDP.MKTP.CD:HIC` -> `NY.GDP.MKTP.CD:XD`, or None when nothing maps.

    Applied ONLY after a direct match fails, so a published id always wins over an alias and
    this can never redirect a key that already resolves."""
    head, sep, geo = legacy_key.rpartition(":")
    if not sep:
        return None
    alias = WDI_GEO_TO_LEGACY.get(geo)
    return f"{head}:{alias}" if alias else None


def current_vintage(unit):
    """None by design - see the module docstring. Cadence-gated, never a fabricated token."""
    return None


def _published_keys() -> set[str]:
    """The 692 ids the catalogue actually publishes, read from the catalogue itself.

    Deliberately NOT a hardcoded list: the set of legacy ids is data, and a Python copy of it
    would drift the moment one is added or withdrawn. If the catalogue is unreadable we fail
    transient rather than guessing a smaller set and quietly shrinking the source.
    """
    import sqlite3
    path = os.path.join(config.ROOT, "data", "catalog.db")
    if not os.path.exists(path):
        raise TransientError(f"{SOURCE}: catalog.db not present at {path}; cannot determine "
                             f"which legacy ids to project")
    con = sqlite3.connect(path)
    try:
        rows = con.execute("SELECT series_id FROM series WHERE source_id=?",
                           (SOURCE,)).fetchall()
    finally:
        con.close()
    # catalogue ids are `worldbank:<key>`; the store key is the part after the source prefix
    return {r[0].split(":", 1)[1] for r in rows if ":" in r[0]}


def _migrate_legacy(path: str) -> None:
    """One-time consolidation of the 692 legacy one-series-per-file parquets.

    The published rows live in data/clean/worldbank/ - the old tier, one file per series -
    while config.source_dir() points at clean_full/. Without this the projection would create
    a NEW store containing only the 684 series the upstream covers, and the other 8 (the
    aggregate codes) would silently vanish from a source that serves them today. A migration
    that drops 8 published series is not a migration, it is a partial deletion.

    Runs only when the destination does not yet exist, reads through blob so it behaves the
    same under the r2 backend, and is best-effort: if the legacy tier is absent (a CI runner
    that never had it) there is simply nothing to carry forward and the projection proceeds.
    """
    if blob.exists(path):
        return
    legacy_dir = os.path.join(config.DATA_ROOT, "..", "clean", SOURCE)
    legacy_dir = os.path.normpath(legacy_dir)
    if not os.path.isdir(legacy_dir):
        print(f"[{SOURCE}] no legacy tier at {legacy_dir}; starting from the projection alone",
              flush=True)
        return
    import glob
    import pyarrow.parquet as pq
    files = sorted(glob.glob(os.path.join(legacy_dir, "*.parquet")))
    if not files:
        return
    parts = []
    for f in files:
        try:
            t = pq.read_table(f, columns=["series_key", "obs_date", "value"])
            if t.num_rows:
                parts.append(t)
        except Exception as e:                               # noqa: BLE001
            print(f"[{SOURCE}] legacy file unreadable, skipped: {os.path.basename(f)} ({e!r})",
                  flush=True)
    if not parts:
        return
    seeded = pa.concat_tables(parts)
    n, _ = merge.merge_and_write(path, seeded, mode="merge", dedup_keys=DEDUP)
    print(f"[{SOURCE}] MIGRATED {len(files):,} legacy one-series files -> {n:,} rows in one "
          f"store; the projection now extends this rather than replacing it", flush=True)


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{SOURCE}.parquet")
    up_path = os.path.join(config.source_dir(UPSTREAM), f"{UPSTREAM}.parquet")

    tally = Tally()
    wanted = _published_keys()
    if not wanted:
        raise TransientError(f"{SOURCE}: catalogue lists no {SOURCE} series to project")

    if not blob.exists(up_path):
        raise TransientError(
            f"{SOURCE}: upstream {UPSTREAM} store not present at {up_path}. This source is a "
            f"projection of it, so there is nothing to do until that source has run.")

    _migrate_legacy(path)

    try:
        up = blob.read_table(up_path, columns=["series_key", "obs_date", "value"])
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"{SOURCE}: cannot read {UPSTREAM}: {e!r}") from e

    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []
    seen: set[str] = set()
    ks = up.column("series_key").to_pylist()
    ds = up.column("obs_date").to_pylist()
    vs = up.column("value").to_pylist()
    for k, d, v in zip(ks, ds, vs):
        if not k or not k.startswith(PREFIX):
            continue
        legacy = k[len(PREFIX):]
        if legacy not in wanted:
            # Income-group aggregates: wdi spells the geo 3-char, the published id 2-char.
            # Tried only on a miss, so a real published id can never be rewritten.
            alias = _legacy_alias(legacy)
            if alias is None or alias not in wanted:
                continue
            legacy = alias
        if d is None or v is None:
            continue
        keys.append(legacy)
        dates.append(d)
        vals.append(float(v))
        seen.add(legacy)

    if not keys:
        # Readable upstream that yielded none of our keys means the wdi key shape moved.
        # Publishing 0 rows over 692 good series would be the worst possible outcome, so this
        # is structural: existing data kept, loud, and the run is not called a success.
        tally.structural_unit(
            f"{UPSTREAM} readable ({up.num_rows:,} rows) but none of the {len(wanted):,} "
            f"published {SOURCE} keys matched - the upstream key shape has changed")
        return finalize(tally, blob.row_count(path) if blob.exists(path) else 0,
                        since or None, source=SOURCE)

    missing = sorted(wanted - seen)
    if missing:
        # Named, not swallowed. This USED to be the 8 income-group aggregates; WDI_GEO_TO_LEGACY
        # now covers those, so a non-empty list here is a NEW gap and worth reading rather than
        # the standing background noise it had become.
        print(f"[{SOURCE}] {len(missing)} published id(s) have no {UPSTREAM} counterpart "
              f"and were NOT refreshed this run (existing rows untouched): "
              f"{missing[:8]}{' ...' if len(missing) > 8 else ''}", flush=True)

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    before = blob.row_count(path) if blob.exists(path) else 0
    total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    tally.added_unit(max(0, total - before), f"{len(seen)} series projected")

    cursors: dict[str, str] = {}
    merge_cursor_map(cursors, cursors_from_table(tbl, cap=CURSOR_CAP), cap=CURSOR_CAP)

    print(f"[{SOURCE}] projected {len(seen):,} of {len(wanted):,} published series "
          f"({len(keys):,} obs) from {UPSTREAM}; store {before:,} -> {total:,}", flush=True)
    return finalize(tally, total, maxd or (since or None), source=SOURCE,
                    series_cursors=cursors or None)
