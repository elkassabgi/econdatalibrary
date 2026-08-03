"""S5 bulk fetcher — USDA NASS Quick Stats (nass.usda.gov, no key for the bulk dumps).

53,529,239 observations across 69,704 served tables. The source was REGISTERED with a strategy
and a script and had NO fetcher, so the orchestrator could never run it: usda has no state at
all, meaning the updater has never once attempted the whole Quick Stats database.

THE VINTAGE IS MEASURED (the R164 rule). NASS publishes the complete database as five dated
sector dumps on its own datasets page:

    https://www.nass.usda.gov/datasets/
    -> qs.animals_products_20260801.txt.gz, qs.crops_20260801.txt.gz,
       qs.demographics_20260801.txt.gz, qs.economics_20260801.txt.gz,
       qs.environmental_20260801.txt.gz          IDENTICAL across two fetches

The five sectors are exactly the five cube directories in the store, and their record counts sum
to the API's own get_counts total — the ingest's docstring records that check. So the token is a
hash over the (sector, datestamp) pairs, and it moves when NASS publishes a new monthly dump.

THE CENSUS-YEAR FILES ARE DELIBERATELY EXCLUDED, and this fetcher must not quietly re-include
them. NASS also offers qs.censusYYYY[zipcode] files, which are an ALTERNATE SLICING of the same
census observations already inside the sector dumps: ingesting both creates exact
(series_key, obs_date) duplicates and inflates every count. The ingest excludes them by default
and this passes no flag to change that.

CLEARING THE `_complete` SENTINEL IS THE WHOLE POINT OF THE REFRESH. The ingest marks a finished
cube with a `_complete` file and SKIPS it on re-run — correct for resuming an interrupted
backfill, and fatal for an update, because a new vintage would be downloaded and then ignored.
The sentinels are removed for exactly the sectors whose datestamp moved.
"""
from __future__ import annotations

import glob
import hashlib
import os
import re
import shutil
import sys

import pyarrow as pa
import requests

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import (CURSOR_CAP, Tally, cursors_from_table, finalize, load_rotation,
                      merge_cursor_map, rotate_after, save_rotation)

SOURCE = "usda"
PAGE = "https://www.nass.usda.gov/datasets/"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
SECTORS = ("animals_products", "crops", "demographics", "economics", "environmental")
RE_DUMP = re.compile(r"(qs\.([a-z_]+)_(\d{8})\.txt\.gz)")


def _listing(sess=None) -> dict:
    """{'crops': ('qs.crops_20260801.txt.gz', '20260801'), ...} from NASS's own page."""
    sess = sess or requests.Session()
    try:
        r = sess.get(PAGE, headers=UA, timeout=180)
        r.raise_for_status()
    except Exception as e:                                     # noqa: BLE001
        raise TransientError(f"{SOURCE}: NASS datasets page unreachable: {e!r}") from e
    out = {}
    for fname, sector, date in RE_DUMP.findall(r.text):
        if sector in SECTORS:
            out[sector] = (fname, date)
    return out


def current_vintage(unit):
    """Hash over the five (sector, datestamp) pairs. None if the page yields nothing —
    undeterminable, so the strategy fetches under cadence rather than freezing."""
    try:
        got = _listing()
    except TransientError:
        return None
    if len(got) < len(SECTORS):
        return None
    pairs = sorted((s, d) for s, (_f, d) in got.items())
    return f"{SOURCE}:" + hashlib.sha256(
        "|".join(f"{s}={d}" for s, d in pairs).encode()).hexdigest()[:16]


def _table_cursors(out: str) -> dict:
    """{table-grain key: max obs_date ISO} for the whole store — the §5.7 changed set.

    WHY THIS EXISTS. usda merged 57,786,638 observations per run and reported NO cursors, so
    orchestrate could not re-derive a single CSV: the run was demoted to `partial` every time
    and every published CSV drifted from the parquet behind it. The module note this replaces
    told a human to "re-run tools/derive_usda_tables.py after a new vintage lands" — a manual
    step nobody performs on a monthly cron, which is exactly how the store gets ahead of what
    users can download.

    THE GRAIN IS THE CATALOGUE'S, NOT THE STORE'S. usda averages 3.7 obs/series across
    15,534,339 series, so it is served as ~72,046 TABLES (_resolve_usda), and a catalog id is
        usda:<SOURCE_DESC>|<AGG_LEVEL_DESC>|<SHORT_DESC>
    while a store key carries 14 pipe-separated fields AND a redundant leading "usda:" — which
    orchestrate._catalog_ids_for would turn into "usda:usda:…", mapping nothing (the trap the
    CURSOR_CAP docstring records for ilostat). So the key is rebuilt from fields 1, 13 and 10
    and emitted WITHOUT the source prefix, so "<source>:" + key == the catalog id.

    Verified over the FULL store, not a sample: 72,046 distinct derived ids, of which 69,704
    exist in the catalogue and — the direction that matters — ZERO catalogued usda rows are
    unreachable from a store key. The 2,342 extra are store tables that were never catalogued.

    Per-file so memory stays bounded: the derived-key column for all ~200M rows at once would
    be tens of GB, while one part at a time is a few million.
    """
    import pyarrow.compute as pc

    # R36: this READS the store to learn which keys we hold, and it listed with a raw
    # recursive glob and read with a raw pq.read_table — both addressing the local disk. Under
    # AQUEDUCT_BACKEND=r2 the loop had nothing to iterate and this returned an EMPTY mapping,
    # which downstream reads as "the store holds no keys" rather than "I could not look".
    # usda's store is nested (out/<sector>/<file>.parquet), so the listing must be recursive —
    # the default basenames-only form returns [] here, the same answer as an empty store.
    full: dict = {}
    for rel in blob.list_parquets(out, recursive=True):
        p = os.path.join(out, rel)
        try:
            t = blob.read_table(p, columns=["series_key", "obs_date"])
            if t.num_rows == 0:
                continue
            k = pc.replace_substring_regex(t.column("series_key"), "^usda:", "")
            parts = pc.split_pattern(k, "|")
            key = pc.binary_join_element_wise(
                pc.list_element(parts, 0),                   # SOURCE_DESC
                pc.list_element(parts, 12),                  # AGG_LEVEL_DESC
                pc.list_element(parts, 9),                   # SHORT_DESC
                "|")
            one = pa.table({"series_key": key, "obs_date": t.column("obs_date")})
            # cap=0 here: the WHOLE table set is gathered first, then a rotating window is
            # taken below. Capping per file would silently fix the window to whichever
            # tables sort first, which is the bug this function goes out of its way to avoid.
            merge_cursor_map(full, cursors_from_table(one, cap=0, key_col="series_key"),
                             cap=10 ** 9)
        except Exception as e:                               # noqa: BLE001
            # A cursor problem must never sink a good publish (§5 rule 7) — but it must not
            # be silent either, or the source quietly returns to reporting nothing.
            print(f"[{SOURCE}] cursor build failed on {os.path.basename(p)}: {e!r}", flush=True)

    # ROTATE THE WINDOW — a bound over a fixed order is a truncation, not a budget (R190).
    #
    # usda has ~72,046 tables against a 50,000 CURSOR_CAP, and cursors_from_table caps by
    # SORTING. Reporting the capped set directly would therefore hand back the same first
    # 50,000 keys alphabetically on every single run, and the last ~21,386 tables' CSVs would
    # never be re-derived — permanently stale, while the run reported a plausible number.
    # That is the self-certifying outage load_rotation exists to prevent.
    #
    # So the window starts just after wherever the last run stopped and wraps around; two
    # runs cover the whole set. The bookmark is saved even on a complete pass, so the next
    # run wraps through the same code path rather than a branch that could stop rotating.
    keys = sorted(full)
    if len(keys) > CURSOR_CAP:
        window = rotate_after(keys, load_rotation(out))[:CURSOR_CAP]
        print(f"[{SOURCE}] {len(keys):,} tables > {CURSOR_CAP:,} cursor cap — reporting a "
              f"ROTATING window of {len(window):,}; the remaining {len(keys) - len(window):,} "
              f"are covered by the next run, not skipped", flush=True)
    else:
        window = keys
    if window:
        save_rotation(out, window[-1])
    return {k: full[k] for k in window}


def update(unit, since) -> Result:
    root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))
    raw = os.path.join(root, "data", "raw", SOURCE)
    out = config.source_dir(SOURCE)
    os.makedirs(raw, exist_ok=True)
    os.makedirs(out, exist_ok=True)

    sess = requests.Session()
    got = _listing(sess)
    missing = [s for s in SECTORS if s not in got]
    if missing:
        # A sector the store holds is no longer published: a publisher change, not a quiet
        # month. Refusing beats re-ingesting four fifths of the database over the top of five.
        raise DefinitiveError(
            f"{SOURCE}: NASS no longer lists {', '.join(missing)} — the page offers "
            f"{', '.join(sorted(got)) or 'nothing'}. Existing data kept.")

    tally = Tally()
    fetched = []
    for sector in SECTORS:
        fname, date = got[sector]
        dest = os.path.join(raw, fname)
        if os.path.exists(dest) and os.path.getsize(dest) > 1_000_000:
            print(f"[{SOURCE}] {sector}: {fname} already downloaded", flush=True)
            fetched.append(sector)
            continue
        url = PAGE + fname
        print(f"[{SOURCE}] {sector}: downloading {fname}", flush=True)
        try:
            with sess.get(url, headers=UA, timeout=3600, stream=True) as r:
                r.raise_for_status()
                declared = int(r.headers.get("Content-Length") or 0)
                tmp = dest + ".part"
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=8 << 20):
                        fh.write(chunk)
                n = os.path.getsize(tmp)
                if declared and n != declared:
                    os.remove(tmp)
                    raise TransientError(
                        f"{SOURCE}/{sector}: truncated download — {n:,} of {declared:,} bytes")
                os.replace(tmp, dest)
        except TransientError:
            raise
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(sector)
            print(f"[{SOURCE}] {sector}: download failed: {e!r}", flush=True)
            continue
        fetched.append(sector)
        # Remove stale dumps for this sector so the ingest cannot pick an older datestamp.
        for old in glob.glob(os.path.join(raw, f"qs.{sector}_*.txt.gz")):
            if os.path.basename(old) != fname:
                try:
                    os.remove(old)
                except OSError:
                    pass

    if not fetched:
        raise TransientError(f"{SOURCE}: no sector dump downloaded this run")

    # THE SENTINEL MUST GO OR THE NEW VINTAGE IS PARSED AND DISCARDED.
    for sector in fetched:
        marker = os.path.join(out, sector, "_complete")
        if os.path.exists(marker):
            os.remove(marker)
            print(f"[{SOURCE}] {sector}: cleared _complete so the new vintage is ingested",
                  flush=True)

    sys.path.insert(0, root)
    from jobs import ingest_usda as J

    argv = sys.argv
    try:
        sys.argv = ["ingest_usda.py"]                          # no --with-census, see docstring
        J.main()
    except Exception as e:                                     # noqa: BLE001
        raise TransientError(f"{SOURCE}: ingest failed: {e!r}") from e
    finally:
        sys.argv = argv

    total = 0
    published = 0
    for p in sorted(glob.glob(os.path.join(out, "**", "*.parquet"), recursive=True)):
        if blob.publish_file(p):
            published += 1
    import pyarrow.parquet as pq
    for p in glob.glob(os.path.join(out, "**", "*.parquet"), recursive=True):
        try:
            total += pq.ParquetFile(p).metadata.num_rows
        except Exception:                                      # noqa: BLE001
            pass
    print(f"[{SOURCE}] published {published:,} object(s), {total:,} rows in the store",
          flush=True)
    tally.added_unit(total, "quickstats")

    cursors = _table_cursors(out)
    if len(cursors) >= CURSOR_CAP:
        print(f"[{SOURCE}] cursor set hit the {CURSOR_CAP:,} cap — usda has 69,704 catalogued "
              f"tables, so ~{69704 - CURSOR_CAP:,} are not individually reported this run and "
              f"their CSVs wait for a later one (the derive budget drains the rest across "
              f"runs; neither bound is silent)", flush=True)
    return finalize(tally, total, since or None, source=SOURCE,
                    series_cursors=cursors or None)
