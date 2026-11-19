"""S5 bulk fetcher — USDA NASS Quick Stats (nass.usda.gov, no key for the bulk dumps).

53,529,239 observations across 69,704 served tables. The source was REGISTERED with a strategy
and a script and had NO fetcher, so the orchestrator could never run it: usda has no state at
all, meaning the updater has never once attempted the whole Quick Stats database.

THE VINTAGE IS MEASURED (the R164 rule). NASS publishes the complete database as five dated
sector dumps on its own datasets page:

    https://www.nass.usda.gov/datasets          (NO trailing slash - see THE 2026-10 MOVE below)
    -> qs.animals_products_20261118.txt.gz, qs.crops_20261118.txt.gz,
       qs.demographics_20261118.txt.gz, qs.economics_20261118.txt.gz,
       qs.environmental_20261118.txt.gz
       each linked as /sites/default/files/datasets/<fname>

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
The sentinels are removed for exactly the sectors whose dump was downloaded THIS run - a dump
already on disk from an earlier run is not new, and clearing its cube would re-ingest it.

THE GATE TOKEN IS STAMPED ON A CLEAN RUN. finalize() stamps new_vintage="date-tail" (it is shared
with the date-tail fetchers) and the strategy fills in the probed token only when new_vintage is
None, so before 2026-11 the stored upstream_vintage was "date-tail", never equal to the probe,
and every monthly cadence re-ingested ~57.8M rows whether NASS had published or not. update()
now stamps the listing hash on ok/no_change (the faostat pattern) and None otherwise.

THE 2026-10 MOVE. After the 2026-10-02 run NASS moved its site. Measured 2026-11-18 (HEAD):
    https://www.nass.usda.gov/datasets/   -> 301 -> https://www-origin.nass.usda.gov/datasets -> 404
    https://www.nass.usda.gov/datasets    -> 200, lists the five sector dumps
    https://www.nass.usda.gov/datasets/qs.crops_20261118.txt.gz                     -> 404
    https://www.nass.usda.gov/sites/default/files/datasets/qs.crops_20261118.txt.gz -> 200
The old code listed from the slash URL and built downloads as PAGE + fname, so every pass after
2026-10-02 ended transient_fail with a bare "page unreachable". Now the listing comes from the
no-slash URL, each download URL is the page's own href joined to the PAGE constant (urljoin -
never to the redirected response URL, which pointed at www-origin), and a 404/410 or a page
that links no dump stays a TransientError but SAYS the publisher moved it and names the URL.
It stays transient on purpose: a DefinitiveError is booked `partial`, which would turn usda's
red SLA amber and log ~1 s runs into the admission-reserve history (review, 2026-11-18).
"""
from __future__ import annotations

import glob
import hashlib
import os
import re
import shutil
import sys
from urllib.parse import urljoin

import pyarrow as pa
import requests

from ... import blob, config
from ...errors import TransientError
from ..base import Result
from ._common import (CURSOR_CAP, Tally, cursors_from_table, finalize, load_rotation,
                      merge_cursor_map, rotate_after, save_rotation)

SOURCE = "usda"
# 3-COLUMN IDENTITY ("the four", Ahmed's go 2026-08-31; R527's correction measured
# 65,122 (series_key, obs_date) collisions that are NASS FORECAST VINTAGES — the same
# target period published at successive forecast dates, distinguished by the stored
# REFERENCE_PERIOD_DESC column). The ingest rewrites whole cubes (ParquetWriter parts,
# no merge), so nothing on disk changes and NO public id changes — this declaration is
# what the dedup auditor reads (R281) and what any future merge/tail must inherit
# (R280: an under-keyed merge would collapse the vintages, last-writer-wins).
#
# RESIDUE, MEASURED IN FULL 2026-08-31 (57,815,063 store rows; the 3-column key does NOT
# make this store unique and saying otherwise would re-arm R280 with better numbers):
#   30,849 duplicate groups / 77,905 rows (0.13% of the store), of which
#     10,174 carry NO non-null value at all (invisible to COUNT(DISTINCT value) — the
#            first pass's arithmetic did not close until these were counted separately),
#     19,846 are value-IDENTICAL (a collapse would lose nothing), and
#        829 are value-DIFFERING (a collapse picks one of two real numbers).
#   Of those 829: 476 are separated by STATE_ALPHA and 353 by NOTHING tested.
#   LOCATION_DESC, WEEK_ENDING, AGG_LEVEL_DESC and COUNTY_CODE each separate ZERO —
#   so WEEK_ENDING is NOT the missing 4th column, and nobody should propose it again.
#
# WHY A 4TH COLUMN CANNOT FIX IT. The 476 are an AMBIGUOUS PUBLIC KEY, not a missing
# dedup column: for zip-code aggregations the series_key ends "...|ZIP CODE|99999" and
# omits the state, so genuinely different geographies share one key (measured example:
# SHEEP & GOATS zip-99999 carries TX=7.0 and MT=4.0 under a byte-identical series_key).
# Adding STATE_ALPHA to DEDUP would keep both rows while leaving them addressed by the
# same public id — the ambiguity moves, it does not close. Fixing it means RE-KEYING
# served ids, which is Ahmed's decision, not this declaration's (RESERVED).
DEDUP = ("series_key", "obs_date", "REFERENCE_PERIOD_DESC")
# NO trailing slash: "/datasets/" 301s to www-origin.nass.usda.gov/datasets, a 404 (measured
# 2026-11-18). Downloads are urljoin(PAGE, href) - always this constant, never the response URL.
PAGE = "https://www.nass.usda.gov/datasets"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
SECTORS = ("animals_products", "crops", "demographics", "economics", "environmental")
RE_DUMP = re.compile(r"(qs\.([a-z_]+)_(\d{8})\.txt\.gz)")
RE_HREF = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)
GONE = (404, 410)


def _listing(sess=None) -> dict:
    """{'crops': ('qs.crops_20261118.txt.gz', '20261118', '<absolute download url>'), ...}
    from NASS's own datasets page. Every failure is a TransientError (see the module note);
    a gone page or a page linking no dump says so and names the URL."""
    sess = sess or requests.Session()
    try:
        r = sess.get(PAGE, headers=UA, timeout=180)
        r.raise_for_status()
    except Exception as e:                                     # noqa: BLE001
        code = getattr(getattr(e, "response", None), "status_code", None)
        if code in GONE:
            raise TransientError(
                f"{SOURCE}: NASS datasets page {PAGE} answers HTTP {code} - the publisher has "
                f"MOVED OR REMOVED it, a retry will not fix this; find the new listing "
                f"(docs/runbook/usda.md): {e!r}") from e
        raise TransientError(f"{SOURCE}: NASS datasets page {PAGE} unreachable: {e!r}") from e
    out = {}
    for href in RE_HREF.findall(r.text):
        # The filename is the LAST path segment matched whole, so the undated census files
        # (qs.census2017.txt.gz) and anything that merely contains the pattern are skipped.
        m = RE_DUMP.fullmatch(href.split("?", 1)[0].split("#", 1)[0].rsplit("/", 1)[-1])
        if not m or m.group(2) not in SECTORS:
            continue
        fname, sector, date = m.groups()
        if sector in out and out[sector][1] >= date:
            continue                                 # a sector listed twice: keep the NEWEST
        out[sector] = (fname, date, urljoin(PAGE, href))
    if not out:
        raise TransientError(
            f"{SOURCE}: NASS datasets page {PAGE} answered but links no "
            f"qs.<sector>_YYYYMMDD.txt.gz dump - the page layout changed, a retry will not fix "
            f"this; existing data kept.")
    return out


def _vintage_of(got: dict) -> str | None:
    """The gate token: a hash over the five (sector, datestamp) pairs; None unless all five."""
    if any(s not in got for s in SECTORS):
        return None
    pairs = sorted((s, got[s][1]) for s in SECTORS)
    return f"{SOURCE}:" + hashlib.sha256(
        "|".join(f"{s}={d}" for s, d in pairs).encode()).hexdigest()[:16]


def current_vintage(unit):
    """Hash over the five (sector, datestamp) pairs. None if the page yields nothing —
    undeterminable, so the strategy fetches under cadence rather than freezing."""
    try:
        got = _listing()
    except TransientError:
        return None
    return _vintage_of(got)


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
              f"are covered by later passes, a window at a time, not skipped", flush=True)
    else:
        window = keys
    if window:
        save_rotation(out, window[-1])
    return {k: full[k] for k in window}


def _root() -> str:
    """The repo root (data/raw/usda lives under it). A function so tests can point it at a temp dir."""
    return os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))))


def update(unit, since) -> Result:
    root = _root()
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
        # TRANSIENT, not Definitive, for the reason in the module note (SLA colour, reserve).
        raise TransientError(
            f"{SOURCE}: NASS page {PAGE} no longer lists {', '.join(missing)} — it offers "
            f"{', '.join(sorted(got)) or 'nothing'}; a retry will not fix this. Existing data kept.")

    tally = Tally()
    present = []        # on disk at this listing's datestamp: the ingest may run over it
    downloaded = []     # fetched THIS run: only these cubes are new
    for sector in SECTORS:
        fname, date, url = got[sector]
        dest = os.path.join(raw, fname)
        if os.path.exists(dest) and os.path.getsize(dest) > 1_000_000:
            # NOT new: its _complete sentinel (if any) was cleared when it was downloaded,
            # so a cube left incomplete by an earlier run still resumes in the ingest.
            print(f"[{SOURCE}] {sector}: {fname} already downloaded", flush=True)
            present.append(sector)
            continue
        print(f"[{SOURCE}] {sector}: downloading {url}", flush=True)
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
                # THE SENTINEL MUST GO OR THE NEW VINTAGE IS PARSED AND DISCARDED - and it goes
                # BEFORE the new dump lands, so a kill between the two leaves an old dump with
                # no sentinel (harmless re-ingest), never a new dump behind a stale sentinel
                # (which the next run would read as "already downloaded" and never ingest).
                marker = os.path.join(out, sector, "_complete")
                if os.path.exists(marker):
                    os.remove(marker)
                    print(f"[{SOURCE}] {sector}: cleared _complete so the new vintage is "
                          f"ingested", flush=True)
                os.replace(tmp, dest)
        except TransientError:
            raise
        except Exception as e:                                 # noqa: BLE001
            tally.transient_unit(sector)
            print(f"[{SOURCE}] {sector}: download of {url} failed: {e!r}", flush=True)
            continue
        downloaded.append(sector)
        present.append(sector)
        # Remove stale dumps for this sector so the ingest cannot pick an older datestamp.
        for old in glob.glob(os.path.join(raw, f"qs.{sector}_*.txt.gz")):
            if os.path.basename(old) != fname:
                try:
                    os.remove(old)
                except OSError:
                    pass

    if not present:
        raise TransientError(f"{SOURCE}: no sector dump downloaded this run")
    print(f"[{SOURCE}] {len(downloaded)} sector dump(s) new this run, "
          f"{len(present) - len(downloaded)} already on disk", flush=True)

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
    res = finalize(tally, total, since or None, source=SOURCE,
                   series_cursors=cursors or None)
    # finalize() stamps new_vintage="date-tail", which never equals current_vintage(), so the
    # gate could never skip and every cadence re-ingested the whole database. Stamp the hash of
    # the listing THIS run used on a clean ok/no_change (the faostat pattern); a partial still
    # owes a sector, so None there and the orchestrator keeps the old stored vintage.
    res.new_vintage = _vintage_of(got) if res.status in ("ok", "no_change") else None
    return res
