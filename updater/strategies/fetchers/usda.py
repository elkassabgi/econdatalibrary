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

import requests

from ... import blob, config
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import Tally, finalize

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
    print(f"[{SOURCE}] NOTE: the served CSVs are TABLE-grain and are NOT regenerated here — "
          f"re-run tools/derive_usda_tables.py and tools/catalog_usda_tables.py after a new "
          f"vintage lands, or the store moves ahead of what users can download", flush=True)
    return finalize(tally, total, since or None, source=SOURCE)
