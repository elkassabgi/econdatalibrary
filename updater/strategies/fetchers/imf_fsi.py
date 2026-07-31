"""S2 fetcher - IMF Financial Soundness Indicators (73,288 series) via DBnomics IMF/FSI.

WHY DBNOMICS AND NOT data.imf.org. jobs/ingest_imf_fsi.py has two branches and they emit
DIFFERENT KEY SHAPES:

    SDMX branch (line 98)      series_key = f"FSI:{indicator}:{country}"   -> two colons
    DBnomics branch (line 200) sk         = f"FSI:{series_code}"           -> one colon

The published store is 100% one-colon keys (measured: 73,288 of 73,288, e.g.
`FSI:Q.RO.FSRR_PT`), matching the catalogue exactly, so the DBnomics branch produced every
row and the SDMX branch never landed one - consistent with the registry note that the IMF
bulk SDMX endpoint is "historically flaky" and with the two _imf_fsi_err*.txt files sitting
in the store directory.

That makes the choice of upstream a CORRECTNESS question, not a preference. A run that fell
back to the SDMX shape would not fail; it would quietly mint 73,288 second copies of every
series under ids nobody has ever published, and the merge would happily accept them - the
comtrade under-keying defect in a new costume. So this fetcher speaks ONE dialect, and
_check_key_shape below REFUSES anything else rather than trusting that it cannot happen.

DATES. DBnomics returns `period_start_day` as an ISO string per observation, which is where
the store's period-start convention comes from: annual -> YYYY-01-01, quarterly -> the
quarter's first day (01/04/07/10-01), monthly -> YYYY-MM-01. Measured against the store, all
three hold with no exceptions, so nothing here needs to parse a frequency code.

VINTAGE: none. This is a date-tail source (`extend_by_date`): DBnomics exposes no per-dataset
publication token that moves reliably, so `current_vintage` returns None, the annual/quarterly
cadence gates the fetch, and merge dedup makes a re-pull harmless. Inventing a token would
either never match (re-pull for ever) or always match (freeze the source).

HONEST-STATUS: a page that fails is one transient sub-unit. Zero usable rows across the WHOLE
run -> TransientError, so the existing 2.4M rows are kept and the run reports partial rather
than a hollow success.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import requests

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, TransientStreak, finalize, merge_cursor_map

SOURCE = "imf_fsi"
DEDUP = ("series_key", "obs_date")
BASE = "https://api.db.nomics.world/v22"
PAGE = 1000                      # the size jobs/ingest_imf_fsi.py used to build the store
RATE = 1.0
BUDGET_MIN = float(os.environ.get("AQUEDUCT_IMF_FSI_BUDGET_MIN", "30"))
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com",
      "Accept": "application/json"}

# Exactly one colon, and neither side empty. `FSI:Q.RO.FSRR_PT` passes;
# `FSI:FSANL:USA` (the SDMX shape) does not.
_KEY_RE = re.compile(r"^FSI:[^:]+$")


def _check_key_shape(keys) -> None:
    """Refuse to publish a key shape the catalogue does not use.

    A guard, not a comment: the whole risk here is that a fallback path silently forks the id
    space, and every id it forked would look like a legitimate new series to the merge.
    """
    bad = [k for k in keys if not _KEY_RE.match(k)]
    if bad:
        raise TransientError(
            f"{SOURCE}: {len(bad)} key(s) are not the published `FSI:<series_code>` shape "
            f"(e.g. {bad[:3]}). Refusing to merge - this is how an id space forks. Existing "
            f"data kept.")


def current_vintage(unit):
    """None by design - see the module docstring. Cadence-gated, never a fabricated token."""
    return None


def _page(offset: int) -> dict | None:
    """One DBnomics page, or None on failure. Never [] - a caller must not read a throttled
    or errored page as 'there are no more series'."""
    url = (f"{BASE}/series/IMF/FSI?observations=1&limit={PAGE}&offset={offset}")
    for attempt in range(4):
        try:
            r = requests.get(url, headers=UA, timeout=180)
        except Exception as e:                                # noqa: BLE001
            print(f"[{SOURCE}] offset={offset} attempt {attempt+1}: "
                  f"{type(e).__name__}: {str(e)[:80]}", flush=True)
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 200:
            return r.json() or {}
        if r.status_code == 429:
            wait = 30 * (attempt + 1)
            print(f"[{SOURCE}] 429 at offset={offset}, sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        print(f"[{SOURCE}] HTTP {r.status_code} at offset={offset}", flush=True)
        time.sleep(5 * (attempt + 1))
    return None


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "imf_fsi.parquet")

    tally = Tally()
    dl = Deadline(minutes=BUDGET_MIN)
    streak = TransientStreak()
    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []
    cursors: dict[str, str] = {}

    offset, total, pages = 0, None, 0
    while True:
        if dl.spent():
            tally.transient_unit(f"budget spent at offset {offset:,}"
                                 f"{' of ' + format(total, ',') if total else ''}")
            break
        j = _page(offset)
        if j is None:
            tally.transient_unit(f"page at offset {offset:,}")
            if streak.fail():
                print(f"[{SOURCE}] {streak.streak} consecutive failed pages - upstream is "
                      f"refusing, stopping early and keeping what merged", flush=True)
                break
            offset += PAGE
            time.sleep(RATE)
            continue
        streak.ok()

        obj = j.get("series") or {}
        docs = obj.get("docs") or []
        if total is None:
            total = obj.get("num_found") or 0
            print(f"[{SOURCE}] upstream reports {total:,} series", flush=True)
        if not docs:
            break

        n_before = len(vals)
        for s in docs:
            sc = s.get("series_code") or ""
            if not sc:
                continue
            sk = f"FSI:{sc}"
            days = s.get("period_start_day") or []
            values = s.get("value") or []
            newest = ""
            for day, vv in zip(days, values):
                if vv is None:
                    continue
                try:
                    fv = float(vv)
                except (TypeError, ValueError):
                    continue
                if fv != fv:                                  # NaN
                    continue
                try:
                    d = dt.date.fromisoformat(day)
                except (TypeError, ValueError):
                    continue
                keys.append(sk)
                dates.append(d)
                vals.append(fv)
                iso = d.isoformat()
                if iso > newest:
                    newest = iso
            if newest:
                cursors[sk] = max(cursors.get(sk, ""), newest)

        pages += 1
        tally.added_unit(len(vals) - n_before, f"offset {offset:,}")
        offset += len(docs)
        print(f"[{SOURCE}] [{offset:,}/{total:,}] series, {len(vals):,} obs", flush=True)
        if total and offset >= total:
            break
        time.sleep(RATE)

    if not vals:
        # Nothing usable from the ENTIRE run. Keep the existing 2.4M rows and report partial
        # rather than writing an empty table or claiming no_change.
        raise TransientError(f"{SOURCE}: no usable observations from any page this run")

    _check_key_shape(set(keys))

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    before = blob.row_count(path) if blob.exists(path) else 0
    total_rows, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    capped: dict[str, str] = {}
    truncated = merge_cursor_map(capped, cursors, cap=CURSOR_CAP)
    if truncated:
        print(f"[{SOURCE}] DISCLOSURE: {len(cursors):,} series changed but only "
              f"{len(capped):,} cursors reported (CURSOR_CAP); the rest keep their previous "
              f"freshness and their CSVs are not re-derived this run", flush=True)

    print(f"[{SOURCE}] {len(vals):,} obs over {pages} page(s) across {len(cursors):,} series; "
          f"store {before:,} -> {total_rows:,}", flush=True)
    return finalize(tally, total_rows, maxd or (since or None), source=SOURCE,
                    series_cursors=capped or None)
