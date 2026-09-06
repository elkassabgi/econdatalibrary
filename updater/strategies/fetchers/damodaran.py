"""S1 fetcher — Damodaran Online Financial Data (NYU Stern, Aswath Damodaran).

Annual cross-sectional snapshots (country risk premiums, betas, margins, multiples,
cost of capital, etc.), re-estimated each January and re-posted at pages.stern.nyu.edu.
Single grouped parquet clean_full/damodaran/damodaran.parquet, schema
(series_key, obs_date, value); series_key = DAMODARAN:<dataset>:<col>:<entity>.

S1 (overwrite_if_changed): each release re-publishes the whole workbooks, so we
re-fetch every dataset by REUSING jobs/ingest_damodaran.py's URL list + parse logic,
build one pyarrow table, and MERGE (dedup series_key+obs_date, new wins on revision,
never-shrink). The cheap vintage is the country-premium workbook's HEAD token
(Last-Modified/ETag) — the canonical annual-release marker per the registry hint.

A 200 that parses 0 rows from a real body is structural; a timeout/5xx/429/network
failure is transient; all datasets 404 (URLs rotated) -> all-empty window -> structural.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os

import pyarrow as pa
import requests

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import http_vintage, UA

SOURCE = "damodaran"
DEDUP = ("series_key", "obs_date")

# The country-premium workbook is the canonical annual-release marker (rotates name each
# January, e.g. ctrypremApr26.xlsx). HEAD Last-Modified/ETag on it is the cheap vintage.
VINTAGE_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/ctrypremApr26.xlsx"

# Transient HTTP statuses (retry next tick) vs definitive (treat as not-found / skip dataset).
_TRANSIENT_STATUS = (429, 500, 502, 503, 504)


def _ingest_mod():
    """Load jobs/ingest_damodaran.py as a module to reuse ANNUAL_DATASETS + parse_dataset.

    The ingester is a standalone script (not a package), so import it by path rather
    than copy its 20-dataset table / multi-sheet parse logic."""
    path = os.path.join(config.JOBS_DIR, "ingest_damodaran.py")
    spec = importlib.util.spec_from_file_location("_ingest_damodaran", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def current_vintage(unit):
    """Cheap probe: HEAD token of the country-premium workbook (annual-release marker)."""
    return http_vintage(VINTAGE_URL)


def _fetch(url):
    """Re-fetch one workbook. Returns (bytes | None, outcome) where outcome is one of
    'ok', 'transient', 'notfound' so update() can tally honestly."""
    try:
        r = requests.get(url, headers=UA, timeout=120, allow_redirects=True)
    except (requests.Timeout, requests.ConnectionError):
        return None, "transient"
    if r.status_code in _TRANSIENT_STATUS:
        return None, "transient"
    if r.status_code == 200 and len(r.content) > 500:
        return r.content, "ok"
    # 403/404/other or a too-small body -> treat as this workbook missing/rotated.
    return None, "notfound"


def _series_maxes(keys, dates):
    out = {}
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


_QUAL = "__"          # the separator ingest_damodaran uses when it disambiguates a label


def _stale_grain_keys(path, new_keys, before=0, limit=5):
    """Keys the store holds in their PRE-FIX, unqualified form. Empty means safe to merge.

    A qualified key looks like `DAMODARAN:<ds>:<label>__<qualifier>:<entity>`; its pre-fix form
    is the same key with `__<qualifier>` removed. If the store still contains that form, the
    store was written by the old parser and a merge would leave BOTH — which is the whole
    failure mode this guard exists for, and one that never-shrink cannot see because the file
    grows.

    Reads only the series_key column, through blob so it works under AQUEDUCT_BACKEND=r2 (R36).

    FAILS CLOSED WHEN THERE IS SOMETHING TO PROTECT. `before` is the store's row count, already
    computed by the caller. If it is 0 there is no store and no first publish should ever be
    blocked by this guard, so an unreadable path yields no stale keys. If it is non-zero the
    store exists and we could not read it — and a guard that waves a doubling merge through on
    a transient read error is the fail-open branch R503 was written about, so it reports the
    failure as stale instead.
    """
    qualified = {k for k in new_keys if _QUAL in k}
    if not qualified:
        return []
    try:
        have = set(blob.read_table(path, columns=["series_key"])
                   .column("series_key").to_pylist())
    except Exception as e:                                            # noqa: BLE001
        if before:
            return [f"<store holds {before:,} rows but could not be read: "
                    f"{type(e).__name__}; refusing to merge blind>"]
        return []                                # genuinely no store yet -> nothing to protect
    # a SET: every qualified sibling of one old key derives back to that same key, so
    # EV_EBITDA__All_firms and EV_EBITDA__Only_positive_EBITDA_firms would otherwise report
    # `...:EV_EBITDA:Advertising` twice and the count would overstate the damage.
    stale = set()
    for k in qualified:
        head, _, tail = k.rpartition(":")        # entity
        label_part, sep, _qual = head.rpartition(_QUAL)
        if not sep:
            continue
        old = f"{label_part}:{tail}"
        if old in have:
            stale.add(old)
            if len(stale) >= limit:
                break
    return sorted(stale)


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "damodaran.parquet")
    before = blob.row_count(path)
    tally = Tally()

    ing = _ingest_mod()

    all_keys, all_dates, all_vals = [], [], []
    for entry in ing.ANNUAL_DATASETS:
        if len(entry) == 4:
            dataset, url, _entity_type, specific_sheets = entry
        else:
            dataset, url, _entity_type = entry
            specific_sheets = None

        data, outcome = _fetch(url)
        if outcome == "transient":
            tally.transient_unit(f"{dataset}: fetch failed (transient)")
            continue
        if outcome == "notfound" or not data:
            # A rotated/missing workbook on a normally-multi-file source: count as empty
            # (genuine 404 for one of ~22 files). If EVERY file 404s, finalize's
            # all-empty-window guard escalates to a structural break (URLs rotated).
            tally.empty_unit(f"{dataset}: 404 or empty body — one workbook rotated")
            continue

        try:
            k, d, v = ing.parse_dataset(data, dataset, url, specific_sheets)
        except Exception as e:  # noqa: BLE001
            # 200 returned a body but it failed to parse -> structural/schema break.
            tally.structural_unit(f"{dataset}: body will not parse — {type(e).__name__}")
            continue

        if v:
            all_keys.extend(k); all_dates.extend(d); all_vals.extend(v)
            tally.attempted += 1            # parsed-ok; real new-row count assigned after merge
        else:
            # 200 with a real (>500B) body but 0 rows parsed -> structural break.
            tally.structural_unit(f"{dataset}: real body parsed 0 rows")

    # If nothing parsed (all transient, or guards not yet tripped), don't write.
    if not all_vals:
        return finalize(tally, before, None, source=SOURCE)

    tbl = pa.table({
        "series_key": pa.array(all_keys, pa.string()),
        "obs_date":   pa.array(all_dates, pa.date32()),
        "value":      pa.array(all_vals, pa.float64()),
    })

    # Publish ONLY via merge (atomic, dedup, never-shrink). The honest new-row count is
    # the merge delta (n-before), not parsed rows — an unchanged release merges to 0 new
    # rows and must read as no_change, not ok.
    #
    # min_ratio=0.92 (vs default 0.97): the legacy ingester wrote damodaran.parquet WITHOUT
    # deduping. CORRECTED TWICE 2026-08-31 ("the four"; R527 then R535 — the first
    # correction of the false "0 distinct observations lost" itself shipped a false
    # "that collapse is history"): the store TODAY still holds every duplicate —
    # 26,536 rows / 24,687 distinct (series_key,obs_date), 1,842 dup groups, 721 of
    # them carrying DIFFERING values (measured; dups surviving under this merge-dedup
    # fetcher PROVE no merge has ever completed here). The first merge that completes
    # will collapse those ~1,849 rows and PICK one of two real numbers for each of
    # the 721 (last-in-sort wins) — a PENDING lossy event, disclosed here, not
    # history. Also pending: ONE mangled-shaped key survives outside the margins
    # class (histretSP:Real_Estate:328_442, R480's disease; the margins-class rows
    # themselves measure 0 in-store). 0.92 admits that collapse yet
    # still catches a real truncation — e.g. a pull missing even one ~1400-obs workbook
    # (~21900 rows) falls below the 0.92 floor (23177) and is refused. After the first run
    # the file is dup-free and stays >= its distinct count. This is an explicit per-call
    # min_ratio (sanctioned by merge_and_write), NOT a weakening of the shared guard.
    # RE-GRAIN GUARD. The key fix for R516 qualifies labels that used to collide, so a fixed
    # parser emits `...:EV_EBITDA__All_firms:Advertising` where the store holds
    # `...:EV_EBITDA:Advertising`. Those two never collide, which is precisely the danger: a
    # merge ADDS the new keys beside the old ones, the file only GROWS, and never-shrink and
    # min_ratio both wave it through (R22/R333 — ons_uk reached 20,198,302 rows for 10,099,151
    # observations exactly this way). The repair is a CLEAN RE-PULL, never a merge.
    #
    # The check needs no sidecar and self-clears: for every qualified key this pull produced,
    # ask whether its UNQUALIFIED form is still in the store. If it is, the store predates the
    # fix and merging would double that series. After `python jobs/ingest_damodaran.py` has
    # rewritten the file (a plain pq.write_table, i.e. an overwrite), no unqualified form
    # remains and the guard passes silently for ever after.
    stale = _stale_grain_keys(path, all_keys, before=before)
    if stale:
        tally.structural_unit(
            f"store is on the pre-R516 key grain: {len(stale):,} key(s) would be DOUBLED by a "
            f"merge (e.g. {stale[0]}). A re-grain needs a clean re-pull, not a merge — run "
            f"`python jobs/ingest_damodaran.py` once, then this fetcher resumes normally.")
        return finalize(tally, before, None, source=SOURCE, merged_rows=0)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP, min_ratio=0.92)
    new_rows = max(0, n - before)
    if new_rows:
        tally.added += new_rows
    else:
        tally.empty += 1
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(all_keys, all_dates))
