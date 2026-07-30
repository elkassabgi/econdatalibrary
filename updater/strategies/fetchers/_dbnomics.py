"""Shared implementation for sources whose ids ARE DBnomics series codes.

WHEN THIS IS THE RIGHT BASE, AND WHEN IT IS A TRAP. Much of this library's long tail arrived
through DBnomics, so the obvious repair for a frozen tail source is "fetch it from DBnomics
again". That only works where DBnomics is STILL INDEXING the provider. It largely is not:
tools/audit_upstream_liveness.py measured the newest DBnomics index per provider across the
served-but-unscheduled sources —

    UNCTAD  38 sources  127,413 series   2023-06-30
    FAO     18           87,579          2024-05-09
    UNESCO   4           57,530          2022-04-04
    WHO      3           34,788          2026-07-24   <- current
    BEA      1              240          2026-07-26   <- current

For the first three, a fetcher built on this base would run nightly, succeed, and transfer
nothing new — a green run asserting currency it cannot have. So this base is ONLY for providers
whose DBnomics index is actually moving, and every source using it must record the measured
index date in its registry entry so the claim is checkable later.

IDS NEED NO CROSSWALK, WHICH IS THE WHOLE REASON THIS IS SAFE. Our stored series_key is
`<PROVIDER>_<DATASET>:<dbnomics_series_code>` — e.g. `WHO_HWF:HWF_0001.AFG.A` — so the codes
are DBnomics' own, not a slugification we would have to reproduce. Verified as a FULL set
comparison rather than a sample: WHO/HWF returned 4,421 codes against our 4,421 published ids,
4,421 reproduced exactly, 0 ours-not-upstream, 0 upstream-new. Contrast the FAO family, where
the best key template reproduced 27.2% and had to be refused.

VINTAGE: the dataset's `indexed_at` from DBnomics. It is the mirror's own statement of when it
last re-indexed, so it moves exactly when there is something new to pull, and there is no
header to infer or flap (unlike fed_board's per-request Last-Modified or bis's replica ETags).

MERGE, NOT OVERWRITE. merge_and_write's never-shrink invariant is kept: a partial or truncated
DBnomics page must not be able to delete published history. The cost is that a series DBnomics
retires stays in our copy until someone retires it deliberately, which is the safer direction.
"""
from __future__ import annotations
import json
import os
import time
import urllib.error
import urllib.request

import pyarrow as pa

from ... import blob, config, merge
from ...errors import TransientError
from ..base import Result
from ._common import Deadline, Tally, finalize

API = "https://api.db.nomics.world/v22"
UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
PAGE = 1000
# DBnomics emits missing observations as the literal string "NA" (not null), so a naive
# float() raises and — if that exception were merely swallowed — the source would look like it
# had silently lost most of its data. Measured on WHO/RS: 7,409 observation slots, of which
# 5,198 are "NA" and 2,211 are real, which reproduces our published store EXACTLY (2,211 rows /
# 2,207 series). Those 5,181 extra upstream series are empty, not data we are missing. Named
# here so the drop is a documented decision with a count, not an accident.
NA_TOKENS = {"NA", "N/A", "", "None", "null", "nan"}


def _get(url, tries=4):
    last = None
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(url, headers=UA), timeout=300) as r:
                return json.loads(r.read())
        except Exception as e:                               # noqa: BLE001
            last = e
            if attempt < tries - 1:
                time.sleep(2 ** attempt)
    raise TransientError(f"dbnomics GET failed: {url} :: {last!r}")


def dataset_indexed_at(provider: str, dataset: str):
    """The dataset's newest index timestamp, or None if it cannot be read cheaply."""
    try:
        d = _get(f"{API}/datasets/{provider}?limit=300")
    except Exception:                                        # noqa: BLE001
        return None
    for x in (d.get("datasets", {}).get("docs") or []):
        if (x.get("code") or "").upper() == dataset.upper():
            return x.get("indexed_at") or x.get("updated_at")
    return None


def vintage(provider: str, dataset: str):
    stamp = dataset_indexed_at(provider, dataset)
    if not stamp:
        return None
    return f"dbnomics:{provider}/{dataset}:{stamp}"


def _iter_series(provider: str, dataset: str, dl: Deadline):
    """Yield (series_code, [(iso_date, value), ...]) for every series in the dataset."""
    off = 0
    while True:
        if dl.spent():
            return
        d = _get(f"{API}/series/{provider}/{dataset}"
                 f"?limit={PAGE}&offset={off}&observations=1")
        s = d.get("series", {})
        docs = s.get("docs") or []
        if not docs:
            return
        for x in docs:
            periods = x.get("period") or []
            values = x.get("value") or []
            yield x.get("series_code"), list(zip(periods, values))
        off += len(docs)
        if off >= (s.get("num_found") or 0):
            return


def _to_date(p: str):
    """DBnomics periods are ISO-ish: 2024, 2024-03, 2024-03-31, 2024-Q1."""
    if not p:
        return None
    p = str(p)
    try:
        if len(p) == 4:
            return f"{p}-01-01"
        if len(p) == 7 and p[4] == "-":
            return f"{p}-01"
        if len(p) == 10:
            return p
        if "Q" in p.upper():
            y, q = p.upper().split("-Q") if "-Q" in p.upper() else (p[:4], p[-1])
            return f"{int(y)}-{(int(q) - 1) * 3 + 1:02d}-01"
    except (ValueError, IndexError):
        return None
    return None


def run(source: str, provider: str, dataset: str, budget_min: int = 25) -> Result:
    """Pull one DBnomics dataset into <source>/<source>.parquet under our own key shape."""
    import datetime as dt

    out_dir = config.source_dir(source)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{source}.parquet")

    stamp = dataset_indexed_at(provider, dataset)
    if stamp is None:
        raise TransientError(f"{source}: DBnomics dataset {provider}/{dataset} not listed")

    tally = Tally()
    dl = Deadline(minutes=budget_min)
    keys, dates, vals = [], [], []
    n_series = 0
    n_na = 0

    for code, obs in _iter_series(provider, dataset, dl):
        if not code:
            continue
        n_series += 1
        got = 0
        for p, v in obs:
            if v is None or (isinstance(v, str) and v.strip() in NA_TOKENS):
                n_na += 1                                    # documented missing, not a loss
                continue
            d = _to_date(p)
            if not d:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                n_na += 1
                continue
            if fv != fv:                                     # NaN
                n_na += 1
                continue
            keys.append(f"{provider}_{dataset}:{code}")
            dates.append(dt.date.fromisoformat(d))
            vals.append(fv)
            got += 1
        if got:
            tally.added_unit(got, code)

    if not keys:
        # Distinguish "everything upstream is NA" from "we parsed nothing": both give zero
        # rows, but only the second is our bug.
        # The dataset listed but yielded no usable observations — a real break, not a quiet
        # period. Raise so the run is partial and the existing parquet is kept untouched.
        raise TransientError(
            f"{source}: {provider}/{dataset} returned {n_series} series, 0 usable observations "
            f"({n_na:,} NA/missing slots)")

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array(vals, pa.float64()),
    })
    before = blob.row_count(path) if blob.exists(path) else 0
    total, maxd = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
    print(f"[{source}] {n_series:,} series, {len(keys):,} obs pulled "
          f"({n_na:,} upstream slots were NA/missing); store {before:,} -> {total:,} "
          f"(indexed_at {stamp})", flush=True)
    return finalize(tally, total, maxd, source=source)
