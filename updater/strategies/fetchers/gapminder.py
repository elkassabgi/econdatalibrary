"""S1 fetcher — Gapminder systema globalis (community GitHub DDF mirror).

License CC BY 4.0. Single grouped parquet clean_full/gapminder/gapminder.parquet,
schema (series_key, obs_date, value); series_key = '{indicator}:{geo}', obs_date is
Dec-31 of the data year. The whole table is rebuilt wholesale from hundreds of
datapoint CSVs in the open-numbers repo (no per-obs date filter upstream), so we
gate on the repo HEAD commit SHA (cheap vintage) and only re-crawl + MERGE when the
SHA moves (dedup series_key+obs_date, new wins on revision, never-shrink @0.97).

Parse logic + URLs are REUSED from jobs/ingest_gapminder.py (loaded by path; jobs/
is not a package). Per-file Tally: a fetch that fails after retries -> transient;
a 200 body that yields no SCALAR rows -> empty (benign: the repo legitimately
carries non-scalar indicators, e.g. income_mountains stores a JSON blob the
scalar parser skips — that is not a schema break). A true structural break is the
WHOLE crawl parsing 0 rows, which is flagged explicitly below.
"""
from __future__ import annotations
import importlib.util
import os

import pyarrow as pa

from ... import config, blob, merge
from ..base import Result
from ._common import Tally, finalize
from ._vintage import github_sha

SOURCE = "gapminder"
DEDUP = ("series_key", "obs_date")
REPO = "open-numbers/ddf--gapminder--systema_globalis"


def _ingester():
    """Load jobs/ingest_gapminder.py by path (jobs/ is not an importable package)."""
    p = os.path.join(config.JOBS_DIR, "ingest_gapminder.py")
    spec = importlib.util.spec_from_file_location("ingest_gapminder", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def current_vintage(unit):
    # vintage_signal: GitHub commits API HEAD SHA for the repo; changes iff the
    # mirror's data (or anything) changed. github_sha raises TransientError on a
    # 429/5xx/network blip — detect_change tolerates that (fetches anyway, safe).
    try:
        return github_sha(REPO)
    except Exception:
        return None


def _series_maxes(tbl):
    out = {}
    if tbl.num_rows == 0:
        return out
    for k, d in zip(tbl.column("series_key").to_pylist(), tbl.column("obs_date").to_pylist()):
        if d is None:
            continue
        if k not in out or d > out[k]:
            out[k] = d
    return {k: v.isoformat() for k, v in out.items()}


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "gapminder.parquet")
    before = blob.row_count(path)
    tally = Tally()

    ing = _ingester()
    files = ing.get_file_list()
    if not files:
        # Could not enumerate the tree at all (rate limit / network / moved repo).
        # Treat as a single transient sub-unit so the orchestrator retries and does
        # NOT stamp last_success on a frozen source.
        tally.transient_unit()
        return finalize(tally, before, None, source=SOURCE)

    keys, dates, vals = [], [], []
    for p in files:
        url = f"{ing.RAWBASE}/{p}"
        content = ing.get_bytes(url)
        if content is None:
            tally.transient_unit()      # fetch failed after retries -> retry next run
            continue
        k, d, v = ing.parse_ddf_csv(content, p)
        if not v:
            # 200 body but no scalar rows: benign for this repo (non-scalar / all-NaN
            # indicators the scalar parser intentionally skips). NOT a schema break.
            tally.empty_unit()
            continue
        keys.extend(k); dates.extend(d); vals.extend(v)
        tally.added_unit(len(v))        # provisional; real new-row count comes from merge below

    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })

    # Whole crawl yielded 0 scalar rows from real bodies (and not all-transient) ->
    # a genuine structural break (parser/format change). Flag it so finalize raises
    # DefinitiveError and existing data is kept (merge is never reached).
    if tbl.num_rows == 0:
        if tally.transient == 0:
            tally.structural_unit()
        return finalize(tally, before, None, source=SOURCE)

    n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)

    # Reconcile the Tally's row counter with the merge's true new-row count so the
    # ok/no_change decision reflects rows that ACTUALLY landed (revisions of existing
    # series_key+obs_date add 0 net rows).
    tally.added = max(0, n - before)
    return finalize(tally, n, md, source=SOURCE, series_cursors=_series_maxes(tbl))
