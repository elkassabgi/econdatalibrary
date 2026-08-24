#!/usr/bin/env python3
"""Keep gus_dbw current after its backfill, by re-sweeping the recent-year tail.

WHY THIS EXISTS. The backfill finished on 2026-08-24 with 1,237,766,278 observations, and at
that moment gus_dbw stopped updating in two independent ways (ledger R475):

  1. `jobs/ingest_gus_dbw.py` wrote `logs/gus_dbw.DONE` to record "the pass completed", but to
     `RELAUNCH_GUARD.ps1` that filename means "never relaunch this job". The crawler retired
     itself.
  2. Even relaunched, its per-area gate is `if os.path.exists(final): skip` — an area that has
     ever been crawled is never looked at again.

GUS exposes no last-modified anywhere (unlike CBS, whose per-table `Modified` drives the
equivalent gate in `ingest_cbs_nl.py`). The registry already prescribes the alternative in its
own `strategy_reason`: "re-sweep only the latest 1-2 years and UPSERT into the area parquet
keyed on (series_key,obs_date) instead of the binary done-flag skip". That is what this does.

THE UPSERT IS BY YEAR BOUNDARY, WHICH IS WHY IT IS SAFE. A refresh re-fetches every observation
the publisher currently holds for the tail years, so for those years the fetch IS the truth and
replacing wholesale is correct — including when it returns FEWER rows, because GUS withdrawing
a series is a real revision we must mirror. What would not be safe is replacing on a PARTIAL
fetch, so `rebuild_with_tail` is only ever called for an area whose sweep completed with zero
transient failures; anything less leaves the area untouched for the next run. This mirrors the
backfill's existing `area_complete` discipline rather than inventing a second one.

IT STREAMS. area_46 alone holds 529,322,150 rows; reading a final parquet into memory to
filter it would repeat R473 (a fix that made failing work succeed, then met a 16 GB ceiling).
Rows are copied batch-by-batch through a ParquetWriter, so peak memory is one batch.
"""
from __future__ import annotations

import datetime as dt
import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "clean_full", "gus_dbw")
STATE_FILE = "_refresh_state.json"

# How many calendar years of tail to re-sweep. Two, not one: GUS revises the previous year well
# into the current one (provisional -> final), and a one-year window would mirror the revision
# only while it happened to fall inside the current calendar year.
TAIL_YEARS = 2

# Registry cadence for gus_dbw is monthly, so an area is re-swept at most every 30 days. The
# request budget is the reason this is not more frequent: ~2,768 sections x 2 years x periods
# is on the order of 16,000 calls, which at the registered tier (X-ClientId, SLEEP=1.0s) is
# hours, and at the anonymous tier (SLEEP=60.5s) would exceed the weekly cap outright.
REFRESH_DAYS = 30

BATCH_ROWS = 250_000


def tail_start_year(today: dt.date | None = None, years: int = TAIL_YEARS) -> int:
    """First calendar year the refresh re-fetches (inclusive)."""
    y = (today or dt.date.today()).year
    return y - years + 1


# ----------------------------------------------------------------- refresh state
def _state_path(out_dir: str = OUT) -> str:
    return os.path.join(out_dir, STATE_FILE)


def load_state(out_dir: str = OUT) -> dict:
    try:
        with open(_state_path(out_dir), encoding="utf-8") as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_state(state: dict, out_dir: str = OUT) -> None:
    tmp = _state_path(out_dir) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=0, sort_keys=True)
    os.replace(tmp, _state_path(out_dir))


def mark_refreshed(area_id, state: dict, when: dt.datetime | None = None,
                   rows_before: int = 0, rows_after: int = 0) -> dict:
    state[str(area_id)] = {
        "last_refresh": (when or dt.datetime.now()).isoformat(timespec="seconds"),
        "rows_before": rows_before,
        "rows_after": rows_after,
    }
    return state


def area_due(area_id, state: dict, now: dt.datetime | None = None,
             days: int = REFRESH_DAYS) -> bool:
    """Is this area due for a re-sweep?

    An area with no recorded refresh is due — that is every area right after the backfill,
    which is the state the whole source is in today. An unparseable timestamp is also due:
    failing towards re-fetching costs requests, while failing towards skipping would freeze the
    area silently, and freezing silently is the defect this module exists to end.
    """
    rec = state.get(str(area_id))
    if not isinstance(rec, dict) or not rec.get("last_refresh"):
        return True
    try:
        last = dt.datetime.fromisoformat(rec["last_refresh"])
    except (ValueError, TypeError):
        return True
    return ((now or dt.datetime.now()) - last).days >= days


# ----------------------------------------------------------------- the upsert
def rebuild_with_tail(final_path: str, tail_parts: list[str], tail_start: int,
                      out_path: str | None = None) -> dict:
    """Replace every row from `tail_start` onward with freshly fetched tail rows.

    Streams: peak memory is one batch, not one area. Writes to a temp file and renames, so a
    crash can never leave a truncated final in place of a good one.

    Returns counts, so the caller can log what the publisher actually changed rather than
    asserting that it changed something.
    """
    out_path = out_path or final_path
    cutoff = dt.date(tail_start, 1, 1)
    schema = pq.read_schema(final_path)
    tmp = out_path + ".refresh.tmp"

    kept = dropped = added = 0
    writer = pq.ParquetWriter(tmp, schema, compression="zstd")
    try:
        # The reader MUST be closed before the rename. On Windows os.replace fails with
        # WinError 5 while any handle on the destination is open, so a `pq.ParquetFile` left
        # open here would abort every refresh at the last step — after paying for the whole
        # sweep. Caught by test_streaming_across_many_batches_preserves_every_row.
        with pq.ParquetFile(final_path) as pf:
            for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                tbl = pa.Table.from_batches([batch], schema=batch.schema)
                n_in = tbl.num_rows
                mask = pc.less(tbl.column("obs_date"), pa.scalar(cutoff, pa.date32()))
                head = tbl.filter(mask)
                kept += head.num_rows
                dropped += n_in - head.num_rows
                if head.num_rows:
                    writer.write_table(head.cast(schema))
        for p in tail_parts:
            t = pq.read_table(p)
            if t.num_rows:
                added += t.num_rows
                writer.write_table(t.cast(schema))
    except Exception:
        writer.close()
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    writer.close()
    os.replace(tmp, out_path)
    return {"kept": kept, "dropped": dropped, "added": added,
            "rows_before": kept + dropped, "rows_after": kept + added}


def tail_parts_for_area(area_id, parts_root: str) -> list[str]:
    """Every per-year part the refresh sweep wrote for this area, oldest first."""
    d = os.path.join(parts_root, "area_%s" % area_id)
    if not os.path.isdir(d):
        return []
    return sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".parquet"))


def clear_tail_parts(area_id, parts_root: str) -> int:
    n = 0
    for p in tail_parts_for_area(area_id, parts_root):
        try:
            os.remove(p)
            n += 1
        except OSError:
            pass
    return n
