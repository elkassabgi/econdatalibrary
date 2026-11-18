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


# ----------------------------------------------------------------- the row-loss guard
# WHY (ledger R1936). `area_ok` in refresh_pass is False only when a fetch RAISES. A sweep that
# completes but silently brings back less - a section answering an empty 200, a page loop that
# stops early - looks complete, and rebuild_with_tail then replaces every tail row with what came
# back. The served copy used to be overwritten in place, and nothing compared the result with what
# it replaced. So the rebuild now writes a CANDIDATE beside the final, this guard compares the
# candidate with the final, and only a candidate that passes replaces it.
#
# The rules mirror the multiset check that cleared the 2026-11-18 manual publish
# (F:/work/econ_finish/gus_dbw/multiset.py, same (series_key, obs_date) multiset semantics):
#   * BELOW the refresh window nothing may change at all. rebuild_with_tail copies those rows
#     verbatim, so any difference there is a defect. Checked as a multiset fingerprint - row count
#     plus the sum of hash(series_key, obs_date, value) - which is one streaming pass with no join,
#     and also catches a changed value, not only a lost row.
#   * INSIDE the window the publisher may withdraw a little (area_43 lost 3 placeholder pairs on
#     2026-11-18, the test_an_upstream_withdrawal_is_mirrored case). A loss above the floor is a
#     refusal, not a publish.
#   * AN EMPTY SWEEP is refused outright: a tail that had rows and now has none (R1937).
#   * NO SERIES MAY VANISH from the tail: a series_key that had tail rows and has none in the new
#     tail is refused, unless it is on GUARD_SERIES_DROP_ALLOW (R1936 required the per-series check).
#     A sweep that stops early loses whole series, and a whole small series is far under any floor.
#
# THE FLOOR is a fraction only, in integer basis points: floor(tail_old * 100 / 10,000) = 1%.
# It used to be max(100, 1%). R1937: that let an EMPTY sweep pass on every area whose tail had
# 1-100 pairs (14 of 194 on snap_20261117), and the 100 term was above 1% on 165 of 194 areas
# (area_12: 100 of 306 pairs = 33%). Measured 2026-11-18 on every area with a real refresh pair
# (R2's pre-publish copy vs snap_20261117, 16 areas; the other 177 were md5-identical to R2 and
# one area is new): the only loss was area_43, 3 of 7,869 pairs = 0.038% and no series dropped,
# 26x under the floor. The 1% itself is still a JUDGEMENT. Its cost: a tail under 100 pairs gets
# a floor of 0, so ANY withdrawal there is refused and the area stays due until someone looks -
# refusals are logged as ERROR and counted in the daily digest. A refusal costs a re-sweep; a
# wrong pass publishes the loss.
GUARD_TAIL_LOSS_BP = 100                      # basis points of the old tail pairs: 100 = 1%
GUARD_SERIES_DROP_ALLOW: frozenset = frozenset()   # series_keys the publisher withdrew, by name
# CLEARING A REFUSAL (R1938). A name list does not scale: area_46 has 14,163,345 tail series, so a
# real withdrawal there can drop thousands of names. A refusal is cleared by ONE reviewed, one-shot,
# COUNT-BASED clearance per area, appended to CLEARANCE_FILE in the store directory:
#   {"area": "46", "max_series_dropped": 1200, "max_tail_lost": 90000,
#    "reviewed_refusal": "20261118T110000Z", "by": "<who reviewed it>", "utc": "<when, ISO>"}
# The reviewer reads the refusal's guard.json (parts/_refresh_refused/area_<id>/<stamp>/), checks
# that the drop is the publisher's (not an early-stopped sweep), and writes the counts it accepts.
# The NEXT rebuild of that area may then drop up to max_series_dropped series and lose up to
# max_tail_lost tail pairs. It never clears an EMPTY sweep or a change below the window. It is
# spent by the first rebuild that passes (the area's last_refresh moves past its utc) and expires
# after GUARD_CLEARANCE_DAYS, so it can never become a standing allowance.
CLEARANCE_FILE = "_guard_clearances.jsonl"
GUARD_CLEARANCE_DAYS = 14
GUARD_DROPPED_EXAMPLES = 10
GUARD_MEMORY_LIMIT = "4GB"      # DuckDB spills past this (R690: the desktop runs the owner's jobs too)
GUARD_THREADS = 2


def _sql_path(p: str) -> str:
    return os.path.abspath(p).replace("\\", "/").replace("'", "''")


def row_loss_guard(old_path: str, new_path: str, tail_start: int, spill_dir: str | None = None,
                   clearance: dict | None = None) -> dict:
    """Compare a rebuilt candidate (new_path) with the final it would replace (old_path).

    Returns a dict with ok (bool), reasons (list of str) and the counts. Never raises for a
    verdict; an error while measuring propagates, and the caller treats that as a refusal.
    `clearance` is a reviewed one-shot clearance (guard_clearance()): it raises the series-drop and
    tail-loss limits for this one rebuild, never the empty-sweep or below-window rules."""
    import duckdb                       # lazy: the guard relaunches this script (R664 rule 2)

    cut = dt.date(tail_start, 1, 1).isoformat()
    o, n = _sql_path(old_path), _sql_path(new_path)
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{GUARD_MEMORY_LIMIT}'")
        con.execute(f"SET threads={GUARD_THREADS}")
        con.execute("SET preserve_insertion_order=false")
        if spill_dir:
            os.makedirs(spill_dir, exist_ok=True)
            con.execute(f"SET temp_directory='{_sql_path(spill_dir)}'")

        def fp(p):
            r = con.execute(
                f"SELECT count(*), coalesce(sum(hash(series_key, obs_date, value))::HUGEINT, 0) "
                f"FROM read_parquet('{p}') WHERE obs_date < DATE '{cut}' OR obs_date IS NULL").fetchone()
            return int(r[0]), int(r[1])

        pre_old, pre_new = fp(o), fp(n)
        pre_lost = 0
        if pre_old != pre_new:
            # only on the failure path: name how many pairs went missing below the window
            pre_lost = int(con.execute(f"""
                WITH a AS (SELECT series_key, obs_date, count(*) c FROM read_parquet('{o}')
                           WHERE obs_date < DATE '{cut}' OR obs_date IS NULL GROUP BY ALL),
                     b AS (SELECT series_key, obs_date, count(*) c FROM read_parquet('{n}')
                           WHERE obs_date < DATE '{cut}' OR obs_date IS NULL GROUP BY ALL)
                SELECT coalesce(sum(greatest(a.c - coalesce(b.c, 0), 0)), 0)
                FROM a LEFT JOIN b ON a.series_key IS NOT DISTINCT FROM b.series_key
                                  AND a.obs_date IS NOT DISTINCT FROM b.obs_date""").fetchone()[0])

        tail_old, tail_new, tail_lost, tail_gained = (int(x) for x in con.execute(f"""
            WITH a AS (SELECT series_key, obs_date, count(*) c FROM read_parquet('{o}')
                       WHERE obs_date >= DATE '{cut}' GROUP BY ALL),
                 b AS (SELECT series_key, obs_date, count(*) c FROM read_parquet('{n}')
                       WHERE obs_date >= DATE '{cut}' GROUP BY ALL)
            SELECT coalesce(sum(a.c), 0), coalesce(sum(b.c), 0),
                   coalesce(sum(greatest(coalesce(a.c, 0) - coalesce(b.c, 0), 0)), 0),
                   coalesce(sum(greatest(coalesce(b.c, 0) - coalesce(a.c, 0), 0)), 0)
            FROM a FULL OUTER JOIN b ON a.series_key IS NOT DISTINCT FROM b.series_key
                                    AND a.obs_date = b.obs_date""").fetchone())
        # A NULL series_key is matched as a key everywhere below (IS NOT DISTINCT FROM): with `USING`
        # and `NOT list_contains(...)` a NULL key never matched and never counted, so a vanished
        # NULL-key series passed the per-series rule unseen (review R1938).

        # series that had tail rows and have none in the new tail, minus the allow list
        dropped_n, dropped_eg = con.execute(f"""
            WITH a AS (SELECT DISTINCT series_key FROM read_parquet('{o}') WHERE obs_date >= DATE '{cut}'),
                 b AS (SELECT DISTINCT series_key FROM read_parquet('{n}') WHERE obs_date >= DATE '{cut}'),
                 d AS (SELECT a.series_key FROM a ANTI JOIN b
                           ON a.series_key IS NOT DISTINCT FROM b.series_key
                       WHERE NOT coalesce(list_contains(?::VARCHAR[], a.series_key), false))
            SELECT (SELECT count(*) FROM d),
                   (SELECT coalesce(list(coalesce(series_key, '<NULL series_key>')), []) FROM
                        (SELECT series_key FROM d ORDER BY 1 NULLS FIRST
                         LIMIT {int(GUARD_DROPPED_EXAMPLES)}))""",
            [sorted(GUARD_SERIES_DROP_ALLOW)]).fetchone()
        dropped_n, dropped_eg = int(dropped_n), list(dropped_eg or [])
    finally:
        con.close()             # a paused DuckDB cursor blocks os.replace on Windows (R664)

    floor = tail_old * int(GUARD_TAIL_LOSS_BP) // 10_000
    allow_series, cleared = 0, None
    if clearance:
        allow_series = int(clearance["max_series_dropped"])
        floor = max(floor, int(clearance["max_tail_lost"]))
        cleared = {k: clearance.get(k) for k in ("max_series_dropped", "max_tail_lost",
                                                 "reviewed_refusal", "by", "utc")}
    reasons = []
    if pre_old != pre_new:
        reasons.append(f"rows below {cut} changed: old {pre_old[0]:,} rows, new {pre_new[0]:,} rows, "
                       f"{pre_lost:,} pair(s) lost, fingerprints differ")
    if tail_old > 0 and tail_new == 0:
        reasons.append(f"EMPTY sweep: the tail from {cut} had {tail_old:,} rows and the rebuild has none")
    if dropped_n > allow_series:
        reasons.append(f"{dropped_n:,} series with tail rows from {cut} have none in the rebuild "
                       f"(not on GUARD_SERIES_DROP_ALLOW"
                       + (f", above the reviewed clearance of {allow_series:,}" if cleared else "")
                       + f"), e.g. {', '.join(dropped_eg)}")
    if tail_lost > floor:
        reasons.append(f"{tail_lost:,} tail pair(s) from {cut} lost, above the floor of {floor:,} "
                       f"(old tail {tail_old:,} rows)")
    return {"ok": not reasons, "reasons": reasons, "cutoff": cut,
            "pre_rows_old": pre_old[0], "pre_rows_new": pre_new[0], "pre_lost": pre_lost,
            "tail_rows_old": tail_old, "tail_rows_new": tail_new,
            "tail_lost": tail_lost, "tail_gained": tail_gained, "tail_loss_floor": floor,
            "series_dropped": dropped_n, "series_dropped_eg": dropped_eg, "clearance": cleared}


def guard_clearance(out_dir: str, area_id, now: dt.datetime | None = None,
                    state: dict | None = None) -> dict | None:
    """The newest reviewed clearance for one area that is still valid, or None (see CLEARANCE_FILE).
    Valid = well-formed, not older than GUARD_CLEARANCE_DAYS, and not yet spent: the area has not
    refreshed since it was written. A malformed line is ignored, so it can only leave a refusal in
    place, never widen one."""
    now = now or dt.datetime.now(dt.timezone.utc)
    aid = str(area_id)
    best = None
    try:
        with open(os.path.join(out_dir, CLEARANCE_FILE), encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return None
    for ln in lines:
        try:
            r = json.loads(ln)
            if str(r["area"]) != aid:
                continue
            t = dt.datetime.fromisoformat(r["utc"])
            t = t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
            if (int(r["max_series_dropped"]) < 0 or int(r["max_tail_lost"]) < 0
                    or not r.get("by") or not r.get("reviewed_refusal")):
                continue
        except (ValueError, KeyError, TypeError):
            continue
        if not (now - dt.timedelta(days=GUARD_CLEARANCE_DAYS) <= t <= now):
            continue
        if best is None or t > best[0]:
            best = (t, r)
    if best is None:
        return None
    rec = (load_state(out_dir) if state is None else state).get(aid)
    try:
        last = dt.datetime.fromisoformat(rec["last_refresh"]).astimezone(dt.timezone.utc)
    except (TypeError, KeyError, ValueError):
        last = None
    if last is not None and last >= best[0]:
        return None                       # spent: a rebuild passed after it was written
    return best[1]


def replace_final(candidate: str, final: str) -> None:
    """Move a candidate that passed the guard over the final. Retried for the Windows transient-handle
    race the way updater/blob.py's writers retry it; the last failure propagates (the caller then keeps
    the final it had)."""
    import time
    for attempt in range(6):
        try:
            os.replace(candidate, final)
            return
        except PermissionError:
            if attempt == 5:
                raise
            time.sleep(0.2 * (2 ** attempt))


def quarantine_tail_parts(area_id, parts_root: str, refused_root: str, guard: dict,
                          when: dt.datetime | None = None) -> str:
    """Move a refused rebuild's tail parts OUT of the next rebuild's reach, with the guard's verdict
    beside them, and return where they went. Kept as evidence, not reused: the next sweep re-fetches
    the tail, and a stale year file left in parts/_refresh would be merged into a later rebuild."""
    stamp = (when or dt.datetime.now(dt.timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    dest = os.path.join(refused_root, "area_%s" % area_id, stamp)
    os.makedirs(dest, exist_ok=True)
    for p in tail_parts_for_area(area_id, parts_root):
        os.replace(p, os.path.join(dest, os.path.basename(p)))
    with open(os.path.join(dest, "guard.json"), "w", encoding="utf-8") as f:
        json.dump(guard, f, indent=1, sort_keys=True, default=str)
    prune_refused(os.path.join(refused_root, "area_%s" % area_id))
    return dest


# A refused area stays due, so a persistent cause refuses it again on every pass. Each refusal
# moves a full tail sweep here, so the directory is CAPPED (R1937): the newest few refusals per
# area are kept as evidence, older ones are removed. Stamps are %Y%m%dT%H%M%SZ, so name order
# is time order.
REFUSED_KEEP_PER_AREA = 3
REFUSED_SUBDIR = os.path.join("parts", "_refresh_refused")


def prune_refused(area_dir: str, keep: int = REFUSED_KEEP_PER_AREA) -> int:
    """Remove all but the newest `keep` refusal folders of one area; return how many were removed."""
    import shutil
    try:
        stamps = sorted(e.name for e in os.scandir(area_dir) if e.is_dir())
    except FileNotFoundError:
        return 0
    old = stamps[:-keep] if keep > 0 else stamps
    for s in old:
        shutil.rmtree(os.path.join(area_dir, s))
    return len(old)


def refused_still_due(out_dir: str = OUT) -> dict:
    """{area: newest refusal UTC datetime} for areas whose newest refusal is later than their last
    recorded refresh - the areas the guard is holding back now. For the digest (R1937)."""
    root = os.path.join(out_dir, REFUSED_SUBDIR)
    state = load_state(out_dir)
    out: dict = {}
    try:
        areas = [e for e in os.scandir(root) if e.is_dir() and e.name.startswith("area_")]
    except FileNotFoundError:
        return out
    for e in areas:
        stamps = []
        for s in os.scandir(e.path):
            try:
                stamps.append(dt.datetime.strptime(s.name, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc))
            except ValueError:
                continue
        if not stamps:
            continue
        aid, newest = e.name[len("area_"):], max(stamps)
        rec = state.get(aid)
        try:
            last = dt.datetime.fromisoformat(rec["last_refresh"]).astimezone(dt.timezone.utc)
        except (TypeError, KeyError, ValueError):
            last = None              # never refreshed (or unreadable): the refusal is current
        if last is None or newest > last:
            out[aid] = newest
    return out


# ----------------------------------------------------------------- the owed-publish record
# WHAT IS OWED. After a rebuild that passed the guard, the local final is ahead of what users are
# served (R2 before T0; after T0 the local store is the parquet store, but the served CSV is still
# derived from it). Nothing in this process publishes - by design (R1936): a crawler that uploads
# and derives would skip the writer gates, eat its crawl budget, and after T0 hold the single-writer
# lock for days. So the crawler only RECORDS the debt, and a separate publisher pays it.
#
# WHY NOT state.db's csv_desktop_owed / full_rederive_owed. Both are rows in data/_aqueduct/state.db,
# which is a LINEAGE pulled from R2 and pushed back around every updater pass: a row this crawler
# wrote between passes is wiped by the next --pull-state, and a crawler holding a connection to it
# makes pull-state's os.replace fail - the R406 30-hour fleet outage. They also hold CSV debts only,
# and this debt starts with the parquet.
#
# THE RECORD. One append-only JSON-lines file per side, each with ONE writer:
#   _publish_owed.jsonl  written ONLY by this crawler - one line per passed rebuild: area, the final's
#                        size and md5 (the exact bytes the guard passed), rows, the guard's counts;
#   _published.jsonl     written ONLY by the publisher (a later PR) - one line per area it published,
#                        naming the md5 it published.
# An area is owed while its newest owed md5 has no published line. A later rebuild of the same area
# appends a new md5, so it is owed again even if an older one was published. Lines are flushed and
# fsynced; a torn last line (a crash mid-append) is skipped and COUNTED, never silently.
OWED_FILE = "_publish_owed.jsonl"
PUBLISHED_FILE = "_published.jsonl"


def file_md5(path: str) -> str:
    import hashlib
    h = hashlib.md5()                                                   # noqa: S324 - content id
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _append_line(path: str, rec: dict) -> None:
    line = json.dumps(rec, sort_keys=True, default=str)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def note_publish_owed(out_dir: str, area_id, final_path: str, guard: dict, rebuild: dict,
                      when: dt.datetime | None = None) -> dict:
    """Append the owed-publish record for a final that passed the guard; returns the record."""
    import pyarrow.parquet as _pq
    md = _pq.read_metadata(final_path)
    rec = {"area": str(area_id), "file": os.path.basename(final_path),
           "size": os.path.getsize(final_path), "md5": file_md5(final_path), "rows": md.num_rows,
           "rebuilt_utc": (when or dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds"),
           "rows_before": rebuild.get("rows_before"), "rows_after": rebuild.get("rows_after"),
           "guard": {k: v for k, v in guard.items() if k != "reasons"}}
    _append_line(os.path.join(out_dir, OWED_FILE), rec)
    return rec


def _read_lines(path: str) -> tuple[list, int]:
    recs, bad = [], 0
    try:
        with open(path, encoding="utf-8") as f:
            for ln in f:
                if not ln.strip():
                    continue
                try:
                    r = json.loads(ln)
                except ValueError:
                    bad += 1
                    continue
                if isinstance(r, dict) and r.get("area") is not None and r.get("md5"):
                    recs.append(r)
                else:
                    bad += 1
    except FileNotFoundError:
        pass
    return recs, bad


def publish_summary(out_dir: str = OUT, now: dt.datetime | None = None) -> dict:
    """The owed/refused facts as one small JSON-able dict, for the daily digest. The digest runs on
    CI, where data/ is absent, so the workstation's guard beat (tools/guard_heartbeat.py) carries this
    dict and the digest renders it (R1938). Times are stored, not ages, so the reader ages them
    against its own clock. Raises on an unreadable record; the beat records that as `unreadable`."""
    now = now or dt.datetime.now(dt.timezone.utc)
    if not os.path.isdir(out_dir):
        return {"measured_utc": now.isoformat(timespec="seconds"), "dir_present": False}
    owed, bad = owed_areas(out_dir)
    refused = refused_still_due(out_dir)
    oldest_utc, oldest_area, unreadable_times = None, None, 0
    for a, r in owed.items():
        try:
            t = dt.datetime.fromisoformat(r["rebuilt_utc"])
            t = t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)
        except (KeyError, TypeError, ValueError):
            unreadable_times += 1
            continue
        if oldest_utc is None or t < oldest_utc:
            oldest_utc, oldest_area = t, a
    newest_ref = max(refused.values()) if refused else None
    return {"measured_utc": now.isoformat(timespec="seconds"), "dir_present": True,
            "owed": len(owed), "oldest_owed_utc": oldest_utc.isoformat() if oldest_utc else None,
            "oldest_owed_area": oldest_area, "owed_time_unreadable": unreadable_times,
            "bad_lines": bad, "refused": len(refused),
            "refused_areas": sorted(refused, key=lambda x: (len(x), x))[:10],
            "newest_refusal_utc": newest_ref.isoformat() if newest_ref else None}


def owed_areas(out_dir: str = OUT) -> tuple[dict, int]:
    """({area: newest owed record} for every area not yet published at that md5, unreadable line count)."""
    owed, bad1 = _read_lines(os.path.join(out_dir, OWED_FILE))
    pub, bad2 = _read_lines(os.path.join(out_dir, PUBLISHED_FILE))
    newest: dict = {}
    for r in owed:                       # file order is append order: the last line per area wins
        newest[str(r["area"])] = r
    done = {(str(r["area"]), r["md5"]) for r in pub}
    return {a: r for a, r in newest.items() if (a, r["md5"]) not in done}, bad1 + bad2
