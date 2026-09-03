"""S3 (sdmx_delta) fetcher — Statistics Latvia (CSP), PxWeb v1 API. No key.

Family note: Statistics Latvia is a PxWeb source (NOT raw SDMX 2.1). The S3
"sdmx_delta" contract is satisfied with the PxWeb equivalent of a date-tail:
for each table we POST a query whose TIME dimension is restricted to the codes
strictly AFTER the table's stored max(obs_date) (the PxWeb analogue of
?startPeriod=<max_obs+1>). updatedAfter is not offered by this PxWeb host, so the
time-code restriction IS the delta mechanism. All non-time dimensions are requested
in full (mirroring jobs/ingest_stat_latvia.py's full-item selection / aggregate
fallback), so a new period brings the full cross-tab for that period.

Layout (set by jobs/ingest_stat_latvia.py): ONE parquet per GROUP under
clean_full/stat_latvia/<db>_<first-path-segment>.parquet, schema
(series_key, obs_date, value) where
  series_key = "LV:<db>:<path-with-'/'->':'>:<dim>=<code>:..."  (TIME excluded)
  obs_date   = date32 (PxWeb period code -> date via the ingester's parse_date)
  value      = float64
The catalog (data/clean_full/stat_latvia/_catalog.json) maps {db, path, id} for
every table; the group filename is f"{db}_{path.split('/')[0]}.parquet". A table's
on-disk rows are exactly the keys with prefix "LV:<db>:<path-with-':'>:".

Incremental, per TABLE (the true sub-unit — NOT per group): a group file holds
many tables of mixed frequency. Annual tables push the GROUP max to YYYY-12-31,
which is LATER than a fresh monthly code (e.g. 2026M05 -> 2026-05-01). Using a
group-wide max would therefore silently skip newer monthly/quarterly data, so we
compute max(obs_date) PER TABLE from its key-prefix slice and restrict that
table's TIME variable to codes parsing strictly after it (the stored boundary day
is re-fetched too, so an in-place revision of the latest period is captured; merge
dedups the overlap). REUSES the ingester's endpoints, catalog crawl, json-stat2
parse, and date parsing verbatim (imported, not re-coded). THE tailed time axis is
picked by the shared value-first resolver (core/pxweb.py: authoritative `time: true`
code, else highest date-parse-rate, else literal name) and threaded into
parse_jsonstat2 as its authoritative time_code, so the axis the delta restricts is
EXACTLY the axis the parser keys obs_date on (the OLD name-first is_time_dim scan
picked a month axis on month+year cubes — its codes '01'..'12' parse to no date, the
">= boundary" tail matched nothing, and the table froze silently).

Honest status (Tally + finalize), one sub-unit PER TABLE:
  added_unit(n)    rows merged for the table (n>0 new / n==0 nothing newer)
  empty_unit()     table already current (no TIME codes after the boundary) or a
                   legitimately-empty 200/404 for the incremental window
  transient_unit() timeout / 5xx / 429 / network / non-JSON 200 -> run is 'partial'
  structural_unit() a FULL re-fetch (table with no stored history, or a metadata
                   probe) that returned a real PxWeb envelope but 0 usable rows /
                   no time dimension -> schema/structural break
A group's merge happens once at the end of the group from all its tables' new rows,
ALWAYS via merge.merge_and_write(path, tbl, mode="merge", dedup_keys=("series_key",
"obs_date")) so existing data is never shrunk. empty_window_floor is set above the
table count so a healthy steady-state run (every table "nothing newer") is honest
no_change, not a false structural break; real breaks are caught per-table.
"""
from __future__ import annotations
import datetime as dt
import os
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, rotate_after,
                      sane_since, save_rotation)

import sys
# The shared value-first PxWeb time-axis resolver lives in this repo's core/ package
# (core/pxweb.py). Derive the repo root from __file__ — updater/strategies/fetchers/ is
# four levels below it — so `from core import pxweb` resolves to THIS checkout's copy both
# when the updater imports this fetcher as a package and if it is loaded standalone. No
# hardcoded ROOT: only the worktree carries core/pxweb.py on this branch (same __file__
# convention as jobs/ingest_hagstofa.py and tools/pxweb_regression.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
from core import pxweb as _pxweb

# Reuse the ingester's endpoints + parse/enumeration logic verbatim (do not re-discover).
from jobs.ingest_stat_latvia import (
    BASE, UA, RATE, MAX_CELLS, DATABASES,
    crawl_catalog, is_time_dim, parse_date, parse_jsonstat2,
)

SOURCE = "stat_latvia"
DEDUP = ("series_key", "obs_date")
TIMEOUT = 90
GET_TRIES = 4
POST_TRIES = 4


# --------------------------------------------------------------------------- #
# HTTP with an HONEST transient/definitive split (ingester swallowed errors to
# None; here a timeout/5xx/429/network/non-JSON-200 must surface as TransientError
# so the run is 'partial', never a silent no_change).
# --------------------------------------------------------------------------- #
def _get(sess, url):
    """GET PxWeb JSON. None on 400/404 (table/metadata not available -> empty).
    TransientError on timeout/5xx/429/network/non-JSON-200 after the budget."""
    last = None
    for a in range(GET_TRIES):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == GET_TRIES - 1:
                raise TransientError(f"stat_latvia GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == GET_TRIES - 1:
                    raise TransientError(f"stat_latvia GET {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == GET_TRIES - 1:
                raise TransientError(f"stat_latvia GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"stat_latvia GET {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"stat_latvia GET {url[-60:]}: {last}")


def _post(sess, url, body):
    """POST a PxWeb query. None on 400/403 (selection rejected/empty -> empty).
    TransientError on timeout/5xx/429/network/non-JSON-200 after the budget."""
    last = None
    for a in range(POST_TRIES):
        try:
            r = sess.post(url, json=body, headers=UA, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == POST_TRIES - 1:
                raise TransientError(f"stat_latvia POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == POST_TRIES - 1:
                    raise TransientError(f"stat_latvia POST {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 403):
            return None  # selection out of range / empty window — legitimately empty
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == POST_TRIES - 1:
                raise TransientError(f"stat_latvia POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"stat_latvia POST {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"stat_latvia POST {url[-60:]}: {last}")


# --------------------------------------------------------------------------- #
# Key/path plumbing (matches jobs/ingest_stat_latvia.query_table's prefix exactly)
# --------------------------------------------------------------------------- #
def _table_prefix(db: str, path: str) -> str:
    """The series_key prefix the ingester wrote for a table, WITHOUT trailing ':'.
    (query_table used prefix = f"LV:{db}:{path.replace('/', ':')}".)"""
    return f"LV:{db}:{path.replace('/', ':')}"


def _key_table_prefix(series_key: str) -> str:
    """Extract the table prefix (head before the first 'dim=code' token) from a key."""
    parts = series_key.split(":")
    head = []
    for p in parts:
        if "=" in p:
            break
        head.append(p)
    return ":".join(head)


def _group_filename(db: str, path: str) -> str:
    return f"{db}_{path.split('/')[0]}.parquet"


def _per_table_max(path: str) -> dict[str, dt.date]:
    """Map table-prefix -> max(obs_date) on disk for one group parquet."""
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    for k, d in zip(keys, dates):
        if d is None:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        pref = _key_table_prefix(k)
        prev = out.get(pref)
        if prev is None or d > prev:
            out[pref] = d
    return out


# --------------------------------------------------------------------------- #
# Query ONE table for periods strictly after `boundary` (None => full backfill)
# --------------------------------------------------------------------------- #
def _query_table_delta(sess, table_info, boundary):
    """Return (rows, status) where rows is list[(series_key, date, value)] and
    status is one of:
       'data'       -> a 200 query returned >=1 usable observation
       'empty'      -> table current / 200-but-no-newer / 400-403-404 empty window,
                       OR a never-on-disk table that yields 0 rows (the ingester got
                       the SAME outcome for it — not a break)
       'structural' -> a table that ALREADY had on-disk history (boundary is not None)
                       whose 200 envelope now has no time dimension / no variables
                       (a genuine schema break for a previously-working table)

    boundary is a date (incremental: keep TIME codes > boundary, inclusive of the
    boundary day so a same-period revision is re-pulled) or None (full backfill of a
    table with no on-disk history — mirrors the ingester's full selection).

    Structural is deliberately NEVER raised for a boundary-None (never-landed) table:
    the ingester itself skipped tables that produced 0 time-series rows (e.g. the
    over-cell-cap aggregate fallback hit an all-eliminated cell), so reproducing that
    "nothing" is honest empty, not a break. Structural is reserved for a table that
    used to work and whose structure vanished.
    """
    # A 0-row / no-structure outcome is a BREAK only for a table that previously had
    # data on disk (boundary set); for a never-landed table it's honest empty. Track the
    # ORIGINAL on-disk state before any boundary cap (a corrupt-boundary table still has
    # history, so it must keep structural-break semantics on a vanished envelope).
    had_history = boundary is not None
    # FAR-FUTURE BOUNDARY CAP (PxWeb sentinel guard): a corrupt stored max(obs_date) — a
    # year-9999/6000/2584-style sentinel from a TIME-dim heuristic misclassification —
    # would make the `>= boundary` delta filter select NOTHING, freezing the table
    # forever. sane_since() returns None for such a date, so we fall back to a FULL pull
    # (boundary=None semantics) instead of a broken delta. A sane boundary passes through
    # unchanged. (zero_status keeps the original on-disk state via had_history.)
    boundary = sane_since(boundary)
    zero_status = "structural" if had_history else "empty"
    db, path = table_info["db"], table_info["path"]
    url = f"{BASE}/{db}/{path}/"

    meta = _get(sess, url)
    time.sleep(RATE)
    if meta is None or not isinstance(meta, dict):
        # 400/404 on the metadata probe -> table unavailable this run (empty).
        return [], "empty"
    variables = meta.get("variables", [])
    if not variables:
        # A 200 envelope with no variables: structural ONLY for a previously-landed
        # table (boundary set); for a never-landed table it's honest empty (zero_status).
        return [], zero_status

    # Identify THE time variable with the shared value-first resolver (core/pxweb.py)
    # fed the metadata's authoritative `time: true` code, else highest date-parse-rate,
    # else literal name — with this source's parse_date grammar. The resolved code is
    # ALSO threaded into parse_jsonstat2 below as its authoritative time_code, so the
    # axis tailed here is EXACTLY the axis the parser keys obs_date on. The OLD
    # selection took the FIRST is_time_dim() match in variable order, which in a
    # month+year cube picked the month axis: its codes ('01'..'12') parse to no date,
    # so the ">= boundary" filter matched NOTHING and the table was reported
    # permanently current — a silent freeze (and, left unthreaded, the imported
    # parser name-matched the same month axis and parsed 0 rows).
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    time_idx = _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=parse_date)
    if time_idx is None:
        # No detectable time dimension. For a previously-landed table that's a break
        # (it used to be a time series); for a never-landed table the ingester also
        # produced nothing -> honest empty (zero_status).
        return [], zero_status
    time_var = variables[time_idx]
    time_code = time_var.get("code")

    all_time_vals = time_var.get("values", []) or []
    if boundary is not None:
        wanted_time = [tv for tv in all_time_vals
                       if (pd := parse_date(tv)) is not None and pd >= boundary]
        if not wanted_time:
            return [], "empty"  # table already current — nothing newer than boundary
    else:
        wanted_time = all_time_vals
        if not wanted_time:
            return [], "empty"  # never-landed table with an empty time dim -> honest empty

    # Build the POST selection: TIME restricted to wanted codes; every other variable
    # selected exactly as the ingester would (full items under the cell cap, else the
    # aggregate/first-item fallback) so the cell budget is respected on big cross-tabs.
    # Recompute the cell budget using the RESTRICTED time length.
    total_cells = 1
    for var in variables:
        n = len(wanted_time) if var.get("code") == time_code else max(len(var.get("values", [])), 1)
        total_cells *= n

    query_vars = []
    if total_cells <= MAX_CELLS:
        for var in variables:
            code = var.get("code", "")
            if code == time_code:
                query_vars.append({"code": code, "selection": {"filter": "item", "values": wanted_time}})
            else:
                vals = var.get("values", [])
                if vals:
                    query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
    else:
        for var in variables:
            code = var.get("code", "")
            vals = var.get("values", [])
            if not vals:
                continue
            if code == time_code:
                query_vars.append({"code": code, "selection": {"filter": "item", "values": wanted_time}})
            elif ((code == meta_time_code) if meta_time_code is not None
                  else is_time_dim(code, vals)):
                # Ingester keep-full PARITY (jobs/ingest_stat_latvia.query_table): over
                # MAX_CELLS the ingester keeps every variable its own is_time test flags
                # at the FULL value list — ONLY the flagged code when `time: true` is
                # present, else every is_time_dim() match — so the stored series_keys
                # cover exactly those values and the date-tail must select them
                # identically (full where the ingester kept full, aggregated where it
                # aggregated), or the tail would refresh a different key-slice than the
                # one on disk.
                query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
            else:
                agg = [v for v in vals if v.upper() in ("0", "000", "TOTAL", "TOT", "T", "ALL", "KOPA", "KOPAA")]
                selected = agg[:1] if agg else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not query_vars:
        return [], zero_status

    body = {"query": query_vars, "response": {"format": "json-stat2"}}
    resp = _post(sess, url, body)
    time.sleep(RATE)
    if resp is None:
        # 400/403: selection rejected/empty for this window — legitimately empty.
        return [], "empty"

    prefix = _table_prefix(db, path)
    # Thread the RESOLVED axis code as the parser's authoritative time_code (the same
    # pass-1 lock the ingester's own query_table passes). Left unthreaded, the imported
    # parser falls back to its NAME-FIRST scan and can key obs_date on a DIFFERENT axis
    # than the one tailed above (month+year cube: it name-matches the month axis, every
    # obs_date parses to None, 0 rows -> false structural / permanent freeze).
    rows = parse_jsonstat2(resp, prefix, time_code)
    # Keep only rows strictly relevant to the delta: parse_jsonstat2 already filters
    # to the codes we asked for, but guard the boundary explicitly (re-pulls the
    # boundary day for revision capture; merge dedups it).
    if boundary is not None:
        rows = [r for r in rows if r[1] >= boundary]

    if rows:
        return rows, "data"
    # A 200 that parsed 0 DATA rows. Distinguish two cases (mirrors statfin/stat_estonia):
    #   - The 200 envelope itself is genuinely empty (no `value` array): the cell we asked
    #     for (boundary period forward) is now null/revised-away — an honest quiet tail.
    #     A never-landed table (boundary None) that yields nothing is also honest empty
    #     (the ingester got the same nothing).
    #   - For a PREVIOUSLY-LANDED table (boundary set) the 200 carried a REAL, non-empty
    #     json-stat2 body (`value` populated, `id` present) yet parse_jsonstat2 reduced it
    #     to 0 rows. Because startPeriod is INCLUSIVE (we ask for the boundary day forward),
    #     a healthy active table MUST re-surface >=1 boundary observation; 0 parsed rows
    #     from a real body means a coding change the parser could not handle (e.g. a TIME
    #     period re-coding that parse_date now rejects) — a genuine structural break, NOT a
    #     quiet tail. Surface it so finalize() raises DefinitiveError for human attention.
    # Other structural breaks (200 with NO variables / NO detectable time dimension) are
    # already caught EARLIER and precisely via zero_status.
    if had_history and resp.get("value") and resp.get("id"):
        return [], "structural"
    return [], "empty"


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    # No isdir guard: catalog + parquet reads are blob-routed, so the local dir legitimately
    # does not exist on a CI runner under AQUEDUCT_BACKEND=r2 (ledger R36).

    # Load the ingester's crawled catalog BLOB-FIRST: in CI (backend=r2) read the cached
    # _catalog.json from R2 instead of re-crawling both Latvia databases live every run; fall
    # back to a fresh crawl only if the cache is absent everywhere (hagstofa/stat_estonia pattern).
    import json as _json
    tables = None
    _craw = blob.read_bytes(os.path.join(out_dir, "_catalog.json"))
    if _craw is not None:
        try:
            _t = _json.loads(_craw.decode("utf-8"))
            if isinstance(_t, list) and _t:
                tables = _t
        except ValueError:
            tables = None
    if tables is None:
        tables = crawl_catalog()      # cache absent -> fresh crawl (reads/writes local cache)
    if not tables:
        raise DefinitiveError("stat_latvia: catalog empty/unavailable")

    sess = requests.Session()
    tally = Tally()
    cursors: dict[str, str] = {}   # series_key (table prefix) -> max obs written/known
    maxd: dt.date | None = None
    total = 0

    # Group the catalog tables by their on-disk parquet (db_<first-segment>).
    from collections import defaultdict
    by_group: dict[str, list] = defaultdict(list)
    for t in tables:
        by_group[_group_filename(t["db"], t["path"])].append(t)

    # Allow the live-test to restrict to a subset of group files WITHOUT changing the
    # production code path (the orchestrator never sets this). See test note in report.
    only = os.environ.get("STAT_LATVIA_ONLY_GROUPS")
    only_set = set(only.split(",")) if only else None

    # BOUND BELOW THE ORCHESTRATOR'S 45-MINUTE CAP, AND ROTATE.
    # Measured cloud runs: 53.3 min median, 62.0 max — over the cap on every run, and the cap
    # landed 2026-08-01 (36130d02) after stat_latvia's last run. The merge is INSIDE this
    # loop so a kill truncates rather than discards, but `sorted(by_group)` is a FIXED order:
    # the kill lands in the same place every run and the tail groups are never reached at
    # all, however many runs pass (R190 — a bound over a fixed order is a truncation, not a
    # budget). Stopping at 30 min also returns time to the shared daily run, which attempted
    # only 20 of ~106 live cloud sources on 2026-08-02.
    budget_min = float(os.environ.get("STAT_LATVIA_BUDGET_MIN", "30"))
    dl = Deadline(minutes=budget_min)
    groups = rotate_after(sorted(by_group.keys()), load_rotation(out_dir))
    last_group = ""

    for fname in groups:
        if dl.spent():
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — stopped after group {last_group!r}, "
                  f"{len(groups) - groups.index(fname)} of {len(groups)} group(s) deferred "
                  f"to the next tick", flush=True)
            break
        last_group = fname
        path = os.path.join(out_dir, fname)
        before = blob.row_count(path)
        # Only maintain groups that already exist on disk (the ingester decided which
        # groups produced time-series; e.g. OSP_OD_tautassk produced none). A brand-new
        # group is out of scope for the date-tail updater.
        if before == 0 and not blob.exists(path):
            continue
        if only_set is not None and fname not in only_set:
            # Test-only subset: keep this group's existing rows in the running total so
            # the never-shrink/after>=before assertion is honest, but skip fetching it.
            total += before
            continue

        per_table_max = _per_table_max(path)
        # Seed cursors from the on-disk frontier so an untouched table still reports
        # its real freshness (a frozen table can't hide behind a group-level max).
        # SANITIZE the seeded value: a corrupt far-future obs_date (PxWeb sentinel) must
        # NOT be written into series_cursors / maxd, else max(cursors) in health.py reads
        # as "fresh" and masks RED-DATA. sane_since() returns None for such a date; we then
        # skip seeding a cursor for that table (its real freshness comes from any rows it
        # fetches this run via the full-pull fallback) and never let it poison maxd.
        for pref, d in per_table_max.items():
            if sane_since(d) is None:
                continue
            cursors[pref] = d.isoformat()
            if maxd is None or d > maxd:
                maxd = d

        grp_keys: list[str] = []
        grp_dates: list[dt.date] = []
        grp_vals: list[float] = []
        seen: set[tuple] = set()

        for t in by_group[fname]:
            pref = _table_prefix(t["db"], t["path"])
            boundary = per_table_max.get(pref)  # None => table not yet on disk (full backfill)
            try:
                rows, st = _query_table_delta(sess, t, boundary)
            except TransientError as e:
                # -> run becomes 'partial'; existing data untouched
                tally.transient_unit(f"{pref}: query failed — {str(e)[:110]}")
                continue
            except DefinitiveError as e:
                # A hard 4xx on one table shouldn't strand the rest; record structural.
                tally.structural_unit(f"{pref}: hard 4xx — {str(e)[:110]}")
                continue

            if st == "structural":
                tally.structural_unit(f"{pref}: parser reported a structural break")
                continue
            if st == "empty":
                tally.empty_unit(f"{pref}: no rows past the boundary")
                continue

            # st == "data": accumulate this table's NEW rows (dedup within the group).
            t_max: dt.date | None = None
            for key, d, v in rows:
                tok = (key, d)
                if tok in seen:
                    continue
                seen.add(tok)
                grp_keys.append(key)
                grp_dates.append(d)
                grp_vals.append(v)
                if t_max is None or d > t_max:
                    t_max = d
            # A table whose 200 POST returned and parsed REAL rows this run is a
            # data-bearing (added) sub-unit — count the rows that flowed (len(rows)),
            # NOT the post-dedup net delta (mirrors bcb.py added_unit(len(s_dates))).
            # On an idempotent quiet tick the boundary period re-returns rows that all
            # dedup away (net 0) within the group / at merge; counting that 0 as "empty"
            # would, on a healthy all-quiet run, drive empty==attempted and trip the
            # all-empty structural floor in finalize() -> false DefinitiveError. The true
            # net-new delta stays reflected in `total` (via merge) and in last_obs.
            tally.added_unit(len(rows))
            if t_max is not None:
                prev = cursors.get(pref)
                if prev is None or t_max.isoformat() > prev:
                    cursors[pref] = t_max.isoformat()
                if maxd is None or t_max > maxd:
                    maxd = t_max

        # Merge this group's accumulated new rows ONCE (never write parquet ourselves).
        if grp_vals:
            new_tbl = pa.table({
                "series_key": pa.array(grp_keys, pa.string()),
                "obs_date":   pa.array(grp_dates, pa.date32()),
                "value":      pa.array(grp_vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total += n
            # merge returns the max obs of the merged on-disk file, which may include a
            # pre-existing corrupt far-future sentinel from another table in this group.
            # Guard it via sane_since so a poisoned on-disk date can't be promoted into
            # maxd -> last_obs (which would read as "fresh" in health and mask staleness).
            if md and sane_since(md) is not None:
                md_d = dt.date.fromisoformat(md)
                if maxd is None or md_d > maxd:
                    maxd = md_d
        else:
            total += before

    last_obs = maxd.isoformat() if maxd is not None else (since or None)
    # Floor above the table count so a healthy "everything current" run is honest
    # no_change (not a false all-empty structural break); real breaks are per-table.
    # Bookmark after a complete pass too, so the wrap goes through this same path and no
    # branch can silently stop the rotation.
    if last_group:
        save_rotation(out_dir, last_group)

    floor = max(tally.attempted, 10) + 1
    return finalize(tally, total, last_obs, source=SOURCE,
                    series_cursors=cursors, empty_window_floor=floor)
