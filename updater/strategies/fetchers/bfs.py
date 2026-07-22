"""S3 (sdmx_delta) fetcher — Swiss Federal Statistical Office (BFS/OFS), PxWeb. No key.

Layout (set by jobs/ingest_bfs.py): a SINGLE parquet under clean_full/bfs/bfs.parquet
with schema (series_key, obs_date, value).  series_key is the composite the ingester
wrote: "BFS:{dbid}:{varcode}={valcode}:{varcode}={valcode}:..." (every non-time
dimension, in declaration order), obs_date = the parsed time period (annual codes ->
Dec-31, monthly "YYYYMnn" -> 1st, quarterly "YYYYQn"/"YYYYKn" -> quarter-start, ...).

SUB-UNIT = one PxWeb table (dbid).  BFS exposes ~654 tables via the catalog
  GET /api/v1/en/                                      -> [{dbid, text}, ...]
and serves table metadata + data from the GERMAN endpoint (the EN endpoint 400s on
individual tables):
  GET  /api/v1/de/{dbid}/{dbid}.px/                    -> {title, variables:[...]}
  POST /api/v1/de/{dbid}/{dbid}.px/  {query, response} -> JSON-stat2

DATE-TAIL (PxWeb has no ?startPeriod; the time DIMENSION is restricted in the POST
query instead).  For each dbid we read the existing parquet's max(obs_date) for that
dbid's series_key prefix, then build the query EXACTLY as the ingester did — the
selection branch (all-cells vs aggregate-only) is decided on the FULL cell count so
the produced series_key matches the on-disk key byte-for-byte — but the TIME variable
is restricted to only the period codes whose parsed date is >= the stored max (the
boundary period is re-requested so an in-place revision of the latest value is caught;
merge dedups the overlap).  A table whose time variable has NO code >= our stored max
is legitimately quiet (upstream hasn't published a newer period) -> empty, no request.

REUSE: catalog enumeration, metadata/data endpoints, MAX_CELLS branch, is_time_dim,
parse_date and parse_jsonstat2_bfs are taken verbatim from jobs/ingest_bfs.py (imported
by file path) so this fetcher never re-discovers structure or re-implements the parser.

MERGE: all new rows across all dbids are accumulated and published with ONE
merge.merge_and_write(path, tbl, mode="merge", dedup_keys=("series_key","obs_date")) —
atomic, dedup (new wins on revision), never-shrink.  The fetcher never writes parquet
itself.

HONEST STATUS (Tally + finalize):
  per dbid (sub-unit) we record on a Tally:
    added_unit(n)     rows merged / a 200 that parsed rows for a new period
    empty_unit()      no time code >= stored max (quiet), or a new-table full fetch
                      that legitimately returned no usable rows / 400 too-large after
                      backing off, or a 400/404 (table gone from data endpoint)
    transient_unit()  timeout / 5xx / 429 / network on the meta GET or data POST
    structural_unit() a 200 whose JSON-stat2 envelope is present but parses 0 rows
                      from a non-trivial selection (schema/structural break)
  finalize() -> 'ok'/'no_change' only when nothing transient/structural-failed;
  'partial' on any transient (orchestrator does NOT stamp success; unit re-runs);
  DefinitiveError on a structural break or a large all-empty window.

series_cursors: {dbid: 'YYYY-MM-DD'} seeded from the on-disk frontier so a frozen
table reports its real cursor and can't hide behind the unit-level max.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
BASE_EN = "https://www.pxweb.bfs.admin.ch/api/v1/en"   # table listing
BASE_DE = "https://www.pxweb.bfs.admin.ch/api/v1/de"   # metadata + data queries
SOURCE = "bfs"
DEDUP = ("series_key", "obs_date")
MAX_CELLS = 100_000
RATE = 0.3
MAX_ATTEMPTS = 4
TIMEOUT = 90


# --------------------------------------------------------------------------- #
# Reuse the ingester's structure/parse helpers verbatim (import by file path so
# we never re-discover or re-implement them). jobs/ingest_bfs.py runs main() only
# under __main__, so importing it is side-effect free.
# --------------------------------------------------------------------------- #
def _load_ingester():
    path = os.path.join(config.JOBS_DIR, "ingest_bfs.py")
    if not os.path.exists(path):
        raise DefinitiveError(f"bfs ingester missing: {path}")
    spec = importlib.util.spec_from_file_location("_ingest_bfs", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_ING = _load_ingester()
is_time_dim = _ING.is_time_dim
parse_date = _ING.parse_date
parse_jsonstat2_bfs = _ING.parse_jsonstat2_bfs


# --------------------------------------------------------------------------- #
# HTTP with transient/definitive discipline
# --------------------------------------------------------------------------- #
def _get_json(sess, url):
    """GET JSON. 200 -> obj; 400/404 -> None (table absent on this endpoint);
    timeout/5xx/429/drop/bad-json after the retry budget -> TransientError."""
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, headers=UA, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"bfs GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"bfs GET {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"bfs GET {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"bfs GET {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"bfs GET {url[-60:]}: {last}")


_TOO_LARGE = object()  # 403 from PxWeb = query exceeded the server cell cap


def _post_json(sess, url, body):
    """POST JSON-stat2 query. 200 -> obj; 403 -> _TOO_LARGE (query too big — the
    ingester's aggregate branch should prevent this; treat as legitimately skippable);
    400/404 -> None; timeout/5xx/429/drop/bad-json after the budget -> TransientError."""
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, headers=UA, timeout=TIMEOUT + 30)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"bfs POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"bfs POST {url[-60:]}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 403:
            return _TOO_LARGE
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"bfs POST {url[-60:]}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"bfs POST {url[-60:]}: HTTP {r.status_code}")
    raise TransientError(f"bfs POST {url[-60:]}: {last}")


# --------------------------------------------------------------------------- #
# Query construction — identical selection branch to the ingester, but with the
# TIME variable restricted to periods >= the stored max for this dbid.
# --------------------------------------------------------------------------- #
def _is_time_var(var) -> bool:
    """Match the ingester's notion of a time variable (PxWeb time flag, or a known
    time code name, or first valueText that looks like a bare year)."""
    code = var.get("code", "")
    vt = var.get("valueTexts", var.get("values", []))
    return bool(var.get("time")) or is_time_dim(code) or bool(
        vt and re.match(r"^\d{4}$", str(vt[0]).strip()))


def _build_query(variables, since_max: dt.date | None):
    """Reproduce the ingester's per-variable selection on the FULL cell count, then
    restrict the time variable to codes whose parsed date is >= since_max (boundary
    re-fetched for revisions). Returns (query_vars, n_new_time_codes).

    n_new_time_codes is the count of selected time codes after restriction; 0 means
    upstream has no period >= our stored max -> legitimately quiet (skip the POST)."""
    total_cells = 1
    for v in variables:
        total_cells *= max(len(v.get("values", [])), 1)

    query_vars = []
    n_time = None
    for var in variables:
        vals = var.get("values", [])
        if not vals:
            continue
        code = var.get("code", "")
        timeflag = _is_time_var(var)

        if total_cells <= MAX_CELLS:
            selected = list(vals)
        else:
            if timeflag:
                selected = list(vals)
            else:
                agg = [x for x in vals if str(x).lower() in ("tot", "total", "0", "all", "t")]
                selected = agg[:1] if agg else vals[:1]

        if timeflag:
            if since_max is not None:
                kept = []
                for code_val in selected:
                    d = parse_date(str(code_val))
                    # Fall back to valueText if the code itself doesn't parse.
                    if d is None:
                        vt = var.get("valueTexts", [])
                        # align valueText by position with this variable's full values
                        try:
                            idx = vals.index(code_val)
                            if idx < len(vt):
                                d = parse_date(str(vt[idx]))
                        except ValueError:
                            d = None
                    if d is not None and d >= since_max:
                        kept.append(code_val)
                selected = kept
            n_time = len(selected)

        query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    # If there is no detectable time variable, n_time stays None — we then treat the
    # whole table as a full (re)fetch (n_time -> count of any selected, or 1 sentinel).
    if n_time is None:
        n_time = 1 if query_vars else 0
    return query_vars, n_time


def _dbid_max(path: str) -> dict[str, dt.date]:
    """Per-dbid max obs_date, parsed from series_key prefix 'BFS:{dbid}:...'."""
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    ks = t.column("series_key").to_pylist()
    ds = t.column("obs_date").to_pylist()
    for k, d in zip(ks, ds):
        if d is None or ":" not in k:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        db = k.split(":")[1]
        prev = out.get(db)
        if prev is None or d > prev:
            out[db] = d
    return out


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "bfs.parquet")
    before = blob.row_count(path)

    sess = requests.Session()

    # Catalog of all tables (sub-units). A transient catalog failure aborts the run
    # transiently (we cannot enumerate sub-units) rather than laundering to no_change.
    try:
        catalog = _get_json(sess, f"{BASE_EN}/")
    except TransientError as e:
        return Result(status="partial", obs=before, last_obs_date=None,
                      new_vintage="date-tail",
                      error=f"bfs catalog transient-failed: {e}; will retry")
    if not isinstance(catalog, list) or not catalog:
        raise DefinitiveError(
            f"bfs catalog GET {BASE_EN}/ returned no table list (structural break)")

    dbid_max = _dbid_max(path)

    tally = Tally()
    cursors: dict[str, str] = {db: d.isoformat() for db, d in dbid_max.items()}

    keys: list[str] = []
    dates: list[dt.date] = []
    vals: list[float] = []

    for item in catalog:
        dbid = item.get("dbid", "")
        if not dbid:
            continue
        url = f"{BASE_DE}/{dbid}/{dbid}.px/"
        since_max = dbid_max.get(dbid)  # None => table not yet on disk -> full fetch

        # 1) metadata
        try:
            meta = _get_json(sess, url)
        except TransientError:
            tally.transient_unit()  # -> partial; keep going so one flaky table can't strand the rest
            time.sleep(RATE)
            continue
        if not isinstance(meta, dict) or not meta.get("variables"):
            # 400/404 on the data endpoint, or empty metadata. A table we already have
            # data for that has vanished from the data endpoint is a per-table structural
            # break; a never-seen catalog id with no metadata is legitimately empty.
            if since_max is not None and before > 0:
                tally.structural_unit()
            else:
                tally.empty_unit()
            time.sleep(RATE)
            continue
        variables = meta["variables"]

        # 2) build the date-tail query (time restricted to >= stored max)
        query_vars, n_time = _build_query(variables, since_max)
        if not query_vars:
            tally.empty_unit()
            time.sleep(RATE)
            continue
        if since_max is not None and n_time == 0:
            # upstream has no period >= our stored max -> legitimately quiet, no request
            tally.empty_unit()
            time.sleep(RATE)
            continue

        # 3) data POST
        body = {"query": query_vars, "response": {"format": "json-stat2"}}
        try:
            resp = _post_json(sess, url, body)
        except TransientError:
            tally.transient_unit()
            time.sleep(RATE)
            continue
        if resp is _TOO_LARGE or resp is None:
            # 403 (query too big — should be rare since aggregate branch caps cells) or
            # 400/404 on the query: treat as legitimately empty for this run, existing
            # data untouched. Not structural: the table still answers metadata.
            tally.empty_unit()
            time.sleep(RATE)
            continue

        meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
        rows = parse_jsonstat2_bfs(resp, f"BFS:{dbid}", meta_time_code)
        if not rows:
            # 200 with a real JSON-stat2 envelope but 0 parsed rows from a non-trivial
            # selection -> schema/structural break; an envelope with no value array is
            # an empty answer (quiet tail). Distinguish via the response body.
            has_envelope = isinstance(resp, dict) and bool(resp.get("id")) and bool(resp.get("value"))
            if has_envelope:
                tally.structural_unit()
            else:
                tally.empty_unit()
            time.sleep(RATE)
            continue

        n_added_for_dbid = 0
        dbmax = since_max
        for k, d, v in rows:
            keys.append(k); dates.append(d); vals.append(v)
            n_added_for_dbid += 1
            if dbmax is None or d > dbmax:
                dbmax = d
        if dbmax is not None:
            cursors[dbid] = dbmax.isoformat()
        # A 200 that returned parseable rows is a SUCCESSFUL sub-unit even if the merge
        # nets zero new rows (every row at/below the boundary, idempotent re-run); mark
        # added so a healthy steady-state re-run doesn't feed the all-empty floor.
        tally.added_unit(n_added_for_dbid)
        time.sleep(RATE)

    floor = len(catalog) + 1  # like treasury: steady-state can have ALL tables quiet

    if not vals:
        # Nothing new fetched. finalize() decides honest status from the Tally.
        last_obs = max(cursors.values()) if cursors else (since or None)
        return finalize(tally, before, last_obs, source=SOURCE,
                        series_cursors=cursors, empty_window_floor=floor)

    new_tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date":   pa.array(dates, pa.date32()),
        "value":      pa.array(vals, pa.float64()),
    })
    n, maxd = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
    last_obs = maxd or (max(cursors.values()) if cursors else None)
    return finalize(tally, n, last_obs, source=SOURCE,
                    series_cursors=cursors, empty_window_floor=floor)
