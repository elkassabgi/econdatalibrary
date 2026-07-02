"""S3 (sdmx_delta) fetcher — Statistics Sweden (SCB), PxWeb statistical database.

License CC BY 4.0, no API key. SCB is a PxWeb source (NOT SDMX 2.1), so the
"sdmx_delta" date tail is realised through PxWeb's time-dimension value selection:
for each table we POST a query that restricts the time variable to ONLY the codes
whose parsed date is newer than what is already stored, and merge the result in.

LAYOUT (set by jobs/ingest_scb.py and preserved here):
  ONE parquet per top-level SUBJECT area under clean_full/scb/<SUBJ>.parquet
  (AA, AM, BE, BO, EN, FM, HA, HE, JO ...), schema (series_key, obs_date, value):
    series_key = "<table/path with '/'→':'>:<dim>=<code>:...:ContentsCode=<cc>"
    obs_date   = date32 parsed from the PxWeb time code (2023, 2023M01, 2023K1, ...)
    value      = float64
  A SUBJECT file holds MANY tables; each table shares one time dimension, so the
  natural SUB-UNIT here is the TABLE, not the series. The table path is recoverable
  from a series_key as the leading colon-parts BEFORE the first "=" token, which is
  exactly the join key used by the ingester (path.replace("/", ":")).

INCREMENTAL (date tail), per TABLE:
  1. read the subject parquet once, group its rows by reconstructed table path, and
     learn that table's max(obs_date) and the set of time codes already on disk;
  2. GET the table metadata, identify the time variable, and select ONLY the time
     codes whose parse_date() is STRICTLY NEWER than the stored max (full pull for a
     table that has no on-disk history — a newly-added table is backfilled, never
     skipped). Non-time variables are selected exactly as the ingester does (all
     values when the cell budget allows, else the aggregate/first value);
  3. POST json-stat2, parse with the ingester's parser, keep only rows strictly
     newer than the stored max (defence against a server echoing the boundary), and
     accumulate per subject;
  4. merge ALL of a subject's new rows in ONE call to merge.merge_and_write(path,
     tbl, mode="merge", dedup_keys=("series_key","obs_date")) — never write parquet
     directly, never shrink.

HONEST STATUS (Tally + finalize): each TABLE is a sub-unit.
  added_unit(n)     rows merged for the table (n>0 new / n==0 nothing newer)
  empty_unit()      table already current (no time code newer than stored max) OR a
                    legitimately-empty incremental tail (200, real metadata, 0 new)
  transient_unit()  timeout / 5xx / 429 / network / non-JSON-200 on metadata or data
                    -> the WHOLE run returns 'partial' (orchestrator re-runs next tick)
  structural_unit() a previously-populated table whose metadata GET 200s but no
                    longer exposes a time dimension, OR whose incremental POST 200s
                    with a real envelope yet yields 0 parseable rows for time codes
                    that DO exist and ARE newer — a schema/structural break.
finalize() then raises DefinitiveError on any structural break or a large all-empty
window, returns 'partial' on any transient, else 'ok'/'no_change'. Existing data is
always preserved by merge (never shrink).

Only the SUBJECT parquet files already on disk are processed (each is its own unit
of merge); within each, EVERY table that maps to that subject is attempted. The
date tail keeps this cheap: a current table pulls 0 new periods.
"""
from __future__ import annotations
import datetime as dt
import os
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize

SOURCE = "scb"
BASE = "https://api.scb.se/OV0104/v1/doris/en/ssd"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = 0.2                  # api.scb.se ~10 req/s; 0.2s is a comfortable margin
MAX_CELLS = 100_000        # SCB hard limit 150K; 100K safe ceiling (matches ingester)
MAX_ATTEMPTS = 4
GET_TIMEOUT = 60
POST_TIMEOUT = 120


# --------------------------------------------------------------------------- #
# date / time parsing — copied verbatim from jobs/ingest_scb.py so the obs_date
# and series_key written here are byte-identical to the ingester's.
# --------------------------------------------------------------------------- #
def _parse_date(s: str) -> dt.date | None:
    """Parse a PxWeb time value (2023, 2023M01, 2023-01, 2023K1/Q1, 2023W01, 2023H1)."""
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})-(\d{2})$", s)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
    except (ValueError, TypeError):
        pass
    return None


_NAMED_TIME = ("tid", "time", "year", "period", "datum", "ar")


def _is_named_time(code: str) -> bool:
    return (code or "").lower() in _NAMED_TIME


def _is_time_dim(code: str, values: list[str]) -> bool:
    """Heuristic from the ingester: is this PxWeb variable the time dimension?"""
    if _is_named_time(code):
        return True
    if values:
        sample = values[:5]
        year_count = sum(1 for v in sample if re.match(r"^\d{4}[MQKHW]?\d*$", str(v).strip()))
        if year_count >= len(sample) * 0.6:
            return True
    return False


def _time_dim_index(dim_ids, candidates):
    """Pick the time dimension index, PREFERRING an explicitly named one (Tid/Time/...)
    over a coincidental numeric-code match.

    `candidates` maps each dim id -> (is_time_dim_bool, sample_values). The ingester's
    own parser picks the FIRST is_time_dim match, which mis-fires on SCB tables that
    have BOTH a real `Tid` AND a non-time variable whose codes look year-like (Kommun
    region codes 0114..2584, 8-digit ContentsCode). Preferring the named dimension
    selects the genuine time series in exactly those tables, leaving every other table
    (the vast majority, single time dim) identical to the ingester. Returns None when
    no dimension qualifies as time.
    """
    named = [i for i, did in enumerate(dim_ids)
             if candidates.get(did, (False, []))[0] and _is_named_time(did)]
    if named:
        return named[0]
    other = [i for i, did in enumerate(dim_ids)
             if candidates.get(did, (False, []))[0]]
    return other[0] if other else None


def _parse_jsonstat2(data: dict, table_path: str) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat2 into (series_key, date, value) — verbatim ingester logic."""
    results: list[tuple[str, dt.date, float]] = []
    try:
        dim_ids = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims = data.get("dimension", {})
        values = data.get("value", [])
        if not dim_ids or not values:
            return results

        candidates = {}
        for did in dim_ids:
            cat_idx = dims.get(did, {}).get("category", {}).get("index", {})
            if isinstance(cat_idx, dict):
                all_vals = list(cat_idx.keys())
            else:
                all_vals = list(cat_idx) if cat_idx else []
            candidates[did] = (_is_time_dim(did, all_vals), all_vals)
        # Prefer an explicitly NAMED time dim (Tid/Time/...) over a coincidental
        # year-like non-time variable — see _time_dim_index. This is the only deviation
        # from the ingester's first-match pick, and it only changes the result for the
        # handful of tables that carry both a real Tid and a year-like category code.
        time_dim_idx = _time_dim_index(dim_ids, candidates)
        if time_dim_idx is None:
            return results

        dim_codes: list[list[str]] = []
        for did in dim_ids:
            cat_idx = dims.get(did, {}).get("category", {}).get("index", {})
            if isinstance(cat_idx, dict):
                size = max(cat_idx.values()) + 1 if cat_idx else 0
                pos_to_code = [""] * size
                for code, pos in cat_idx.items():
                    if pos < size:
                        pos_to_code[pos] = code
            elif isinstance(cat_idx, list):
                pos_to_code = list(cat_idx)
            else:
                pos_to_code = []
            dim_codes.append(pos_to_code)

        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        prefix = table_path.replace("/", ":")

        for flat_idx, raw_v in enumerate(values):
            if raw_v is None:
                continue
            try:
                v = float(raw_v)
                if v != v:                       # NaN
                    continue
            except (ValueError, TypeError):
                continue

            remainder = flat_idx
            dim_indices = []
            for stride in strides:
                dim_indices.append(remainder // stride)
                remainder %= stride

            t_pos = dim_indices[time_dim_idx]
            t_codes = dim_codes[time_dim_idx]
            if t_pos >= len(t_codes):
                continue
            obs_date = _parse_date(t_codes[t_pos])
            if obs_date is None:
                continue

            key_parts = [prefix]
            for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
                if i == time_dim_idx:
                    continue
                codes_for_dim = dim_codes[i]
                code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
                key_parts.append(f"{did}={code_val}")
            results.append((":".join(key_parts), obs_date, v))
    except Exception:
        # A genuine parse blow-up on a real body is signalled to the caller as 0 rows;
        # the caller decides structural vs empty from the metadata it already has.
        return results
    return results


# --------------------------------------------------------------------------- #
# HTTP with honest transient/definitive classification
# --------------------------------------------------------------------------- #
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s


def _get_meta(sess: requests.Session, url: str):
    """GET table metadata.

    Returns (meta, gone):
      (dict, False) -> usable 200 metadata.
      (None, True)  -> HTTP 400/404: the table endpoint is ABSENT/MOVED (a retired or
                       relocated table path). Per the bcb/treasury references a 4xx on a
                       sub-unit endpoint is "legitimately empty for this run", NOT a
                       schema break of a LIVE endpoint — existing data is preserved on
                       disk and the table is simply skipped this tick.
      (None, False) -> a 200 whose body is not a usable metadata dict (structural).
    Raises TransientError on timeout/5xx/429/network/non-JSON-200 after the budget.
    """
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, timeout=GET_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"scb GET {url}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                js = r.json()
            except ValueError:
                last = "non-JSON 200"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"scb GET {url}: {last}")
                time.sleep(min(2 ** a, 20)); continue
            return (js, False) if isinstance(js, dict) else (None, False)
        if r.status_code in (400, 404):
            return None, True
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"scb GET {url}: {last}")
            time.sleep(30 if r.status_code == 429 else min(2 ** a, 20)); continue
        raise DefinitiveError(f"scb GET {url}: HTTP {r.status_code}")
    raise TransientError(f"scb GET {url}: {last}")


def _post_data(sess: requests.Session, url: str, body: dict):
    """POST a json-stat2 query. Returns (json|None, http400) where http400 marks a
    400 (oversized/invalid query — caller treats as empty for that table). Raises
    TransientError on timeout/5xx/429/network/non-JSON-200 after the budget."""
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, timeout=POST_TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"scb POST {url}: {last}")
            time.sleep(min(2 ** a, 20)); continue
        if r.status_code == 200:
            try:
                return r.json(), False
            except ValueError:
                last = "non-JSON 200"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"scb POST {url}: {last}")
                time.sleep(min(2 ** a, 20)); continue
        if r.status_code in (400, 404):
            return None, True
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"scb POST {url}: {last}")
            time.sleep(30 if r.status_code == 429 else min(2 ** a, 20)); continue
        raise DefinitiveError(f"scb POST {url}: HTTP {r.status_code}")
    raise TransientError(f"scb POST {url}: {last}")


# --------------------------------------------------------------------------- #
# parquet introspection: per-table frontier from a subject file
# --------------------------------------------------------------------------- #
def _table_path_of(series_key: str) -> str:
    """Reconstruct the table path from a series_key: the leading colon-parts BEFORE
    the first dimension token (the first part containing '='), joined back with '/'.
    This is the inverse of the ingester's prefix = table_path.replace('/', ':')."""
    parts = []
    for p in series_key.split(":"):
        if "=" in p:
            break
        parts.append(p)
    return "/".join(parts)


# A plausibility ceiling for on-disk obs_date. The ingester's _is_time_dim heuristic
# occasionally mis-classified a NON-time variable whose codes look like a year (e.g. a
# 4-digit category code "2584"), writing garbage far-future obs_dates such as
# 2584-12-31. Such a value must NOT become a table's incremental boundary (nothing is
# ever "newer" than 2584, so the table would freeze forever) nor be surfaced as the
# reported frontier. We ignore any on-disk date beyond (today.year + 2): real SCB data
# is labelled at most year-end of the current/next year, so this preserves every
# legitimate frontier while discarding the corrupt artifacts.
def _frontier_ceiling() -> dt.date:
    return dt.date(dt.date.today().year + 2, 12, 31)


def _table_frontiers(path: str) -> dict[str, dt.date]:
    """Per-table max(PLAUSIBLE obs_date) on disk for one subject parquet, keyed by
    table path. Implausible far-future dates (ingester time-dim mis-classification)
    are skipped so a corrupt value can't freeze a table or pollute the frontier.

    Computed in pyarrow: filter out corrupt dates, group max(obs_date) per series_key
    (cheap, vectorised), then fold the much-smaller distinct-key set into per-table max
    in Python — so a 6.8M-row subject (AM) doesn't pay a per-row Python loop."""
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    # read only the two columns we need (cheap projection on a wide/large subject file)
    t = pq.read_table(path, columns=["series_key", "obs_date"])
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    ceiling = _frontier_ceiling()
    # drop rows with null or corrupt-far-future obs_date before aggregating
    od = t.column("obs_date")
    mask = pc.and_(pc.is_valid(od), pc.less_equal(od, pa.scalar(ceiling, pa.date32())))
    t = t.filter(mask)
    if t.num_rows == 0:
        return out
    grouped = t.group_by("series_key").aggregate([("obs_date", "max")])
    g_keys = grouped.column("series_key").to_pylist()
    g_max = grouped.column("obs_date_max").to_pylist()
    for k, d in zip(g_keys, g_max):
        if d is None:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        tp = _table_path_of(k)
        prev = out.get(tp)
        if prev is None or d > prev:
            out[tp] = d
    return out


# --------------------------------------------------------------------------- #
# build the incremental PxWeb query for one table
# --------------------------------------------------------------------------- #
def _build_query(variables: list[dict], stored_max: dt.date | None):
    """Build (query_vars, time_code, has_time, new_time_codes) for a table.

    The time variable is restricted to ONLY codes strictly newer than stored_max
    (all codes when stored_max is None — a never-landed table is fully backfilled).
    Non-time variables follow the ingester: all values within the cell budget, else
    the aggregate/first value. Returns has_time=False when no time dim is present.
    """
    # locate the time variable — prefer an explicitly NAMED time dim (Tid/Time/...)
    # over a coincidental year-like non-time variable, matching the parser's pick so
    # the query restriction and the parse agree on the same dimension.
    time_candidates = [v for v in variables
                       if _is_time_dim(v.get("code", ""), v.get("values", []))]
    time_var = None
    for v in time_candidates:
        if _is_named_time(v.get("code", "")):
            time_var = v
            break
    if time_var is None and time_candidates:
        time_var = time_candidates[0]
    if time_var is None:
        return [], None, False, []

    all_time = time_var.get("values", []) or []
    if stored_max is None:
        new_time = list(all_time)
    else:
        new_time = [c for c in all_time
                    if (_parse_date(c) is not None and _parse_date(c) > stored_max)]
    if not new_time:
        return [], time_var.get("code"), True, []

    # cell budget over the SELECTED time slice (only new periods)
    total_cells = len(new_time)
    for v in variables:
        if v is time_var:
            continue
        total_cells *= max(len(v.get("values", [])), 1)

    query_vars = [{"code": time_var["code"],
                   "selection": {"filter": "item", "values": new_time}}]
    for v in variables:
        if v is time_var:
            continue
        vals = v.get("values", []) or []
        code = v.get("code", "")
        if not vals:
            continue
        if total_cells <= MAX_CELLS:
            sel = vals
        else:
            agg = [x for x in vals if str(x).upper() in
                   ("000", "TOTAL", "TOT", "T", "0", "ALL")]
            sel = agg[:1] if agg else vals[:1]
        query_vars.append({"code": code, "selection": {"filter": "item", "values": sel}})

    return query_vars, time_var["code"], True, new_time


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    if not os.path.isdir(out_dir):
        raise DefinitiveError(f"scb source dir missing: {out_dir}")

    # Load the table catalog the ingester crawled (table path/id/text). REUSE it
    # rather than re-discovering the tree.
    import json
    cat_file = os.path.join(out_dir, "_catalog.json")
    if not os.path.exists(cat_file):
        raise DefinitiveError(f"scb catalog missing: {cat_file}")
    try:
        catalog = json.load(open(cat_file, encoding="utf-8"))
    except (ValueError, OSError) as e:
        raise DefinitiveError(f"scb catalog unreadable: {e}")

    # Tables grouped by SUBJECT (first path component) = the on-disk parquet name.
    from collections import defaultdict
    by_subject: dict[str, list[dict]] = defaultdict(list)
    for t in catalog:
        subj = t["path"].split("/")[0] if t.get("path") else "root"
        by_subject[subj].append(t)

    # Only the subjects that ALREADY have a parquet are managed (each is a merge unit).
    subj_files = sorted(f[:-len(".parquet")] for f in os.listdir(out_dir)
                        if f.endswith(".parquet"))
    if not subj_files:
        raise DefinitiveError(f"no scb parquet files under {out_dir}")

    sess = _session()
    tally = Tally()
    total = 0
    maxd: dt.date | None = None
    cursors: dict[str, str] = {}          # table path -> max obs_date (per-table freshness)
    n_subunits = 0                        # total tables attempted (for empty_window_floor)
    ceiling = _frontier_ceiling()         # reject implausible far-future obs_dates

    for subj in subj_files:
        path = os.path.join(out_dir, f"{subj}.parquet")
        before = blob.row_count(path)
        frontiers = _table_frontiers(path)               # table path -> stored max date
        tables = by_subject.get(subj, [])
        if not tables:
            # Subject parquet exists but no catalog tables map to it — leave it alone,
            # keep its rows in the total.
            total += before
            continue

        new_keys: list[str] = []
        new_dates: list[dt.date] = []
        new_vals: list[float] = []

        for tinfo in tables:
            tpath = tinfo["path"]
            n_subunits += 1
            stored_max = frontiers.get(tpath)             # None => never landed -> backfill
            # seed cursor from the on-disk frontier so an untouched/current table still
            # reports its real freshness (a frozen table can't hide behind subject max).
            if stored_max is not None:
                cursors[tpath] = stored_max.isoformat()

            url = f"{BASE}/{tpath}/"
            try:
                meta, gone = _get_meta(sess, url)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            time.sleep(RATE)

            if gone:
                # HTTP 400/404: this table path is retired or moved upstream. The
                # endpoint is absent, not a LIVE schema break — treat as legitimately
                # empty (existing on-disk data preserved), matching bcb/treasury 4xx
                # handling, so one relocated table can't fail the whole source forever.
                tally.empty_unit()
                continue

            if not meta or not isinstance(meta, dict) or not meta.get("variables"):
                # A 200 that did NOT 4xx but returned no usable metadata/variables: on a
                # previously-populated table this is a schema/structural break; on a
                # never-landed table it is just unusable -> empty.
                if stored_max is not None:
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                continue

            variables = meta["variables"]
            query_vars, time_code, has_time, new_time_codes = _build_query(
                variables, stored_max)

            if not has_time:
                # metadata 200 but no time dimension on a previously-populated table
                # => structural break; on a never-landed table it's just unusable -> empty.
                if stored_max is not None:
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                continue

            if not new_time_codes:
                # table already current: no time code newer than stored max.
                tally.empty_unit()
                continue

            try:
                resp, http400 = _post_data(sess, url, {
                    "query": query_vars, "response": {"format": "json-stat2"}})
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            time.sleep(RATE)

            if http400 or not resp:
                # 400/404 on the POST (oversized/invalid query, or empty). Treat as a
                # legitimately-empty sub-unit for this run; existing data untouched.
                tally.empty_unit()
                continue

            rows = _parse_jsonstat2(resp, tpath)
            # keep only rows strictly newer than the stored frontier (guard against an
            # echoed boundary period or a parser that returned the whole slice), and
            # never accept an implausible far-future date (the same ingester time-dim
            # mis-classification that produced the 2584-12-31 artifacts) — so a corrupt
            # period can't pollute the merged data, the cursors, or the reported frontier.
            kept = 0
            tmax: dt.date | None = None
            for sk, d, v in rows:
                if d > ceiling:
                    continue
                if stored_max is not None and d <= stored_max:
                    continue
                new_keys.append(sk)
                new_dates.append(d)
                new_vals.append(v)
                kept += 1
                if tmax is None or d > tmax:
                    tmax = d

            if kept == 0:
                # 0 newer rows merged. Classify HONESTLY but conservatively:
                #
                #  * structural_unit (=> DefinitiveError) ONLY when the parser found a
                #    BONA FIDE time dimension and produced rows, yet NONE are newer than
                #    the stored max even though we explicitly requested codes that exist
                #    and parse to dates strictly after it. For a previously-populated
                #    table that is a genuine schema/structural anomaly.
                #
                #  * empty_unit otherwise. Crucially, _parse_jsonstat2 returning ZERO
                #    rows total (rows == []) is NOT a structural break — it is an
                #    UNPARSEABLE table: SCB has tables whose only "year-like" variable is
                #    actually a non-time category (Kommun codes 0114..2584, 8-digit
                #    ContentsCode) that the shared is_time_dim heuristic mis-classifies.
                #    The ingester ALSO fails these (they are on disk with garbage dates
                #    or never landed), so 0-rows-total is a known parse limitation, not a
                #    LIVE break of a healthy series. Flagging it structural would fail the
                #    whole SCB source on every tick. Existing (garbage) data is preserved
                #    by never-shrink; we simply do not make it worse.
                if stored_max is not None and rows:
                    tally.structural_unit()
                else:
                    tally.empty_unit()
                continue

            tally.added_unit(kept)        # rows flowed for this table (net-new vs merge counted later)
            if tmax is not None:
                cursors[tpath] = tmax.isoformat()
                if maxd is None or tmax > maxd:
                    maxd = tmax

        # Merge this subject's new rows in ONE atomic, never-shrink call.
        if new_vals:
            tbl = pa.table({
                "series_key": pa.array(new_keys, pa.string()),
                "obs_date":   pa.array(new_dates, pa.date32()),
                "value":      pa.array(new_vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
            total += n
            if md:
                md_d = dt.date.fromisoformat(md) if isinstance(md, str) else md
                # merge.max is over the WHOLE file, which may still hold pre-existing
                # corrupt far-future rows; clamp so the reported frontier stays sane.
                if md_d <= ceiling and (maxd is None or md_d > maxd):
                    maxd = md_d
        else:
            total += before

    # last_obs reports the REAL frontier even on a no_change run: when nothing newer was
    # fetched (maxd is None) fall back to the max of the per-table on-disk cursors so the
    # orchestrator persists a true frontier (a frozen source can't report None/last-since).
    if maxd is not None:
        last_obs = maxd.isoformat()
    elif cursors:
        last_obs = max(cursors.values())
    else:
        last_obs = since or None
    # all-empty structural floor.
    #
    # The contract nominal is <#subunits>-1, but — exactly as documented in treasury.py
    # — a perfectly healthy STEADY-STATE date-tail run of SCB is legitimately all-empty:
    # SCB is annual/quarterly-heavy, so on a given tick EVERY table commonly has "no
    # time code newer than the stored max" (current) and reports empty, which is genuine
    # no_change, NOT a wholesale outage. With the nominal floor that idempotent re-run
    # would false-raise DefinitiveError. Real structural breaks are already caught
    # PRECISELY per-table via tally.structural_unit() (a LIVE 200 whose time dimension
    # vanished, or a 200 data body with 0 parseable newer rows when newer codes were
    # explicitly requested). So we raise the floor above the attempted-table count, so
    # the blunt all-empty heuristic can never false-positive on a quiet steady state
    # while the precise per-table signal still turns a true break into a DefinitiveError.
    floor = n_subunits + 1
    return finalize(tally, total, last_obs, source=SOURCE,
                    series_cursors=cursors, empty_window_floor=floor)
