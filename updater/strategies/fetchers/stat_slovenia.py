"""S3 fetcher — Statistics Slovenia (SURS), PxWeb v1 (pxweb.stat.si SiStatData).

License CC BY 4.0, no API key. Despite the strategy name `sdmx_delta`, SURS exposes
a **PxWeb** API (not SDMX 2.1): per-table JSON metadata listing variables/values, and
a POST query that selects explicit value codes per dimension and returns JSON-stat2.
The S3 contract's "PxWeb branch" applies verbatim: POST the table query with the time
dimension restricted to the codes AFTER the stored max(obs_date).

Layout (set by jobs/ingest_stat_slovenia.py — reused verbatim here):
  ONE parquet per GROUP under clean_full/stat_slovenia/<grp>.parquet, where
  grp = first 3 chars of the .px table id (e.g. 0300220S.px -> "030"). A group file
  holds MANY tables. Schema is the simple 3-column shape:
      series_key : "SI:<table_id_no_px>:<DIM>=<code>:..."   (the .px suffix is stripped)
      obs_date   : date32  (year -> Dec-31, month -> 1st, quarter -> first month, ...)
      value      : float64
  (series_key, obs_date) is the unique key — dedup_keys for the merge.

Each CATALOG TABLE is a sub-unit. For every table we:
  1. read its group parquet to learn THIS table's max(obs_date) and the exact codes it
     already wrote (per-table, by splitting series_key on ':'),
  2. GET the table's PxWeb metadata (variables + value lists),
  3. detect the time dimension EXACTLY like the ingester (is_time_dim), parse every time
     code to a date, and keep ONLY codes whose date > stored max  (date-tail: tiny query),
  4. replicate the ingester's dimension selection — the all-values-vs-one-aggregate branch
     is decided on the FULL total_cells (NOT the date-restricted count) so the series_keys
     produced match the ones already on disk EXACTLY (no parallel keys),
  5. POST the query, parse JSON-stat2 with the SAME parser as the ingester, accumulate the
     rows under the table's group,
  6. merge each touched group file via merge.merge_and_write(..., mode="merge",
     dedup_keys=("series_key","obs_date")) — never write parquet directly, never shrink.

Honest status (Tally + finalize):
  - timeout / 5xx / 429 / network drop / bad-json after retries  -> transient_unit() -> run is 'partial'
  - 200 metadata whose `variables` block is gone/empty, OR a 200 POST that parsed 0 rows
    from a NON-trivial body on a FULL (no date restriction) fetch of a populated table
                                                                  -> structural_unit() -> DefinitiveError
  - first-seen NEW table (no on-disk history) that yields rows    -> added_unit(n)
  - table already current (no time codes newer than max), or a legitimately-quiet tail
                                                                  -> empty_unit()
empty_window_floor = (#sub-units) - 1, so only a TRULY total all-empty window trips the
wholesale-outage floor — a healthy monthly run where most of 4,696 tables have nothing new
is legitimate no_change, caught precisely per-table via structural_unit().
"""
from __future__ import annotations
import datetime as dt
import json
import os
import random
import re
import time
from collections import defaultdict

import pyarrow as pa
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Tally, finalize, sane_since

SOURCE = "stat_slovenia"
BASE = "https://pxweb.stat.si/SiStatData/api/v1/en/Data"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = 0.3
MAX_CELLS = 100_000          # same cap the ingester used to pick the dim-selection branch
TIMEOUT = 90
MAX_ATTEMPTS = 4

# Horizon for the REPORTED freshness watermark only. SURS publishes legitimate
# demographic projections out to ~2100; a handful of tables have a numeric *category*
# dimension (e.g. codes 1000/2000/.../6000) that the ingester's value-pattern
# `is_time_dim` heuristic mis-reads as a time dim, parsing 6000 -> year-6000-12-31.
# We REPRODUCE the ingester's parse verbatim on disk (so series_keys/dates match and
# merge dedups cleanly — never our place to silently rewrite history here), but the
# `last_obs_date`/cursor signals returned to the orchestrator are clamped past this
# horizon so a mis-parsed year-6000 row can't masquerade as the source's freshness.
_FRESH_HORIZON = dt.date(dt.date.today().year + 80, 12, 31)


def _out_dir() -> str:
    return config.source_dir(SOURCE)


# --------------------------------------------------------------------------- #
# date / time-dim helpers — copied verbatim from jobs/ingest_stat_slovenia.py
# so parsed obs_date and time-dim detection are byte-for-byte identical.
# --------------------------------------------------------------------------- #
def _parse_date(s: str):
    s = (s or "").strip()
    try:
        if re.match(r"^\d{4}$", s):
            return dt.date(int(s), 12, 31)
        m = re.match(r"^(\d{4})M(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), 1)
        m = re.match(r"^(\d{4})[QK](\d)$", s, re.IGNORECASE)
        if m:
            q = int(m.group(2))
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        m = re.match(r"^(\d{4})W(\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
    except (ValueError, TypeError):
        pass
    return None


def _is_time_dim(code: str, values: list) -> bool:
    code_l = (code or "").lower()
    if code_l in ("mesec", "leto", "kvartal", "year", "time", "period", "tid",
                  "month", "quarter", "half", "week"):
        return True
    if values:
        sample = values[:5]
        yr_count = sum(1 for v in sample if re.match(r"^\d{4}[MQKHW]?\d*$", str(v).strip()))
        return yr_count >= len(sample) * 0.6
    return False


def _parse_jsonstat2(data: dict, prefix: str):
    """JSON-stat2 -> [(series_key, obs_date, value)] — same logic as the ingester."""
    results = []
    try:
        dim_ids = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims = data.get("dimension", {})
        values = data.get("value", [])
        if not dim_ids or not values:
            return results

        time_dim_idx = None
        dim_codes = []
        for i, did in enumerate(dim_ids):
            cat = dims.get(did, {}).get("category", {})
            cat_idx = cat.get("index", {})
            if isinstance(cat_idx, dict):
                size = dim_sizes[i] if i < len(dim_sizes) else max(cat_idx.values(), default=-1) + 1
                pos_to_code = [""] * size
                for code, pos in cat_idx.items():
                    if pos < len(pos_to_code):
                        pos_to_code[pos] = code
            elif isinstance(cat_idx, list):
                pos_to_code = list(cat_idx)
            else:
                pos_to_code = []
            dim_codes.append(pos_to_code)
            if time_dim_idx is None and _is_time_dim(did, pos_to_code):
                time_dim_idx = i

        if time_dim_idx is None:
            return results

        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        for flat_idx, raw_v in enumerate(values):
            if raw_v is None:
                continue
            try:
                v = float(raw_v)
                if v != v:
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
        # A malformed envelope is handled by the caller as a structural signal
        # (200 + non-trivial body that parses to nothing); never silently swallowed.
        return results
    return results


# --------------------------------------------------------------------------- #
# HTTP with honest transient/definitive classification
# --------------------------------------------------------------------------- #
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s


def _get_meta(sess, table_id: str):
    """GET a table's PxWeb metadata.

    Returns the parsed dict on 200, or None on 400/404 (table retired / unavailable —
    legitimately empty). Raises TransientError on timeout/5xx/429/drop/bad-json after
    the retry budget.
    """
    url = f"{BASE}/{table_id}/"
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET meta {table_id}: {last}")
            time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"{SOURCE} GET meta {table_id}: {last}")
                time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
                continue
        if r.status_code in (400, 404):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if r.status_code == 429:
                time.sleep(30)
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} GET meta {table_id}: {last}")
            time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
            continue
        raise DefinitiveError(f"{SOURCE} GET meta {table_id}: HTTP {r.status_code}")
    raise TransientError(f"{SOURCE} GET meta {table_id}: {last}")


def _post_query(sess, table_id: str, body: dict):
    """POST a PxWeb query.

    Returns the parsed JSON-stat2 dict on 200, or None on 400/403 (selection invalid /
    nothing matched — legitimately empty). Raises TransientError on timeout/5xx/429/
    drop/bad-json after the retry budget.
    """
    url = f"{BASE}/{table_id}/"
    last = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} POST {table_id}: {last}")
            time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
            continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if attempt == MAX_ATTEMPTS - 1:
                    raise TransientError(f"{SOURCE} POST {table_id}: {last}")
                time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
                continue
        if r.status_code in (400, 403):
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if r.status_code == 429:
                time.sleep(30)
            if attempt == MAX_ATTEMPTS - 1:
                raise TransientError(f"{SOURCE} POST {table_id}: {last}")
            time.sleep(min(2 ** attempt, 20) + random.uniform(0, 0.5))
            continue
        raise DefinitiveError(f"{SOURCE} POST {table_id}: HTTP {r.status_code}")
    raise TransientError(f"{SOURCE} POST {table_id}: {last}")


# --------------------------------------------------------------------------- #
# catalog + per-table on-disk frontier
# --------------------------------------------------------------------------- #
def _load_catalog(out_dir: str):
    """Reuse the ingester's cached catalog (table list). If absent, fetch it from the
    flat PxWeb listing exactly as the ingester does."""
    cat_file = os.path.join(out_dir, "_catalog.json")
    if os.path.exists(cat_file):
        try:
            cat = json.load(open(cat_file, encoding="utf-8"))
            if isinstance(cat, list) and cat:
                return [t["id"] for t in cat if "id" in t]
        except (ValueError, OSError):
            pass
    # Fallback: re-discover the table list (same call the ingester makes).
    sess = _session()
    try:
        r = sess.get(f"{BASE}/", timeout=TIMEOUT)
    except (requests.Timeout, requests.ConnectionError) as e:
        raise TransientError(f"{SOURCE} catalog GET: {e}")
    if r.status_code != 200:
        raise TransientError(f"{SOURCE} catalog GET: HTTP {r.status_code}")
    try:
        data = r.json()
    except ValueError as e:
        raise TransientError(f"{SOURCE} catalog GET: bad json {e}")
    if not isinstance(data, list):
        raise DefinitiveError(f"{SOURCE} catalog: expected a list, got {type(data).__name__}")
    return [item["id"] for item in data if item.get("type") == "t" and "id" in item]


def _group_of(table_id: str) -> str:
    tid = table_id
    return tid[:3] if len(tid) >= 3 else tid[:1]


def _group_path(out_dir: str, grp: str) -> str:
    return os.path.join(out_dir, f"{grp}.parquet")


def _table_max_by_group(path: str) -> dict:
    """Read a group parquet ONCE and return {table_id: max(obs_date)} for every table
    embedded in it. table_id is parsed from series_key 'SI:<table_id>:...'."""
    out: dict = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names or "obs_date" not in t.column_names:
        return out
    sks = t.column("series_key").to_pylist()
    ods = t.column("obs_date").to_pylist()
    for sk, od in zip(sks, ods):
        if od is None or not sk:
            continue
        parts = sk.split(":")
        if len(parts) < 2:
            continue
        tid = parts[1]
        prev = out.get(tid)
        if prev is None or od > prev:
            out[tid] = od
    return out


# --------------------------------------------------------------------------- #
# query building (mirror ingester's dim selection, but date-tail the time dim)
# --------------------------------------------------------------------------- #
def _build_query(variables: list, new_time_codes: list):
    """Build the PxWeb query var list, replicating jobs/ingest_stat_slovenia.query_table.

    The all-values-vs-one-aggregate branch is decided on the FULL total_cells (the
    product over ALL value counts), NOT the date-restricted count, so the dimension
    selection — and therefore the produced series_keys — match what is already on disk.
    The time dimension is then restricted to `new_time_codes` only (date-tail).
    """
    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)

    query_vars = []
    if total_cells <= MAX_CELLS:
        for var in variables:
            vals = var.get("values", [])
            code = var.get("code", "")
            if not vals:
                continue
            if _is_time_dim(code, vals):
                query_vars.append({"code": code, "selection": {"filter": "item", "values": new_time_codes}})
            else:
                query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
    else:
        for var in variables:
            vals = var.get("values", [])
            code = var.get("code", "")
            if not vals:
                continue
            if _is_time_dim(code, vals):
                query_vars.append({"code": code, "selection": {"filter": "item", "values": new_time_codes}})
            else:
                agg = [v for v in vals if v.upper() in ("0", "000", "TOTAL", "TOT", "T", "ALL", "SKUPAJ")]
                selected = agg[:1] if agg else vals[:1]
                query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})
    return query_vars


def _time_var(variables: list):
    """Return (code, values) of the table's time dimension, or (None, None)."""
    for var in variables:
        vals = var.get("values", [])
        code = var.get("code", "")
        if _is_time_dim(code, vals):
            return code, vals
    return None, None


def _sane(d):
    """A date fit to report as a freshness watermark (excludes mis-parsed category
    codes that land beyond any plausible projection horizon). Data on disk is untouched;
    this only filters the SIGNAL returned to the orchestrator."""
    return d is not None and d <= _FRESH_HORIZON


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = _out_dir()
    os.makedirs(out_dir, exist_ok=True)

    table_ids = _load_catalog(out_dir)
    if not table_ids:
        raise DefinitiveError(f"{SOURCE}: empty table catalog")

    # Optional bounded subset for the one-shot live test only (production passes none).
    limit = None
    only = None
    if isinstance(unit.config, dict):
        limit = unit.config.get("_test_limit")
        only = unit.config.get("_test_groups")  # iterable of group prefixes
    if only:
        only = set(only)
        table_ids = [tid for tid in table_ids if _group_of(tid) in only]
    if limit:
        table_ids = table_ids[:int(limit)]

    # Group tables so each group parquet is read once and merged once.
    by_group: dict = defaultdict(list)
    for tid in table_ids:
        by_group[_group_of(tid)].append(tid)

    sess = _session()
    tally = Tally()
    cursors: dict = {}        # table_id -> max obs_date written/known (ISO str)
    total_rows = 0
    overall_max = None
    # Tables whose METADATA returned 200 (table alive) but that are simply current —
    # no time codes newer than the stored frontier, so no fetch was needed. These are
    # SUCCESSFUL, confirmed-fresh sub-units; counting them as empty_unit() would let a
    # perfectly healthy steady-state monthly run (where most of 4,696 tables have nothing
    # newer) trip the all-empty wholesale-outage floor. We therefore tally them as
    # added_unit(0) (attempted+success, NOT empty) so only genuine 404/no-metadata and
    # structural-empty results feed the outage floor — the same resolution treasury/bcb use.
    current = 0

    for grp in sorted(by_group.keys()):
        path = _group_path(out_dir, grp)
        before = blob.row_count(path)
        total_rows += before
        tbl_max = _table_max_by_group(path)   # one read per group file

        # seed cursors from the on-disk frontier so untouched tables still report real
        # freshness (clamped past the projection horizon — see _FRESH_HORIZON).
        for tid in by_group[grp]:
            tid_clean = tid.replace(".px", "")
            md = tbl_max.get(tid_clean)
            if _sane(md):
                cursors[tid_clean] = md.isoformat()
                if overall_max is None or md > overall_max:
                    overall_max = md

        grp_keys: list = []
        grp_dates: list = []
        grp_vals: list = []
        seen: set = set()

        for tid in by_group[grp]:
            tid_clean = tid.replace(".px", "")
            try:
                meta = _get_meta(sess, tid)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            time.sleep(RATE)

            if meta is None:
                # 400/404 — table retired / unavailable. Legitimately empty for this run.
                tally.empty_unit()
                continue
            if not isinstance(meta, dict) or "variables" not in meta or not meta.get("variables"):
                # 200 but the expected PxWeb metadata structure is gone -> structural break.
                tally.structural_unit()
                continue

            variables = meta["variables"]
            tcode, tvals = _time_var(variables)
            if not tcode or not tvals:
                # Metadata 200 (table alive) but no detectable time dimension -> this table
                # contributes no obs_date series (the ingester's parser yields nothing for
                # it either). Alive-but-not-a-timeseries: a confirmed sub-unit, not an
                # outage-feeding empty.
                current += 1
                continue

            # Codes whose value actually PARSES to a date under the ingester's parser.
            # The SURS catalog holds tables whose detected "time" dimension is in fact a
            # numeric *category* dim (e.g. 6-digit product codes 010000/011000) that the
            # shared is_time_dim value-heuristic mis-reads; those parse to NO dates and the
            # ingester likewise wrote nothing for them (hence they have no on-disk history).
            parseable = [c for c in tvals if _parse_date(c) is not None]

            stored_max = tbl_max.get(tid_clean)   # date or None (new table)
            if not parseable:
                # The detected time dim yields no real dates -> a known-unparseable table
                # under the shared heuristic (the ingester skipped it too). Alive but not
                # writable here: a confirmed sub-unit, NOT a structural break and NOT an
                # outage-feeding empty. (If it somehow had on-disk history yet now parses to
                # nothing, that IS a break -> structural.)
                if stored_max is not None:
                    tally.structural_unit()
                else:
                    current += 1
                continue

            # Guard the FETCH boundary against a CORRUPT far-future stored_max. SURS has
            # numeric *category* dims (codes like 1000/.../6000) that the shared is_time_dim
            # heuristic mis-reads as time, parsing 6000 -> year-6000-12-31. If such a sentinel
            # is the on-disk max for a table, a raw `> stored_max` delta selects NOTHING and
            # the table would freeze FOREVER (every code <= year-6000). sane_since() returns
            # None for a stored_max more than ~400d past today; in that case we fetch the FULL
            # parseable set (trailing/full re-pull) instead of a delta. The reported cursor is
            # still clamped to _FRESH_HORIZON via _sane(), so this never re-surfaces the
            # sentinel as freshness — it just keeps the table from going stale.
            # Use this source's documented projection horizon (_FRESH_HORIZON, today+80yr)
            # as the corruption threshold, NOT the generic 400d default: SURS publishes
            # legitimate projections to ~2100, which must still delta-fetch; only a sentinel
            # beyond any plausible horizon (year-6000) trips the full-re-pull fallback. This
            # keeps the FETCH boundary consistent with the _sane()/_FRESH_HORIZON cursor clamp.
            _horizon_days = (_FRESH_HORIZON - dt.date.today()).days
            safe_max = sane_since(stored_max, max_future_days=_horizon_days)
            if stored_max is None or safe_max is None:
                # First-seen table (no on-disk history), OR a corrupt far-future stored_max:
                # backfill ALL parseable time codes rather than a frozen `> stored_max` delta.
                new_codes = list(parseable)
            else:
                new_codes = [c for c in parseable if _parse_date(c) > safe_max]

            if not new_codes:
                # Metadata 200 (table alive) and already current through the stored
                # frontier — a SUCCESSFUL, confirmed-fresh sub-unit. Not counted as empty
                # (would false-trip the outage floor on a healthy steady-state run); not
                # an attempt-failure either. Tracked separately for honest accounting.
                current += 1
                continue

            query_vars = _build_query(variables, new_codes)
            if not query_vars:
                tally.empty_unit()
                continue

            body = {"query": query_vars, "response": {"format": "json-stat2"}}
            try:
                resp = _post_query(sess, tid, body)
            except TransientError:
                tally.transient_unit()
                time.sleep(RATE)
                continue
            time.sleep(RATE)

            if resp is None:
                # 400/403 — the value selection was rejected / matched nothing. The table
                # metadata was 200 (alive); this is a legitimately-empty selection window,
                # not an outage. Confirmed sub-unit, not an outage-feeding empty.
                current += 1
                continue

            prefix = f"SI:{tid_clean}"
            rows = _parse_jsonstat2(resp, prefix)

            if not rows:
                # 200 POST but parsed 0 rows even though the requested time codes WERE
                # parseable dates. Distinguish a structural break from a quiet window:
                #   * an ESTABLISHED table (has on-disk history) whose response carries a
                #     real, non-empty value array yet yields nothing parseable -> the data
                #     shape changed under us -> structural break.
                #   * an empty value array (the new period simply has no data published
                #     yet), or a FIRST-SEEN table that the body yields nothing usable for
                #     (the ingester would write nothing either) -> alive & confirmed, not
                #     an outage-feeding empty, not a break.
                has_body = bool(resp.get("value")) if isinstance(resp, dict) else False
                if stored_max is not None and has_body:
                    tally.structural_unit()
                else:
                    current += 1
                continue

            n_added = 0
            tmax = None      # sane max for the reported cursor (data on disk keeps ALL rows)
            for key, d, v in rows:
                tok = (key, d)
                if tok in seen:
                    continue
                seen.add(tok)
                grp_keys.append(key)
                grp_dates.append(d)
                grp_vals.append(v)
                n_added += 1
                if _sane(d) and (tmax is None or d > tmax):
                    tmax = d
            tally.added_unit(n_added)
            if tmax is not None:
                prev = cursors.get(tid_clean)
                if prev is None or tmax.isoformat() > prev:
                    cursors[tid_clean] = tmax.isoformat()
                if overall_max is None or tmax > overall_max:
                    overall_max = tmax

        # Merge this group's accumulated new rows (if any) into its parquet.
        if grp_vals:
            new_tbl = pa.table({
                "series_key": pa.array(grp_keys, pa.string()),
                "obs_date":   pa.array(grp_dates, pa.date32()),
                "value":      pa.array(grp_vals, pa.float64()),
            })
            n, md = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total_rows = total_rows - before + n   # replace the pre-count with the merged count
            if md:
                md_d = dt.date.fromisoformat(md)
                # merge's max can be a mis-parsed category code (year-6000); only let a
                # sane date advance the REPORTED watermark (on-disk rows are all kept).
                if _sane(md_d) and (overall_max is None or md_d > overall_max):
                    overall_max = md_d

    last_obs = overall_max.isoformat() if overall_max else (since or None)
    # empty_window_floor = (#sub-units) - 1 per the S3 contract, where #sub-units is the
    # TOTAL of all tables processed this run (added + empty[404] + structural + transient +
    # confirmed-current). The all-empty floor in finalize() trips only when added==0 AND
    # every tally-attempted sub-unit is empty AND attempted > floor — i.e. a true wholesale
    # 404 outage of essentially all tables. Because confirmed-current (alive-200) tables are
    # NOT in tally.empty, a healthy steady-state run (most tables current, nothing new) has
    # empty << attempted and correctly returns no_change, while a real outage (catalog
    # vanishes / host moved -> every table 404) still trips the floor. structural breaks are
    # caught precisely per-table via structural_unit().
    total_subunits = tally.attempted + current
    return finalize(tally, total_rows, last_obs, source=SOURCE, series_cursors=cursors,
                    empty_window_floor=max(total_subunits - 1, 1))
