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
  3. resolve THE time dimension with the shared value-first resolver (core/pxweb.py:
     authoritative `time: true` code, else highest date-parse-rate, else literal name —
     the SAME axis _parse_jsonstat2 keys obs_date on), parse every time code to a date,
     and keep ONLY codes whose date > stored max  (date-tail: tiny query),
  4. replicate the ingester's dimension selection — the all-values-vs-one-aggregate branch
     is decided on the FULL total_cells (NOT the date-restricted count) so the series_keys
     produced match the ones already on disk EXACTLY (no parallel keys); ONLY the resolved
     time axis is restricted to the new codes (the OLD builder restricted EVERY time-ish
     variable, so a month+year cube had BOTH axes filtered to "newer" period codes — the
     month selection was emptied of valid codes and the tail silently froze),
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
from ._common import Deadline, Tally, finalize, sane_since

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

SOURCE = "stat_slovenia"
BASE = "https://pxweb.stat.si/SiStatData/api/v1/en/Data"


def _body_has_data(resp) -> bool:
    """A json-stat2 body carries DATA only if some value is non-null.

    SURS pre-lists the NEXT period in a table's time codelist before publishing any
    data for it: a boundary tail for that period returns a real json-stat2 structure
    whose `value` array is ALL NULLS (2221405S reproduced live 2026-08-06: LETO
    carries '2024', the POST returns 36 values, every one null). `bool(vals)` is True
    for a list of nulls, so that unpopulated forward period was classified a
    STRUCTURAL break every sweep. All-null is the same publisher condition as an
    empty array — nothing published yet — wearing a different JSON shape."""
    vals = resp.get("value") if isinstance(resp, dict) else None
    return bool(vals) and any(v is not None for v in vals)
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
# Where the rotating sweep offset lives. Beside the DATA, not in the state store, because it has
# to survive the interruption that state-writing does not: finalize() never runs on a killed unit.
_SWEEP_FILE = "_sweep_offset.json"
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
# date / time-dim helpers — parse grammar and key assembly copied verbatim from
# jobs/ingest_stat_slovenia.py so parsed obs_date and series_keys are byte-for-byte
# identical. THE time axis is picked by the shared value-first resolver
# (core/pxweb.py) — same precedence in the parser and the delta-query builder.
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
    """The fetcher's LEGACY per-variable time-ish test (name list, else a loose
    4-digit value regex). No longer used to pick the axis the date-tail restricts —
    that is _time_var_index's job (shared value-first resolver) — but kept
    byte-identical for the over-MAX_CELLS keep-full PARITY branch of _build_query:
    the ingester keeps every variable its own is_time test flags at the FULL value
    list (the stored series_keys cover those values, e.g. a demoted month axis of a
    month+year cube), so the date-tail must keep selecting them identically."""
    code_l = (code or "").lower()
    if code_l in ("mesec", "leto", "kvartal", "year", "time", "period", "tid",
                  "month", "quarter", "half", "week"):
        return True
    if values:
        sample = values[:5]
        yr_count = sum(1 for v in sample if re.match(r"^\d{4}[MQKHW]?\d*$", str(v).strip()))
        return yr_count >= len(sample) * 0.6
    return False


def _parse_jsonstat2(data: dict, prefix: str, meta_time_code: str | None = None):
    """JSON-stat2 -> [(series_key, obs_date, value)] — decoding, date grammar and key
    assembly identical to the ingester's parser. THE time axis is picked by the shared
    value-first resolver (core/pxweb.py): authoritative `time: true` code / role.time,
    else highest date-parse-rate, else literal name. Value-first stops a month axis
    (codes that parse to no date) from outranking the year axis — the name-first
    defect that froze the PxWeb family — and it is the SAME resolution _build_query
    tails, so the restricted axis is exactly the axis keyed here. _parse_date keeps
    this source's exact grammar so working tables stay byte-identical."""
    results = []
    try:
        dim_ids = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims = data.get("dimension", {})
        values = data.get("value", [])
        if not dim_ids or not values:
            return results

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

        time_dim_idx = _pxweb.resolve_time_dim(
            dim_ids, dim_codes, meta_time_code=meta_time_code,
            role_time=_pxweb.role_time_of(data), parse_fn=_parse_date)

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
def _time_var_index(variables: list):
    """Index of THE time variable, resolved exactly as _parse_jsonstat2 keys
    obs_date: the shared value-first resolver (core/pxweb.py) fed the same
    authoritative `time: true` code and _parse_date grammar the parser uses —
    authoritative flag first, else highest date-parse-rate, else literal name.
    SURS is CODE-coded (the category codes ARE the period strings, '2024M01'),
    so the metadata `values` are scored directly. The OLD selection took the
    FIRST _is_time_dim match in variable order, which in a month+year cube
    picked whichever time-ish axis came first — e.g. a MESEC axis whose codes
    parse to no date — so the tail froze while the parser keyed the year axis.
    Returns None when no axis carries dates at all (the parser writes nothing
    for such a cube either)."""
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    return _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=_parse_date)


def _build_query(variables: list, new_time_codes: list):
    """Build the PxWeb query var list, replicating jobs/ingest_stat_slovenia.query_table.

    The all-values-vs-one-aggregate branch is decided on the FULL total_cells (the
    product over ALL value counts), NOT the date-restricted count, so the dimension
    selection — and therefore the produced series_keys — match what is already on disk.

    ONLY the resolved time axis (_time_var_index — the axis _parse_jsonstat2 keys
    obs_date on) is restricted to `new_time_codes` (date-tail); every OTHER variable
    is requested per the ingester's normal rule: full when total_cells <= MAX_CELLS;
    over the cap, keep-full for variables the legacy _is_time_dim flags (ingester
    PARITY — the stored series_keys cover every value of e.g. a demoted month axis)
    and the single aggregate/first code for the rest. The OLD builder restricted
    EVERY _is_time_dim variable to new_time_codes, so in a month+year cube BOTH axes
    were filtered to "newer" period codes — the month axis's selection was emptied
    of valid codes, the POST degenerated, and the tail silently froze.

    Returns [] when no time axis resolves (nothing is date-tailable): the caller
    records the legitimately-quiet verdict without a doomed POST — the same fringe
    handling bfs uses for a stored table with no resolvable axis."""
    time_idx = _time_var_index(variables)
    if time_idx is None:
        return []

    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)
    small = total_cells <= MAX_CELLS

    query_vars = []
    for vi, var in enumerate(variables):
        vals = var.get("values", [])
        code = var.get("code", "")
        if not vals:
            continue
        if vi == time_idx:
            query_vars.append({"code": code, "selection": {"filter": "item", "values": new_time_codes}})
        elif small:
            query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
        elif _is_time_dim(code, vals):
            # Ingester keep-full PARITY (jobs/ingest_stat_slovenia.query_table): over
            # MAX_CELLS the ingester keeps every variable its own is_time test flags
            # at the FULL value list — e.g. the demoted month axis of a month+year
            # cube — so the stored series_keys cover all its values; collapsing it
            # here would tail only a sliver of those stored series.
            query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
        else:
            agg = [v for v in vals if v.upper() in ("0", "000", "TOTAL", "TOT", "T", "ALL", "SKUPAJ")]
            selected = agg[:1] if agg else vals[:1]
            query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})
    return query_vars


def _time_var(variables: list):
    """Return (code, values) of THE table's time dimension — the axis
    _time_var_index resolves (the same one _build_query date-tails and
    _parse_jsonstat2 keys obs_date on) — or (None, None) when the cube has
    no resolvable date axis."""
    idx = _time_var_index(variables)
    if idx is None:
        return None, None
    var = variables[idx]
    return var.get("code", ""), var.get("values", [])


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

    # OBSERVABILITY FIRST. This fetcher had ZERO print statements, and it has spent the full
    # 45-minute unit cap on every tick since 2026-07-22 while merging nothing — 144 store files
    # untouched for ten days. A source that runs three quarters of an hour and says nothing can
    # only be guessed at, so the loop now reports where the time goes. That matters here
    # specifically because a group merges only AFTER all of its tables are fetched: a group
    # bigger than the remaining budget commits nothing at all, which is consistent with every
    # symptom observed.
    _t0 = time.time()
    _groups = sorted(by_group.keys())

    # ROTATE THE STARTING POINT so the tail is not permanently unreachable.
    #
    # A full sweep is 145 groups / 4,696 tables and MEASURED 3,916s = 65.3 minutes on the
    # workstation, which is faster than CI; the unit cap is 45 minutes and CI measured exactly
    # 2,700s, i.e. killed. Group order was fixed and sorted, so every run died around the same
    # 60% mark and the SAME later groups were never visited — and they are the biggest (group 39
    # alone has 279 tables). Letting one pass run to completion modified 71 of 146 group files
    # and created 2 more, so that tail was holding real, unfetched updates.
    #
    # Starting where the last run stopped makes the whole list reachable inside the existing
    # budget: two ticks cover all 145 groups. It is safe precisely because this fetcher commits
    # per group and re-seeds its cursors from the ON-DISK frontier rather than from saved state,
    # so a partial sweep leaves nothing inconsistent behind.
    #
    # The offset lives beside the data, not in the state store, because it must survive the
    # interruption that state-writing does not: finalize() never runs when the unit is killed.
    # Written after EVERY group for the same reason.
    _cur = os.path.join(out_dir, _SWEEP_FILE)
    _start = 0
    try:
        _start = int(json.loads(blob.read_bytes(_cur) or b"{}").get("next_group", 0))
    except Exception:                                          # noqa: BLE001
        _start = 0
    if not 0 <= _start < len(_groups):
        _start = 0
    if _start:
        _groups = _groups[_start:] + _groups[:_start]

    print(f"[{SOURCE}] {len(_groups)} group(s), {sum(len(v) for v in by_group.values()):,} "
          f"table(s) to consider; starting at offset {_start} "
          f"({_groups[0] if _groups else '-'})", flush=True)

    # YIELD BEFORE THE CAP KILLS US. The rotation above already makes the tail reachable, and
    # it survives a kill by design (the offset is written after every group, beside the data,
    # because finalize() never runs when the unit is interrupted). What it does NOT survive is
    # the STATUS: cloud run 2026-08-01 was `transient_fail` at exactly 45.0 min — "exceeded its
    # 45-minute hard limit and was interrupted" — and a killed unit records no success, so this
    # source can never set last_success_utc and is invisible to the SLA gate no matter how well
    # the sweep is actually going (R231).
    #
    # Stopping at 40 min instead of being killed at 45 costs one group and buys an honest
    # finalize: real status, real cursors, and the same offset write that already happens.
    budget_min = float(os.environ.get("STAT_SLOVENIA_BUDGET_MIN", "40"))
    _dl = Deadline(minutes=budget_min)

    for _gi, grp in enumerate(_groups, 1):
        if _dl.spent():
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{_dl.elapsed_min():.1f} min — stopping cleanly after {_gi - 1} of "
                  f"{len(_groups)} group(s); the sweep offset is already saved, so the next "
                  f"tick resumes here instead of being killed and reported as a failure",
                  flush=True)
            break
        path = _group_path(out_dir, grp)
        before = blob.row_count(path)
        total_rows += before
        tbl_max = _table_max_by_group(path)   # one read per group file
        print(f"[{SOURCE}] group {_gi}/{len(_groups)} (offset {_start}) {grp}: "
              f"{len(by_group[grp]):,} table(s), {before:,} row(s) on disk, "
              f"{time.time()-_t0:,.0f}s elapsed", flush=True)

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
            except TransientError as e:
                # NAMED, like the structural paths this file's own regression gate covers.
                # Distinguished from the query POST below: different endpoint, different fault.
                tally.transient_unit(f"{tid}: metadata GET failed — {str(e)[:150]}")
                time.sleep(RATE)
                continue
            time.sleep(RATE)

            if meta is None:
                # 400/404 — table retired / unavailable. Legitimately empty for this run.
                tally.empty_unit()
                continue
            if not isinstance(meta, dict) or "variables" not in meta or not meta.get("variables"):
                # 200 but the expected PxWeb metadata structure is gone -> structural break.
                # NAMED, like hagstofa's: "1/85 sub-unit(s) ... parsed 0 rows" with no table id
                # is a break you cannot act on. The three structural sites below are three
                # DIFFERENT failures, so each says which one it was.
                tally.structural_unit(f"{tid_clean}: metadata envelope gone")
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

            # Codes whose value actually PARSES to a date under the ingester's grammar.
            # The value-first resolver picks a date-parsing axis whenever one exists, so
            # an empty `parseable` here can only mean an authoritative `time: true` axis
            # (or a last-resort name-matched one) whose codes parse to no date — a cube
            # the parser writes nothing for either.
            parseable = [c for c in tvals if _parse_date(c) is not None]

            stored_max = tbl_max.get(tid_clean)   # date or None (new table)
            if not parseable:
                # The resolved time dim yields no real dates -> a date-less table under
                # the shared resolver (the parser writes nothing for it either). Alive but
                # not writable here: a confirmed sub-unit, NOT a structural break and NOT
                # an outage-feeding empty. (If it somehow had on-disk history yet now
                # parses to nothing, that IS a break -> structural.)
                if stored_max is not None:
                    tally.structural_unit(f"{tid_clean}: time axis parses to no dates")
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
            except TransientError as e:
                tally.transient_unit(f"{tid}: data POST failed — {str(e)[:150]}")
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
            # Thread the AUTHORITATIVE PxWeb `time: true` flag so the parser's shared
            # resolver locks onto the same axis the query tailed; None -> value-first.
            meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
            rows = _parse_jsonstat2(resp, prefix, meta_time_code)

            if not rows:
                # FIRST: is this even a time series? SURS publishes census CROSS-TABULATIONS
                # (05W: "Families, census 2002 by SETTLEMENT" — 6,152 settlement codes, no time
                # axis on ANY dimension). Those legitimately yield 0 rows, and they have on-disk
                # history plus a real value array, so the structural test below fires on them
                # every single run and holds the whole source at `partial` forever. That is
                # exactly what made ons_uk unable to succeed once in its history: 287 of its 337
                # "datasets" were Cantabular cross-tabs, and `empty` is not a transient state.
                #
                # Asked via _time_var_index, the SAME shared resolver _parse_jsonstat2 keys on,
                # so the two can never disagree about whether an axis exists (R333). A table
                # with no date-bearing axis is not a failure and not a break — it is not a time
                # series, and the ingester writes nothing for it either.
                if _time_var_index(variables) is None:
                    current += 1
                    continue
                # 200 POST but parsed 0 rows even though the requested time codes WERE
                # parseable dates. Distinguish a structural break from a quiet window:
                #   * an ESTABLISHED table (has on-disk history) whose response carries a
                #     real, non-empty value array yet yields nothing parseable -> the data
                #     shape changed under us -> structural break.
                #   * an empty value array (the new period simply has no data published
                #     yet), or a FIRST-SEEN table that the body yields nothing usable for
                #     (the ingester would write nothing either) -> alive & confirmed, not
                #     an outage-feeding empty, not a break.
                if stored_max is not None and _body_has_data(resp):
                    tally.structural_unit(f"{tid_clean}: non-empty body parsed 0 rows")
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

        # Advance the sweep offset AFTER each group, not at the end of the run: the whole point
        # is to survive being killed mid-sweep, and an offset written only on a clean finish
        # would never be written at all on the runs that need it.
        try:
            blob.write_bytes_atomic(
                _cur, json.dumps({"next_group": (_start + _gi) % len(_groups)}).encode())
        except Exception:                                      # noqa: BLE001
            pass    # a lost offset costs one repeated sweep, never correctness

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
