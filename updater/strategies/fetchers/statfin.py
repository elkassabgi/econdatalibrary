"""S3 (sdmx_delta) fetcher — Statistics Finland (StatFin), PxWeb API. No key.

Family: PxWeb (JSON-stat2). Statistics Finland's StatFin database is exposed as a
PxWeb tree of ~1,500 tables under
    https://pxdata.stat.fi/PxWeb/api/v1/en/StatFin/<subject>/<table>.px
Each table is a multi-dimensional cube with exactly ONE time variable
(metadata flags it as {"time": true}); its value codes are period strings
(annual "YYYY", monthly "YYYYMmm", quarterly "YYYYQq"/"YYYYKk", ...).

STORAGE (set by jobs/ingest_statfin.py, REUSED verbatim here): ONE parquet per
SUBJECT AREA (the first PxWeb path component) at clean_full/statfin/<subject>.parquet
with the 3-column schema (series_key, obs_date, value):
  series_key : "<subject>:<table_id>:<dim_code>=<value>:...:contentscode=<m>"
               (== table_path.replace('/', ':') + sorted-by-source dim assignments;
                produced by parse_jsonstat2 below, copied from the ingester so the
                key is byte-identical and merge dedups against the existing rows)
  obs_date   : date32 parsed from the table's time period code (parse_date below)
  value      : float64 observation
A subject parquet aggregates EVERY table under that subject; the catalog
(_catalog.json, written by the ingester's crawl) maps table -> subject.

DATE-TAIL DELTA (the S3 contract): for each table we
  1. read the table's max(obs_date) ALREADY on disk (derived per-table from the
     existing series_key prefix "<subject>:<table_id>" of the subject parquet),
  2. GET the table metadata, and on the time variable select ONLY period codes whose
     parse_date(code) >= that stored max (re-fetching the boundary period so an
     in-place revision of the latest value is captured; merge dedups the overlap).
     For non-time dimensions we replicate the ingester's MAX_CELLS selection EXACTLY
     (full item lists when small; the SSS/000/TOTAL aggregate + first value when the
     cross-product is huge) so the reconstructed series_key matches what is on disk.
  3. POST the json-stat2 query, parse with the ingester's parser, and merge the new
     rows into the subject parquet via merge.merge_and_write (never write parquet
     ourselves, never shrink). PxWeb REJECTS an empty time selection (HTTP 400), so a
     table that is already current (no period >= stored max would be... impossible,
     since the boundary period itself is always selected) always has >=1 selected
     value; a table that is genuinely up to date simply re-returns its boundary period
     and nets zero new rows after dedup.

HONEST STATUS (Tally + finalize): each TABLE is a sub-unit.
  added_unit(n)     a 200 json-stat2 body that parsed real rows (data flowed) — even
                    if every row is <= the boundary and merge nets 0 new (a healthy
                    idempotent re-run), so a quiet steady state never trips the floor.
  empty_unit()      a table legitimately current/empty (e.g. a tiny non-time cube, or
                    a 400/404 on an individual table whose subject still has data).
  transient_unit()  timeout / 5xx / 429 / network drop  -> whole run becomes 'partial'.
  structural_unit() a 200 with the EXPECTED PxWeb structure gone (no variables / no
                    time variable / 0 rows parsed from a non-trivial 200 body on a
                    previously-populated table) -> DefinitiveError via finalize.
Existing data is always preserved by merge (never shrink). A transient sub-failure
makes the WHOLE run 'partial' (never silent no_change); a structural break raises
DefinitiveError.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time
from collections import defaultdict, deque

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import (Deadline, Tally, finalize, load_rotation, retry_after_seconds,
                      rotate_after, sane_since, save_rotation)

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

SOURCE = "statfin"
BASE = "https://pxdata.stat.fi/PxWeb/api/v1/en/StatFin"
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")

def _backoff(resp, attempt: int) -> float:
    """Seconds to wait after a throttled/5xx response.

    On 429 the SERVER states how long to wait; obeying it is what turns a throttle into a
    recovery instead of an escalating block. Every other status keeps the plain
    exponential backoff. statfin's crawl failed with "statfin crawl ymtu: HTTP 429" after
    exhausting retries that each slept on a guess rather than the stated window
    (CI run 30150817644).
    """
    if getattr(resp, "status_code", None) == 429:
        return retry_after_seconds(resp, default=min(2 ** attempt, 30))
    return min(2 ** attempt, 30)

RATE = 0.3                 # polite delay between table requests
MAX_CELLS = 100_000        # MUST match jobs/ingest_statfin.py so dim selection is identical
TIMEOUT = 90
MAX_ATTEMPTS = 5
# Aggregate codes the ingester collapses huge non-time dimensions to (verbatim).
AGG_CODES = ("SSS", "000", "TOTAL", "TOT", "T", "0", "ALL", "1KP")

# A PxWeb path/id segment is alphanumerics plus _-. and "px" suffixes; nothing that
# could escape the source dir (no separators, no "." / ".." traversal, no drive char).
_SAFE_SEG = re.compile(r"[A-Za-z0-9_.-]+")


def _safe_segment(seg: str) -> bool:
    """True only if `seg` is a single, non-traversing PxWeb path component. The Parquet
    output filename is built from an upstream catalog field; this rejects any value that
    could write outside the source dir ('/', '\\', '..', drive letters, empty)."""
    seg = (seg or "").strip()
    if not seg or seg in (".", ".."):
        return False
    return bool(_SAFE_SEG.fullmatch(seg))


def _contained(path: str, base: str) -> bool:
    """Defense-in-depth: confirm `path` resolves inside `base` before any write. The shared
    publish path (merge.merge_and_write -> blob.write_table_atomic) does NOT enforce this,
    so we assert containment here rather than weaken a shared guard."""
    try:
        real = os.path.realpath(path)
        broot = os.path.realpath(base)
        return os.path.commonpath([real, broot]) == broot
    except (ValueError, OSError):
        return False


# Mis-parsed numeric *category* codes (and PxWeb time-dim heuristic misfires) can land an
# obs_date centuries in the future (year 9999/6000/2584 sentinels). On-disk rows are kept
# verbatim (never our place to rewrite history), but a far-future value must never (a) be
# used as a `>= since_date` delta boundary (that selects NOTHING and freezes the table
# forever) nor (b) be reported as the source's freshness watermark / written into a cursor
# (that would mask a frozen series from health.py's max(cursors)).
_FRESH_HORIZON = dt.date(dt.date.today().year + 80, 12, 31)


def _sane(d: dt.date | None) -> bool:
    """True if `d` is fit to report as a freshness watermark / cursor (not a far-future
    sentinel). Filters only the SIGNAL returned to the orchestrator; data on disk is kept."""
    return d is not None and d <= _FRESH_HORIZON


# --------------------------------------------------------------------------- #
# date / time helpers (copied verbatim from jobs/ingest_statfin.py)
# --------------------------------------------------------------------------- #
def parse_date(s: str) -> dt.date | None:
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


# Fallback time-dimension names (copied verbatim from jobs/ingest_statfin.py — the
# authoritative signal is the metadata `time: true` flag; these back it up).
TIME_CODES = (
    "vuosi", "tid", "time", "year", "kuukausi", "kk", "period", "quarter",
    "neljännes", "neljannes", "kvartal", "kausi", "viikko", "week", "month",
    "maaned", "datum", "aika", "timeperiod_y", "timeperiod_m", "timeperiod_q",
)


def is_time_dim(code: str, values: list[str]) -> bool:
    """The INGESTER's per-variable time test (copied verbatim from
    jobs/ingest_statfin.py). No longer used to pick the tailed axis — that is
    resolve_time_dim's job in _build_query — but still needed there for
    ingester keep-full PARITY: over MAX_CELLS the ingester keeps every variable
    this test flags at its FULL value list, so the stored series_keys cover it
    and the date-tail must select it identically."""
    if str(code).strip().lower() in TIME_CODES:
        return True
    if values:
        sample = [str(v).strip() for v in values[:8]]
        cur = dt.date.today().year
        sane = sum(1 for v in sample
                   if (d := parse_date(v)) is not None and 1900 <= d.year <= cur + 2)
        if sample and sane >= max(1, int(len(sample) * 0.6)):
            return True
    return False


def parse_jsonstat2(data: dict, table_path: str, meta_time_code: str | None = None) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat2 -> (series_key, date, value). Copied from the ingester so the
    series_key is byte-identical to what is already stored (merge dedup relies on it)."""
    results: list[tuple[str, dt.date, float]] = []
    try:
        dim_ids = data.get("id", [])
        dim_sizes = data.get("size", [])
        dims = data.get("dimension", {})
        values = data.get("value", [])
        if not dim_ids or not values:
            return results

        dim_codes: list[list[str]] = []
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

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py):
        # authoritative `time: true` / role.time, else highest date-parse-rate, else name.
        # Value-first stops a month axis (codes '0'..'11') from outranking a year axis —
        # the name-first defect that froze hagstofa/statfin (MISTAKES R19/R22). parse_date
        # keeps statfin's exact grammar so working tables stay byte-identical.
        time_dim_idx = _pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=meta_time_code, role_time=_pxweb.role_time_of(data), parse_fn=parse_date)

        if time_dim_idx is None:
            return results

        strides = [1] * len(dim_sizes)
        for i in range(len(dim_sizes) - 2, -1, -1):
            strides[i] = strides[i + 1] * dim_sizes[i + 1]

        prefix = table_path.replace("/", ":")

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
            obs_date = parse_date(t_codes[t_pos])
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
        # A genuinely malformed body is signalled to the caller as "no rows"; the caller
        # decides structural-vs-empty from the envelope (variables/time present).
        return []
    return results


# --------------------------------------------------------------------------- #
# HTTP with the Transient/Definitive contract
# --------------------------------------------------------------------------- #
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(UA)
    return s


def _get_meta(sess, path):
    """GET a table's metadata. Returns the dict, or None for a 400/404 (table gone —
    legitimately empty for that ONE table). Transient (timeout/5xx/429/drop/bad-json)
    -> TransientError after the retry budget."""
    url = f"{BASE}/{path}/"
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.get(url, timeout=TIMEOUT)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"statfin GET meta {path}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"statfin GET meta {path}: {last}")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (400, 404):
            return None  # table/path no longer available — empty for this one table
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"statfin GET meta {path}: {last}")
            time.sleep(_backoff(r, a)); continue
        raise DefinitiveError(f"statfin GET meta {path}: HTTP {r.status_code}")
    raise TransientError(f"statfin GET meta {path}: {last}")


# Sentinel: the data POST returned 400 because the (restricted) query selected an
# empty/invalid cross-section — treated as "no data for this tail", not transient.
_NO_DATA = object()


def _post_data(sess, path, body):
    """POST a json-stat2 query. Returns the dict, or _NO_DATA for a 400/403 (the
    restricted selection matched nothing / forbidden), or None for a 404. Transient
    -> TransientError after the retry budget."""
    url = f"{BASE}/{path}/"
    last = None
    for a in range(MAX_ATTEMPTS):
        try:
            r = sess.post(url, json=body, timeout=120)
        except (requests.Timeout, requests.ConnectionError) as e:
            last = str(e)[:120]
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"statfin POST {path}: {last}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                return r.json()
            except ValueError as e:
                last = f"bad json: {e}"
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"statfin POST {path}: {last}")
                time.sleep(min(2 ** a, 30)); continue
        if r.status_code in (400, 403):
            return _NO_DATA
        if r.status_code == 404:
            return None
        if r.status_code in (429, 500, 502, 503, 504):
            last = f"HTTP {r.status_code}"
            if a == MAX_ATTEMPTS - 1:
                raise TransientError(f"statfin POST {path}: {last}")
            time.sleep(_backoff(r, a)); continue
        raise DefinitiveError(f"statfin POST {path}: HTTP {r.status_code}")
    raise TransientError(f"statfin POST {path}: {last}")


# --------------------------------------------------------------------------- #
# catalog + on-disk per-table frontier
# --------------------------------------------------------------------------- #
def _crawl_catalog(sess) -> list[dict]:
    """BFS crawl of the StatFin PxWeb tree -> [{path,id,text}]. Mirrors the ingester's
    crawl so NEW tables are discovered. Cached _catalog.json (written by the ingester)
    is reused when present so a routine run is cheap; a fresh crawl is only the
    first-time / cache-deleted path."""
    out_dir = config.source_dir(SOURCE)
    cache = os.path.join(out_dir, "_catalog.json")
    if os.path.exists(cache):
        try:
            with open(cache, encoding="utf-8") as f:
                tables = json.load(f)
            if isinstance(tables, list) and tables:
                return tables
        except (ValueError, OSError):
            pass

    tables: list[dict] = []
    queue: deque[str] = deque([""])
    while queue:
        path = queue.popleft()
        url = f"{BASE}/{path}" if path else BASE
        last = None
        data = None
        for a in range(MAX_ATTEMPTS):
            try:
                r = sess.get(url, timeout=TIMEOUT)
            except (requests.Timeout, requests.ConnectionError) as e:
                last = str(e)[:120]
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"statfin crawl {path}: {last}")
                time.sleep(min(2 ** a, 30)); continue
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    data = None
                break
            if r.status_code in (400, 404):
                data = None; break
            if r.status_code in (429, 500, 502, 503, 504):
                if a == MAX_ATTEMPTS - 1:
                    raise TransientError(f"statfin crawl {path}: HTTP {r.status_code}")
                time.sleep(_backoff(r, a)); continue
            raise DefinitiveError(f"statfin crawl {path}: HTTP {r.status_code}")
        time.sleep(RATE)
        if not isinstance(data, list):
            continue
        for item in data:
            item_id = item.get("id", "")
            item_type = item.get("type", "l")
            # The id becomes a filesystem path component (subject -> '<subject>.parquet').
            # Reject anything that isn't a single safe segment so a malformed / poisoned
            # catalog can never steer a write outside the source dir.
            if not _safe_segment(item_id):
                continue
            child = f"{path}/{item_id}".lstrip("/")
            if item_type == "t":
                tables.append({"path": child, "id": item_id, "text": item.get("text", "")})
            elif item_type == "l":
                queue.append(child)
    if tables:
        try:
            os.makedirs(out_dir, exist_ok=True)
            with open(cache, "w", encoding="utf-8") as f:
                json.dump(tables, f)
        except OSError:
            pass
    return tables


def _table_frontier(out_dir: str, subject: str) -> dict[str, dt.date]:
    """Per-table max(obs_date) for one subject parquet, keyed by table prefix
    '<subject>:<table_id>'. Parsed from the existing series_key, which the ingester
    wrote as '<subject>:<table_id>:<dim>=...'. Empty dict if no parquet yet."""
    path = os.path.join(out_dir, f"{subject}.parquet")
    out: dict[str, dt.date] = {}
    if not blob.exists(path):
        return out
    t = blob.read_table(path)
    if t.num_rows == 0 or "series_key" not in t.column_names:
        return out
    keys = t.column("series_key").to_pylist()
    dates = t.column("obs_date").to_pylist()
    for k, d in zip(keys, dates):
        if d is None or not k:
            continue
        if isinstance(d, dt.datetime):
            d = d.date()
        parts = k.split(":")
        if len(parts) < 2:
            continue
        pref = f"{parts[0]}:{parts[1]}"
        cur = out.get(pref)
        if cur is None or d > cur:
            out[pref] = d
    return out


# --------------------------------------------------------------------------- #
# per-table query (date-tail) — REUSES the ingester's selection logic
# --------------------------------------------------------------------------- #
def _build_query(variables: list[dict], since_date: dt.date | None):
    """Build the json-stat2 query body restricting THE time dimension to codes
    >= since_date (re-fetch the boundary). Non-time dims replicate the ingester's
    MAX_CELLS rule so the reconstructed series_key matches the stored data.

    THE time dimension is picked with the shared value-first resolver
    (core/pxweb.py) fed the SAME inputs parse_jsonstat2 resolves with — the
    authoritative `time: true` code, else highest date-parse-rate, else literal
    name, using this source's parse_date grammar — so the axis restricted to
    ">= since_date" is EXACTLY the axis the parser will key obs_date on. The OLD
    per-variable test (`var.time OR is_time_dim`) could mark BOTH a month axis
    (codes '00'..'12', named "kuukausi") AND the year axis as time: the month
    codes parse to no date, so its ">= since" filter matched nothing and fell
    back to a single arbitrary month code — permanently excluding every other
    month's series from the tail while the run reported a healthy quiet state (a
    silent freeze, and an axis mismatch with the parser).

    Returns (body, time_var_present). If a time var exists but NO code is >= since_date
    the body is None (nothing to ask — PxWeb 400s on an empty selection)."""
    total_cells = 1
    for var in variables:
        total_cells *= max(len(var.get("values", [])), 1)
    small = total_cells <= MAX_CELLS

    # THE time axis, exactly as parse_jsonstat2 will pick it on the response
    # (same authoritative flag, same parse_date grammar, same precedence).
    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    time_idx = _pxweb.resolve_time_dim(
        [v.get("code", "") for v in variables],
        [[str(c) for c in (v.get("values") or [])] for v in variables],
        meta_time_code=meta_time_code, parse_fn=parse_date)

    query_vars = []
    time_present = False
    for i, var in enumerate(variables):
        code = var.get("code", "")
        vals = var.get("values", [])
        if not vals:
            continue
        if i == time_idx:
            time_present = True
            if since_date is not None:
                sel = [c for c in vals if (parse_date(c) or dt.date.min) >= since_date]
                if not sel:
                    # boundary already includes max; empty only if max no longer offered
                    # upstream -> fall back to the single newest code so we still probe.
                    sel = vals[-1:]
            else:
                sel = vals  # first-time backfill: full history
            query_vars.append({"code": code, "selection": {"filter": "item", "values": sel}})
        elif small:
            query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
        elif ((code == meta_time_code) if meta_time_code is not None
              else is_time_dim(code, vals)):
            # Ingester keep-full PARITY (jobs/ingest_statfin.py query_table): over
            # MAX_CELLS the ingester keeps every variable its own is_time test flags
            # at the FULL value list — e.g. the demoted month axis of a month+year
            # cube — so the stored keys cover all its values. Collapsing it to the
            # aggregate here would tail only a sliver of those stored series.
            query_vars.append({"code": code, "selection": {"filter": "item", "values": vals}})
        else:
            agg = [v for v in vals if str(v).upper() in AGG_CODES]
            selected = agg[:1] if agg else vals[:1]
            query_vars.append({"code": code, "selection": {"filter": "item", "values": selected}})

    if not time_present or not query_vars:
        return None, time_present
    return {"query": query_vars, "response": {"format": "json-stat2"}}, time_present


def _query_table(sess, path, since_date):
    """Date-tail query for one table.

    Returns (rows, outcome) where outcome is one of:
      'data'        rows parsed (>=0) from a real 200 json-stat2 body
      'empty'       table legitimately empty/current (no meta, no time var, 400/404,
                    or a tiny non-time cube)
      'structural'  a 200 with a real json-stat2 envelope but 0 parseable rows on a
                    table that previously HAD data (since_date is not None)
    Raises TransientError on timeout/5xx/429/network (caller -> partial)."""
    meta = _get_meta(sess, path)
    time.sleep(RATE)
    if not meta or not isinstance(meta, dict):
        return [], "empty"
    variables = meta.get("variables", [])
    if not variables:
        return [], "empty"

    body, time_present = _build_query(variables, since_date)
    if not time_present:
        # No time dimension -> this table never contributed dated rows; nothing to tail.
        return [], "empty"
    if body is None:
        return [], "empty"

    resp = _post_data(sess, path, body)
    time.sleep(RATE)
    if resp is None or resp is _NO_DATA:
        # 400/403/404 on the restricted selection -> nothing for this tail.
        return [], "empty"

    meta_time_code = next((v.get("code") for v in variables if v.get("time") is True), None)
    rows = parse_jsonstat2(resp, path, meta_time_code)
    if rows:
        return rows, "data"

    # 0 rows from a 200 json-stat2 body. Distinguish structural break from quiet tail:
    # a real envelope is one that declared dimensions + a value array. For an INCREMENTAL
    # tail (since_date set) on a previously-populated table, a real envelope yielding 0
    # parseable rows is a structural break (the cube's shape/time coding changed). For a
    # first-time full fetch (since_date is None) with an empty value array it is simply a
    # currently-empty table.
    has_envelope = bool(resp.get("id")) and ("value" in resp)
    nonempty_values = bool(resp.get("value"))
    if since_date is not None and has_envelope and nonempty_values:
        return [], "structural"
    return [], "empty"


# --------------------------------------------------------------------------- #
# contract entry point
# --------------------------------------------------------------------------- #
def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    sess = _session()

    tables = _crawl_catalog(sess)
    if not tables:
        raise DefinitiveError(f"statfin: catalog crawl returned 0 tables (PxWeb tree gone?)")

    # Group tables by subject area (first path component) == subject parquet file.
    # `subj` is the output filename stem; the cached _catalog.json is read and trusted, so
    # re-validate here (not just at crawl time) — a poisoned/stale cache must not be able to
    # write '<subj>.parquet' outside the source dir.
    by_subject: dict[str, list[dict]] = defaultdict(list)
    for t in tables:
        subj = (t.get("path", "").split("/") or ["root"])[0] or "root"
        if not _safe_segment(subj):
            continue
        by_subject[subj].append(t)

    tally = Tally()
    total_rows = 0
    global_max: dt.date | None = None
    cursors: dict[str, str] = {}   # table prefix '<subj>:<table_id>' -> max obs_date written

    # BOUND ITSELF BELOW THE ORCHESTRATOR'S CAP, AND ROTATE.
    # Measured in the 2026-08-02 cloud run: statfin was killed by the 45-minute hard
    # timeout, as were stat_estonia and worldbank_wdi — three sources burning ~135 of the
    # run's ~262 minutes and then being interrupted. Only 20 sources were attempted all
    # day, so a "daily" source is really touched about every fifth day (dst had gone 8).
    #
    # Being killed and yielding are different events: the kill runs no cleanup and, worse,
    # the subject list is `sorted(...)` — a FIXED order. A bound over a fixed order is a
    # truncation, not a budget (R190): every run re-walked the same prefix and the tail
    # subjects were never reached at all, no matter how many runs went by.
    #
    # So: stop at 30 minutes, under the 45-minute cap, and resume after the subject the
    # last run finished. Per-subject merges already land inside the loop, so a stop keeps
    # everything done so far; rotation is what makes the remainder actually arrive.
    budget_min = float(os.environ.get("STATFIN_BUDGET_MIN", "30"))
    dl = Deadline(minutes=budget_min)
    subjects = rotate_after(sorted(by_subject.keys()), load_rotation(out_dir))
    stopped_early = False
    last_subj = ""

    for subj in subjects:
        if dl.spent():
            stopped_early = True
            print(f"[{SOURCE}] budget of {budget_min:.0f} min spent after "
                  f"{dl.elapsed_min():.1f} min — stopped after subject {last_subj!r}, "
                  f"{len(subjects) - subjects.index(subj)} of {len(subjects)} subject(s) "
                  f"deferred to the next tick", flush=True)
            break
        last_subj = subj
        subj_tables = by_subject[subj]
        path = os.path.join(out_dir, f"{subj}.parquet")
        before = blob.row_count(path)
        frontier = _table_frontier(out_dir, subj)

        # Seed cursors from the on-disk frontier so an untouched table still reports its
        # real freshness (a frozen table can't hide behind the unit-level max). A corrupt
        # far-future on-disk max (mis-parsed category code -> year 9999/6000 sentinel) is
        # NOT written into the cursor — otherwise it would mask a frozen series from
        # health.py's max(cursors). On-disk rows themselves are kept untouched.
        for pref, d in frontier.items():
            if not _sane(d):
                continue
            cursors[pref] = d.isoformat()
            if global_max is None or d > global_max:
                global_max = d

        # Accumulate all NEW rows for this subject, then ONE merge into the subject file.
        s_keys: list[str] = []
        s_dates: list[dt.date] = []
        s_vals: list[float] = []

        for t in subj_tables:
            tpath = t.get("path", "")
            if not tpath:
                continue
            pref = tpath.replace("/", ":")          # '<subj>:<table_id>'
            # None => never-seen table: full backfill. A corrupt far-future stored max
            # (year 9999/6000 sentinel) would make the `>= since_date` time filter select
            # NOTHING and freeze the table forever; sane_since() returns None for such a
            # value, falling back to a full re-pull instead of a frozen delta.
            since_date = sane_since(frontier.get(pref))
            try:
                rows, outcome = _query_table(sess, tpath, since_date)
            except TransientError:
                # One flaky table can't strand the subject; record & keep going -> partial.
                tally.transient_unit()
                continue

            if outcome == "structural":
                tally.structural_unit()              # finalize() -> DefinitiveError
                continue
            if outcome == "empty":
                tally.empty_unit()
                continue

            # outcome == 'data': rows parsed from a real 200 body (data flowed). All parsed
            # rows are kept on disk; only a SANE date may advance the reported watermark /
            # cursor (a category code mis-parsed as a far-future year must not pose as fresh).
            tmax = None
            for key, d, v in rows:
                s_keys.append(key); s_dates.append(d); s_vals.append(v)
                if _sane(d) and (tmax is None or d > tmax):
                    tmax = d
            if tmax is not None:
                prev = cursors.get(pref)
                if prev is None or tmax.isoformat() > prev:
                    cursors[pref] = tmax.isoformat()
                if global_max is None or tmax > global_max:
                    global_max = tmax
            # Count as a successful sub-unit even if it nets 0 after dedup (healthy
            # idempotent re-run) so a quiet steady state never trips the all-empty floor.
            tally.added_unit(len(rows))

        # Merge this subject's new rows (one atomic publish per subject file).
        if s_vals:
            # Defense-in-depth: the shared publish path does not assert containment, so
            # confirm the write target resolves inside the source dir before merging. `subj`
            # is already allowlisted above; this catches any residual path-escape.
            if not _contained(path, out_dir):
                raise DefinitiveError(f"statfin: refusing write outside source dir: {path}")
            new_tbl = pa.table({
                "series_key": pa.array(s_keys, pa.string()),
                "obs_date":   pa.array(s_dates, pa.date32()),
                "value":      pa.array(s_vals, pa.float64()),
            })
            # merge_and_write creates the file when absent and dedups against existing rows.
            n, _ = merge.merge_and_write(path, new_tbl, mode="merge", dedup_keys=DEDUP)
            total_rows += n
        else:
            total_rows += before

    # Save the bookmark even after a COMPLETE pass: it is then the last subject in order
    # and the next run wraps to the top through this same path, so there is no branch that
    # could quietly stop rotating.
    if last_subj:
        save_rotation(out_dir, last_subj)

    last_obs = global_max.isoformat() if global_max else (since or None)
    # Sub-units == tables; contract floor is (#subunits - 1). Active-but-quiet tables are
    # counted added_unit (data flowed), so the floor only trips on a true wholesale outage
    # where EVERY table returned empty AND none added.
    #
    # A budget stop only walked PART of the subject list, so the floor must be scaled to
    # what was actually visited — otherwise a clean partial pass trips a "wholesale
    # outage" that did not happen.
    if stopped_early:
        visited = set(subjects[:subjects.index(last_subj) + 1])
        n_subunits = sum(len(by_subject[s]) for s in visited if s in by_subject)
    else:
        n_subunits = sum(len(v) for v in by_subject.values())
    return finalize(tally, total_rows, last_obs, source=SOURCE,
                    series_cursors=cursors, empty_window_floor=max(n_subunits - 1, 1))
