#!/usr/bin/env python3
"""Statistics Denmark (DST/Danmarks Statistik) — full StatBank ingest.

License: Creative Commons Attribution 4.0 (CC BY 4.0)
Source: https://www.dst.dk/en/Statistik/statistikbanken
No API key required.

Coverage: ~2,300 tables in the Danish StatBank:
  * National accounts (GDP, GNI, output)
  * Labour force (employment, unemployment, wages)
  * Consumer and producer prices
  * Trade in goods and services
  * Population and demography
  * Housing prices and construction
  * Financial statistics
  * Business statistics
  * Energy statistics

API: GET  /v1/tables?lang=en          → list all tables
     GET  /v1/tableinfo?id=X&lang=en  → table metadata (variables + values)
     POST /v1/data                    → data in JSON-stat format

Run: python jobs/ingest_dst.py
"""
from __future__ import annotations
import datetime as dt, json, os, re, time
from collections import defaultdict
import pyarrow as pa, pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
import sys as _sys
# The shared value-first time-axis resolver lives in THIS module's own repo (jobs/ and
# core/ are siblings). Derive the repo root from __file__ so `from core import pxweb`
# resolves both when this file is run standalone AND when the fetcher importlib-loads it
# (same convention as updater/config.py and tools/pxweb_regression.py). The hardcoded ROOT
# above is the DATA tree, which does not carry core/pxweb.py on this branch.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in _sys.path:
    _sys.path.insert(0, _REPO_ROOT)
from core import pxweb as _pxweb
OUT  = os.path.join(ROOT, "data", "clean_full", "dst")
BASE = "https://api.statbank.dk/v1"
UA   = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
RATE = 0.3
MAX_CELLS = 50_000  # DST has ~100K limit; use 50K for safety


def log(m):
    try:
        print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)
    except UnicodeEncodeError:
        print(f"[{time.strftime('%H:%M:%S')}] {str(m).encode('ascii','replace').decode()}", flush=True)


def _reraise_fence(e) -> None:
    """The updater's unit timeout (orchestrate.UnitTimeout, raised by the alarm wherever the main thread is - most
    often inside requests) must end the pass. This module's `except Exception` blocks caught it and returned None,
    so the fetcher saw one more failed table and the pass ran on past its limit (review R1297). Read from
    sys.modules: standalone runs have no orchestrator and nothing to re-raise."""
    orch = _sys.modules.get("updater.orchestrate")
    if orch is not None and (getattr(orch, "UNIT_TIMEOUT_FIRED", False) or
                             isinstance(e, getattr(orch, "UnitTimeout", ()))):
        raise e


def get_json(url: str, retries: int = 3) -> dict | list | None:
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=UA, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (400, 404):
                return None
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  HTTP {r.status_code}: {url[-80:]}")
        except Exception as e:
            _reraise_fence(e)
            log(f"  ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


# The FINAL answer of the last post_json call: its HTTP status (None when the last attempt raised before any
# answer) and DST's errorTypeCode. post_json returns None for every kind of failure; the BULK fallback in
# _post_rows must fire on exactly one kind - a final HTTP 500 - so the cause is kept here.
_LAST_POST = {"status": None, "code": None}


def _error_code(r) -> "str | None":
    try:
        return (r.json() or {}).get("errorTypeCode")
    except Exception:                                                # noqa: BLE001 - a non-JSON error body
        return None


def post_json(url: str, body: dict, retries: int = 3) -> dict | None:
    _LAST_POST.update(status=None, code=None)
    for attempt in range(retries):
        try:
            r = requests.post(url, json=body, headers=UA, timeout=120)
            if r.status_code == 200:
                _LAST_POST.update(status=200, code=None)
                return r.json()
            _LAST_POST.update(status=r.status_code, code=_error_code(r))
            if r.status_code in (400, 403, 404):
                return None
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  POST HTTP {r.status_code}: {url[-60:]}")
        except Exception as e:
            _reraise_fence(e)
            _LAST_POST.update(status=None, code=None)                 # this attempt got no answer
            log(f"  POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    return None


def parse_date(s: str) -> dt.date | None:
    """Parse DST time values: 2023, 2023M01, 2023Q1/2023K1, 2023H1, 2023W01/2023U01, 2022M04D01, 2021:2022.

    The Danish letters (K = kvartal, U = uge) and the one-year span arrive when DST has no English label for a
    code: FOLK1A's '2008K1' parses through its label '2008Q1', REGR63's '2007K2' has none. Measured 2026-09-29 over
    all 2,309 active tables (review R1297): 41 tables had no parseable code - 24 'YYYY:YYYY+1', 12 'YYYY/YYYY+1',
    3 'YYYYKn', 2 'YYYYUnn' - and each read as 'unparsed' on every run."""
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
            return dt.date(int(m.group(1)), (q - 1) * 3 + 1, 1)       # K0 / K5: impossible month -> ValueError
        m = re.match(r"^(\d{4})H(\d)$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), 1 if m.group(2) == "1" else 7, 1)
        m = re.match(r"^(\d{4})[WU](\d{2})$", s, re.IGNORECASE)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            return dt.date.fromisocalendar(yr, wk, 1)
        # ONE-YEAR SPAN (school / season / split year): '2021:2022', '2020/2021'. Dated to 31 Dec of the year it
        # begins - the project's convention for a split year (core/pxweb.parse_period, R288). ONLY y2 == y1 + 1:
        # a longer window ('2007:2009', RECIDIV*) is a convention not chosen and stays span_time.
        m = re.match(r"^(\d{4})\s*[:/\-]\s*(\d{4})$", s)
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2))
            return dt.date(y1, 12, 31) if y2 == y1 + 1 else None
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return dt.date.fromisoformat(s)
        # DAILY: "2022M04D01" (DNINDEX, DNVALD - 2026-09-29). Real bodies of 1,121 non-null values parsed to 0
        # rows and were booked as legitimately EMPTY tables, advancing the manifest. Dated to the day itself,
        # the convention cso's identical grammar uses (R299); an impossible day ("M02D30") stays None.
        m = re.match(r"^(\d{4})M(\d{2})D(\d{2})$", s, re.IGNORECASE)
        if m:
            return dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except (ValueError, TypeError):
        pass
    return None


# Codes that literally name a DST time dimension. A real time axis (DST always uses
# "Tid") must win over any non-time category whose numeric codes happen to look date-ish.
TIME_CODES = ("tid", "time", "year", "aar", "år", "periode", "period", "datum",
              "maaned", "måned", "maned", "kvartal", "uge", "week", "month", "quarter")


def is_time_dim(code: str, values: list[str]) -> bool:
    """RETAINED BUT NO LONGER CALLED. Time-axis selection now goes through the
    shared value-first resolver core.pxweb.resolve_time_dim (see parse_jsonstat),
    which takes the JSON-stat `dimension.role.time` array as its authoritative
    step (DST always populates it with "Tid") and value-parses before any name
    match. Kept defined for parity with sibling ingesters and because the
    pxweb_regression superset check re-derives per-source time-name tokens from
    this file's TIME_CODES tuple. DST's over-cap branch (query_table) uses the
    tableinfo `time` flag, not this function.

    A dimension is 'time' only if its code names a time dim, OR its values parse via
    parse_date() to a SANE year (~1900..current_year+2). The old fallback matched
    ^\\d{4}[MQHW]?\\d*$ against the first code, which over-matched non-time numeric
    category/ContentsCode codes (e.g. '00000858', a category code '2584', municipality
    codes) and treated them as the time axis, writing garbage obs_dates like 2584-12-31.
    Anchoring on parse_date() + a sane year range removes that false match (out-of-range
    years are rejected; 8-digit codes don't parse to a date at all)."""
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


def parse_jsonstat(data: dict, table_id: str) -> list[tuple[str, dt.date, float]]:
    """Parse JSON-stat (v1) format from DST.

    Time-axis selection goes through the shared value-first resolver
    (core/pxweb.py): authoritative JSON-stat role.time first (DST always
    populates it with "Tid"; note v1 nests `role` under `dimension`, so it is
    read from dim_obj, NOT the response root as in JSON-stat2), else highest
    date-parse-rate, else literal name as a last resort. Value-first stops a
    month axis (non-date codes) from outranking a year axis when role.time is
    absent — the old name-first fallback picked the month dim of a month+year
    cube and produced 0 rows. DST is code-coded: the dates live in the
    category.index CODES (the key/date assembly below reads dim_codes), so
    dim_codes is what the resolver scores."""
    results = []
    try:
        ds = data.get("dataset", data)  # handle both root and wrapped
        dim_obj = ds.get("dimension", {})
        dim_ids = dim_obj.get("id", [])
        dim_sizes = dim_obj.get("size", [])
        role = dim_obj.get("role", {})
        metric_dims = set(role.get("metric", []))
        values = ds.get("value", [])

        if not dim_ids or not values:
            return results

        # Build dimension code maps
        dim_codes = []
        dim_labels = []
        for i, did in enumerate(dim_ids):
            dim_info = dim_obj.get(did, {})
            cat = dim_info.get("category", {})
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
            # Some tables index the time axis POSITIONALLY ("0","1","2"…) and carry the real
            # period only in the category label. A code-only date lookup then parses nothing,
            # every observation is skipped, and a good 200 yields zero rows — which the fetcher
            # reports as a structural break. Keep the labels as a date fallback; the KEY still
            # uses codes, so no existing series_key changes.
            lab = cat.get("label", {})
            dim_labels.append([lab.get(c, "") if isinstance(lab, dict) else ""
                               for c in pos_to_code])

        # Pick the time dimension via the shared value-first resolver (core/pxweb.py):
        # authoritative role.time (v1: nested under `dimension`, hence role.get("time")
        # rather than _pxweb.role_time_of(data)), else highest date-parse-rate, else name.
        time_dim_idx = _pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=None, role_time=role.get("time"), parse_fn=parse_date)

        if time_dim_idx is None:
            return results

        # Compute strides
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
            obs_date = parse_date(t_codes[t_pos])
            if obs_date is None:
                # positional-index time codes: the period is in the label (see above)
                t_labels = dim_labels[time_dim_idx] if time_dim_idx < len(dim_labels) else []
                if t_pos < len(t_labels):
                    obs_date = parse_date(t_labels[t_pos])
            if obs_date is None:
                continue

            key_parts = [f"DST:{table_id}"]
            for i, (did, pos) in enumerate(zip(dim_ids, dim_indices)):
                if i == time_dim_idx:
                    continue
                if did in metric_dims:
                    continue  # ContentCode is redundant when there's only one metric
                codes_for_dim = dim_codes[i]
                code_val = codes_for_dim[pos] if pos < len(codes_for_dim) else str(pos)
                key_parts.append(f"{did}={code_val}")

            results.append((":".join(key_parts), obs_date, v))

    except Exception as e:
        _reraise_fence(e)                # a trip mid-parse must not return the rows read so far as the table
        log(f"  JSON-stat parse error for {table_id}: {e}")
    return results


def query_table(table_id: str, variables: list[dict], suppressed=None) -> list[tuple[str, dt.date, float]]:
    """Fetch data for one DST table (rows only - the bulk path's contract, unchanged)."""
    return query_table_detailed(table_id, variables, suppressed=suppressed)[0]


# DST answers HTTP 500 to a data POST with too many TIME values even when MAX_CELLS holds (DNVALD, 2026-09-29:
# 12,551 daily values -> 500 every time; the last 50 or 500 -> 200). A table with more time values than this is
# fetched in time chunks of this size.
TIME_CHUNK = 1000
_SPAN_PERIOD = re.compile(r"^(\d{4})\s*[:\-/]\s*(\d{4})$")
# A PART-YEAR window: BARLOV1 publishes 'AUG - DEC 2019' (five months of one year). Which date it stands for is the
# same undecided convention as a multi-year window, so it is named span_time - not our parser gap, which would keep
# the table owed and dst partial on every run (review R1297, measured live 2026-09-29).
_PART_YEAR = re.compile(r"^[A-Z]{3}\s*-\s*[A-Z]{3}\s+\d{4}$", re.IGNORECASE)


def _is_undated_window(code: str) -> bool:
    """A window the dating convention does not cover: longer than one year ('2007:2009'), or part of one year.
    A one-year span ('2021:2022') is NOT one - parse_date dates it."""
    m = _SPAN_PERIOD.match(code)
    if m:
        return int(m.group(2)) > int(m.group(1)) + 1
    return bool(_PART_YEAR.match(code))


def query_table_detailed(table_id: str, variables: list[dict],
                         suppressed=None) -> "tuple[list[tuple[str, dt.date, float]], str]":
    """(rows, outcome). `suppressed` is the table's tableinfo `suppressedDataValue`, used only by the BULK fallback
    (_post_rows, parse_bulk). outcome is one of
        ok            rows parsed
        no_selection  the table offers nothing to select - legitimately empty
        failed        a data POST failed (5xx/timeout after retries, 400/403/404; any one chunk fails the table) - the
                      PUBLISHER's bad hour, retried next run; NEVER a quiet table (review R1283: it was booked
                      empty and the manifest advanced, so the release was skipped until DST republished)
        all_null      a real body whose every value is missing ('..') - legitimately empty
        span_time     every time code is a window the dating convention does not cover - multi-year ('2007:2009')
                      or part-year ('AUG - DEC 2019'); not chosen yet, like cso's span_time; nothing is stored
        unparsed      a real body with values that parsed to 0 rows - OUR gap, never self-heals"""
    selection = _query_selection(variables)
    if not selection:
        return [], "no_selection"
    tvar = next((s for s in selection if s.get("_time")), None)
    if tvar is None or len(tvar["values"]) <= TIME_CHUNK:
        return _post_rows(table_id, selection, variables, suppressed)
    # MORE THAN TIME_CHUNK TIME VALUES: CHUNKS FROM THE START. The full POST was tried first, and for the daily
    # tables it fails every time (DNVALD, DNRENTD: 3 x HTTP 500 plus ~30 s of back-off, ~110 s per table per day,
    # review R1297) - a cost with no case where it helped that chunks do not also cover (BEV3A, 1,506 values,
    # is 2 POSTs instead of 1).
    rows, spans = [], 0
    for i in range(0, len(tvar["values"]), TIME_CHUNK):
        part = [dict(s, values=tvar["values"][i:i + TIME_CHUNK]) if s is tvar else s for s in selection]
        got, why = _post_rows(table_id, part, variables, suppressed)
        if why == "failed":
            return [], "failed"              # a table is whole or owed: never keep the chunks before the failure
        if got:
            rows.extend(got)
            continue
        if why == "all_null":
            continue                         # an all-missing stretch (holidays, a gap) - the other chunks stand
        if why == "span_time":
            spans += 1                       # windows are not dated anywhere; the dated chunks still stand
            continue
        return [], why                       # unparsed: our gap - the whole table stays owed
    if rows:
        return rows, "ok"
    return [], ("span_time" if spans else "all_null")


def _wire_values(s) -> list:
    """The values to SEND for one selection entry. DST's /data reads a value that starts with '<' or '>' as a
    comparison, not an id: AKU240K's hours bucket '<15' became "Can't find value: 15 (<15)" - HTTP 400 on every run
    (measured 2026-09-29; review R1297). When the whole value list is selected, DST's own wildcard '*' asks for the
    same set and returns 200. A time variable is never sent as '*' (it is chunked by id)."""
    vals = s["values"]
    if s.get("_all") and not s.get("_time") and any(str(v)[:1] in "<>" for v in vals):
        return ["*"]
    return vals


def _post_data(table_id, selection):
    body = {"table": table_id, "format": "JSONSTAT", "lang": "en",
            "variables": [{"code": s["code"], "values": _wire_values(s)} for s in selection]}
    resp = post_json(f"{BASE}/data", body)
    time.sleep(RATE)
    return resp or None


# THE BULK FALLBACK (2026-10-19). Since 2026-09-30 DST answers every data POST for VNASFK and VNKSFK - even ONE
# cell, in JSONSTAT and in CSV - with HTTP 500 {"errorTypeCode":"SYSTEM-100"}, while the same selection in its
# streaming BULK format answers 200. Measured live 2026-10-19: of the 23 tables with a version variable plus NASFK,
# these 2 fail this way, and the control FOLK1A answers 200 in all three formats. So when a JSONSTAT POST's FINAL answer is
# HTTP 500 - and only then: never a 4xx, a 429, a 502/503/504, a timeout or a 200 - the same selection is asked
# for once in BULK with codes (valuePresentation=Code) and parsed into exactly the rows parse_jsonstat makes.
# Every use is printed and appended here; the fetcher resets the list per pass and names it in its note.
BULK_FALLBACKS: list = []
BULK_VALUE_COL = "INDHOLD"


def _post_rows(table_id, selection, variables, suppressed=None) -> "tuple[list[tuple[str, dt.date, float]], str]":
    """One data POST as (rows, why): why is ok | failed | all_null | span_time | unparsed (query_table_detailed)."""
    _LAST_POST.update(status=None, code=None)
    resp = _post_data(table_id, selection)
    if resp is not None:
        rows = parse_jsonstat(resp, table_id)
        return (rows, "ok") if rows else ([], _why_empty(resp))
    if _LAST_POST["status"] != 500:
        return [], "failed"
    log(f"  {table_id}: JSONSTAT POST answered HTTP 500 ({_LAST_POST['code'] or 'no error code'}); "
        f"asking for the same selection in BULK format")
    BULK_FALLBACKS.append(table_id)
    text = _post_bulk(table_id, selection)
    if text is None:
        log(f"  {table_id}: the BULK POST failed too")
        return [], "failed"
    rows = parse_bulk(text, table_id, variables, selection, suppressed)
    if rows is None:
        log(f"  {table_id}: the BULK body does not have the requested columns - not parsed")
        return [], "unparsed"
    log(f"  {table_id}: BULK fallback parsed {len(rows):,} rows")
    return (rows, "ok") if rows else ([], _why_empty_bulk(text, selection, suppressed))


def _post_bulk(table_id, selection, retries: int = 3) -> "str | None":
    """The same selection as _post_data, in DST's streaming BULK format with CODES (not labels), as text."""
    body = {"table": table_id, "format": "BULK", "lang": "en", "valuePresentation": "Code",
            "variables": [{"code": s["code"], "values": _wire_values(s)} for s in selection]}
    out = None
    for attempt in range(retries):
        try:
            r = requests.post(f"{BASE}/data", json=body, headers=UA, timeout=300)
            if r.status_code == 200:
                out = r.content.decode("utf-8-sig")
                break
            if r.status_code in (400, 403, 404):
                break
            if r.status_code == 429:
                log("  429 throttle, sleeping 30s"); time.sleep(30); continue
            log(f"  BULK POST HTTP {r.status_code}: {table_id}")
        except Exception as e:
            _reraise_fence(e)
            log(f"  BULK POST ERR: {e}")
        time.sleep(5 * (attempt + 1))
    time.sleep(RATE)
    return out


def _bulk_records(text, selection):
    """(header_ok, data rows) of a BULK body. The header must be the requested variable ids, upper-cased as DST
    writes them, in request order, then the value column - anything else is refused, never guessed at."""
    import csv
    import io
    recs = list(csv.reader(io.StringIO(text.lstrip(chr(0xFEFF))), delimiter=";"))
    want = [str(s["code"]).upper() for s in selection] + [BULK_VALUE_COL]
    if not recs or [h.strip().upper() for h in recs[0]] != want:
        return False, []
    body = [r for r in recs[1:] if r]
    if any(len(r) != len(want) for r in body):
        return False, []
    return True, body


def _finite(x) -> "float | None":
    try:
        v = float(str(x).strip())
    except (TypeError, ValueError):
        return None
    return v if v == v and v not in (float("inf"), float("-inf")) else None


def parse_bulk(text: str, table_id: str, variables: list[dict], selection: list[dict], suppressed=None):
    """A BULK (valuePresentation=Code) body as the SAME (series_key, obs_date, value) rows parse_jsonstat makes for
    the same selection, or None when the body does not have the requested columns.

    series_key = 'DST:<table>' + ':<id>=<code>' for every non-time variable in request order: JSON-stat lists the
    dimensions in that order, and its one extra dimension (ContentsCode, role metric) is skipped by parse_jsonstat
    and absent here. The time column is the tableinfo variable flagged time; its date is parse_date(code), else
    parse_date(label) with the label from tableinfo (parse_jsonstat's positional-code fallback). A missing value
    ('..', empty, or anything that is not a finite number) is skipped, as JSON-stat's null is.

    BULK STREAMS ONLY THE CELLS THAT EXIST. JSON-stat is dense: a cell the table does not hold is written as the
    table's `suppressedDataValue` (tableinfo; "0" on the tables measured), so JSON-stat carries a 0.0 row there and
    BULK carries nothing (measured 2026-10-19: VAN1UGE 248 JSON-stat rows, 17 BULK rows, the 231 others all 0.0).
    Pass that value as `suppressed` and every selected cell BULK left out is filled with it, exactly as JSON-stat
    fills it; a suppressed value that is not a number (e.g. '..') fills nothing, as JSON-stat's null stores nothing."""
    ok, body = _bulk_records(text, selection)
    if not ok:
        return None
    t_idx = next((i for i, s in enumerate(selection) if s.get("_time")), None)
    if t_idx is None:
        return None
    tcode = selection[t_idx]["code"]
    tvar = next((v for v in variables if v.get("id") == tcode), {})
    labels = {str(v.get("id")): str(v.get("text") or "") for v in tvar.get("values", [])}
    others = [(i, s["code"]) for i, s in enumerate(selection) if i != t_idx]

    def _row(codes, v):
        code = codes[t_idx]
        obs_date = parse_date(code) or parse_date(labels.get(code, ""))
        if obs_date is None:
            return None
        return (":".join([f"DST:{table_id}"] + [f"{cid}={codes[i]}" for i, cid in others]), obs_date, v)

    rows, present = [], set()
    for rec in body:
        codes = tuple(rec[:-1])
        present.add(codes)
        v = _finite(rec[-1])
        if v is None:
            continue
        r = _row(codes, v)
        if r is not None:
            rows.append(r)
    fill = _finite(suppressed) if suppressed is not None else None
    if fill is not None:
        import itertools
        for codes in itertools.product(*[[str(x) for x in s["values"]] for s in selection]):
            if codes not in present:
                r = _row(codes, fill)
                if r is not None:
                    rows.append(r)
    return rows


def _why_empty_bulk(text, selection, suppressed=None) -> str:
    """_why_empty for a BULK body that gave no rows: every value missing, every period an undated window, or ours.
    A cell BULK left out counts as JSON-stat writes it - the suppressed value - so the verdict is JSON-stat's."""
    _ok, body = _bulk_records(text, selection)
    numeric = any(_finite(r[-1]) is not None for r in body)
    if not numeric and suppressed is not None and _finite(suppressed) is not None:
        n_cells = 1
        for s in selection:
            n_cells *= len(s["values"])
        numeric = len({tuple(r[:-1]) for r in body}) < n_cells       # some cell is filled with the number
    if not numeric:
        return "all_null"
    tvar = next((s for s in selection if s.get("_time")), None)
    codes = [str(c) for c in (tvar or {}).get("values", [])]
    if codes and all(_is_undated_window(c) for c in codes):
        return "span_time"
    return "unparsed"


def _why_empty(resp) -> str:
    """Name a real body that yielded no rows: the publisher's empty table, an undated window, or our gap."""
    try:
        ds = resp.get("dataset", resp)
        vals = ds.get("value")
        vlist = list(vals.values()) if isinstance(vals, dict) else list(vals or [])
        if not vlist or all(v is None for v in vlist):
            return "all_null"
        dims = ds.get("dimension") or {}
        role = (dims.get("role") or ds.get("role") or {}).get("time") or []
        tdim = next(iter(role), None)
        if tdim is not None:
            idx = (dims.get(tdim) or {}).get("category", {}).get("index")
            codes = list(idx) if isinstance(idx, (dict, list)) else []
            if codes and all(_is_undated_window(str(c)) for c in codes):
                return "span_time"
    except (AttributeError, TypeError, ValueError):
        pass
    return "unparsed"


def _query_selection(variables: list[dict]) -> list[dict]:
    """The select-all query (the old query_table's body logic, unchanged) as [{code, values, _time, _all}]; _all
    says every value of the variable is selected (so '*' may be sent for it - _wire_values)."""
    # Build select-all query
    total_cells = 1
    var_selection = []
    time_var_ids = []

    for var in variables:
        vid = var["id"]
        is_time = var.get("time", False)
        vals = [v["id"] for v in var.get("values", [])]
        if not vals:
            continue
        total_cells *= len(vals)
        if is_time:
            time_var_ids.append(vid)

    # If too large, restrict non-time vars to first/aggregate value
    if total_cells > MAX_CELLS:
        for var in variables:
            vid = var["id"]
            is_time = var.get("time", False)
            vals = [v["id"] for v in var.get("values", [])]
            if not vals:
                continue
            if is_time:
                var_selection.append({"code": vid, "values": vals, "_time": True, "_all": True})
            else:
                # Prefer aggregate/total codes
                agg = [v for v in vals if v.upper() in ("TOT", "0", "000", "TOTAL", "T", "ALL")]
                selected = agg[:1] if agg else vals[:1]
                var_selection.append({"code": vid, "values": selected, "_time": False,
                                      "_all": len(selected) == len(vals)})
    else:
        for var in variables:
            vid = var["id"]
            vals = [v["id"] for v in var.get("values", [])]
            if vals:
                var_selection.append({"code": vid, "values": vals, "_time": bool(var.get("time", False)),
                                      "_all": True})

    return var_selection


def main():
    os.makedirs(OUT, exist_ok=True)

    # Get all tables
    log("Fetching DST table catalog...")
    tables_raw = get_json(f"{BASE}/tables?lang=en")
    if not tables_raw:
        log("Failed to get table list"); return

    # Filter to active tables
    tables = [t for t in tables_raw if t.get("active", True)]
    log(f"Found {len(tables)} active tables")

    # Group by subject area (first 2 chars of table ID)
    by_subject: dict[str, list] = defaultdict(list)
    for t in tables:
        tid = t.get("id", "")
        subj = re.sub(r"\d+$", "", tid)[:6] or tid[:2]
        by_subject[subj].append(t)

    log(f"Found {len(by_subject)} subject groups")

    total_obs = 0
    for subj in sorted(by_subject.keys()):
        subj_tables = by_subject[subj]
        out_path = os.path.join(OUT, f"{subj}.parquet")
        if os.path.exists(out_path):
            n = pq.read_metadata(out_path).num_rows
            log(f"  Skip {subj}: {n:,} rows"); total_obs += n; continue

        log(f"  Subject '{subj}': {len(subj_tables)} tables")
        all_keys, all_dates, all_vals = [], [], []
        seen: set[tuple] = set()

        for i, t in enumerate(subj_tables):
            table_id = t["id"]
            try:
                # Get table metadata
                meta = get_json(f"{BASE}/tableinfo?id={table_id}&lang=en")
                time.sleep(RATE)
                if not meta:
                    continue
                variables = meta.get("variables", [])
                if not variables:
                    continue

                rows = query_table(table_id, variables, suppressed=meta.get("suppressedDataValue"))
                n = 0
                for key, d, v in rows:
                    tok = (key, d)
                    if tok not in seen:
                        seen.add(tok)
                        all_keys.append(key)
                        all_dates.append(d)
                        all_vals.append(v)
                        n += 1
                if n > 0:
                    log(f"    [{i+1}/{len(subj_tables)}] {table_id}: {n:,} obs")
            except Exception as e:
                log(f"    [{i+1}] {table_id} ERR: {e}")

        if all_vals:
            tbl = pa.table({
                "series_key": pa.array(all_keys,  pa.string()),
                "obs_date":   pa.array(all_dates, pa.date32()),
                "value":      pa.array(all_vals,  pa.float64()),
            })
            pq.write_table(tbl, out_path, compression="zstd")
            n = pq.read_metadata(out_path).num_rows
            log(f"  {subj}: {n:,} obs saved")
            total_obs += n

    log(f"DONE: {total_obs:,} total DST Denmark observations")


if __name__ == "__main__":
    main()
