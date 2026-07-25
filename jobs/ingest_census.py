#!/usr/bin/env python3
"""FULL-COVERAGE grouped ingest of U.S. Census Bureau TIME-SERIES datasets.

Enumerates every dataset in https://api.census.gov/data.json, keeps the ones whose
c_dataset starts with "timeseries/", and pulls each one FULLY via the Census API
(api.census.gov). ONE Parquet per dataset under data/clean_full/census/, in long
"wide-record" form: every retrievable variable as a column (Census returns strings),
plus a constructed `series_key` (the non-time dimension columns joined) and the
time/period columns. us-public-domain.

Strategy per dataset (driven by data.json + variables.json + geography.json):
  * time-based ("time" required/present)  -> one pull per no-parent geo level with
        time=from+1900 (full history in a single response); plus child geo levels
        iterated over their required parent(s).
  * YEAR / year+quarter only (asm area/benchmark, qwi) -> enumerate the year(s)
        (and quarters) discovered from the API, iterate geographies.
  * no predicate at all (pseo, some asm value/benchmark) -> single pull per geo.
The Census API caps get= at 50 variables, so variables are chunked (<=45) with the
key columns repeated in every chunk and the chunks merged on those keys.

Usage:
  python jobs/ingest_census.py --list                # just print the catalog census
  python jobs/ingest_census.py --only timeseries/govs # one dataset
  python jobs/ingest_census.py --group eits           # one group
  python jobs/ingest_census.py                        # FULL run (all 93)
"""
import datetime as dt
import json
import os
import sys
import time
import threading
from collections import OrderedDict
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)

RAW = os.path.join(ROOT, "data", "raw", "census")
OUT = os.path.join(ROOT, "data", "clean_full", "census")
os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
LICENSE_ID = "us-public-domain"
MAX_GET = 45          # under the API's 50-variable cap, leaves room for key cols
VAR_MIN_YEAR = 1900   # "time=from+1900" => entire history

_KEY = None
def key():
    global _KEY
    if _KEY is None:
        for line in open(os.path.join(ROOT, ".env"), encoding="utf-8"):
            if line.startswith("CENSUS_API_KEY="):
                _KEY = line.split("=", 1)[1].strip()
    if not _KEY:
        raise SystemExit("missing CENSUS_API_KEY in .env")
    return _KEY

sess = requests.Session()
sess.headers.update(UA)
_lock = threading.Lock()


import json as _json

# Hard wall-clock ceiling for a single request's body download. The Census API
# sometimes trickles a giant response for minutes, which resets requests' per-read
# timeout and hangs forever -> we stream and abort once total elapsed exceeds this.
HARD_DEADLINE = 120


def _get_with_deadline(url, connect_timeout, deadline):
    """Stream a GET and abort if the body isn't fully received within `deadline`
    seconds total. Returns (status, parsed_json_or_None, text). Raises
    requests.exceptions.Timeout on deadline (treated as a timeout upstream)."""
    t0 = time.time()
    r = sess.get(url, timeout=(connect_timeout, connect_timeout), stream=True)
    status = r.status_code
    if status in (204,):
        r.close(); return status, None, ""
    chunks = []
    try:
        for chunk in r.iter_content(chunk_size=1 << 16):
            if chunk:
                chunks.append(chunk)
            if time.time() - t0 > deadline:
                r.close()
                raise requests.exceptions.Timeout("hard deadline")
    finally:
        r.close()
    text = b"".join(chunks).decode("utf-8", "replace")
    if status == 200:
        try:
            return 200, _json.loads(text), text
        except Exception:
            return 200, None, text
    return status, None, text


def _req(url, tries=5, timeout=100):
    """GET with retry/backoff + a hard body-download deadline. Returns
    (status, json_or_None, text). status -2 == timed out (caller subdivides);
    -1 == other failure."""
    last = ""
    timed_out = False
    deadline = min(timeout, HARD_DEADLINE)
    for i in range(tries):
        try:
            status, js, text = _get_with_deadline(url, connect_timeout=30,
                                                  deadline=deadline)
            if status == 200:
                return 200, js, text
            if status == 204:
                return 204, None, ""
            if status == 404:
                return 404, None, text
            if status == 400:
                return 400, None, text
            if status in (500, 502, 503, 504):
                last = f"{status}: {text[:120]}"
                time.sleep(2.0 * (i + 1))
                continue
            last = f"{status}: {text[:160]}"
        except requests.exceptions.Timeout:
            timed_out = True
            last = "timeout"
            break
        except Exception as e:  # noqa: BLE001
            last = repr(e)
        time.sleep(1.0 * (i + 1))
    return (-2 if timed_out else -1), None, last


# ---------------------------------------------------------------------------
# Catalog enumeration
# ---------------------------------------------------------------------------
def load_catalog():
    p = os.path.join(RAW, "data.json")
    if not os.path.exists(p):
        st, js, _ = _req("https://api.census.gov/data.json")
        if st != 200:
            raise SystemExit(f"cannot fetch data.json: {st}")
        json.dump(js, open(p, "w", encoding="utf-8"))
    data = json.load(open(p, encoding="utf-8"))
    return data["dataset"]


def timeseries_datasets(ds):
    return [d for d in ds if d.get("c_dataset")
            and "/".join(d["c_dataset"]).startswith("timeseries/")]


def load_meta():
    """Per-dataset metadata (variables + geography), cached to ts_meta.json."""
    p = os.path.join(RAW, "ts_meta.json")
    if os.path.exists(p):
        return {m["path"]: m for m in json.load(open(p, encoding="utf-8"))}
    # build it
    import concurrent.futures as cf
    ts = timeseries_datasets(load_catalog())
    skip = {"for", "in", "ucgid"}

    def probe(d):
        path = "/".join(d["c_dataset"])
        base = "https://api.census.gov/data/" + path
        _, v, _ = _req(base + "/variables.json")
        _, g, _ = _req(base + "/geography.json")
        v = v or {}
        g = g or {}
        vars_ = v.get("variables", {})
        allvars = [k for k in vars_ if k not in skip]
        required = [k for k, info in vars_.items()
                    if str(info.get("required")).lower() == "true"]
        geos = [(f.get("name"), f.get("requires"), f.get("optionalWithWCFor"))
                for f in g.get("fips", [])]
        return {"path": path, "group": d["c_dataset"][1], "title": d.get("title"),
                "allvars": allvars, "required": required,
                "has_YEAR": "YEAR" in vars_, "has_year": "year" in vars_,
                "has_quarter": "quarter" in vars_, "has_time": "time" in vars_,
                "geos": geos}

    out = {}
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for r in ex.map(probe, ts):
            out[r["path"]] = r
    json.dump(list(out.values()), open(p, "w", encoding="utf-8"), indent=1)
    return out


# ---------------------------------------------------------------------------
# Pull helpers
# ---------------------------------------------------------------------------
# columns that are MEASURES (the observed value), never dimensions/keys even if
# the API flags them required (case-insensitive match).
_MEASURE_COLS = {"cell_value"}
_MEASURE_COLS_U = {c.upper() for c in _MEASURE_COLS}


def key_columns(m):
    """Dimension/identifier columns that uniquely tag a record (kept in every
    variable chunk and used to merge chunks + build series_key)."""
    av = set(m["allvars"])
    keys = []
    for k in m["allvars"]:
        # predicate-ish: required dimension columns (but not the measure)
        if k in m["required"] and k.upper() not in _MEASURE_COLS_U:
            keys.append(k)
    # always-keep identifiers if present
    for cand in ("time", "YEAR", "year", "quarter", "MONTH", "NAME", "GEO_ID",
                 "us", "state", "county", "SURVEY_YEAR", "ucgid"):
        if cand in av and cand not in keys:
            keys.append(cand)
    return keys


# Some datasets are multi-dimensional cubes whose dimension axes are mutually
# exclusive in a single get= (the API 400s if you request them all at once).
# For these we curate the columns to a combination the API accepts; the result
# is the main cube (documented as not the full cross-tab in coverage notes).
# QWI enforces a 400k-cell query limit, so only a modest cross-tab fits per
# request. sex x agegrp x ownercode + headline measures stays under it.
_QWI_COLS = ["sex", "agegrp", "ownercode", "seasonadj",
             "Emp", "EmpEnd", "EmpS", "EmpTotal", "HirA", "HirN", "HirAEnd",
             "Sep", "SepBeg", "FrmJbGn", "FrmJbLs", "FrmJbC", "Payroll",
             "EarnBeg", "EarnS", "TurnOvrS"]

COLUMN_OVERRIDES = {
    # QWI is a cube; its demographic axes (sex/agegrp/race/ethnicity/education)
    # vs firm axes (firmage/firmsize) are separate cross-tabs. We pull the
    # demographic x industry cross-tab with the headline measures (one <=45-var
    # chunk -> no lossy merge). firmage/firmsize cross-tabs are not enumerated.
    "timeseries/qwi/sa": list(_QWI_COLS),
    "timeseries/qwi/se": list(_QWI_COLS),
    "timeseries/qwi/rh": list(_QWI_COLS),
    # PSEO flows: the full 24-var request times out (no time axis to subdivide);
    # curate to cohort + program + institution dims + grads-employed measures.
    "timeseries/pseo/flows": ["GRAD_COHORT", "NAME", "CIPCODE", "CIP_LEVEL",
                              "INSTITUTION", "INST_STATE", "GRAD_DEGREE",
                              "Y1_GRADS_EMP", "Y5_GRADS_EMP", "Y10_GRADS_EMP",
                              "Y1_GRADS_EMP_INSTATE", "Y5_GRADS_EMP_INSTATE",
                              "Y10_GRADS_EMP_INSTATE", "TOT_GRADS"],
    # BDS is a cube whose cross-tab axes (NAICS, FAGE, FSIZE, EAGE, ESIZE,
    # METRO) are largely mutually exclusive in a single get=. NAICS x FAGE
    # (sector x firm-age) is the richest combination the API serves together;
    # we pull that with all measures. Full FSIZE/EAGE/ESIZE/METRO cross-tabs
    # are NOT exhaustively enumerated (documented in coverage notes).
    "timeseries/bds": ["YEAR", "NAME", "NAICS", "NAICS_LABEL", "FAGE",
                       "FIRM", "ESTAB", "EMP", "DENOM", "ESTABS_ENTRY",
                       "ESTABS_ENTRY_RATE", "ESTABS_EXIT", "ESTABS_EXIT_RATE",
                       "JOB_CREATION", "JOB_CREATION_RATE", "JOB_DESTRUCTION",
                       "JOB_DESTRUCTION_RATE", "NET_JOB_CREATION",
                       "NET_JOB_CREATION_RATE", "REALLOCATION_RATE",
                       "FIRMDEATH_FIRMS", "FIRMDEATH_ESTABS", "FIRMDEATH_EMP"],
}


def value_columns(m, keys):
    ov = COLUMN_OVERRIDES.get(m["path"])
    if ov:
        return [v for v in ov if v not in keys and v in set(m["allvars"])]
    return [v for v in m["allvars"] if v not in keys]


def _enc_geo(clause):
    """URL-encode a for/in clause value (geo level names contain spaces and
    slashes) while preserving the ':' that separates level from selector."""
    if ":" in clause:
        lvl, sel = clause.split(":", 1)
        return quote(lvl, safe="") + ":" + quote(sel, safe="*,")
    return quote(clause, safe="")


def build_url(path, getvars, geo_for, geo_in, time_clause, extra):
    base = "https://api.census.gov/data/" + path + "?get=" + quote(
        ",".join(getvars), safe=",")
    if geo_for:
        base += "&for=" + _enc_geo(geo_for)
    if geo_in:
        base += "&in=" + _enc_geo(geo_in)
    if time_clause:
        base += "&" + time_clause          # time uses '+' as space (Census conv.)
    for k, v in (extra or {}).items():
        base += f"&{k}={quote(str(v), safe='*-/+')}"
    base += "&key=" + key()
    return base


import re
_GEO_PSEUDO = ("us", "state", "county", "region", "division", "place",
               "metropolitan statistical area/micropolitan statistical area")
# predicate variables: supplied in the predicate/clause, NOT in get= (they are
# echoed back as columns automatically). time is datetime; YEAR/year/quarter/MONTH
# are the time predicates we drive iteration with.
_PRED_VARS = ("time", "YEAR", "year", "quarter", "MONTH")
_varlimit_cache = {}


def _not_in_get(name, time_clause, extra):
    if name in _GEO_PSEUDO:
        return True
    # any var supplied as a predicate in `extra` (e.g. CTY_CODE=-, YEAR=2020) is
    # echoed back as a column automatically -> requesting it in get= too yields a
    # DUPLICATE column. Strip it.
    if extra and name in extra:
        return True
    # strip predicate vars used as a predicate this call (returned as columns
    # automatically). When time= is the predicate it also encodes year/quarter/
    # month, which must NOT be requested in get= (the API errors/empties).
    if name == "time" and time_clause and "time=" in time_clause:
        return True
    if name in ("YEAR", "year", "quarter", "MONTH"):
        if time_clause and "time=" in time_clause:
            return True
    return False


def _fetch_chunk(path, req_get, geo_for, geo_in, time_clause, extra):
    """Fetch one get= request; on geo-default 400 retry without for/in; return
    (status, js, txt)."""
    url = build_url(path, req_get, geo_for, geo_in, time_clause, extra)
    st, js, txt = _req(url)
    if st == 200 and js:
        return st, js, txt
    # geography-as-default datasets reject for=us:* -> retry with no geo clause
    if geo_for and txt and ("not a valid" in txt.lower()
                            or "unknown" in txt.lower()
                            or "geography" in txt.lower()
                            or "valid geography" in txt.lower()):
        url2 = build_url(path, req_get, None, None, time_clause, extra)
        st2, js2, txt2 = _req(url2)
        if st2 == 200 and js2:
            return st2, js2, txt2
        return st2, js2, txt2
    return st, js, txt


def _var_limit(path, txt):
    m = re.search(r"limited to (\d+) variables", txt or "")
    if m:
        lim = int(m.group(1))
        _varlimit_cache[path] = lim
        return lim
    return None


def pull_slice(path, m, keys, geo_for, geo_in, time_clause, extra):
    """Pull one (geo, time) slice, chunking variables; return (header, rows)
    or (None, []) on empty/204. Rows are lists aligned to a unified header.
    Adapts to per-dataset get= variable caps and default-geography rejection."""
    vals = value_columns(m, keys)
    cap = _varlimit_cache.get(path, MAX_GET)

    def make_chunks(cap):
        room = max(1, cap - len(keys))
        if not vals:
            return [list(keys)]
        out = []
        i = 0
        while i < len(vals):
            out.append(list(keys) + vals[i:i + room])
            i += room
        return out

    def fetch(getvars):
        req_get = [g for g in getvars if not _not_in_get(g, time_clause, extra)]
        if not req_get:
            cand = [k for k in keys if not _not_in_get(k, time_clause, extra)]
            req_get = cand[:1] or ["NAME"]
        return _fetch_chunk(path, req_get, geo_for, geo_in, time_clause, extra)

    chunks = make_chunks(cap)
    # First request: also lets us detect a tighter var cap.
    st, js, txt = fetch(chunks[0])
    if "limited to" in (txt or ""):
        newcap = _var_limit(path, txt)
        if newcap and newcap < cap:
            cap = newcap
            chunks = make_chunks(cap)
            st, js, txt = fetch(chunks[0])
    if st == -2:
        return None, [], "timeout"
    if st in (204, 404) or not js:
        return None, [], "empty"

    # SINGLE-CHUNK (the common case): return rows verbatim, NO dedup/merge.
    if len(chunks) == 1:
        return js[0], js[1:], "ok"

    # MULTI-CHUNK: Census returns rows in a stable order for identical predicates,
    # so we splice the extra value columns onto chunk-0 rows by position, after
    # verifying the shared key columns line up row-for-row.
    base_h = js[0]
    base_rows = js[1:]
    header = list(base_h)
    cols = [list(col) for col in zip(*base_rows)] if base_rows else [[] for _ in base_h]
    base_kidx = [base_h.index(k) for k in keys if k in base_h]
    nbase = len(base_rows)
    for getvars in chunks[1:]:
        st2, js2, txt2 = fetch(getvars)
        if not js2 or len(js2) < 2:
            continue
        h2 = js2[0]
        rows2 = js2[1:]
        # alignment check on shared keys (sample); fall back to dict-merge if off
        aligned = len(rows2) == nbase
        if aligned and base_kidx:
            k2idx = [h2.index(k) for k in keys if k in h2 and k in base_h]
            bk = [base_h.index(k) for k in keys if k in h2 and k in base_h]
            for s in range(0, nbase, max(1, nbase // 25 + 1)):
                if tuple(rows2[s][i] for i in k2idx) != tuple(base_rows[s][i] for i in bk):
                    aligned = False
                    break
        new_cols = [c for c in h2 if c not in header]
        if aligned:
            for c in new_cols:
                ci2 = h2.index(c)
                header.append(c)
                cols.append([rows2[r][ci2] for r in range(nbase)])
        else:
            # robust fallback: dict-merge on the full shared-key tuple
            kcommon = [k for k in keys if k in h2 and k in base_h]
            k2idx = [h2.index(k) for k in kcommon]
            idx = {}
            for r in range(nbase):
                idx.setdefault(tuple(base_rows[r][base_h.index(k)] for k in kcommon), []).append(r)
            for c in new_cols:
                header.append(c)
                cols.append([None] * nbase)
            for row in rows2:
                kt = tuple(row[i] for i in k2idx)
                targets = idx.get(kt)
                if not targets:
                    continue
                for c in new_cols:
                    val = row[h2.index(c)]
                    for r in targets:
                        cols[header.index(c)][r] = val
    out_rows = [list(t) for t in zip(*cols)] if cols and cols[0] else base_rows
    return header, out_rows, "ok"


# ---------------------------------------------------------------------------
# Geography plan
# ---------------------------------------------------------------------------
US_STATES = ["01","02","04","05","06","08","09","10","11","12","13","15","16",
             "17","18","19","20","21","22","23","24","25","26","27","28","29",
             "30","31","32","33","34","35","36","37","38","39","40","41","42",
             "44","45","46","47","48","49","50","51","53","54","55","56",
             "60","66","69","72","78"]  # incl DC + territories


# Whether to iterate child geographies that require a state parent (county,
# school district, place, ...). Default OFF: these multiply the crawl by ~56x
# per level and several are multi-dimensional cubes that hang/time out via the
# API. With it OFF we pull the no-parent backbone (us / state / region /
# division / nation / world / country) FULLY; sub-state detail is documented as
# not pulled (Census ships it via separate bulk files). Enable with --substate.
INCLUDE_SUBSTATE = False


def geo_plan(m):
    """Return list of (for_clause, in_clause, label). No-parent levels use :* .
    Child levels needing a state parent are iterated over US_STATES (only when
    INCLUDE_SUBSTATE)."""
    plan = []
    geos = m["geos"]
    if not geos:
        plan.append((None, None, "(none)"))
        return plan
    for name, requires, _ in geos:
        if not requires:
            plan.append((f"{name}:*", None, name))
        elif INCLUDE_SUBSTATE and (requires == ["state"] or requires == "state"):
            for s in US_STATES:
                plan.append((f"{name}:*", f"state:{s}", f"{name}@state{s}"))
        # deeper / multi-parent nests are skipped (need bulk files, not API)
    return plan


INTL_START_YEAR = 1990          # base CTY=- series start (HS ~2002; NAICS ~1990)
INTL_VARIANT_START = 2010       # *export/*import/state/port variants start 2010
INTL_END_YEAR = dt.date.today().year


def _intltrade_plan(m):
    """intltrade is too large to dump per-country/per-commodity via the API
    (HS10 single-quarter world-total already ~0.8M rows). We pull the tractable,
    headline views, chunked PER YEAR to stay under the API's size/time limits:

      * base datasets (have CTY_CODE)  -> CTY_CODE=- (all-countries commodity
        total), one request per year -> the COMPLETE commodity time series
        summed over countries.
      * *export/*import/state*/port* variants (geography levels: world/usitc/
        port) -> for=world:* (world total), one request per year.
    The per-country / per-commodity-detail split is NOT pulled (infeasible via
    API) -- see coverage_note.
    """
    av = set(m["allvars"])
    if "CTY_CODE" in av:
        # base: all-countries commodity total. ~185k rows/yr (76s) -> recent
        # first so a finite budget keeps the most-recent years; older years
        # captured as the budget allows (coverage marked partial if truncated).
        for y in range(INTL_END_YEAR, INTL_START_YEAR - 1, -1):
            tclause = f"time=from+{y}-01+to+{y}-12"
            yield None, None, "CTY-total", tclause, {"CTY_CODE": "-"}
    else:
        # variant: needs a 'for' geography; world is the top-line total. These
        # are huge even at world level -> recent-first so a tight budget still
        # captures the latest data (coverage marked partial).
        for y in range(INTL_END_YEAR, INTL_VARIANT_START - 1, -1):
            tclause = f"time=from+{y}-01+to+{y}-12"
            yield "world:*", None, "world", tclause, {}


def time_plan(m):
    """Return (time_clause, extra_dict_list). For time-based: single from+1900.
    For YEAR/year+quarter: enumerate via the API."""
    path = m["path"]
    if m["has_time"]:
        return [("time=from+%d" % VAR_MIN_YEAR, {})]
    extras = []
    if m["has_YEAR"]:
        years = discover_values(path, "YEAR")
        for y in years:
            extras.append(("", {"YEAR": y}))
        if not extras:
            extras.append(("", {}))
        return extras
    if m["has_year"]:
        years = discover_values(path, "year")
        quarters = discover_values(path, "quarter") or ["1", "2", "3", "4"]
        for y in years:
            for q in quarters:
                extras.append(("", {"year": y, "quarter": q}))
        if not extras:
            extras.append(("", {}))
        return extras
    return [("", {})]


def discover_values(path, var):
    """Discover valid values for a predicate var (e.g. YEAR) via variables.json
    'values' enumeration, else by probing get=<var> at us level."""
    base = "https://api.census.gov/data/" + path
    _, v, _ = _req(base + "/variables.json")
    try:
        item = v["variables"][var]
        vals = item.get("values", {}).get("item")
        if vals:
            return sorted(vals.keys() if isinstance(vals, dict) else vals)
    except Exception:
        pass
    return []


_VARMETA_CACHE = {}


def var_classes(path):
    """From variables.json: (enum_vars, measure_vars). enum_vars have an
    enumerated values list (categorical dimensions); measure_vars have an
    int/float predicateType (continuous measures)."""
    if path in _VARMETA_CACHE:
        return _VARMETA_CACHE[path]
    base = "https://api.census.gov/data/" + path
    _, v, _ = _req(base + "/variables.json")
    enum_vars, measure_vars = set(), set()
    try:
        for k, info in v.get("variables", {}).items():
            pt = str(info.get("predicateType", "")).lower()
            has_enum = bool(info.get("values", {}).get("item"))
            if has_enum:
                enum_vars.add(k)
            elif pt in ("int", "float", "double", "decimal"):
                measure_vars.add(k)
    except Exception:
        pass
    _VARMETA_CACHE[path] = (enum_vars, measure_vars)
    return enum_vars, measure_vars


# ---------------------------------------------------------------------------
# Per-dataset driver
# ---------------------------------------------------------------------------
def safe_name(path):
    return path.replace("timeseries/", "").replace("/", "__")


_TIME_COLS = {"time", "YEAR", "year", "quarter", "MONTH", "obs_date",
              "time_slot_id", "time_slot_date", "time_slot_name"}
_TIME_COLS_U = {c.upper() for c in _TIME_COLS}
# descriptive/derived columns that label or restate a code/value -> not part of
# the series identity (kept as data columns, just excluded from series_key).
_DESC_SUFFIX = ("_NAME", "_LDESC", "_SDESC", "_DESC", "_FORMATTED",
                "_DISTRIBUTION", "_LABEL", "NAME", "_TTL", "_TITLE")
# statistical measure suffixes (estimate, margin of error, bounds, CV, std err)
# -> measures, never part of the series identity.
_MEASURE_SUFFIX = ("_MOE", "_CV", "_LB90", "_UB90", "_LB", "_UB", "_SE",
                   "_PT", "_PCT", "_PI", "_PER", "_PUPIL", "_CHANGE")


def _looks_numeric(s):
    if s is None or s == "":
        return None
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def detect_dim_cols(header, sample_rows, enum_vars=None, measure_vars=None):
    """Classify columns into dimension columns used for series_key.

    A column is a DIMENSION if it is not a time column, not a descriptive label,
    and not a continuous numeric measure. Heuristics, in priority order:
      * geography code columns                       -> dimension
      * variables.json enumerated `values.item`      -> dimension
      * variables.json int/float predicateType       -> measure (skip)
      * sampled mostly-numeric AND high-cardinality  -> measure (skip)
      * otherwise                                    -> dimension
    Low-cardinality numeric columns (e.g. AGECAT 0..5) stay DIMENSIONS."""
    geo_codes = {"us", "state", "county", "region", "division", "place",
                 "GEO_ID", "GEOID", "ucgid", "GEOCOMP", "SUMLEVEL", "GEO_TTL",
                 "NATION", "STATE", "COUNTY", "US"}
    enum_vars = enum_vars or set()       # NOTE: unreliable in Census (estimate
    measure_vars = measure_vars or set()  # cols carry value ranges) -> not forced
    geo_codes_u = {g.upper() for g in geo_codes}
    dims = []
    hidx = {c: i for i, c in enumerate(header)}
    for c in header:
        cu = c.upper()
        if cu in _TIME_COLS_U:
            continue
        if cu in geo_codes_u:
            dims.append(c)
            continue
        if any(cu.endswith(suf) for suf in _DESC_SUFFIX):
            continue
        if any(cu.endswith(suf) for suf in _MEASURE_SUFFIX):
            continue
        if c in measure_vars:        # int/float predicateType => continuous
            continue
        # sample numeric-ness + cardinality
        i = hidx[c]
        num = nonnull = 0
        seen = set()
        for r in sample_rows:
            v = r[i] if i < len(r) else None
            t = _looks_numeric(v)
            if t is None:
                continue
            nonnull += 1
            seen.add(v)
            if t:
                num += 1
        # mostly-numeric AND many distinct values => continuous measure -> skip
        if nonnull >= 8 and num / nonnull > 0.9 and len(seen) > 24:
            continue
        dims.append(c)
    return dims


def series_key_of(row, hidx, dim_cols, path):
    parts = [path.replace("timeseries/", "")]
    for c in dim_cols:
        i = hidx.get(c)
        if i is None:
            continue
        val = row[i] if i < len(row) else None
        if val is not None and val != "":
            parts.append(f"{c}={val}")
    return "|".join(parts)


def parse_obs_date(d):
    if not d:
        return None
    d = str(d).strip()
    try:
        if len(d) == 4 and d.isdigit():
            return dt.date(int(d), 12, 31)
        if "-Q" in d:
            y, q = d.split("-Q"); return dt.date(int(y), (int(q)-1)*3+1, 1)
        if d.count("-") == 1:
            y, mth = d.split("-"); return dt.date(int(y), int(mth), 1)
        if d.count("-") == 2:
            y, mth, day = d.split("-")[:3]
            return dt.date(int(y), int(mth), int(day[:2]))
    except Exception:
        return None
    return None


def time_col_of(header):
    for c in ("time", "YEAR", "year"):
        if c in header:
            return c
    return None


def _subdivide_time(tclause):
    """Split an over-large time slice into smaller pieces so it can be retried.
    Handles both an explicit 'time=from+YYYY-MM+to+YYYY-MM' range and the
    open-ended 'time=from+YYYY[-MM]' form (-> explicit per-year ranges, current
    year inclusive). Returns [] if it can't be subdivided further."""
    # open-ended: time=from+YYYY or time=from+YYYY-MM  -> per-year ranges
    mo = re.fullmatch(r"time=from\+(\d{4})(?:-(\d{2}))?", (tclause or "").strip())
    if mo:
        y0 = int(mo[1]); m0 = int(mo[2]) if mo[2] else 1
        # a slice this large that timed out is high-frequency (monthly) data,
        # which in Census effectively starts ~1990; avoid ~90 empty early-year
        # probes when y0 is our 1900 sentinel.
        if y0 <= 1950:
            y0, m0 = 1990, 1
        y1 = dt.date.today().year
        if y1 <= y0:
            return []
        out = []
        for y in range(y0, y1 + 1):
            sm = m0 if y == y0 else 1
            out.append(f"time=from+{y}-{sm:02d}+to+{y}-12")
        return out if len(out) > 1 else []
    m = re.search(r"time=from\+(\d{4})-(\d{2})\+to\+(\d{4})-(\d{2})", tclause or "")
    if not m:
        return []
    y0, m0, y1, m1 = int(m[1]), int(m[2]), int(m[3]), int(m[4])
    start = y0 * 12 + (m0 - 1)
    end = y1 * 12 + (m1 - 1)
    span = end - start + 1
    if span <= 1:
        return []
    # choose ~3-month chunks; if already <=3 months, go monthly
    step = 3 if span > 3 else 1
    out = []
    s = start
    while s <= end:
        e = min(s + step - 1, end)
        ys, ms = divmod(s, 12)
        ye, me = divmod(e, 12)
        out.append(f"time=from+{ys}-{ms+1:02d}+to+{ye}-{me+1:02d}")
        s = e + 1
    return out if len(out) > 1 else []


# groups whose (geo x all-time) single request is huge (country x demographic
# detail, firm age x size cross-tabs) -> pre-split into per-year requests
# instead of the slow timeout path.
PER_YEAR_GROUPS = {"idb", "bds"}
# groups that reject for=state:* (must enumerate states one by one) -- e.g. QWI.
STATE_ENUM_GROUPS = {"qwi"}

# geography levels that require iterating over a parent (state) and explode the
# crawl on the heaviest datasets. For datasets flagged heavy we keep only the
# no-parent levels (us/state/region/division/nation/...) -> the backbone time
# series; the sub-state detail is left to a separate bulk path (documented).
SUBSTATE_HEAVY_GROUPS = set()
# datasets to pull at the US level ONLY (state+ levels are empty or pathological
# for the cube combination we use) -- documented in coverage notes.
US_ONLY_PATHS = {"timeseries/bds"}

# intltrade HS/port "monster" cubes (HS10 x country x district x month). A single
# month at the all-countries-total is ~0.3-1.1M rows and times out; the full
# detail is shipped by Census via bulk FTP, NOT the API. We SKIP these (each
# would burn the whole budget for 0 usable rows). Documented as not-pulled.
INTL_SKIP_PATHS = {
    "timeseries/intltrade/exports/hs", "timeseries/intltrade/imports/hs",
    "timeseries/intltrade/exports/porths", "timeseries/intltrade/imports/porths",
}


def _years_for(m):
    """Per-year time clauses covering a dataset's actual data extent. Probe the
    time range cheaply (try a geographyless probe, then a us-level probe, since
    some datasets reject a geographyless get=); fall back to a wide window."""
    path = m["path"]
    av = m["allvars"]
    probe_var = next((v for v in av if v not in ("time", "YEAR", "year")), "NAME")
    base = "https://api.census.gov/data/" + path
    yrs = []
    for url in (f"{base}?get={probe_var}&time=from+1900",
                f"{base}?get={probe_var}&for=us:*&time=from+1900",
                f"{base}?get=NAME&for=us:*&time=from+1900"):
        st, js, _ = _req(url, timeout=60)
        if st == 200 and js and "time" in js[0]:
            ti = js[0].index("time")
            yrs = [int(str(r[ti])[:4]) for r in js[1:] if str(r[ti])[:4].isdigit()]
            if yrs:
                break
    if yrs:
        y0, y1 = min(yrs), max(yrs)
    else:
        y0, y1 = 1978, dt.date.today().year
    return [f"time=from+{y}+to+{y}" for y in range(y0, y1 + 1)]


def _qwi_years(m):
    """QWI year range. Probe one state with a bounded wide range for its time
    extent (QWI rejects open-ended from+1900); fall back to 1990..current."""
    path = m["path"]
    yr_now = dt.date.today().year
    url = ("https://api.census.gov/data/" + path +
           f"?get=Emp&for=state:06&time=from+1990+to+{yr_now}")
    st, js, _ = _req(url, timeout=80)
    if st == 200 and js and "time" in js[0]:
        ti = js[0].index("time")
        ys = sorted({int(str(r[ti])[:4]) for r in js[1:] if str(r[ti])[:4].isdigit()})
        if ys:
            return list(range(ys[0], ys[-1] + 1))
    return list(range(1990, yr_now + 1))


def _slice_plan(m, max_child_states=None):
    """Yield (geo_for, geo_in, label, time_clause, extra) work items, applying
    the intltrade special-case (commodity-total, per-year) and child-geo caps."""
    path = m["path"]
    if m["group"] == "intltrade":
        yield from _intltrade_plan(m)
        return
    if m["group"] in STATE_ENUM_GROUPS:
        # for=state:* unsupported -> enumerate states. qwi also requires a BOUNDED
        # time range -> iterate per calendar year (time=YYYY returns its quarters).
        # recent years first (state-inner) so a finite budget keeps recent data
        # across all states (coverage marked partial if truncated).
        years = list(reversed(_qwi_years(m)))
        for y in years:
            for s in US_STATES:
                yield f"state:{s}", None, f"state{s}", f"time={y}", {}
        return
    gplan = geo_plan(m)
    if m["group"] in SUBSTATE_HEAVY_GROUPS:
        # keep only no-parent geo levels (gin is None) -> backbone series
        gplan = [(f, i, lab) for (f, i, lab) in gplan if i is None]
    if m["path"] in US_ONLY_PATHS:
        gplan = [(f, i, lab) for (f, i, lab) in gplan
                 if (f or "").startswith("us")] or [("us:*", None, "us")]
    if max_child_states is not None:
        capped, seen_child = [], 0
        for f, i, lab in gplan:
            if i is not None:
                seen_child += 1
                if seen_child > max_child_states:
                    continue
            capped.append((f, i, lab))
        gplan = capped
    if m["group"] in PER_YEAR_GROUPS and m["has_time"]:
        tclauses = _years_for(m)
        for (gfor, gin, glab) in gplan:
            for tclause in tclauses:
                yield gfor, gin, glab, tclause, {}
        return
    tplan = time_plan(m)
    for (gfor, gin, glab) in gplan:
        for (tclause, extra) in tplan:
            yield gfor, gin, glab, tclause, extra


def ingest_dataset(m, max_child_states=None, budget_s=None):
    """Pull one dataset fully and STREAM it to one Parquet (bounded memory).
    Return (n_rows, n_series, n_calls, truncated). If budget_s is set, stop
    enumerating new slices once the wall-clock budget is exceeded (the rows
    already pulled are kept; truncated=True is reported)."""
    path = m["path"]
    keys = key_columns(m)
    enum_vars, measure_vars = var_classes(path)

    writer = None
    schema = None
    schema_cols = None         # ordered data columns (union_header) frozen on slice 1
    dim_cols = None
    tcol = None
    series = set()
    n = n_calls = 0
    truncated = False
    t_start = time.time()
    outpath = os.path.join(OUT, safe_name(path) + ".parquet")

    class _BudgetHit(Exception):
        pass

    def _over_budget():
        return budget_s is not None and (time.time() - t_start) > budget_s

    def fetch_slice(gfor, gin, tclause, extra, depth=0):
        """Pull one slice; on timeout subdivide the time range and recurse.
        Yields (header, rows) batches. The budget is checked here too so a single
        pathological slice's deep subdivision can't run past the budget."""
        nonlocal n_calls
        if _over_budget():
            raise _BudgetHit()
        header, rows, status = pull_slice(path, m, keys, gfor, gin, tclause, extra)
        n_calls += 1
        if status == "timeout" and depth < 3:
            subs = _subdivide_time(tclause)
            if subs:
                for sub in subs:
                    if _over_budget():
                        raise _BudgetHit()
                    yield from fetch_slice(gfor, gin, sub, extra, depth + 1)
                return
        if status == "timeout":
            print(f"    [timeout, gave up] {path} {gfor} {tclause}", flush=True)
            return
        if rows:
            yield header, rows

    try:
      for (gfor, gin, glab, tclause, extra) in _slice_plan(m, max_child_states):
        if _over_budget():
            truncated = True
            print(f"    [budget {budget_s}s exceeded -> stopping at {n:,} rows]", flush=True)
            break
        for (header, rows) in fetch_slice(gfor, gin, tclause, extra):
            if not rows:
                continue
            if schema_cols is None:
                # dedupe header while preserving order (the API can echo a
                # predicate column that is also a requested var -> duplicate).
                seen_c = set()
                schema_cols = [c for c in header
                               if not (c in seen_c or seen_c.add(c))]
                uhidx = {c: i for i, c in enumerate(schema_cols)}
                dim_cols = detect_dim_cols(schema_cols, rows[:400], enum_vars, measure_vars)
                tcol = time_col_of(schema_cols)
                fields = [pa.field("series_key", pa.string())]
                for c in schema_cols:
                    fields.append(pa.field(c, pa.string()))
                if tcol:
                    fields.append(pa.field("obs_date", pa.date32()))
                schema = pa.schema(fields)
                writer = pq.ParquetWriter(outpath, schema, compression="zstd")
            # realign this slice's rows to frozen schema_cols
            hidx = {c: i for i, c in enumerate(header)}
            src = [hidx.get(c) for c in schema_cols]
            ncol = len(schema_cols)
            col_arrays = [[] for _ in range(ncol)]
            skcol = []
            obs = [] if tcol else None
            tci = schema_cols.index(tcol) if tcol else None
            for r in rows:
                ar = [r[i] if (i is not None and i < len(r)) else None for i in src]
                for ci2 in range(ncol):
                    col_arrays[ci2].append(ar[ci2])
                sk = series_key_of(ar, uhidx, dim_cols, path)
                skcol.append(sk)
                series.add(sk)
                if tcol:
                    obs.append(parse_obs_date(ar[tci]))
            n += len(rows)
            data = {"series_key": pa.array(skcol, type=pa.string())}
            for ci2, c in enumerate(schema_cols):
                data[c] = pa.array(col_arrays[ci2], type=pa.string())
            if tcol:
                data["obs_date"] = pa.array(obs, type=pa.date32())
            writer.write_table(pa.table(data, schema=schema))
    except _BudgetHit:
        truncated = True
        print(f"    [budget {budget_s}s exceeded mid-slice -> stopping at {n:,} rows]",
              flush=True)

    if writer is not None:
        writer.close()
    return n, len(series), n_calls, truncated


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = sys.argv[1:]
    meta = load_meta()
    paths = sorted(meta.keys())

    if "--list" in args:
        ds = load_catalog()
        ts = timeseries_datasets(ds)
        print(f"data.json total datasets: {len(ds)}")
        print(f"timeseries datasets: {len(ts)}")
        groups = {}
        for m in meta.values():
            groups[m["group"]] = groups.get(m["group"], 0) + 1
        for g, c in sorted(groups.items(), key=lambda x: -x[1]):
            print(f"  {g:18} {c}")
        return

    if "--only" in args:
        paths = [args[args.index("--only") + 1]]
    if "--group" in args:
        grp = args[args.index("--group") + 1]
        paths = [p for p in paths if meta[p]["group"] == grp]
    if "--match" in args:
        sub = args[args.index("--match") + 1]
        paths = [p for p in paths if sub in p]
    cap = None
    if "--capstates" in args:
        cap = int(args[args.index("--capstates") + 1])
    if "--substate" in args:
        globals()["INCLUDE_SUBSTATE"] = True
    # per-dataset wall-clock budgets (seconds). intltrade detail is effectively
    # unbounded via the API; budgets keep the crawl polite + finite.
    budget = 1800            # ordinary datasets
    if "--budget" in args:
        budget = int(args[args.index("--budget") + 1])
    intl_base_budget = 2400  # intltrade base (CTY=- commodity totals, per year)
    if "--intlbudget" in args:
        intl_base_budget = int(args[args.index("--intlbudget") + 1])
    intl_var_budget = 300    # intltrade *export/*import/state/port variants:
    if "--intlvarbudget" in args:  # world-level, recent window only (partial)
        intl_var_budget = int(args[args.index("--intlvarbudget") + 1])
    skip_existing = "--resume" in args

    total_rows = total_series = total_calls = 0
    done = 0
    manifest = []
    mpath = os.path.join(OUT, "_manifest.json")
    if os.path.exists(mpath):
        try:
            raw_m = json.load(open(mpath))
            # manifest may be a list of records OR a summary dict; handle both
            if isinstance(raw_m, list):
                manifest = raw_m
            # if it's a dict (summary), leave manifest as empty list
        except Exception:
            manifest = []
    manifest = [r for r in manifest if isinstance(r, dict) and r.get("path") in set(paths)]

    def parquet_ok(p):
        fn = os.path.join(OUT, safe_name(p) + ".parquet")
        if not os.path.exists(fn):
            return False
        try:
            return pq.read_metadata(fn).num_rows > 0
        except Exception:
            return False

    done_paths = {p for p in paths if parquet_ok(p)}
    allow_skipped = "--include-monsters" in args
    for p in paths:
        if skip_existing and p in done_paths:
            print(f"[skip] {p} (parquet present)", flush=True)
            continue
        if p in INTL_SKIP_PATHS and not allow_skipped:
            print(f"[skip] {p} (HS/port monster -- not API-extractable, see notes)",
                  flush=True)
            manifest.append({"path": p, "rows": 0, "skipped_monster": True})
            json.dump(manifest, open(os.path.join(OUT, "_manifest.json"), "w"), indent=1)
            continue
        m = meta[p]
        if m["group"] == "intltrade":
            b = intl_base_budget if "CTY_CODE" in m["allvars"] else intl_var_budget
        else:
            b = budget
        t0 = time.time()
        try:
            nrows, nser, ncalls, trunc = ingest_dataset(m, max_child_states=cap, budget_s=b)
        except Exception as e:  # noqa: BLE001
            print(f"  !! {p}: ERROR {e!r}", flush=True)
            manifest.append({"path": p, "error": repr(e)})
            json.dump(manifest, open(os.path.join(OUT, "_manifest.json"), "w"), indent=1)
            continue
        total_rows += nrows; total_series += nser; total_calls += ncalls
        done += 1
        print(f"[{done}/{len(paths)}] {p:46} rows={nrows:>9,} series={nser:>8,} "
              f"calls={ncalls:>4} {'TRUNC ' if trunc else ''}{time.time()-t0:5.1f}s", flush=True)
        manifest.append({"path": p, "rows": nrows, "series": nser,
                         "calls": ncalls, "truncated": trunc})
        json.dump(manifest, open(os.path.join(OUT, "_manifest.json"), "w"), indent=1)
    print(f"\nDONE: {done} datasets / {total_rows:,} rows / {total_series:,} series "
          f"/ {total_calls:,} API calls", flush=True)


if __name__ == "__main__":
    main()
