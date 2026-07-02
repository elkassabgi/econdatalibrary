#!/usr/bin/env python3
"""FULL-HISTORY ingest of the Census intltrade time-series datasets.

Re-pulls every intltrade dataset with COMPLETE history (no recency truncation),
overwriting the recency-truncated files left by the generic connector run, and
adds the missing HS-commodity detail (hs / statehs / statenaics) plus the BETA
*export/*import country variants.

Grain (documented, anti-bloat):
  * base datasets (no `for` geography, have CTY_CODE)
        -> CTY_CODE=-  (all-country commodity total), the canonical commodity
           time series.  Pulled per-half-year 2010..now.
  * state* base datasets (have STATE)
        -> STATE=* & CTY_CODE=- (per-state commodity total). statehs is huge
           (~96 MB/yr) so it is pulled per-MONTH; statenaics per-half-year.
  * BETA *export/*import variants (geography world/usitc...)
        -> for=world:*  (the headline world total; the BETA twin of the base).
           Country/region detail of the BETA endpoints is NOT exhaustively
           pulled (~230 countries x 12k HS x 192 months = many GB, BETA-quality
           duplicate geography) -- see coverage notes.

ONE grouped Parquet per dataset under data/clean_full/census/, columns:
  series_key | <dimension+measure columns, all strings> | obs_date(date32)

Usage:
  python jobs/_census_intltrade_full.py --list
  python jobs/_census_intltrade_full.py --only timeseries/intltrade/imports/hs
  python jobs/_census_intltrade_full.py --base          # 18 base+state datasets
  python jobs/_census_intltrade_full.py --beta          # 18 BETA world-level
  python jobs/_census_intltrade_full.py                 # everything
"""
import datetime as dt
import json
import os
import sys
import time
import urllib.request
import urllib.error
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
RAW = os.path.join(ROOT, "data", "raw", "census")
OUT = os.path.join(ROOT, "data", "clean_full", "census")
os.makedirs(OUT, exist_ok=True)

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
START_YEAR = 2010
END = dt.date.today()
MAXVARS = 45            # under the API 50-var cap, leaves room for predicates

_KEY = None
def key():
    global _KEY
    if _KEY is None:
        for line in open(os.path.join(ROOT, ".env"), encoding="utf-8"):
            if line.startswith("CENSUS_API_KEY="):
                _KEY = line.split("=", 1)[1].strip()
    if not _KEY:
        raise SystemExit("missing CENSUS_API_KEY")
    return _KEY

# NOTE: we deliberately do NOT reuse a keep-alive requests.Session here.
# Long/large Census responses left pooled keep-alive sockets half-open on
# Windows; subsequent reads then hung for minutes (read timeout not honored on
# a stale socket) and stalled every worker at once -- while a brand-new
# connection still returned in <1s. Each request now opens a FRESH connection
# with `Connection: close` and a real per-read timeout, so a slow/dead socket
# trips the read timeout (CONNECT, READ) instead of hanging.
# urllib (NOT requests) -- in this Windows environment requests' streamed reads
# silently hung for many minutes on slow Census responses (the socket read
# timeout never fired), stalling workers; urllib.urlopen(timeout=) fires a clean
# socket timeout every time. We read the whole body in one urlopen call whose
# socket timeout doubles as the per-slice deadline.
def _get(url, deadline=240):
    """One-shot GET via urllib with a hard socket timeout = `deadline`. Returns
    (status, json_or_None, text). status -2 == timeout; -1 == network error."""
    req = urllib.request.Request(url, headers=dict(UA))
    try:
        r = urllib.request.urlopen(req, timeout=deadline)
        body = r.read()
        status = r.status
    except urllib.error.HTTPError as e:
        try:
            txt = e.read().decode("utf-8", "replace")
        except Exception:
            txt = ""
        return e.code, None, txt[:300]
    except (TimeoutError, OSError):
        return -2, None, "timeout"
    except Exception as e:  # noqa: BLE001
        return -1, None, repr(e)
    if status == 204:
        return 204, None, ""
    text = body.decode("utf-8", "replace")
    if status == 200:
        try:
            return 200, json.loads(text), text
        except Exception:
            return 200, None, text
    return status, None, text[:300]


def req(url, tries=5, deadline=240):
    last = ""
    deadline_hits = 0
    for i in range(tries):
        st, js, txt = _get(url, deadline=deadline)
        if st == 200:
            return 200, js, txt
        if st == 204:
            return 204, None, ""
        if st == -2:
            # could be a transient read-timeout on a fresh socket OR a genuinely
            # too-large slice. Retry once on a new connection; if it deadlines
            # again, hand to the caller to subdivide the time range.
            deadline_hits += 1
            if deadline_hits >= 2:
                return -2, None, "deadline"
            time.sleep(1.0); continue
        if st == 500:
            # Census 500s on some over-large enumerations; a SMALLER time window
            # often succeeds -> after a couple retries, signal "subdivide" (-2)
            # rather than giving up. (A few endpoints 500 at every window; those
            # bottom out at per-month and are reported as timeouts/empties.)
            last = f"500:{str(txt)[:80]}"
            if i >= 1:
                return -2, None, last
            time.sleep(2.0 * (i + 1)); continue
        if st in (502, 503, 504, -1):
            last = f"{st}:{str(txt)[:80]}"
            time.sleep(2.0 * (i + 1)); continue
        return st, None, str(txt)[:200]      # 400/404 = no retry
    return -2 if deadline_hits else -1, None, last


META = {m["path"]: m for m in json.load(open(os.path.join(RAW, "ts_meta.json"), encoding="utf-8"))}

# statehs keys geography on 2-letter STATE abbreviations (NOT FIPS). 50 states +
# DC + PR + territories (the values the API actually serves).
STATE_ABBR = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",
              "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
              "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
              "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX",
              "UT", "VT", "VA", "WA", "WV", "WI", "WY", "PR", "VI"]


# ---------------------------------------------------------------------------
# Column selection: commodity/dim columns + a comprehensive measure set, kept
# to <=MAXVARS so each request is a single chunk (no lossy multi-chunk merge).
# ---------------------------------------------------------------------------
DIM_PRIORITY = ["I_COMMODITY", "E_COMMODITY", "NAICS", "I_ENDUSE", "E_ENDUSE",
                "SITC", "USDA", "HITECH", "STATE", "DISTRICT", "DF",
                "SUMMARY_LVL", "SUMMARY_LVL2", "COMM_LVL", "CTY_CODE"]
DESC_COLS = ["I_COMMODITY_SDESC", "E_COMMODITY_SDESC", "NAICS_SDESC",
             "I_ENDUSE_SDESC", "E_ENDUSE_SDESC", "SITC_SDESC",
             "I_COMMODITY_LDESC", "E_COMMODITY_LDESC", "DIST_NAME", "CTY_NAME"]
# headline measures present across most intltrade datasets (monthly + ytd)
MEAS_PRIORITY = [
    "GEN_VAL_MO", "GEN_VAL_YR", "CON_VAL_MO", "CON_VAL_YR",   # imports value
    "ALL_VAL_MO", "ALL_VAL_YR",                               # exports value
    "GEN_CIF_MO", "GEN_CIF_YR", "CON_CIF_MO", "CON_CIF_YR",   # CIF
    "AIR_VAL_MO", "AIR_VAL_YR", "VES_VAL_MO", "VES_VAL_YR",   # mode value
    "CNT_VAL_MO", "CNT_VAL_YR",                               # containerized
    "AIR_WGT_MO", "AIR_WGT_YR", "VES_WGT_MO", "VES_WGT_YR",   # weight
    "CNT_WGT_MO", "CNT_WGT_YR",
    "DUT_VAL_MO", "DUT_VAL_YR", "CAL_DUT_MO", "CAL_DUT_YR",   # duties
    "GEN_QY1_MO", "GEN_QY1_YR", "CON_QY1_MO", "CON_QY1_YR",   # quantity
    "UNIT_QY1", "LAST_UPDATE", "MONTH",
]


# Customs-DISTRICT detail crosses every commodity with ~45 districts -> a single
# month of HS is ~36 MB / 120 s and stalls the API; even smaller commodity
# universes (NAICS/SITC) hit ~90 s/yr and contend badly. The value the task
# targets is the full COMMODITY history (HS / NAICS / SITC / end-use codes),
# which lives at the NATIONAL (DISTRICT=-) level and pulls in ~10-50 s/yr with
# no stalls. So every base dataset that HAS a DISTRICT dimension is pulled
# national-only (DISTRICT=- predicate, DISTRICT column dropped). Customs-district
# detail is documented as not pulled via the API (Census ships it via bulk FTP).
# (A handful of already-present files retain bonus district detail from an
#  earlier run; that is harmless.)
def national_only(path):
    # plain base dataset (no `for` geography) that carries a DISTRICT axis
    return "DISTRICT" in set(META[path]["allvars"]) and not META[path]["geos"]


def drop_district(path):
    # We never want the 45x customs-district fan-out (bloat + API stalls). Drop
    # it for ANY intltrade dataset that has it -- base (national-only) AND the
    # BETA world variants (kept at the world aggregate, not country detail).
    return "DISTRICT" in set(META[path]["allvars"])


# BETA world variants are supplementary (the world-aggregate twin of the base);
# we keep them LEAN -- commodity dim + summary level + the two headline value
# measures only -- so they stay small/fast and don't stall.
BETA_MEAS = ["GEN_VAL_MO", "GEN_VAL_YR", "ALL_VAL_MO", "ALL_VAL_YR",
             "CON_VAL_MO", "CON_VAL_YR"]


def select_columns(path):
    """Return (dim_cols_for_series_key, get_vars) for a dataset, <=MAXVARS."""
    av = set(META[path]["allvars"])
    is_beta = bool(META[path]["geos"])
    drop = {"DISTRICT", "DIST_NAME"} if drop_district(path) else set()
    # HS / statehs cross ~24k (or ~97 HS2) commodities x many periods; the
    # repeated description text dominates file size -> drop long AND short
    # descriptions (the HS code in I_/E_COMMODITY is the key; descriptions are a
    # static lookup) plus CTY_NAME.
    if path in HS_PER_MONTH or "statehs" in path:
        drop |= {"I_COMMODITY_LDESC", "E_COMMODITY_LDESC",
                 "I_COMMODITY_SDESC", "E_COMMODITY_SDESC", "CTY_NAME"}
    dims = [c for c in DIM_PRIORITY if c in av and c != "CTY_CODE" and c not in drop]
    meas_src = BETA_MEAS if is_beta else MEAS_PRIORITY
    desc = [] if is_beta else [c for c in DESC_COLS if c in av and c not in drop]
    meas = [c for c in meas_src if c in av]
    getv = []
    for c in dims + desc + meas:
        if c not in getv:
            getv.append(c)
    getv = getv[:MAXVARS]
    # series_key dims = identifier/categorical dim columns (not desc, not measure)
    skdims = [c for c in dims if c in getv]
    return skdims, getv


# ---------------------------------------------------------------------------
# Time slicing
# ---------------------------------------------------------------------------
def months(y0, m0, y1, m1):
    a = y0 * 12 + (m0 - 1); b = y1 * 12 + (m1 - 1)
    out = []
    while a <= b:
        out.append((a // 12, a % 12 + 1)); a += 1
    return out


def year_clauses():
    # API cost is dominated by commodity enumeration, not the time span, so a
    # full calendar year per request is far more efficient than monthly while
    # still staying under the per-request deadline. Recent year first so a
    # truncated run keeps the most recent data.
    return [f"time=from+{y}-01+to+{y}-12" for y in range(END.year, START_YEAR - 1, -1)]


def half_year_clauses():
    out = []
    for y in range(START_YEAR, END.year + 1):
        out.append(f"time=from+{y}-01+to+{y}-06")
        out.append(f"time=from+{y}-07+to+{y}-12")
    return out


def quarter_clauses():
    out = []
    for y in range(START_YEAR, END.year + 1):
        for q in range(4):
            m0 = q * 3 + 1
            out.append(f"time=from+{y}-{m0:02d}+to+{y}-{m0+2:02d}")
    return out


def month_clauses():
    # cap at the current month -> no wasted probes on not-yet-existing months
    out = []
    for (y, m) in months(START_YEAR, 1, END.year, END.month):
        out.append(f"time=from+{y}-{m:02d}+to+{y}-{m:02d}")
    return out


def parse_obs_date(d):
    if not d:
        return None
    d = str(d).strip()
    try:
        if len(d) == 7 and d[4] == "-":
            return dt.date(int(d[:4]), int(d[5:7]), 1)
        if len(d) == 4 and d.isdigit():
            return dt.date(int(d), 12, 31)
        if len(d) >= 10 and d[4] == "-" and d[7] == "-":
            return dt.date(int(d[:4]), int(d[5:7]), int(d[8:10]))
    except Exception:
        return None
    return None


def build_url(path, getv, geo_for, cty_total, state_star, tclause, extra=None):
    # a predicate supplied in `extra` (e.g. NAICS=11) is echoed back as a column
    # automatically -> don't also request it in get= (would duplicate).
    if extra:
        getv = [g for g in getv if g not in extra]
    g = quote(",".join(getv), safe=",")
    u = f"https://api.census.gov/data/{path}?get={g}"
    if geo_for:
        u += "&for=" + quote(geo_for, safe=":*")
    if cty_total:
        u += "&CTY_CODE=-"
    if state_star:
        u += "&STATE=*"
    for kk, vv in (extra or {}).items():
        u += f"&{kk}=" + quote(str(vv), safe="*-/")
    if tclause:
        u += "&" + tclause
    u += "&key=" + key()
    return u


# ---------------------------------------------------------------------------
# Per-dataset pull
# ---------------------------------------------------------------------------
# Endpoints that 500 server-side when asked to ENUMERATE the whole commodity
# axis at CTY_CODE=- (any time window). We instead iterate the commodity code as
# a PREDICATE (which the API serves fast), looping the harvested code universe.
def _naics_codes():
    p = os.path.join(RAW, "naics_codes.json")
    return json.load(open(p)) if os.path.exists(p) else []


CODE_ITER = {
    # exports/naics: full-commodity enumeration 500s at every window -> per code.
    "timeseries/intltrade/exports/naics": ("NAICS", _naics_codes),
}

# HS-commodity datasets have ~12k codes; even national (DISTRICT=-) a full-year
# response (~17 MB) sometimes trickles/stalls server-side (minutes), which wastes
# the deadline before subdividing. We request these PER MONTH (~2 MB / ~10 s),
# which is reliable. Recent month first so a partial run keeps recent data.
HS_PER_MONTH = {
    "timeseries/intltrade/imports/hs", "timeseries/intltrade/exports/hs",
}


def month_clauses_recent_first():
    out = month_clauses()
    out.reverse()
    return out


def plan(path):
    """Yield (geo_for, cty_total, state_star, tclause, extra) work items."""
    m = META[path]
    av = set(m["allvars"])
    geos = [g[0] for g in m["geos"]]
    is_state = "STATE" in av and not geos     # base state* dataset
    is_beta = bool(geos)                       # *export/*import variant
    if path in CODE_ITER:
        var, getter = CODE_ITER[path]
        codes = getter()
        base_extra = {"DISTRICT": "-"} if national_only(path) else {}
        tc = "time=from+%d" % START_YEAR
        # all-commodity total first, then each code, full history per request.
        yield None, True, False, tc, {**base_extra, var: "-"}
        for c in codes:
            yield None, True, False, tc, {**base_extra, var: c}
        return
    if is_beta:
        # world aggregate, per year, DISTRICT collapsed (the BETA twin of the
        # base world-total commodity series; country/region detail not pulled).
        extra = {"DISTRICT": "-"} if drop_district(path) else None
        for tc in year_clauses():
            yield "world:*", False, False, tc, extra
    elif is_state:
        leaf = path.rsplit("/", 1)[-1]
        if "statehs" in leaf:
            # STATE x full HS10 (~24k codes) is a true monster (250M+ rows, tens
            # of GB, many hours -- bulk-FTP territory). We capture the tractable,
            # analytically useful grain: STATE x HS2 chapter (COMM_LVL=HS2, ~97
            # chapters), CTY_CODE=- (all-country state totals), per STATE x YEAR
            # (~60 KB / ~4 s each; the open-ended per-state request trickles for
            # minutes, the per-year one is fast). HS4/HS6 state detail not pulled.
            for st in STATE_ABBR:
                for tc in year_clauses():
                    yield None, True, False, tc, {"STATE": st, "COMM_LVL": "HS2"}
        else:                                   # statenaics ~3.7 MB/yr -> per year
            for tc in year_clauses():
                yield None, True, True, tc, None
    else:                                       # plain base -> CTY=- per year
        extra = {"DISTRICT": "-"} if national_only(path) else None
        # per YEAR (urllib gives clean timeouts; a slow HS year subdivides to
        # months via the fetch fallback). Far fewer requests than per-month.
        for tc in year_clauses():
            yield None, True, False, tc, extra


def series_key(path, dimcols, hidx, row):
    leaf = path.replace("timeseries/", "")
    parts = [leaf]
    for c in dimcols:
        i = hidx.get(c)
        if i is None:
            continue
        v = row[i] if i < len(row) else None
        if v not in (None, ""):
            parts.append(f"{c}={v}")
    return "|".join(parts)


def safe_name(path):
    return path.replace("timeseries/", "").replace("/", "__")


def ingest(path, budget_s=None):
    skdims, getv = select_columns(path)
    outpath = os.path.join(OUT, safe_name(path) + ".parquet")
    tmp = outpath + ".tmp"
    writer = None; schema = None; cols0 = None; tci = None
    n = 0; ncalls = 0; series = set(); empties = 0
    t0 = time.time()
    timeouts = []

    def over():
        return budget_s is not None and (time.time() - t0) > budget_s

    def fetch(gf, cty, st, tc, extra, depth=0):
        nonlocal ncalls
        url = build_url(path, getv, gf, cty, st, tc, extra)
        if os.environ.get("CENSUS_DEBUG"):
            print(f"      REQ {tc} {extra or ''} d{depth} ...", flush=True)
            _t = time.time()
        code, js, txt = req(url)
        ncalls += 1
        if os.environ.get("CENSUS_DEBUG"):
            print(f"      -> code={code} rows={len(js)-1 if js else 0} "
                  f"{time.time()-_t:.1f}s", flush=True)
        if code == -2 and depth < 6:
            # subdivide the time clause: range -> halves, recursively to months
            subs = subdivide(tc)
            if subs:
                for s in subs:
                    if over():
                        return
                    yield from fetch(gf, cty, st, s, extra, depth + 1)
                return
        if code == -2:
            timeouts.append((tc, extra)); return
        if code in (204, 404, 400) or not js or len(js) < 2:
            return
        yield js[0], js[1:]

    for (gf, cty, st, tc, extra) in plan(path):
        if over():
            print(f"    [budget hit at {n:,} rows]", flush=True); break
        for header, rows in fetch(gf, cty, st, tc, extra):
            if not rows:
                empties += 1; continue
            if cols0 is None:
                seen = set()
                cols0 = [c for c in header if not (c in seen or seen.add(c))]
                hidx0 = {c: i for i, c in enumerate(cols0)}
                tcol = "time" if "time" in cols0 else None
                fields = [pa.field("series_key", pa.string())]
                fields += [pa.field(c, pa.string()) for c in cols0]
                if tcol:
                    fields.append(pa.field("obs_date", pa.date32()))
                schema = pa.schema(fields)
                writer = pq.ParquetWriter(tmp, schema, compression="zstd")
                tci = cols0.index(tcol) if tcol else None
                dimset = [c for c in skdims if c in hidx0]
            hidx = {c: i for i, c in enumerate(header)}
            src = [hidx.get(c) for c in cols0]
            ncol = len(cols0)
            carr = [[] for _ in range(ncol)]
            sk = []; obs = [] if tci is not None else None
            for r in rows:
                ar = [r[i] if (i is not None and i < len(r)) else None for i in src]
                for ci in range(ncol):
                    carr[ci].append(ar[ci])
                k = series_key(path, dimset, hidx0, ar)
                sk.append(k); series.add(k)
                if tci is not None:
                    obs.append(parse_obs_date(ar[tci]))
            data = {"series_key": pa.array(sk, type=pa.string())}
            for ci, c in enumerate(cols0):
                data[c] = pa.array(carr[ci], type=pa.string())
            if tci is not None:
                data["obs_date"] = pa.array(obs, type=pa.date32())
            writer.write_table(pa.table(data, schema=schema))
            n += len(rows)

    if writer is not None:
        writer.close()
        os.replace(tmp, outpath)
    else:
        # nothing written; leave any prior file intact only if we wrote nothing
        if os.path.exists(tmp):
            os.remove(tmp)
    return n, len(series), ncalls, timeouts


def subdivide(tc):
    import re
    # open-ended "time=from+YYYY" -> make it an explicit range to current month
    mo = re.fullmatch(r"time=from\+(\d{4})(?:-(\d{2}))?", (tc or "").strip())
    if mo:
        y0 = int(mo[1]); m0 = int(mo[2]) if mo[2] else 1
        tc = f"time=from+{y0}-{m0:02d}+to+{END.year}-{END.month:02d}"
    m = re.search(r"time=from\+(\d{4})-(\d{2})\+to\+(\d{4})-(\d{2})", tc or "")
    if not m:
        return []
    y0, m0, y1, m1 = int(m[1]), int(m[2]), int(m[3]), int(m[4])
    a = y0 * 12 + (m0 - 1); b = y1 * 12 + (m1 - 1)
    span = b - a + 1
    if span <= 1:
        return []
    mid = a + span // 2
    def fmt(x0, x1):
        ya, ma = divmod(x0, 12); yb, mb = divmod(x1, 12)
        return f"time=from+{ya}-{ma+1:02d}+to+{yb}-{mb+1:02d}"
    return [fmt(a, mid - 1), fmt(mid, b)]


def main():
    args = sys.argv[1:]
    intl = sorted(p for p in META if META[p]["group"] == "intltrade")
    base = [p for p in intl if not META[p]["geos"]]
    beta = [p for p in intl if META[p]["geos"]]

    if "--list" in args:
        print(f"intltrade total: {len(intl)}  base+state: {len(base)}  beta: {len(beta)}")
        for p in base:
            d, g = select_columns(p)
            print(f"  BASE {p[10:]:24} skdims={d} getvars={len(g)}")
        for p in beta:
            print(f"  BETA {p[10:]:24}")
        return

    # Only a subset of the BETA *export/*import world variants are tractable at
    # the world aggregate. The HS, porths and NAICS variants enumerate a huge
    # commodity axis and time out server-side (the same wall the base HS/exports-
    # NAICS hit); the state* variants reject the world+DISTRICT predicate (they
    # key on STATE, not DISTRICT). All of those duplicate base series we already
    # capture, so we pull only the clean classifications (end-use, hi-tech, SITC,
    # USDA) at world level; the rest are documented as not-pulled.
    BETA_OK = {
        "timeseries/intltrade/exports/enduseexport",
        "timeseries/intltrade/imports/enduseimport",
        "timeseries/intltrade/exports/hitechexport",
        "timeseries/intltrade/imports/hitechimport",
        "timeseries/intltrade/exports/sitcexport",
        "timeseries/intltrade/imports/sitcimport",
        "timeseries/intltrade/exports/usdaexport",
        "timeseries/intltrade/imports/usdaimport",
    }

    paths = intl
    if "--base" in args:
        paths = base
    if "--beta" in args:
        paths = [p for p in beta if p in BETA_OK]
    if "--only" in args:
        paths = [args[args.index("--only") + 1]]
    if "--match" in args:
        sub = args[args.index("--match") + 1]
        paths = [p for p in paths if sub in p]

    # budgets (seconds). HS datasets run PER MONTH (~204 slices) and must run with
    # NO other concurrent census job (concurrent large responses make month-slices
    # stall and get dropped) -> generous budget so all 2010..now months complete.
    BASE_BUDGET = 1800
    HS_BUDGET = 5400
    STATE_BUDGET = 9000
    BETA_BUDGET = 1500
    if "--budget" in args:
        BASE_BUDGET = STATE_BUDGET = BETA_BUDGET = HS_BUDGET = int(args[args.index("--budget") + 1])

    tot = 0
    for i, p in enumerate(paths, 1):
        m = META[p]
        if m["geos"]:
            b = BETA_BUDGET
        elif "STATE" in set(m["allvars"]):
            b = STATE_BUDGET
        elif p in HS_PER_MONTH:
            b = HS_BUDGET
        else:
            b = BASE_BUDGET
        t0 = time.time()
        try:
            n, ns, nc, tos = ingest(p, budget_s=b)
        except Exception as e:  # noqa: BLE001
            print(f"[{i}/{len(paths)}] {p} ERROR {e!r}", flush=True)
            continue
        tot += n
        extra = f" timeouts={len(tos)}" if tos else ""
        print(f"[{i}/{len(paths)}] {p[10:]:26} rows={n:>9,} series={ns:>7,} "
              f"calls={nc:>4} {time.time()-t0:6.0f}s{extra}", flush=True)
    print(f"\nTOTAL rows written this run: {tot:,}", flush=True)


if __name__ == "__main__":
    main()
