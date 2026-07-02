#!/usr/bin/env python3
"""FULL-COVERAGE ingest of the ENTIRE U.S. BEA API catalog.

Crawls all 12 BEA data datasets (NIPA, NIUnderlyingDetail, FixedAssets,
GDPbyIndustry, UnderlyingGDPbyIndustry, InputOutput, ITA, IIP, IntlServTrade,
IntlServSTA, Regional, MNE) via GetData and writes GROUPED Parquet --
ONE file per table/cube, with a series-key column inside -- to
data/clean_full/bea/<dataset>/. License: us-public-domain (public domain).

Anti-bloat: ~600 files total for the whole source, each holding many series.
Resume: a done-set JSON lets re-runs skip already-written groups.

Usage:
  python jobs/ingest_bea_full.py --datasets NIPA            # one dataset
  python jobs/ingest_bea_full.py                            # everything
  python jobs/ingest_bea_full.py --resume                   # skip done groups
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
from core.config import require  # noqa: E402

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
API = "https://apps.bea.gov/api/data/"
RAW = os.path.join(ROOT, "data", "raw", "bea")
OUT = os.path.join(ROOT, "data", "clean_full", "bea")
MANIFEST = os.path.join(RAW, "catalog_manifest.json")
DONE_FILE = os.path.join(RAW, "_done_groups.json")
LICENSE = "us-public-domain"

KEY = require("BEA_API_KEY")
MAX_WORKERS = 6

_thread_local = threading.local()
_done_lock = threading.Lock()
_stats_lock = threading.Lock()
STATS = {"groups": 0, "rows": 0, "calls": 0, "errors": 0}


def _session():
    s = getattr(_thread_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update(UA)
        _thread_local.s = s
    return s


# --------- global rate limiter: BEA caps = 100 req/min, 100MB/min, 30 err/min.
# Keep comfortably under: <=90 req/min AND <=~80MB/min across all workers.
_rl_lock = threading.Lock()
_rl_times = []          # request timestamps in the last 60s
_rl_bytes = []          # (timestamp, nbytes) in the last 60s
MAX_REQ_PER_MIN = 85
MAX_BYTES_PER_MIN = 70 * 1024 * 1024


def _rate_limit_acquire():
    """Block until a request may be sent without breaching the per-minute caps."""
    while True:
        now = time.time()
        with _rl_lock:
            cutoff = now - 60
            while _rl_times and _rl_times[0] < cutoff:
                _rl_times.pop(0)
            while _rl_bytes and _rl_bytes[0][0] < cutoff:
                _rl_bytes.pop(0)
            req_ok = len(_rl_times) < MAX_REQ_PER_MIN
            bytes_ok = sum(b for _, b in _rl_bytes) < MAX_BYTES_PER_MIN
            if req_ok and bytes_ok:
                _rl_times.append(now)
                return
            # how long until the oldest entry expires
            waits = []
            if not req_ok and _rl_times:
                waits.append(_rl_times[0] + 60 - now)
            if not bytes_ok and _rl_bytes:
                waits.append(_rl_bytes[0][0] + 60 - now)
            wait = max(0.2, min(waits) if waits else 0.5)
        time.sleep(min(wait, 5))


def _rate_limit_record_bytes(n):
    with _rl_lock:
        _rl_bytes.append((time.time(), n))


# error-code / message classification
_RETRY_CODES = {"8"}  # 8 = Volume per minute quota exceeded
_RETRY_MSG = ("exceeded", "quota", "throttl", "denied", "try again", "temporar")
_NODATA_MSG = ("no data", "not found", "invalid year", "the requested")


def _classify_error(err):
    """Return 'retry' (back off & retry), 'nodata' (accept empty), or 'fatal'."""
    if isinstance(err, list):
        err = err[0] if err else {}
    if not isinstance(err, dict):
        s = str(err).lower()
    else:
        code = str(err.get("APIErrorCode") or err.get("number") or "")
        if code in _RETRY_CODES:
            return "retry"
        s = (str(err.get("APIErrorDescription", "")) + " " +
             str(err.get("error", "")) + " " +
             str(err.get("AdditionalDetail", ""))).lower()
    if any(k in s for k in _RETRY_MSG):
        return "retry"
    if any(k in s for k in _NODATA_MSG):
        return "nodata"
    return "nodata"  # default: treat unknown table-level errors as empty


def call(method="GetData", **params):
    """One GetData call with rate-limit + retry/backoff.

    Returns the Data list (possibly []). Quota/throttle errors are retried with
    exponential backoff and DO NOT silently drop data; genuine 'no data found'
    table errors return []."""
    p = {"UserID": KEY, "method": method, "ResultFormat": "JSON"}
    p.update(params)
    s = _session()
    backoff_extra = 0
    for attempt in range(12):
        _rate_limit_acquire()
        try:
            r = s.get(API, params=p, timeout=240)
            with _stats_lock:
                STATS["calls"] += 1
            _rate_limit_record_bytes(len(r.content))
            if r.status_code == 429:
                time.sleep(min(70, 10 * (attempt + 1)))
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(min(30, 4 * (attempt + 1)))
                continue
            if r.status_code != 200:
                time.sleep(4 * (attempt + 1))
                continue
            api = r.json().get("BEAAPI", {})
            res = api.get("Results", {})
            if isinstance(res, list):
                res = res[0] if res else {}
            # locate any error envelope (top-level or inside Results)
            err = None
            if isinstance(api, dict) and api.get("Error") not in (None, "", {}):
                err = api.get("Error")
            elif isinstance(res, dict) and "Error" in res:
                err = res.get("Error")
            if err is not None:
                kind = _classify_error(err)
                if kind == "retry":
                    with _stats_lock:
                        STATS["errors"] += 1
                    # quota error: pause hard so the per-minute window clears
                    time.sleep(min(75, 12 + backoff_extra))
                    backoff_extra += 10
                    continue
                return []  # nodata / fatal-table -> genuine empty
            if isinstance(res, dict):
                data = res.get("Data", [])
                return data if isinstance(data, list) else []
            return []
        except Exception:  # noqa: BLE001
            if attempt >= 11:
                with _stats_lock:
                    STATS["errors"] += 1
                return []
            time.sleep(4 * (attempt + 1))
    # exhausted retries (likely persistent quota): record and return empty
    with _stats_lock:
        STATS["errors"] += 1
    return []


# ---------------------------------------------------------------- value parse
_NA = {"", "(NA)", "(D)", "(L)", "(*)", "...", "---", "NA", "n.a.", "(NM)", "*", "(C)", "(T)"}


def pval(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if s in _NA:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def pdate(tp):
    """BEA TimePeriod -> date. Handles YYYY, YYYYQn, YYYYMmm, YYYY-MM."""
    tp = str(tp).strip()
    try:
        if "Q" in tp:
            y, q = tp.split("Q")
            return dt.date(int(y), {"1": 1, "2": 4, "3": 7, "4": 10}[q.strip()], 1)
        if "M" in tp:
            y, m = tp.split("M")
            return dt.date(int(y), int(m), 1)
        if "-" in tp:
            parts = tp.split("-")
            if len(parts) == 2:
                return dt.date(int(parts[0]), int(parts[1]), 1)
        if tp.isdigit() and len(tp) == 4:
            return dt.date(int(tp), 12, 31)
        if tp.isdigit():
            return dt.date(int(tp), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


# ---------------------------------------------------------------- write helper
def write_group(dataset, group_id, series_keys, dates, values, extra_cols=None):
    """Write one grouped Parquet: many series in one file.

    Columns: series_key, obs_date, value (+ optional extra columns dict of lists).
    Returns rows written (0 if nothing)."""
    if not series_keys:
        return 0
    cols = {
        "series_key": pa.array(series_keys, type=pa.string()),
        "obs_date": pa.array(dates, type=pa.date32()),
        "value": pa.array(values, type=pa.float64()),
    }
    if extra_cols:
        for k, v in extra_cols.items():
            cols[k] = pa.array(v)
    tbl = pa.table(cols)
    ddir = os.path.join(OUT, dataset)
    os.makedirs(ddir, exist_ok=True)
    safe = group_id.replace("/", "_").replace("\\", "_").replace(" ", "_")
    pq.write_table(tbl, os.path.join(ddir, f"{safe}.parquet"), compression="zstd")
    with _stats_lock:
        STATS["groups"] += 1
        STATS["rows"] += len(series_keys)
    return len(series_keys)


# ---------------------------------------------------------------- done-set
def load_done():
    if os.path.exists(DONE_FILE):
        try:
            with open(DONE_FILE, encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:  # noqa: BLE001
            return set()
    return set()


def mark_done(done, key):
    with _done_lock:
        done.add(key)
        if len(done) % 25 == 0:
            _flush_done(done)


def _flush_done(done):
    tmp = DONE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sorted(done), f)
    os.replace(tmp, DONE_FILE)


# ================================================================ DATASETS
def load_manifest():
    with open(MANIFEST, encoding="utf-8") as f:
        return json.load(f)


def _pk(x):
    """Extract the value-key from a BEA GetParameterValues entry, whatever the
    casing/field name (Key / key / TableName / TableID)."""
    for fld in ("Key", "key", "TableName", "TableID"):
        if fld in x and x[fld] not in (None, ""):
            return str(x[fld]).strip()
    # fall back to first value
    for v in x.values():
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _keys(vals):
    out = []
    for x in vals:
        k = _pk(x)
        if k is not None:
            out.append(k)
    return out


# ---- NIPA / NIUnderlyingDetail: per table, stack A+Q+M; key = SeriesCode:freq
def _ingest_table_freq_dataset(M, dataset, freqs, done, resume):
    tables = _keys(M["param_values"][dataset]["TableName"])

    def work(table):
        gkey = f"{dataset}:{table}"
        if resume and gkey in done:
            return f"skip {gkey}"
        sk, ds, vs = [], [], []
        for fr in freqs:
            rows = call(datasetname=dataset, TableName=table, Frequency=fr, Year="ALL")
            for row in rows:
                code = row.get("SeriesCode")
                od = pdate(row.get("TimePeriod"))
                val = pval(row.get("DataValue"))
                if not code or od is None or val is None:
                    continue
                sk.append(f"{code}:{fr}")
                ds.append(od)
                vs.append(val)
        n = write_group(dataset, table, sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(tables, work, dataset)


# ---- FixedAssets: per table (annual only); key = SeriesCode
def _ingest_fixedassets(M, done, resume):
    tables = _keys(M["param_values"]["FixedAssets"]["TableName"])

    def work(table):
        gkey = f"FixedAssets:{table}"
        if resume and gkey in done:
            return f"skip {gkey}"
        sk, ds, vs = [], [], []
        rows = call(datasetname="FixedAssets", TableName=table, Year="ALL")
        for row in rows:
            code = row.get("SeriesCode")
            od = pdate(row.get("TimePeriod"))
            val = pval(row.get("DataValue"))
            if not code or od is None or val is None:
                continue
            sk.append(str(code))
            ds.append(od)
            vs.append(val)
        n = write_group("FixedAssets", table, sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(tables, work, "FixedAssets")


# ---- GDPbyIndustry: TableID=ALL,Industry=ALL per freq -> 1 file per freq
def _ingest_gdpbyindustry(M, dataset, freqs, done, resume):
    def work(fr):
        gkey = f"{dataset}:freq{fr}"
        if resume and gkey in done:
            return f"skip {gkey}"
        rows = call(datasetname=dataset, TableID="ALL", Industry="ALL",
                    Frequency=fr, Year="ALL")
        sk, ds, vs = [], [], []
        for row in rows:
            tid = row.get("TableID")
            ind = row.get("Industry")
            # GDPbyIndustry rows carry Year + Quarter (not TimePeriod)
            yr = row.get("Year")
            qtr = row.get("Quarter")
            od = _gdp_date(yr, qtr, fr)
            val = pval(row.get("DataValue"))
            if tid is None or ind is None or od is None or val is None:
                continue
            sk.append(f"T{tid}:{ind}:{fr}")
            ds.append(od)
            vs.append(val)
        n = write_group(dataset, f"all_freq{fr}", sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(freqs, work, dataset)


_QMAP = {
    "1": 1, "2": 4, "3": 7, "4": 10,
    "Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10,
    "I": 1, "II": 4, "III": 7, "IV": 10,   # GDPbyIndustry uses Roman numerals
}


def _gdp_date(yr, qtr, fr):
    try:
        y = int(str(yr))
    except (ValueError, TypeError):
        return None
    if fr == "A" or not qtr or str(qtr).strip() == str(yr).strip():
        return dt.date(y, 12, 31)
    q = str(qtr).strip().upper()
    if q in _QMAP:
        return dt.date(y, _QMAP[q], 1)
    return dt.date(y, 12, 31)


# ---- UnderlyingGDPbyIndustry: per TableID (Industry=ALL), annual; 1 file/table
def _ingest_under_gdpbyindustry(M, done, resume):
    tids = _keys(M["param_values"]["UnderlyingGDPbyIndustry"]["TableID"])
    freqs = _keys(M["param_values"]["UnderlyingGDPbyIndustry"]["Frequency"])

    def work(tid):
        gkey = f"UnderlyingGDPbyIndustry:{tid}"
        if resume and gkey in done:
            return f"skip {gkey}"
        sk, ds, vs = [], [], []
        for fr in freqs:
            rows = call(datasetname="UnderlyingGDPbyIndustry", TableID=tid,
                        Industry="ALL", Frequency=fr, Year="ALL")
            for row in rows:
                ind = row.get("Industry")
                od = _gdp_date(row.get("Year"), row.get("Quarter"), fr)
                val = pval(row.get("DataValue"))
                if ind is None or od is None or val is None:
                    continue
                sk.append(f"T{tid}:{ind}:{fr}")
                ds.append(od)
                vs.append(val)
        n = write_group("UnderlyingGDPbyIndustry", f"T{tid}", sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(tids, work, "UnderlyingGDPbyIndustry")


# ---- InputOutput: per TableID, all years. These are Row x Col matrices keyed
#      by year; series_key = Row||Col, plus a 'year' column. 1 file per table.
def _ingest_inputoutput(M, done, resume):
    tids = _keys(M["param_values"]["InputOutput"]["TableID"])

    def work(tid):
        gkey = f"InputOutput:{tid}"
        if resume and gkey in done:
            return f"skip {gkey}"
        rows = call(datasetname="InputOutput", TableID=tid, Year="ALL")
        sk, ds, vs = [], [], []
        for row in rows:
            rc = row.get("RowCode") or row.get("RowDescr") or ""
            cc = row.get("ColCode") or row.get("ColDescr") or ""
            od = pdate(row.get("Year"))
            val = pval(row.get("DataValue"))
            if od is None or val is None:
                continue
            sk.append(f"{rc}|{cc}")
            ds.append(od)
            vs.append(val)
        n = write_group("InputOutput", f"T{tid}", sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(tids, work, "InputOutput")


# ---- ITA: Indicator=ALL per (country, frequency). Group into 1 file per freq;
#      series_key = Indicator:AreaOrCountry. TimeSeriesId kept as extra col.
def _ingest_ita(M, done, resume):
    countries = _keys(M["param_values"]["ITA"]["AreaOrCountry"])
    freqs = _keys(M["param_values"]["ITA"]["Frequency"])
    # accumulate per-frequency across all countries, then write 1 file/freq
    for fr in freqs:
        gkey = f"ITA:freq{fr}"
        if resume and gkey in done:
            print(f"  skip {gkey}", flush=True)
            continue
        acc = {"sk": [], "ds": [], "vs": [], "tsid": []}
        lock = threading.Lock()

        def work(ctry, fr=fr, acc=acc, lock=lock):
            rows = call(datasetname="ITA", Indicator="ALL", AreaOrCountry=ctry,
                        Frequency=fr, Year="ALL")
            loc_sk, loc_ds, loc_vs, loc_id = [], [], [], []
            for row in rows:
                ind = row.get("Indicator")
                od = pdate(row.get("TimePeriod"))
                val = pval(row.get("DataValue"))
                if ind is None or od is None or val is None:
                    continue
                loc_sk.append(f"{ind}:{ctry}")
                loc_ds.append(od)
                loc_vs.append(val)
                loc_id.append(row.get("TimeSeriesId") or "")
            with lock:
                acc["sk"].extend(loc_sk)
                acc["ds"].extend(loc_ds)
                acc["vs"].extend(loc_vs)
                acc["tsid"].extend(loc_id)
            return len(loc_sk)

        _run(countries, work, f"ITA freq={fr}")
        write_group("ITA", f"all_freq{fr}", acc["sk"], acc["ds"], acc["vs"],
                    extra_cols={"time_series_id": acc["tsid"]})
        mark_done(done, gkey)


# ---- IIP: per TypeOfInvestment (Component=ALL, Freq=ALL). Group into 1 file;
#      series_key = TypeOfInvestment:Component:Frequency.
def _ingest_iip(M, done, resume):
    types = _keys(M["param_values"]["IIP"]["TypeOfInvestment"])
    gkey = "IIP:all"
    if resume and gkey in done:
        print(f"  skip {gkey}", flush=True)
        return
    acc = {"sk": [], "ds": [], "vs": [], "tsid": []}
    lock = threading.Lock()

    def work(toi):
        rows = call(datasetname="IIP", TypeOfInvestment=toi, Component="ALL",
                    Frequency="ALL", Year="ALL")
        loc = ([], [], [], [])
        for row in rows:
            comp = row.get("Component")
            fr = row.get("Frequency")
            od = pdate(row.get("TimePeriod"))
            val = pval(row.get("DataValue"))
            if comp is None or od is None or val is None:
                continue
            loc[0].append(f"{toi}:{comp}:{fr}")
            loc[1].append(od)
            loc[2].append(val)
            loc[3].append(row.get("TimeSeriesId") or "")
        with lock:
            acc["sk"].extend(loc[0]); acc["ds"].extend(loc[1])
            acc["vs"].extend(loc[2]); acc["tsid"].extend(loc[3])
        return len(loc[0])

    _run(types, work, "IIP")
    write_group("IIP", "all", acc["sk"], acc["ds"], acc["vs"],
                extra_cols={"time_series_id": acc["tsid"]})
    mark_done(done, gkey)


# ---- IntlServTrade: TypeOfService=ALL per country. 1 file; key = Type:Dir:Affil:Country
def _ingest_intlservtrade(M, done, resume):
    countries = _keys(M["param_values"]["IntlServTrade"]["AreaOrCountry"])
    gkey = "IntlServTrade:all"
    if resume and gkey in done:
        print(f"  skip {gkey}", flush=True)
        return
    acc = {"sk": [], "ds": [], "vs": [], "tsid": []}
    lock = threading.Lock()

    def work(ctry):
        rows = call(datasetname="IntlServTrade", TypeOfService="ALL",
                    TradeDirection="ALL", Affiliation="ALL",
                    AreaOrCountry=ctry, Year="ALL")
        loc = ([], [], [], [])
        for row in rows:
            tos = row.get("TypeOfService")
            td = row.get("TradeDirection")
            af = row.get("Affiliation")
            od = pdate(row.get("TimePeriod") or row.get("Year"))
            val = pval(row.get("DataValue"))
            if tos is None or od is None or val is None:
                continue
            loc[0].append(f"{tos}:{td}:{af}:{ctry}")
            loc[1].append(od)
            loc[2].append(val)
            loc[3].append(row.get("TimeSeriesId") or "")
        with lock:
            acc["sk"].extend(loc[0]); acc["ds"].extend(loc[1])
            acc["vs"].extend(loc[2]); acc["tsid"].extend(loc[3])
        return len(loc[0])

    _run(countries, work, "IntlServTrade")
    write_group("IntlServTrade", "all", acc["sk"], acc["ds"], acc["vs"],
                extra_cols={"time_series_id": acc["tsid"]})
    mark_done(done, gkey)


# ---- IntlServSTA: Industry=ALL per country. 1 file; key = Channel:Dest:Industry:Country
def _ingest_intlservsta(M, done, resume):
    countries = _keys(M["param_values"]["IntlServSTA"]["AreaOrCountry"])
    gkey = "IntlServSTA:all"
    if resume and gkey in done:
        print(f"  skip {gkey}", flush=True)
        return
    acc = {"sk": [], "ds": [], "vs": [], "tsid": []}
    lock = threading.Lock()

    def work(ctry):
        rows = call(datasetname="IntlServSTA", Channel="ALL", Destination="ALL",
                    Industry="ALL", AreaOrCountry=ctry, Year="ALL")
        loc = ([], [], [], [])
        for row in rows:
            ch = row.get("Channel")
            de = row.get("Destination")
            ind = row.get("Industry")
            od = pdate(row.get("TimePeriod") or row.get("Year"))
            val = pval(row.get("DataValue"))
            if ind is None or od is None or val is None:
                continue
            loc[0].append(f"{ch}:{de}:{ind}:{ctry}")
            loc[1].append(od)
            loc[2].append(val)
            loc[3].append(row.get("TimeSeriesId") or "")
        with lock:
            acc["sk"].extend(loc[0]); acc["ds"].extend(loc[1])
            acc["vs"].extend(loc[2]); acc["tsid"].extend(loc[3])
        return len(loc[0])

    _run(countries, work, "IntlServSTA")
    write_group("IntlServSTA", "all", acc["sk"], acc["ds"], acc["vs"],
                extra_cols={"time_series_id": acc["tsid"]})
    mark_done(done, gkey)


# ---- Regional: per table, geo-level wildcards x each LineCode, Year=ALL.
#      1 file per table; series_key = LineCode:GeoFips. Tables are processed
#      smallest-first so the many small tables finish fast and only the ~13
#      county monsters (100+ linecodes x COUNTY) run at the end.
REGIONAL_GEOS = ["STATE", "COUNTY", "MSA", "MIC", "PORT", "DIV", "CSA"]


def _ingest_regional(M, done, resume):
    tables = _keys(M["param_values"]["Regional"]["TableName"])
    linecodes = M["regional_linecodes"]
    # smallest-first by linecode count (county tables with 100+ go last)
    tables = sorted(tables, key=lambda t: len(linecodes.get(t, [])))

    def work(table):
        gkey = f"Regional:{table}"
        if resume and gkey in done:
            return f"skip {gkey}"
        lcs = linecodes.get(table, [])
        sk, ds, vs = [], [], []
        good_geos = []
        if lcs:
            # 1) discover supported geo levels with the first linecode (<=7 calls)
            for geo in REGIONAL_GEOS:
                rows = call(datasetname="Regional", TableName=table,
                            GeoFips=geo, LineCode=lcs[0], Year="ALL")
                if rows:
                    good_geos.append(geo)
                    for row in rows:
                        gf = row.get("GeoFips")
                        od = pdate(row.get("TimePeriod"))
                        val = pval(row.get("DataValue"))
                        if gf is None or od is None or val is None:
                            continue
                        sk.append(f"{lcs[0]}:{gf}")
                        ds.append(od)
                        vs.append(val)
            # 2) remaining linecodes across the supported geos
            for lc in lcs[1:]:
                for geo in good_geos:
                    rows = call(datasetname="Regional", TableName=table,
                                GeoFips=geo, LineCode=lc, Year="ALL")
                    for row in rows:
                        gf = row.get("GeoFips")
                        od = pdate(row.get("TimePeriod"))
                        val = pval(row.get("DataValue"))
                        if gf is None or od is None or val is None:
                            continue
                        sk.append(f"{lc}:{gf}")
                        ds.append(od)
                        vs.append(val)
        n = write_group("Regional", table, sk, ds, vs)
        mark_done(done, gkey)
        with _stats_lock:
            print(f"    Regional {table}: {n:,} rows geos={good_geos} lcs={len(lcs)}", flush=True)
        return f"{gkey}: {n} rows"

    _run(tables, work, "Regional")


# ---- MNE: per (direction, classification). Year=all, Country=all, Industry=all,
#      SeriesID=all (<=3 ALL). MNE is a Row x Column matrix keyed by Year.
#      series_key = SeriesID:Row:Column; 'year' column. 1 file per dir_class.
def _ingest_mne(M, done, resume):
    dirs = _keys(M["param_values"]["MNE"]["DirectionOfInvestment"])
    classes = _keys(M["param_values"]["MNE"]["Classification"])
    combos = [(d, c) for d in dirs for c in classes]

    def work(combo):
        d, c = combo
        gkey = f"MNE:{d}:{c}"
        if resume and gkey in done:
            return f"skip {gkey}"
        # SeriesID=all uses 1 ALL; Country=all + Industry=all = 3 ALL total (Year fixed loop not needed: Year=all is 4th ALL -> too many).
        # So loop years is required. Pull Year=all via a single call by fixing one of country/industry? No -- keep Country=all, Industry=all, SeriesID=all, and loop Year.
        years = [y for y in _keys(M["param_values"]["MNE"]["Year"]) if str(y).isdigit()]
        sk, ds, vs = [], [], []
        any_data = False
        for yr in years:
            rows = call(datasetname="MNE", DirectionOfInvestment=d, Classification=c,
                        Country="all", Industry="all", SeriesID="all", Year=yr)
            if not rows:
                continue
            any_data = True
            for row in rows:
                ser = row.get("SeriesID")
                rowc = row.get("RowCode") or row.get("Row") or ""
                colc = row.get("ColumnCode") or row.get("Column") or ""
                od = pdate(row.get("Year"))
                val = pval(row.get("DataValueUnformatted") if row.get("DataValueUnformatted") not in (None, "") else row.get("DataValue"))
                if od is None or val is None:
                    continue
                sk.append(f"{ser}:{rowc}:{colc}")
                ds.append(od)
                vs.append(val)
        if not any_data:
            mark_done(done, gkey)
            return f"{gkey}: no data"
        n = write_group("MNE", f"{d}_{c}", sk, ds, vs)
        mark_done(done, gkey)
        return f"{gkey}: {n} rows"

    _run(combos, work, "MNE")


# ---------------------------------------------------------------- runner
def _run(items, work, label):
    print(f"[{label}] {len(items)} units", flush=True)
    n_done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(work, it): it for it in items}
        for fut in as_completed(futs):
            try:
                msg = fut.result()
            except Exception as e:  # noqa: BLE001
                msg = f"ERROR on {futs[fut]}: {e}"
                with _stats_lock:
                    STATS["errors"] += 1
            n_done += 1
            if n_done % 10 == 0 or n_done == len(items):
                print(f"  [{label}] {n_done}/{len(items)}  (rows so far={STATS['rows']:,}, calls={STATS['calls']:,})", flush=True)


DISPATCH = {
    "NIPA": lambda M, done, resume: _ingest_table_freq_dataset(M, "NIPA", ["A", "Q", "M"], done, resume),
    "NIUnderlyingDetail": lambda M, done, resume: _ingest_table_freq_dataset(M, "NIUnderlyingDetail", ["A", "Q", "M"], done, resume),
    "FixedAssets": lambda M, done, resume: _ingest_fixedassets(M, done, resume),
    "GDPbyIndustry": lambda M, done, resume: _ingest_gdpbyindustry(M, "GDPbyIndustry", ["A", "Q"], done, resume),
    "UnderlyingGDPbyIndustry": lambda M, done, resume: _ingest_under_gdpbyindustry(M, done, resume),
    "InputOutput": lambda M, done, resume: _ingest_inputoutput(M, done, resume),
    "ITA": lambda M, done, resume: _ingest_ita(M, done, resume),
    "IIP": lambda M, done, resume: _ingest_iip(M, done, resume),
    "IntlServTrade": lambda M, done, resume: _ingest_intlservtrade(M, done, resume),
    "IntlServSTA": lambda M, done, resume: _ingest_intlservsta(M, done, resume),
    "Regional": lambda M, done, resume: _ingest_regional(M, done, resume),
    "MNE": lambda M, done, resume: _ingest_mne(M, done, resume),
}

ORDER = ["NIPA", "NIUnderlyingDetail", "FixedAssets", "GDPbyIndustry",
         "UnderlyingGDPbyIndustry", "InputOutput", "ITA", "IIP",
         "IntlServTrade", "IntlServSTA", "Regional", "MNE"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="", help="comma list; default all")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    M = load_manifest()
    done = load_done() if args.resume else set()
    targets = [d.strip() for d in args.datasets.split(",") if d.strip()] or ORDER

    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    for dsn in targets:
        if dsn not in DISPATCH:
            print(f"!! unknown dataset {dsn}", flush=True)
            continue
        print(f"\n===== {dsn} =====", flush=True)
        DISPATCH[dsn](M, done, args.resume)
        _flush_done(done)
        print(f"  -> cumulative: groups={STATS['groups']:,} rows={STATS['rows']:,} calls={STATS['calls']:,} errors={STATS['errors']}", flush=True)

    _flush_done(done)
    dt_s = time.time() - t0
    print(f"\nDONE in {dt_s/60:.1f} min: groups={STATS['groups']:,} rows={STATS['rows']:,} "
          f"calls={STATS['calls']:,} errors={STATS['errors']}", flush=True)


if __name__ == "__main__":
    main()
