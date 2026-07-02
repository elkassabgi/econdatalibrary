#!/usr/bin/env python3
"""Fill the remaining non-intltrade census coverage gaps:

  * timeseries/poverty/saipe/schdist  -- School-District SAIPE. 4 geo levels
        (elementary / secondary / unified / school-district-admin-area), each
        requiring a state parent. Iterate level x state, full time history.
  * timeseries/aies/miscsector        -- verified to return 204/no-content for
        every year & sector via the API (published in data.json but not served);
        we record it as published-but-empty (no parquet written).

Grouped Parquet -> data/clean_full/census/. us-public-domain.
"""
import datetime as dt
import json
import os
import sys
import time
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
sys.path.insert(0, ROOT)
RAW = os.path.join(ROOT, "data", "raw", "census")
OUT = os.path.join(ROOT, "data", "clean_full", "census")

UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
_KEY = None
def key():
    global _KEY
    if _KEY is None:
        for line in open(os.path.join(ROOT, ".env"), encoding="utf-8"):
            if line.startswith("CENSUS_API_KEY="):
                _KEY = line.split("=", 1)[1].strip()
    return _KEY

sess = requests.Session(); sess.headers.update(UA)
META = {m["path"]: m for m in json.load(open(os.path.join(RAW, "ts_meta.json"), encoding="utf-8"))}

US_STATES = ["01","02","04","05","06","08","09","10","11","12","13","15","16",
             "17","18","19","20","21","22","23","24","25","26","27","28","29",
             "30","31","32","33","34","35","36","37","38","39","40","41","42",
             "44","45","46","47","48","49","50","51","53","54","55","56","72"]


def req(url, tries=5, deadline=90):
    for i in range(tries):
        try:
            r = sess.get(url, timeout=(30, deadline))
            if r.status_code == 200:
                try:
                    return 200, r.json()
                except Exception:
                    return 200, None
            if r.status_code in (204, 404, 400):
                return r.status_code, None
            time.sleep(2 * (i + 1))
        except requests.exceptions.RequestException:
            time.sleep(2 * (i + 1))
    return -1, None


def parse_obs_date(d):
    if not d:
        return None
    d = str(d).strip()
    try:
        if len(d) == 4 and d.isdigit():
            return dt.date(int(d), 12, 31)
        if len(d) == 7 and d[4] == "-":
            return dt.date(int(d[:4]), int(d[5:7]), 1)
        if len(d) >= 10 and d[4] == "-":
            return dt.date(int(d[:4]), int(d[5:7]), int(d[8:10]))
    except Exception:
        return None
    return None


def ingest_schdist():
    path = "timeseries/poverty/saipe/schdist"
    m = META[path]
    av = m["allvars"]
    skip = {"for", "in", "ucgid", "time"}
    getv = [v for v in av if v not in skip]
    # ensure manageable: all 11 data vars fit in one chunk
    levels = [g[0] for g in m["geos"]]      # the 4 school-district geo levels
    outpath = os.path.join(OUT, "poverty__saipe__schdist.parquet")
    tmp = outpath + ".tmp"
    writer = None; schema = None; cols0 = None; tci = None; dimset = None
    n = 0; series = set(); ncalls = 0
    for lvl in levels:
        for st in US_STATES:
            g = quote(",".join(getv), safe=",")
            u = (f"https://api.census.gov/data/{path}?get={g}"
                 f"&for={quote(lvl, safe='')}:*&in=state:{st}"
                 f"&time=from+1995&key={key()}")
            code, js = req(u); ncalls += 1
            if code != 200 or not js or len(js) < 2:
                continue
            header, rows = js[0], js[1:]
            if cols0 is None:
                seen = set()
                cols0 = [c for c in header if not (c in seen or seen.add(c))]
                hidx0 = {c: i for i, c in enumerate(cols0)}
                tcol = "time" if "time" in cols0 else None
                tci = cols0.index(tcol) if tcol else None
                # dims = geo + grade/category id columns (not the SAE* estimates)
                dimset = [c for c in cols0 if c in
                          ("state", "GEOID", "LEAID", "GRADE", "GEOCAT", "SD_NAME")
                          or c == lvl or c in levels]
                fields = [pa.field("series_key", pa.string())]
                fields += [pa.field("geo_level", pa.string())]
                fields += [pa.field(c, pa.string()) for c in cols0]
                if tcol:
                    fields.append(pa.field("obs_date", pa.date32()))
                schema = pa.schema(fields)
                writer = pq.ParquetWriter(tmp, schema, compression="zstd")
            hidx = {c: i for i, c in enumerate(header)}
            src = [hidx.get(c) for c in cols0]
            ncol = len(cols0)
            carr = [[] for _ in range(ncol)]
            sk = []; glev = []; obs = [] if tci is not None else None
            for r in rows:
                ar = [r[i] if (i is not None and i < len(r)) else None for i in src]
                for ci in range(ncol):
                    carr[ci].append(ar[ci])
                parts = ["poverty/saipe/schdist", f"level={lvl}"]
                for c in dimset:
                    if c in hidx0:
                        v = ar[hidx0[c]]
                        if v not in (None, ""):
                            parts.append(f"{c}={v}")
                k = "|".join(parts)
                sk.append(k); series.add(k); glev.append(lvl)
                if tci is not None:
                    obs.append(parse_obs_date(ar[tci]))
            data = {"series_key": pa.array(sk, type=pa.string()),
                    "geo_level": pa.array(glev, type=pa.string())}
            for ci, c in enumerate(cols0):
                data[c] = pa.array(carr[ci], type=pa.string())
            if tci is not None:
                data["obs_date"] = pa.array(obs, type=pa.date32())
            writer.write_table(pa.table(data, schema=schema))
            n += len(rows)
    if writer is not None:
        writer.close(); os.replace(tmp, outpath)
    return n, len(series), ncalls


def check_miscsector():
    """Confirm miscsector is empty across years/sectors via the API."""
    path = "timeseries/aies/miscsector"
    hits = 0
    for y in range(1992, 2024):
        u = (f"https://api.census.gov/data/{path}?get=SECTOR,DEPR_VAL,RCPT_TOYS_VAL"
             f"&for=us:*&time={y}&key={key()}")
        code, js = req(u, tries=2, deadline=40)
        if code == 200 and js and len(js) > 1:
            hits += 1
    return hits


def main():
    args = sys.argv[1:]
    if "--miscsector" in args:
        h = check_miscsector()
        print(f"miscsector years-with-data: {h} (0 => published-but-empty)", flush=True)
        return
    if "--schdist" in args or not args:
        t0 = time.time()
        n, ns, nc = ingest_schdist()
        print(f"schdist: rows={n:,} series={ns:,} calls={nc} {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
