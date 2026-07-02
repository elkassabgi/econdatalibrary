#!/usr/bin/env python3
"""FULL-COVERAGE ingest of the U.S. EIA bulk download files.

Source: https://api.eia.gov/bulk/manifest.txt  -- lists every bulk dataset zip
(PET, NG, ELEC, EBA, COAL, TOTAL, SEDS, STEO, INTL, EMISS, NUC_STATUS,
PET_IMPORTS, IEO, AEO.<year>, ...). Each zip contains a single line-delimited
JSON file: most lines are SERIES records, some are CATEGORY (taxonomy) records.

SERIES record (one per line):
  {"series_id","name","units","f","unitsshort","description","copyright",
   "source","iso3166","geography","start","end","last_updated",
   "data": [[period, value], ...]}
CATEGORY record (no observations -> skipped):
  {"category_id","parent_category_id","name","notes","childseries"}

Period tokens by frequency `f`:
  A  annual      "YYYY"                 -> Dec-31
  Q  quarterly   "YYYYQn"               -> first day of quarter
  M  monthly     "YYYYMM"               -> 1st of month
  W  weekly      "YYYYMMDD"             -> that date
  D  daily       "YYYYMMDD"             -> that date
  H  hourly UTC  "YYYYMMDDThh"          -> that date  (hour kept in `period`)
  HL hourly local"YYYYMMDDThh-07"       -> that date  (hour+tz kept in `period`)

GROUPED storage (mirrors jobs/ingest_bls.py / ingest_eurostat.py /
ingest_sec_edgar.py): ONE Parquet per bulk dataset ->
data/clean_full/eia/<DATASET>.parquet with a `series_id` column inside.
Columns: series_id, obs_date (date32), value (float64), period (str, raw EIA
period token -> lossless for hourly), freq (str). Memory is bounded: the zip's
single text member is streamed line-by-line and flushed to the ParquetWriter in
row-group batches (the 148M-row EBA dataset never sits in RAM whole).

Sidecar metadata: ONE Parquet per dataset ->
data/clean_full/eia/_meta/<DATASET>.parquet with one row per series
(series_id, name, units, freq, geography, iso3166, start, end, copyright,
source, n_obs) so the catalog ingest can build SeriesMeta rows without
re-reading the giant data file.

License: us-public-domain (configs/sources.yaml). EIA ToS: attribute to EIA;
public-domain values; copyright carve-out is captured per-series in the
`copyright` sidecar column for serve-time filtering.

Usage:
  python jobs/ingest_eia.py --probe            # parse small datasets, print, no big writes
  python jobs/ingest_eia.py --only NG,STEO     # one/few datasets
  python jobs/ingest_eia.py                     # full run (all bulk datasets)
  python jobs/ingest_eia.py --skip-download     # reuse already-downloaded zips
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "eia")
OUT = os.path.join(ROOT, "data", "clean_full", "eia")
META = os.path.join(OUT, "_meta")
MANIFEST_URL = "https://api.eia.gov/bulk/manifest.txt"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
BATCH = 1_000_000          # rows per Parquet row-group flush
CHUNK = 1 << 20            # 1 MiB download chunks

os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)
os.makedirs(META, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})


# ----------------------------------------------------------------------------
# period parsing  ->  (obs_date, ok)
# ----------------------------------------------------------------------------
def parse_obs_date(period, freq):
    """Map an EIA period token to a calendar date. Returns None if unparseable.

    For hourly (H/HL) the hour/timezone is preserved separately in the raw
    `period` column; here we anchor to the calendar date.
    """
    if period is None:
        return None
    p = str(period).strip()
    if not p:
        return None
    try:
        # Hourly: YYYYMMDDThh  or  YYYYMMDDThh-07
        if "T" in p:
            datepart = p.split("T", 1)[0]
            return dt.date(int(datepart[0:4]), int(datepart[4:6]), int(datepart[6:8]))
        # Quarterly: YYYYQn
        if "Q" in p:
            y, q = p.split("Q")
            q = int(q)
            if 1 <= q <= 4:
                return dt.date(int(y), (q - 1) * 3 + 1, 1)
            return None
        # Semiannual (rare): YYYYSn
        if "S" in p and p[4:5] == "S":
            y = int(p[0:4]); s = int(p[5:6])
            return dt.date(y, 1 if s == 1 else 7, 1)
        # Pure digits
        if p.isdigit():
            n = len(p)
            if n == 4:                       # annual YYYY
                return dt.date(int(p), 12, 31)
            if n == 6:                       # monthly YYYYMM
                mm = int(p[4:6])
                if 1 <= mm <= 12:
                    return dt.date(int(p[0:4]), mm, 1)
                return None
            if n == 8:                       # daily/weekly YYYYMMDD
                return dt.date(int(p[0:4]), int(p[4:6]), int(p[6:8]))
            if n == 5:                       # YYYYQ? without Q? -> treat as YYYY + ? : skip
                return None
    except (ValueError, TypeError, IndexError):
        return None
    return None


def parse_value(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except (ValueError, OverflowError):
            return None
        # NaN check
        return f if f == f else None
    s = str(v).strip()
    if not s or s in ("-", "NA", "(NA)", "N/A", "w", "W", "*", "--", "."):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ----------------------------------------------------------------------------
# download with retry/backoff
# ----------------------------------------------------------------------------
def download(url, dest, expected=None):
    """Stream a file to dest with retry/backoff. Skips if size already matches."""
    if expected and os.path.exists(dest) and os.path.getsize(dest) == expected:
        return os.path.getsize(dest)
    last = None
    for attempt in range(5):
        try:
            with SESSION.get(url, stream=True, timeout=600) as r:
                r.raise_for_status()
                exp = expected or int(r.headers.get("Content-Length", 0)) or None
                tmp = dest + ".part"
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(CHUNK):
                        if chunk:
                            fh.write(chunk)
                os.replace(tmp, dest)
            sz = os.path.getsize(dest)
            if exp and sz != exp:
                raise IOError(f"size {sz} != expected {exp}")
            return sz
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(60, 3 * (attempt + 1) ** 2))
    raise RuntimeError(f"download failed {url}: {last}")


# ----------------------------------------------------------------------------
# schemas
# ----------------------------------------------------------------------------
DATA_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("period", pa.string()),
    ("freq", pa.string()),
])

META_SCHEMA = pa.schema([
    ("series_id", pa.string()),
    ("name", pa.string()),
    ("units", pa.string()),
    ("freq", pa.string()),
    ("geography", pa.string()),
    ("iso3166", pa.string()),
    ("start", pa.string()),
    ("end", pa.string()),
    ("copyright", pa.string()),
    ("source", pa.string()),
    ("n_obs", pa.int64()),
])


def _s(x):
    if x is None:
        return None
    s = str(x).strip()
    return s or None


# ----------------------------------------------------------------------------
# parse one dataset zip -> grouped data Parquet + meta Parquet (streamed)
# ----------------------------------------------------------------------------
def write_dataset(dataset, zip_path, probe=False):
    """Parse one bulk zip -> one data Parquet + one meta Parquet.

    Returns dict of stats. Streams line-by-line; flushes in BATCH row-groups.
    """
    import io
    import zipfile

    safe = dataset.replace("/", "_").replace(":", "_")
    data_path = os.path.join(OUT, safe + ".parquet")
    meta_path = os.path.join(META, safe + ".parquet")

    writer = None if probe else pq.ParquetWriter(data_path, DATA_SCHEMA, compression="zstd")

    sid_b, date_b, val_b, per_b, fr_b = [], [], [], [], []
    n_obs = n_baddate = n_badval = 0
    n_series = n_cat = 0
    mn = mx = None
    freq_counts = {}
    # meta buffers
    m_sid, m_name, m_units, m_freq, m_geo, m_iso = [], [], [], [], [], []
    m_start, m_end, m_copy, m_src, m_nobs = [], [], [], [], []

    def flush_data():
        nonlocal sid_b, date_b, val_b, per_b, fr_b
        if not sid_b or probe:
            sid_b, date_b, val_b, per_b, fr_b = [], [], [], [], []
            return
        tbl = pa.table({
            "series_id": sid_b,
            "obs_date": pa.array(date_b, type=pa.date32()),
            "value": pa.array(val_b, type=pa.float64()),
            "period": per_b,
            "freq": fr_b,
        }, schema=DATA_SCHEMA)
        writer.write_table(tbl)
        sid_b, date_b, val_b, per_b, fr_b = [], [], [], [], []

    z = zipfile.ZipFile(zip_path)
    members = [n for n in z.namelist() if n.endswith((".txt", ".json"))]
    if not members:
        members = z.namelist()
    for member in members:
        with z.open(member) as fh:
            for raw in io.TextIOWrapper(fh, encoding="utf-8", errors="replace"):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    o = json.loads(raw)
                except (ValueError, json.JSONDecodeError):
                    continue
                sid = o.get("series_id")
                data = o.get("data")
                if not sid or data is None:
                    # category / taxonomy record
                    if "category_id" in o:
                        n_cat += 1
                    continue
                f = o.get("f") or o.get("frequency") or ""
                f = str(f).strip()
                n_series += 1
                freq_counts[f] = freq_counts.get(f, 0) + 1
                ser_obs = 0
                for row in data:
                    if not isinstance(row, (list, tuple)) or len(row) < 2:
                        continue
                    od = parse_obs_date(row[0], f)
                    if od is None:
                        n_baddate += 1
                        continue
                    v = parse_value(row[1])
                    if v is None:
                        n_badval += 1
                        continue
                    if not probe:
                        sid_b.append(sid)
                        date_b.append(od)
                        val_b.append(v)
                        per_b.append(str(row[0]).strip())
                        fr_b.append(f or None)
                    n_obs += 1
                    ser_obs += 1
                    if mn is None or od < mn:
                        mn = od
                    if mx is None or od > mx:
                        mx = od
                    if not probe and len(sid_b) >= BATCH:
                        flush_data()
                # meta row (one per series)
                m_sid.append(sid)
                m_name.append(_s(o.get("name")))
                m_units.append(_s(o.get("units")) or _s(o.get("unitsshort")))
                m_freq.append(f or None)
                m_geo.append(_s(o.get("geography")))
                m_iso.append(_s(o.get("iso3166")))
                m_start.append(_s(o.get("start")))
                m_end.append(_s(o.get("end")))
                m_copy.append(_s(o.get("copyright")))
                m_src.append(_s(o.get("source")))
                m_nobs.append(ser_obs)
    flush_data()
    if writer is not None:
        writer.close()
        if n_obs == 0:
            try:
                os.remove(data_path)
            except OSError:
                pass

    # write meta sidecar
    if not probe and m_sid:
        mtbl = pa.table({
            "series_id": m_sid, "name": m_name, "units": m_units, "freq": m_freq,
            "geography": m_geo, "iso3166": m_iso, "start": m_start, "end": m_end,
            "copyright": m_copy, "source": m_src, "n_obs": m_nobs,
        }, schema=META_SCHEMA)
        pq.write_table(mtbl, meta_path, compression="zstd")

    return {
        "n_obs": n_obs, "n_series": n_series, "n_categories": n_cat,
        "start": str(mn) if mn else None, "end": str(mx) if mx else None,
        "bad_date": n_baddate, "bad_val": n_badval, "freqs": freq_counts,
    }


# ----------------------------------------------------------------------------
# driver
# ----------------------------------------------------------------------------
def fetch_manifest():
    m = SESSION.get(MANIFEST_URL, timeout=120).json()
    return m["dataset"]


def main():
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    probe = "--probe" in sys.argv
    skip_dl = "--skip-download" in sys.argv

    ds = fetch_manifest()
    keys = sorted(ds.keys())
    if only:
        keys = [k for k in keys if k in only]
    if probe and not only:
        keys = ["NG", "STEO", "TOTAL", "EMISS"]  # small, exercise A/Q/M/W/D

    grand_obs = grand_series = grand_cat = 0
    summary = {}
    print(f"{'PROBE' if probe else 'RUN'}: {len(keys)} EIA bulk datasets "
          f"(of {len(ds)} in manifest) -> {OUT}", flush=True)

    for k in keys:
        v = ds[k]
        url = v["accessURL"]
        safe = k.replace("/", "_").replace(":", "_")
        zip_path = os.path.join(RAW, safe + ".zip")
        t0 = time.time()

        # 1) download
        dl_bytes = 0
        if not skip_dl:
            dl_bytes = download(url, zip_path)
        elif os.path.exists(zip_path):
            dl_bytes = os.path.getsize(zip_path)
        else:
            dl_bytes = download(url, zip_path)
        dl_s = time.time() - t0

        # 2) parse -> grouped parquet (streamed)
        try:
            st = write_dataset(k, zip_path, probe=probe)
        except Exception as e:  # noqa: BLE001
            print(f"{k:14} PARSE ERROR: {e}", flush=True)
            summary[k] = {"error": str(e)}
            continue

        grand_obs += st["n_obs"]
        grand_series += st["n_series"]
        grand_cat += st["n_categories"]
        summary[k] = {
            "name": v.get("name"),
            "n_obs": st["n_obs"], "n_series": st["n_series"],
            "n_categories": st["n_categories"],
            "start": st["start"], "end": st["end"],
            "bad_date": st["bad_date"], "bad_val": st["bad_val"],
            "freqs": st["freqs"],
            "dl_mb": round(dl_bytes / 1e6, 1),
            "last_updated": v.get("last_updated"),
            "accessURL": url,
        }
        print(
            f"{k:14} obs={st['n_obs']:>13,} series={st['n_series']:>8,} "
            f"cat={st['n_categories']:>5,} {str(st['start']):>10}..{str(st['end']):<10} "
            f"freqs={st['freqs']} dl={dl_s:5.0f}s parse={time.time()-t0-dl_s:6.0f}s "
            f"bd={st['bad_date']} bv={st['bad_val']}",
            flush=True,
        )

    # write run summary
    if not probe:
        json.dump(summary, open(os.path.join(OUT, "_summary.json"), "w"), indent=2)
    print("=" * 78, flush=True)
    print(f"{'PROBE-DONE' if probe else 'DONE'}: {len(summary)} datasets / "
          f"{grand_obs:,} observations / {grand_series:,} series / "
          f"{grand_cat:,} category records (skipped)", flush=True)


if __name__ == "__main__":
    main()
