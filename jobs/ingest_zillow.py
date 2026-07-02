#!/usr/bin/env python3
"""Full-coverage ingest of the ENTIRE Zillow Research public CSV catalog.

Source page: https://www.zillow.com/research/data/  (the page embeds a JS
`var data` object mapping  SET -> TYPE -> GEOGRAPHY -> direct CSV URL on
files.zillowstatic.com).  License class: zillow-research (re-serveable).
Attribution MUST read exactly "Data Provided by Zillow Group".

What we pull
------------
EVERY metric x EVERY geography x EVERY frequency Zillow publishes:
  Home Values (ZHVI, all tiers/home-types), Home-Value Forecast (ZHVF),
  Rentals (ZORI/ZORDI), Rental Forecast (ZORF), For-Sale Listings
  (inventory, median list price, new listings, new pending, share price-cut...),
  Sales (median/mean sale price, sales count, sale-to-list, days-to-close,
  total transaction value...), Days-on-Market & Price-Cuts, Market Heat Index,
  New Construction, and Affordability.

Across ~206 live CSVs spanning geographies National / Metro / State / County /
City / Zip / Neighborhood, monthly + weekly.

GROUPED storage (anti-bloat)
----------------------------
ONE Parquet "cube" per source CSV (the dataset), each holding MANY region-level
series in long form -- never one-file-per-series.  ~206 cubes total.

Per cube we write under data/clean_full/zillow/ :
  <dataset>.parquet          long obs:  dataset, series_key, obs_date, value
  <dataset>__series.parquet  one row per region series + all ID/metadata columns
  <dataset>.meta.json        verification stats (re-read from disk)
And a top-level zillow.meta.json aggregating the run.

Each wide Zillow CSV is region rows x date columns.  Leading non-date columns
(RegionID, SizeRank, RegionName, RegionType, StateName, and -- for finer geos --
State, City, Metro, CountyName, FIPS, plus BaseDate on forecasts) are metadata;
they go in the __series sidecar.  Every YYYY-MM-DD column becomes a long
observation (obs_date = that date, value = the cell).

series_key format:  zillow:<metric>:<geo>:<RegionID>
   e.g.  zillow:zhvi:Metro:394913

Usage
-----
  python jobs/ingest_zillow.py --refresh        # re-fetch the research page + re-enumerate
  python jobs/ingest_zillow.py --dry 5          # parse 5 cubes to memory, print, no writes
  python jobs/ingest_zillow.py                  # FULL run (download + parse + write + verify)
  python jobs/ingest_zillow.py --catalog        # also upsert into catalog.db (off by default)
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import re
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ROOT = r"D:/research/econfindatalibrary"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

RESEARCH_PAGE = "https://www.zillow.com/research/data/"
OUT = os.path.join(ROOT, "data", "clean_full", "zillow")
RAW = os.path.join(ROOT, "data", "raw", "zillow")
FILES_JSON = os.path.join(ROOT, "data", "_zillow_files.json")
PAGE_HTML = os.path.join(ROOT, "data", "_zillow_page_live.html")

UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "zillow-research"
ATTRIBUTION = "Data Provided by Zillow Group"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# The full universe of leading, non-date metadata columns Zillow uses across all
# CSV shapes. Anything matching DATE_RE is an observation; everything else here
# (or any other non-date leading column) is per-series metadata.
KNOWN_ID_COLS = {
    "RegionID", "SizeRank", "RegionName", "RegionType", "StateName",
    "State", "City", "Metro", "CountyName", "StateCodeFIPS",
    "MunicipalCodeFIPS", "BaseDate", "Home Type",
}


# --------------------------------------------------------------------------- #
# HTTP session with polite UA + retry/backoff
# --------------------------------------------------------------------------- #
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=6, backoff_factor=1.5, connect=6, read=6,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": UA})
    return s


# --------------------------------------------------------------------------- #
# Catalog enumeration -- the FULL list of CSV URLs from the research page
# --------------------------------------------------------------------------- #
def _extract_data_object(t: str) -> str:
    start = t.find("var data")
    brace = t.find("{", start)
    i = brace
    depth = 0
    instr = False
    esc = False
    while i < len(t):
        c = t[i]
        if instr:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = False
        else:
            if c == '"':
                instr = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return t[brace:i + 1]
        i += 1
    raise RuntimeError("no balanced `var data` object found on the research page")


def refresh_catalog(sess: requests.Session) -> list[dict]:
    """Fetch the live research page, extract every SET->TYPE->GEO->URL, dedupe by
    URL, and write data/_zillow_files.json. Returns the list of file records."""
    r = sess.get(RESEARCH_PAGE, timeout=120)
    r.raise_for_status()
    html = r.text
    open(PAGE_HTML, "w", encoding="utf-8").write(html)
    data = json.loads(_extract_data_object(html))
    seen: dict[str, dict] = {}
    for setname, types in data.items():
        for typelabel, geos in types.items():
            for geolabel, url in geos.items():
                u = url.split("?")[0]
                if u not in seen:
                    seen[u] = {"set": setname, "type": typelabel,
                               "geo": geolabel, "url": u}
    files = list(seen.values())
    json.dump(files, open(FILES_JSON, "w", encoding="utf-8"), indent=1)
    print(f"catalog: {len(data)} sets -> {len(files)} unique CSV files "
          f"(written {FILES_JSON})", flush=True)
    return files


def load_catalog() -> list[dict]:
    if os.path.exists(FILES_JSON):
        return json.load(open(FILES_JSON, encoding="utf-8"))
    raise SystemExit("no _zillow_files.json -- run with --refresh first")


# --------------------------------------------------------------------------- #
# Parse one wide Zillow CSV -> long observations + per-series metadata
# --------------------------------------------------------------------------- #
def _dataset_name(url: str) -> str:
    """Filename stem, unique across the catalog -> the cube name."""
    return url.rsplit("/", 1)[-1][:-4] if url.endswith(".csv") else url.rsplit("/", 1)[-1]


def _metric_of(url: str) -> str:
    """The folder after public_csvs/ or public_v2/ -> the Zillow metric code."""
    for marker in ("/public_csvs/", "/public_v2/"):
        if marker in url:
            return url.split(marker, 1)[1].split("/", 1)[0]
    return "unknown"


def _geo_of(dataset: str) -> str:
    """Leading token of the filename = geography level (Metro/State/Zip/...)."""
    return dataset.split("_", 1)[0]


def _to_float(x):
    if x is None:
        return None
    s = x.strip() if isinstance(x, str) else x
    if s == "" or s is None:
        return None
    try:
        f = float(s)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    return f


def parse_csv(text: str, url: str):
    """Yield (obs_rows, series_rows) for one wide CSV.

    obs_rows: list of (series_key, date(obs_date), value(float))
    series_rows: list of dict (one per region) with metric/geo + all ID columns.
    """
    import csv as _csv

    metric = _metric_of(url)
    geo = _geo_of(_dataset_name(url))
    rdr = _csv.reader(io.StringIO(text))
    header = next(rdr)
    # classify columns
    date_cols = []   # (col_index, date)
    id_idx = []      # indices of metadata columns
    for j, col in enumerate(header):
        c = col.strip()
        if DATE_RE.match(c):
            try:
                date_cols.append((j, dt.date.fromisoformat(c)))
            except ValueError:
                id_idx.append(j)
        else:
            id_idx.append(j)

    # locate RegionID for the key
    name_to_idx = {col.strip(): j for j, col in enumerate(header)}
    rid_j = name_to_idx.get("RegionID")

    obs_rows = []
    series_rows = []
    for row in rdr:
        if not row:
            continue
        # pad short rows defensively
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        rid = row[rid_j].strip() if rid_j is not None and rid_j < len(row) else ""
        if rid == "":
            # no region id -> skip (cannot key); extremely rare
            continue
        # region id is numeric in Zillow; keep as int string
        try:
            rid_key = str(int(float(rid)))
        except (TypeError, ValueError):
            rid_key = rid
        series_key = f"zillow:{metric}:{geo}:{rid_key}"

        # series metadata row
        meta = {"series_key": series_key, "metric": metric, "geo_level": geo}
        for j in id_idx:
            colname = header[j].strip()
            val = row[j].strip() if j < len(row) else ""
            meta[colname] = val
        n_obs_this = 0
        for j, d in date_cols:
            if j >= len(row):
                continue
            v = _to_float(row[j])
            if v is None:
                continue
            obs_rows.append((series_key, d, v))
            n_obs_this += 1
        meta["n_obs"] = n_obs_this
        series_rows.append(meta)
    return obs_rows, series_rows


# --------------------------------------------------------------------------- #
# Write one cube (obs + series sidecar + meta) and verify by re-reading
# --------------------------------------------------------------------------- #
def write_cube(dataset: str, obs_rows, series_rows, rec: dict) -> dict:
    os.makedirs(OUT, exist_ok=True)
    keys = [r[0] for r in obs_rows]
    dates = [r[1] for r in obs_rows]
    vals = [r[2] for r in obs_rows]

    obs_tbl = pa.table({
        "dataset": pa.array([dataset] * len(keys), type=pa.string()),
        "series_key": pa.array(keys, type=pa.string()),
        "obs_date": pa.array(dates, type=pa.date32()),
        "value": pa.array(vals, type=pa.float64()),
    })
    obs_path = os.path.join(OUT, f"{dataset}.parquet")
    pq.write_table(obs_tbl, obs_path, compression="zstd")

    # series sidecar: union of all metadata keys across rows
    all_keys = []
    for sr in series_rows:
        for k in sr:
            if k not in all_keys:
                all_keys.append(k)
    s_cols = {}
    for k in all_keys:
        if k == "n_obs":
            s_cols[k] = pa.array([int(sr.get(k, 0)) for sr in series_rows], type=pa.int64())
        else:
            s_cols[k] = pa.array([str(sr[k]) if sr.get(k, "") not in ("", None) else None
                                  for sr in series_rows], type=pa.string())
    s_tbl = pa.table(s_cols)
    s_path = os.path.join(OUT, f"{dataset}__series.parquet")
    pq.write_table(s_tbl, s_path, compression="zstd")

    # verify by re-reading from disk
    back = pq.read_table(obs_path)
    n_obs = back.num_rows
    n_series = back.column("series_key").to_pandas().nunique()
    d = back.column("obs_date").to_pandas().dropna()
    meta = {
        "dataset": dataset,
        "metric": _metric_of(rec["url"]),
        "geo_level": _geo_of(dataset),
        "set": rec.get("set"),
        "type": rec.get("type"),
        "geo_label": rec.get("geo"),
        "url": rec["url"],
        "license_id": LICENSE_ID,
        "attribution": ATTRIBUTION,
        "n_obs": int(n_obs),
        "n_series": int(n_series),
        "n_series_meta_rows": int(s_tbl.num_rows),
        "start": str(d.min()) if len(d) else None,
        "end": str(d.max()) if len(d) else None,
        "verify_ok": bool(n_obs == len(obs_rows) and s_tbl.num_rows == len(series_rows)),
        "obs_parquet": os.path.basename(obs_path),
        "series_parquet": os.path.basename(s_path),
    }
    with open(os.path.join(OUT, f"{dataset}.meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    return meta


# --------------------------------------------------------------------------- #
# Download a single CSV (cache to RAW), with retry/backoff
# --------------------------------------------------------------------------- #
def fetch_csv_text(sess: requests.Session, url: str, cache_raw: bool = True) -> str:
    raw_path = os.path.join(RAW, _dataset_name(url) + ".csv")
    if cache_raw and os.path.exists(raw_path) and os.path.getsize(raw_path) > 0:
        return open(raw_path, encoding="utf-8", errors="replace").read()
    last = None
    for attempt in range(5):
        try:
            r = sess.get(url, timeout=300)
            if r.status_code == 404:
                raise FileNotFoundError(f"404 {url}")
            r.raise_for_status()
            text = r.text
            if cache_raw:
                os.makedirs(RAW, exist_ok=True)
                open(raw_path, "w", encoding="utf-8").write(text)
            return text
        except FileNotFoundError:
            raise
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"download failed after retries: {url} ({last})")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main():
    args = sys.argv[1:]
    sess = make_session()

    if "--refresh" in args:
        files = refresh_catalog(sess)
    else:
        files = load_catalog()

    dry = "--dry" in args
    limit = None
    if dry:
        try:
            limit = int(args[args.index("--dry") + 1])
        except (ValueError, IndexError):
            limit = 5
        files = files[:limit]

    do_catalog = "--catalog" in args

    print(f"{'DRY-RUN' if dry else 'FULL'}: {len(files)} Zillow CSV datasets "
          f"-> {OUT}", flush=True)
    if not dry:
        os.makedirs(OUT, exist_ok=True)
        os.makedirs(RAW, exist_ok=True)

    db = fts = None
    if do_catalog and not dry:
        from core import catalog
        from connectors.base import SeriesMeta  # noqa: F401
        db = catalog.connect()
        fts = catalog.init(db)
        catalog.upsert_source(db, "zillow", "Zillow Research", LICENSE_ID,
                              ATTRIBUTION, RESEARCH_PAGE)

    summaries = []
    dead = []
    n_ok = n_obs_total = n_series_total = 0
    t0 = time.time()

    for i, rec in enumerate(files):
        url = rec["url"]
        dataset = _dataset_name(url)
        try:
            text = fetch_csv_text(sess, url, cache_raw=not dry)
        except FileNotFoundError:
            dead.append({"url": url, "reason": "404"})
            print(f"  [{i+1}/{len(files)}] DEAD 404  {dataset}", flush=True)
            continue
        except Exception as e:  # noqa: BLE001
            dead.append({"url": url, "reason": str(e)})
            print(f"  [{i+1}/{len(files)}] ERROR     {dataset}: {e}", flush=True)
            continue

        try:
            obs_rows, series_rows = parse_csv(text, url)
        except Exception as e:  # noqa: BLE001
            dead.append({"url": url, "reason": f"parse: {e}"})
            print(f"  [{i+1}/{len(files)}] PARSE-ERR {dataset}: {e}", flush=True)
            continue

        if not obs_rows:
            print(f"  [{i+1}/{len(files)}] EMPTY     {dataset} (0 obs)", flush=True)
            # still record the (empty) series rows? skip writing empty cube
            continue

        if dry:
            uniq = len({r[0] for r in obs_rows})
            sample = obs_rows[0]
            print(f"  [{i+1}/{len(files)}] {dataset:62} series={uniq:>6,} "
                  f"obs={len(obs_rows):>9,} sample={sample}", flush=True)
            n_ok += 1
            n_obs_total += len(obs_rows)
            n_series_total += uniq
            continue

        meta = write_cube(dataset, obs_rows, series_rows, rec)
        summaries.append(meta)
        n_ok += 1
        n_obs_total += meta["n_obs"]
        n_series_total += meta["n_series"]

        if do_catalog and db is not None:
            from connectors.base import SeriesMeta
            sm = SeriesMeta(
                series_id=f"zillow:{meta['metric']}:{meta['geo_level']}",
                title=f"Zillow {rec.get('type', meta['metric'])} [{rec.get('geo', meta['geo_level'])}]",
                frequency="W" if dataset.endswith("week") else "M",
                unit=None, geography="US", category="housing",
                license_id=LICENSE_ID,
                metadata={"dataset": dataset, "grouped": True, "metric": meta["metric"],
                          "geo_level": meta["geo_level"], "n_series": meta["n_series"],
                          "n_obs": meta["n_obs"], "source_url": url,
                          "attribution": ATTRIBUTION},
            )
            catalog.upsert_series(db, sm, start=meta["start"], end=meta["end"])

        flag = "" if meta["verify_ok"] else "  !! VERIFY MISMATCH"
        print(f"  [{i+1}/{len(files)}] OK  {dataset:62} "
              f"series={meta['n_series']:>6,} obs={meta['n_obs']:>9,} "
              f"[{meta['start']}..{meta['end']}]{flag}", flush=True)

        if not dry and (i + 1) % 40 == 0:
            if db is not None:
                db.commit()
            el = time.time() - t0
            print(f"  --- progress {i+1}/{len(files)}: {n_ok} cubes, "
                  f"{n_obs_total:,} obs, {el:,.0f}s ---", flush=True)

    if do_catalog and db is not None and not dry:
        db.commit()
        catalog.rebuild_fts(db, fts)

    if not dry:
        top = {
            "source_id": "zillow",
            "name": "Zillow Research",
            "license_id": LICENSE_ID,
            "attribution": ATTRIBUTION,
            "homepage": RESEARCH_PAGE,
            "n_catalog_urls": len(files),
            "n_datasets_written": len(summaries),
            "n_dead_urls": len(dead),
            "dead": dead,
            "total_obs": n_obs_total,
            "total_series": n_series_total,
            "generated": dt.datetime.now().isoformat(timespec="seconds"),
            "datasets": summaries,
        }
        with open(os.path.join(OUT, "zillow.meta.json"), "w", encoding="utf-8") as f:
            json.dump(top, f, indent=2)

    el = time.time() - t0
    print(f"\n{'DRY' if dry else 'DONE'}: {n_ok} datasets, "
          f"{n_series_total:,} series, {n_obs_total:,} observations, "
          f"{len(dead)} dead URLs, {el:,.0f}s", flush=True)
    if dead:
        print("DEAD URLs:")
        for d in dead:
            print("   ", d["reason"], d["url"], flush=True)


if __name__ == "__main__":
    main()
