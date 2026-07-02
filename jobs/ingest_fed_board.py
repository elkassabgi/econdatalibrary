#!/usr/bin/env python3
"""FULL-COVERAGE ingest of the U.S. Federal Reserve Board Data Download Program (DDP).

Source: https://www.federalreserve.gov/datadownload/  -- the Fed's bulk-download
facility. For EVERY statistical release the DDP publishes a single ZIP package
containing the entire release as one SDMX-compact XML file:

    https://www.federalreserve.gov/datadownload/Output.aspx?rel=<REL>&filetype=zip

The 18 releases that exist (enumerated from the DDP home page's Choose.aspx?rel=
links on 2026-06-03):
    CHGDEL CP DSR E2 FOR G17 G19 G20 H10 H15 H3 H41 H6 H8 PRATES SCOOS SLOOS Z1

SDMX layout (compact schema):
    <frb:DataSet id="...">
      <kf:Series SERIES_NAME="..." FREQ="9" UNIT="..." ...dimension attrs...>
        <frb:Annotations>... Short/Long Description ...</frb:Annotations>
        <frb:Obs OBS_STATUS="A" OBS_VALUE="1.13" TIME_PERIOD="1954-07-01"/>
        ...
      </kf:Series>
    </frb:DataSet>
A release may contain several DataSets (e.g. CP -> RATES/VOL/OUTST...; G17 -> 14
IP_* cubes). SERIES_NAME is unique within a DataSet but can repeat ACROSS DataSets
in the same release, so the unique series key is (dataset, series_name).
OBS_STATUS="ND" with OBS_VALUE="-9999" means "no data published" -> skipped.

GROUPED storage (mirrors jobs/ingest_eurostat.py / ingest_bls.py): ONE Parquet per
RELEASE -> data/clean_full/fed_board/<REL>.parquet with a `dataset` + `series_key`
column inside (NOT one file per series). A sidecar <REL>__series.parquet holds
one metadata row per series (freq, unit, descriptions, full dimension dict as JSON).
Memory is bounded via lxml.iterparse with el.clear() and row-group batch flushing
-- essential for Z.1 (590 MB XML, ~40k series).

License: us-public-domain (configs/sources.yaml -> fed_board).

Usage:
  python jobs/ingest_fed_board.py --probe          # parse 3 small releases, no big ones
  python jobs/ingest_fed_board.py --only H15,Z1    # one/few releases
  python jobs/ingest_fed_board.py                   # full run (all 18 releases)
  python jobs/ingest_fed_board.py --skip-download   # reuse already-downloaded ZIPs
  python jobs/ingest_fed_board.py --force           # ignore resume sidecars, redo
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import sys
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import etree

ROOT = r"D:/research/econfindatalibrary"
RAW = os.path.join(ROOT, "data", "raw", "fed_board")
OUT = os.path.join(ROOT, "data", "clean_full", "fed_board")
OUTPUT_URL = "https://www.federalreserve.gov/datadownload/Output.aspx"
DDP_HOME = "https://www.federalreserve.gov/datadownload/"
UA = "Econ-Fin Data Library admin@hfdatalibrary.com"
LICENSE_ID = "us-public-domain"
BATCH = 1_000_000          # observation rows per Parquet row-group flush
CHUNK = 1 << 20            # 1 MiB download chunks
MISSING_VALUE = -9999.0    # DDP sentinel for "no data" (paired with OBS_STATUS=ND)

# Canonical, complete release list (verified each resolves to a bulk ZIP on
# 2026-06-03). Kept as a constant so the run is deterministic; `discover_releases()`
# re-scrapes the DDP home page and warns if the live set ever diverges.
RELEASES = [
    "CHGDEL", "CP", "DSR", "E2", "FOR", "G17", "G19", "G20", "H10",
    "H15", "H3", "H41", "H6", "H8", "PRATES", "SCOOS", "SLOOS", "Z1",
]

# SDMX namespaces used by the Fed compact schema.
NS_MSG = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/message"
NS_COMMON = "http://www.SDMX.org/resources/SDMXML/schemas/v1_0/common"
NS_FRB = "http://www.federalreserve.gov/structure/compact/common"
Q_OBS = "{%s}Obs" % NS_FRB
Q_ANNOTATION = "{%s}Annotation" % NS_COMMON
Q_ANN_TYPE = "{%s}AnnotationType" % NS_COMMON
Q_ANN_TEXT = "{%s}AnnotationText" % NS_COMMON

# Numeric FREQ code -> single-letter frequency tag (decoded from a release struct
# CL_FREQ codelist). We only need a coarse bucket for the catalog; the exact code
# is preserved in the series-metadata sidecar.
FREQ_TAG = {
    "0": "irregular",
    "8": "D", "9": "D",                                   # daily / business day
    "16": "W", "17": "W", "18": "W", "19": "W", "20": "W", "21": "W", "22": "W",
    "32": "irregular",                                    # ten-day
    "64": "W", "65": "W", "66": "W", "67": "W", "68": "W", "69": "W", "70": "W",
    "71": "W", "72": "W", "73": "W", "74": "W", "75": "W", "76": "W", "77": "W",
    "128": "M", "129": "M",                               # twice-monthly / monthly
    "144": "M", "145": "M",                               # bi-monthly
    "160": "Q", "161": "Q", "162": "Q",                   # quarterly
    "192": "A", "193": "A", "194": "A", "195": "A", "196": "A", "197": "A",
    "198": "A", "199": "A", "200": "A", "201": "A", "202": "A", "203": "A",
    "204": "S", "205": "S", "206": "S", "207": "S", "208": "S", "209": "S",
}

os.makedirs(RAW, exist_ok=True)
os.makedirs(OUT, exist_ok=True)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

OBS_SCHEMA = pa.schema([
    ("dataset", pa.string()),       # SDMX DataSet id within the release
    ("series_key", pa.string()),    # SERIES_NAME (unique within a dataset)
    ("obs_date", pa.date32()),
    ("value", pa.float64()),
    ("obs_status", pa.string()),    # 'A' actual, 'NA', etc. (ND missing rows dropped)
])

META_SCHEMA = pa.schema([
    ("dataset", pa.string()),
    ("series_key", pa.string()),
    ("freq", pa.string()),          # coarse tag D/W/M/Q/S/A/irregular
    ("freq_code", pa.string()),     # raw numeric SDMX FREQ code
    ("unit", pa.string()),
    ("unit_mult", pa.string()),
    ("currency", pa.string()),
    ("short_desc", pa.string()),
    ("long_desc", pa.string()),
    ("n_obs", pa.int64()),          # observations actually kept for this series
    ("start", pa.string()),
    ("end", pa.string()),
    ("dimensions", pa.string()),    # full attribute dict as JSON (all SDMX dims)
])


# ----------------------------------------------------------------------------
# enumerate the catalog
# ----------------------------------------------------------------------------
def discover_releases() -> list[str]:
    """Re-scrape the DDP home page for all Choose.aspx?rel=<REL> codes.

    Returns the live list; the caller compares it to the pinned RELEASES constant
    and warns on any divergence so we never silently miss a newly-added release.
    """
    import re
    try:
        r = SESSION.get(DDP_HOME, timeout=60)
        r.raise_for_status()
        live = sorted(set(re.findall(r"[Cc]hoose\.aspx\?rel=([A-Za-z0-9._]+)", r.text)))
        return live
    except requests.RequestException as e:  # noqa: BLE001
        print(f"  (could not re-scrape DDP home: {e}; using pinned list)", flush=True)
        return list(RELEASES)


# ----------------------------------------------------------------------------
# download with retry/backoff
# ----------------------------------------------------------------------------
def download_zip(rel: str, dest: str) -> int:
    """Stream a release's bulk ZIP to dest with retry/backoff.

    Reuses an existing file only if it is a valid, openable ZIP that contains the
    expected <REL>_data.xml (guards against a half-written or HTML-error file from
    a prior interrupted run).
    """
    if _valid_zip(dest, rel):
        return os.path.getsize(dest)
    last = None
    for attempt in range(5):
        try:
            with SESSION.get(OUTPUT_URL, params={"rel": rel, "filetype": "zip"},
                             stream=True, timeout=600) as r:
                r.raise_for_status()
                tmp = dest + ".part"
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_content(CHUNK):
                        if chunk:
                            fh.write(chunk)
                os.replace(tmp, dest)
            if not _valid_zip(dest, rel):
                raise IOError("downloaded file is not a valid release ZIP")
            return os.path.getsize(dest)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(min(60, 3 * (attempt + 1) ** 2))
    raise RuntimeError(f"download failed for rel={rel}: {last}")


def _valid_zip(path: str, rel: str) -> bool:
    if not os.path.exists(path) or os.path.getsize(path) < 200:
        return False
    try:
        with zipfile.ZipFile(path) as z:
            return any(n.lower().endswith("_data.xml") for n in z.namelist())
    except (zipfile.BadZipFile, OSError):
        return False


# ----------------------------------------------------------------------------
# parse helpers
# ----------------------------------------------------------------------------
def parse_period(p: str):
    """SDMX TIME_PERIOD -> date. Handles YYYY-MM-DD, YYYY-MM, YYYY."""
    p = p.strip()
    if not p:
        return None
    try:
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            return dt.date(int(p[:4]), int(p[5:7]), int(p[8:10]))
        if len(p) == 7 and p[4] == "-":              # YYYY-MM -> first of month
            return dt.date(int(p[:4]), int(p[5:7]), 1)
        if len(p) == 4 and p.isdigit():              # YYYY -> Dec 31
            return dt.date(int(p), 12, 31)
        # fallback: split on '-'
        parts = p.split("-")
        if len(parts) == 3:
            return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
        if len(parts) == 2:
            return dt.date(int(parts[0]), int(parts[1]), 1)
    except (ValueError, IndexError):
        return None
    return None


def parse_value(tok, status):
    """Return float value, or None if missing.

    Missing is signalled by OBS_STATUS='ND' and/or the -9999 sentinel; some rows
    carry an empty value. Anything non-numeric is treated as missing.
    """
    if tok is None:
        return None
    tok = tok.strip()
    if not tok:
        return None
    try:
        v = float(tok)
    except ValueError:
        return None
    if status == "ND" and v == MISSING_VALUE:
        return None
    # A bare -9999 with no status is still the documented sentinel.
    if v == MISSING_VALUE and (status in (None, "", "ND")):
        return None
    return v


def _series_descs(series_el):
    short = long = None
    for ann in series_el.iter(Q_ANNOTATION):
        t = ann.findtext(Q_ANN_TYPE)
        if t == "Short Description":
            short = ann.findtext(Q_ANN_TEXT)
        elif t == "Long Description":
            long = ann.findtext(Q_ANN_TEXT)
    return short, long


# ----------------------------------------------------------------------------
# parse one release ZIP -> grouped Parquet (streamed) + metadata sidecar
# ----------------------------------------------------------------------------
def ingest_release(rel: str, zip_path: str):
    """Stream-parse a release ZIP into <REL>.parquet (+ <REL>__series.parquet).

    Returns a dict of stats. Memory stays bounded: we iterparse the data XML and
    clear each <Series> after writing its rows; observation rows flush to the
    Parquet writer in BATCH-sized row groups.
    """
    with zipfile.ZipFile(zip_path) as z:
        data_name = [n for n in z.namelist() if n.lower().endswith("_data.xml")][0]
        # Stream the (possibly 590 MB) XML straight from the zip without holding the
        # decompressed bytes in a Python object longer than necessary.
        xml_stream = z.open(data_name, "r")

        obs_path = os.path.join(OUT, rel + ".parquet")
        writer = pq.ParquetWriter(obs_path, OBS_SCHEMA, compression="zstd")

        ds_b, key_b, date_b, val_b, st_b = [], [], [], [], []
        meta_rows = []
        n_obs = 0
        n_series_total = 0          # every <Series> element (incl. all-missing)
        n_series_with_data = 0
        n_baddate = n_missing = 0
        datasets = {}
        cur_ds = None
        mn = mx = None

        def flush():
            nonlocal ds_b, key_b, date_b, val_b, st_b
            if not ds_b:
                return
            tbl = pa.table({
                "dataset": ds_b,
                "series_key": key_b,
                "obs_date": pa.array(date_b, type=pa.date32()),
                "value": pa.array(val_b, type=pa.float64()),
                "obs_status": st_b,
            }, schema=OBS_SCHEMA)
            writer.write_table(tbl)
            ds_b, key_b, date_b, val_b, st_b = [], [], [], [], []

        ctx = etree.iterparse(xml_stream, events=("start", "end"))
        for ev, el in ctx:
            tag = etree.QName(el).localname
            if ev == "start" and tag == "DataSet":
                cur_ds = el.get("id")
                datasets.setdefault(cur_ds, 0)
            elif ev == "end" and tag == "Series":
                n_series_total += 1
                a = dict(el.attrib)
                key = a.get("SERIES_NAME") or f"_unnamed_{n_series_total}"
                datasets[cur_ds] = datasets.get(cur_ds, 0) + 1
                freq_code = a.get("FREQ", "")
                short, long = _series_descs(el)
                s_n = 0
                s_mn = s_mx = None
                for ob in el.iter(Q_OBS):
                    status = ob.get("OBS_STATUS")
                    od = parse_period(ob.get("TIME_PERIOD", ""))
                    if od is None:
                        n_baddate += 1
                        continue
                    v = parse_value(ob.get("OBS_VALUE"), status)
                    if v is None:
                        n_missing += 1
                        continue
                    ds_b.append(cur_ds)
                    key_b.append(key)
                    date_b.append(od)
                    val_b.append(v)
                    st_b.append(status)
                    n_obs += 1
                    s_n += 1
                    if s_mn is None or od < s_mn:
                        s_mn = od
                    if s_mx is None or od > s_mx:
                        s_mx = od
                    if len(ds_b) >= BATCH:
                        flush()
                if s_n:
                    n_series_with_data += 1
                    if mn is None or s_mn < mn:
                        mn = s_mn
                    if mx is None or s_mx > mx:
                        mx = s_mx
                # one metadata row per series (even if all obs were missing)
                dims = {k: v for k, v in a.items()
                        if k not in ("SERIES_NAME", "UNIT", "UNIT_MULT", "CURRENCY", "FREQ")}
                meta_rows.append({
                    "dataset": cur_ds,
                    "series_key": key,
                    "freq": FREQ_TAG.get(freq_code, "irregular"),
                    "freq_code": freq_code,
                    "unit": a.get("UNIT"),
                    "unit_mult": a.get("UNIT_MULT"),
                    "currency": a.get("CURRENCY"),
                    "short_desc": short,
                    "long_desc": long,
                    "n_obs": s_n,
                    "start": str(s_mn) if s_mn else None,
                    "end": str(s_mx) if s_mx else None,
                    "dimensions": json.dumps(dims, separators=(",", ":")) if dims else None,
                })
                # free the whole subtree (incl. obs + annotations) to bound memory
                el.clear()
                # also drop now-empty preceding siblings to keep the tree small
                while el.getprevious() is not None:
                    del el.getparent()[0]
        flush()
        writer.close()
        xml_stream.close()

    # write the metadata sidecar (one row per series)
    meta_path = os.path.join(OUT, rel + "__series.parquet")
    if meta_rows:
        mtbl = pa.Table.from_pylist(meta_rows, schema=META_SCHEMA)
        pq.write_table(mtbl, meta_path, compression="zstd")

    if n_obs == 0:
        # nothing kept -> remove empty obs file but keep meta for the record
        try:
            os.remove(obs_path)
        except OSError:
            pass

    return {
        "n_obs": n_obs,
        "n_series_total": n_series_total,
        "n_series_with_data": n_series_with_data,
        "datasets": datasets,
        "n_datasets": len(datasets),
        "start": str(mn) if mn else None,
        "end": str(mx) if mx else None,
        "bad_date": n_baddate,
        "missing_obs": n_missing,
    }


# ----------------------------------------------------------------------------
# verification: re-read the written Parquet and recount
# ----------------------------------------------------------------------------
def verify_release(rel: str) -> dict:
    obs_path = os.path.join(OUT, rel + ".parquet")
    if not os.path.exists(obs_path):
        return {"rows": 0, "series": 0}
    pf = pq.ParquetFile(obs_path)
    rows = pf.metadata.num_rows
    # count distinct (dataset, series_key) cheaply via a set over those two columns
    seen = set()
    for batch in pf.iter_batches(columns=["dataset", "series_key"], batch_size=500_000):
        ds = batch.column(0).to_pylist()
        sk = batch.column(1).to_pylist()
        seen.update(zip(ds, sk))
    return {"rows": rows, "series": len(seen)}


# ----------------------------------------------------------------------------
# driver
# ----------------------------------------------------------------------------
def main():
    only = None
    if "--only" in sys.argv:
        only = set(sys.argv[sys.argv.index("--only") + 1].split(","))
    probe = "--probe" in sys.argv
    skip_dl = "--skip-download" in sys.argv
    force = "--force" in sys.argv

    live = discover_releases()
    pinned = set(RELEASES)
    if set(live) != pinned:
        print(f"NOTE: live DDP releases {sorted(live)} differ from pinned "
              f"{sorted(pinned)}", flush=True)
        # union so we never miss a newly added one
        releases = sorted(pinned | set(live))
    else:
        releases = list(RELEASES)
    print(f"CATALOG: {len(releases)} releases in the Fed DDP: {releases}", flush=True)

    if probe:
        releases = ["DSR", "FOR", "PRATES"]   # small, fast, exercise multi-dataset+missing
    if only:
        releases = [r for r in releases if r in only]

    grand_obs = grand_series = 0
    summary = {}
    print(f"{'PROBE' if probe else 'RUN'}: {len(releases)} releases -> {OUT}", flush=True)

    for rel in releases:
        meta_path = os.path.join(OUT, rel + ".meta.json")
        obs_path = os.path.join(OUT, rel + ".parquet")
        if not force and os.path.exists(meta_path):
            prev = json.load(open(meta_path))
            summary[rel] = prev
            grand_obs += prev["n_obs"]
            grand_series += prev["n_series_with_data"]
            print(f"{rel:8} (done already: obs={prev['n_obs']:,} "
                  f"series={prev['n_series_with_data']:,}) -- skip", flush=True)
            continue

        t0 = time.time()
        zip_path = os.path.join(RAW, rel + ".zip")
        dl_bytes = 0
        if not skip_dl:
            dl_bytes = download_zip(rel, zip_path)
        elif not _valid_zip(zip_path, rel):
            dl_bytes = download_zip(rel, zip_path)   # missing locally -> must fetch
        dl_s = time.time() - t0

        stats = ingest_release(rel, zip_path)

        # verify by re-reading the Parquet we just wrote
        ver = verify_release(rel)
        stats["verified_rows"] = ver["rows"]
        stats["verified_series"] = ver["series"]
        stats["dl_mb"] = round(dl_bytes / 1e6, 2)
        ok = (ver["rows"] == stats["n_obs"])
        stats["verify_ok"] = ok

        summary[rel] = stats
        grand_obs += stats["n_obs"]
        grand_series += stats["n_series_with_data"]
        json.dump(stats, open(meta_path, "w"), indent=2)

        print(
            f"{rel:8} datasets={stats['n_datasets']:>2} "
            f"series(pub)={stats['n_series_total']:>7,} "
            f"series(data)={stats['n_series_with_data']:>7,} "
            f"obs={stats['n_obs']:>12,} (verified={ver['rows']:>12,} {'OK' if ok else 'MISMATCH!'}) "
            f"{str(stats['start'])}..{str(stats['end'])} "
            f"dl={dl_s:5.0f}s/{stats['dl_mb']:.1f}MB parse={time.time()-t0-dl_s:6.0f}s "
            f"missing={stats['missing_obs']:,} baddate={stats['bad_date']}",
            flush=True,
        )

    json.dump(summary, open(os.path.join(OUT, "_summary.json"), "w"), indent=2)
    tot_pub = sum(v["n_series_total"] for v in summary.values())
    tot_verified = sum(v.get("verified_rows", 0) for v in summary.values())
    all_ok = all(v.get("verify_ok", False) for v in summary.values())
    print("=" * 78, flush=True)
    print(f"DONE: {len(summary)} releases / {grand_obs:,} observations "
          f"(verified {tot_verified:,}, {'ALL OK' if all_ok else 'SOME MISMATCH'}) / "
          f"{grand_series:,} series-with-data / {tot_pub:,} series published",
          flush=True)


if __name__ == "__main__":
    main()
