#!/usr/bin/env python3
"""Grouped ingest of the Eurostat bulk TSVs.

ONE Parquet per dataset (all dimension-combination series inside: columns
series_key, obs_date, value) + ONE coarse catalog row per dataset. cc-by-4.0.
Carve-outs (non-EU country data / some trade) are applied later at serve time.

Eurostat SDMX-TSV: header first cell = "<dim1,dim2,...>\\TIME_PERIOD" then tab-separated
period labels; each data row begins with the comma-joined dimension values, then
tab-separated values that may carry a flag (space + letters) or ":" for missing.

Usage:
  python jobs/ingest_eurostat.py --dry 5     # parse 5 files, print, no writes
  python jobs/ingest_eurostat.py             # full run
"""
import datetime as dt
import glob
import gzip
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # derived, never hardcoded
sys.path.insert(0, ROOT)
RAW = os.path.join(ROOT, "data", "raw", "eurostat")
OUT = os.path.join(ROOT, "data", "clean_grouped", "eurostat")


def parse_period(p):
    p = p.strip()
    try:
        if "-Q" in p:
            y, q = p.split("-Q"); return dt.date(int(y), (int(q) - 1) * 3 + 1, 1)
        if "-S" in p:
            y, s = p.split("-S"); return dt.date(int(y), 1 if s == "1" else 7, 1)
        if "-W" in p:
            y, w = p.split("-W"); return dt.date.fromisocalendar(int(y), int(w), 1)
        if "-" in p:
            parts = p.split("-")
            if len(parts) == 2:
                return dt.date(int(parts[0]), int(parts[1]), 1)
            if len(parts) == 3:
                return dt.date(int(parts[0]), int(parts[1]), int(parts[2]))
        if "M" in p:
            y, m = p.split("M"); return dt.date(int(y), int(m), 1)
        if p.isdigit() and len(p) == 4:
            return dt.date(int(p), 12, 31)
    except (ValueError, KeyError):
        return None
    return None


def parse_value(cell):
    v = cell.strip()
    if not v or v.startswith(":"):
        return None
    tok = v.split()[0]
    if tok in ("", ":", "-"):
        return None
    try:
        return float(tok)
    except ValueError:
        return None


def parse_file(path):
    """Yield (series_key, obs_date, value) for one dataset .tsv.gz."""
    with gzip.open(path, "rt", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        periods = [c.strip() for c in header[1:]]
        pdates = [parse_period(p) for p in periods]
        for line in f:
            cells = line.rstrip("\n").split("\t")
            key = cells[0].strip()
            for i, cell in enumerate(cells[1:]):
                if i >= len(pdates):
                    break
                od = pdates[i]
                if od is None:
                    continue
                val = parse_value(cell)
                if val is None:
                    continue
                yield key, od, val


def load_titles():
    titles = {}
    toc = os.path.join(RAW, "_toc.txt")
    if os.path.exists(toc):
        with open(toc, encoding="utf-8") as f:
            for line in f.read().splitlines()[1:]:
                parts = [p.strip().strip('"') for p in line.split("\t")]
                if len(parts) >= 2 and parts[1]:
                    titles[parts[1]] = parts[0]
    return titles


def main():
    files = sorted(glob.glob(os.path.join(RAW, "*.tsv.gz")))
    dry = "--dry" in sys.argv
    limit = int(sys.argv[sys.argv.index("--dry") + 1]) if dry else None
    if limit:
        files = files[:limit]
    titles = load_titles()
    print(f"{'DRY-RUN' if dry else 'FULL'}: {len(files)} dataset files", flush=True)

    if not dry:
        os.makedirs(OUT, exist_ok=True)
        from core import catalog
        from connectors.base import SeriesMeta
        db = catalog.connect()
        fts = catalog.init(db)
        catalog.upsert_source(db, "eurostat", "Eurostat", "cc-by-4.0",
                              "Source: Eurostat (CC BY 4.0)", "https://ec.europa.eu/eurostat")

    n_ds = n_obs = 0
    for path in files:
        code = os.path.basename(path)[:-7]  # strip .tsv.gz
        keys, dates, vals = [], [], []
        try:
            for k, od, v in parse_file(path):
                keys.append(k); dates.append(od); vals.append(v)
        except Exception as e:  # noqa: BLE001
            print(f"  {code}: parse error {e}", flush=True)
            continue
        if not keys:
            continue
        if dry:
            uniq = len(set(keys))
            print(f"  {code:28} series={uniq:>6,} obs={len(keys):>8,} sample=({keys[0]}, {dates[0]}, {vals[0]})", flush=True)
        else:
            tbl = pa.table({"series_key": keys, "obs_date": pa.array(dates, type=pa.date32()), "value": vals})
            pq.write_table(tbl, os.path.join(OUT, code + ".parquet"))
            meta = SeriesMeta(f"eurostat:{code}", titles.get(code, code), "irregular", None, "EU",
                              "macro", "cc-by-4.0", {"dataset": code, "grouped": True,
                                                     "n_series": len(set(keys)), "n_obs": len(keys)})
            catalog.upsert_series(db, meta, start=str(min(dates)), end=str(max(dates)))
        n_ds += 1
        n_obs += len(keys)
        if not dry and n_ds % 500 == 0:
            db.commit()
            print(f"  {n_ds} datasets, {n_obs:,} obs", flush=True)

    if not dry:
        db.commit()
        catalog.rebuild_fts(db, fts)
    print(f"{'DRY' if dry else 'DONE'}: {n_ds:,} datasets / {n_obs:,} observations", flush=True)


if __name__ == "__main__":
    main()
