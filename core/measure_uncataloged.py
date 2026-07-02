"""Measure every uncataloged data-bearing source: key column, file count, distinct
series count, total rows. Drives the catalog-broadening grain policy (per-series vs
cap/defer). Reads only the key column (cheap). Writes dist/broaden/measure.json.
"""
from __future__ import annotations
import glob, json, os, sqlite3, time
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STORE = os.path.join(ROOT, "data", "clean_full")
OUTDIR = os.path.join(ROOT, "dist", "broaden")
PROTECTED = {"cbs_nl", "gus_dbw", "dbnomics"}  # running backfills — never touch


def main():
    os.makedirs(OUTDIR, exist_ok=True)
    cat = sqlite3.connect("data/catalog.db") if os.path.exists("data/catalog.db") else \
          sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    cataloged = {r[0] for r in cat.execute("SELECT DISTINCT source_id FROM series")}
    cat.close()

    out = []
    for d in sorted(os.listdir(STORE)):
        p = os.path.join(STORE, d)
        if not os.path.isdir(p) or d.startswith("_") or d in cataloged or d in PROTECTED:
            continue
        files = [f for f in glob.glob(os.path.join(p, "**", "*.parquet"), recursive=True)
                 if not os.path.basename(f).endswith("__series.parquet")]
        if not files:
            continue
        rec = {"source": d, "n_files": len(files)}
        try:
            cols = set(pq.read_schema(files[0]).names)
            key_col = "series_key" if "series_key" in cols else ("series_id" if "series_id" in cols else None)
            rec["has_long"] = bool(key_col and "obs_date" in cols and "value" in cols)
            rec["key_col"] = key_col
            if not rec["has_long"]:
                rec["note"] = "not uniform-long (relational/wide) -> needs explicit handling"
                out.append(rec); continue
            t0 = time.time()
            dset = ds.dataset(files)
            tbl = dset.to_table(columns=[key_col])
            col = tbl.column(key_col)
            rec["total_rows"] = tbl.num_rows
            rec["distinct_series"] = pc.count_distinct(col).as_py()
            rec["scan_s"] = round(time.time() - t0, 1)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:100]}"
        out.append(rec)
        r = out[-1]
        print(f"{d:22} files={r.get('n_files'):>5} "
              f"distinct={r.get('distinct_series','?'):>9} rows={r.get('total_rows','?'):>11} "
              f"{r.get('error') or r.get('note') or 'OK'}", flush=True)

    with open(os.path.join(OUTDIR, "measure.json"), "w") as f:
        json.dump(out, f, indent=2)
    long_ok = [r for r in out if r.get("has_long") and "distinct_series" in r]
    tot = sum(r["distinct_series"] for r in long_ok)
    print(f"\n{len(long_ok)} uniform-long sources measured; total distinct series = {tot:,}")
    for cap in (10_000, 50_000, 100_000):
        inc = [r for r in long_ok if r["distinct_series"] <= cap]
        print(f"  cap {cap:>7,}/source: {len(inc)} sources, "
              f"{sum(r['distinct_series'] for r in inc):,} series")


if __name__ == "__main__":
    main()
