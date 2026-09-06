"""Measure every uncataloged data-bearing source: key column, file count, distinct
series count, total rows. Drives the catalog-broadening grain policy (per-series vs
cap/defer). Reads only the key column (cheap). Writes dist/broaden/measure.json.

A SOURCE WITH ONE CATALOGUE ROW USED TO BE SKIPPED AS "CATALOGED" (R834). That is how
`abs` - 18 catalogue rows over 376,333,085 distinct store keys - and `bls` - 9 over
154,190,127 - stayed invisible to the measure that decides what to catalogue next. Both
are SERIES grain with exact-key resolvers, so those keys are reachable by nobody: an id
absent from the catalogue 404s (api/worker/src/series.ts:39). Between abs, bls and bis
that is 532,044,393 series this tool was structurally unable to see.

The skip is still the cheap default - measuring a giant costs a full key-column scan, 662 s
for abs alone - but it is now RECORDED AND PRINTED, never silent, and `--include-cataloged`
measures the skipped sources too. A catalogue row count is not evidence of coverage in
either direction (R525): bls has 9 rows and is series grain; wid has 2,465,197 rows with
neither a frequency nor a geography attribute and each still names one series.
"""
from __future__ import annotations
import argparse, glob, json, os, sqlite3, time
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
STORE = os.path.join(ROOT, "data", "clean_full")
OUTDIR = os.path.join(ROOT, "dist", "broaden")
PROTECTED = {"cbs_nl", "gus_dbw", "dbnomics"}  # running backfills — never touch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--include-cataloged", action="store_true",
                    help="also measure sources that already hold catalogue rows. They are "
                         "skipped by default because a full key-column scan of a giant is "
                         "expensive - NOT because holding a row means being covered "
                         "(R834: abs held 18 rows over 376,333,085 store keys).")
    a = ap.parse_args()
    os.makedirs(OUTDIR, exist_ok=True)
    cat = sqlite3.connect("data/catalog.db") if os.path.exists("data/catalog.db") else \
          sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    # the row COUNT, not just membership: it is one group-by and it is what makes the
    # skip legible instead of a bare "already done".
    cat_rows = dict(cat.execute("SELECT source_id, count(*) FROM series GROUP BY 1"))
    cataloged = set(cat_rows)
    cat.close()

    out, skipped_cataloged = [], []
    for d in sorted(os.listdir(STORE)):
        p = os.path.join(STORE, d)
        if not os.path.isdir(p) or d.startswith("_") or d in PROTECTED:
            continue
        if d in cataloged and not a.include_cataloged:
            # RECORDED, NEVER SILENT. This branch is a COST choice, not a verdict, and
            # printing it is the difference between "not measured" and "clean".
            skipped_cataloged.append((d, cat_rows[d]))
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
    if skipped_cataloged:
        print(f"\nNOT MEASURED - {len(skipped_cataloged)} source(s) skipped because they "
              f"already hold catalogue rows.\n  That is a COST choice, not a clean bill. A "
              f"row count is not evidence of coverage:\n  abs held 18 rows over 376,333,085 "
              f"store keys and bls 9 over 154,190,127, both SERIES\n  grain, and this skip "
              f"is why neither was ever measured here (R834/R525).\n  Re-run with "
              f"--include-cataloged to measure them.")
        for d, n in sorted(skipped_cataloged, key=lambda x: x[1]):
            print(f"   {d:24s} catalogue rows {n:>10,}")


if __name__ == "__main__":
    main()
