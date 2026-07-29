"""tools/flowgrain_ons_uk.py — dataset-grain catalog + CSV publish for ons_uk.

WHY DATASET GRAIN. After the re-key (tools/rekey_ons_uk.py) ons_uk holds 3,897,884 genuine
series across 42 datasets. Per-series would mean 3.9M catalog rows and ~43 hours of derive,
and — the deciding point — those keys are opaque dimension codes
(`administrative-geography=K02000001:sic-unofficial=14:...`) with no per-series title
anywhere in ONS's API. Cataloguing 3.9M rows titled with their own key is code soup: it
bloats search without making one series findable. The DATASET is the unit ONS actually
names, so that is the unit we publish, exactly as cso is published per table and
insee_melodi per dataflow.

TITLES ARE ONS'S OWN, from the /v1/datasets catalogue walk. The fallback is the dataset id,
which is honest if terse; nothing is invented.

LICENCE: OGL v3.0, CLEARED. The terms say OGL covers "MOST content ... Some content is
exempt", and that carve-out was resolved in DATABASE_LICENSES_VERBATIM.md — the exemptions
are "photographs, illustrations and videos" under third-party image licences, which cannot
reach a statistical series. Attribution is required and is carried on every download.

Reads the parquets from R2 (the published store), so what is catalogued is what is served.

  python tools/flowgrain_ons_uk.py --dry-run
  python tools/flowgrain_ons_uk.py --catalog --upload
"""
from __future__ import annotations
import argparse, csv, io, os, sqlite3, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
CATALOG = os.environ.get("ECONDL_CATALOG") or os.path.join(ROOT, "data", "catalog.db")
BUCKET = "econ-data"
SOURCE = "ons_uk"
PREFIX = f"clean_full/{SOURCE}/"


def ons_titles() -> dict:
    """{dataset_id -> ONS title} from ONS's own catalogue walk."""
    from jobs import ingest_ons_uk as ig
    out = {}
    for d in ig.get_all_datasets():
        i, t = d.get("id"), (d.get("title") or "").strip()
        if i and t:
            out[i] = t
    return out


def build_csv(tbl):
    keys = tbl.column("series_key").to_pylist()
    dates = tbl.column("obs_date").to_pylist()
    vals = tbl.column("value").to_pylist()
    rows = [(k, d.isoformat(), v) for k, d, v in zip(keys, dates, vals)
            if d is not None and v is not None]
    rows.sort(key=lambda r: (r[0], r[1]))
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    w.writerows(rows)
    if not rows:
        return buf.getvalue().encode(), 0, None, None, 0
    return (buf.getvalue().encode(), len(rows),
            min(r[1] for r in rows), max(r[1] for r in rows),
            len({r[0] for r in rows}))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--catalog", action="store_true")
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--threads", type=int, default=3)
    a = ap.parse_args()
    if not (a.dry_run or a.catalog or a.upload):
        ap.error("pick --dry-run, --catalog and/or --upload")

    from core import r2_util
    c = r2_util.client(write=bool(a.upload))
    titles = ons_titles()

    objs, tok = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": PREFIX, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = c.list_objects_v2(**kw)
        objs += [(o["Key"].split("/")[-1][:-8], o["Key"]) for o in r.get("Contents", [])
                 if o["Key"].endswith(".parquet")]
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    objs.sort()
    print(f"datasets in R2: {len(objs)}   ONS titles available: {len(titles)}", flush=True)

    lic = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True).execute(
        "SELECT license_id FROM source WHERE source_id=?", (SOURCE,)).fetchone()
    if not lic or not lic[0]:
        raise SystemExit(f"{SOURCE} has no source/licence row — refusing to catalogue")
    lic = lic[0]

    rows_out, stats, put_ok = [], [], 0
    t0 = time.time()

    def one(ds, key):
        body = c.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        tbl = pq.read_table(io.BytesIO(body), columns=["series_key", "obs_date", "value"])
        return (ds,) + build_csv(tbl)

    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        for fu in as_completed([ex.submit(one, d, k) for d, k in objs]):
            ds, body, n, mn, mx, nk = fu.result()
            title = titles.get(ds) or ds
            stats.append((ds, n, nk, len(body), ds in titles))
            sid = f"{SOURCE}:{ds}"
            rows_out.append((sid, SOURCE, title[:500], None, None, None, None, lic,
                             mn, mx, None, "{}"))
            if a.upload:
                k = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
                for att in range(6):
                    try:
                        c.put_object(Bucket=BUCKET, Key=k, Body=body, ContentType="text/csv")
                        put_ok += 1
                        break
                    except Exception:                          # noqa: BLE001
                        if att == 5:
                            raise
                        time.sleep(2 ** att)

    print(f"datasets={len(stats)}  rows={sum(s[1] for s in stats):,}  "
          f"series={sum(s[2] for s in stats):,}  csv={sum(s[3] for s in stats)/1e9:.2f} GB  "
          f"titled_by_ONS={sum(1 for s in stats if s[4])}/{len(stats)}  "
          f"{time.time()-t0:.0f}s", flush=True)
    for d, n, nk, b, _t in sorted(stats, key=lambda s: -s[3])[:5]:
        print(f"   {d[:34]:34s} rows={n:>9,} series={nk:>8,} csv={b/1e6:>7.1f} MB", flush=True)

    if a.catalog:
        conn = sqlite3.connect(CATALOG, timeout=180)
        conn.execute("DELETE FROM series WHERE source_id=?", (SOURCE,))
        conn.executemany(
            "INSERT OR REPLACE INTO series (series_id,source_id,title,frequency,unit,"
            "geography,category,license_id,start_date,end_date,last_updated,metadata) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows_out)
        conn.execute("DELETE FROM series_fts WHERE series_id LIKE ?", (SOURCE + ":%",))
        conn.execute("INSERT INTO series_fts(series_id,title,geography) "
                     "SELECT series_id,title,geography FROM series WHERE source_id=?", (SOURCE,))
        conn.commit(); conn.close()
        print(f"catalogued {len(rows_out)} dataset rows (licence {lic})", flush=True)
    if a.upload:
        print(f"uploaded {put_ok} CSVs", flush=True)


if __name__ == "__main__":
    main()
