"""tools/flowgrain_insee_melodi.py — flow-grain (per-dataflow) catalog + CSV publish for INSEE Melodi.

WHY FLOW GRAIN, not per-series. Melodi's natural unit is the dataflow: the flow code IS the
parquet name, and it is the only level at which INSEE gives us a real human title. Per-series
we would have 21,310,429 keys whose only available "title" would be the key itself —
`ACTIVITY=BE:FLORES_MEASURE=UNIT_LOC:GEO=2025-UU2020-29104:...` — because the dimension
values are opaque codes and Melodi exposes no codelist endpoint (checked: /codelist/all is
404, and a flow's catalog entry only points at a DSD name, `structure: {"dsd": "DSD_ICA"}`).
Cataloguing 21.3M rows of code soup would bloat search without making anything findable.

It also lets the source be hosted WHOLE. 84 of its 139 flows are single-period censuses —
DS_BPE* (facilities), DS_FLORES_* (establishments) — averaging 1.11 observations per key.
Those are honest cross-sectional micro-data, not broken keys (verified: the date is NOT in
the key, and those flows carry 1-2 distinct dates over millions of rows). Per series they
would be 20.8M one-point "series"; per flow they are 84 bulk files, which is what that kind
of data actually is. So nothing gets dropped for a ratio it was never going to meet.

TITLES ARE INSEE'S OWN. Taken from /melodi/dataflow/all (code -> {en, fr}); English
preferred, French next, and the bare flow code last. Nothing is invented.

READS FROM R2, not the local store: only 84 of the 139 flows exist locally, and publishing
from a partial store is how a source ends up half-hosted. AFTER T0 (self-hosting plan step 1) the
local store IS the published store, so it reads that (updater.blob.store_reader), and an empty
listing is refused - before, 0 flows with --catalog deleted every row of the source and wrote none.

  python tools/flowgrain_insee_melodi.py --dry-run          # scan + sizes, writes nothing
  python tools/flowgrain_insee_melodi.py --catalog          # write catalog.db rows
  python tools/flowgrain_insee_melodi.py --upload           # PUT CSVs to the CSV store (R2 before T0)
"""
from __future__ import annotations
import argparse, csv, io, json, os, sqlite3, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow.parquet as pq
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
CATALOG = os.environ.get("ECONDL_CATALOG") or os.path.join(ROOT, "data", "catalog.db")
BUCKET = "econ-data"
SOURCE = "insee_melodi"
PREFIX = f"clean_full/{SOURCE}/"
DATAFLOW = "https://api.insee.fr/melodi/dataflow/all"
UA = {"User-Agent": "EconDataLibrary/1.0 (Elkassabgi Data Library +https://econdatalibrary.com)",
      "Accept": "application/json"}


def flow_titles() -> dict:
    """{flow_code -> title} from INSEE's own dataflow catalogue. Never fabricated."""
    r = requests.get(DATAFLOW, headers=UA, timeout=120)
    r.raise_for_status()
    d = r.json()
    items = d if isinstance(d, list) else (d.get("dataflows") or d.get("data") or [])
    out = {}
    for e in items:
        code = e.get("code")
        lab = e.get("label") or {}
        if not code:
            continue
        if isinstance(lab, dict):
            t = lab.get("en") or lab.get("fr")
        else:
            t = str(lab) or None
        if t:
            out[code] = t.strip()
    return out


def list_flows(reader) -> list:
    """[(flow_code, key)] for every published parquet — the published store is the full set: R2 before T0,
    the local store after it (updater.blob.store_reader). An empty listing is refused by the caller."""
    return sorted((k.split("/")[-1][:-8], k) for k in reader.list_keys(PREFIX) if k.endswith(".parquet"))


def build_csv(tbl):
    """(csv_bytes, n_rows, min_iso, max_iso) — contract shape, sorted by (series_id, date)."""
    keys = tbl.column("series_key").to_pylist()
    dates = tbl.column("obs_date").to_pylist()
    vals = tbl.column("value").to_pylist()
    rows = [(k, d.isoformat(), v) for k, d, v in zip(keys, dates, vals)
            if d is not None and v is not None]
    rows.sort(key=lambda r: (r[0], r[1]))          # ISO dates sort chronologically
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["series_id", "obs_date", "value"])
    w.writerows(rows)
    if not rows:
        return buf.getvalue().encode(), 0, None, None
    return buf.getvalue().encode(), len(rows), min(r[1] for r in rows), max(r[1] for r in rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--catalog", action="store_true")
    ap.add_argument("--upload", action="store_true")
    ap.add_argument("--threads", type=int, default=8)
    a = ap.parse_args()
    if not (a.dry_run or a.catalog or a.upload):
        ap.error("pick --dry-run, --catalog and/or --upload")

    # plan step 1: the parquets are read from the published store (R2 before T0, the local store after it)
    # and the CSVs written to the CSV store (R2 before T0, the self-hosted blob store after it - which
    # holds no parquets). put_atomic gzips a series CSV through the shared helper.
    from updater import blob as _blob, derive as _derive                  # noqa: PLC0415
    reader = _blob.store_reader()
    store = _blob.csv_store(BUCKET) if a.upload else None
    titles = flow_titles()
    flows = list_flows(reader)
    print(f"flows in the published store: {len(flows)}   INSEE titles available: {len(titles)}", flush=True)
    if not flows:
        raise SystemExit(f"no parquets under {PREFIX} - refusing to report an empty source")

    lic = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True).execute(
        "SELECT license_id FROM source WHERE source_id=?", (SOURCE,)).fetchone()
    if not lic or not lic[0]:
        raise SystemExit(f"{SOURCE} has no source/licence row — refusing to catalogue")
    lic = lic[0]

    rows_out, stats = [], []
    t0 = time.time()

    def one(flow, key):
        body = reader.get(key)
        if body is None:
            raise SystemExit(f"{key} was listed but is gone - refusing to publish a partial source")
        tbl = pq.read_table(io.BytesIO(body), columns=["series_key", "obs_date", "value"])
        csv_b, n, mn, mx = build_csv(tbl)
        return flow, csv_b, n, mn, mx

    put_ok = 0
    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        futs = [ex.submit(one, f, k) for f, k in flows]
        for fu in as_completed(futs):
            flow, csv_b, n, mn, mx = fu.result()
            title = titles.get(flow) or flow          # honest fallback: the real flow code
            stats.append((flow, n, len(csv_b), title is not flow))
            sid = f"{SOURCE}:{flow}"
            rows_out.append((sid, SOURCE, title[:500], None, None, None, None, lic, mn, mx,
                             None, "{}"))
            if a.upload:
                k = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
                if not _derive._put_with_retry(store, k, csv_b):
                    raise SystemExit(f"{k}: gave up after {_derive.PUT_TRIES} tries")
                put_ok += 1

    tot_rows = sum(s[1] for s in stats)
    tot_b = sum(s[2] for s in stats)
    titled = sum(1 for s in stats if s[3])
    print(f"flows={len(stats)}  rows={tot_rows:,}  csv_total={tot_b/1e9:.2f} GB  "
          f"titled_by_INSEE={titled}/{len(stats)}  {time.time()-t0:.0f}s", flush=True)
    print("largest CSVs:", flush=True)
    for f, n, b, _t in sorted(stats, key=lambda s: -s[2])[:6]:
        print(f"   {f[:34]:34s} rows={n:>9,}  csv={b/1e6:>8.1f} MB", flush=True)

    if a.catalog:
        conn = sqlite3.connect(CATALOG)
        conn.execute("DELETE FROM series WHERE source_id=?", (SOURCE,))
        conn.executemany(
            "INSERT OR REPLACE INTO series (series_id,source_id,title,frequency,unit,geography,"
            "category,license_id,start_date,end_date,last_updated,metadata) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows_out)
        conn.execute("DELETE FROM series_fts WHERE series_id LIKE ?", (SOURCE + ":%",))
        conn.execute("INSERT INTO series_fts(series_id,title,geography) "
                     "SELECT series_id,title,geography FROM series WHERE source_id=?", (SOURCE,))
        conn.commit()
        conn.close()
        print(f"catalogued {len(rows_out):,} flow rows (licence {lic})", flush=True)
    if a.upload:
        print(f"uploaded {put_ok:,} CSVs", flush=True)


if __name__ == "__main__":
    main()
