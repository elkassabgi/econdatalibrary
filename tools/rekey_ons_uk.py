"""tools/rekey_ons_uk.py — rebuild ons_uk series ids without the date or the quality stat.

APPROVED re-key (Ahmed, 2026-07-29). ons_uk serves nothing today (0 catalog rows), so no
published id or download breaks; this is a re-ingest of a quarantined source.

THE DEFECT. The live parser treats every column that is not time or value as a dimension,
which in ONS V4 sweeps in the observation-metadata columns AND the time code column. A
stored key reads `CV=14.0:calendar-years=2018:administrative-geography=...` — a coefficient
of variation (a property of one measurement) and the observation period, both baked into the
series identity. Every row therefore becomes its own series: ashe-table-5 is 5,323,152 rows
and 5,323,152 distinct keys, and the per-series cursor dict alone drove peak RSS to 32.26 GB
on a 16 GB runner.

NO RE-DOWNLOAD. The obvious route — refetch and re-parse — would pull tens of GB (ashe-table-5's
CSV alone is 3.9 GB) from a publisher that blocks automated clients over their rate limits
(R132). It is also unnecessary: the OLD key already carries every dimension as `name=value`
in header order, so the new key is the old key MINUS the segments that never belonged in an
identity — the `v4_N`-declared metadata columns, the time CODE column, and every LABEL column
of each (code, label) dimension pair. Labels are dropped because ONS can re-word a display
string without the series changing; codes are the stable half.

Validated before use, not assumed: for weekly-deaths-region the transform reproduced the real
v4 parser's key set EXACTLY — 4,004 keys, 0 only-in-parser, 0 only-in-transform.

COLLISIONS ARE REPORTED, NEVER SILENTLY MERGED. Dropping `CV` can map two stored rows onto the
same (new_key, obs_date). That is a real merge of distinct measurements, so this counts them
per dataset and refuses to publish a file where they occur unless --allow-collisions is given.
A silent dedup here would quietly delete observations.

  python tools/rekey_ons_uk.py --dry-run     # counts + collisions, writes nothing
  python tools/rekey_ons_uk.py --stage       # write rekeyed parquets to data/_rekey/ons_uk/
"""
from __future__ import annotations
import argparse, csv as _csv, io, json, os, re, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("AQUEDUCT_BACKEND", "r2")

import pyarrow as pa                                           # noqa: E402
import pyarrow.parquet as pq                                   # noqa: E402
from core import r2_util                                       # noqa: E402

BUCKET = "econ-data"
PREFIX = "clean_full/ons_uk/"
STAGE = os.path.join(ROOT, "data", "_rekey", "ons_uk")
HDR_JSON = (r"D:/temp/claude/D--research-hfdatalibrary/5bda36f5-59a1-4804-b441-06c56c3755da"
            r"/scratchpad/ons_headers_serial_full.json")


def load_headers() -> dict:
    rows = json.load(io.open(HDR_JSON, encoding="utf-8"))
    return {a: b for a, b, _c in rows if b and not b.lstrip().startswith("<")}


def keep_names(header_line: str):
    """Column names whose `name=value` segments survive into the new key, in order."""
    cols = next(_csv.reader([header_line]))
    m = re.match(r"^v4_(\d+)$", cols[0].strip(), re.I)
    if not m:
        return None
    dims = cols[1 + int(m.group(1)):]
    if len(dims) % 2:
        return None
    pairs = [(dims[i], dims[i + 1]) for i in range(0, len(dims), 2)]
    t = [i for i, (_c, lab) in enumerate(pairs) if lab.strip().lower() == "time"]
    if len(t) != 1:
        return None
    return [c for i, (c, _lab) in enumerate(pairs) if i != t[0]]


def rekey(old: str, keep: list) -> str:
    got = {}
    for seg in old.split(":"):
        if "=" in seg:
            k, v = seg.split("=", 1)
            got.setdefault(k, v)
    return ":".join(f"{k}={got[k]}" for k in keep if got.get(k))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--stage", action="store_true")
    ap.add_argument("--allow-collisions", action="store_true")
    a = ap.parse_args()
    if not (a.dry_run or a.stage):
        ap.error("pick --dry-run or --stage")

    hdr = load_headers()
    c = r2_util.client()
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
    if a.stage:
        os.makedirs(STAGE, exist_ok=True)

    tot_rows = tot_old = tot_new = tot_coll = 0
    skipped, staged = [], 0
    print(f"ons_uk parquets: {len(objs)}", flush=True)
    for ds, key in objs:
        h = hdr.get(ds)
        keep = keep_names(h) if h else None
        if not keep:
            skipped.append((ds, "no usable V4 header" if not h else "header not V4 grammar"))
            continue
        body = c.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        tbl = pq.read_table(io.BytesIO(body))
        oldk = tbl.column("series_key").to_pylist()
        dates = tbl.column("obs_date").to_pylist()
        vals = tbl.column("value").to_pylist()
        newk = [rekey(k, keep) for k in oldk]
        seen, coll = set(), 0
        for k, d in zip(newk, dates):
            t = (k, d)
            if t in seen:
                coll += 1
            seen.add(t)
        tot_rows += len(oldk); tot_old += len(set(oldk)); tot_new += len(set(newk))
        tot_coll += coll
        flag = "  <-- COLLISIONS" if coll else ""
        print(f"  {ds[:34]:34s} rows {len(oldk):>9,}  keys {len(set(oldk)):>9,} -> "
              f"{len(set(newk)):>8,}  collisions {coll:>6,}{flag}", flush=True)
        if a.stage:
            if coll and not a.allow_collisions:
                skipped.append((ds, f"{coll:,} (key,date) collisions — refusing to merge"))
                continue
            out = pa.table({"series_key": pa.array(newk, pa.string()),
                            "obs_date": tbl.column("obs_date"),
                            "value": pa.array(vals, pa.float64())})
            pq.write_table(out, os.path.join(STAGE, f"{ds}.parquet"), compression="zstd")
            staged += 1

    print()
    print(f"TOTAL rows {tot_rows:,}  old keys {tot_old:,} -> new keys {tot_new:,}"
          f"  ({tot_old / max(tot_new, 1):.1f}x fewer)  collisions {tot_coll:,}")
    print(f"obs/series {tot_rows / max(tot_old,1):.2f} -> {tot_rows / max(tot_new,1):.2f}")
    if a.stage:
        print(f"staged {staged} parquet(s) to {STAGE}")
    if skipped:
        print(f"SKIPPED {len(skipped)}:")
        for d, why in skipped[:10]:
            print(f"   {d}: {why}")


if __name__ == "__main__":
    main()
