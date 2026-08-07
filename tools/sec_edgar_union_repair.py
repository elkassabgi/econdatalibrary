"""Reunite a sec_edgar company's facts when the store and the mirror each hold rows the other lost.

THE DEFECT THIS REPAIRS. `tools/refresh_sec_edgar.py` writes each company as
`pq.write_table(tbl, path)` — a REPLACE with whatever one CIK's companyfacts payload contains.
That is safe only while a company keeps one CIK forever. It does not:

    XOM   ticker maps to CIK 2115436 since Exxon's 2024 reorganisation.
          data.sec.gov/CIK0002115436 -> 274 facts, 100 metrics, 2024-12-31 onward.
          data.sec.gov/CIK0000034088 -> 20,629 facts, 448 metrics, from 2006-12-31.
    The refresher fetched the new registrant and overwrote the store keyed by TICKER, so
    r2://clean_grouped/sec_edgar/XOM.parquet went 20,629 rows -> 274 and eighteen years of
    Exxon fundamentals left the store. The served CSV still had them only because it was built
    from a local mirror that had not been refreshed since May.

Measured across the catalogue, seven companies have had their CIK re-assigned — NVRI, CLBK,
CBAT, XOM, GORO, XPRO, UROY — so this is a class with six more members armed and waiting for
their next filing.

WHY UNION AND NOT "PICK THE BIGGER ONE". The divergence is real on both sides: the mirror holds
history the store lost, and the store holds the new registrant's recent facts the mirror never
saw. Either side alone is incomplete.

WHY A MULTISET UNION, WHICH LOOKS LIKE OVERKILL AND IS NOT. This table has no key. The first
version of this tool deduped on (metric, obs_date, vintage_date) and its own superset guard
refused to write, because that "key" collapses 2,013 of XOM's 20,629 rows. The reason is in
`parse_companyfacts`: it flattens SEC's `units` points keeping only end/val/filed and DROPPING
`start`, so one filing's 3-month and 9-month figures for the same period END are two legitimate
facts that the stored schema cannot tell apart —

    us-gaap:AmortizationOfIntangibleAssets:USD  end 2015-09-30  filed 2015-10-21  x2, 2 values

Even all four columns together are not unique (XOM: 20,629 rows, 20,578 distinct). So the union
is taken over MULTIPLICITIES: for each distinct row, keep max(times in mirror, times in store).
That is a superset of both sides by construction and invents nothing beyond the larger side's
count. Restoring `start` to the schema would be the real fix, and it would re-key every served
sec_edgar CSV — a separate, deliberate decision, not a side effect of a repair.

It refuses to write when the union is not a superset of both inputs, which is the only way this
tool could itself lose a row.

    python tools/sec_edgar_union_repair.py --from-json data/_probe/sec_edgar_diff.json
    python tools/sec_edgar_union_repair.py --ticker XOM --apply
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"
PREFIX = "clean_grouped/sec_edgar"
LOCAL = os.path.join(ROOT, "data", "clean_grouped", "sec_edgar")


def union_one(s3, con, name: str, apply: bool):
    import pyarrow.parquet as pq
    lp = os.path.join(LOCAL, name + ".parquet")
    tmp = os.path.join(ROOT, "data", "_probe", f"r2_union_{name}.parquet")
    os.makedirs(os.path.dirname(tmp), exist_ok=True)
    s3.download_file(BUCKET, f"{PREFIX}/{name}.parquet", tmp)
    lq, rq = lp.replace(os.sep, "/"), tmp.replace(os.sep, "/")
    n_l = pq.read_metadata(lp).num_rows
    n_r = pq.read_metadata(tmp).num_rows

    # Multiset union: max multiplicity per distinct row, then re-expand. `unnest(range(n))`
    # replays a row n times; the join is FULL so a row present on only one side survives.
    con.execute(f"""
        create or replace table u as
        with l as (select metric, obs_date, value, vintage_date, count(*) c
                     from read_parquet('{lq}') group by all),
             r as (select metric, obs_date, value, vintage_date, count(*) c
                     from read_parquet('{rq}') group by all),
             m as (select coalesce(l.metric, r.metric) as "metric",
                          coalesce(l.obs_date, r.obs_date) as "obs_date",
                          coalesce(l.value, r.value) as "value",
                          coalesce(l.vintage_date, r.vintage_date) as "vintage_date",
                          greatest(coalesce(l.c, 0), coalesce(r.c, 0)) as n
                     from l full outer join r
                       on l.metric = r.metric and l.obs_date = r.obs_date
                      and l.value is not distinct from r.value
                      and l.vintage_date is not distinct from r.vintage_date)
        select metric, obs_date, value, vintage_date from m, unnest(range(m.n))
        order by metric, obs_date, vintage_date""")
    n_u, n_m = con.execute(
        "select count(*), count(distinct metric) from u").fetchone()
    lo, hi = con.execute("select min(obs_date), max(obs_date) from u").fetchone()

    # The one way this tool could destroy data is by writing a union smaller than an input.
    if n_u < max(n_l, n_r):
        print(f"   {name}: REFUSING — union {n_u:,} < max(local {n_l:,}, R2 {n_r:,})")
        return None
    print(f"   {name:14s} local {n_l:>7,} + R2 {n_r:>7,} -> union {n_u:>7,} rows, "
          f"{n_m:,} metrics, {lo}..{hi}   (+{n_u - n_r:,} restored to the store)")
    if not apply:
        return n_u

    # Archive the object being replaced. The union is a superset, so nothing is lost by
    # construction — but "by construction" is an argument, and an archived copy is a fact.
    s3.copy_object(Bucket=BUCKET, Key=f"archive/sec_edgar_cik_change/{name}.parquet",
                   CopySource={"Bucket": BUCKET, "Key": f"{PREFIX}/{name}.parquet"})
    out = os.path.join(ROOT, "data", "_probe", f"union_{name}.parquet")
    con.execute(f"copy u to '{out.replace(os.sep, '/')}' (format parquet, compression zstd)")
    body = open(out, "rb").read()
    s3.put_object(Bucket=BUCKET, Key=f"{PREFIX}/{name}.parquet", Body=body)
    with open(lp, "wb") as f:                       # keep the mirror identical to the store
        f.write(body)
    back = pq.read_metadata(io.BytesIO(
        s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}/{name}.parquet")["Body"].read())).num_rows
    if back != n_u:
        print(f"   {name}: WROTE BUT READBACK IS {back:,}, expected {n_u:,}")
        return None
    return n_u


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticker", action="append", default=[])
    ap.add_argument("--from-json", help="footer_diff.py output; repairs its `ahead` list")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    names = list(a.ticker)
    if a.from_json:
        d = json.load(open(a.from_json, encoding="utf-8"))
        names += [n for n, _l, _r in d.get("ahead", [])]
    names = sorted(set(names))
    if not names:
        print("nothing to do — pass --ticker or --from-json")
        return 1

    import duckdb
    from core import r2_util
    s3 = r2_util.client()
    con = duckdb.connect()
    print(f"MODE: {'APPLY' if a.apply else 'REPORT ONLY'}   {len(names)} company(ies)\n")
    ok = 0
    for n in names:
        if union_one(s3, con, n, a.apply) is not None:
            ok += 1
    print(f"\n{ok}/{len(names)} "
          f"{'repaired in R2 + mirror' if a.apply else 'would repair (report only)'}")
    return 0 if ok == len(names) else 1


if __name__ == "__main__":
    sys.exit(main())
