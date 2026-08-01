"""Materialise census at TABLE grain to R2 — one CSV per table, large tables split.

WHY TABLE GRAIN. census is 80 WIDE tables (11 to 238 columns, the measures ARE the columns) over
44,242,170 rows. At cell grain it would be 1,202,396,034 nominal series. Measured, 70 of the 80
are genuine PANELS (more than 3 periods) and only 8 are single-period censuses — the opposite of
what two early samples suggested, and the reason this is worth serving at all rather than
treating as a pile of cross-sections.

IT CALLS THE RESOLVER, IT DOES NOT REIMPLEMENT IT. Every other derive in this repo builds the
CSV itself and is then checked for byte-parity against core.derive_csv — a check that has caught
a real defect every single time it was run today (a missing null filter, an unqualified id, a
non-deterministic tiebreak). census has only ~300 units, so the scan-per-unit cost that forces
the other sources to hand-roll their own writer simply does not apply here. Producing the bytes
THROUGH the reference implementation makes parity structural instead of tested: there is one
construction, so there is nothing to diverge.

SPLITTING large tables reuses the same evidence-based rule as istat: measure each named
dimension in the table's own pipe-delimited keys, verify the LARGEST resulting part fits the
bound (cardinality is not balance — that mistake produced a 775,206-row "part" for istat), and
record the choice in _split_map.json so the resolver can reproduce it.

    python tools/derive_census_tables.py --dry-run
    python tools/derive_census_tables.py --bucket econ-data --skip-existing
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from core import r2_util                                       # noqa: E402
from core.derive_csv import _series_csv_bytes                  # noqa: E402

SOURCE = "census"
STORE = os.path.join(ROOT, "data", "clean_full", SOURCE)
MAX_ROWS_DEFAULT = 500_000


def table_sid(table: str, part: str | None = None) -> str:
    return f"{SOURCE}:{table}" + (f"#{part}" if part else "")


def csv_key(prefix: str, sid: str) -> str:
    return f"{prefix}/{urllib.parse.quote(sid, safe='')}.csv"


def choose_split(con, path: str, n_rows: int, max_rows: int):
    """-> (dimension-expression, label, parts) or (None, None, []) when no split is needed.

    Keys are pipe-delimited NAME=VALUE fragments. Candidates are tried coarsest-first and each is
    CHECKED by grouping — the largest part must fit, because a dimension can have plenty of
    distinct values and still put almost every row in one of them.

    COMPOSITE SPLITS, because single dimensions are not always enough. Six census tables were
    refused on the first run — intltrade__exports__hs (8,718,542 rows),
    intltrade__imports__hs (4,623,339), intltrade__exports__sitcexport (1,670,570) and three
    more, about 15.6 million rows in total — because no ONE dimension divided them below the
    bound. Trade data is naturally two-dimensional (commodity x country, commodity x district),
    so pairs are tried next, ordered by the product of their cardinalities so the coarsest
    workable pair wins. A table neither can divide is still refused and named.
    """
    if n_rows <= max_rows:
        return None, None, []
    row = con.execute(f"select series_key from read_parquet('{path}') limit 1").fetchone()
    if not row:
        return None, None, []
    dims = [p.split("=", 1)[0] for p in row[0].split("|") if "=" in p]
    card = {}
    for d in dims:
        try:
            card[d] = con.execute(
                f"select count(distinct regexp_extract(series_key, '{d}=([^|]*)', 1)) "
                f"from read_parquet('{path}')").fetchone()[0]
        except Exception:                                      # noqa: BLE001
            continue

    def expr_for(ds_list):
        parts = [f"regexp_extract(series_key, '{d}=([^|]*)', 1)" for d in ds_list]
        return parts[0] if len(parts) == 1 else " || '~' || ".join(parts)

    def try_combo(ds_list):
        e = expr_for(ds_list)
        try:
            biggest = con.execute(
                f"select max(n) from (select count(*) n from read_parquet('{path}') "
                f"where obs_date is not null group by {e})").fetchone()[0]
        except Exception:                                      # noqa: BLE001
            return None
        if not biggest or biggest > max_rows:
            return None
        parts = [r[0] for r in con.execute(
            f"select distinct {e} from read_parquet('{path}') "
            f"where obs_date is not null order by 1").fetchall()]
        return parts

    usable = {d: c for d, c in card.items() if 2 <= c <= 2000}
    for _, d in sorted((c, d) for d, c in usable.items()):
        parts = try_combo([d])
        if parts:
            return expr_for([d]), [d], parts

    # TRUNCATIONS, which is what trade data actually wants. intltrade__exports__hs has an
    # E_COMMODITY dimension of 18,511 values whose largest part is 585 rows — a perfect
    # splitter that the 2,000 cap above excluded, and excluding it is why this table was
    # refused on the first run. But 18,511 units of ~471 rows each is shredding, and HS codes
    # are HIERARCHICAL: 551529 is chapter 55, heading 5515, subheading 551529. Truncating to 2
    # gives ~99 chapters, to 4 gives ~1,200 headings — real classification levels a user
    # recognises, not arbitrary shards. Coarsest first.
    for d, c in sorted(card.items(), key=lambda kv: -kv[1]):
        if c <= 2000:
            continue
        for t in (2, 3, 4, 6):
            e = f"substr(regexp_extract(series_key, '{d}=([^|]*)', 1), 1, {t})"
            try:
                tc = con.execute(f"select count(distinct {e}) "
                                 f"from read_parquet('{path}')").fetchone()[0]
                if not (2 <= tc <= 2000):
                    continue
                biggest = con.execute(
                    f"select max(n) from (select count(*) n from read_parquet('{path}') "
                    f"where obs_date is not null group by {e})").fetchone()[0]
                if biggest and biggest <= max_rows:
                    parts = [r[0] for r in con.execute(
                        f"select distinct {e} from read_parquet('{path}') "
                        f"where obs_date is not null order by 1").fetchall()]
                    return e, [f"{d}:{t}"], parts
            except Exception:                                  # noqa: BLE001
                continue

    # pairs, coarsest product first
    pairs = sorted(((usable[a] * usable[b], a, b)
                    for i, a in enumerate(sorted(usable))
                    for b in sorted(usable)[i + 1:]
                    if usable[a] * usable[b] <= 20_000))
    for _, a, b in pairs:
        parts = try_combo([a, b])
        if parts:
            return expr_for([a, b]), [a, b], parts
    return "", None, []                                        # refused


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--prefix", default="series")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--max-rows", type=int, default=MAX_ROWS_DEFAULT)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    if not a.dry_run and not a.bucket:
        ap.error("--bucket is required unless --dry-run")

    files = sorted(f.replace("\\", "/") for f in glob.glob(os.path.join(STORE, "*.parquet"))
                   if not f.endswith("__series.parquet"))
    if not files:
        raise SystemExit(f"no parquet under {STORE} — refusing to report an empty derive")
    print(f"{len(files)} table(s); splitting any over {a.max_rows:,} rows", flush=True)

    spill = os.path.join(ROOT, "logs", "_duckspill")
    os.makedirs(spill, exist_ok=True)

    existing = set()
    s3 = None
    if not a.dry_run:
        s3 = r2_util.client(write=True)
        if a.skip_existing:
            pref = f"{a.prefix}/{urllib.parse.quote(SOURCE + ':', safe='')}"
            tok = None
            while True:
                kw = {"Bucket": a.bucket, "Prefix": pref, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                r = s3.list_objects_v2(**kw)
                existing.update(o["Key"] for o in r.get("Contents", []))
                if not r.get("IsTruncated"):
                    break
                tok = r["NextContinuationToken"]
            print(f"skip-existing: {len(existing):,} already in R2", flush=True)

    split_map, refused, ids = {}, [], []
    for i, f in enumerate(files, 1):
        table = os.path.splitext(os.path.basename(f))[0]
        try:
            n_rows = pq.ParquetFile(f).metadata.num_rows
            cols = set(pq.read_schema(f).names)
        except Exception as e:                                 # noqa: BLE001
            refused.append((table, f"unreadable {type(e).__name__}"))
            continue
        if "series_key" not in cols or "obs_date" not in cols:
            # pseo__earnings / pseo__flows are entity-level and have no obs_date; named, not
            # silently dropped, because "80 tables" minus two with no explanation is a gap.
            refused.append((table, f"no series_key/obs_date (cols={len(cols)})"))
            continue
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        expr, dim, parts = choose_split(con, f, n_rows, a.max_rows)
        con.close()
        if expr == "":
            refused.append((table, f"{n_rows:,} rows, no dimension divides it"))
            print(f"  [{i}/{len(files)}] {table}: REFUSED — {n_rows:,} rows", flush=True)
            continue
        if expr:
            # dims as a LIST, not a "~"-joined string: the join character also
            # separates the VALUES in a composite part id, so one string cannot be split back
            # unambiguously if a code ever contains it.
            split_map[table] = {"dims": dim, "sep": "~",
                                "parts": len(parts), "rows": n_rows}
            ids += [table_sid(table, p) for p in parts if p]
            print(f"  [{i}/{len(files)}] {table}: {n_rows:,} rows split by "
                  f"{'+'.join(dim)} -> {len(parts)} parts", flush=True)
        else:
            ids.append(table_sid(table))
        if a.limit and i >= a.limit:
            break

    print(f"\n{len(ids):,} unit(s) to write; {len(refused)} table(s) refused", flush=True)
    if not a.dry_run:
        with open(os.path.join(STORE, "_split_map.json"), "w", encoding="utf-8") as fh:
            json.dump(split_map, fh, indent=1, sort_keys=True)

    t0, put, skip, err = time.time(), 0, 0, 0
    for n, sid in enumerate(ids, 1):
        key = csv_key(a.prefix, sid)
        if key in existing:
            skip += 1
            continue
        try:
            body = _series_csv_bytes(sid)                      # THE reference construction
        except Exception as e:                                 # noqa: BLE001
            err += 1
            if err <= 5:
                print(f"  BUILD FAILED {sid}: {type(e).__name__} {str(e)[:90]}", flush=True)
            continue
        if a.dry_run:
            if n <= 3:
                print(f"  would PUT {key} ({len(body):,} B)")
            continue
        try:
            s3.put_object(Bucket=a.bucket, Key=key, Body=body, ContentType="text/csv")
            put += 1
            if put % 25 == 0:
                print(f"  put {put:,}/{len(ids):,}  {time.time()-t0:,.0f}s", flush=True)
        except Exception as e:                                 # noqa: BLE001
            err += 1
            if err <= 5:
                print(f"  PUT FAILED {key}: {str(e)[:90]}", flush=True)

    print(f"\nunits {len(ids):,}   put {put:,}   skipped {skip:,}   errors {err:,}   "
          f"{time.time()-t0:,.0f}s")
    if refused:
        print(f"REFUSED — {len(refused)}:")
        for t, why in refused:
            print(f"   {t:44s} {why}")
    return 1 if err else 0


if __name__ == "__main__":
    sys.exit(main())
