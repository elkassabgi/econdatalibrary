"""Seed the 440 never-ingested eurostat flows from local RAW bulk TSVs (R532's gap).

THE GRAMMAR CONTRACT (review v2 change 1 — this is NEW code minting public series
identity, R22 class, adversarial-review gated before its first write):
  * key grammar   — the FETCHER's own `_NON_KEY`/`_norm`/`_build_key`, IMPORTED
    (updater/strategies/fetchers/eurostat.py; never retyped — R191/R192). The bulk
    TSV header's first cell is "<dim1,dim2,...>\\TIME_PERIOD"; each row's first cell
    is the comma-joined dimension VALUES in header order, so name=value pairs are
    fully recoverable and `_build_key` applies verbatim (drop-empty rule included).
  * date grammar  — jobs/ingest_eurostat.parse_period, IMPORTED (annual → Dec-31;
    process_eurostat's Jan-1 is the WRONG convention for this store).
  * value grammar — jobs/ingest_eurostat.parse_value, IMPORTED (flag-stripping,
    ':' missing → row skipped, matching the store's real-observations convention).
  * schema/codec  — series_key string, obs_date date32, value float64, ZSTD
    (byte-shape of the 7,214 existing store files, read 2026-08-31).

WRITE PATH: giants forbid in-memory tables (118 GB RSS at 358M rows, measured), so
each flow STREAMS through a raw pq.ParquetWriter to "<store>/<FLOW>.parquet" and is
then published via blob.publish_file — the documented pattern for exactly this case
(blob.py:284, "a reused production ingest that writes its own parquet"). Before
publish, duckdb (out-of-core) must prove rows == distinct (series_key, obs_date) —
a drop-empty key collision aborts the flow LOUDLY instead of collapsing silently.

GUARDS: NAMQ_10_GDP is hard-excluded (fresh 2026-08-24 API pull in the store; June
raw over it would be a 3-month regression — review change 2). An existing store
target is REFUSED, never overwritten (never-shrink). Writes need --apply AND a
parity PASS recorded this run or --parity-receipt pointing at one.

PARITY MODE (--parity FLOW, read-only): mint rows for a flow that already has BOTH
a raw TSV and a store file; every sampled minted key must exist in the store's key
set, and sampled minted (key, obs_date) pairs must exist in the store. Values are
NOT compared (the store may carry newer API revisions); cardinality is reported.
The review's premise measurement was 711/711 sampled keys byte-matching across 3
flows — this mode re-runs that gate mechanically before any seed write.

Usage:
  py tools/seed_eurostat_440.py --parity AACT_ALI01
  py tools/seed_eurostat_440.py --flow ABC_XYZ --apply --parity-receipt <path>
  py tools/seed_eurostat_440.py --list data/_aqueduct/eurostat_missing440_names.txt \
      --apply --parity-receipt <path>          # the 440 bulk run (quiet window only)
"""
from __future__ import annotations

import argparse
import datetime as _dt
import gzip
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs.ingest_eurostat import parse_period, parse_value  # noqa: E402
from updater import blob, config  # noqa: E402
from updater.strategies.fetchers.eurostat import (_NON_KEY, _build_key,  # noqa: E402
                                                  _norm)

RAW_DIR = os.path.join(ROOT, "data", "raw", "eurostat")
STORE = config.source_dir("eurostat")
EXCLUDED = {"NAMQ_10_GDP"}          # review change 2: fresh API pull, do not regress
BATCH_ROWS = 4_000_000
SCHEMA = pa.schema([("series_key", pa.string()),
                    ("obs_date", pa.date32()),
                    ("value", pa.float64())])


def _raw_path(flow: str) -> str | None:
    for cand in (flow, flow.lower(), flow.upper()):
        p = os.path.join(RAW_DIR, f"{cand}.tsv.gz")
        if os.path.exists(p):
            return p
    return None


def _mint(raw_path: str):
    """Yield (series_key, obs_date, value) — the seeder's single grammar choke point."""
    bad_width = 0
    with gzip.open(raw_path, "rt", encoding="utf-8") as f:
        header = f.readline().rstrip("\n").split("\t")
        head0 = header[0]
        dims = head0.split("\\")[0].split(",")
        dim_cols = [c for c in dims if _norm(c) not in _NON_KEY]
        periods = [c.strip() for c in header[1:]]
        pdates = [parse_period(p) for p in periods]
        for line in f:
            cells = line.rstrip("\n").split("\t")
            vals = cells[0].split(",")
            if len(vals) != len(dims):
                bad_width += 1
                if bad_width > 50:
                    raise SystemExit(f"ABORT {raw_path}: >50 rows whose dimension "
                                     f"arity != header ({len(dims)}) — structural")
                continue
            row = dict(zip(dims, vals))
            key = _build_key(row, dim_cols)
            if not key:
                continue
            for i, cell in enumerate(cells[1:]):
                if i >= len(pdates):
                    break
                od = pdates[i]
                if od is None:
                    continue
                v = parse_value(cell)
                if v is None:
                    continue
                yield key, od, v
    if bad_width:
        print(f"  note: {bad_width} bad-arity row(s) skipped", flush=True)


def _flush(writer, keys, dates, vals):
    writer.write_table(pa.table(
        {"series_key": pa.array(keys, pa.string()),
         "obs_date": pa.array(dates, pa.date32()),
         "value": pa.array(vals, pa.float64())}, schema=SCHEMA))


def parity(flow: str, sample: int) -> int:
    import duckdb
    rp = _raw_path(flow)
    sp = os.path.join(STORE, f"{flow.upper()}.parquet")
    if rp is None or not os.path.exists(sp):
        print(f"parity needs BOTH raw and store for {flow}: raw={rp} store_exists="
              f"{os.path.exists(sp)}")
        return 2
    minted_n = 0
    first_date: dict[str, object] = {}      # key -> one minted date (first seen)
    for k, d, v in _mint(rp):
        minted_n += 1
        if k not in first_date:
            first_date[k] = d
    # sample at the KEY grain (every distinct key is a grammar specimen; a row
    # stride over a small flow yielded 4 samples — vacuously thin)
    all_keys = sorted(first_date)
    step = max(1, len(all_keys) // sample)
    sampled = [(k, first_date[k]) for k in all_keys[::step]][:sample]
    con = duckdb.connect()
    spx = sp.replace("\\", "/")
    store_n, store_keys = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT series_key) FROM read_parquet('{spx}')"
    ).fetchone()
    key_hits = date_hits = 0
    for k, d in sampled:
        kh = con.execute(
            f"SELECT 1 FROM read_parquet('{spx}') WHERE series_key = ? LIMIT 1",
            [k]).fetchone()
        if kh:
            key_hits += 1
            dh = con.execute(
                f"SELECT 1 FROM read_parquet('{spx}') WHERE series_key = ? AND "
                f"obs_date = ? LIMIT 1", [k, d]).fetchone()
            if dh:
                date_hits += 1
    n = len(sampled)
    ok = n > 0 and key_hits == n and date_hits == n
    receipt = {"flow": flow.upper(), "sampled": n, "key_hits": key_hits,
               "date_hits": date_hits, "minted_rows": minted_n,
               "store_rows": store_n, "store_distinct_keys": store_keys,
               "verdict": "PASS" if ok else "FAIL"}
    out = os.path.join(ROOT, "data", "_aqueduct",
                       f"seed_parity_{flow.upper()}.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=1)
    print(json.dumps(receipt, indent=1))
    print("wrote", out)
    return 0 if ok else 1


def seed_one(flow: str) -> tuple[str, int]:
    """Returns (status, rows). status: seeded | refused-exists | no-raw | failed-dup."""
    import duckdb
    flow_u = flow.upper()
    if flow_u in EXCLUDED:
        return "refused-excluded", 0
    target = os.path.join(STORE, f"{flow_u}.parquet")
    if os.path.exists(target) or blob.exists(target):
        return "refused-exists", 0
    rp = _raw_path(flow)
    if rp is None:
        return "no-raw", 0
    os.makedirs(STORE, exist_ok=True)
    tmp = f"{target}.{os.getpid()}.seedtmp"
    n = 0
    try:
        writer = pq.ParquetWriter(tmp, SCHEMA, compression="zstd")
        keys: list = []
        dates: list = []
        vals: list = []
        for k, d, v in _mint(rp):
            keys.append(k); dates.append(d); vals.append(v)
            if len(keys) >= BATCH_ROWS:
                _flush(writer, keys, dates, vals)
                n += len(keys)
                keys, dates, vals = [], [], []
        if keys:
            _flush(writer, keys, dates, vals)
            n += len(keys)
        writer.close()
        con = duckdb.connect()
        tpx = tmp.replace("\\", "/")
        rows, dk = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT (series_key, obs_date)) "
            f"FROM read_parquet('{tpx}')").fetchone()
        con.close()
        if rows != n or dk != rows:
            print(f"  ABORT {flow_u}: rows={rows:,} streamed={n:,} "
                  f"distinct(key,date)={dk:,} — dup or write skew", flush=True)
            return "failed-dup", 0
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
    published = blob.publish_file(target)
    print(f"  seeded {flow_u}: rows={n:,} published_bytes={published:,}", flush=True)
    return "seeded", n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parity", metavar="FLOW")
    ap.add_argument("--sample", type=int, default=300)
    ap.add_argument("--flow", metavar="FLOW")
    ap.add_argument("--list", dest="list_file")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--parity-receipt", dest="receipt",
                    help="path to a PASS receipt from --parity (required with --apply)")
    a = ap.parse_args()

    if a.parity:
        return parity(a.parity, a.sample)

    flows: list[str] = []
    if a.flow:
        flows = [a.flow]
    elif a.list_file:
        with open(a.list_file, encoding="utf-8") as f:
            flows = [ln.strip() for ln in f if ln.strip()]
    else:
        ap.error("need --parity, --flow or --list")

    flows = [f for f in flows if f.upper() not in EXCLUDED]
    print(f"{len(flows)} flow(s) to seed; backend={config.BACKEND}")
    if not a.apply:
        for f in flows[:10]:
            rp = _raw_path(f)
            tgt = os.path.join(STORE, f"{f.upper()}.parquet")
            print(f"  DRY {f.upper()}: raw={'yes' if rp else 'NO'} "
                  f"target_exists={os.path.exists(tgt)}")
        if len(flows) > 10:
            print(f"  ... and {len(flows) - 10} more")
        print("dry run — pass --apply (with --parity-receipt) to write")
        return 0

    if not a.receipt or not os.path.exists(a.receipt):
        print("REFUSED: --apply requires --parity-receipt <existing PASS receipt>")
        return 2
    with open(a.receipt, encoding="utf-8") as f:
        if json.load(f).get("verdict") != "PASS":
            print("REFUSED: parity receipt is not a PASS")
            return 2

    tally: dict[str, int] = {}
    total_rows = 0
    for i, f in enumerate(flows, 1):
        print(f"[{i}/{len(flows)}] {f}", flush=True)
        status, n = seed_one(f)
        tally[status] = tally.get(status, 0) + 1
        total_rows += n
    print(json.dumps({"tally": tally, "total_rows": total_rows}))
    return 0 if tally.get("failed-dup", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
