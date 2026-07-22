"""tools/pxweb_regression_live.py — byte-identical-migration proof over the FULL
on-disk dataset, no upstream re-fetching. Now CATEGORISES every at-risk table.

The new value-first resolver differs from old name-first selection ONLY on a cube
with >=2 date-parseable axes. Every non-time dimension is in the series_key and the
chosen time axis is obs_date, so scanning every distinct key + its obs_dates lets us
split each table WITHOUT fetching:

  CLEAN        no non-time dim parses as dates -> single date axis -> new == old.
  CORRUPT      a non-time dim parses as dates AND the on-disk obs_dates are GARBAGE
               (year < 1500, e.g. Swedish municipality code 0114 read as year 114) ->
               the OLD parser already mis-picked the time axis. New FIXES it, but the
               keys change, so a merge would DUPLICATE -> needs a CLEAN RE-PULL.
  TWO_AXIS     obs_dates are SANE and a non-time dim ALSO parses fully (rate ~1.0) ->
               a genuine second date axis; the resolver tie-break decides -> verify.
  FALSE_ALARM  obs_dates SANE, non-time dim only PARTIALLY parses (0.6<=rate<1.0) ->
               the real (sane) time axis out-scores it, new picks the same axis -> safe.

Run:  python tools/pxweb_regression_live.py
"""
from __future__ import annotations
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import pxweb  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

DATA = r"D:/research/econfindatalibrary/data/clean_full"
SOURCES = ["hagstofa", "statfin", "scb", "stat_estonia", "bfs"]
MIN_RATE = 0.6


def parse_key(k: str):
    prefix, dims = [], {}
    for part in k.split(":"):
        if "=" in part:
            d, _, c = part.partition("=")
            dims.setdefault(d, set()).add(c)
        else:
            prefix.append(part)
    return ":".join(prefix), dims


def _year(v):
    try:
        return v.year
    except AttributeError:
        return None


def categorise(dims: dict, obs_years: set):
    max_rate, risk_dim = 0.0, None
    for dim, codes in dims.items():
        r = pxweb.date_parse_rate(list(codes))
        if r > max_rate:
            max_rate, risk_dim = r, dim
    if max_rate < MIN_RATE:
        return "clean", None, max_rate
    ys = [y for y in obs_years if y is not None]
    sane = sum(1 for y in ys if 1500 <= y <= 2100)
    frac_sane = sane / len(ys) if ys else 1.0
    if frac_sane < 0.5:
        return "corrupt", risk_dim, max_rate
    if max_rate >= 0.999:
        return "two_axis", risk_dim, max_rate
    return "false_alarm", risk_dim, max_rate


def scan_source(src: str):
    d = os.path.join(DATA, src)
    if not os.path.isdir(d):
        return None
    tables: dict[str, dict] = {}
    for f in sorted(os.listdir(d)):
        if not f.endswith(".parquet"):
            continue
        pf = pq.ParquetFile(os.path.join(d, f))
        seen: set[str] = set()
        for batch in pf.iter_batches(columns=["series_key", "obs_date"], batch_size=250_000):
            ks = batch.column("series_key").to_pylist()
            ds = batch.column("obs_date").to_pylist()
            for k, od in zip(ks, ds):
                if not k or k in seen:
                    continue          # heavy work once per DISTINCT key (fast)
                seen.add(k)
                pref, dims = parse_key(k)
                tt = tables.setdefault(pref, {"dims": {}, "years": set()})
                yr = _year(od)
                if yr is not None:
                    tt["years"].add(yr)   # one representative obs-year per key is enough
                for dim, codes in dims.items():           # to flag garbage (year<1500)
                    tt["dims"].setdefault(dim, set()).update(codes)
    cats = {"clean": 0, "corrupt": 0, "two_axis": 0, "false_alarm": 0}
    examples: dict[str, list] = {"corrupt": [], "two_axis": [], "false_alarm": []}
    for pref, tt in tables.items():
        cat, dim, rate = categorise(tt["dims"], tt["years"])
        cats[cat] += 1
        if cat != "clean" and len(examples[cat]) < 4:
            examples[cat].append(f"{pref[:60]} (dim={dim} rate={rate:.2f})")
    return {"tables": len(tables), "cats": cats, "examples": examples}


def main() -> int:
    tot = {"clean": 0, "corrupt": 0, "two_axis": 0, "false_alarm": 0}
    print(f"  {'source':<14} {'tables':>7} {'clean':>7} {'corrupt':>8} {'two_axis':>9} {'false':>7}")
    for src in SOURCES:
        r = scan_source(src)
        if r is None:
            print(f"  {src:<14}  (no on-disk data — skipped)")
            continue
        c = r["cats"]
        for k in tot:
            tot[k] += c[k]
        print(f"  {src:<14} {r['tables']:>7} {c['clean']:>7} {c['corrupt']:>8} "
              f"{c['two_axis']:>9} {c['false_alarm']:>7}")
        for cat in ("corrupt", "two_axis"):
            for ex in r["examples"][cat]:
                print(f"        {cat:<11} {ex}")
    print(f"\n  {'TOTAL':<14} {'':>7} {tot['clean']:>7} {tot['corrupt']:>8} "
          f"{tot['two_axis']:>9} {tot['false_alarm']:>7}")
    print()
    print(f"clean (byte-identical migrate): {tot['clean']}")
    print(f"corrupt (need CLEAN RE-PULL, pre-existing old-parser bug): {tot['corrupt']}")
    print(f"two_axis (genuine 2nd date axis, verify tie-break): {tot['two_axis']}")
    print(f"false_alarm (real time axis out-scores; safe): {tot['false_alarm']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
