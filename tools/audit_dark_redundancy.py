"""Which dark stores are REDUNDANT re-crawls of something already served?

The dark-store triage (#37) put 27 series-shaped sources in a bucket labelled "servable today,
blocked on a licence assessment". Two of them turned out not to be gaps at all:

    ilo       1,154 of its 1,157 indicators are in the served ilostat, which has 49 more
    ecb_sdmx  199 of 199 sampled keys already present in the served ecb store

Both are earlier or narrower crawls of a source the library already hosts, kept because nothing
ever compared them. Assessing a licence for one of those is wasted work, and serving it would
publish the same observations twice under a second id.

So this asks the cheap question FIRST, for every dark store: are its series keys already in some
served store? A source whose keys are all present somewhere served is redundant; one with keys of
its own is a genuine gap and goes to the licence queue.

MATCHING IS BY KEY, NOT BY NAME. Name similarity is what suggested these pairs, but it is not
evidence -- `imf_ifs` and `imf_afrreo` share a prefix and share nothing else. Candidate targets
are proposed by name and then CONFIRMED by sampling actual series keys, and a candidate that
fails is reported as such rather than quietly dropped.

WHAT A KEY MISS DOES *NOT* PROVE -- read this before deleting anything. Two stores can hold the
SAME observations under different key conventions, and then no key matches while the data is
entirely redundant. That is not hypothetical: `ilo` scores 0 of 115 keys against `ilostat`, yet
1,154 of ilo's 1,157 INDICATORS are in ilostat (which has 49 more and 180M more rows). ilo keys
read `REF_AREA=ABW:FREQ=A:MEASURE=...` while ilostat's read `ilostat:<flow>:<ref_area>:<sex>:...`
-- same publisher, same indicators, two vocabularies. So a no-overlap result is reported as
NO KEY OVERLAP, never as "genuine gap": it rules out the cheap kind of duplication and nothing
more. Confirming the expensive kind means comparing on indicator/flow identity, per pair, by
hand.

The sample is a sample and says so: results are reported with N, tiny samples are flagged
LOW-CONFIDENCE, and a partial overlap is called MIXED and left for a human, because that is a
merge question rather than a delete question.
"""
from __future__ import annotations

import argparse
import glob
import os
import random
import re
import sqlite3
import sys

import duckdb
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SERIES_COLS = {"series_key", "obs_date", "value"}


def supported() -> set:
    ts = open(os.path.join(ROOT, "api", "worker", "src", "util.ts"), encoding="utf-8").read()
    blk = ts.split("SUPPORTED_SOURCES: readonly string[] = [", 1)[1].split("];", 1)[0]
    return set(re.findall(r'"([a-z0-9_]+)"', re.sub(r"//[^\n]*", "", blk)))


def store_files(src: str) -> list:
    out = []
    for tier in ("clean_full", "clean"):
        d = os.path.join(ROOT, "data", tier, src)
        if os.path.isdir(d):
            out = [f.replace("\\", "/") for f in
                   glob.glob(os.path.join(d, "**", "*.parquet"), recursive=True)
                   if not f.endswith("__series.parquet")]
            if out:
                return out
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=150, help="keys sampled per dark source")
    ap.add_argument("--source", action="append", help="limit to these dark sources")
    a = ap.parse_args()

    sup = supported()
    con = sqlite3.connect(f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro",
                          uri=True, timeout=180.0)
    cat = dict(con.execute("select source_id, count(*) from series group by 1").fetchall())
    served = {s for s in cat if s in sup and cat[s]}
    con.close()

    dark = []
    for d in sorted(glob.glob(os.path.join(ROOT, "data", "*", "*"))):
        if not os.path.isdir(d):
            continue
        s = os.path.basename(d)
        if s in sup or cat.get(s, 0) or s in {x for x, _ in dark}:
            continue
        fs = store_files(s)
        if not fs:
            continue
        try:
            if not SERIES_COLS <= set(pq.read_schema(fs[0]).names):
                continue
        except Exception:                                      # noqa: BLE001
            continue
        dark.append((s, fs))
    if a.source:
        want = set(a.source)
        dark = [(s, fs) for s, fs in dark if s in want]
    print(f"{len(dark)} dark series-shaped source(s); {len(served)} served source(s)\n")

    spill = os.path.join(ROOT, "logs", "_duckspill", f"pid{os.getpid()}")
    os.makedirs(spill, exist_ok=True)
    verdicts = {"REDUNDANT": [], "NO KEY OVERLAP": [], "MIXED": [], "NO CANDIDATE": []}

    for s, fs in dark:
        # Candidate served targets, proposed by NAME and confirmed by KEY.
        toks = [t for t in s.split("_") if len(t) > 2]
        cands = [x for x in served
                 if x == s or x.startswith(s) or s.startswith(x)
                 or any(x == t or x.startswith(t + "_") or t == x.split("_")[0] for t in toks)]
        if not cands:
            verdicts["NO CANDIDATE"].append((s, len(fs), ""))
            print(f"{s:16s} NO CANDIDATE — no served source shares its name, so nothing cheap "
                  f"to compare against. Likely a real gap; not proof of one.")
            continue

        con = duckdb.connect()
        con.execute("SET memory_limit='8GB'")
        con.execute(f"SET temp_directory='{spill}'")
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        random.seed(17)
        try:
            keys = [r[0] for r in con.execute(
                f"select distinct series_key from read_parquet({[random.choice(fs)]}) "
                f"using sample {a.sample} rows").fetchall()]
        except Exception as e:                                 # noqa: BLE001
            print(f"{s:16s} SAMPLE FAILED {type(e).__name__} {str(e)[:50]}")
            con.close()
            continue
        if not keys:
            con.close()
            continue
        vals = ",".join(repr(k) for k in keys)

        best, best_n = None, 0
        for c in sorted(cands):
            tf = store_files(c)
            if not tf:
                continue
            try:
                n = con.execute(
                    f"select count(distinct series_key) from read_parquet({tf}, "
                    f"union_by_name=true) where series_key in ({vals})").fetchone()[0]
            except Exception:                                  # noqa: BLE001
                continue
            if n > best_n:
                best, best_n = c, n
        con.close()

        pct = 100.0 * best_n / len(keys)
        if best_n == len(keys):
            verdicts["REDUNDANT"].append((s, len(fs), best))
            print(f"{s:16s} REDUNDANT — {best_n}/{len(keys)} sampled keys are in {best}")
        elif best_n == 0:
            low = " LOW-CONFIDENCE (tiny sample)" if len(keys) < 30 else ""
            verdicts["NO KEY OVERLAP"].append((s, len(fs), best or ""))
            print(f"{s:16s} NO KEY OVERLAP — 0/{len(keys)} keys in "
                  f"{', '.join(sorted(cands)[:3])}{low}. Rules out same-key duplication ONLY; "
                  f"the same data under a DIFFERENT key convention is still possible "
                  f"(see ilo/ilostat).")
        else:
            verdicts["MIXED"].append((s, len(fs), f"{best} {pct:.0f}%"))
            print(f"{s:16s} MIXED — {best_n}/{len(keys)} ({pct:.0f}%) in {best}; a partial "
                  f"overlap is a MERGE question, not a delete one")

    print(f"\n{'='*72}")
    for k in ("REDUNDANT", "NO KEY OVERLAP", "NO CANDIDATE", "MIXED"):
        rows = verdicts[k]
        print(f"{k:14s} {len(rows):>3} source(s)"
              + (f": {', '.join(r[0] for r in rows)}" if rows else ""))
    print(f"\nsample size {a.sample} keys per source — a sample, not a census. REDUNDANT means "
          f"every sampled key was found, which is strong evidence and not a proof; confirm the "
          f"full key set before deleting anything.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
