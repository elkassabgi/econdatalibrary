"""Which store files has the fetcher never touched? The generic form of the census blind spot.

WHY. A fetcher covers the files it enumerates. Anything it leaves out is invisible to every
instrument we have: nothing fails, no gate fires, and the coverage audit still counts the source
as SERVED and SCHEDULED — because it is. census sat like that for months. Its header asserted the
other 60 of its 80 files "do not gain periods, they gain a whole new reference year"; Census's own
catalogue lists every one as a timeseries dataset, and 16 of them were two months behind with
45,659 rows waiting on exports/hs alone. Nothing was broken. The 21 flows the fetcher did cover
were genuinely current, so "census is up to date" was true of the measured part and false of the
whole (ledger R284).

WHAT THIS MEASURES, and what it deliberately does not. It compares each store file's
LastModified against the newest file in that source. A file the fetcher writes on a normal run is
recent; a file it never enumerates keeps whatever date the first-pass ingest left. So the output
is "N of M files have not moved since <date>, while the source's newest is <date>" — a question,
not a verdict.

IT IS NOT A BUG REPORT. Plenty of untouched files are correct: a retired flow (eits__mhs, the
Manufactured Housing Survey, frozen at 2014 on purpose), a source whose fetcher legitimately
writes ONE consolidated file beside a served tree written by the ingester (bea writes bea.parquet
and nothing else — 592 files, 591 untouched, entirely correct), a genuinely static reference
table. The point is to make the SET visible so each member gets a reason, because today they have
none and nobody is looking.

    python tools/audit_untouched_files.py census bea
    python tools/audit_untouched_files.py --live          # every live source
    python tools/audit_untouched_files.py --live --days 30
"""
from __future__ import annotations
import argparse
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import r2_util                                          # noqa: E402
from updater import config, registry                              # noqa: E402

BUCKET = "econ-data"


def _prefix_for(source: str) -> str:
    """The R2 key prefix holding this source's store files."""
    d = config.source_dir(source)
    root = os.path.dirname(os.path.dirname(os.path.abspath(config.DATA_ROOT)))
    rel = os.path.relpath(d, root).replace(os.sep, "/")
    return rel.lstrip("./") + "/"


def scan(source: str):
    """[(key, LastModified)] for every parquet under the source's prefix."""
    c = r2_util.client()
    out, tok = [], None
    prefix = f"clean_full/{source}/"
    while True:
        kw = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = c.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith(".parquet"):
                out.append((o["Key"][len(prefix):], o["LastModified"]))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="*")
    ap.add_argument("--live", action="store_true", help="every live registry source")
    ap.add_argument("--days", type=float, default=14.0,
                    help="a file older than this many days behind the source's newest is 'untouched'")
    a = ap.parse_args()

    srcs = list(a.sources)
    if a.live:
        srcs += [e["source_id"] for e in registry.load().get("sources", []) if e.get("live")]
    srcs = sorted(dict.fromkeys(srcs))
    if not srcs:
        print("name at least one source, or pass --live")
        return 2

    flagged = 0
    for s in srcs:
        try:
            files = scan(s)
        except Exception as e:                                    # noqa: BLE001
            print(f"{s}: cannot list ({type(e).__name__})")
            continue
        if not files:
            print(f"{s}: no parquet files")
            continue
        newest = max(lm for _, lm in files)
        cutoff = newest - dt.timedelta(days=a.days)
        stale = [(k, lm) for k, lm in files if lm < cutoff]
        if not stale:
            print(f"{s:<18} {len(files):>5} file(s), all within {a.days:g}d of newest "
                  f"({newest:%Y-%m-%d}) — the fetcher reaches all of them")
            continue
        flagged += 1
        pct = len(stale) / len(files) * 100
        print(f"\n{s:<18} {len(stale):,} of {len(files):,} file(s) ({pct:.0f}%) NOT written since "
              f"{cutoff:%Y-%m-%d}; source newest {newest:%Y-%m-%d}")
        oldest = sorted(stale, key=lambda kv: kv[1])[:5]
        for k, lm in oldest:
            print(f"    {lm:%Y-%m-%d}  {k}")
        if len(stale) > 5:
            print(f"    ... and {len(stale) - 5:,} more")
        print("    -> each of these needs a REASON: retired flow, ingester-owned tree, static "
              "reference table, or a gap nobody has noticed.")

    print(f"\n{flagged} of {len(srcs)} source(s) hold files the fetcher has not written recently")
    # Informational by design: an untouched file is a QUESTION. Exiting non-zero here would make
    # a correct source (bea writes one file beside an ingester-owned tree) fail a gate for ever,
    # and a gate that is always red is one nobody reads.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
