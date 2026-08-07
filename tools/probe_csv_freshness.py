"""Do the bytes we SERVE still match the store? Sampled, rotating, and it reddens.

WHY THIS EXISTS. On 2026-08-07 the orchestrator was found to re-derive a series' CSV only on a
run whose status was exactly `ok` (ledger R380). Chronically partial sources never return ok,
so their served objects froze while their parquet advanced — and not merely by a missing tail:
SH.DYN.MORT:PAK served 58.5 for 2023 where the publisher had revised it to 57.8. Fourteen live
sources were affected, ecb worst at 0 of 25 byte-identical.

The gate is fixed and `tests/test_partial_runs_rederive_csvs.py` pins it. But that test asserts
the SHAPE OF THE CODE — it would not notice a different path arriving at the same outcome, and
nothing in `updater/health.py` looks at served bytes at all: it reads state.db, so a source can
be green on freshness while every object a user downloads is a year old. R377's rule is that a
class is not closed until the check is MECHANICAL and EMPIRICAL. This is that check.

HOW IT STAYS CHEAP. Byte-comparing every served object is millions of GETs, so this samples:
a few sources per run, a few series each, and it ROTATES — the bookmark is the last source
checked, and the next run starts after it. Over enough runs the whole surface is covered
without any single run being expensive. That is deliberately the same shape as the fetcher
rotation whose absence caused R190: a bounded pass over a fixed order with no bookmark
re-walks the same prefix forever, which for a MONITOR means permanently blind to the tail.

THE BOOKMARK IS BLOB-ROUTED. A local file is scratch on a CI runner (R36) — it would be lost
every run, the rotation would restart at 'abs' every time, and the sources late in the
alphabet would never be probed while the check reported clean.

    python tools/probe_csv_freshness.py                     # 5 sources x 8 series, rotating
    python tools/probe_csv_freshness.py --sources 12 --sample 15
    python tools/probe_csv_freshness.py --source wid        # one named source, no rotation

Exit 1 if any sampled object differs from what the resolver produces now.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

BUCKET = "econ-data"
BOOKMARK = "_aqueduct/csv_freshness_cursor.json"


def _load_cursor(blob) -> str:
    try:
        raw = blob.read_bytes(BOOKMARK)
        return (json.loads(raw.decode("utf-8")) or {}).get("after", "") if raw else ""
    except Exception:                                                 # noqa: BLE001
        return ""


def _save_cursor(blob, after: str) -> None:
    try:
        blob.write_bytes_atomic(
            BOOKMARK, json.dumps({"after": after}, separators=(",", ":")).encode("utf-8"))
    except Exception as e:                                            # noqa: BLE001
        # Never fail the probe over its own bookkeeping — but SAY SO, because a silently
        # unwritten cursor means this rotates nowhere and re-checks the same prefix forever.
        print(f"WARNING: could not persist the rotation bookmark ({e!r}); the next run will "
              f"re-probe the same sources and the tail of the alphabet stays unchecked",
              flush=True)


def _mirror_matches_store(src: str, sample: int = 4) -> bool:
    """Is the LOCAL parquet mirror at least level with R2 for this source?

    Compared by ROW COUNT and MAX OBSERVATION DATE, deliberately. LastModified is upload time,
    not content-change time, and a parquet re-written with different compression has a
    different md5 with byte-identical data — both proxies produced false verdicts on 2026-08-07
    (R383). Returns False if any sampled file has fewer rows or an earlier max period locally.
    """
    import os
    import random
    import tempfile

    import duckdb
    from core import r2_util

    d = os.path.join(ROOT, "data", "clean_full", src)
    if not os.path.isdir(d):
        return False
    files = [f for f in os.listdir(d) if f.endswith(".parquet")]
    if not files:
        return False
    s3 = r2_util.client()
    q = duckdb.connect()
    tmp = tempfile.mkdtemp()

    def stats(path):
        p = path.replace(os.sep, "/")
        cols = [r[0] for r in q.execute(
            f"describe select * from read_parquet('{p}')").fetchall()]
        dc = [c for c in cols if "date" in c.lower()]
        n = q.execute(f"select count(*) from read_parquet('{p}')").fetchone()[0]
        mx = q.execute(
            f"select max({dc[0]})::VARCHAR from read_parquet('{p}')").fetchone()[0] if dc else None
        return n, mx

    for f in random.Random(0).sample(files, min(sample, len(files))):
        rp = os.path.join(tmp, "r.parquet")
        try:
            s3.download_file("econ-data", f"clean_full/{src}/{f}", rp)
        except Exception:                                             # noqa: BLE001
            continue
        try:
            ln, lmx = stats(os.path.join(d, f))
            rn, rmx = stats(rp)
        except Exception:                                             # noqa: BLE001
            return False
        if rn > ln or (rmx and lmx and str(rmx) > str(lmx)):
            return False
    return True


def _rotate_after(items: list[str], after: str) -> list[str]:
    if not after or after not in items:
        return items
    i = items.index(after) + 1
    return items[i:] + items[:i]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", help="probe these only; disables rotation")
    ap.add_argument("--sources", type=int, default=5, help="how many sources this run")
    ap.add_argument("--sample", type=int, default=8, help="series per source")
    ap.add_argument("--seed", type=int, default=0, help="0 = derive from the cursor")
    a = ap.parse_args()

    from core import r2_util
    from core.derive_csv import _series_csv_bytes
    from updater import blob

    cat = sqlite3.connect(f"file:{os.path.join(ROOT,'data','catalog.db')}?mode=ro", uri=True)
    by_src: dict[str, int] = {r[0]: r[1] for r in cat.execute(
        "SELECT source_id, count(*) FROM series GROUP BY source_id")}

    if a.source:
        targets, cursor = [s for s in a.source if s in by_src], None
    else:
        cursor = _load_cursor(blob)
        order = sorted(by_src)
        targets = _rotate_after(order, cursor)[: a.sources]
        print(f"rotating after {cursor!r}: probing {targets}")

    s3 = r2_util.client()
    rnd = random.Random(a.seed or (hash(cursor or "start") & 0xFFFF))
    bad_sources: list[tuple[str, int, int, str]] = []
    skipped: list[str] = []
    total_bad = total_cmp = 0

    for src in targets:
        # THE MIRROR GATE (ledger R383). `_series_csv_bytes` resolves from data/clean_full/,
        # which under AQUEDUCT_BACKEND=r2 is a SCRATCH copy of whatever this machine last ran.
        # If it is behind R2, every comparison below reports the served object as stale when
        # only the mirror is — this probe did exactly that for bcb and bcrp within an hour of
        # being written. Judge by CONTENT, never by LastModified (upload time, not change
        # time) and never by hash (a re-encoded parquet differs with identical data).
        if not _mirror_matches_store(src):
            print(f"  SKIP   {src:24s} local mirror is behind R2 — cannot judge served bytes "
                  f"from it; sync the source's parquets first")
            skipped.append(src)
            continue
        ids = [r[0] for r in cat.execute(
            "SELECT series_id FROM series WHERE source_id=?", (src,))]
        if not ids:
            continue
        pick = rnd.sample(ids, min(a.sample, len(ids)))
        bad, n, first = 0, 0, ""
        for sid in pick:
            key = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
            try:
                served = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            except Exception:                                         # noqa: BLE001
                continue          # absent object is the MISSING class, not staleness
            try:
                fresh = _series_csv_bytes(sid)
            except Exception:                                         # noqa: BLE001
                continue          # unresolvable locally — a mirror gap, not a serving defect
            n += 1
            if served != fresh:
                bad += 1
                first = first or sid
        total_bad += bad
        total_cmp += n
        if bad:
            bad_sources.append((src, bad, n, first))
            print(f"  STALE  {src:24s} {bad}/{n} differ   e.g. {first}")
        else:
            print(f"  ok     {src:24s} {n} identical")

    if targets and not a.source:
        _save_cursor(blob, targets[-1])

    print(f"\ncompared {total_cmp} object(s) across {len(targets) - len(skipped)} source(s); "
          f"{total_bad} stale")
    if skipped:
        print(f"SKIPPED — local mirror behind R2, verdict WITHHELD (not 'clean'): {skipped}")
    if bad_sources:
        print("SERVED BYTES DISAGREE WITH THE STORE — users are downloading superseded data.")
        print("Repair: python tools/repair_stale_csvs.py --source <sid> --apply")
        print("Then ask WHY it went stale: something merged without re-deriving (R380).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
