"""Sync a source's parquets from R2, then repair its served CSVs only if they are stale.

WHY THE SYNC COMES FIRST, and why that ordering is the whole point. `core/derive_csv.py`
derives FROM `data/clean_full/`, and `tools/verify_source_served.py` compares served bytes
AGAINST the same directory. When that mirror is behind R2, both read the same wrong copy: the
derive publishes stale bytes and the verify then certifies them as correct. That is not
hypothetical — it produced a sixteen-line table of "verified" repairs that an adversarial audit
took apart (ledger R385), with ons_uk serving 31,878 rows against a 37,950-row store parquet
while the checker printed 25/25 identical.

The same audit measured the scale from scratch (parquet footer row counts over 55,394 local
files vs 36,972 R2 objects): 1,379 local files BEHIND R2 — ilostat 952, eurostat 124, owid 58,
ember 26, boe 25, statfin 23, ssb 22, dst 21, defillama 18, ksh_stadat 15, fed_board 13,
cso 12 — plus 79 AHEAD, with six sources diverging both ways at once.

So per source: SYNC from the authoritative store, SAMPLE the served objects against the now
correct mirror, and re-derive ONLY if the sample proves staleness. Sampling first matters —
the true stale rate ranges from 0% to 100% and a blanket re-derive would rewrite millions of
billed Class A PUTs to fix a handful.

WHAT THIS DELIBERATELY DOES NOT DO: push local over R2 when local is AHEAD. That divergence is
two-directional on the same files — abs/LF_HOURS has 160 rows on R2 that are not local,
zillow/State_zhvi differs in every row, sec_edgar/XOM is local 20,629 vs R2 274 yet R2's max
date is NEWER. A blind push destroys data. Ahead files are reported and left alone.

    python tools/resync_and_repair.py --source dst --source cso
    python tools/resync_and_repair.py --source eurostat --apply
"""
from __future__ import annotations

import argparse
import concurrent.futures
import os
import random
import sqlite3
import subprocess
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

BUCKET = "econ-data"


STORE_ROOTS = ("clean_full", "clean_grouped")


def store_root_for(src: str) -> str:
    """Which data/<root>/ holds this source? NOT always clean_full.

    sec_edgar's 17,276 parquets live under data/clean_grouped/. Assuming clean_full made the
    R383 preflight blind to it — the directory did not exist, so the guard returned "nothing
    behind" and its re-derive proceeded from a mirror three months stale (RF.parquet: local
    43,739 rows to 2026-05-06 against R2's 44,330 to 2026-08-05).
    """
    for r in STORE_ROOTS:
        d = os.path.join(ROOT, "data", r, src)
        if os.path.isdir(d) and any(f.endswith(".parquet") for f in os.listdir(d)):
            return r
    return "clean_full"


def sync(src: str, workers: int = 16) -> int:
    """Pull ONLY the files R2 is ahead on. Never the ones where local is ahead.

    THIS USED TO COPY EVERY FILE. Divergence is not one-directional: an adversarial audit
    measured ilostat as 952 files behind R2 and 41 files AHEAD of it at the same time, and this
    function's blind copy overwrote those 41 and destroyed 967,043 rows that existed nowhere
    else (ledger R388). Six sources diverge both ways at once. So the copy list now comes from
    `tools/footer_diff.py`, which row-counts every file on both sides, and the ahead set is
    reported and skipped rather than quietly flattened.
    """
    from core import r2_util
    from tools.footer_diff import one_source
    s3 = r2_util.client()
    root = store_root_for(src)
    d = os.path.join(ROOT, "data", root, src)
    os.makedirs(d, exist_ok=True)

    cls = one_source(s3, src, workers, quiet=True)
    if cls is None:
        print(f"  {src}: no local parquets — UNCHECKED, refusing to sync blind")
        return 0
    names = [n for n, _l, _r in cls["behind"]] + list(cls["r2_only"])
    if cls["ahead"]:
        print(f"  {src}: {len(cls['ahead'])} file(s) where LOCAL IS AHEAD are being LEFT ALONE "
              f"— they need a merge, not a copy: "
              f"{', '.join(n for n, _l, _r in cls['ahead'][:6])}")
    if not names:
        print(f"  {src}: mirror already level with the store ({cls['same']:,} file(s))")
        return 0

    def one(n):
        dest = os.path.join(d, *(n + ".parquet").split("/"))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        s3.download_file(BUCKET, f"{root}/{src}/{n}.parquet", dest)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, names))
    print(f"  {src}: pulled {len(names):,} of {len(names) + cls['same'] + len(cls['ahead']):,} "
          f"parquet(s) from r2://econ-data/{root}/ (only where R2 was ahead)")
    return len(names)


def stale_sample(src: str, k: int, seed: int):
    """(mismatches, compared, example). Run AFTER sync, so the mirror is the store."""
    from core import r2_util
    from core.derive_csv import _series_csv_bytes
    s3 = r2_util.client()
    con = sqlite3.connect(f"file:{os.path.join(ROOT,'data','catalog.db')}?mode=ro", uri=True)
    ids = [r[0] for r in con.execute(
        "SELECT series_id FROM series WHERE source_id=?", (src,))]
    if not ids:
        return 0, 0, ""
    bad = n = 0
    ex = ""
    for sid in random.Random(seed).sample(ids, min(k, len(ids))):
        key = "series/" + urllib.parse.quote(sid, safe="") + ".csv"
        try:
            served = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            want = _series_csv_bytes(sid)
        except Exception:                                             # noqa: BLE001
            continue
        n += 1
        if served != want:
            bad += 1
            ex = ex or sid
    return bad, n, ex


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", required=True)
    ap.add_argument("--sample", type=int, default=15)
    ap.add_argument("--seed", type=int, default=20260807)
    ap.add_argument("--apply", action="store_true", help="re-derive the proven-stale sources")
    ap.add_argument("--no-sync", action="store_true", help="assume the mirror is already level")
    a = ap.parse_args()
    print(f"MODE: {'APPLY' if a.apply else 'REPORT ONLY'}  sources={a.source}\n")

    stale = []
    for src in a.source:
        print(f"=== {src}")
        if not a.no_sync:
            sync(src)
        bad, n, ex = stale_sample(src, a.sample, a.seed)
        if n == 0:
            print(f"  {src}: nothing comparable (no catalogue rows or no objects)")
            continue
        if bad:
            stale.append(src)
            print(f"  {src}: STALE — {bad}/{n} sampled differ (e.g. {ex})")
        else:
            print(f"  {src}: {n} sampled identical against the SYNCED mirror")

    print(f"\nproven stale: {stale or 'none'}")
    if not stale or not a.apply:
        if stale:
            print("report only — re-run with --apply to re-derive")
        return 1 if stale else 0

    for src in stale:
        print(f"\n=== re-deriving {src}", flush=True)
        env = dict(os.environ, AQUEDUCT_BACKEND="r2", PYTHONIOENCODING="utf-8")
        r = subprocess.run([sys.executable, "-m", "core.derive_csv", "--source", src,
                            "--bucket", BUCKET, "--workers", "12"],
                           cwd=ROOT, env=env, capture_output=True, text=True)
        tail = [l for l in (r.stdout or "").splitlines() if l.strip()][-2:]
        print("\n".join(tail) or (r.stderr or "")[-300:], flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
