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


def _cursor_local() -> str:
    return os.path.join(ROOT, "data", "_aqueduct", "csv_freshness_cursor.json")


def _load_cursor(blob) -> str:
    """The source this probe stopped after last time, from R2 with a local fallback.

    NOT through the blob helper. `updater.blob` derives an R2 key from a STORE path and rejects
    anything without a `/data/<tier>/` segment, so every attempt to persist
    `_aqueduct/csv_freshness_cursor.json` raised and the probe never advanced. Measured
    2026-08-07: it re-probed abs, adb, barro_lee, bcb, bcrp on every single run and had never
    reached anything past 'b' — a rotating monitor that does not rotate, which is precisely the
    R190 shape it was written to avoid. The warning printed each time and said so; nothing read
    it. This is operational bookkeeping, not store data, so it goes to a plain R2 key.
    """
    try:
        from core import r2_util
        raw = r2_util.client().get_object(Bucket=BUCKET, Key=BOOKMARK)["Body"].read()
        return (json.loads(raw.decode("utf-8")) or {}).get("after", "") or ""
    except Exception:                                                 # noqa: BLE001
        pass
    try:
        with open(_cursor_local(), encoding="utf-8") as fh:
            return (json.load(fh) or {}).get("after", "") or ""
    except Exception:                                                 # noqa: BLE001
        return ""


def _save_cursor(blob, after: str) -> None:
    body = json.dumps({"after": after}, separators=(",", ":")).encode("utf-8")
    wrote = []
    try:
        from core import r2_util
        r2_util.client().put_object(Bucket=BUCKET, Key=BOOKMARK, Body=body,
                                    ContentType="application/json")
        wrote.append("r2")
    except Exception as e:                                            # noqa: BLE001
        print(f"  (bookmark: R2 write failed, {type(e).__name__}) ", flush=True)
    try:
        os.makedirs(os.path.dirname(_cursor_local()), exist_ok=True)
        with open(_cursor_local(), "wb") as fh:
            fh.write(body)
        wrote.append("local")
    except Exception:                                                 # noqa: BLE001
        pass
    if wrote:
        print(f"  rotation bookmark -> after={after!r} ({'+'.join(wrote)})", flush=True)
    else:
        # Never fail the probe over its own bookkeeping — but SAY SO, because a silently
        # unwritten cursor means this rotates nowhere and re-checks the same prefix forever.
        print("WARNING: could not persist the rotation bookmark anywhere; the next run will "
              "re-probe the same sources and the tail of the alphabet stays unchecked",
              flush=True)


def _mirror_matches_store(src: str, sample: int = 4) -> bool:
    """Is the LOCAL parquet mirror at least level with R2 for this source?

    Compared by ROW COUNT and MAX OBSERVATION DATE, deliberately. LastModified is upload time,
    not content-change time, and a parquet re-written with different compression has a
    different md5 with byte-identical data — both proxies produced false verdicts on 2026-08-07
    (R383). Returns False if any sampled file has fewer rows or an earlier max period locally.

    AND, when those two tie, by a DATA-LEVEL fingerprint. Rows and dates cannot see a publisher
    REVISION, which rewrites values in place; on 2026-09-01 that gap left three eurostat flows
    serving superseded numbers, one of them headline real GDP growth, while this probe would
    have reported them level (R549). The fingerprint is computed over values rather than bytes,
    so R383's objection to md5 still holds: a re-encode by a different pyarrow version does not
    move it.
    """
    import os
    import random
    import tempfile

    import duckdb
    from core import r2_util

    # RESOLVE THE STORE ROOT. This hardcoded clean_full on both sides, so every clean_grouped
    # source returned False here and never reached the comparison at all — sec_edgar's 17,276
    # served series were outside this daily probe entirely. False is the safe direction (it
    # reads as "cannot prove level"), but a monitor that silently declines to look at a source
    # is not monitoring it.
    root = None
    for _r in ("clean_full", "clean_grouped"):
        _d = os.path.join(ROOT, "data", _r, src)
        if os.path.isdir(_d) and any(f.endswith(".parquet")
                                     for _dp, _dn, _fs in os.walk(_d) for f in _fs):
            root, d = _r, _d
            break
    if root is None:
        return False
    # WALK. bea and usda nest their parquets one level down, so a flat listdir returns [] and
    # this probe skips them without saying so — in a daily CI step whose whole job is to notice
    # silence (ledger R390).
    files = [f if rel == "." else f"{rel}/{f}"
             for dp, _dn, fs in os.walk(d)
             for rel in [os.path.relpath(dp, d).replace(os.sep, "/")]
             for f in fs if f.endswith(".parquet")]
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
            s3.download_file("econ-data", f"{root}/{src}/{f}", rp)
        except Exception:                                             # noqa: BLE001
            continue
        try:
            ln, lmx = stats(os.path.join(d, *f.split("/")))
            rn, rmx = stats(rp)
        except Exception:                                             # noqa: BLE001
            return False
        if rn > ln or (rmx and lmx and str(rmx) > str(lmx)):
            return False
        if rn == ln and str(rmx) == str(lmx):
            # SAME SHAPE is where a publisher revision hides — it rewrites values and moves
            # neither the row count nor the max date, so everything above clears it. That is
            # how TEC00115 (real GDP growth) was served at a superseded vintage while this
            # probe reported the mirror level (R549). Content fingerprint, not md5: the
            # desktop and CI write parquet with different pyarrow versions, so bytes differ
            # for identical data (R383's objection, still respected).
            from core.derive_csv import content_fingerprint_sql
            try:
                lp = os.path.join(d, *f.split("/")).replace(os.sep, "/")
                rpq = rp.replace(os.sep, "/")
                lcols = [r[0] for r in q.execute(
                    f"describe select * from read_parquet('{lp}')").fetchall()]
                rcols = [r[0] for r in q.execute(
                    f"describe select * from read_parquet('{rpq}')").fetchall()]
                # Describe BOTH sides. Fingerprinting the R2 copy with the LOCAL column list
                # makes a column added upstream invisible — the query simply never reads it.
                if lcols != rcols:
                    return False
                if ln <= 5_000_000:
                    lfp = q.execute(content_fingerprint_sql(lcols, lp)).fetchone()[0]
                    rfp = q.execute(content_fingerprint_sql(rcols, rpq)).fetchone()[0]
                    if lfp != rfp:
                        return False
                else:
                    print(f"  {src}/{f}: {ln:,} rows — same shape but NOT content-checked "
                          f"(over the 5,000,000-row cap); a revision here is not detected",
                          flush=True)
            except Exception:                                         # noqa: BLE001
                # Cannot prove it is level. Same direction as every other failure here.
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
            # INFLATE FIRST. Objects are stored gzip-at-rest since 2026-08-18, and the serving
            # contract is the DECOMPRESSED text - the worker inflates before it does anything
            # (api/worker/src/series.ts:260 keys on ContentEncoding). Comparing compressed
            # bytes against a freshly built CSV makes EVERY gzipped object look stale, and
            # this tool runs daily and prints "users are downloading superseded data".
            # Magic-byte detection, not metadata, so it works on any client copy - the same
            # pattern tools/verify_source_served.py:202 already uses. Commit d866c43d3
            # (2026-08-18) noted this fix was owed to four tools; this is one of them.
            if served[:2] == b"\x1f\x8b":
                import gzip as _gzip                                  # noqa: PLC0415
                try:
                    served = _gzip.decompress(served)
                except Exception:                                     # noqa: BLE001
                    continue      # an unreadable object is not evidence of staleness
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
