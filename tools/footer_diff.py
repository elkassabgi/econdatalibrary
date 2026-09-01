"""Row-count every parquet in a source, LOCAL vs R2, by reading footers — not sampling.

WHY FOOTERS AND NOT A SAMPLE. `core/derive_csv.py`'s preflight samples a few files per source
and asks "is the mirror behind?". An adversarial audit measured what that misses: dst has 21 of
813 files behind, so a 4-file sample catches it about 10% of the time — and it didn't, so a
re-derive proceeded and rolled 37 served series back a full year. A parquet footer is a ranged
GET of a few KB; 17,322 of them take minutes. At that price there is no reason to guess.

WHY NOT SIZE, LastModified OR md5. Bytes shrink when a file re-encodes; LastModified is upload
time, not content (7 of 9 flags from it were false, ledger R383); md5 changes on any re-write.
Row count plus max obs date is the only comparison that answers the question asked.

WHAT IT REFUSES TO DO. It classifies and prints. It never copies, because the direction is not
uniform: sec_edgar has 2,039 files where R2 is ahead AND 6 where LOCAL is ahead — XOM holds
20,629 local rows against 274 on R2, because Exxon re-registered under a new CIK and the
refresher overwrote 18 years of history with the new registrant's 274 facts. A blind sync in
either direction destroys one side. AHEAD files are a MERGE queue and are reported as such.

    python tools/footer_diff.py --source sec_edgar
    python tools/footer_diff.py --source dst --json data/_probe/dst_diff.json
"""
from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"
ROOTS = ("clean_full", "clean_grouped")


def store_root_for(src: str) -> str | None:
    for r in ROOTS:
        d = os.path.join(ROOT, "data", r, src)
        if os.path.isdir(d) and any(f.endswith(".parquet")
                                    for _dp, _dn, fs in os.walk(d) for f in fs):
            return r
    return None


class _S3File(io.RawIOBase):
    """Just enough file object for pyarrow to read a footer over HTTP range requests.

    pq.read_metadata seeks to the end, reads the 8-byte trailer, then reads the footer. That is
    two or three small ranged GETs instead of downloading a 100 KB-2 MB parquet, which is what
    makes footer-diffing 17,000 files practical at all.
    """

    def __init__(self, client, bucket, key, size):
        self._c, self._b, self._k, self._n, self._pos = client, bucket, key, size, 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self._pos

    def seek(self, off, whence=0):
        self._pos = off if whence == 0 else (self._pos + off if whence == 1 else self._n + off)
        return self._pos

    def read(self, n=-1):
        if n is None or n < 0:
            n = self._n - self._pos
        if n <= 0 or self._pos >= self._n:
            return b""
        end = min(self._n, self._pos + n) - 1
        body = self._c.get_object(Bucket=self._b, Key=self._k,
                                  Range=f"bytes={self._pos}-{end}")["Body"].read()
        self._pos += len(body)
        return body


def file_meta(path_or_file):
    """(num_rows, max observation date as a string or None) from the footer alone.

    THE DATE WAS MISSING AND THE DOCSTRING SAID IT WAS THERE. This returned `m.num_rows` and
    nothing else while the header above claimed "row count plus max obs date is the only
    comparison that answers the question asked", so every verdict this tool has ever produced
    was a row-count verdict. That gap is not theoretical: on 2026-09-01, after a full `--all`
    run reported 0 files behind across 322 sources, a content fingerprint found **fed_board
    differing on 11 of 36 objects and fhfa on 2 of 18 — every one at an identical row count**
    (264 vs 264, 2,681 vs 2,681). A count cannot see a restatement.

    Max date does not catch a pure value revision either — nothing short of reading the data
    does — but it catches new observations that leave the row count unchanged, and it is FREE:
    parquet row-group statistics already carry per-column min/max, so this adds no I/O to a
    footer read that was happening anyway.
    """
    import pyarrow.parquet as pq
    m = pq.read_metadata(path_or_file)
    names = [m.schema.column(i).name for i in range(m.num_columns)]
    di = next((i for i, c in enumerate(names)
               if c.lower() in ("obs_date", "date", "time_period")), None)
    if di is None:
        return m.num_rows, None
    best = None
    for g in range(m.num_row_groups):
        try:
            st = m.row_group(g).column(di).statistics
        except Exception:                                            # noqa: BLE001
            continue
        if st is not None and getattr(st, "has_min_max", False):
            v = str(st.max)
            if best is None or v > best:
                best = v
    return m.num_rows, best


def classify(local, remote):
    """'behind' | 'ahead' | 'same' for a (rows, max_date) pair on each side.

    A file ahead on ONE axis and behind on the other is DIVERGED, and is filed as `ahead` on
    purpose: `ahead` is the merge queue that mirror_sync never overwrites, so an ambiguous file
    is left alone rather than synced in a direction that loses rows.
    """
    lr, ld = local
    rr, rd = remote
    dated = ld is not None and rd is not None
    r2_newer = (rr > lr) or (dated and rd > ld)
    loc_newer = (lr > rr) or (dated and ld > rd)
    if r2_newer and not loc_newer:
        return "behind"
    if loc_newer:
        return "ahead"
    return "same"


def _meta(path_or_file):
    return file_meta(path_or_file)


def catalogued_sources():
    import sqlite3
    con = sqlite3.connect(
        f"file:{os.path.join(ROOT, 'data', 'catalog.db')}?mode=ro", uri=True)
    return [r[0] for r in con.execute(
        "select distinct source_id from series order by source_id")]


def one_source(s3, src, workers, json_path=None, quiet=False):
    root = store_root_for(src)
    if root is None:
        # Say it, do not `continue`. A guard that goes quiet when it cannot find the store is
        # how sec_edgar's re-derive passed a preflight that had checked nothing (R383 hole 1).
        if quiet:
            return None
        print(f"{src}: NO LOCAL PARQUETS under {' or '.join(ROOTS)} — UNCHECKED, not clean")
        return 2
    d = os.path.join(ROOT, "data", root, src)

    # KEY ON THE RELATIVE PATH, NOT THE BASENAME, and walk the local tree. Two stores are
    # nested: bea keeps clean_full/bea/<Dataset>/<Table>.parquet and eia
    # clean_full/eia/<subdir>/<key>.parquet. Flattening to the basename made eia's 60 objects
    # collapse into 30 names — each local file compared against whichever of its two namesakes
    # the listing happened to yield last — and reported "30 files AHEAD of the store", a
    # finding that was entirely an artefact of my own key. It also made all 588 of bea's nested
    # objects look R2-only, because os.listdir sees only the top directory.
    prefix = f"{root}/{src}/"
    r2 = {}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            k = o["Key"]
            if k.endswith(".parquet"):
                r2[k[len(prefix):-8]] = (k, o["Size"])
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    loc = set()
    for dirpath, _dirs, files in os.walk(d):
        rel = os.path.relpath(dirpath, d).replace(os.sep, "/")
        for f in files:
            if f.endswith(".parquet"):
                loc.add(f[:-8] if rel == "." else f"{rel}/{f[:-8]}")
    if not quiet:
        print(f"{src} [{root}]  local {len(loc):,} file(s)   R2 {len(r2):,} object(s)")

    common = sorted(loc & set(r2))

    def one(n):
        key, size = r2[n]
        try:
            rr = _meta(_S3File(s3, BUCKET, key, size))
        except Exception as e:                                    # noqa: BLE001
            return n, None, f"R2 {type(e).__name__}"
        try:
            ll = _meta(os.path.join(d, *(n + ".parquet").split("/")))
        except Exception as e:                                    # noqa: BLE001
            return n, None, f"local {type(e).__name__}"
        return n, (ll, rr), None

    behind, ahead, same, errs = [], [], 0, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for i, (n, pair, err) in enumerate(ex.map(one, common), 1):
            if err:
                errs.append((n, err))
            else:
                # The tuples stay (name, local_rows, r2_rows): tools/mirror_sync.py unpacks
                # exactly three, and widening them here would break the consumer.
                verdict = classify(pair[0], pair[1])
                if verdict == "behind":
                    behind.append((n, pair[0][0], pair[1][0]))
                elif verdict == "ahead":
                    ahead.append((n, pair[0][0], pair[1][0]))
                else:
                    same += 1
            if not quiet and i % 2000 == 0:
                print(f"   {i:,}/{len(common):,} compared", flush=True)

    only_r2 = sorted(set(r2) - loc)
    only_loc = sorted(loc - set(r2))
    res = {"source": src, "root": root, "same": same, "behind": behind, "ahead": ahead,
           "r2_only": only_r2, "local_only": only_loc, "errors": errs}
    if quiet:
        return res
    print(f"\nSAME            {same:,}")
    print(f"LOCAL BEHIND    {len(behind):,}   (sync these from R2)")
    print(f"LOCAL AHEAD     {len(ahead):,}   (MERGE queue — do NOT overwrite)")
    print(f"R2 ONLY         {len(only_r2):,}   local ONLY {len(only_loc):,}   errors {len(errs):,}")
    for n, l, r_ in sorted(ahead, key=lambda t: -(t[1] - t[2]))[:20]:
        print(f"   AHEAD  {n:14s} local {l:>9,} rows  R2 {r_:>9,}")
    for n, l, r_ in sorted(behind, key=lambda t: -(t[2] - t[1]))[:10]:
        print(f"   behind {n:14s} local {l:>9,} rows  R2 {r_:>9,}")
    for n, e in errs[:5]:
        print(f"   ERROR  {n}: {e}")

    if json_path:
        os.makedirs(os.path.dirname(json_path), exist_ok=True)
        json.dump(res, open(json_path, "w", encoding="utf-8"), indent=1)
        print(f"\nwrote {json_path}")
    return 0 if not behind and not ahead else 1




def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", action="append", default=[])
    ap.add_argument("--all", action="store_true",
                    help="every catalogued source — the fleet answer, not one at a time")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--json", help="write the classified lists here")
    a = ap.parse_args()

    from core import r2_util
    s3 = r2_util.client()
    if not a.all:
        if len(a.source) != 1:
            print("pass exactly one --source, or --all")
            return 2
        return one_source(s3, a.source[0], a.workers, a.json)

    srcs = a.source or catalogued_sources()
    print(f"sweeping {len(srcs):,} catalogued source(s) by parquet footer\n")
    rows, unchecked = [], []
    for i, src in enumerate(srcs, 1):
        r = one_source(s3, src, a.workers, quiet=True)
        if r is None:
            unchecked.append(src)
            print(f"  [{i:>3}/{len(srcs)}] {src:22s} NO LOCAL PARQUETS — UNCHECKED", flush=True)
            continue
        rows.append(r)
        flag = "" if not (r["behind"] or r["ahead"]) else "  <-- "
        print(f"  [{i:>3}/{len(srcs)}] {src:22s} same {r['same']:>6,}  behind {len(r['behind']):>5,}"
              f"  ahead {len(r['ahead']):>4,}  r2-only {len(r['r2_only']):>5,}{flag}", flush=True)

    dirty = [r for r in rows if r["behind"] or r["ahead"]]
    print(f"\n{len(rows):,} source(s) compared, {len(unchecked):,} UNCHECKED (no local store)")
    print(f"{len(dirty):,} diverge:  {sum(len(r['behind']) for r in rows):,} local-behind file(s), "
          f"{sum(len(r['ahead']) for r in rows):,} local-ahead, "
          f"{sum(len(r['r2_only']) for r in rows):,} R2-only")
    for r in sorted(dirty, key=lambda r: -(len(r["behind"]) + len(r["ahead"]))):
        print(f"   {r['source']:22s} behind {len(r['behind']):>5,}  ahead {len(r['ahead']):>4,}")
    if unchecked:
        # NOT "clean". A source with no local mirror cannot be compared, and calling that a
        # pass is the exact hole that hid sec_edgar (R383/R386).
        print(f"\nUNCHECKED ({len(unchecked)}): {', '.join(unchecked)}")
    if a.json:
        os.makedirs(os.path.dirname(a.json), exist_ok=True)
        json.dump({"sources": rows, "unchecked": unchecked},
                  open(a.json, "w", encoding="utf-8"), indent=1)
        print(f"wrote {a.json}")
    return 0 if not dirty else 1


if __name__ == "__main__":
    sys.exit(main())
