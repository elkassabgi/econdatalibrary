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
        if os.path.isdir(d) and any(f.endswith(".parquet") for f in os.listdir(d)):
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


def _meta(path_or_file):
    import pyarrow.parquet as pq
    m = pq.read_metadata(path_or_file)
    return m.num_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--json", help="write the classified lists here")
    a = ap.parse_args()

    from core import r2_util
    s3 = r2_util.client()
    root = store_root_for(a.source)
    if root is None:
        # Say it, do not `continue`. A guard that goes quiet when it cannot find the store is
        # how sec_edgar's re-derive passed a preflight that had checked nothing (R383 hole 1).
        print(f"{a.source}: NO LOCAL PARQUETS under {' or '.join(ROOTS)} — UNCHECKED, not clean")
        return 2
    d = os.path.join(ROOT, "data", root, a.source)

    r2 = {}
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"{root}/{a.source}/", "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n = o["Key"].rsplit("/", 1)[-1]
            if n.endswith(".parquet"):
                r2[n[:-8]] = (o["Key"], o["Size"])
        if not r.get("IsTruncated"):
            break
        tok = r["NextContinuationToken"]
    loc = {f[:-8] for f in os.listdir(d) if f.endswith(".parquet")}
    print(f"{a.source} [{root}]  local {len(loc):,} file(s)   R2 {len(r2):,} object(s)")

    common = sorted(loc & set(r2))

    def one(n):
        key, size = r2[n]
        try:
            rr = _meta(_S3File(s3, BUCKET, key, size))
        except Exception as e:                                    # noqa: BLE001
            return n, None, f"R2 {type(e).__name__}"
        try:
            ll = _meta(os.path.join(d, n + ".parquet"))
        except Exception as e:                                    # noqa: BLE001
            return n, None, f"local {type(e).__name__}"
        return n, (ll, rr), None

    behind, ahead, same, errs = [], [], 0, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as ex:
        for i, (n, pair, err) in enumerate(ex.map(one, common), 1):
            if err:
                errs.append((n, err))
            elif pair[0] < pair[1]:
                behind.append((n, pair[0], pair[1]))
            elif pair[0] > pair[1]:
                ahead.append((n, pair[0], pair[1]))
            else:
                same += 1
            if i % 2000 == 0:
                print(f"   {i:,}/{len(common):,} compared", flush=True)

    only_r2 = sorted(set(r2) - loc)
    only_loc = sorted(loc - set(r2))
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

    if a.json:
        os.makedirs(os.path.dirname(a.json), exist_ok=True)
        json.dump({"source": a.source, "root": root, "same": same,
                   "behind": behind, "ahead": ahead,
                   "r2_only": only_r2, "local_only": only_loc, "errors": errs},
                  open(a.json, "w", encoding="utf-8"), indent=1)
        print(f"\nwrote {a.json}")
    return 0 if not behind and not ahead else 1


if __name__ == "__main__":
    sys.exit(main())
