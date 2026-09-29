"""Raise the eurostat re-key marker's count for NAMED new flow files, after checking every key of each is stable.

WHY. updater/strategies/fetchers/eurostat.py `_require_rekeyed` refuses a run unless the marker
(clean_full/eurostat/_rekeyed.json) counts exactly as many parquets as the store holds. The 2026-09-22 evening
run created NAIO_10_FGDFEF and NAIO_10_FGDFI, the store went to 7,656 against the marker's 7,654, and every
later run was refused. The fetcher now grows the marker itself for files it creates (`_grow_marker`); this
tool does the same once, by hand, for files created before that fix.

WHAT IT CHECKS before writing (any failure -> nothing written):
  1. the marker exists and its count + len(--files) equals the store's parquet count EXACTLY;
  2. every named file exists on the store;
  3. EVERY series_key of every named file is stable (no 'LAST UPDATE=') - all rows, not a sample;
  4. the guard's own evenly spaced content sample over the whole store passes.
It then writes files_seen = the store count and appends {"utc", "files", "by": "tool"} under `grown`, and reads
the marker back. Dry run by default.

Usage (R2):
  AQUEDUCT_BACKEND=r2 py tools/grow_eurostat_rekey_marker.py --files NAIO_10_FGDFEF.parquet,NAIO_10_FGDFI.parquet
  ... --apply
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config  # noqa: E402
from updater.strategies.fetchers import eurostat as E  # noqa: E402


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--files", required=True, help="comma-separated parquet names created after the marker")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    out_dir = config.source_dir("eurostat")
    path = os.path.join(out_dir, E.REKEY_MARKER)
    named = sorted({f.strip() for f in a.files.split(",") if f.strip()})
    files = blob.list_parquets(out_dir)
    raw = blob.read_bytes(path)
    if not raw:
        print("REFUSED: no re-key marker - run tools/rekey_eurostat.py --apply instead")
        return 2
    m = json.loads(raw.decode("utf-8"))
    seen = m.get("files_seen")
    print(f"backend={config.BACKEND} store parquets={len(files):,} marker files_seen={seen!r} named={named}")
    if not isinstance(seen, int) or seen + len(named) != len(files):
        print(f"REFUSED: marker {seen!r} + {len(named)} named != {len(files)} in the store - the difference is "
              f"not exactly these files")
        return 2
    missing = [n for n in named if n not in set(files)]
    if missing:
        print(f"REFUSED: named file(s) not in the store: {missing}")
        return 2
    for n in named:
        if not E._stable_file(out_dir, n):
            print(f"REFUSED: {n} holds UNSTABLE 'LAST UPDATE=' keys")
            return 2
        print(f"  {n}: every key stable")
    # The guard's own content sample over the whole store, run as if the count already matched (the
    # read of the marker is answered in memory; nothing is written).
    orig_read = blob.read_bytes
    try:
        blob.read_bytes = lambda p: (json.dumps(dict(m, files_seen=len(files))).encode("utf-8")
                                     if p == path else orig_read(p))
        E._require_rekeyed()
    except Exception as e:                                     # noqa: BLE001
        print(f"REFUSED: the guard's content sample fails over the store ({type(e).__name__}: {str(e)[:150]})")
        return 2
    finally:
        blob.read_bytes = orig_read
    if not a.apply:
        print("dry run - nothing written")
        return 0
    m["files_seen"] = len(files)
    m.setdefault("grown", []).append({"utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
                                      "files": named, "by": "tools/grow_eurostat_rekey_marker.py"})
    body = json.dumps(m, sort_keys=True).encode("utf-8")
    blob.write_bytes_atomic(path, body)
    back = blob.read_bytes(path)
    ok = back == body
    print(f"marker written: files_seen={len(files):,}; read back {'IDENTICAL' if ok else 'DIFFERENT'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
