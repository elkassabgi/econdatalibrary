"""One-off clear of fed_board Z1 csv_retry_queue rows whose served CSV is PROVEN current, byte for byte.

WHY (2026-11-07 diagnosis, review AR-455). 29,975 fed_board:Z1:* ids sit in csv_retry_queue since
2026-09-23. On a runner Z1.parquet exists only in a pass that republished Z1, so every drain attempts
20,000 of them and fails ("no fed_board flow file for 'Z1'"). A desktop heal can rewrite their served CSVs
(tools/derive_csv_bulk.py --source fed_board --qualify-with-shard, from a proven copy of R2's Z1.parquet),
but nothing then removes the queue rows: the next drain still fails on the runner. This tool removes ONLY
rows whose served object is proven to hold exactly the CSV a fresh derive from R2's own Z1.parquet makes.

  py tools/clear_fed_board_z1_retries.py --copy C:\\scratch\\root\\data\\clean_full\\fed_board\\Z1.parquet \\
         --ids F:\\...\\queued_z1_ids.txt --out F:\\...\\clear_evidence.tsv            # report only
  py tools/clear_fed_board_z1_retries.py ... --apply                                    # pull, clear, push

THE EVIDENCE, per id, before anything is touched:
  1. --copy is R2's object clean_full/fed_board/Z1.parquet: size == ContentLength and its md5 / multipart
     reconstruction == the ETag (core.derive_csv._compare_one's rule, through a HEAD-ONLY client, so an
     unknown multipart layout reads "unproven", never a GET). Anything but "equal" refuses (exit 2).
  2. Each id's CSV is derived from the copy in one sorted stream (tools/derive_csv_bulk.py's own _stream_one
     and _csv_bytes, the campaign's code), and N random ids are also derived through the resolver
     (core.derive_csv._series_csv_bytes) and must match it (exit 2 otherwise).
  3. Each served object series/<id>.csv is HEADed: EQUAL when its csvmd5 metadata == md5(local CSV), or,
     without csvmd5, when a plain single-part ETag == md5(local CSV) or a gzip ETag == md5 of this machine's
     r2_util.gzip_bytes(CSV). Every other case (differ, unknown, 404, HEAD error, no local rows) STANDS.
Only EQUAL ids are cleared. Every id's verdict is written to --out.

WITH --apply (the state store lifecycle, R263/R340/R529): refused after T0 (core.cutover); refused while
logs/local_heavy.lock exists or while another writer may start inside the window
(tools.derive_csv_bulk._writers_quiet: the CI writer gate and the desktop pass cadence) - checked BEFORE
the pull and again BEFORE the push; pull-state; the rows are selected by exact id, source fed_board and
prefix fed_board:Z1:; R2's Z1 ETag is HEADed again and must not have moved (a republish meanwhile means
the evidence is about an old release - exit 5, nothing cleared, nothing pushed); the rows are deleted;
push-state (compare-and-swap: a raced writer refuses it, exit 6, and the local clear dies at the next pull).
Without --apply nothing is pulled, cleared or pushed.

READ-ONLY ON R2: the only R2 operation is HeadObject (_HeadOnlyClient refuses every other one). Credentials
come from core.r2_util.client(write=False), loaded in-process; this file never reads a key file and never
prints a credential, the endpoint or an exception message from the client.

Exit codes: 0 every requested id was EQUAL (and, with --apply, cleared and pushed); 1 some ids STAND (the
EQUAL ones are cleared with --apply); 2 refused (usage, cutover, copy not proven, spot mismatch, output);
3 a writer guard refused; 4 pull-state failed; 5 Z1's ETag moved; 6 push-state failed.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import random
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (ROOT, os.path.join(ROOT, "clients", "python")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

BUCKET = "econ-data"
Z1_KEY = "clean_full/fed_board/Z1.parquet"
PREFIX = "fed_board:Z1:"
EXIT_OK, EXIT_STAND, EXIT_REFUSED, EXIT_GUARD, EXIT_PULL, EXIT_MOVED, EXIT_PUSH = 0, 1, 2, 3, 4, 5, 6
HEAD_WORKERS = 16


class _HeadOnlyClient:
    """HeadObject and nothing else: any other attribute raises, so this tool cannot write, list or GET."""

    def __init__(self, s3):
        self._s3 = s3

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def __getattr__(self, name):
        raise RuntimeError(f"clear_fed_board_z1_retries is HEAD-only; {name} refused")


def classify(head, plain) -> str:
    """'equal' only when the served object provably holds `plain` (see the module docstring)."""
    from core.r2_util import gzip_bytes                                  # noqa: PLC0415
    if plain is None:
        return "no_rows"
    if head is None:
        return "error"
    if head.get("status") == "404":
        return "missing"
    if head.get("status") != "200":
        return "error"
    m = hashlib.md5(plain).hexdigest()                                   # noqa: S324
    if head.get("csvmd5"):
        return "equal" if head["csvmd5"].lower() == m else "differ"
    etag = (head.get("etag") or "").strip('"').lower()
    if not etag or "-" in etag:
        return "unknown"
    if (head.get("content_encoding") or "").lower() == "gzip":
        return "equal" if etag == hashlib.md5(gzip_bytes(plain)).hexdigest() else "unknown"   # noqa: S324
    return "equal" if etag == m else "differ"


def head_row(s3, key: str) -> dict:
    """One served object's HEAD as {status, etag, csvmd5, content_encoding}; never raises."""
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
    except Exception as e:                                               # noqa: BLE001
        code = getattr(e, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") \
            if hasattr(e, "response") else None
        return {"status": "404" if code == 404 else f"err:{type(e).__name__}"}
    meta = {str(k).lower(): v for k, v in (h.get("Metadata") or {}).items()}
    return {"status": "200", "etag": (h.get("ETag") or "").strip('"'), "csvmd5": meta.get("csvmd5", ""),
            "content_encoding": h.get("ContentEncoding") or ""}


def prove_copy(s3, copy: str):
    """(etag, detail) when --copy is R2's Z1 object, else (None, detail). _compare_one keys the file by its
    path under the data root, so the copy must sit at <root>/fed_board/Z1.parquet."""
    from core.derive_csv import _compare_one                            # noqa: PLC0415
    data_root = os.path.dirname(os.path.dirname(os.path.abspath(copy)))
    if os.path.basename(copy) != "Z1.parquet" or os.path.basename(os.path.dirname(copy)) != "fed_board":
        return None, f"--copy must be <root>\\fed_board\\Z1.parquet, got {copy}"
    c = _compare_one(s3, BUCKET, os.path.abspath(copy), data_root)
    if c["verdict"] != "equal":
        return None, f"{c['verdict']}: {c['local']} | R2 {c['r2']}"
    h = s3.head_object(Bucket=BUCKET, Key=Z1_KEY)
    return (h.get("ETag") or "").strip('"'), f"equal: {c['local']} | R2 {c['r2']}"


def derive_local(copy: str, ids: list) -> dict:
    """{id: CSV bytes} for every id the copy holds rows for, in ONE sorted stream (derive_csv_bulk's code)."""
    import duckdb                                                        # noqa: PLC0415
    from tools.derive_csv_bulk import _csv_bytes, _stream_one            # noqa: PLC0415
    want = set(ids)
    out = {}
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    for k, rows in _stream_one(con, copy):
        sid = PREFIX + k
        if sid in want:
            out[sid] = _csv_bytes("Z1:" + k, rows)
    return out


def spot_check(copy: str, bodies: dict, n: int, seed: int = 20261107) -> list:
    """Ids among n random ones whose stream bytes differ from core.derive_csv._series_csv_bytes."""
    from core.derive_csv import _series_csv_bytes                        # noqa: PLC0415
    prev = os.environ.get("ECONDL_DATA")
    os.environ["ECONDL_DATA"] = os.path.dirname(os.path.dirname(os.path.abspath(copy)))
    try:
        pick = random.Random(seed).sample(sorted(bodies), min(n, len(bodies)))
        return [s for s in pick if _series_csv_bytes(s) != bodies[s]]
    finally:
        if prev is None:
            os.environ.pop("ECONDL_DATA", None)
        else:
            os.environ["ECONDL_DATA"] = prev


def run_state(*args) -> int:
    p = subprocess.run([sys.executable, "-m", "updater.run", *args], cwd=ROOT, capture_output=True, text=True,
                       encoding="utf-8", errors="replace", timeout=1800)
    for ln in (p.stdout or "").strip().splitlines()[-3:]:
        print("   ", ln, flush=True)
    return p.returncode


def guards_ok(window_min=None) -> bool:
    """local_heavy.lock absent AND derive_csv_bulk._writers_quiet (CI writer gate + desktop cadence)."""
    from tools import derive_csv_bulk as dcb                              # noqa: PLC0415
    lock = os.path.join(ROOT, "logs", "local_heavy.lock")
    if os.path.exists(lock):
        print(f"REFUSED: {lock} is present - a heavy pass may be mid-run (R340/R529).", flush=True)
        return False
    manual = "py tools/clear_fed_board_z1_retries.py ... --apply"
    return bool(dcb._writers_quiet(manual) if window_min is None else dcb._writers_quiet(manual, window_min))


def _read_ids(path: str) -> list:
    with open(path, encoding="utf-8") as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.lstrip().startswith("#")]


def main(argv=None, s3=None, state_runner=None) -> int:
    from core import cutover                                              # noqa: PLC0415
    try:
        cutover.refuse_if_cut_over("clear_fed_board_z1_retries - it clears state rows by what R2 holds")
    except Exception as e:                                               # noqa: BLE001
        print(str(e))
        return EXIT_REFUSED
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--copy", required=True, help="a local copy of R2's clean_full/fed_board/Z1.parquet")
    ap.add_argument("--ids", required=True, help="fed_board:Z1:* ids, one per line")
    ap.add_argument("--out", required=True, help="NEW evidence TSV (not on D: or E:)")
    ap.add_argument("--spot", type=int, default=300, help="random ids also derived through the resolver")
    ap.add_argument("--apply", action="store_true", help="pull state, clear the EQUAL rows, push state")
    a = ap.parse_args(argv)
    runner = state_runner or run_state

    out = os.path.abspath(a.out)
    if out[:2].upper() in ("D:", "E:") or os.path.exists(out) or not os.path.isdir(os.path.dirname(out)):
        print(f"REFUSED: --out must be a new file in an existing folder, not on D: or E: ({out})")
        return EXIT_REFUSED
    ids = _read_ids(a.ids)
    bad = [s for s in ids if not s.startswith(PREFIX)]
    if not ids or bad:
        print(f"REFUSED: --ids must hold only {PREFIX}* ids ({len(ids)} read, first outside: {bad[:3]})")
        return EXIT_REFUSED
    if len(set(ids)) != len(ids):
        print("REFUSED: --ids holds duplicates")
        return EXIT_REFUSED

    if s3 is None:
        from core import r2_util                                          # noqa: PLC0415
        s3 = _HeadOnlyClient(r2_util.client(write=False))
    etag, detail = prove_copy(s3, a.copy)
    print(f"Z1 copy vs R2: {detail}", flush=True)
    if etag is None:
        print("REFUSED: the copy is not R2's object - derive nothing from it")
        return EXIT_REFUSED

    bodies = derive_local(a.copy, ids)
    mism = spot_check(a.copy, bodies, a.spot) if a.spot else []
    if mism:
        print(f"REFUSED: {len(mism)} random id(s) derive differently through the resolver: {mism[:3]}")
        return EXIT_REFUSED
    from tools.derive_csv_bulk import csv_key                             # noqa: PLC0415
    keys = {s: csv_key("series", "fed_board", s.split(":", 1)[1]) for s in ids}
    with ThreadPoolExecutor(HEAD_WORKERS) as ex:
        heads = dict(zip(ids, ex.map(lambda s: head_row(s3, keys[s]), ids)))
    verdict = {s: classify(heads[s], bodies.get(s)) for s in ids}
    with open(out, "x", encoding="utf-8", newline="\n") as fh:
        fh.write("series_id\tverdict\thead_status\tcsvmd5\tetag\n")
        for s in ids:
            h = heads[s]
            fh.write(f"{s}\t{verdict[s]}\t{h.get('status', '')}\t{h.get('csvmd5', '')}\t{h.get('etag', '')}\n")
    tally = {}
    for v in verdict.values():
        tally[v] = tally.get(v, 0) + 1
    equal = [s for s in ids if verdict[s] == "equal"]
    print(f"evidence: {dict(sorted(tally.items()))} over {len(ids):,} id(s) (Z1 ETag {etag}); written to {out}",
          flush=True)
    stand = len(ids) - len(equal)
    if not a.apply:
        print(f"(report only: {len(equal):,} clearable, {stand:,} stand - pass --apply to pull, clear, push)")
        return EXIT_OK if not stand else EXIT_STAND
    if not equal:
        print("nothing clearable; state untouched, no pull, no push")
        return EXIT_STAND

    if not guards_ok():
        return EXIT_GUARD
    if runner("--pull-state") != 0:
        print("REFUSED: pull-state failed; a clear applied to a stale copy dies at the next pull")
        return EXIT_PULL
    from updater.state import StateStore                                 # noqa: PLC0415
    st = StateStore()
    want = set(equal)
    rows =[r["series_id"] for r in st.csv_retries("fed_board")
            if r["series_id"] in want and r["series_id"].startswith(PREFIX)]
    print(f"queue after pull: {len(rows):,} of the {len(equal):,} clearable id(s) are queued", flush=True)
    h = s3.head_object(Bucket=BUCKET, Key=Z1_KEY)
    if (h.get("ETag") or "").strip('"') != etag:
        print(f"REFUSED: Z1's ETag moved ({etag} -> {(h.get('ETag') or '').strip(chr(34))}) - a pass republished "
              f"Z1, the evidence is about the old release. Nothing cleared, nothing pushed.")
        return EXIT_MOVED
    from tools.derive_csv_bulk import CLEAR_WINDOW_MIN                    # noqa: PLC0415
    if not guards_ok(CLEAR_WINDOW_MIN // 2):
        print("REFUSED before the clear: a writer may start inside the push window. Nothing cleared.")
        return EXIT_GUARD
    if not rows:
        print("no queued row to clear; nothing pushed")
        return EXIT_OK if not stand else EXIT_STAND
    st.clear_csv_retries(rows)
    left = sum(1 for r in st.csv_retries("fed_board") if r["series_id"] in set(rows))
    if left:
        print(f"REFUSED: {left} row(s) still present after the delete - not pushing")
        return EXIT_PUSH
    st.db.close()
    if runner("--push-state") != 0:
        print("push-state failed (a writer likely raced us): the clear is LOCAL ONLY and dies at the next pull. "
              "Re-run when the store is quiet.")
        return EXIT_PUSH
    print(f"cleared {len(rows):,} fed_board Z1 csv_retry_queue row(s) and pushed; {stand:,} requested id(s) stand")
    return EXIT_OK if not stand else EXIT_STAND


if __name__ == "__main__":
    sys.exit(main())
