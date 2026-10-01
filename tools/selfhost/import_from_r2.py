"""Copy objects from R2 `econ-data` into the self-hosted blob store, verified object by object.

docs/ECON_SELF_HOSTING_PLAN.md step 2. READ-ONLY on R2 (GetObject/ListObjectsV2 only). Each object is
checked before it is stored:
  * single-part etag  = MD5 of the bytes;
  * multipart etag    = MD5 of the concatenated part MD5s + "-N"; the part size is not recorded, so it is
                        recomputed with 8 MiB and 64 MiB parts (the sizes econ's uploaders use), then every
                        whole-MiB size that the part count allows. No match = a copy failure (review R1164).
Content-Encoding, Content-Type and custom metadata are kept, so the worker serves exactly what R2 served.

    python tools/selfhost/import_from_r2.py --root <blob store> --key series/<...>.csv [--key ...]
    python tools/selfhost/import_from_r2.py --root <blob store> --prefix _aqueduct/stats.json

BULK (plan step 2: ~14M objects, 798 GB): --workers N copies N objects at a time; --resume skips an object the
store already holds with the same etag, size AND LastModified (whole seconds) as the listing (so an interrupted
run continues where it stopped, and a re-run copies only what changed on R2 since); the listing is streamed page
by page, never held whole in memory; --quiet prints only FAIL lines; --progress FILE is rewritten every 30 s with
the counts, bytes, rate and the last listed key; --limit N stops after N objects are listed (a trial).

After the copy, each --prefix is listed again and merge-compared with the store: an object the store holds that
R2 no longer has (deleted or re-keyed on R2) is counted, written to --absent-out, and fails the run (rc 1);
--prune-absent (with --absent-out as its receipt) deletes it from the store instead, only after the whole merge
finished on a strictly ascending listing, within --prune-max, and with R2's own HEAD answering 404 for that key.
Before T0 only - after T0 the store is newer than R2.

    python tools/selfhost/import_from_r2.py --root F:/econ_live/blobs --create --prefix series/ \\
        --workers 16 --resume --quiet --progress F:/econ_selfhost_probe/import/series.progress.json
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from blobstore import BlobStore  # noqa: E402

BUCKET = "econ-data"
MIB = 1 << 20
# The run start that splits NEW from MISSING is taken this much LATER than this machine's clock says: the clock
# is compared with R2's LastModified. Windows does sync it - the scheduled task "SynchronizeTime" ran 2026-09-30
# with result 0; the w32time service starts for the sync and stops, so "service not running" is its normal state
# (an earlier version of this comment said nothing kept the clock right: wrong, ledger R1339) - but a weekly sync
# still drifts between runs (~1 s on 2026-10-01). A clock BEHIND R2 makes a key written just before the run
# look NEW, which would hide it if the copy listing lost it (AR-182 round 5). Later only turns a key written in
# the run's first minutes into a reported MISSING (fails closed; the next run copies it). An EARLIER start is
# the wrong way round - it makes more keys NEW (the first draft of this fix did that; its test caught it).
CLOCK_MARGIN_S = 600


def multipart_etag(data: bytes, part_size: int) -> str:
    parts = [data[i:i + part_size] for i in range(0, len(data), part_size)] or [b""]
    digest = hashlib.md5(b"".join(hashlib.md5(p).digest() for p in parts)).hexdigest()
    return f"{digest}-{len(parts)}"


def etag_matches(data: bytes, etag: str) -> bool:
    etag = etag.strip('"')
    if "-" not in etag:
        return hashlib.md5(data).hexdigest() == etag
    n = int(etag.rsplit("-", 1)[1])
    tried = set()
    for ps in (8 * MIB, 64 * MIB):
        tried.add(ps)
        if multipart_etag(data, ps) == etag:
            return True
    # every whole-MiB part size that yields exactly n parts
    if n >= 1 and data:
        lo = max(1, -(-len(data) // n) // MIB)
        hi = (len(data) // max(1, n - 1)) // MIB if n > 1 else len(data) // MIB + 1
        for mib in range(lo, hi + 1):
            ps = mib * MIB
            if ps in tried:
                continue
            if -(-len(data) // ps) == n and multipart_etag(data, ps) == etag:
                return True
    return False


def copy_one(s3, store: BlobStore, key: str, overwrite: bool = False,
             restore_missing: bool = False) -> tuple[bool, str]:
    # AFTER T0 the store is what users are served and R2 is a frozen copy of the past:
    #   * the store's write rule applies (the live checkout and the single-writer lock - R1217 finding 3);
    #   * an object the store holds is NEWER than R2's: kept unless --overwrite (a deliberate repair);
    #   * an object the store does NOT hold may be missing on purpose - a licence removal deletes it, and
    #     importing it again put a retired source's CSVs back on sale (R1220 finding 1): refused unless
    #     --restore-missing, and a key under a logged licence removal is refused whatever the flags say.
    from core import cutover                                        # noqa: PLC0415
    if cutover.is_cut_over():
        from updater.blob import _refuse_or_own_store_write         # noqa: PLC0415
        from core.licence_targets import removed_csv_prefixes       # noqa: PLC0415
        _refuse_or_own_store_write(f"import {key} from the frozen R2 copy")
        gone = [p for p in removed_csv_prefixes() if key.startswith(p)]
        if gone:
            raise cutover.CutoverRefused(f"refused: {key} is under {gone[0]}, which a licence removal took out "
                                         f"of the store after T0 - it is never restored from R2 (R1220)")
        held = store.head(key) is not None
        if held and not overwrite:
            return True, f"{key} kept: the store already holds it, and after T0 it is newer than R2"
        if not held and not restore_missing:
            return False, (f"{key} NOT imported: the store does not hold it, and after T0 an absent object may "
                           f"have been removed on purpose - pass --restore-missing to put it back")
    r = s3.get_object(Bucket=BUCKET, Key=key)
    data = r["Body"].read()
    etag = r["ETag"].strip('"')
    if not etag_matches(data, etag):
        return False, f"etag mismatch for {key} ({len(data):,} bytes, etag {etag})"
    lm = r.get("LastModified")                       # boto3: a tz-aware datetime
    stored = lm.astimezone(dt.timezone.utc).isoformat(timespec="seconds") if lm is not None else None
    store.put(key, data, etag=etag, content_encoding=r.get("ContentEncoding"),
              content_type=r.get("ContentType"), custom_metadata=r.get("Metadata") or {}, stored_utc=stored)
    return True, f"{key} {len(data):,} bytes etag ok"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--key", action="append", default=[])
    ap.add_argument("--prefix", action="append", default=[])
    ap.add_argument("--create", action="store_true", help="create the blob store if it does not exist")
    ap.add_argument("--overwrite", action="store_true",
                    help="after T0, replace an object the store already holds (a repair; before T0 always)")
    ap.add_argument("--restore-missing", action="store_true",
                    help="after T0, import an object the store does not hold (never one a licence removal took)")
    ap.add_argument("--workers", type=int, default=1, help="objects copied at once (bulk)")
    ap.add_argument("--resume", action="store_true",
                    help="skip an object the store already holds with the listing's etag and size")
    ap.add_argument("--quiet", action="store_true", help="print only FAIL lines and the summary")
    ap.add_argument("--progress", help="rewrite this JSON file every 30 s with the run's counts")
    ap.add_argument("--limit", type=int, default=0, help="stop after this many listed objects (a trial)")
    ap.add_argument("--absent-out", help="write the keys the store holds under --prefix that R2 no longer has")
    ap.add_argument("--prune-absent", action="store_true",
                    help="before T0, delete from the store what R2 no longer has under --prefix (else rc 1); "
                         "needs --absent-out (the receipt); each key is confirmed gone by its own R2 HEAD")
    ap.add_argument("--prune-max", type=int, default=1000,
                    help="refuse a prune of more than this many keys (a listing fault looks like mass deletion)")
    a = ap.parse_args()
    if a.prune_absent and not a.absent_out:
        ap.error("--prune-absent needs --absent-out: a prune writes every key it deletes before deleting it")
    if a.prune_absent and (a.limit or not a.prefix):
        ap.error("--prune-absent needs a whole --prefix: with --limit or only --key nothing would be judged")
    nested = [(p, q) for p in a.prefix for q in a.prefix if p is not q and q.startswith(p)]
    if nested:
        ap.error(f"--prefix {nested[0][1]!r} lies inside --prefix {nested[0][0]!r}: its keys would be judged twice")
    if a.prune_absent:
        from core import cutover                                                 # noqa: PLC0415
        if cutover.is_cut_over():
            ap.error("--prune-absent is refused after T0: the store is then newer than R2")
    if a.absent_out:
        # the receipt must not be a file another output of this run rewrites: the progress writer's os.replace
        # replaced a prune receipt, DELETE lines and all, and the run passed (AR-182 round 5 finding 1)
        def same(x):
            return os.path.normcase(os.path.realpath(x))
        rec = same(a.absent_out)
        root = same(a.root)
        if a.progress and rec in (same(a.progress), same(a.progress + ".tmp")):
            ap.error("--absent-out and --progress name the same file: the progress writer would replace the receipt")
        if rec == root or rec.startswith(root.rstrip(os.sep) + os.sep):
            ap.error("--absent-out lies inside the blob store --root")
    if a.absent_out:
        # the receipt belongs to THIS run from its first moment: an earlier run's "# END" never survives a run
        # that dies, is skipped or never reaches the absent check (AR-182 round 4 finding 2)
        with open(a.absent_out, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f"# BEGIN run {_utc_s(dt.datetime.now(dt.timezone.utc))} prefixes {a.prefix} "
                     f"keys {len(a.key)} prune {bool(a.prune_absent)}\n")
    try:
        from core import r2_util  # noqa: PLC0415
        s3 = r2_util.cloud_client()     # a named final-sync reader: keeps reading the cloud after T0
        store = BlobStore(a.root, create=a.create)
        # a --prefix always takes the bulk path: it alone checks for objects R2 no longer has
        if not a.prefix and a.workers <= 1 and not (a.resume or a.progress or a.limit):
            rc = _serial(s3, store, a)
            if a.absent_out:
                with open(a.absent_out, "a", encoding="utf-8", newline="\n") as fh:
                    fh.write("# NOT RUN absent check NOT RUN: no --prefix\n")
            return rc
        return _bulk(r2_util, s3, store, a)
    except BaseException as e:
        if a.absent_out and not getattr(e, "receipt_marked", False):
            with open(a.absent_out, "a", encoding="utf-8", newline="\n") as fh:
                fh.write(f"# ABORTED {type(e).__name__}: {e} - this receipt is NOT a complete list\n")
        raise


def _serial(s3, store, a) -> int:
    keys = list(a.key)
    for p in a.prefix:
        tok = None
        while True:
            kw = {"Bucket": BUCKET, "Prefix": p}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents") or []]
            if not resp.get("IsTruncated"):
                break
            tok = resp["NextContinuationToken"]
    from core.cutover import CutoverRefused  # noqa: PLC0415
    bad = 0
    for k in keys:
        try:
            ok, msg = copy_one(s3, store, k, overwrite=a.overwrite, restore_missing=a.restore_missing)
        except CutoverRefused as e:
            # one refused key (under a licence removal, say) is a FAIL line, not the end of the batch with no
            # count (R1225); a refusal of the whole run (another checkout, another writer) repeats per key
            ok, msg = False, f"{k} REFUSED: {e}"
        bad += not ok
        print(("OK   " if ok else "FAIL ") + msg, flush=True)
    print(f"copied {len(keys) - bad} of {len(keys)}; failures {bad}")
    return 1 if bad else 0


def _utc_s(lm) -> str | None:
    """A boto3 LastModified as the store keeps it (ISO, UTC, whole seconds) - see copy_one."""
    return lm.astimezone(dt.timezone.utc).isoformat(timespec="seconds") if lm is not None else None


def _prefix_listing(s3, prefix):
    """(key, etag, size, last_modified) under one prefix, streamed page by page, in R2's (UTF-8 byte) key order."""
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix}
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents") or []:
            yield o["Key"], o["ETag"].strip('"'), o["Size"], _utc_s(o.get("LastModified"))
        if not resp.get("IsTruncated"):
            break
        tok = resp["NextContinuationToken"]


def _listed(s3, a):
    """(key, etag, size, last_modified) for every --key and every object under every --prefix, streamed."""
    for k in a.key:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        yield k, h["ETag"].strip('"'), h["ContentLength"], _utc_s(h.get("LastModified"))
    for p in a.prefix:
        yield from _prefix_listing(s3, p)


class ListingOutOfOrder(RuntimeError):
    """R2's listing did not strictly ascend by UTF-8 bytes: the merge against the store is then meaningless."""


def absent_on_r2(s3, store, prefix, seen=None, on_missing=None, tick=None):
    """CANDIDATE keys the STORE holds under `prefix` that R2 no longer lists (review AR-182 finding 1: a delete on
    R2 - a backup prune, a retire, a licence removal, a re-key - otherwise never reaches the store, and a resume
    reports success over it). A merge of two streams in the same byte order: neither side is held in memory.
    The listing is checked to strictly ascend as it is read (ListingOutOfOrder otherwise - AR-182 round 2 P2);
    a candidate is only a candidate: nothing is deleted on it before the whole merge finished and R2 confirmed
    the key gone with its own HEAD. The other direction is counted too (AR-182 round 3 finding 4): a key R2
    lists that the store does NOT hold goes to on_missing(key, its LastModified) and seen["listed_not_held"].
    `seen` (a dict) also
    counts listed and judged keys; tick() runs once per listed key (the caller's progress clock)."""
    seen = seen if seen is not None else {}
    for f in ("absent_listed", "absent_judged", "listed_not_held"):
        seen.setdefault(f, 0)

    modified = {}

    def theirs():
        prev = None
        for k, _e, _s, m in _prefix_listing(s3, prefix):
            b = k.encode("utf-8")
            if prev is not None and b <= prev:
                raise ListingOutOfOrder(f"R2 listed {k!r} after {prev.decode('utf-8')!r} under {prefix!r}")
            prev = b
            seen["absent_listed"] += 1
            if tick:
                tick()
            modified.clear()
            modified[k] = m                # only the current key's time is ever needed
            yield k

    def missing(k):
        seen["listed_not_held"] += 1
        if on_missing:
            on_missing(k, modified.get(k))
    it = theirs()
    nxt = next(it, None)
    for mine in store.iter_keys(prefix):
        seen["absent_judged"] += 1
        mb = mine.encode("utf-8")
        while nxt is not None and nxt.encode("utf-8") < mb:
            missing(nxt)
            nxt = next(it, None)
        if nxt == mine:
            nxt = next(it, None)
        else:
            yield mine
    while nxt is not None:                 # the listing is read to its end: R2 keys past the store's last are
        missing(nxt)                       # missing from it, and an order fault late in the listing still counts
        nxt = next(it, None)


def _gone_on_r2(s3, key) -> bool:
    """True only when R2 answers HEAD for `key` with 404. Present -> False; any other error propagates."""
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return False
    except Exception as e:                                                     # noqa: BLE001 - re-raised below
        code = str(getattr(e, "response", {}).get("Error", {}).get("Code", ""))
        if code in ("404", "NoSuchKey", "NotFound"):
            return True
        raise


def _bulk(r2_util, s3, store, a) -> int:
    """Parallel, resumable copy. One R2 client per thread; BlobStore.put is serialised by its own lock."""
    import json as _json                                                       # noqa: PLC0415
    import threading                                                           # noqa: PLC0415
    import time                                                                # noqa: PLC0415
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait   # noqa: PLC0415
    from core.cutover import CutoverRefused                                    # noqa: PLC0415
    local = threading.local()
    lock = threading.Lock()
    t0 = time.time()
    # R2 keys the absent check finds unheld are split on this: LastModified before it = a copy gap, at or after
    # it = new on R2 since the run began (AR-182 round 4 finding 1). Whole seconds, as _utc_s writes them.
    start = _utc_s(dt.datetime.fromtimestamp(int(t0) + CLOCK_MARGIN_S, dt.timezone.utc))
    c = {"listed": 0, "skipped_held": 0, "copied": 0, "failed": 0, "bytes": 0, "last_key": None,
         "run_start_utc": start}

    def client():
        if not hasattr(local, "s3"):
            local.s3 = r2_util.cloud_client()
        return local.s3

    def one(key, size):
        try:
            ok, msg = copy_one(client(), store, key, overwrite=a.overwrite, restore_missing=a.restore_missing)
        except CutoverRefused as e:
            ok, msg = False, f"{key} REFUSED: {e}"
        except Exception as e:                                                 # noqa: BLE001 - counted, never lost
            ok, msg = False, f"{key} ERROR {type(e).__name__}: {str(e)[:200]}"
        with lock:
            if ok:
                c["copied"] += 1
                c["bytes"] += size
            else:
                c["failed"] += 1
        if not ok or not a.quiet:
            print(("OK   " if ok else "FAIL ") + msg, flush=True)

    def report(final=False):
        if not a.progress:
            return
        el = max(time.time() - t0, 1e-9)
        with lock:
            snap = dict(c, seconds=round(el), objects_per_s=round(c["copied"] / el, 1),
                        mb_per_s=round(c["bytes"] / el / 1e6, 2), done=final,
                        utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        tmp = a.progress + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            _json.dump(snap, fh, indent=1)
        # on Windows os.replace fails while another process holds the target open (a watcher reading it): a
        # progress write must never end a multi-day copy (AR-182 round 3 finding 9) - retry, then skip this one
        for attempt in range(5):
            try:
                os.replace(tmp, a.progress)
                return
            except PermissionError:
                time.sleep(0.2 * (attempt + 1))
        print(f"WARN progress file {a.progress} is held open by another process; this update was skipped",
              flush=True)

    last = time.time()
    pending = set()
    with ThreadPoolExecutor(max(1, a.workers)) as ex:
        for key, etag, size, modified in _listed(s3, a):
            if a.limit and c["listed"] >= a.limit:
                c["limited"] = True
                break
            c["listed"] += 1
            c["last_key"] = key
            if a.resume:
                h = store.head(key)
                # the SAME object: bytes (etag, size) and R2's LastModified (whole seconds, as copy_one keeps it).
                # A re-PUT of identical bytes moves LastModified, and a resume that skips it leaves the store
                # holding an older stored time than R2 (review AR-182 finding 2)
                if (h is not None and h["etag"] == etag and h["size"] == size
                        and (modified is None or h["stored_utc"] == modified)):
                    c["skipped_held"] += 1
                    continue
            pending.add(ex.submit(one, key, size))
            if len(pending) >= a.workers * 4:                  # bounded: the listing never runs far ahead
                _done, pending = wait(pending, return_when=FIRST_COMPLETED)
            if time.time() - last >= 30:
                report()
                last = time.time()
        wait(pending)
    c["phase"] = "absent check"
    unresolved = _absent_pass(s3, store, a, c, report)
    c["phase"] = "done"
    report(final=True)
    print(f"listed {c['listed']:,}; held already {c['skipped_held']:,}; copied {c['copied']:,} "
          f"({c['bytes'] / 1e9:.2f} GB); failures {c['failed']:,}; {c['absent_note']}")
    return 1 if c["failed"] or unresolved else 0


def _absent_pass(s3, store, a, c, report=lambda: None) -> int:
    """After the copy: what the store holds under each --prefix that R2 no longer has. A fresh listing, so keys
    the copy itself wrote are never mistaken for deletions. Returns how many stay UNRESOLVED (rc 1 when > 0).

    Report only (default): every candidate is a FAIL of the run, its key in --absent-out (else the first 20
    printed). --prune-absent (before T0, --absent-out required) deletes them, and only when (AR-182 round 2):
      * the whole merge finished with R2's listing strictly ascending (ListingOutOfOrder aborts, no delete);
      * the count is plausible: never when R2 listed nothing under a prefix the store holds keys under, never
        more than --prune-max candidates;
      * R2 answers HEAD for that key with 404 right before its delete (a key R2 still has - a listing gap, or
        an object another importer copied meanwhile - is kept and stays unresolved);
      * its key is in the receipt (flushed) BEFORE the delete;
      * T0 has not begun: the flag is re-read before EVERY delete, and the rest are refused once it is set
        (AR-182 round 3 finding 1 - a merge of 14M keys takes hours).
    The other direction is a FAIL too: a key R2 lists that the store does not hold. Split by its LastModified
    against the run's start (round 4 finding 1): written BEFORE the run began = a copy gap (MISSING, counted
    unresolved, rc 1); at or after it = new on R2 since the run began (NEW, counted, not a failure - the next
    --resume copies it). Without a run start (a direct call) every one is a gap. NEW means DEFERRED to the next
    run, not verified: a key the copy lost and CI re-PUT during the run also reads NEW. So the last run before T0
    is followed by one whose NEW count is 0 (AR-182 round 5 note 2).
    The receipt is opened by main() at the start of the run ("# BEGIN run"); this pass appends its own lines
    and ends with END (it finished), NOT RUN (it was skipped, and why) or ABORTED (it did not finish). A receipt
    without END is never a complete list (rounds 3 and 4, finding 3 / 2). Progress is reported every 30 s
    while the listing is read and while the prune runs, candidates or not.
    Not run on a --limit trial (it saw part of the prefix only) nor after T0 (the store is then newer than R2,
    and absence on R2 says nothing)."""
    import time                                                                # noqa: PLC0415
    from core import cutover                                                   # noqa: PLC0415
    c["absent"] = c["pruned"] = c["kept_on_r2"] = c["r2_not_held"] = c["r2_new"] = 0
    start = c.get("run_start_utc")
    skip = None
    if not a.prefix:
        skip = "absent check NOT RUN: no --prefix"
    elif c.get("limited"):
        skip = "absent check NOT RUN: --limit saw part of the prefix only"
    elif cutover.is_cut_over():
        skip = "absent check NOT RUN: after T0 the store is newer than R2"
    if skip:
        c["absent_note"] = skip
        if a.absent_out:
            with open(a.absent_out, "a", encoding="utf-8", newline="\n") as fh:
                fh.write(f"# NOT RUN {skip}\n")
        return 0
    out = open(a.absent_out, "a", encoding="utf-8", newline="\n") if a.absent_out else None
    if out:
        out.write(f"# ABSENT CHECK BEGIN {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
                  f"prefixes {a.prefix} prune {bool(a.prune_absent)} run start {start}\n")
    keep = []                                       # candidates held for a prune: at most --prune-max + 1
    refused = None
    clock = {"last": time.time()}

    def tick():
        if time.time() - clock["last"] >= 30:
            report()
            clock["last"] = time.time()

    def on_missing(k, modified):
        new = start is not None and modified is not None and modified >= start
        c["r2_new" if new else "r2_not_held"] += 1
        tag = "NEW" if new else "MISSING"
        if out:
            out.write(f"{tag} {k} {modified}\n")
        elif c[("r2_new" if new else "r2_not_held")] <= 20:
            print(f"{tag} {k} {modified}", flush=True)
    try:
        for p in a.prefix:
            before = c.get("absent_listed", 0)
            held_before = c["absent"]
            for k in absent_on_r2(s3, store, p, seen=c, on_missing=on_missing, tick=tick):
                c["absent"] += 1
                if out:
                    out.write(f"ABSENT {k}\n")
                elif c["absent"] <= 20:
                    print(f"ABSENT {k}", flush=True)
                if a.prune_absent and len(keep) <= a.prune_max:
                    keep.append(k)
            if c["absent_listed"] == before and c["absent"] > held_before:
                refused = (f"R2 listed nothing under {p!r} while the store holds {c['absent'] - held_before:,} "
                           f"keys there")
        if a.prune_absent:
            if refused is None and c["absent"] > a.prune_max:
                refused = f"{c['absent']:,} candidates exceed --prune-max {a.prune_max:,}"
            if refused is None:
                for k in keep:
                    tick()
                    if cutover.is_cut_over():
                        refused = (f"T0 began during the prune: {len(keep) - c['pruned'] - c['kept_on_r2']:,} "
                                   f"deletes not done - after T0 the store is the served copy")
                        break
                    if not _gone_on_r2(s3, k):
                        c["kept_on_r2"] += 1
                        out.write(f"KEPT {k} (R2 HEAD found it)\n")
                        continue
                    out.write(f"DELETE {k}\n")
                    out.flush()
                    c["pruned"] += store.delete(k)
        if out:
            out.write(f"# END absent {c['absent']} missing {c['r2_not_held']} new {c['r2_new']} "
                      f"pruned {c['pruned']} kept {c['kept_on_r2']}"
                      + (f" REFUSED: {refused}" if refused else "") + "\n")
    except BaseException as e:
        if out:
            out.write(f"# ABORTED {type(e).__name__}: {e} - this receipt is NOT a complete list\n")
            e.receipt_marked = True                                           # main() does not write it twice
        raise
    finally:
        if out:
            out.close()
    note = (f"held but not on R2 {c['absent']:,}; on R2 but not held {c['r2_not_held']:,} "
            f"(+ {c['r2_new']:,} new on R2 since the run began)")
    if a.prune_absent:
        note += (f" (prune REFUSED: {refused})" if refused else
                 f" (pruned {c['pruned']:,}; kept, R2 has them {c['kept_on_r2']:,})")
    c["absent_note"] = note + (f" -> {a.absent_out}" if a.absent_out and
                               (c["absent"] or c["r2_not_held"] or c["r2_new"]) else "")
    return c["absent"] - c["pruned"] + c["r2_not_held"]


if __name__ == "__main__":
    raise SystemExit(main())
