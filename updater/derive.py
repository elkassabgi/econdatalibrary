"""Per-series CSV re-derive + publish — step 5 of the update contract (plan §1.1).

WHY this exists: any series whose parquet changed in a run MUST get a fresh CSV
in the same run (UPDATER_BUILD_PLAN.md §5 rule 7 — CSV/parquet coherence). The
byte contract lives in core/derive_csv.py::_series_csv_bytes (byte-identical to
the Worker's /v1/series/{id}.csv response); we IMPORT it — never duplicate the
projection — and core/derive_csv.py itself stays untouched (a long-running
backfill process is using it). This module adds only the update-run concerns:
key encoding, patient PUT retry, and a failure report the orchestrator feeds
into the csv_retry_queue state table instead of crashing the data publish.

CI usage note (plan §1.1 step 5): the derive stack reads LOCAL-ONLY assets and
neither default path exists on a GitHub runner, so both overrides below are
MANDATORY in CI. They are read *inside* econdl at call time — nothing here
touches them; just set the process env before calling derive_and_put:
  ECONDL_DATA    root of the parquet store (econdl/_resolve.py::default_data_root).
                 In CI: the runner scratch dir laid out like the store, holding
                 the just-merged step-3 parquet objects (bytes already in hand —
                 no extra download). Local default: <repo>/data/clean_full.
  ECONDL_CATALOG path to catalog.db (econdl/_catalog.py::default_db). In CI: the
                 copy pulled from R2 '_aqueduct/catalog.db' (~1.8 GB, O-9).
                 Local default: <repo>/data/catalog.db.

Self-check (local store only, zero R2 contact):
  python -m updater.derive --check [--series-id "abs:ANA_AGG:M1.GPM.20.AUS.Q"]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import os
import threading
import time
import urllib.parse

# Importing core.derive_csv also puts clients/python on sys.path (it does so at
# module import), which is what makes `from econdl import ...` work below.
from core.derive_csv import _series_csv_bytes

PREFIX = "series"
PUT_TRIES = 7


def r2_key(series_id: str) -> str:
    """R2 object key for one series CSV — encoding identical to core/derive_csv.py."""
    return f"{PREFIX}/{urllib.parse.quote(series_id, safe='')}.csv"


def _put_with_retry(blob, key: str, body: bytes) -> bool:
    """PUT one object, patiently. True on success, False after PUT_TRIES failures.

    R2 can throw transient ServiceUnavailable/SlowDown throttles that outlast
    boto's built-in retries (killed the 2026-07-02 bulk run at 103k objects —
    see core/derive_csv.py). Same app-level backoff on top: 7 tries, exponential
    2**attempt seconds between them, then report failure — the caller records
    the series id (csv_retry_queue), never crashes the data publish.
    """
    for attempt in range(PUT_TRIES):
        try:
            blob.put_atomic(key, body)
            return True
        except Exception as e:
            if attempt == PUT_TRIES - 1:
                print(f"  CSV PUT FAILED after {PUT_TRIES} tries {key}: {str(e)[:100]}",
                      flush=True)
                return False
            wait = 2 ** attempt  # 1..64s
            print(f"  CSV PUT retry {attempt + 1}/{PUT_TRIES} in {wait}s ({str(e)[:70]})",
                  flush=True)
            time.sleep(wait)
    return False  # unreachable; keeps the contract explicit


def derive_and_put(series_ids: list[str], blob) -> dict:
    """Derive the contract CSV for each series id and PUT it via `blob`.

    blob: any updater/blob.py backend — only put_atomic(key, data: bytes) is used.
    Returns {'put': <count>, 'failed': [<series_id>, ...]} — 'failed' holds ids
    (derive errors AND exhausted PUTs) ready for the csv_retry_queue verbatim;
    reasons are printed loudly here, never swallowed (§5 rule 7). Per-series
    problems NEVER raise — a partial CSV publish must not undo a good parquet
    publish; the orchestrator marks the run 'partial' off the failed list.
    Duplicate ids are deduped (re-PUTs would be byte-identical no-ops anyway).
    """
    # CONCURRENCY. Each series is an independent derive plus one PUT, and the PUT is
    # almost entirely round-trip latency to R2 — so serial execution ran at about ONE
    # PER SECOND. Measured live on the yale_epi re-derive: 5,596 objects in ~90
    # minutes (~62/min), which put its remaining ~15,700 at ~253 further minutes
    # against a 300-minute job ceiling. That run would have been killed at the
    # ceiling with its state never pushed — hours spent for nothing, the exact
    # failure recorded in M-20260727-07. Threads suit this because the work is
    # I/O-bound, not CPU-bound.
    #
    # Each worker gets its OWN blob handle. boto3 clients are documented thread-safe
    # for most calls, but "most" is not a guarantee worth a corrupted upload, and a
    # per-thread handle costs nothing.
    workers = int(os.environ.get("AQUEDUCT_DERIVE_WORKERS", "8") or 8)
    ids = list(dict.fromkeys(series_ids))        # dedupe, order preserved
    if workers <= 1 or len(ids) < 2:
        workers = 1

    _local = threading.local()

    def _blob():
        if workers == 1:
            return blob
        b = getattr(_local, "b", None)
        if b is None:
            try:
                from . import blob as blob_mod
                b = blob_mod.from_env()
            except Exception:                    # noqa: BLE001 — fall back, never fail
                b = blob
            _local.b = b
        return b

    put = 0
    failed: list[str] = []
    lock = threading.Lock()

    def _one(sid):
        try:
            body = _series_csv_bytes(sid)
        except Exception as e:  # store-coverage gap or resolver error — loud, queued
            return sid, False, f"{type(e).__name__}: {str(e)[:90]}"
        return ((sid, True, None) if _put_with_retry(_blob(), r2_key(sid), body)
                else (sid, False, "PUT exhausted"))

    def _record(sid, ok, why):
        nonlocal put
        with lock:
            if ok:
                put += 1
                if put % 500 == 0:
                    print(f"  derived+put {put:,} CSVs (failed {len(failed):,})...",
                          flush=True)
            else:
                failed.append(sid)
                print(f"  CSV derive FAILED {sid}: {why}", flush=True)

    if workers == 1:
        for sid in ids:
            _record(*_one(sid))
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            for sid, ok, why in ex.map(_one, ids):
                _record(sid, ok, why)
    return {"put": put, "failed": failed}


def _check(series_id: str | None) -> int:
    """Derive ONE series from the local store — no blob, no R2, pure smoke test.

    Without --series-id, tries the first few catalog ids in catalog order and
    passes on the first that derives (a single store-coverage gap must not fail
    the smoke test, but every miss is printed). Exit 0 = derive stack works.
    """
    if series_id:
        candidates = [series_id]
    else:
        from econdl import _catalog  # on sys.path via the core.derive_csv import
        conn = _catalog.connect()
        try:
            candidates = [r["series_id"] for r in conn.execute(
                "SELECT series_id FROM series ORDER BY source_id, series_id LIMIT 5")]
        finally:
            conn.close()
        if not candidates:
            print("CHECK FAILED: catalog has no series rows")
            return 1
    for sid in candidates:
        try:
            body = _series_csv_bytes(sid)
        except Exception as e:
            print(f"  check candidate unresolvable {sid}: {str(e)[:100]}")
            continue
        first_line = body.split(b"\n", 1)[0].decode("utf-8", "replace")
        print(f"CHECK OK: {sid} -> {len(body):,} B (header: {first_line})")
        print(f"  would PUT (not putting): {r2_key(sid)}")
        return 0
    print(f"CHECK FAILED: none of {len(candidates)} candidate series derived")
    return 1


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Update-contract step 5: re-derive changed series CSVs "
                    "(library for the orchestrator; only --check runs standalone)")
    ap.add_argument("--check", action="store_true",
                    help="derive one series from the local store, zero R2 contact")
    ap.add_argument("--series-id",
                    help="series id for --check (default: first resolvable catalog row)")
    a = ap.parse_args()
    if not a.check:
        ap.error("derive_and_put is called by the orchestrator; standalone use is --check only")
    raise SystemExit(_check(a.series_id))


if __name__ == "__main__":
    main()
