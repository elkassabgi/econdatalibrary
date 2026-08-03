"""S1 bulk fetcher — ILOSTAT indicators (rplumber.ilo.org, no key).

ILO publishes 1,947 indicators, one parquet each, plus a table-of-contents CSV describing
every one of them.

THE VINTAGE IS PUBLISHER-SUPPLIED, WHICH IS THE BEST CASE. The TOC carries a `last.update`
timestamp per indicator (e.g. '27/05/2026 10:13:03') and it is populated for 1,947 of 1,947
rows. That beats every HTTP-validator scheme in this tree: it is ILO's own statement of when
each dataset changed, so there is nothing to infer and nothing that can flap. It sidesteps the
defect class that bit fed_board (Last-Modified regenerated per request) and bis (ETags flapping
across replicas) — there is no header to be wrong about.

THE TOC MUST BE RE-DOWNLOADED, NOT READ FROM CACHE. `ig.read_toc()` uses the file on disk when
one exists and only downloads if it is missing. On a schedule that is a staleness bomb of the
exact stats_nz shape (R159): the gate would compare every indicator against a frozen snapshot
of last.update and conclude, forever, that nothing had changed. So `ig.download_toc()` is
called first, explicitly, every run.

COST. One TOC download per run, then a fetch ONLY for indicators whose last.update moved.
Steady state is therefore one small CSV. The FIRST run is different and deliberately so: the
sidecar starts empty, so every indicator reads as changed and the wall-clock budget defers most
of them to later runs. That is honest — the alternative, seeding the sidecar from the current
TOC because 1,947 parquets happen to already be on disk, would ASSERT those files match ILO's
current last.update without ever checking, which is how a source ends up frozen and green.

PARSING AND WRITING ARE REUSED from jobs.ingest_ilostat (`process_one` -> build_table ->
parquet), so the fetcher and the first-pass ingest cannot drift. `process_one` writes locally
with pq.write_table and ig.OUT is the same directory as config.source_dir, so the bytes are
already at their store path and blob.publish_file streams them to R2 (only blob knows about
R2 — R36).

HONEST-STATUS: TOC unreachable -> TransientError (partial, retried, data kept). A per-indicator
failure -> transient_unit for that indicator only, so one bad download cannot stop the other
1,946. An indicator that returns ZERO rows while the TOC says it has records -> structural_unit
with its last.update NOT recorded, so it resurfaces next run; an indicator the TOC itself
declares empty (n.records = 0) is NOT an error and is counted as unchanged.
"""
from __future__ import annotations
import hashlib
import json
import os

from ... import config, blob
from ...errors import TransientError
from ..base import Result
from ._common import CURSOR_CAP, Deadline, Tally, finalize, merge_cursors
from jobs import ingest_ilostat as ig     # TOC + the production downloader/parser

SOURCE = "ilostat"
SIDECAR = "_toc_vintages.json"
BUDGET_MIN = 25


def _toc(fresh: bool):
    """TOC rows. fresh=True forces a re-download — see the module docstring."""
    if fresh:
        try:
            ig.download_toc()
        except Exception:                                    # noqa: BLE001
            pass                                             # fall back to whatever is cached
    try:
        return ig.read_toc() or []
    except Exception:                                        # noqa: BLE001
        return []


def _expected(ds) -> int:
    try:
        return int(float(ds.get("n.records") or 0))
    except (ValueError, TypeError):
        return 0


def current_vintage(unit):
    """Hash of every indicator's id + ILO's own last.update stamp.

    Moves when ILO republishes ANY indicator or adds one. One CSV download.
    """
    rows = _toc(fresh=True)
    if not rows:
        return None
    h = hashlib.sha256()
    for ds in sorted(rows, key=lambda r: r.get("id") or ""):
        h.update(f"{ds.get('id')}={ds.get('last.update')};".encode())
    return f"ilostat:{len(rows)}:{h.hexdigest()[:16]}"


def _load(out_dir) -> dict:
    raw = blob.read_bytes(os.path.join(out_dir, SIDECAR))
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def _save(out_dir, data) -> None:
    blob.write_bytes_atomic(os.path.join(out_dir, SIDECAR),
                            json.dumps(data, indent=2, sort_keys=True).encode("utf-8"))


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    rows = _toc(fresh=True)
    if not rows:
        raise TransientError("ilostat: TOC unreachable and no cached copy")

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    deferred = 0
    cursors: dict = {}          # §5.7 changed-series set, per re-pulled indicator
    dl = Deadline(minutes=BUDGET_MIN)

    for ds in sorted(rows, key=lambda r: r.get("id") or ""):
        iid = ds.get("id")
        if not iid:
            continue
        stamp = ds.get("last.update") or ""
        stored = os.path.join(out_dir, f"{iid}.parquet")
        if stamp and sidecar.get(iid) == stamp and blob.exists(stored):
            unchanged += 1
            continue

        if dl.spent():
            deferred += 1
            tally.deferred_unit(iid)                         # deferral, not a verdict (R303)
            continue

        try:
            _id, n_rows, _nser, _exp, status, _mn, _mx, _b, _s = ig.process_one(ds)
        except Exception:                                    # noqa: BLE001
            tally.transient_unit(iid)
            continue

        if not n_rows:
            if _expected(ds) > 0:
                # ILO says this indicator HAS records and we parsed none — a real break.
                # last.update is NOT recorded, so it resurfaces next run.
                tally.structural_unit(f"{iid}: {_expected(ds)} records expected, parsed 0")
            else:
                unchanged += 1        # the TOC itself declares it empty; not an error
                if stamp:
                    sidecar[iid] = stamp
            continue

        blob.publish_file(stored)
        # Bounded accumulation: ILOSTAT holds ~30.8M store series across 1,947 indicators, and
        # every cursor costs one SQLite lookup plus one state.db row. Its 80 catalog ids sit
        # under the derive-all cap, so a partial cursor set triggers exactly the same
        # re-derive as a complete one.
        merge_cursors(cursors, stored)
        published += n_rows
        tally.added_unit(n_rows, iid)
        if stamp:
            sidecar[iid] = stamp                             # record ONLY after publishing

    if unchanged:
        print(f"[ilostat] {unchanged}/{len(rows)} indicator(s) unchanged — skipped", flush=True)
    if len(cursors) >= CURSOR_CAP:
        print(f"[ilostat] cursor set hit the {CURSOR_CAP:,} cap — further changed series are "
              f"not individually reported (catalog grain is 80 ids, so the derive-all path "
              f"covers them)", flush=True)
    if deferred:
        print(f"[ilostat] budget {BUDGET_MIN} min spent — {deferred} indicator(s) deferred "
              f"to the next run", flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    return finalize(tally, published, since or None, source=SOURCE,
                    series_cursors=cursors or None)
