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
with its last.update NOT recorded, so it resurfaces on a later pass; an indicator the TOC itself
declares empty (n.records = 0) is NOT an error and is counted as unchanged.

ROTATION (AR-328). The walk resumes just after the bookmark (`_rotation.json`, saved per indicator
before its work), so a budget-capped pass extends coverage instead of re-walking the alphabetical
head. See _resume_order.

THE KILL PATH, stated so nobody assumes otherwise. The sidecar is still saved ONCE, at the end of
update(), on purpose (one write per pass, not per indicator). An exception that escapes update() - a
process kill, a unit timeout landing outside the per-indicator try, publish_file raising - loses that pass's sidecar stamps
AND its changed set: the indicators it published are on the store, but nothing books their CSVs in
csv_desktop_owed, and with rotation the bookmark has already moved past them, so they are re-fetched
(and their CSVs re-derived) only when the rotation comes round again - one cycle later.
"""
from __future__ import annotations
import bisect
import hashlib
import json
import os

from ... import config, blob
from ...errors import DefinitiveError, TransientError
from ..base import Result
from ._common import (CURSOR_CAP, Deadline, Tally, finalize, load_rotation, merge_cursors,
                      rotate_after, save_rotation)
from jobs import ingest_ilostat as ig     # TOC + the production downloader/parser

SOURCE = "ilostat"
SIDECAR = "_toc_vintages.json"
BUDGET_MIN = 25


def _rid(ds) -> str:
    return ds.get("id") or ""


def _resume_order(rows, after: str) -> list:
    """The TOC rows in id order, starting just after `after` (the last indicator the previous pass
    STARTED) and wrapping round.

    WHY (AR-328, R190/R313). The sidecar skips an indicator only while its last.update stamp is
    unchanged, and ILO re-stamps most of the list between weekly passes. A walk that always began
    at the alphabetical top therefore re-fetched the same head every pass: across all 11 passes
    from 2026-07-30 to 2026-10-14 the first deferred id sat at positions 675-1298 of ~1,960, and
    every indicator after that point was never fetched by any pass. The sidecar persisted fine; the
    head simply never stopped changing.

    A bookmark that is no longer in the TOC (ILO renames indicators, R1137) resumes at the next id
    in order. rotate_after's own fallback, "start at the top", is exactly the defect here.
    """
    order = sorted(rows, key=_rid)
    if after and not any(_rid(r) == after for r in order):
        i = bisect.bisect_right([_rid(r) for r in order], after)
        return order[i:] + order[:i]
    return rotate_after(order, after, key=_rid)


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


SPLIT_MAP = "_split_map.json"


def _fetch_split_map(out_dir) -> None:
    """Put the store's _split_map.json on THIS machine before the CSV phase (review R1131). The
    resolver reads it from the local store directory for every `ilostat:<stem>#<part>` id (1,470 of
    the 3,305 catalogued ids); on a runner only the files this pass wrote are local, so every part
    id raised ResolveError. Read through blob (R2 under the r2 backend) and written with a plain
    write - write_bytes_atomic would PUT it back. Absent everywhere: said loudly."""
    path = os.path.join(out_dir, SPLIT_MAP)
    raw = blob.read_bytes(path)
    if raw is None:
        print(f"[ilostat] {SPLIT_MAP} is absent from the store - every '#part' id will fail to "
              f"resolve in the CSV phase (upload the desktop tool's map, R1131)", flush=True)
        return
    try:
        with open(path, "wb") as fh:
            fh.write(raw)
    except OSError as e:
        print(f"[ilostat] could not write {path} locally ({e}) - '#part' ids will not resolve",
              flush=True)


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)

    rows = _toc(fresh=True)
    if not rows:
        raise TransientError("ilostat: TOC unreachable and no cached copy")
    _fetch_split_map(out_dir)

    sidecar = _load(out_dir)
    tally = Tally()
    published = 0
    unchanged = 0
    deferred = 0
    cursors: dict = {}          # per-series freshness for health (newest obs, discontinued series)
    # THE CSV PHASE'S CHANGED SET, at the catalogue's grain (review R1131): {indicator stem: max
    # date}. process_one REWRITES the whole <stem>.parquet, so every catalogued id of that stem -
    # 'ilostat:<stem>', each 'ilostat:<stem>#<part>', and the legacy 'ilostat:<flow>:<c1>:<geo>'
    # ids a '<flow>_A' stem holds - may have changed. The series cursors above could never map:
    # they are store keys, capped at CURSOR_CAP (50,000), and every pass read "csv coherence
    # unmet: 50000 changed series_keys have no catalog mapping". Complete: <= ~1,961 stems.
    changed: dict = {}
    dl = Deadline(minutes=BUDGET_MIN)

    for ds in _resume_order(rows, load_rotation(out_dir)):
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

        # BOOKMARK, once per indicator, AFTER the deferral branch (a deferred id is never the bookmark)
        # and BEFORE the work (R273, the ecb / insee_melodi / abs placement). Saved after the work, an
        # indicator that never completes - the process dies in it, or publish_file raises out of
        # update() - would leave the bookmark just before it, and every later pass would start on it
        # and die there (review AR-329). Saved before, the next pass starts after it; the indicator
        # keeps no sidecar stamp and is worked again when the rotation comes round.
        save_rotation(out_dir, iid)
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
        changed[iid] = str(_mx) if _mx else None
        # Bounded accumulation: ILOSTAT holds ~30.8M store series across 1,947 indicators, and
        # every cursor costs one SQLite lookup plus one state.db row. They feed health's
        # freshness reading only; the CSV phase reads `changed` (above).
        merge_cursors(cursors, stored)
        published += n_rows
        tally.added_unit(n_rows, iid)
        if stamp:
            sidecar[iid] = stamp                             # record ONLY after publishing

    if unchanged:
        print(f"[ilostat] {unchanged}/{len(rows)} indicator(s) unchanged — skipped", flush=True)
    if len(cursors) >= CURSOR_CAP:
        print(f"[ilostat] cursor set hit the {CURSOR_CAP:,} cap — freshness cursors only; the "
              f"CSV phase reads the {len(changed):,} changed indicator(s)", flush=True)
    if deferred:
        print(f"[ilostat] budget {BUDGET_MIN} min spent — {deferred} indicator(s) deferred "
              f"to a later pass", flush=True)
    _save(out_dir, sidecar)
    if published == 0:
        published = sum(blob.row_count(os.path.join(out_dir, f))
                        for f in blob.list_parquets(out_dir))
    try:
        res = finalize(tally, published, since or None, source=SOURCE,
                       series_cursors=cursors or None)
    except DefinitiveError as e:
        if not changed:
            raise
        # THE CHANGED SET MUST SURVIVE A STRUCTURAL RAISE (review R1131, the _giant precedent): the
        # indicators published before one broke are already served and their sidecar stamps have
        # advanced, so the orchestrator's "partial, no derive" would leave their CSVs stale for good.
        print(f"[ilostat] finalize raised structural with {len(changed):,} indicator(s) published - "
              f"returning partial WITH the changed set: {str(e)[:120]}", flush=True)
        res = Result(status="partial", obs=published, last_obs_date=since or None,
                     new_vintage="date-tail", series_cursors=cursors or None, error=str(e))
    res.changed_keys = changed
    return res
