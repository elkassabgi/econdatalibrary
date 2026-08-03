"""S1 fetcher — Statistics Denmark (DST/Danmarks Statistik) StatBank.

License CC BY 4.0; no API key. Layout: ONE parquet per subject-group under
clean_full/dst/<SUBJ>.parquet (~705 files, SUBJ = table-id with trailing digits
stripped, first 6 chars — e.g. AKU110A -> AKU110), schema
(series_key str, obs_date date32, value float64), dedup keys (series_key, obs_date).
series_key = "DST:<table>:DIM=code:DIM=code...". URL + parse logic are reused
VERBATIM from jobs/ingest_dst.py (get_json/post_json/query_table/parse_jsonstat).

Why S1 (overwrite_if_changed): DST /v1/data has NO startPeriod param, so every
table is re-pulled over its WHOLE history and merged (new wins on revision,
never-shrink). The cheap vintage probe is the catalog's per-table 'updated'
timestamps (GET /v1/tables) — a content hash over (id, updated) pairs that moves
iff any table was republished or added/removed.

REFRESH MODEL (registry vintage_signal: per-table 'updated' > last_run):
  A manifest clean_full/dst/_vintage_manifest.json stores {table_id: updated} from
  prior runs. update() re-pulls ONLY tables whose 'updated' moved OR whose subject
  parquet is missing on disk, under a per-run table budget (DST_MAX_TABLES_PER_RUN,
  default 40) so a single tick stays bounded; manifest progress persists so the
  remaining changed/new tables drain over subsequent runs. On a genuine cold start
  (no manifest) the already-complete on-disk subject files are adopted as the
  baseline (their tables seeded into the manifest), so we don't needlessly re-pull
  the whole 2,300-table StatBank — only genuinely new/changed tables are fetched.

HONEST STATUS (Tally + finalize):
  catalog GET timeout/5xx/429/network -> transient (TransientError surfaces it);
  catalog 200 that parses to 0 tables  -> structural (DefinitiveError);
  per-table data fetch timeout/5xx     -> tally.transient_unit() (-> partial, requeued);
  per-table 200 parsing 0 rows         -> tally.empty_unit() (many DST tables are
                                          legitimately empty, e.g. AKU100K);
  per-table net-new rows merged        -> tally.added_unit(n).
NEVER writes parquet directly; publishes only via merge.merge_and_write.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import re
import time

import pyarrow as pa
import pyarrow.compute as pc
import requests

from ... import config, blob, merge
from ...errors import TransientError, DefinitiveError
from ..base import Result
from ._common import Deadline, Tally, finalize
from ._vintage import content_hash

# Reuse the ingester's URLs + parse logic VERBATIM (do not re-implement).
from jobs import ingest_dst as ing

SOURCE = "dst"
BASE = ing.BASE                       # https://api.statbank.dk/v1
UA = {"User-Agent": "Econ-Fin Data Library admin@hfdatalibrary.com"}
DEDUP = ("series_key", "obs_date")
RATE = ing.RATE                       # 0.3s self-throttle, matches ingester
MANIFEST_NAME = "_vintage_manifest.json"
# Per-run table budget so one tick is bounded (steady-state monthly runs touch only
# the handful of tables whose 'updated' moved; the budget mainly bounds cold-catchup).
MAX_TABLES = int(os.environ.get("DST_MAX_TABLES_PER_RUN", "40"))
# Wall-clock budget for one dst run. The CHUNK size above bounds memory per merge; this
# bounds the run. Before they were the same knob, so draining more tables meant holding
# more rows before a single end-of-run manifest write — see the drain loop in update().
BUDGET_MIN = float(os.environ.get("DST_BUDGET_MIN", "20"))


def _subj(tid: str) -> str:
    """Subject-group key — IDENTICAL to jobs/ingest_dst.py (trailing digits stripped,
    first 6 chars), so we write the SAME per-subject file the ingester created."""
    return re.sub(r"\d+$", "", tid)[:6] or tid[:2]


def _subj_path(subj: str) -> str:
    return os.path.join(config.source_dir(SOURCE), f"{subj}.parquet")


def _fetch_catalog(tries: int = 4):
    """GET /v1/tables (the cheap catalog). Returns list of active table dicts.
    Raises TransientError on timeout/5xx/429/network; DefinitiveError on a 200 that
    parses to 0 tables (structural break) or a hard 4xx."""
    url = f"{BASE}/tables?lang=en"
    for a in range(tries):
        try:
            r = requests.get(url, headers=UA, timeout=90)
        except (requests.Timeout, requests.ConnectionError) as e:
            if a == tries - 1:
                raise TransientError(f"DST catalog: {e}")
            time.sleep(min(2 ** a, 30)); continue
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError as e:
                if a == tries - 1:
                    raise TransientError(f"DST catalog: bad json: {e}")
                time.sleep(min(2 ** a, 30)); continue
            tables = [t for t in (data or []) if t.get("active", True)]
            if not tables:
                raise DefinitiveError("DST catalog: 200 but 0 active tables (structural break)")
            return tables
        if r.status_code in (429, 500, 502, 503, 504):
            if a == tries - 1:
                raise TransientError(f"DST catalog HTTP {r.status_code}")
            time.sleep(min(2 ** a, 30)); continue
        raise DefinitiveError(f"DST catalog HTTP {r.status_code}")
    raise TransientError("DST catalog: no response after retries")


def _catalog_token(tables) -> str:
    """Stable hash over (id, updated) pairs — moves iff any table changed/added/removed."""
    pairs = sorted((t.get("id", ""), str(t.get("updated", ""))) for t in tables)
    return content_hash(("\n".join(f"{i}\t{u}" for i, u in pairs)).encode("utf-8"))


def current_vintage(unit):
    """Cheap probe: a content hash of the catalog's per-table 'updated' timestamps.
    Returns None (never raise) on a transient detection failure — the strategy then
    fetches anyway, which is safe (merge dedups + never-shrinks)."""
    try:
        tables = _fetch_catalog()
    except (TransientError, DefinitiveError):
        return None
    return _catalog_token(tables)


def _load_manifest() -> dict:
    path = os.path.join(config.source_dir(SOURCE), MANIFEST_NAME)
    if os.path.exists(path):
        try:
            d = json.load(open(path, encoding="utf-8"))
            d.setdefault("tables", {})
            return d
        except Exception:
            pass
    return {"tables": {}}


def _save_manifest(man: dict) -> None:
    os.makedirs(config.source_dir(SOURCE), exist_ok=True)
    path = os.path.join(config.source_dir(SOURCE), MANIFEST_NAME)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(man, f)
    os.replace(tmp, path)


# THE READS BELOW WERE BLOB-ROUTED; THE LISTINGS WERE NOT (R36).
#
# All three of these ask the store a question that decides what the run does: which subjects do
# we already hold, how many rows, how far does the data reach. Each listed the store with
# os.listdir behind an `os.path.isdir` guard — so under AQUEDUCT_BACKEND=r2, which is EVERY CI
# run, the local directory is absent, the guard returns the empty answer, and dst concludes it
# holds nothing at all: no subjects, zero rows, no frontier. Not a crash; an empty set that
# reads as a fact. blob.row_count and blob.read_table underneath were already routed, so the
# listing was the only half still addressing the wrong machine.
#
# `_`-prefixed sidecars stay excluded exactly as before.
def _store_names() -> list[str]:
    return [f for f in blob.list_parquets(config.source_dir(SOURCE))
            if not f.startswith("_")]


def _existing_subjects() -> set[str]:
    return {os.path.splitext(f)[0] for f in _store_names()}


def _total_rows() -> int:
    d = config.source_dir(SOURCE)
    return sum(blob.row_count(os.path.join(d, f)) for f in _store_names())


def _global_max_date() -> str | None:
    d = config.source_dir(SOURCE)
    best = None
    for f in _store_names():
        p = os.path.join(d, f)
        try:
            t = blob.read_table(p)
        except Exception:
            continue
        if t.num_rows == 0 or "obs_date" not in t.column_names:
            continue
        m = pc.max(t.column("obs_date")).as_py()
        if isinstance(m, dt.datetime):
            m = m.date()
        if m and (best is None or m > best):
            best = m
    return best.isoformat() if best else None


def _fetch_table_rows(table_id: str):
    """Re-pull one table's whole history via the ingester's query_table (which itself
    GETs /tableinfo then POSTs /data and parses JSON-stat). Returns
    (rows, transient): rows is list[(series_key, date, value)]; transient True iff a
    network/5xx fault prevented a clean fetch (so the table is NOT marked processed)."""
    try:
        meta = ing.get_json(f"{BASE}/tableinfo?id={table_id}&lang=en")
    except Exception:
        return [], True
    time.sleep(RATE)
    if not meta:
        # get_json returns None on 400/404 (definitively gone) OR after exhausting
        # retries on 5xx/timeout. Treat as transient: a real retired table just yields
        # 0 rows next time; we must not silently freeze on a wholesale outage.
        return [], True
    variables = meta.get("variables", [])
    if not variables:
        return [], False  # genuinely no variables -> legitimately empty table
    try:
        rows = ing.query_table(table_id, variables)
    except Exception:
        return [], True
    return rows, False


def update(unit, since) -> Result:
    out_dir = config.source_dir(SOURCE)
    os.makedirs(out_dir, exist_ok=True)
    before = _total_rows()

    # Catalog drives both change-detection and the work set. A transient/definitive
    # here propagates (honest: do not stamp success on a catalog outage).
    tables = _fetch_catalog()
    by_id = {t["id"]: t for t in tables if t.get("id")}

    man = _load_manifest()
    seen = man.get("tables", {})  # {table_id: updated} processed in prior runs
    on_disk = _existing_subjects()

    # COLD START: no manifest yet but data already on disk. Adopt the existing
    # subject files as the baseline so we don't re-pull the whole StatBank — seed the
    # manifest for every table whose subject parquet already exists, and only treat
    # genuinely new/missing or moved tables as work.
    cold = not seen
    if cold and on_disk:
        for tid, t in by_id.items():
            if _subj(tid) in on_disk:
                seen[tid] = str(t.get("updated", ""))

    # Work set: a table is "due" if its 'updated' moved vs the manifest, OR its
    # subject parquet is missing on disk (never ingested / new table).
    due = []
    for tid, t in by_id.items():
        upd = str(t.get("updated", ""))
        if seen.get(tid) != upd or _subj(tid) not in on_disk:
            due.append(tid)
    # Deterministic order so progress across budgeted runs is stable.
    due.sort()

    tally = Tally()
    cursors: dict[str, str] = {}

    if not due:
        # Everything current. Persist baseline + report the honest on-disk frontier.
        man["tables"] = seen
        man["catalog_token"] = _catalog_token(tables)
        _save_manifest(man)
        return finalize(tally, before, _global_max_date(), source=SOURCE,
                        series_cursors=cursors, empty_window_floor=10 ** 9)

    # DRAIN IN CHECKPOINTED CHUNKS, bounded by wall clock rather than a table count.
    #
    # This used to fetch exactly MAX_TABLES tables and write the manifest ONCE, at the
    # very end. Two consequences, both measured 2026-08-02: (1) with 623 tables due
    # (445 behind upstream + 178 never tracked) against 40 per run, dst could not catch
    # up with a publisher that moves ~10 tables a day; (2) a run stopped by the
    # orchestrator's per-source wall-clock cap persisted NOTHING, so the next run
    # re-fetched the same prefix — raising the count alone would have made that worse.
    #
    # Each chunk now fetches, merges AND checkpoints, so partial progress always
    # survives, and the deadline (not the count) is what ends the run. MAX_TABLES stays
    # as the CHUNK size: it bounds how many tables' rows are held in memory before a
    # merge, which is a real constraint and a different one from the time budget.
    #
    # Deadline's contract requires a budgeted fetcher to skip already-fresh sub-units so
    # a bound is a budget and not a truncation. dst satisfies that via the per-table
    # manifest: `due` is recomputed from it every run, so each run starts somewhere new.
    dl = Deadline(minutes=BUDGET_MIN)
    drained = 0
    capped = False

    for start in range(0, len(due), MAX_TABLES):
        if dl.spent():
            capped = True
            break
        batch = due[start:start + MAX_TABLES]

        # Accumulate per-subject so each subject parquet is merged once (read+append+dedup
        # then atomic publish), matching the ingester's per-subject file layout.
        by_subject: dict[str, list[tuple[str, dt.date, float]]] = {}
        processed_ok: dict[str, str] = {}  # table_id -> updated, only for clean fetches

        for tid in batch:
            rows, transient = _fetch_table_rows(tid)
            if transient:
                tally.transient_unit()  # -> partial; table NOT marked processed; requeued
                continue
            if not rows:
                tally.empty_unit()  # 200 parsed 0 rows (legitimately empty DST table)
                processed_ok[tid] = str(by_id[tid].get("updated", ""))
                continue
            by_subject.setdefault(_subj(tid), []).extend(rows)
            processed_ok[tid] = str(by_id[tid].get("updated", ""))

        # Merge each affected subject parquet via merge_and_write (atomic/dedup/never-shrink).
        for subj, rows in by_subject.items():
            path = _subj_path(subj)
            # In-memory dedup on (key,date) before merge (the ingester did this too).
            seen_kd: set = set()
            keys, dates, vals = [], [], []
            for k, d, v in rows:
                kd = (k, d)
                if kd in seen_kd:
                    continue
                seen_kd.add(kd)
                keys.append(k); dates.append(d); vals.append(v)
            tbl = pa.table({
                "series_key": pa.array(keys, pa.string()),
                "obs_date":   pa.array(dates, pa.date32()),
                "value":      pa.array(vals, pa.float64()),
            })
            if tbl.num_rows == 0:
                continue
            sbefore = blob.row_count(path)
            try:
                n, md = merge.merge_and_write(path, tbl, mode="merge", dedup_keys=DEDUP)
            except DefinitiveError:
                # A never-shrink / column-drop refusal for ONE subject must not abort the
                # whole run or fake success: count it transient so status is 'partial' and
                # the affected tables are retried (their manifest entries are NOT advanced).
                tally.transient_unit()
                for tid in batch:
                    if _subj(tid) == subj:
                        processed_ok.pop(tid, None)
                continue
            tally.added_unit(max(0, n - sbefore))
            if md:
                cursors[f"DST:{subj}"] = md

        # CHECKPOINT: advance the manifest only for cleanly-fetched tables. Written per
        # chunk so a cap kill cannot throw away everything this run fetched.
        seen.update(processed_ok)
        man["tables"] = seen
        man["catalog_token"] = _catalog_token(tables)
        _save_manifest(man)
        drained += len(batch)

    remaining = max(0, len(due) - drained)
    print(f"[{SOURCE}] {drained}/{len(due)} due table(s) drained in "
          f"{dl.elapsed_min():.1f} min"
          + (f"; BUDGET SPENT with {remaining} still due — they drain next run "
             f"(manifest checkpointed)" if capped else "; backlog clear"), flush=True)

    total = _total_rows()
    last = _global_max_date()
    # empty_window_floor huge: a budget batch that legitimately hits only empty DST
    # tables (e.g. AKU100K) is NOT a wholesale outage — real outages surface as
    # transient at the catalog/table HTTP layer, not as "200 parsed 0 rows".
    res = finalize(tally, total, last, source=SOURCE,
                   series_cursors=cursors, empty_window_floor=10 ** 9)

    # A run that stopped on its budget with tables still due has NOT finished the work.
    # Reporting `ok` there would set last_success_utc and read as healthy while our copy
    # is knowingly behind the publisher — the same false green that let this source sit
    # 472 tables behind Statistics Denmark while reporting "no new rows". `partial` does
    # not set last_success_utc, which is exactly the honest signal, and it matches the
    # convention Deadline documents for a budgeted fetcher.
    if capped and res.status in ("ok", "no_change"):
        res = Result(status="partial", obs=res.obs, last_obs_date=res.last_obs_date,
                     new_vintage=res.new_vintage, series_cursors=res.series_cursors,
                     error=f"budget spent with {remaining} of {len(due)} due table(s) "
                           f"still behind upstream; manifest checkpointed, drains next run")
    return res
