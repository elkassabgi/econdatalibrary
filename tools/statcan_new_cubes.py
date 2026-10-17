"""Ingest the cubes the statcan lane lists as NEW, so they are stored, served and catalogued like every
cube the library already holds.

WHY (2026-10-17, ledger R1602). jobs/statcan_lane.py v1 refreshes only cubes it already holds. A cube
StatCan releases under a NEW product id is booked under `new_cubes` in _lane_state.json and never
ingested ("WHAT IT DOES NOT DO (v1)"). On 2026-10-17 that list held 58 cubes, the oldest released
2026-08-14, while the lane reported idle. Most are new EDITIONS (StatCan gives each quarter of a
recurring survey its own product id), a few are new surveys or new tables, and 13100975 no longer
exists at StatCan. The owner hosts econ on the workstation and keeps the D1 catalogue sync frozen, so
the catalogue step is the LOCAL catalogue, never D1.

WHERE A NEW CUBE LANDS (the places every held cube already lives, before T0):
  parquet      the statcan store through updater.blob - R2 `clean_full/statcan/<pid>.parquet` plus the
               local mirror (the backend is pinned to r2 exactly as the lane pins it)
  .done        the ingester's own sidecar record (jobs/ingest_statcan.done_record), local - the
               cataloguer takes the cube's title from it
  split map    data/clean_full/statcan/_split_map.json, for a cube over the cap (atomic, merged)
  CSVs         the CSV store (updater.blob.csv_store - R2 before T0), through the lane's own serving
               scan and put path (jobs/statcan_lane.serve_cube / _store_put): byte-identical
  catalogue    the LOCAL catalogue's `series` and `series_fts`, rows built by
               tools/catalog_statcan_tables.cube_rows (the cataloguer's own row builder)
  lane record  _lane_state.json / _catalogue_debt.json: the cube leaves `new_cubes` and becomes a held
               cube, so its next release is merged and served by the lane itself
The self-hosted origin serves from F:\\econ_live\\blobs and a snapshot of the local catalogue. The CSVs
reach that store by tools/selfhost/import_from_r2.py (per cube: --prefix series/statcan%3A<pid>
--resume), and the origin shows them after a new catalogue snapshot and a generation swap. Those are
separate reviewed steps; this tool does neither. Writing ONLY to the self-hosted store before T0 is
wrong: the importer's absent check fails a resume pass on an object R2 does not hold.

ORDER PER CUBE (objects before catalogue rows, R341), each stage recorded in
logs/statcan_new_cubes/<pid>.json, so a re-run resumes a cube at its next stage:
  1. prepare    - fetch (statcan._fetch_cube_whole: the full table, the ingester's parser, StatCan's
                  counts), split (derive_statcan_tables.choose_split at the lane's cap), and EVERY check
                  that can refuse the cube - nothing of it held, the licence and source rows, the ids it
                  would serve equal the rows it would catalogue, none of them in series_fts - all BEFORE
                  anything is written
  2. commit     - receipt `committing`, then the parquet (blob.commit_local_file), then .done -> committed
  3. serve      - split-map entry, then every id of the cube put to the CSV store              -> served
  4. catalogue  - one transaction: series + series_fts rows, verified before commit           -> catalogued
  5. record     - the lane's state and debt                                                    -> done

GUARDS. Dry by default (no store, map, catalogue or state write; --fetch also runs `prepare` in the lane's
scratch dir and removes the file). Refused after T0, like the lane (core.cutover; the post-T0 statcan
writer is the owner's T0 decision). --apply runs only when every path it writes is the live checkout's
(the store dir, split map, catalogue, writer-lock dir, receipts). --apply holds logs/statcan_writer.lock
for the run (never waits: held by the lane -> refused, exit 1) and stops starting cubes after
--max-minutes (default 90, counted from the lock, the series_fts read included): the waiting lane cannot
beat while the lock is held, and RELAUNCH_GUARD.ps1 kills a lane whose log is silent for 3 h; a started
cube runs to its end, so run the three giants (34100299, 34100298, 15100050) as their own batch first.
The one series_fts read holds a READ lock on the local catalogue for its ~18 minutes: no other catalogue
writer may run then (none has written it since 2026-10-01; the run step must check). No D1 module is imported, and no object outside
clean_full/statcan/ and series/statcan%3A<pid> is written.

    python tools/statcan_new_cubes.py                      # DRY: what would be ingested
    python tools/statcan_new_cubes.py --fetch --pids 33101174
    python tools/statcan_new_cubes.py --apply [--pids a,b] [--max-minutes 90]
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import shutil
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover                            # noqa: E402
from updater import blob, merge, writer_lock                       # noqa: E402
from updater.errors import TransientError                          # noqa: E402
from updater.strategies.fetchers import statcan as sc              # noqa: E402

import jobs.ingest_statcan as ing                                   # noqa: E402
import jobs.statcan_lane as lane                                    # noqa: E402

LOCK_NAME = lane.LOCK_NAME
WHAT = "tools/statcan_new_cubes.py (an --apply run)"
# THE LIVE CHECKOUT, whose files an --apply run writes (module docstring). Tests point it at a temporary folder.
LIVE_ROOT = catalog_path.LIVE_STORE_ROOT
CATALOGUE = catalog_path.under(ROOT)
RECEIPTS = os.path.join(ROOT, "logs", "statcan_new_cubes")
STAGES = ("committing", "committed", "served", "catalogued", "done")
# Under RELAUNCH_GUARD.ps1's 3-hour stall limit for the lane, which cannot beat while this run holds the lock.
MAX_MINUTES_DEFAULT = 90.0
# what a receipt keeps of the parse: what jobs/ingest_statcan.done_record and the lane record need
PARSE_KEYS = ("n_obs", "n_null", "n_series", "series_capped", "start", "end", "layout", "skipped",
              "needs_stored_floor")

_CAT = os.path.join(ROOT, "tools", "catalog_statcan_tables.py")


def _cat():
    """tools/catalog_statcan_tables.py, for cube_rows, LICENSE_ID and SOURCE. Loaded by path (tools/ is
    not a package); importing it runs no cataloguing."""
    m = sys.modules.get("_catalog_statcan_tables_for_new_cubes")
    if m is None:
        tools_dir = os.path.join(ROOT, "tools")
        if tools_dir not in sys.path:
            sys.path.insert(0, tools_dir)
        spec = importlib.util.spec_from_file_location("_catalog_statcan_tables_for_new_cubes", _CAT)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        sys.modules["_catalog_statcan_tables_for_new_cubes"] = m
    return m


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Refused(Exception):
    """A cube this run must not touch (the reason is the message)."""


class AlreadyRefused(Refused):
    """Refused before for the same release; its receipt already holds the reason, and is left as it is."""


# --------------------------------------------------------------------------- #
# receipts
# --------------------------------------------------------------------------- #
def _receipt_path(pid: str) -> str:
    return os.path.join(RECEIPTS, f"{pid}.json")


def load_receipt(pid: str) -> dict:
    p = _receipt_path(pid)
    if not os.path.exists(p):
        return {"pid": pid, "stage": None}
    with open(p, encoding="utf-8") as fh:
        rec = json.load(fh)                    # a corrupt receipt raises: never silently start over
    if rec.get("stage") not in (None, "refused") + STAGES:
        raise ValueError(f"{p}: unknown stage {rec.get('stage')!r}")
    return rec


def save_receipt(rec: dict) -> None:
    os.makedirs(RECEIPTS, exist_ok=True)
    rec["updated_utc"] = _now()
    p = _receipt_path(rec["pid"])
    tmp = f"{p}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(rec, fh, indent=1, sort_keys=True)
    os.replace(tmp, p)


# --------------------------------------------------------------------------- #
# what is already held, and whether the catalogue can take the cube
# --------------------------------------------------------------------------- #
def _paths(pid: str) -> tuple[str, str]:
    return (os.path.join(sc.OUT_DIR, f"{pid}.parquet"), os.path.join(sc.OUT_DIR, f"{pid}.done"))


def read_split_map() -> dict:
    """The split map, FAIL CLOSED: an unreadable or empty map would let a write drop every split."""
    try:
        with open(lane.SPLIT_MAP, encoding="utf-8") as fh:
            smap = json.load(fh)
    except (OSError, ValueError) as e:
        raise SystemExit(f"REFUSING: the split map {lane.SPLIT_MAP} is unreadable ({e!r})") from e
    if not isinstance(smap, dict) or not smap:
        raise SystemExit(f"REFUSING: the split map {lane.SPLIT_MAP} holds no split decisions")
    return smap


def catalogued_ids(pid: str) -> set:
    """Every catalogued id of the cube, by a primary-key range read (lane.catalogued_ids' rule)."""
    return lane.catalogued_ids(pid, CATALOGUE)


_FTS: dict | None = None      # {pid: series_fts ids of that cube}, for the run's cubes (load_fts)


def load_fts(pids) -> None:
    """Read series_fts ONCE for every cube of the run. series_id is UNINDEXED in series_fts, so ANY
    series_id predicate is a full scan of it: measured 2026-10-17 on the local 11.9 GB catalogue, one
    `WHERE series_id IN (2 ids)` took 1,067.8 s (cold, with other disk load) - one scan per cube, or
    one per 500 ids, would be hours. One scan with all the run's product ids instead; the run then keeps
    the set current with its own inserts (and holds the statcan writer lock)."""
    global _FTS
    want = sorted({str(p) for p in pids})
    out = {p: set() for p in want}
    if want:
        marks = ",".join("?" * len(want))
        con = catalog_path.connect_path(CATALOGUE, write=False, timeout=180)
        try:
            for (sid,) in con.execute(
                    f"SELECT series_id FROM series_fts WHERE series_id >= ? AND series_id < ? "
                    f"AND substr(series_id, 9, 8) IN ({marks})", ("statcan:", "statcan;", *want)):
                out[sid[8:16]].add(sid)
        finally:
            con.close()
    _FTS = out


def fts_has(ids) -> set:
    """Which of `ids` series_fts holds, from the run's one read (load_fts). Fails closed on a cube that
    read did not cover."""
    if _FTS is None:
        raise RuntimeError("series_fts was not read for this run (load_fts)")
    out = set()
    for i in ids:
        pid = i[8:16]
        if pid not in _FTS:
            raise RuntimeError(f"series_fts was not read for cube {pid}")
        if i in _FTS[pid]:
            out.add(i)
    return out


def held_anywhere(pid: str, smap: dict) -> list:
    """Why cube `pid` is already (partly) held: [] means nothing of it exists in any place this tool
    writes. Asked of every place, so a half-ingested cube from another writer is never overwritten."""
    parquet, done = _paths(pid)
    why = []
    if blob.exists(parquet):
        why.append("its parquet is in the statcan store")
    if os.path.exists(parquet):
        why.append("its parquet is in the local mirror")
    if os.path.exists(done):
        why.append("its .done record exists")
    if str(pid) in smap:
        why.append("the split map has an entry for it")
    if catalogued_ids(pid):
        why.append("the catalogue holds ids in its range")
    return why


def catalogue_refusals(ids, check_fts: bool = True) -> list:
    """Why the catalogue cannot take these ids ([] = it can): the licence and source rows a row offers to
    serve under, and (check_fts) no id already in series_fts. Read-only."""
    cat = _cat()
    con = catalog_path.connect_path(CATALOGUE, write=False, timeout=180)
    try:
        out = []
        lic = con.execute("SELECT reservable FROM license WHERE license_id=?", (cat.LICENSE_ID,)).fetchone()
        if not lic or not lic[0]:
            out.append(f"licence {cat.LICENSE_ID!r} missing or not reservable - a row is an offer to serve")
        if not con.execute("SELECT 1 FROM source WHERE source_id=?", (cat.SOURCE,)).fetchone():
            out.append(f"no source row {cat.SOURCE!r} in the catalogue")
        stray = fts_has(ids) if check_fts else set()
        if stray:
            out.append(f"series_fts already holds {len(stray)} of its ids (first {sorted(stray)[:3]})")
        return out
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# prepare: everything that can refuse a cube, before anything is written
# --------------------------------------------------------------------------- #
def lite_rows() -> dict:
    """{pid: getAllCubesListLite entry}. Fails closed on a short list, as the fetcher's enumeration does."""
    j = sc._get("getAllCubesListLite", timeout=300)
    cubes = j if isinstance(j, list) else (j.get("object") if isinstance(j, dict) else None)
    if not isinstance(cubes, list) or len(cubes) < sc._LITE_FLOOR:
        raise TransientError(f"statcan getAllCubesListLite returned "
                             f"{len(cubes) if isinstance(cubes, list) else 'no list'} cube(s), below the "
                             f"{sc._LITE_FLOOR} floor - refusing to read that as 'withdrawn'")
    return {str(o["productId"]): o for o in cubes if isinstance(o, dict) and o.get("productId") is not None}


def _duck(spill):
    import duckdb                                                    # noqa: PLC0415
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET temp_directory='%s'" % spill.replace("\\", "/").replace("'", "''"))
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


def choose(path: str, n_rows: int) -> tuple:
    """(dim, parts) from the derive tool's own choose_split at the lane's cap (the store's cap)."""
    spill = merge._bounded_spill_dir()
    con = _duck(spill)
    try:
        return lane._tool().choose_split(con, path.replace("\\", "/"), n_rows, lane.MAX_ROWS)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)


def emitted_ids(pid: str, path: str, dim) -> set:
    """The ids lane.serve_cube WILL write for this file and split: its own predicate (a non-null value and
    date), its own part expression, and no id for a NULL split value."""
    t = lane._tool()
    f = path.replace("\\", "/").replace("'", "''")
    expr = t.part_expr(dim) if dim else "''"
    spill = merge._bounded_spill_dir()
    con = _duck(spill)
    try:
        parts = [p for (p,) in con.execute(f"SELECT DISTINCT {expr} FROM read_parquet('{f}') "
                                           f"WHERE value IS NOT NULL AND obs_date IS NOT NULL").fetchall()]
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)
    if dim:
        return {t.unit_id(str(pid), p) for p in parts if p not in (None, "")}
    return {t.unit_id(str(pid))} if parts else set()


def cube_rows(pid: str, path: str, sidecar: dict, split) -> list:
    spill = merge._bounded_spill_dir()
    try:
        rows, _untitled = _cat().cube_rows(pid, path.replace("\\", "/"), sidecar, split, spill)
    finally:
        shutil.rmtree(spill, ignore_errors=True)
    return rows


def prepare(pid: str, lite: dict) -> dict:
    """Fetch, split and check one cube. Returns {scratch, st, split, ids}; the caller removes the scratch
    file. Refused (with the scratch removed) when anything would stop the cube part-way."""
    import pyarrow.parquet as pq                                     # noqa: PLC0415
    why = held_anywhere(pid, read_split_map())
    if why:
        raise Refused("already held: " + "; ".join(why))
    scratch, st = sc._fetch_cube_whole(int(pid))
    try:
        st["rows"] = pq.read_metadata(scratch).num_rows
        dim, parts = choose(scratch, st["rows"])
        if dim == "":
            raise Refused(f"{st['rows']:,} rows and no column or level divides it below the "
                          f"{lane.MAX_ROWS:,}-row cap (derive_statcan_tables.choose_split) - nothing written")
        split = {"dim": dim, "parts": parts, "rows": st["rows"]} if dim else None
        rows = cube_rows(pid, scratch, ing.done_record(lite, int(pid), st, scratch), split)
        ids = {r[0] for r in rows}
        served = emitted_ids(pid, scratch, dim)
        if not ids or ids != served:
            raise Refused(f"it would catalogue {len(ids)} id(s) and serve {len(served)} (e.g. every value "
                          f"null) - the two must be the same non-empty set; nothing written")
        bad = catalogue_refusals(ids)
        if bad:
            raise Refused("; ".join(bad))
    except BaseException:
        sc._remove_quietly(scratch)
        raise
    return {"scratch": scratch, "st": st, "split": split, "ids": ids}


# --------------------------------------------------------------------------- #
# the stages that write
# --------------------------------------------------------------------------- #
def stage_commit(pid: str, lite: dict, rec: dict) -> None:
    """Publish the prepared parquet and its .done record."""
    p = prepare(pid, lite)
    scratch, st = p["scratch"], p["st"]
    parquet, _done = _paths(pid)
    try:
        if blob.exists(parquet) or os.path.exists(parquet):          # re-asked just before the publish
            raise Refused("its parquet appeared while this run fetched it")
        # RECORDED BEFORE THE PUBLISH: a kill between the publish and the .done record must leave a
        # receipt that says a parquet may now exist, or the re-run would find it, call the cube
        # "already held" and refuse it for ever.
        rec.update(stage="committing", rows=st["rows"], zip_bytes=st.get("zip_bytes"),
                   parse={k: st.get(k) for k in PARSE_KEYS}, split=p["split"], ids=len(p["ids"]))
        save_receipt(rec)
        blob.commit_local_file(scratch, parquet)
    finally:
        sc._remove_quietly(scratch)
    finish_commit(pid, lite, rec)


def finish_commit(pid: str, lite: dict, rec: dict) -> None:
    """Make sure the parquet THIS run published is in the store of record and the mirror, then write
    its .done record. Called right after the publish, and by a re-run of a cube left `committing`."""
    import pyarrow.parquet as pq                                     # noqa: PLC0415
    parquet, done = _paths(pid)
    in_store, in_mirror = blob.exists(parquet), os.path.exists(parquet)
    if in_mirror and pq.read_metadata(parquet).num_rows != rec["rows"]:
        now_rows = pq.read_metadata(parquet).num_rows
        c = lane.load_state()["cubes"].get(str(pid)) or {}
        if c.get("not_held") or not c.get("merged"):
            raise RuntimeError(f"the mirror's {pid}.parquet holds {now_rows:,} rows, not the {rec['rows']:,} "
                               f"this run published, and the lane has not merged it - another writer; a human's")
        # THE LANE MERGED IT between two runs (a new release, the take-over case of stage_record): the store
        # now holds the lane's keep-old merge of this run's table. Go on with what is stored; the .done
        # record keeps this run's parse, as every held cube's .done keeps its first ingest.
        rec.update(rows=now_rows, lane_merged_before_done=c.get("merged"))
    if in_mirror and not in_store:
        # the local replace happened and the store PUT did not (commit_local_file's two steps): publish
        # the same file again by the same path, never a second fetch
        again = os.path.join(sc.TMP_DIR, f"{pid}.{os.getpid()}.republish.parquet")
        os.makedirs(sc.TMP_DIR, exist_ok=True)
        os.replace(parquet, again)
        blob.commit_local_file(again, parquet)
        in_store = blob.exists(parquet)
    if not (in_store and in_mirror):
        raise RuntimeError(f"after the publish of {pid}: in the store {in_store}, in the mirror {in_mirror}")
    record = ing.done_record(lite, int(pid), rec["parse"], parquet)
    tmp = f"{done}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(record, fh)
    os.replace(tmp, done)
    p = rec["parse"]
    rec.update(stage="committed", n_series=p["n_series"], parquet_bytes=os.path.getsize(parquet),
               start=p["start"], end=p["end"], layout=p.get("layout"),
               needs_stored_floor=bool(p.get("needs_stored_floor")))


def _rows_for(pid: str, rec: dict) -> list:
    parquet, done = _paths(pid)
    with open(done, encoding="utf-8") as fh:
        sidecar = json.load(fh)
    return cube_rows(pid, parquet, sidecar, rec.get("split"))


def stage_serve(pid: str, rec: dict, put) -> None:
    """Record the split, then put every id of the cube to the CSV store with the lane's own scan."""
    parquet, _done = _paths(pid)
    smap = read_split_map()
    split = rec.get("split")
    if split:
        have = smap.get(str(pid))
        if have is not None and have != split:
            raise Refused(f"the split map already records {have} for it, not {split}")
        if have is None:
            lane._tool().write_map_atomic(lane.SPLIT_MAP, {**smap, str(pid): split})
            smap = read_split_map()
            if smap.get(str(pid)) != split:
                raise RuntimeError(f"the split map does not read back {split} for {pid}")
    elif str(pid) in smap:
        raise Refused(f"the split map holds {smap[str(pid)]} for a cube this run serves whole")
    ids = {r[0] for r in _rows_for(pid, rec)}
    if not ids:
        raise Refused("the cube yields no catalogue row (no non-null observation)")
    r = lane.serve_cube(pid, parquet, smap, ids, put)
    if r["status"] != "ok" or r["put_errors"] or r["new"] or r["vanished"] or r["refusal"]:
        raise RuntimeError(f"serving {pid} did not write exactly its ids: status {r['status']}, "
                           f"{r['put']} put, {r['put_errors']} put error(s), new {r['new'][:5]}, "
                           f"vanished {r['vanished'][:5]}, refusal {r['refusal']}")
    t = lane._tool()
    rec.update(stage="served", ids=len(ids), parts_put=r["put"], null_part_rows=r["null_part_rows"],
               csv_keys=sorted(t.csv_key(lane.PREFIX, s) for s in ids))


def stage_catalogue(pid: str, rec: dict) -> None:
    """One transaction on the local catalogue: the cube's rows into series and series_fts, verified."""
    rows = _rows_for(pid, rec)
    ids = [r[0] for r in rows]
    if len(ids) != rec.get("ids"):
        raise RuntimeError(f"{len(ids)} catalogue row(s) now, {rec.get('ids')} served - the store moved")
    lo = lane._tool().unit_id(str(pid))
    with catalog_path.write_session():
        con = catalog_path.connect_path(CATALOGUE, write=True, timeout=180)
        try:
            con.execute("PRAGMA busy_timeout = 180000")
            have = {r[0] for r in con.execute("SELECT series_id FROM series WHERE series_id >= ? AND "
                                              "series_id < ?", (lo, lo + "$"))}
            have_fts = fts_has(ids)
            if have == set(ids) and have_fts == set(ids):
                # ALREADY CATALOGUED by an earlier run killed before its receipt (the commit came first)
                rec.update(stage="catalogued", catalogue_rows=len(ids), catalogued_by="an earlier run")
                return
            if have or have_fts:
                raise Refused(f"the catalogue already holds {len(have)} row(s) and series_fts {len(have_fts)} "
                              f"id(s) of this cube, not exactly its {len(ids)} - written by something else")
            refusals = catalogue_refusals(ids, check_fts=False)     # series_fts was read just above
            if refusals:
                raise Refused("; ".join(refusals))
            con.execute("BEGIN IMMEDIATE")
            try:
                # the index is checked by ROWID, not by series_id: a series_id predicate is a full scan of
                # series_fts per statement, and this runs inside the write transaction
                top = con.execute("SELECT rowid FROM series_fts ORDER BY rowid DESC LIMIT 1").fetchone()
                top = top[0] if top else 0
                con.executemany(
                    """INSERT INTO series
                       (series_id,source_id,title,frequency,unit,geography,category,license_id,
                        start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", rows)
                con.executemany("INSERT INTO series_fts(series_id, title, geography) VALUES(?,?,?)",
                                [(r[0], r[2], r[5]) for r in rows])
                n = con.execute("SELECT count(*) FROM series WHERE series_id >= ? AND series_id < ?",
                                (lo, lo + "$")).fetchone()[0]
                f = sorted(r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE rowid > ?", (top,)))
                if n != len(ids) or f != sorted(ids):
                    raise RuntimeError(f"after the insert: {n} series row(s) in the cube's range and {len(f)} "
                                       f"new fts row(s), want exactly its {len(ids)} ids in each")
            except BaseException:
                con.rollback()
                raise
            con.commit()
            _FTS[str(pid)].update(ids)                     # the run's read stays current
        finally:
            con.close()
    rec.update(stage="catalogued", catalogue_rows=len(ids))


def stage_record(pid: str, rec: dict) -> None:
    """The lane's record: the cube is held now, so it leaves new_cubes and its next release is the lane's
    to merge and serve. The release recorded is the lane's own (a newer StatCan release since then makes
    the lane refresh it once - a keep-old merge of the same table)."""
    st = lane.load_state()
    c = st["cubes"].get(str(pid))
    if not c:
        raise RuntimeError(f"the lane state has no record of {pid}")
    debt = lane._read_json(lane.DEBT, {"cubes": {}, "new_cubes": {}})
    in_new = str(pid) in st.get("new_cubes", {})

    def clear_debt():
        if str(pid) in debt.get("new_cubes", {}):
            debt["new_cubes"].pop(str(pid), None)
            blob.write_bytes_atomic(lane.DEBT, json.dumps(debt, sort_keys=True).encode("utf-8"))

    if not c.get("not_held") and not in_new:
        # ALREADY RECORDED by an earlier run that stopped before its receipt (the state is saved first)
        clear_debt()
        rec.update(stage="done", lane_release=c.get("merged"))
        return
    if not c.get("not_held") and in_new:
        # THE LANE TOOK IT OVER between two runs of this tool: a newer release came, the lane found the
        # parquet this tool published, merged it and dropped not_held - but served nothing (the ids were
        # not catalogued yet) and keeps it in new_cubes. Its ids are catalogued now: owe the lane a
        # serve (served < merged) so it writes them from ITS merge, and close the new-cube debt.
        c["served"] = None
        st["new_cubes"].pop(str(pid), None)
        lane.save_state(st)
        clear_debt()
        rec.update(stage="done", lane_release=c.get("merged"), lane_took_over=True)
        return
    if not in_new:
        raise RuntimeError(f"the lane state is half-way for {pid} (not_held, but not in new_cubes) - "
                           f"written by something else")
    rel = c["release"]
    b = rec.get("parquet_bytes")
    c.update(merged=rel, served=rel, rows=rec["rows"], max_obs=rec.get("end"), bytes=b,
             est_min=round(b * sc.EST_SECONDS_PER_STORED_BYTE / 60.0, 1) if b else None,
             fails=0, transient_fails=0, retry_after=None, last_error=None)
    c.pop("not_held", None)
    if rec.get("needs_stored_floor"):
        st["parsed_rows"][str(pid)] = rec["rows"]
    st["new_cubes"].pop(str(pid), None)
    lane.save_state(st)
    clear_debt()
    rec.update(stage="done", lane_release=rel)


def ingest(pid: str, lite: dict | None, put, release=None, retry_refused=False) -> dict:
    """Carry one cube from its recorded stage to `done`. Returns its receipt."""
    rec = load_receipt(pid)
    if rec.get("stage") == "done":
        return {**rec, "this_run": "already done"}
    if rec.get("stage") == "refused" and rec.get("release") == release and not retry_refused:
        # the same release was refused before: not downloaded again (a refused giant is ~0.6 GB)
        raise AlreadyRefused(f"refused before for this release ({rec.get('reason')}); --retry-refused to try again")
    if rec.get("stage") == "committing":
        parquet, _done = _paths(pid)
        if not blob.exists(parquet) and not os.path.exists(parquet):
            rec = {"pid": pid, "stage": None}            # the publish never happened: start again
        else:
            if lite is None:
                raise RuntimeError("StatCan no longer lists it, and its parquet is half-published - a human's")
            finish_commit(pid, lite, rec)
            save_receipt(rec)
    if rec.get("stage") in (None, "refused"):
        if lite is None:
            raise Refused("StatCan no longer lists it (getAllCubesListLite) - withdrawn upstream")
        rec = {"pid": pid, "stage": None, "started_utc": _now(), "release": release}
        stage_commit(pid, lite, rec)
        save_receipt(rec)
    if rec["stage"] == "committed":
        stage_serve(pid, rec, put)
        save_receipt(rec)
    if rec["stage"] == "served":
        stage_catalogue(pid, rec)
        save_receipt(rec)
    if rec["stage"] == "catalogued":
        stage_record(pid, rec)
        save_receipt(rec)
    return {**rec, "this_run": "ingested"}


# --------------------------------------------------------------------------- #
# the run
# --------------------------------------------------------------------------- #
def new_cubes(pids_arg: str | None) -> dict:
    """{pid: release} from the lane's state - the population this tool may touch, and nothing else."""
    st = lane.load_state()
    listed = dict(st.get("new_cubes") or {})
    # A cube this tool has begun stays in scope until its receipt says done, even once the lane's
    # record has landed (the state is saved before the receipt): a re-run then finishes it.
    for name in sorted(os.listdir(RECEIPTS)) if os.path.isdir(RECEIPTS) else []:
        p = name[:-5]
        if name.endswith(".json") and not name.startswith("_") and p not in listed:
            if load_receipt(p).get("stage") in ("committing", "committed", "served", "catalogued"):
                listed[p] = None
    if pids_arg:
        want = [s.strip() for s in pids_arg.split(",") if s.strip()]
        bad = [p for p in want if p not in listed]
        if bad:
            raise SystemExit(f"REFUSING: not in the lane's new_cubes (and no ingest in progress): {bad}")
        return {p: listed[p] for p in want}
    return listed


def live_problems() -> list:
    """Every path an --apply run writes must be the live checkout's ([] = all are). Checked as PATHS, not
    as the code's location: the store dir follows ECONDL_ROOT / AQUEDUCT_DATA_ROOT, and the lock dir is
    writer_lock's own (review F8)."""
    same = lambda a, b: os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))  # noqa: E731
    store = os.path.join(LIVE_ROOT, "data", "clean_full", "statcan")
    want = (("the statcan store dir", sc.OUT_DIR, store),
            ("the lane state", lane.STATE, os.path.join(store, "_lane_state.json")),
            ("the catalogue debt", lane.DEBT, os.path.join(store, "_catalogue_debt.json")),
            ("the split map", lane.SPLIT_MAP, os.path.join(store, "_split_map.json")),
            ("the catalogue", CATALOGUE, catalog_path.under(LIVE_ROOT)),
            ("the writer-lock dir", writer_lock.LOCK_DIR, os.path.join(LIVE_ROOT, "logs")),
            ("the receipts", RECEIPTS, os.path.join(LIVE_ROOT, "logs", "statcan_new_cubes")))
    return [f"{name} is {got}, not {exp}" for name, got, exp in want if not same(got, exp)]


def run(a, put=None, clock=time.monotonic, started=None) -> int:
    cutover.refuse_if_cut_over("statcan_new_cubes - the statcan store is R2 before T0, and its post-T0 "
                               "writer is the owner's T0 decision (jobs/statcan_lane.POST_T0_READY)")
    cubes = new_cubes(a.pids)
    print(f"[new-cubes] {len(cubes)} cube(s) in scope; backend {os.environ.get('AQUEDUCT_BACKEND')}; "
          f"{'APPLY' if a.apply else 'DRY'}{' + fetch' if a.fetch and not a.apply else ''}", flush=True)
    lite = lite_rows()
    results, failed = {}, 0
    if not a.apply:
        if a.fetch:
            load_fts(cubes)
        for pid in sorted(cubes):
            row = {"release": cubes[pid], "listed_by_statcan": pid in lite}
            try:
                row["held"] = held_anywhere(pid, read_split_map())
                if pid in lite and not row["held"]:
                    want_rows, want_series = sc._cube_counts(int(pid))
                    row.update(nbDatapointsCube=want_rows, nbSeriesCube=want_series,
                               title=(lite[pid].get("cubeTitleEn") or "")[:120])
                    if a.fetch:
                        p = prepare(pid, lite[pid])
                        sc._remove_quietly(p["scratch"])
                        row.update(rows=p["st"]["rows"], zip_bytes=p["st"].get("zip_bytes"),
                                   split=p["split"], ids=len(p["ids"]))
            except Refused as e:
                row["would_refuse"] = str(e)
            except Exception as e:                                   # noqa: BLE001 - reported per cube
                row["error"] = f"{type(e).__name__}: {str(e)[:200]}"
                failed += 1
            results[pid] = row
            print(f"  {pid}: {json.dumps(row, sort_keys=True)}", flush=True)
        print("[new-cubes] DRY RUN: nothing was written", flush=True)
        return 1 if failed else 0
    bad = live_problems()
    if bad:
        raise SystemExit("REFUSING --apply: only the live checkout's files may be written - " + "; ".join(bad))
    # THE BUDGET COUNTS FROM THE LOCK (review C1): main() passes the moment it took it, so the series_fts
    # read below (measured ~18 min cold) is inside the budget, not added to it
    started = clock() if started is None else started
    load_fts(cubes)                                # ONE series_fts scan for the whole run (load_fts)
    put = put or lane._store_put()
    budget_s = 60.0 * float(getattr(a, "max_minutes", MAX_MINUTES_DEFAULT))
    deferred = []
    for pid in sorted(cubes):
        if clock() - started >= budget_s:
            deferred.append(pid)                 # not started: the lane must get its lock back (docstring)
            continue
        try:
            rec = ingest(pid, lite.get(pid), put, release=cubes[pid],
                         retry_refused=bool(getattr(a, "retry_refused", False)))
            results[pid] = {k: rec.get(k) for k in ("stage", "this_run", "rows", "ids", "split",
                                                     "parquet_bytes", "zip_bytes", "catalogue_rows")}
        except Refused as e:
            r = load_receipt(pid)
            if r.get("stage") in (None, "refused"):
                # NOTHING WRITTEN for it: a refusal, named, not a failure of this run
                results[pid] = {"stage": "refused", "reason": str(e)}
                if not isinstance(e, AlreadyRefused):    # keep the first reason; never nest it (review C4)
                    save_receipt({"pid": pid, "stage": "refused", "reason": str(e), "release": cubes[pid]})
            else:
                # refused PART-WAY (objects exist, the rest cannot follow): a human's, so it fails the run
                failed += 1
                results[pid] = {"stage": r.get("stage"), "error": f"Refused: {e}"}
        except Exception as e:                                          # noqa: BLE001 - per cube; recorded
            # ANY failure of one cube (a store client error included) is recorded and the run goes on;
            # SystemExit (an unreadable split map) still stops the whole run.
            failed += 1
            results[pid] = {"stage": load_receipt(pid).get("stage"), "error": f"{type(e).__name__}: {str(e)[:300]}"}
        print(f"  {pid}: {json.dumps(results[pid], sort_keys=True)}", flush=True)
    os.makedirs(RECEIPTS, exist_ok=True)
    summary = os.path.join(RECEIPTS, f"_run_{_now().replace(':', '')}.json")
    with open(summary, "w", encoding="utf-8") as fh:
        json.dump({"utc": _now(), "cubes": results, "deferred_by_budget": deferred}, fh, indent=1, sort_keys=True)
    done = sum(1 for r in results.values() if r.get("stage") == "done")
    refused = sum(1 for r in results.values() if r.get("stage") == "refused")
    print(f"[new-cubes] {done} done, {refused} refused, {failed} failed of {len(results)} started; "
          f"{len(deferred)} not started (--max-minutes){': ' + ','.join(deferred) if deferred else ''}; "
          f"receipt {summary}", flush=True)
    return 1 if failed else 0


def main(argv=None) -> int:
    # allow_abbrev=False: __main__ takes the catalogue writer lock on the exact "--apply"; a prefix such as
    # "--app" must not reach main() as --apply without it (tests/test_catalogue_writers_main.py)
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0], allow_abbrev=False)
    ap.add_argument("--apply", action="store_true", help="write (default: a dry report)")
    ap.add_argument("--fetch", action="store_true",
                    help="dry run only: also download, parse, split and check each cube (scratch, removed)")
    ap.add_argument("--pids", default=None, help="comma-separated subset of the lane's new_cubes")
    ap.add_argument("--max-minutes", type=float, default=MAX_MINUTES_DEFAULT,
                    help="--apply starts no cube after this many minutes (the lane waits on the lock)")
    ap.add_argument("--retry-refused", action="store_true",
                    help="try again a cube refused before for the same release")
    a = ap.parse_args(argv)
    cutover.refuse_if_cut_over("statcan_new_cubes - refused after T0 (see run())")
    # THE STORE OF RECORD BEFORE T0 IS R2, as for the lane: set when nothing chose a backend, refused when
    # something chose another (jobs/statcan_lane.pin_backend). A dry run reads the same store.
    lane.pin_backend()
    if a.apply:
        bad = live_problems()
        if bad:
            raise SystemExit("REFUSING --apply: only the live checkout's files may be written - " + "; ".join(bad))
        writer_lock.hold_or_refuse(LOCK_NAME, WHAT)
        return run(a, started=time.monotonic())        # the budget counts from the lock (review C1)
    return run(a)


if __name__ == "__main__":
    if "--apply" in sys.argv[1:]:
        with catalog_path.write_session():   # after T0: the single-writer lock (refused anyway: see run())
            sys.exit(main())
    sys.exit(main())                         # a dry run reads only
