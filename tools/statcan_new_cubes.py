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

ORDER PER CUBE (objects before catalogue rows, R341), each stage recorded before the next in
logs/statcan_new_cubes/<pid>.json, so a re-run resumes a cube at its next stage:
  1. pre-checks - in the lane's new_cubes, listed by StatCan, nothing of it held anywhere yet
  2. fetch      - statcan._fetch_cube_whole: the full table, the ingester's parser, StatCan's counts
  3. split      - derive_statcan_tables.choose_split at the lane's cap; a refusal writes nothing
  4. commit     - the parquet (blob.commit_local_file), then the .done record         -> committed
  5. serve      - split-map entry, then every id of the cube put to the CSV store     -> served
  6. catalogue  - one transaction: series + series_fts rows, verified before commit  -> catalogued
  7. record     - the lane's state and debt                                           -> done

GUARDS. Dry by default (no store, map, catalogue or state write; --fetch also downloads and splits into
the lane's scratch dir and removes it). Refused after T0, like the lane (core.cutover; the post-T0 statcan
writer is the owner's T0 decision). --apply runs only from the live checkout: the writer lock, the split
map, the catalogue and the .done files are all paths under the checkout, so a worktree run would lock
nothing and write the wrong files. --apply takes logs/statcan_writer.lock for the whole run (never waits:
held by the lane -> refused; its idle iteration ends within minutes). No D1 module is imported, and no
object outside clean_full/statcan/ and series/statcan%3A<pid> is written.

    python tools/statcan_new_cubes.py                      # DRY: what would be ingested
    python tools/statcan_new_cubes.py --fetch --pids 33101174
    python tools/statcan_new_cubes.py --apply [--pids a,b]
"""
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover                            # noqa: E402
from updater import blob, merge, writer_lock                       # noqa: E402
from updater.errors import DefinitiveError, TransientError         # noqa: E402
from updater.strategies.fetchers import statcan as sc              # noqa: E402

import jobs.ingest_statcan as ing                                   # noqa: E402
import jobs.statcan_lane as lane                                    # noqa: E402

LOCK_NAME = lane.LOCK_NAME
WHAT = "tools/statcan_new_cubes.py (an --apply run)"
# THE LIVE CHECKOUT, the one place an --apply run may write from (module docstring). Tests point it at a
# temporary folder.
LIVE_ROOT = catalog_path.LIVE_STORE_ROOT
CATALOGUE = catalog_path.under(ROOT)
RECEIPTS = os.path.join(ROOT, "logs", "statcan_new_cubes")
STAGES = ("committed", "served", "catalogued", "done")
FTS_IDS_PER_STMT = 500   # series_fts(series_id UNINDEXED): one statement = one scan; raise arity (R492)

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
    """A cube this run must not touch (the reason is the message). Nothing was written for it."""


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
# what is already held
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


def fts_has(con, ids) -> set:
    ids = list(ids)
    out = set()
    for i in range(0, len(ids), FTS_IDS_PER_STMT):
        ch = ids[i:i + FTS_IDS_PER_STMT]
        marks = ",".join("?" * len(ch))
        out.update(r[0] for r in con.execute(
            f"SELECT series_id FROM series_fts WHERE series_id IN ({marks})", ch))
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


# --------------------------------------------------------------------------- #
# the stages
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


def choose(path: str, n_rows: int) -> tuple:
    """(dim, parts) from the derive tool's own choose_split at the lane's cap (the store's cap)."""
    import duckdb                                                    # noqa: PLC0415
    spill = merge._bounded_spill_dir()
    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='8GB'")
        con.execute("SET temp_directory='%s'" % spill.replace("\\", "/").replace("'", "''"))
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET enable_progress_bar=false")
        return lane._tool().choose_split(con, path.replace("\\", "/"), n_rows, lane.MAX_ROWS)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)


def fetch_and_split(pid: str) -> tuple:
    """(scratch_parquet, parse_stats, dim, parts). The caller removes the scratch file."""
    import pyarrow.parquet as pq                                     # noqa: PLC0415
    scratch, st = sc._fetch_cube_whole(int(pid))
    try:
        n_rows = pq.read_metadata(scratch).num_rows
        dim, parts = choose(scratch, n_rows)
    except BaseException:
        sc._remove_quietly(scratch)
        raise
    st["rows"] = n_rows
    return scratch, st, dim, parts


def stage_commit(pid: str, lite: dict, rec: dict, smap: dict) -> None:
    """Fetch, split, and publish the parquet and its .done record. Nothing is written when the gate or
    the splitter refuses."""
    why = held_anywhere(pid, smap)
    if why:
        raise Refused("already held: " + "; ".join(why))
    scratch, st, dim, parts = fetch_and_split(pid)
    parquet, done = _paths(pid)
    try:
        if dim == "":
            raise Refused(f"{st['rows']:,} rows and no column or level divides it below the "
                          f"{lane.MAX_ROWS:,}-row cap (derive_statcan_tables.choose_split) - nothing written")
        if blob.exists(parquet) or os.path.exists(parquet):          # re-asked just before the publish
            raise Refused("its parquet appeared while this run fetched it")
        blob.commit_local_file(scratch, parquet)
    finally:
        sc._remove_quietly(scratch)
    record = ing.done_record(lite, int(pid), st, parquet)
    tmp = f"{done}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(record, fh)
    os.replace(tmp, done)
    rec.update(stage="committed", rows=st["rows"], n_series=st["n_series"], zip_bytes=st.get("zip_bytes"),
               parquet_bytes=os.path.getsize(parquet), start=st["start"], end=st["end"],
               layout=st.get("layout"), needs_stored_floor=bool(st.get("needs_stored_floor")),
               split={"dim": dim, "parts": parts, "rows": st["rows"]} if dim else None)


def _rows_for(pid: str, rec: dict) -> list:
    parquet, done = _paths(pid)
    with open(done, encoding="utf-8") as fh:
        sidecar = json.load(fh)
    spill = merge._bounded_spill_dir()
    try:
        rows, _untitled = _cat().cube_rows(pid, parquet.replace("\\", "/"), sidecar, rec.get("split"), spill)
    finally:
        shutil.rmtree(spill, ignore_errors=True)
    return rows


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
    cat = _cat()
    rows = _rows_for(pid, rec)
    ids = [r[0] for r in rows]
    if len(ids) != rec.get("ids"):
        raise RuntimeError(f"{len(ids)} catalogue row(s) now, {rec.get('ids')} served - the store moved")
    with catalog_path.write_session():
        con = catalog_path.connect_path(CATALOGUE, write=True, timeout=180)
        try:
            con.execute("PRAGMA busy_timeout = 180000")
            lic = con.execute("SELECT reservable FROM license WHERE license_id=?", (cat.LICENSE_ID,)).fetchone()
            if not lic or not lic[0]:
                raise Refused(f"licence {cat.LICENSE_ID!r} missing or not reservable - a row is an offer to serve")
            if not con.execute("SELECT 1 FROM source WHERE source_id=?", (cat.SOURCE,)).fetchone():
                raise Refused(f"no source row {cat.SOURCE!r} in the catalogue")
            if fts_has(con, ids):
                raise Refused("series_fts already holds ids of this cube")
            lo = lane._tool().unit_id(str(pid))
            con.execute("BEGIN IMMEDIATE")
            try:
                con.executemany(
                    """INSERT INTO series
                       (series_id,source_id,title,frequency,unit,geography,category,license_id,
                        start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""", rows)
                con.executemany("INSERT INTO series_fts(series_id, title, geography) VALUES(?,?,?)",
                                [(r[0], r[2], r[5]) for r in rows])
                n = con.execute("SELECT count(*) FROM series WHERE series_id >= ? AND series_id < ?",
                                (lo, lo + "$")).fetchone()[0]
                f = fts_has(con, ids)
                if n != len(ids) or f != set(ids):
                    raise RuntimeError(f"after the insert: {n} series row(s) and {len(f)} fts id(s) "
                                       f"in the cube's range, want {len(ids)} of each")
            except BaseException:
                con.rollback()
                raise
            con.commit()
        finally:
            con.close()
    rec.update(stage="catalogued", catalogue_rows=len(ids))


def stage_record(pid: str, rec: dict) -> None:
    """The lane's record: the cube is held now, so it leaves new_cubes and its next release is the
    lane's to merge and serve. The release recorded is the lane's own (a newer StatCan release since
    then makes the lane refresh it once - a keep-old merge of the same table)."""
    st = lane.load_state()
    c = st["cubes"].get(str(pid))
    if not c:
        raise RuntimeError(f"the lane state has no record of {pid}")
    debt = lane._read_json(lane.DEBT, {"cubes": {}, "new_cubes": {}})
    if not c.get("not_held") and str(pid) not in st.get("new_cubes", {}):
        # ALREADY RECORDED by an earlier run that stopped before its receipt (the state is saved first):
        # finish the debt half, never record twice.
        if str(pid) in debt.get("new_cubes", {}):
            debt["new_cubes"].pop(str(pid), None)
            blob.write_bytes_atomic(lane.DEBT, json.dumps(debt, sort_keys=True).encode("utf-8"))
        rec.update(stage="done", lane_release=c.get("merged"))
        return
    if not c.get("not_held") or str(pid) not in st.get("new_cubes", {}):
        raise RuntimeError(f"the lane state is half-way for {pid} (not_held {c.get('not_held')!r}, in "
                           f"new_cubes {str(pid) in st.get('new_cubes', {})}) - written by something else")
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
    debt.setdefault("new_cubes", {}).pop(str(pid), None)
    blob.write_bytes_atomic(lane.DEBT, json.dumps(debt, sort_keys=True).encode("utf-8"))
    rec.update(stage="done", lane_release=rel)


def ingest(pid: str, lite: dict | None, put) -> dict:
    """Carry one cube from its recorded stage to `done`. Returns its receipt."""
    rec = load_receipt(pid)
    if rec.get("stage") == "done":
        return {**rec, "this_run": "already done"}
    if rec.get("stage") in (None, "refused"):
        if lite is None:
            raise Refused("StatCan no longer lists it (getAllCubesListLite) - withdrawn upstream")
        rec = {"pid": pid, "stage": None, "started_utc": _now()}
        stage_commit(pid, lite, rec, read_split_map())
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
            if load_receipt(p).get("stage") in ("committed", "served", "catalogued"):
                listed[p] = None
    if pids_arg:
        want = [s.strip() for s in pids_arg.split(",") if s.strip()]
        bad = [p for p in want if p not in listed]
        if bad:
            raise SystemExit(f"REFUSING: not in the lane's new_cubes (and no ingest in progress): {bad}")
        return {p: listed[p] for p in want}
    return listed


def _is_live_checkout() -> bool:
    same = lambda a, b: os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))  # noqa: E731
    return same(ROOT, LIVE_ROOT)


def run(a, put=None) -> int:
    cutover.refuse_if_cut_over("statcan_new_cubes - the statcan store is R2 before T0, and its post-T0 "
                               "writer is the owner's T0 decision (jobs/statcan_lane.POST_T0_READY)")
    cubes = new_cubes(a.pids)
    print(f"[new-cubes] {len(cubes)} cube(s) in scope; backend {os.environ.get('AQUEDUCT_BACKEND')}; "
          f"{'APPLY' if a.apply else 'DRY'}{' + fetch' if a.fetch and not a.apply else ''}", flush=True)
    lite = lite_rows()
    smap = read_split_map()
    results, failed = {}, 0
    if not a.apply:
        for pid in sorted(cubes):
            row = {"release": cubes[pid], "listed_by_statcan": pid in lite}
            try:
                why = held_anywhere(pid, smap)
                row["held"] = why
                if pid in lite and not why:
                    want_rows, want_series = sc._cube_counts(int(pid))
                    row.update(nbDatapointsCube=want_rows, nbSeriesCube=want_series,
                               title=(lite[pid].get("cubeTitleEn") or "")[:120])
                    if a.fetch:
                        scratch, st, dim, parts = fetch_and_split(pid)
                        sc._remove_quietly(scratch)
                        row.update(rows=st["rows"], zip_bytes=st.get("zip_bytes"),
                                   split=(dim or None) if dim != "" else "REFUSED", parts=parts)
            except (TransientError, DefinitiveError) as e:
                row["error"] = f"{type(e).__name__}: {str(e)[:200]}"
                failed += 1
            results[pid] = row
            print(f"  {pid}: {json.dumps(row, sort_keys=True)}", flush=True)
        print("[new-cubes] DRY RUN: nothing was written", flush=True)
        return 1 if failed else 0
    if not _is_live_checkout():
        raise SystemExit(f"REFUSING --apply from {ROOT}: only the live checkout {LIVE_ROOT} may write - the "
                         f"writer lock, the split map, the catalogue and the .done records are its files")
    put = put or lane._store_put()
    for pid in sorted(cubes):
        try:
            rec = ingest(pid, lite.get(pid), put)
            results[pid] = {k: rec.get(k) for k in ("stage", "this_run", "rows", "ids", "split",
                                                     "parquet_bytes", "zip_bytes", "catalogue_rows")}
        except Refused as e:
            r = load_receipt(pid)
            if r.get("stage") in (None, "refused"):
                # NOTHING WRITTEN for it: a refusal, named, not a failure of this run
                results[pid] = {"stage": "refused", "reason": str(e)}
                save_receipt({"pid": pid, "stage": "refused", "reason": str(e)})
            else:
                # refused PART-WAY (objects exist, the rest cannot follow): a human's, so it fails the run
                failed += 1
                results[pid] = {"stage": r.get("stage"), "error": f"Refused: {e}"}
        except (TransientError, DefinitiveError, RuntimeError, OSError) as e:
            failed += 1
            results[pid] = {"stage": load_receipt(pid).get("stage"), "error": f"{type(e).__name__}: {str(e)[:300]}"}
        print(f"  {pid}: {json.dumps(results[pid], sort_keys=True)}", flush=True)
    os.makedirs(RECEIPTS, exist_ok=True)
    summary = os.path.join(RECEIPTS, f"_run_{_now().replace(':', '')}.json")
    with open(summary, "w", encoding="utf-8") as fh:
        json.dump({"utc": _now(), "cubes": results}, fh, indent=1, sort_keys=True)
    done = sum(1 for r in results.values() if r.get("stage") == "done")
    refused = sum(1 for r in results.values() if r.get("stage") == "refused")
    print(f"[new-cubes] {done} done, {refused} refused, {failed} failed of {len(results)}; receipt {summary}",
          flush=True)
    return 1 if failed else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--apply", action="store_true", help="write (default: a dry report)")
    ap.add_argument("--fetch", action="store_true",
                    help="dry run only: also download, parse and split each cube (scratch, removed)")
    ap.add_argument("--pids", default=None, help="comma-separated subset of the lane's new_cubes")
    a = ap.parse_args(argv)
    cutover.refuse_if_cut_over("statcan_new_cubes - refused after T0 (see run())")
    # THE STORE OF RECORD BEFORE T0 IS R2, as for the lane: set when nothing chose a backend, refused when
    # something chose another (jobs/statcan_lane.pin_backend). A dry run reads the same store.
    lane.pin_backend()
    if a.apply:
        if not _is_live_checkout():
            raise SystemExit(f"REFUSING --apply from {ROOT}: only the live checkout {LIVE_ROOT} may write")
        writer_lock.hold_or_refuse(LOCK_NAME, WHAT)
    return run(a)


if __name__ == "__main__":
    sys.exit(main())
