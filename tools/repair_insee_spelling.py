"""One-off repair of the 8 insee_melodi flows that run 39024352275 doubled (2026-10-17), after INSEE added a
constant GEO dimension (GEO=2025-FRANCE-F) to the DD_CNA keys. Decided by the coordinator, 2026-10-17: the
repair removes only rows the new spelling exactly replaces, and deletes no row INSEE no longer serves.

    python tools/repair_insee_spelling.py --work F:/work/insee_repair --mirror <pre-run mirror dir>
    python tools/repair_insee_spelling.py --work ... --mirror ... --apply      (after review; writes R2)

THE PLAN, per flow (computed the same way in a dry run and in --apply):
  SUPERSEDE (DD_CNA_APU, DD_CNA_BRANCHES - origin pulls; the new spelling covers every year):
      new  = the served rows the run added (served now, ANTI JOIN the pre-run mirror on (series_key, obs_date))
      old  = the served rows the mirror holds
      kept = old rows with NO exact counterpart: no new row whose key minus GEO=2025-FRANCE-F equals the old
             key, on the same date. They are series INSEE no longer serves in any spelling, so they are not
             re-crawlable and are KEPT, reported as residue.
      publish new + kept.
  REVERT (the 6 DD_CNA tails - only 2025 exists in the new spelling):
      publish the served rows minus the added rows, PROVED equal to the pre-run mirror by a row hash (count,
      sum and xor of hash(flow, series_key, obs_date, value)); the bytes published are the mirror's file.
      The rows removed are 2025 rows in the new spelling: re-crawlable, by a later origin re-pull.
  DS_ANTIPOL is NOT touched (its added rows are a new year, 2024, not a doubling).

PRECONDITIONS, checked before anything is planned, refused otherwise:
  - _flows.json was last written by the run being repaired (gh:39024352275), and each flow's served
    identity there equals the object on R2 now (nothing has written it since);
  - the mirror is a subset of the served object by (key, date), and served = mirror + added exactly;
  - every added key carries GEO=2025-FRANCE-F exactly once, no old key carries any GEO, and the mapping
    stripped-new-key -> old key is one to one.

--apply, per flow, in this order (one flow fully before the next):
  1. a byte copy of the served object (and of _flows.json) goes to <work>/rollback/ - the rollback;
  2. the parquet is PUT with If-Match on the ETag the plan read (refused if anything wrote it since);
  3. _flows.json is updated under its compare-and-swap (If-Match = md5 of the bytes read): served identity,
     rows, max_obs, maxyear_rows, residue (supersede), modified cleared (revert: the walk must re-check),
     and a `repair` record;
  4. the CSV is re-derived: core/derive_csv.py --only insee_melodi:<FLOW> with ECONDL_DATA pinned to
     <work>/pinned, a copy of exactly the bytes just written (md5 == R2's ETag, which #156's check proves).
  It refuses after T0 (the cloud copy is frozen then) and needs the R2 write keys.
A dry run reads R2 with the READ keys, writes only under --work, and PUTs nothing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import urllib.parse

ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, ROOT)

BUCKET = "econ-data"
PREFIX = "clean_full/insee_melodi/"
STATE_KEY = PREFIX + "_flows.json"
RUN_WRITER = "gh:39024352275"
GEO = "GEO=2025-FRANCE-F"
SUPERSEDE = ("DD_CNA_APU", "DD_CNA_BRANCHES")
REVERT = ("DD_CNA_AGREGATS", "DD_CNA_CONSO_MENAGES_COICOP", "DD_CNA_CONSO_MENAGES_PRODUITS", "DD_CNA_ECH_EXT",
          "DD_CNA_PATRIMOINE", "DD_CNA_REBS")
UNTOUCHED = ("DS_ANTIPOL",)
KEY_SQL = "regexp_replace(regexp_replace(series_key, '(^|:)GEO=2025-FRANCE-F', ''), '^:', '')"
PART_SQL = ("regexp_replace(series_key, '=[^:]*', '', 'g') || '|' || "
            "coalesce(regexp_extract(series_key, 'GEO=(\\d{4})-', 1), '')")


class Refused(Exception):
    pass


# --------------------------------------------------------------------------------------------- the plan
def _con(now_tbl, old_tbl):
    import duckdb
    con = duckdb.connect()
    con.register("now_t", now_tbl)
    con.register("old_t", old_tbl)
    con.execute("CREATE TABLE added AS SELECT n.* FROM now_t n ANTI JOIN old_t o "
                "ON n.series_key = o.series_key AND n.obs_date = o.obs_date")
    return con


def _row_hash(con, rel: str) -> tuple:
    return tuple(con.execute(
        f"SELECT count(*), coalesce(sum(hash(flow, series_key, obs_date, value)::HUGEINT), 0), "
        f"coalesce(bit_xor(hash(flow, series_key, obs_date, value)), 0) FROM {rel}").fetchone())


def preconditions(con, n_now: int, n_old: int) -> dict:
    """Shared checks: mirror ⊆ served, served = mirror + added, the added rows carry the GEO spelling."""
    missing = con.execute("SELECT count(*) FROM old_t o ANTI JOIN now_t n "
                          "ON n.series_key = o.series_key AND n.obs_date = o.obs_date").fetchone()[0]
    n_added = con.execute("SELECT count(*) FROM added").fetchone()[0]
    dup_now = con.execute("SELECT count(*) - count(DISTINCT (series_key, obs_date)) FROM now_t").fetchone()[0]
    if missing:
        raise Refused(f"{missing:,} mirror rows are not on R2: the mirror is not the pre-run object")
    if dup_now:
        raise Refused(f"the served object holds {dup_now:,} duplicate (key, date)")
    if n_now != n_old + n_added:
        raise Refused(f"served {n_now:,} != mirror {n_old:,} + added {n_added:,}")
    bad_new = con.execute(f"SELECT count(*) FROM added WHERE (length(series_key) - length(replace(series_key, "
                          f"'{GEO}', ''))) / {len(GEO)} != 1").fetchone()[0]
    geo_old = con.execute("SELECT count(*) FROM old_t WHERE series_key LIKE '%GEO=%'").fetchone()[0]
    many = con.execute(f"SELECT count(*) FROM (SELECT {KEY_SQL} k FROM (SELECT DISTINCT series_key FROM added) "
                       f"GROUP BY k HAVING count(*) > 1)").fetchone()[0]
    if bad_new or geo_old or many:
        raise Refused(f"the spelling rule is not exact: {bad_new:,} added keys without exactly one {GEO}, "
                      f"{geo_old:,} old keys with a GEO, {many:,} stripped keys shared by two new keys")
    return {"added": n_added}


def plan_supersede(now_tbl, old_tbl):
    """(published table, report). Publish = added + old rows without an exact counterpart."""
    con = _con(now_tbl, old_tbl)
    pre = preconditions(con, now_tbl.num_rows, old_tbl.num_rows)
    con.execute(f"CREATE TABLE added_k AS SELECT *, {KEY_SQL} AS k FROM added")
    con.execute("CREATE TABLE kept AS SELECT o.* FROM old_t o ANTI JOIN added_k a "
                "ON a.k = o.series_key AND a.obs_date = o.obs_date")
    pub = con.execute("SELECT flow, series_key, obs_date, value FROM (SELECT flow, series_key, obs_date, value "
                      "FROM added UNION ALL SELECT flow, series_key, obs_date, value FROM kept) "
                      "ORDER BY series_key, obs_date").to_arrow_table()
    con.register("pub_t", pub)
    dup = con.execute("SELECT count(*) - count(DISTINCT (series_key, obs_date)) FROM pub_t").fetchone()[0]
    if dup:
        raise Refused(f"the published table would hold {dup:,} duplicate (key, date)")
    kept_n = con.execute("SELECT count(*) FROM kept").fetchone()[0]
    revised = con.execute("SELECT count(*) FROM old_t o JOIN added_k a ON a.k = o.series_key AND "
                          "a.obs_date = o.obs_date WHERE o.value IS DISTINCT FROM a.value").fetchone()[0]
    residue = {"rows": int(kept_n), "by_partition": {str(p): int(n) for p, n in con.execute(
        f"SELECT {PART_SQL} AS p, count(*) FROM kept GROUP BY 1").fetchall()}}
    rep = {"action": "supersede", "rows_before": now_tbl.num_rows, "rows_after": pub.num_rows,
           "old_rows": old_tbl.num_rows, "added_rows": pre["added"],
           "dropped_old_rows": old_tbl.num_rows - kept_n, "kept_old_rows_residue": kept_n,
           "kept_old_series": con.execute("SELECT count(DISTINCT series_key) FROM kept").fetchone()[0],
           "dropped_rows_whose_value_the_new_pull_revised": revised, "residue": residue}
    con.close()
    return pub, rep


def plan_revert(now_tbl, old_tbl):
    """(published table, report). Publish = served minus added, proved equal to the mirror by row hash."""
    con = _con(now_tbl, old_tbl)
    pre = preconditions(con, now_tbl.num_rows, old_tbl.num_rows)
    con.execute("CREATE TABLE back AS SELECT n.* FROM now_t n SEMI JOIN old_t o "
                "ON n.series_key = o.series_key AND n.obs_date = o.obs_date")
    h_back, h_old = _row_hash(con, "back"), _row_hash(con, "old_t")
    if h_back != h_old:
        raise Refused(f"served minus added is not the mirror by row hash: {h_back} != {h_old}")
    rep = {"action": "revert", "rows_before": now_tbl.num_rows, "rows_after": old_tbl.num_rows,
           "added_rows_removed": pre["added"], "row_hash": [str(x) for x in h_back]}
    con.close()
    return old_tbl, rep


def maxyear_rows(tbl) -> tuple[str | None, int]:
    import pyarrow.compute as pc
    od = tbl.column("obs_date")
    if tbl.num_rows == 0:
        return None, 0
    mx = pc.max(od).as_py()
    return mx.isoformat(), int(pc.sum(pc.equal(pc.year(od), mx.year)).as_py() or 0)


# --------------------------------------------------------------------------------------------- I/O
def _read_client():
    """The client the plan reads with. NOTE r2_util.creds(write=False) falls back to the WRITE key pair when
    no real read pair is configured, and still labels the result mode "read" - so the pair is checked here,
    and said. The plan itself only ever calls get_object and head_object."""
    from core import r2_util
    e = r2_util.load_env()
    read_pair = all(r2_util._real(e.get(k)) for k in ("R2_READ_ENDPOINT", "R2_READ_ACCESS_KEY_ID",
                                                      "R2_READ_SECRET_ACCESS_KEY"))
    if r2_util.creds(write=False) is None:
        raise Refused("no R2 credentials are configured")
    print("R2 reads authenticate with the " + ("READ key pair" if read_pair else
          "WRITE key pair (no real read pair is configured); the plan calls get_object/head_object only"),
          flush=True)
    return r2_util.client(write=False)


def _get(c, key) -> tuple[bytes, str]:
    o = c.get_object(Bucket=BUCKET, Key=key)
    return o["Body"].read(), o["ETag"].strip('"')


def _csv_key(flow: str) -> str:
    return "series/" + urllib.parse.quote(f"insee_melodi:{flow}", safe="") + ".csv"


def planned_csv(catalog_repo: str, plan_root: str, flow: str) -> dict:
    """Derive the CSV the --apply step would publish, with the same code (core/derive_csv.py of the checkout
    that holds catalog.db), from the PLANNED parquet. Nothing is uploaded."""
    os.environ["ECONDL_DATA"] = plan_root
    if catalog_repo not in sys.path:
        sys.path.insert(0, catalog_repo)
        sys.path.insert(0, os.path.join(catalog_repo, "clients", "python"))
    from core import derive_csv                                      # noqa: PLC0415
    body = derive_csv._series_csv_bytes(f"insee_melodi:{flow}")
    lines = body.count(b"\n")
    return {"key": _csv_key(flow), "csv_bytes": len(body), "lines_incl_header": lines,
            "gzip_bytes": len(gzip.compress(body, compresslevel=9, mtime=0)),
            "md5": hashlib.md5(body).hexdigest()}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--work", required=True, help="a folder for the plan, the rollback and the pinned copies")
    ap.add_argument("--mirror", required=True, help="the pre-run mirror of clean_full/insee_melodi")
    ap.add_argument("--catalog-repo", required=True, help="the checkout whose data/catalog.db derives CSVs")
    ap.add_argument("--flows", nargs="*", default=list(SUPERSEDE + REVERT))
    ap.add_argument("--apply", action="store_true", help="WRITE R2 (after review). Default: dry run")
    ap.add_argument("--r2-env", help="a .env to take the R2_READ_* and R2_WRITE_* keys from; "
                                     "values are loaded into this process only and never printed")
    a = ap.parse_args(argv)
    if a.r2_env:
        wanted = ("R2_READ_", "R2_WRITE_")        # the WRITE pair is the reads' fallback (see _read_client)
        with open(a.r2_env, encoding="utf-8") as fh:
            for line in fh:
                k, sep, v = line.strip().partition("=")
                if sep and k.strip().startswith(wanted) and not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")
    work = os.path.abspath(a.work)
    if os.path.splitdrive(work)[0].upper() == "D:" or work.startswith(ROOT):
        raise Refused(f"--work {work} must not be on D: or inside the checkout")
    for f in a.flows:
        if f not in SUPERSEDE + REVERT:
            raise Refused(f"{f} is not one of the 8 flows this repair covers (DS_ANTIPOL is untouched)")
    from core import cutover
    if a.apply:
        cutover.refuse_if_cut_over("the insee_melodi spelling repair (it writes the cloud copy)")
    import pyarrow.parquet as pq

    c = _read_client()
    state_bytes, state_etag = _get(c, STATE_KEY)
    state = json.loads(state_bytes)
    if state.get("writer") != RUN_WRITER:
        raise Refused(f"_flows.json was last written by {state.get('writer')!r}, not {RUN_WRITER}: something "
                      f"ran since the pass being repaired - re-plan")
    for d in ("plan/insee_melodi", "rollback", "pinned/insee_melodi"):
        os.makedirs(os.path.join(work, d), exist_ok=True)
    with open(os.path.join(work, "rollback", "_flows.json"), "wb") as fh:
        fh.write(state_bytes)
    report = {"mode": "APPLY" if a.apply else "DRY RUN", "utc": dt.datetime.now(dt.timezone.utc).isoformat(),
              "state_etag": state_etag, "state_written_utc": state.get("written_utc"), "flows": {}}
    for flow in a.flows:
        fe = state["flows"].get(flow) or {}
        body, etag = _get(c, PREFIX + f"{flow}.parquet")
        want = ((fe.get("served") or {}).get("identity") or {}).get("etag")
        if want != etag:
            raise Refused(f"{flow}: R2's ETag {etag} is not the one _flows.json records ({want}) - re-plan")
        with open(os.path.join(work, "rollback", f"{flow}.parquet"), "wb") as fh:
            fh.write(body)
        now_tbl = pq.read_table(io.BytesIO(body))
        mirror_path = os.path.join(a.mirror, f"{flow}.parquet")
        old_tbl = pq.read_table(mirror_path)
        if flow in SUPERSEDE:
            pub, rep = plan_supersede(now_tbl, old_tbl)
            out = os.path.join(work, "plan", "insee_melodi", f"{flow}.parquet")
            pq.write_table(pub.cast(now_tbl.schema), out, compression="zstd")
        else:
            pub, rep = plan_revert(now_tbl, old_tbl)
            out = os.path.join(work, "plan", "insee_melodi", f"{flow}.parquet")
            shutil.copyfile(mirror_path, out)                             # the pre-run bytes themselves
        with open(out, "rb") as fh:
            new_bytes = fh.read()
        mx, my = maxyear_rows(pub)
        rep.update(r2_etag_before=etag, r2_bytes_before=len(body), planned_bytes=len(new_bytes),
                   planned_md5=hashlib.md5(new_bytes).hexdigest(), max_obs=mx, maxyear_rows=my,
                   rollback=os.path.join(work, "rollback", f"{flow}.parquet"))
        try:
            h = c.head_object(Bucket=BUCKET, Key=_csv_key(flow))
            rep["served_csv"] = {"key": _csv_key(flow), "bytes": int(h["ContentLength"]),
                                 "last_modified": str(h["LastModified"])}
        except Exception as e:                                            # noqa: BLE001
            rep["served_csv"] = {"key": _csv_key(flow), "error": f"{type(e).__name__}: {str(e)[:80]}"}
        rep["planned_csv"] = planned_csv(a.catalog_repo, os.path.join(work, "plan"), flow)
        rep["planned_state"] = {
            "served": {"identity": {"etag": rep["planned_md5"], "size": len(new_bytes)}, "rows": pub.num_rows,
                       "max_obs": mx, "maxyear_rows": my},
            **({"residue": rep["residue"]} if flow in SUPERSEDE else {"modified": None}),
            "repair": {"utc": "<at apply>", "of_run": RUN_WRITER, "action": rep["action"],
                       "rows_before": rep["rows_before"], "rows_after": rep["rows_after"]}}
        report["flows"][flow] = rep
        print(f"{flow:30s} {rep['action']:9s} rows {rep['rows_before']:>9,} -> {rep['rows_after']:>9,}"
              + (f"  dropped {rep['dropped_old_rows']:,} old (of which {rep['dropped_rows_whose_value_the_new_pull_revised']:,} "
                 f"revised by the new pull), kept {rep['kept_old_rows_residue']:,} old rows in "
                 f"{rep['kept_old_series']:,} series as residue" if flow in SUPERSEDE else
                 f"  removed {rep['added_rows_removed']:,} added rows; = mirror by row hash")
              + f" | CSV {rep['planned_csv']['lines_incl_header'] - 1:,} rows, "
                f"{rep['planned_csv']['gzip_bytes']:,} B gz (served now {rep['served_csv'].get('bytes', '?')} B)",
              flush=True)
        if a.apply:
            apply_one(flow, etag, new_bytes, rep, work, a.catalog_repo)
    with open(os.path.join(work, "plan_report.json"), "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1, default=str)
    print(f"\n{report['mode']}: {len(report['flows'])} flow(s); report {os.path.join(work, 'plan_report.json')}; "
          f"rollback copies in {os.path.join(work, 'rollback')}" + ("" if a.apply else "; nothing was written to R2"))
    return 0


def apply_one(flow, etag_before, new_bytes, rep, work, catalog_repo):
    """WRITES R2. Parquet (If-Match), then _flows.json (compare-and-swap), then the CSV. Refused unless the
    caller passed --apply; the cut-over refusal ran in main()."""
    from updater import blob
    r2 = blob.R2Blob(write=True)
    new_etag = r2.put_if(PREFIX + f"{flow}.parquet", new_bytes, if_match=etag_before)
    if new_etag is None:
        raise Refused(f"{flow}: the parquet changed on R2 since the plan read it - nothing written")
    if new_etag and new_etag != rep["planned_md5"]:
        raise Refused(f"{flow}: R2 returned ETag {new_etag}, not md5 {rep['planned_md5']} - stop and look")
    raw = r2.get(STATE_KEY)
    st = json.loads(raw)
    fe = st["flows"][flow]
    ps = rep["planned_state"]
    fe["served"] = ps["served"]
    if "residue" in ps:
        fe["residue"] = ps["residue"]
    else:
        fe["modified"] = None
    fe["repair"] = dict(ps["repair"], utc=dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat())
    st["written_utc"] = fe["repair"]["utc"]
    st["writer"] = "tools/repair_insee_spelling.py"
    body = json.dumps(st, indent=1, sort_keys=True).encode("utf-8")
    if r2.put_if(STATE_KEY, body, if_match=hashlib.md5(raw).hexdigest()) is None:
        raise Refused(f"{flow}: _flows.json changed between its read and its write - parquet written, state NOT; "
                      f"re-run the state step for {flow}")
    pinned = os.path.join(work, "pinned", "insee_melodi", f"{flow}.parquet")
    with open(pinned, "wb") as fh:
        fh.write(new_bytes)
    env = dict(os.environ, ECONDL_DATA=os.path.join(work, "pinned"), AQUEDUCT_BACKEND="r2")
    r = subprocess.run([sys.executable, os.path.join(catalog_repo, "core", "derive_csv.py"), "--bucket", BUCKET,
                        "--only", f"insee_melodi:{flow}"], cwd=catalog_repo, env=env)
    if r.returncode:
        raise Refused(f"{flow}: parquet and state written, CSV derive exited {r.returncode} - re-run it")
    print(f"{flow}: APPLIED (parquet {new_etag}, state, CSV)", flush=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        raise SystemExit(2)
