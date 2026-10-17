"""One-off repair of the 8 insee_melodi flows that run 39024352275 doubled (2026-10-17), after INSEE added a
constant GEO dimension (GEO=2025-FRANCE-F) to the DD_CNA keys. Decided by the coordinator, 2026-10-17:
INSEE's CURRENT spelling (with GEO) is canonical; the repair removes only rows the new spelling exactly
replaces, and deletes no row INSEE no longer serves. Reviewed as AR-333 (rebuilt after its NO-GO, R1594).

    python -B tools/repair_insee_spelling.py plan     --work W --mirror M --catalog-repo E [--r2-env E/.env]
    python -B tools/repair_insee_spelling.py apply    --work W --catalog-repo E [--r2-env ...]   (after review)
    python -B tools/repair_insee_spelling.py rollback --work W --catalog-repo E [--r2-env ...]   (undo an apply)

THE PLAN (`plan`, a dry run: only get_object/head_object can be called - the client is wrapped):
  SUPERSEDE (DD_CNA_APU, DD_CNA_BRANCHES - origin pulls; the new spelling covers every year):
      publish = the added rows (GEO spelling) + the old rows with NO exact counterpart (no added row whose key
      minus GEO=2025-FRANCE-F equals the old key, on the same date). Those old rows are series INSEE no longer
      serves, so they are not re-crawlable: KEPT, recorded as residue.
  REVERT (the 6 DD_CNA tails - only 2025 exists in the GEO spelling): publish the pre-run mirror's bytes,
      proved equal by row hash to the served rows minus the added ones. The removed rows (2025, GEO spelling)
      are re-crawlable: they move to the canonical spelling later, by a forced origin re-pull in supersede mode
      (F1b) once the fetcher's spelling hold (F1) has landed. insee_melodi stays paused until then.
  DS_ANTIPOL is NOT touched (its added rows are a new year, not a doubling).
  Preconditions, each a refusal:
  - _flows.json was last written by the run being repaired (or by this tool, for a resumed apply);
  - each flow's served ETag equals what _flows.json records;
  - the served non-GEO rows equal the mirror ROW FOR ROW (key, date AND value), and served = mirror + added;
  - every added key carries GEO=2025-FRANCE-F once, no old key has a GEO, and the mapping is one to one.
  `plan` writes under --work, and nowhere else:
  - plan/: the planned parquets;
  - rollback/: byte copies of the 8 served parquets, the 8 served CSVs (with their headers) and _flows.json;
  - the planned state entry of each flow: served identity and rows, partitions, s_norm and why recomputed
    by the fetcher's own _partition_profile, residue, csv_owed, and a `spelling_repair` record (NOT
    `repair`, which the fetcher reads as a flag);
  - a REAL derive of each CSV: python -m core.derive_csv --dry-run --source insee_melodi --only <ids file>,
    with ECONDL_DATA pinned to plan/;
  - plan_report.json.

`apply` (refused after T0, and unless the registry says insee_melodi `live: false`) works from that plan,
one flow fully before the next, and RESUMES: a step already done is recognised and skipped.
  1. the parquet: PUT with If-Match on the ETag the plan read (done when R2 already holds the planned md5);
  2. _flows.json: the planned entry, under compare-and-swap (If-Match = md5 of the bytes read). Done when the
     entry's served ETag is the planned md5 and its spelling_repair names it;
  3. the CSV: python -B -m core.derive_csv --bucket econ-data --source insee_melodi --only <ids file> in the
     catalogue checkout, with ECONDL_DATA pinned to a copy of exactly the bytes written (#156's md5 == ETag);
  4. the read-back: GET the parquet (rows == plan) and the CSV (data lines == plan rows), recorded in
     plan_report.json under "receipts"; then csv_owed is cleared under compare-and-swap.
  If step 3 fails, csv_owed (set in step 2) makes the fetcher's next pass hand the flow's CSV to the
  orchestrator.

`rollback` undoes an apply flow by flow:
  - the parquet goes back to the rollback bytes (If-Match on the repaired ETag);
  - the flow's _flows.json entry goes back to the rollback copy's (compare-and-swap);
  - the CSV goes back to the rollback bytes with their Content-Type, Content-Encoding and metadata
    (If-Match on its current ETag).
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
TOOL_WRITER = "tools/repair_insee_spelling.py"
GEO = "GEO=2025-FRANCE-F"
SUPERSEDE = ("DD_CNA_APU", "DD_CNA_BRANCHES")
REVERT = ("DD_CNA_AGREGATS", "DD_CNA_CONSO_MENAGES_COICOP", "DD_CNA_CONSO_MENAGES_PRODUITS", "DD_CNA_ECH_EXT",
          "DD_CNA_PATRIMOINE", "DD_CNA_REBS")
ALL = SUPERSEDE + REVERT
KEY_SQL = "regexp_replace(regexp_replace(series_key, '(^|:)GEO=2025-FRANCE-F', ''), '^:', '')"
PART_SQL = ("regexp_replace(series_key, '=[^:]*', '', 'g') || '|' || "
            "coalesce(regexp_extract(series_key, 'GEO=(\\d{4})-', 1), '')")


class Refused(Exception):
    pass


def md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def pkey(flow):
    return PREFIX + f"{flow}.parquet"


def csv_key(flow: str) -> str:
    return "series/" + urllib.parse.quote(f"insee_melodi:{flow}", safe="") + ".csv"


# --------------------------------------------------------------------------------------------- the plan
def _con(now_tbl, old_tbl):
    import duckdb
    con = duckdb.connect()
    con.register("now_t", now_tbl)
    con.register("old_t", old_tbl)
    con.execute("CREATE TABLE added AS SELECT n.* FROM now_t n ANTI JOIN old_t o "
                "ON n.series_key = o.series_key AND n.obs_date = o.obs_date")
    con.execute("CREATE TABLE back AS SELECT n.* FROM now_t n SEMI JOIN old_t o "
                "ON n.series_key = o.series_key AND n.obs_date = o.obs_date")
    return con


def _row_hash(con, rel: str) -> tuple:
    return tuple(con.execute(
        f"SELECT count(*), coalesce(sum(hash(flow, series_key, obs_date, value)::HUGEINT), 0), "
        f"coalesce(bit_xor(hash(flow, series_key, obs_date, value)), 0) FROM {rel}").fetchone())


def preconditions(con, n_now: int, n_old: int) -> dict:
    """mirror ⊆ served by (key, date); the served copy of the mirror's rows equals the mirror ROW FOR ROW
    (values too); served = mirror + added; the added rows carry the GEO spelling exactly."""
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
    h_back, h_old = _row_hash(con, "back"), _row_hash(con, "old_t")
    if h_back != h_old:
        raise Refused(f"the served rows the mirror holds differ from the mirror by value (row hash {h_back} != "
                      f"{h_old}): the mirror is stale")
    bad_new = con.execute(f"SELECT count(*) FROM added WHERE (length(series_key) - length(replace(series_key, "
                          f"'{GEO}', ''))) / {len(GEO)} != 1").fetchone()[0]
    geo_old = con.execute("SELECT count(*) FROM old_t WHERE series_key LIKE '%GEO=%'").fetchone()[0]
    many = con.execute(f"SELECT count(*) FROM (SELECT {KEY_SQL} k FROM (SELECT DISTINCT series_key FROM added) "
                       f"GROUP BY k HAVING count(*) > 1)").fetchone()[0]
    if bad_new or geo_old or many:
        raise Refused(f"the spelling rule is not exact: {bad_new:,} added keys without exactly one {GEO}, "
                      f"{geo_old:,} old keys with a GEO, {many:,} stripped keys shared by two new keys")
    return {"added": n_added, "row_hash": [str(x) for x in h_back]}


def plan_supersede(now_tbl, old_tbl):
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
           "old_rows": old_tbl.num_rows, "added_rows": pre["added"], "row_hash_mirror": pre["row_hash"],
           "dropped_old_rows": old_tbl.num_rows - kept_n, "kept_old_rows_residue": kept_n,
           "kept_old_series": con.execute("SELECT count(DISTINCT series_key) FROM kept").fetchone()[0],
           "dropped_rows_whose_value_the_new_pull_revised": revised, "residue": residue}
    con.close()
    return pub, rep


def plan_revert(now_tbl, old_tbl):
    con = _con(now_tbl, old_tbl)
    pre = preconditions(con, now_tbl.num_rows, old_tbl.num_rows)
    rep = {"action": "revert", "rows_before": now_tbl.num_rows, "rows_after": old_tbl.num_rows,
           "added_rows_removed": pre["added"], "row_hash_mirror": pre["row_hash"]}
    con.close()
    return old_tbl, rep


def maxyear_rows(tbl) -> tuple[str | None, int]:
    import pyarrow.compute as pc
    if tbl.num_rows == 0:
        return None, 0
    od = tbl.column("obs_date")
    mx = pc.max(od).as_py()
    return mx.isoformat(), int(pc.sum(pc.equal(pc.year(od), mx.year)).as_py() or 0)


def planned_entry(fe_now: dict, flow: str, rep: dict, prof: dict, new_md5: str, size: int, mx, my, today: str):
    """The flow's _flows.json entry after the repair, from its current entry. Never touches `repair` (the
    fetcher's flag); the record is `spelling_repair`."""
    fe = json.loads(json.dumps(fe_now))
    fe["served"] = {"identity": {"etag": new_md5, "size": size}, "rows": rep["rows_after"], "max_obs": mx,
                    "maxyear_rows": my}
    fe["partitions"], fe["s_norm"] = prof["partitions"], prof["s_norm"]
    if rep["action"] == "supersede":
        fe["residue"] = rep["residue"]
        fe["why"] = (f"complete per INSEE paging; spelling repair {today}: {rep['dropped_old_rows']:,} rows of the "
                     f"old spelling replaced by the {GEO} spelling; {rep['kept_old_rows_residue']:,} stored rows "
                     f"no longer served by INSEE kept as residue")
    else:
        fe["modified"] = None
        fe["why"] = (f"legacy; spelling repair {today}: reverted to the pre-run content; the {GEO} spelling "
                     f"of 2025 waits for a supersede re-pull")
    fe["csv_owed"] = mx or ""
    fe["spelling_repair"] = {"date": today, "of_run": RUN_WRITER, "action": rep["action"],
                             "rows_before": rep["rows_before"], "rows_after": rep["rows_after"],
                             "planned_md5": new_md5}
    return fe


# --------------------------------------------------------------------------------------------- stores
class ReadOnlyStore:
    """Only get_object and head_object are reachable (AR-333 condition 12)."""

    def __init__(self, client):
        self._c = client

    def get(self, key):
        o = self._c.get_object(Bucket=BUCKET, Key=key)
        meta = {"ContentType": o.get("ContentType"), "ContentEncoding": o.get("ContentEncoding"),
                "Metadata": o.get("Metadata") or {}}
        return o["Body"].read(), o["ETag"].strip('"'), meta

    def etag(self, key):
        try:
            return self._c.head_object(Bucket=BUCKET, Key=key)["ETag"].strip('"')
        except Exception as e:                                           # noqa: BLE001
            if "404" in str(e) or "Not Found" in str(e):
                return None
            raise


class WriteStore(ReadOnlyStore):
    def put_if(self, key, data, if_match, meta=None):
        """ONE conditional PUT (R2 evaluates If-Match atomically). The new ETag, or None when refused."""
        from botocore.exceptions import ClientError
        kw = {"IfMatch": '"' + if_match.strip('"') + '"'}
        for k in ("ContentType", "ContentEncoding"):
            if meta and meta.get(k):
                kw[k] = meta[k]
        if meta and meta.get("Metadata"):
            kw["Metadata"] = meta["Metadata"]
        try:
            r = self._c.put_object(Bucket=BUCKET, Key=key, Body=data, **kw)
        except ClientError as e:
            code = (e.response or {}).get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "412") or (e.response or {}).get("ResponseMetadata", {}).get(
                    "HTTPStatusCode") == 412:
                return None
            raise
        return (r.get("ETag") or "").strip('"')


def _r2(write: bool):
    from core import r2_util
    e = r2_util.load_env()
    pair = "WRITE" if write or not all(r2_util._real(e.get(k)) for k in (
        "R2_READ_ENDPOINT", "R2_READ_ACCESS_KEY_ID", "R2_READ_SECRET_ACCESS_KEY")) else "READ"
    print(f"R2 authenticates with the {pair} key pair"
          + ("" if write else "; this phase can only call get_object/head_object"), flush=True)
    return (WriteStore if write else ReadOnlyStore)(r2_util.client(write=write))


# --------------------------------------------------------------------------------------------- derive
def _ids_file(work, flow):
    p = os.path.join(work, "ids", f"{flow}.txt")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        fh.write(f"insee_melodi:{flow}\n")
    return p


def run_derive(catalog_repo, data_root, ids_file, *, dry: bool) -> str:
    """core.derive_csv as a MODULE, scoped to the source (no statcan writer lock, no other ids), -B (no
    bytecode written into the checkout)."""
    cmd = [sys.executable, "-B", "-m", "core.derive_csv", "--source", "insee_melodi", "--only", ids_file]
    cmd += ["--dry-run"] if dry else ["--bucket", BUCKET]
    env = dict(os.environ, ECONDL_DATA=data_root, PYTHONDONTWRITEBYTECODE="1")
    if not dry:
        env["AQUEDUCT_BACKEND"] = "r2"
    r = subprocess.run(cmd, cwd=catalog_repo, env=env, capture_output=True, text=True)
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode:
        raise Refused(f"derive_csv exited {r.returncode}: {out.strip()[-400:]}")
    return out


def planned_csv(catalog_repo: str, plan_root: str, flow: str) -> dict:
    os.environ["ECONDL_DATA"] = plan_root
    if catalog_repo not in sys.path:
        sys.path.insert(0, catalog_repo)
        sys.path.insert(0, os.path.join(catalog_repo, "clients", "python"))
    from core import derive_csv                                      # noqa: PLC0415
    body = derive_csv._series_csv_bytes(f"insee_melodi:{flow}")
    return {"key": csv_key(flow), "csv_bytes": len(body), "data_lines": body.count(b"\n") - 1, "md5": md5(body)}


def csv_lines(body: bytes) -> int:
    raw = gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body
    return raw.count(b"\n") - 1


# --------------------------------------------------------------------------------------------- phases
def _registry_live(catalog_repo) -> bool:
    import yaml
    with open(os.path.join(catalog_repo, "updater", "registry.yaml"), encoding="utf-8") as fh:
        reg = yaml.safe_load(fh)
    entries = reg.get("sources", reg) if isinstance(reg, dict) else reg
    e = next(x for x in entries if x.get("source_id") == "insee_melodi")
    return bool(e.get("live"))


def plan(store, work, mirror, catalog_repo, flows, *, today=None, derive=run_derive) -> dict:
    import pyarrow.parquet as pq
    from updater.strategies.fetchers import insee_melodi as IM
    today = today or dt.date.today().isoformat()
    state_bytes, state_etag, _ = store.get(STATE_KEY)
    state = json.loads(state_bytes)
    if state.get("writer") != RUN_WRITER:
        raise Refused(f"_flows.json was last written by {state.get('writer')!r}, not {RUN_WRITER}: re-plan only "
                      f"against the state of the run being repaired")
    for d in ("plan/insee_melodi", "rollback/csv", "pinned/insee_melodi", "ids"):
        os.makedirs(os.path.join(work, d), exist_ok=True)
    with open(os.path.join(work, "rollback", "_flows.json"), "wb") as fh:
        fh.write(state_bytes)
    report = {"phase": "plan", "utc": dt.datetime.now(dt.timezone.utc).isoformat(), "state_etag": state_etag,
              "state_written_utc": state.get("written_utc"), "registry_live": _registry_live(catalog_repo),
              "flows": {}}
    for flow in flows:
        fe = state["flows"].get(flow) or {}
        body, etag, _ = store.get(pkey(flow))
        want = ((fe.get("served") or {}).get("identity") or {}).get("etag")
        if want != etag:
            raise Refused(f"{flow}: R2's ETag {etag} is not the one _flows.json records ({want})")
        with open(os.path.join(work, "rollback", f"{flow}.parquet"), "wb") as fh:
            fh.write(body)
        cbody, cetag, cmeta = store.get(csv_key(flow))
        with open(os.path.join(work, "rollback", "csv", f"{flow}.csv.bytes"), "wb") as fh:
            fh.write(cbody)
        with open(os.path.join(work, "rollback", "csv", f"{flow}.meta.json"), "w", encoding="utf-8") as fh:
            json.dump({"etag": cetag, **cmeta}, fh, indent=1)
        now_tbl = pq.read_table(io.BytesIO(body))
        mirror_path = os.path.join(mirror, f"{flow}.parquet")
        old_tbl = pq.read_table(mirror_path)
        out = os.path.join(work, "plan", "insee_melodi", f"{flow}.parquet")
        if flow in SUPERSEDE:
            pub, rep = plan_supersede(now_tbl, old_tbl)
            pq.write_table(pub.cast(now_tbl.schema), out, compression="zstd")
        else:
            pub, rep = plan_revert(now_tbl, old_tbl)
            shutil.copyfile(mirror_path, out)
        with open(out, "rb") as fh:
            new_bytes = fh.read()
        mx, my = maxyear_rows(pub)
        prof = IM._partition_profile(out)
        if prof["rows"] != rep["rows_after"]:
            raise Refused(f"{flow}: the planned file holds {prof['rows']:,} rows, the plan says {rep['rows_after']:,}")
        rep.update(r2_etag_before=etag, planned_md5=md5(new_bytes), planned_bytes=len(new_bytes), max_obs=mx,
                   maxyear_rows=my, served_csv={"key": csv_key(flow), "etag": cetag, "bytes": len(cbody),
                                                "data_lines": csv_lines(cbody)})
        rep["planned_csv"] = planned_csv(catalog_repo, os.path.join(work, "plan"), flow)
        if rep["planned_csv"]["data_lines"] != rep["rows_after"]:
            raise Refused(f"{flow}: the planned CSV has {rep['planned_csv']['data_lines']:,} data lines, the plan "
                          f"{rep['rows_after']:,} rows")
        dout = derive(catalog_repo, os.path.join(work, "plan"), _ids_file(work, flow), dry=True)
        if "derived 1, unresolvable 0" not in dout:
            raise Refused(f"{flow}: the derive_csv dry run did not derive the id: {dout.strip()[-300:]}")
        rep["derive_dry_run"] = [ln for ln in dout.splitlines() if "DRY RUN" in ln or "--only" in ln]
        rep["planned_entry"] = planned_entry(fe, flow, rep, prof, rep["planned_md5"], len(new_bytes), mx, my, today)
        report["flows"][flow] = rep
        print(f"{flow:30s} {rep['action']:9s} rows {rep['rows_before']:>9,} -> {rep['rows_after']:>9,}"
              + (f"  dropped {rep['dropped_old_rows']:,} old (of which {rep['dropped_rows_whose_value_the_new_pull_revised']:,} "
                 f"revised by the new pull), kept {rep['kept_old_rows_residue']:,} old rows in "
                 f"{rep['kept_old_series']:,} series as residue" if flow in SUPERSEDE else
                 f"  removed {rep['added_rows_removed']:,} added rows; = mirror by row hash")
              + f" | CSV {rep['served_csv']['data_lines']:,} -> {rep['planned_csv']['data_lines']:,} rows "
                f"(derive_csv dry run: derived 1)", flush=True)
    _save_report(work, report)
    return report


def _save_report(work, report):
    with open(os.path.join(work, "plan_report.json"), "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=1, default=str)


def _load_report(work):
    with open(os.path.join(work, "plan_report.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _state_cas(store, mutate) -> bool:
    """GET _flows.json, mutate(state) -> True to write, PUT under If-Match = md5 of the bytes read."""
    raw, _, meta = store.get(STATE_KEY)
    st = json.loads(raw)
    if st.get("writer") not in (RUN_WRITER, TOOL_WRITER):
        raise Refused(f"_flows.json was last written by {st.get('writer')!r} - someone else ran; re-plan")
    if not mutate(st):
        return False
    st["writer"] = TOOL_WRITER
    st["written_utc"] = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    body = json.dumps(st, indent=1, sort_keys=True).encode("utf-8")
    if store.put_if(STATE_KEY, body, md5(raw), {"ContentType": "application/json"}) is None:
        raise Refused("_flows.json changed between its read and its write - re-run (the step resumes)")
    return True


def _hook(step, flow):                    # tests kill the run here
    pass


def apply(store, work, catalog_repo, flows, *, derive=run_derive, hook=_hook) -> dict:
    report = _load_report(work)
    report.setdefault("receipts", {})
    for flow in flows:
        rep = report["flows"][flow]
        plan_path = os.path.join(work, "plan", "insee_melodi", f"{flow}.parquet")
        with open(plan_path, "rb") as fh:
            new_bytes = fh.read()
        with open(os.path.join(work, "rollback", f"{flow}.parquet"), "rb") as fh:
            if md5(fh.read()) != rep["r2_etag_before"]:
                raise Refused(f"{flow}: the rollback copy is not the object the plan read")
        if md5(new_bytes) != rep["planned_md5"]:
            raise Refused(f"{flow}: the plan file changed since the plan")
        steps = []
        # 1. parquet
        e = store.etag(pkey(flow))
        if e == rep["r2_etag_before"]:
            got = store.put_if(pkey(flow), new_bytes, e, {"ContentType": "application/vnd.apache.parquet"})
            if got is None:
                raise Refused(f"{flow}: the parquet changed on R2 since the plan read it - nothing written")
            if got and got != rep["planned_md5"]:
                raise Refused(f"{flow}: R2 returned ETag {got}, not md5 {rep['planned_md5']} - stop and look")
            steps.append("parquet written")
        elif e == rep["planned_md5"]:
            steps.append("parquet already written")
        else:
            raise Refused(f"{flow}: R2 holds ETag {e}, neither the plan's before nor its after - re-plan")
        hook("parquet", flow)

        # 2. state
        def put_entry(st, flow=flow, rep=rep):
            fe = st["flows"].get(flow) or {}
            cur = ((fe.get("served") or {}).get("identity") or {}).get("etag")
            if cur == rep["planned_md5"] and (fe.get("spelling_repair") or {}).get("planned_md5") == rep["planned_md5"]:
                return False
            if cur != rep["r2_etag_before"]:
                raise Refused(f"{flow}: _flows.json records served ETag {cur}, not the plan's before - re-plan")
            st["flows"][flow] = rep["planned_entry"]
            return True
        steps.append("state written" if _state_cas(store, put_entry) else "state already written")
        hook("state", flow)

        # 3. CSV, from a pinned copy of exactly the bytes on R2
        pinned = os.path.join(work, "pinned", "insee_melodi", f"{flow}.parquet")
        with open(pinned, "wb") as fh:
            fh.write(new_bytes)
        derive(catalog_repo, os.path.join(work, "pinned"), _ids_file(work, flow), dry=False)
        steps.append("CSV derived")
        hook("csv", flow)

        # 4. read back, then clear csv_owed
        import pyarrow.parquet as pq
        pbody, petag, _ = store.get(pkey(flow))
        prow = pq.read_metadata(pa_buffer(pbody)).num_rows
        cbody, cetag, _ = store.get(csv_key(flow))
        clines = csv_lines(cbody)
        if petag != rep["planned_md5"] or prow != rep["rows_after"] or clines != rep["rows_after"]:
            raise Refused(f"{flow}: read-back differs - parquet {petag}/{prow:,} rows, CSV {clines:,} lines, "
                          f"plan {rep['planned_md5']}/{rep['rows_after']:,}")

        def clear_owed(st, flow=flow):
            fe = st["flows"].get(flow) or {}
            if "csv_owed" not in fe:
                return False
            fe.pop("csv_owed")
            return True
        _state_cas(store, clear_owed)
        report["receipts"][flow] = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "steps": steps,
                                    "parquet_etag": petag, "parquet_rows": prow, "csv_etag": cetag,
                                    "csv_data_lines": clines}
        _save_report(work, report)
        print(f"{flow}: APPLIED - {', '.join(steps)}; read back parquet {prow:,} rows, CSV {clines:,} rows",
              flush=True)
    return report


def pa_buffer(b):
    import pyarrow as pa
    return pa.BufferReader(b)


def rollback(store, work, flows) -> None:
    report = _load_report(work)
    with open(os.path.join(work, "rollback", "_flows.json"), "rb") as fh:
        before_state = json.loads(fh.read())
    for flow in flows:
        rep = report["flows"][flow]
        with open(os.path.join(work, "rollback", f"{flow}.parquet"), "rb") as fh:
            old = fh.read()
        e = store.etag(pkey(flow))
        if e == rep["planned_md5"]:
            if store.put_if(pkey(flow), old, e, {"ContentType": "application/vnd.apache.parquet"}) is None:
                raise Refused(f"{flow}: the parquet changed during the rollback")
        elif e != rep["r2_etag_before"]:
            raise Refused(f"{flow}: R2 holds {e}, neither the repaired nor the original object - stop and look")
        _state_cas(store, lambda st, flow=flow: st["flows"].__setitem__(flow, before_state["flows"][flow]) or True)
        with open(os.path.join(work, "rollback", "csv", f"{flow}.csv.bytes"), "rb") as fh:
            cold = fh.read()
        with open(os.path.join(work, "rollback", "csv", f"{flow}.meta.json"), encoding="utf-8") as fh:
            cmeta = json.load(fh)
        ce = store.etag(csv_key(flow))
        if ce != cmeta["etag"]:
            if store.put_if(csv_key(flow), cold, ce, cmeta) is None:
                raise Refused(f"{flow}: the CSV changed during the rollback")
        print(f"{flow}: ROLLED BACK (parquet, state entry, CSV)", flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("phase", choices=("plan", "apply", "rollback"))
    ap.add_argument("--work", required=True)
    ap.add_argument("--mirror", help="the pre-run mirror of clean_full/insee_melodi (plan only)")
    ap.add_argument("--catalog-repo", required=True, help="the checkout with data/catalog.db and the registry")
    ap.add_argument("--flows", nargs="*", default=list(ALL))
    ap.add_argument("--r2-env", help="a .env to take the R2 keys from (into this process only, never printed)")
    a = ap.parse_args(argv)
    work = os.path.abspath(a.work)
    if os.path.splitdrive(work)[0].upper() == "D:" or work.startswith(ROOT):
        raise Refused(f"--work {work} must not be on D: or inside the checkout")
    for f in a.flows:
        if f not in ALL:
            raise Refused(f"{f} is not one of the 8 flows this repair covers (DS_ANTIPOL is untouched)")
    if a.r2_env:
        with open(a.r2_env, encoding="utf-8") as fh:
            for line in fh:
                k, sep, v = line.strip().partition("=")
                if sep and k.strip().startswith(("R2_READ_", "R2_WRITE_")) and not os.environ.get(k.strip()):
                    os.environ[k.strip()] = v.strip().strip('"').strip("'")
    os.environ.setdefault("ECONDL_DUCKDB_TMP", os.path.join(work, "spill"))
    if a.phase == "plan":
        if not a.mirror:
            raise Refused("plan needs --mirror")
        plan(_r2(write=False), work, a.mirror, a.catalog_repo, a.flows)
        print(f"\nPLAN (dry run): {len(a.flows)} flow(s); {os.path.join(work, 'plan_report.json')}; nothing was "
              f"written to R2")
        return 0
    from core import cutover
    cutover.refuse_if_cut_over(f"the insee_melodi spelling repair ({a.phase} writes the cloud copy)")
    if _registry_live(a.catalog_repo):
        raise Refused("insee_melodi is live in the registry: pause it (live: false) before writing")
    if a.phase == "apply":
        apply(_r2(write=True), work, a.catalog_repo, a.flows)
    else:
        rollback(_r2(write=True), work, a.flows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Refused as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        raise SystemExit(2)
