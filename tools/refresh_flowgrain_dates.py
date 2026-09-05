"""tools/refresh_flowgrain_dates.py — re-derive start_date/end_date for a flow-grain PxWeb
source's catalogue rows from the store it is SERVED from, and push the corrected rows to D1.

WHY (2026-09-05, hagstofa). The flow-grain cataloguer (tools/catalog_pxweb_flowgrain.py)
writes each table's real min/max obs_date ONCE, at catalogue time. Nothing refreshes them
afterwards: the daily merge advances the parquet, core/sync_catalog_d1.py upserts whatever the
LOCAL catalog.db holds, and core/backfill_coverage.py touches only rows whose dates are both
NULL. So the coverage a user sees freezes at catalogue time — and when a table is REPAIRED (a
re-pull that removes fabricated years) the stale metadata keeps advertising the fabrication.
Measured on hagstofa on 2026-09-05 — catalogue side = the 2026-08-16 snapshot
D:/temp/claude/catalog_snapshot_20260816.db (the live db was under the crawlers' lock), store
side = the true per-table min/max over all five R2 parquets (9,460,453 rows, 1,551 prefixes;
receipt D:/temp/claude/hagstofa_date_class.json): 946 rows exact; 3 advertised end_date
3004/3005 (PxWeb normal-period sentinels the old parser read as years; the store has been
clean since the re-pull — `audit_impossible_dates.py --r2 --source hagstofa` reports 0); 104 an
end_date OLDER than the store's; 22 a start_date EARLIER than the store's (the R394 census
tables still dated 2000); 5 a start_date LATER than the store's. 122 distinct ids.

WHAT THE CLAIM IS, AND IS NOT. After --apply the catalogue dates EQUAL THE SERVED STORE. That
is not "correct": the store itself disagrees with the publisher's own title year-ranges on
~80 hagstofa tables (a 1900 floor on MAN00000/00101/00102/08000/05201-05204/05210/05302 and
UTA06105 where the publisher's Ár axis reaches 1703; MAN05301 title/table mismatch; MAN02007
store 2021..2100 n=35). Those are a separate full re-pull + title check, not a date edit.

UNFREEZE HAZARD (reviewer, 2026-09-05). CI's catalogue sync (`CATALOG_SYNC_ENABLED`, frozen,
R542) reads the R2 coherence copy `_aqueduct/catalog.db.zst` (LastModified 2026-08-27 15:30Z),
which still carries the old dates. Refresh that copy (`tools/refresh_r2_catalog.py`, Ahmed —
R250) BEFORE unfreezing, or the next re-derive of these tables re-upserts 3004-12-31 into D1.

WHAT IT DOES
  1. Store truth: every parquet of the source, read through updater.blob under
     AQUEDUCT_BACKEND=r2 (the served store; the local tree is a scratch mirror of the last
     run, R296/R36 — the tool REFUSES any other backend), grouped by table prefix with the
     cataloguer's OWN PREFIX_RE and its empty-capture guard — one definition, imported, never
     re-typed (R66).
  2. Catalogue state: the source's rows from LOCAL catalog.db (busy_timeout — crawlers write
     to it) and from D1 by primary key in chunked IN-lists (an index seek, never a scan;
     `wrangler d1 execute --file` returns only a summary, so reads go through --command in
     chunks small enough for the Windows command line).
  3. Diff: rows whose (start_date, end_date) differ from the store truth, per store.
     Catalogued tables with NO store rows are reported and left alone (never NULLed).
  4. --apply: UPDATE by series_id in local catalog.db, then the same UPDATEs in D1 as ONE
     --file batch (every statement a PK seek). Never inserts, deletes, or touches title/FTS
     (series_fts is fts5(series_id UNINDEXED, title, geography) — it carries no dates).
  5. Verify: re-read D1 by PK for every changed id and GET the LIVE metadata.json for each of
     them plus one untouched control; exit 1 on any D1 mismatch, list any live mismatch.
A JSON receipt is written under D:/temp/claude/ (path printed).

  python tools/refresh_flowgrain_dates.py --source hagstofa           # dry run: plan only
  python tools/refresh_flowgrain_dates.py --source hagstofa --apply
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
import importlib.util
import io
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import pyarrow as pa            # noqa: E402
import pyarrow.compute as pc    # noqa: E402
import pyarrow.parquet as pq    # noqa: E402

from updater import blob, config  # noqa: E402

API = os.environ.get("ECONDL_API", "https://econdl-api.elkassabgi.workers.dev")
UA = {"User-Agent": "Mozilla/5.0 econdl-refresh-flowgrain-dates"}
CATALOG = os.path.join(ROOT, "data", "catalog.db")
WRANGLER = os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler.cmd")
WORKER_DIR = os.path.join(ROOT, "api", "worker")
D1_DB = "econ-catalog"
CHUNK = 40              # ids per --command IN-list: ~3 KB of SQL, well under cmd.exe's 8,191
RECEIPT_DIR = "D:/temp/claude"


def _load_cataloguer():
    """Import tools/catalog_pxweb_flowgrain.py by path for its PREFIX_RE and SOURCES."""
    p = os.path.join(ROOT, "tools", "catalog_pxweb_flowgrain.py")
    spec = importlib.util.spec_from_file_location("_catalog_pxweb_flowgrain", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def store_truth(src: str) -> tuple[dict, list]:
    """{table_prefix: (min_iso, max_iso, rows)} from the served store; plus the file list."""
    cat = _load_cataloguer()
    d = config.source_dir(src)
    files = sorted(f for f in blob.list_parquets(d) if not os.path.basename(f).startswith("_"))
    if not files:
        raise SystemExit(f"{src}: no parquet files under {d} (backend={config.BACKEND}) — refusing")
    parts = []
    for f in files:
        path = os.path.join(d, os.path.basename(f))
        t = pq.read_table(io.BytesIO(blob.read_bytes(path)), columns=["series_key", "obs_date"])
        keys = t["series_key"].combine_chunks()          # extract_regex().field() needs one array
        p = pc.extract_regex(keys, pattern=cat.PREFIX_RE).field("p")
        # Same guard as the cataloguer (its lines 91-96): an EMPTY (not null) capture means a
        # time-only table whose key IS the prefix — fall back to the whole key.
        usable = pc.and_(pc.is_valid(p), pc.not_equal(p, ""))
        pref = pc.if_else(usable, p, keys)
        parts.append(pa.table({"table": pref, "obs_date": t["obs_date"]}))
        print(f"  store file {os.path.basename(f):24s} rows={t.num_rows:>10,}")
    allt = pa.concat_tables(parts)
    g = allt.group_by("table").aggregate([("obs_date", "min"), ("obs_date", "max"), ("obs_date", "count")])
    truth = {r["table"]: (str(r["obs_date_min"]), str(r["obs_date_max"]), int(r["obs_date_count"]))
             for r in g.to_pylist()}
    return truth, [os.path.basename(f) for f in files]


def local_rows(src: str) -> dict:
    con = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    con.execute("PRAGMA busy_timeout=180000")
    # PK RANGE, never `WHERE source_id=?`: `series` carries only its primary-key index, so a
    # source_id predicate is a SCAN of the whole 13.5M-row live catalogue under the crawlers'
    # write lock (R715, R721 — this tool's first dry run sat in exactly that scan). The range
    # `>= 'src:' AND < 'src;'` (';' is the byte after ':') is SEARCH … USING INDEX, ~0 s.
    rows = con.execute("SELECT series_id, start_date, end_date FROM series "
                       "WHERE series_id >= ? AND series_id < ?", (f"{src}:", f"{src};")).fetchall()
    con.close()
    return {sid: (sd, ed) for sid, sd, ed in rows}


def _wrangler_json(args: list[str], timeout: int = 600):
    r = subprocess.run([WRANGLER, "d1", "execute", D1_DB, "--remote", "--json", *args],
                       cwd=WORKER_DIR, capture_output=True, text=True, encoding="utf-8",
                       errors="replace", timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"wrangler rc={r.returncode}: {r.stderr[-1500:]} {r.stdout[-800:]}")
    lines = r.stdout.splitlines()
    start = next((i for i, ln in enumerate(lines) if ln.strip() == "["), None)
    if start is None:
        raise RuntimeError(f"no JSON array in wrangler output: {r.stdout[-800:]}")
    return json.loads("\n".join(lines[start:]))


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def d1_rows(ids: list[str]) -> tuple[dict, int]:
    """{series_id: (start, end)} for the given ids, by primary key, chunked. Returns rows_read too."""
    out, rows_read = {}, 0
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        sql = ("SELECT series_id, start_date, end_date FROM series WHERE series_id IN ("
               + ",".join(_q(s) for s in chunk) + ")")
        res = _wrangler_json(["--command", sql])
        for entry in res:
            rows_read += int((entry.get("meta") or {}).get("rows_read") or 0)
            for row in entry.get("results") or []:
                if "series_id" in row:
                    out[row["series_id"]] = (row.get("start_date"), row.get("end_date"))
    return out, rows_read


def live_meta(sid: str) -> tuple[int, str | None, str | None]:
    url = f"{API}/v1/series/{urllib.parse.quote(sid, safe='')}.metadata.json?v={int(time.time())}"
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=60) as f:
            m = json.loads(f.read())
            return f.status, m.get("start_date"), m.get("end_date")
    except urllib.error.HTTPError as e:
        return e.code, None, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--dry-run", action="store_true", help="measure and print the plan; write nothing")
    ap.add_argument("--apply", action="store_true", help="write local + D1, then verify")
    a = ap.parse_args()
    # Exactly one mode, spelled out in ARGV (R323: a run's ARGV, not its printout, says what it did).
    if a.dry_run == a.apply:
        raise SystemExit("pass exactly one of --dry-run / --apply")
    src = a.source
    cat = _load_cataloguer()
    if src not in cat.SOURCES:
        raise SystemExit(f"{src} is not a flow-grain PxWeb source ({cat.SOURCES}) — refusing")
    if config.BACKEND != "r2":
        raise SystemExit(f"AQUEDUCT_BACKEND resolved to {config.BACKEND!r}; this tool reads the SERVED store — set AQUEDUCT_BACKEND=r2")
    print(f"backend={config.BACKEND} source={src} mode={'APPLY' if a.apply else 'DRY-RUN'}")

    truth, files = store_truth(src)
    print(f"store: {len(files)} files, {len(truth):,} table prefixes")
    loc = local_rows(src)
    print(f"local catalog.db: {len(loc):,} rows for {src}")
    ids = sorted(loc)
    d1, rr = d1_rows(ids)
    print(f"D1: {len(d1):,} rows returned for {len(ids):,} ids (rows_read={rr:,})")
    # R338: an absence check is void without a present control. These ids are SERVED (the
    # worker answers metadata.json for them from D1), so zero rows back means the READ is
    # broken (wrong output shape, wrong database), never that the catalogue is empty.
    if ids and not d1:
        raise SystemExit("D1 read returned 0 rows for a non-empty served id list — instrument broken, refusing to plan")

    plan, nostore, exact_local, exact_d1 = [], [], 0, 0
    for sid in ids:
        key = sid.split(f"{src}:", 1)[1] if sid.startswith(f"{src}:") else sid
        if key not in truth:
            nostore.append(sid)
            continue
        mn, mx, n = truth[key]
        lsd, led = loc[sid]
        dsd, ded = d1.get(sid, (None, None))
        need_local = (lsd, led) != (mn, mx)
        need_d1 = sid in d1 and (dsd, ded) != (mn, mx)
        exact_local += (not need_local)
        exact_d1 += (sid in d1 and not need_d1)
        if need_local or need_d1:
            plan.append({"series_id": sid, "truth": [mn, mx, n], "local": [lsd, led],
                         "d1": [dsd, ded] if sid in d1 else None,
                         "need_local": need_local, "need_d1": need_d1})
    missing_d1 = [s for s in ids if s not in d1]
    print(f"already exact — local: {exact_local:,}   D1: {exact_d1:,}")
    print(f"catalogued but no store rows (left alone): {len(nostore)}")
    for s in nostore[:10]:
        print("   ", s)
    print(f"catalogued locally but NOT in D1 (reported, not touched): {len(missing_d1)}")
    for s in missing_d1[:10]:
        print("   ", s)
    print(f"PLAN: {len(plan)} row(s) to correct  (local {sum(p['need_local'] for p in plan)}, D1 {sum(p['need_d1'] for p in plan)})")
    by_kind = {"end_ahead": 0, "end_behind": 0, "start_before": 0, "start_after": 0}
    for p in plan:
        mn, mx, _ = p["truth"]; sd, ed = p["local"]
        if ed and ed > mx: by_kind["end_ahead"] += 1
        if ed and ed < mx: by_kind["end_behind"] += 1
        if sd and sd < mn: by_kind["start_before"] += 1
        if sd and sd > mn: by_kind["start_after"] += 1
    print("  by kind (local vs truth):", by_kind)
    for p in sorted(plan, key=lambda p: p["local"][1] or "", reverse=True)[:12]:
        print(f"   {p['series_id'].split(':')[-1]:16s} local={p['local']} d1={p['d1']} -> truth={p['truth'][:2]}")

    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    receipt = {"source": src, "mode": "apply" if a.apply else "dry-run", "utc": stamp,
               "backend": config.BACKEND, "store_files": files, "tables_in_store": len(truth),
               "local_rows": len(loc), "d1_rows": len(d1), "d1_rows_read_reads": rr,
               "no_store_rows": nostore, "missing_in_d1": missing_d1, "by_kind": by_kind, "plan": plan}
    rpath = os.path.join(RECEIPT_DIR, f"refresh_flowgrain_dates_{src}_{stamp}.json")

    if not a.apply:
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
        print(f"dry run — nothing written. receipt: {rpath}")
        return 0
    if not plan:
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
        print(f"nothing to correct. receipt: {rpath}")
        return 0

    # ---- local ----
    todo_local = [(p["truth"][0], p["truth"][1], p["series_id"]) for p in plan if p["need_local"]]
    con = sqlite3.connect(CATALOG, timeout=180)
    con.execute("PRAGMA busy_timeout=180000")
    cur = con.executemany("UPDATE series SET start_date=?, end_date=? WHERE series_id=?", todo_local)
    con.commit()
    n_local = cur.rowcount
    con.close()
    print(f"local: UPDATE applied to {n_local} row(s) (planned {len(todo_local)})")
    receipt["local_updated"] = n_local

    # ---- D1: one --file batch, every statement a PK seek ----
    todo_d1 = [p for p in plan if p["need_d1"]]
    sqlp = os.path.join(RECEIPT_DIR, f"refresh_flowgrain_dates_{src}_{stamp}.sql")
    with open(sqlp, "w", encoding="utf-8") as fh:
        for p in todo_d1:
            mn, mx, _ = p["truth"]
            fh.write(f"UPDATE series SET start_date={_q(mn)}, end_date={_q(mx)} WHERE series_id={_q(p['series_id'])};\n")
    res = _wrangler_json(["--file", sqlp])
    # Sum over EVERY entry of the batch, not res[0] alone (reviewer change 5): a --file batch
    # may answer one entry per statement, and reading only the first would report one change.
    changes = sum(int((e.get("meta") or {}).get("changes") or 0) for e in res)
    summ = [e.get("results") for e in res][:3]
    print(f"D1: batch of {len(todo_d1)} UPDATE(s): meta.changes summed = {changes}; first entries: {summ}")
    if changes != len(todo_d1):
        print(f"   WARNING: D1 reported {changes} change(s) for {len(todo_d1)} statement(s) — the PK verify below decides")
    receipt["d1_batch_changes"] = changes
    receipt["d1_batch_entries"] = len(res)

    # ---- verify: D1 by PK, then the live surface ----
    changed = [p["series_id"] for p in todo_d1]
    after, rr2 = d1_rows(changed)
    bad = [(s, after.get(s)) for s in changed if after.get(s) != tuple(truth[s.split(f'{src}:', 1)[1]][:2])]
    print(f"verify D1: {len(changed) - len(bad)}/{len(changed)} rows now equal the store truth (rows_read={rr2:,})")
    for s, v in bad[:10]:
        print("   MISMATCH", s, v)
    control = next((s for s in ids if s in d1 and s not in changed and s.split(f'{src}:', 1)[1] in truth), None)
    probe = changed + ([control] if control else [])
    with cf.ThreadPoolExecutor(8) as ex:
        live = dict(zip(probe, ex.map(live_meta, probe)))
    live_bad = [(s, live[s]) for s in changed if (live[s][1], live[s][2]) != tuple(truth[s.split(f'{src}:', 1)[1]][:2])]
    print(f"verify LIVE metadata.json: {len(changed) - len(live_bad)}/{len(changed)} equal the store truth; "
          f"control {control.split(':')[-1] if control else None} -> {live.get(control)} (expected {d1.get(control)})")
    for s, v in live_bad[:10]:
        print("   LIVE MISMATCH", s, v)
    receipt.update({"d1_verify_bad": bad, "live_verify_bad": live_bad, "control": control,
                    "control_live": live.get(control), "control_expected": d1.get(control)})
    json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
    print(f"receipt: {rpath}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
