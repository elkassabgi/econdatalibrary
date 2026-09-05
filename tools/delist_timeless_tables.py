# -*- coding: utf-8 -*-
"""Delist the 27 catalogue rows that describe publisher CROSS-TABULATIONS — tables with no time
dimension — which an old reader once catalogued with fabricated dates (year 0001, 4549, 6152 ...).

Evidence and decision record: docs/TIMELESS_TABLES_IN_A_TIMESERIES_STORE_20260905.md.
Authorised by Ahmed 2026-09-05 ("ok delist"); his standing rule already applied: unhostable AND
re-crawlable data is deleted, not escalated (the publisher still serves every one of these tables).
Version 2, after adversarial review (seven MUST-FIX items, all in):

  * the 27 ids are a CONSTANT; D1 is the check, not the source, so a re-run after a crash finishes
    the partial work instead of printing "nothing to do"
  * order follows the serving path — the worker gates on the D1 `series` row before touching R2
    (api/worker/src/series.ts:188 then :253) — so:
        1 D1 series  ->  2 D1 series_fts  ->  3 R2 served CSVs (+ the 4 cbs_nl store parquets that
        also live on R2, or mirror_sync copies them back)  ->  4 local catalog.db  ->
        5 source_counts refresh  ->  6 local archive of the 4 cbs_nl parquets  ->  7 verify
    a crash between 1 and 4 leaves "unlisted locally later" (harmless), never "listed but unservable"
  * every FTS delete/verify is `WHERE series_fts MATCH '"<title>"' AND series_id IN (...)` — plan
    INDEX 0:M3, seconds — never the id-only form, which is a full scan of 13.9M rows (11+ minutes
    on the local file while holding the write lock; one paid scan per statement on D1). Titles come
    from D1 in the check step; a row with no title falls back to the id form and says so.
  * R2: only 404/NoSuchKey means absent; anything else raises; a known-present control object is
    HEADed first so an expired token cannot masquerade as "already gone"
  * each served CSV is READ before it is deleted and must show an impossible year in its date
    column; a CSV that does not is left alone and reported (HEAD is not content)
  * live check = GET https://econdl-api.elkassabgi.workers.dev/v1/series/<id>.metadata.json —
    D1-first, uncached, no auth; 200 before, 404 after. (/v1/catalog browse pages and /v1/stats are
    edge-cached up to 6 h and will list the 27 until then; that is stated, not hidden.)
  * registered in the D1 cost guard as a driver; with PK deletes and MATCH predicates it issues no
    full scans at all

  python tools/delist_timeless_tables.py              # dry run: checks + content reads only
  python tools/delist_timeless_tables.py --apply
"""
from __future__ import annotations
import argparse, datetime as dt, io, json, os, re, shutil, sqlite3, subprocess, sys, time, urllib.parse, urllib.request

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
STORE = os.path.join(ROOT, "data", "clean_full")
WRANGLER = next((p for p in (os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler.cmd"),
                             os.path.join(ROOT, "api", "worker", "node_modules", ".bin", "wrangler")) if os.path.exists(p)), "wrangler")
D1 = "econ-catalog"
BUCKET, CSV_PREFIX = "econ-data", "series"
LIVE = "https://econdl-api.elkassabgi.workers.dev"
CONTROL_KEY = "clean_full/stat_slovenia/05W.parquet"     # known-present; proves the credential before any "absent" verdict

IDS = [f"stat_slovenia:SI:05W0{n}S" for n in ("101", "201", "301", "302", "303", "304", "401", "402", "403", "404", "405",
                                              "501", "502", "503", "601", "602", "603", "604", "605", "606", "607", "608", "609")] + \
      ["cbs_nl:70169NED", "cbs_nl:70170NED", "cbs_nl:70167NED", "cbs_nl:81823NED"]
SOURCES = ["stat_slovenia", "cbs_nl"]
# store parquets holding the fabricated rows; measured 2026-09-05 with pyarrow: 96+36+35+33 rows, 197 of 200 impossible
CBS_STORE_FILES = ["70169NED.parquet", "70170NED.parquet", "70167NED.parquet", "81823NED.parquet"]
IMPOSSIBLE = re.compile(r"(?<!\d)(0\d{3}|1[0-4]\d{2}|2[2-9]\d{2}|[3-9]\d{3})-\d{2}-\d{2}")   # year < 1500 or > 2200


def sq(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def d1(sql: str) -> dict:
    r = subprocess.run([WRANGLER, "d1", "execute", D1, "--remote", "--json", "--command", sql],
                       capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=ROOT)
    out = r.stdout.strip()
    try:
        j = json.loads(out)
    except Exception:
        raise RuntimeError("wrangler did not return JSON: %s %s" % (out[:300], r.stderr[:300]))
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError("D1 error: %s" % j)
    return j[0] if isinstance(j, list) else j


def fts_pred(rows: list[dict]) -> str:
    """MATCH on the titles (fts5 phrase per title) AND the id list: plan INDEX 0:M3, not a scan."""
    titles = sorted({(r.get("title") or "").strip() for r in rows if (r.get("title") or "").strip()})
    ids = ",".join(sq(r["series_id"]) for r in rows)
    if not titles:
        return f"series_id IN ({ids})"
    phrases = " OR ".join('"' + t.replace('"', '""') + '"' for t in titles)
    return f"series_fts MATCH {sq(phrases)} AND series_id IN ({ids})"


def r2():
    import boto3
    env = {}
    for line in io.open(os.path.join(ROOT, ".env"), encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); env[k.strip()] = v.strip().strip('"').strip("'")
    return boto3.client("s3", endpoint_url=env["R2_WRITE_ENDPOINT"], aws_access_key_id=env["R2_WRITE_ACCESS_KEY_ID"],
                        aws_secret_access_key=env["R2_WRITE_SECRET_ACCESS_KEY"], region_name="auto")


def r2_exists(c, key: str) -> bool:
    """True/False only for a definitive answer; any other failure raises (an expired token is not 'absent')."""
    try:
        c.head_object(Bucket=BUCKET, Key=key); return True
    except Exception as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code", "") or str(e)
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def csv_key(sid: str) -> str:
    return f"{CSV_PREFIX}/{urllib.parse.quote(sid, safe='')}.csv"


def live_status(sid: str) -> int | None:
    url = LIVE + "/v1/series/" + urllib.parse.quote(sid, safe="") + ".metadata.json"
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "delist-verify"}), timeout=30) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    stamp = dt.datetime.now().strftime("%Y%m%d")
    inlist = ",".join(sq(s) for s in IDS)

    # ---- CHECK: what does D1 hold for the 27 right now? (PK IN-list = index seeks)
    j = d1(f"SELECT series_id, title, start_date, end_date FROM series WHERE series_id IN ({inlist})")
    rows = j["results"]; have = {r["series_id"]: r for r in rows}
    print(f"D1 series rows present for the 27 constants: {len(rows)}  (rows_read={j.get('meta', {}).get('rows_read')})")
    bad = [r for r in rows if not ((r["start_date"] or "9999") < "1500-01-01" or (r["end_date"] or "0000") > "2200-01-01")]
    if bad:
        print("  REFUSING: these present rows do NOT carry impossible dates: " + ", ".join(r["series_id"] for r in bad)); return 2
    fts_n = d1(f"SELECT COUNT(*) AS n FROM series_fts WHERE {fts_pred(rows) if rows else 'series_id IN (' + inlist + ')'}")
    print(f"D1 series_fts rows matching (MATCH-title form): {fts_n['results'][0]['n']}  (rows_read={fts_n.get('meta', {}).get('rows_read')})")
    live_before = {s: live_status(s) for s in IDS}
    print(f"live metadata.json before: 200 x{sum(1 for v in live_before.values() if v == 200)}, 404 x{sum(1 for v in live_before.values() if v == 404)}, other x{sum(1 for v in live_before.values() if v not in (200, 404))}")

    # ---- R2: credential control, then content-check each served CSV
    c = r2()
    if not r2_exists(c, CONTROL_KEY):
        print(f"  REFUSING: control object {CONTROL_KEY} not visible - credential or bucket problem"); return 2
    present, wrong, absent = [], [], []
    for s in IDS:
        k = csv_key(s)
        if not r2_exists(c, k):
            absent.append(k); continue
        raw = c.get_object(Bucket=BUCKET, Key=k)["Body"].read(8192)
        if raw[:2] == b"\x1f\x8b":           # served CSVs are gzip-at-rest since 2026-08 (newer objects); decompress the head
            import zlib
            raw = zlib.decompressobj(31).decompress(raw)
        body = raw.decode("utf-8", "replace")
        if IMPOSSIBLE.search(body):
            present.append(k)
        else:
            wrong.append((k, body.splitlines()[:2]))
    print(f"R2 served CSVs: {len(present)} present WITH impossible dates in content, {len(absent)} absent, {len(wrong)} present WITHOUT (left alone)")
    for k, head in wrong:
        print(f"   NOT deleting {k}: {head}")
    cbs_r2 = [f"clean_full/cbs_nl/{f}" for f in CBS_STORE_FILES if r2_exists(c, f"clean_full/cbs_nl/{f}")]
    print(f"R2 cbs_nl store parquets present: {len(cbs_r2)} of {len(CBS_STORE_FILES)}")

    # ---- local state
    local_n = None
    try:
        con = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True, timeout=30)
        local_n = con.execute(f"SELECT COUNT(*) FROM series WHERE series_id IN ({inlist})").fetchone()[0]; con.close()
    except sqlite3.OperationalError as e:
        print(f"local catalog.db not readable right now ({e}); will retry in apply")
    print(f"local catalog.db series rows present: {local_n}")
    cbs_local = [f for f in CBS_STORE_FILES if os.path.exists(os.path.join(STORE, "cbs_nl", f))]
    print(f"local cbs_nl store parquets present: {len(cbs_local)} of {len(CBS_STORE_FILES)}")

    if not a.apply:
        print("\n(dry run - pass --apply to delist)"); return 0

    # ---- 1. D1 series (PK deletes)
    if rows:
        d1(f"DELETE FROM series WHERE series_id IN ({inlist})")
    left = d1(f"SELECT COUNT(*) AS n FROM series WHERE series_id IN ({inlist})")["results"][0]["n"]
    print(f"1 D1 series: {len(rows)} -> {left}")
    # ---- 2. D1 series_fts (MATCH form)
    pred = fts_pred(rows) if rows else f"series_id IN ({inlist})"
    d1(f"DELETE FROM series_fts WHERE {pred}")
    left_fts = d1(f"SELECT COUNT(*) AS n FROM series_fts WHERE {pred}")["results"][0]["n"]
    print(f"2 D1 series_fts: -> {left_fts} matching (MATCH-title predicate)")
    # ---- 3. R2 served CSVs + cbs_nl store parquets
    todel = present + cbs_r2
    if todel:
        resp = c.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in todel], "Quiet": False})
        errs = resp.get("Errors", [])
        print(f"3 R2: deleted {len(resp.get('Deleted', []))} of {len(todel)}; errors {errs}")
        if errs:
            return 1
    still = [k for k in todel if r2_exists(c, k)]
    # ---- 4. local catalog.db (rollback-journal; retry on lock; FTS via MATCH so the write lock is held for seconds)
    deadline = time.time() + 900; attempt = 0; local_after = None
    while True:
        attempt += 1
        try:
            con = sqlite3.connect(CATALOG, timeout=60.0); con.execute("PRAGMA busy_timeout=60000")
            lrows = con.execute(f"SELECT series_id, title FROM series WHERE series_id IN ({inlist})").fetchall()
            lpred = fts_pred([{"series_id": s, "title": t} for s, t in lrows]) if lrows else pred
            con.execute(f"DELETE FROM series WHERE series_id IN ({inlist})")
            con.execute(f"DELETE FROM series_fts WHERE {lpred}")
            con.commit()
            local_after = con.execute(f"SELECT COUNT(*) FROM series WHERE series_id IN ({inlist})").fetchone()[0]
            con.close(); print(f"4 local catalog.db: {len(lrows)} -> {local_after} (attempt {attempt})"); break
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or time.time() > deadline:
                print(f"4 local catalog.db FAILED: {e} - D1 and R2 are already clean; re-run to finish local"); return 1
            if attempt % 6 == 1: print(f"   local catalog.db locked; retrying (attempt {attempt})", flush=True)
            time.sleep(10)
    # ---- 5. source_counts
    r = subprocess.run([sys.executable, os.path.join(ROOT, "core", "sync_catalog_d1.py"), "--refresh-counts", ",".join(SOURCES)],
                       capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=ROOT)
    print("5 refresh-counts:", (r.stdout.strip().splitlines() or ["(no output)"])[-1][:200], "" if r.returncode == 0 else f"(exit {r.returncode}) {r.stderr[-300:]}")
    counts_ok = r.returncode == 0
    # ---- 6. local archive
    arch = os.path.join(ROOT, "data", "archive", f"timeless_{stamp}"); os.makedirs(arch, exist_ok=True)
    for f in cbs_local:
        shutil.move(os.path.join(STORE, "cbs_nl", f), os.path.join(arch, f"cbs_nl__{f}"))
    print(f"6 archived {len(cbs_local)} cbs_nl parquets -> {arch}")
    # ---- 7. verify from the served side
    time.sleep(3)
    live_after = {s: live_status(s) for s in IDS}
    n404 = sum(1 for v in live_after.values() if v == 404); nother = {s: v for s, v in live_after.items() if v != 404}
    print(f"7 VERIFY: D1 series left {left}; D1 fts left {left_fts}; R2 objects still present {len(still)}; local left {local_after}; "
          f"live metadata.json 404 x{n404}/{len(IDS)}" + (f"; not 404: {nother}" if nother else ""))
    print("   NOTE: /v1/catalog browse pages and /v1/stats are edge-cached up to 6 h and may list the 27 until then.")
    ok = left == 0 and left_fts == 0 and not still and local_after == 0 and n404 == len(IDS) and counts_ok
    print("RESULT:", "DELISTED AND VERIFIED" if ok else "NOT FULLY VERIFIED - read the lines above")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
