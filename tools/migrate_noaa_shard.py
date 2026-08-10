"""Migrate noaa's 3,137,871 catalogue rows to the econ-catalog-climate D1 shard (task #45).

WHY. econ-catalog measured 9.35 GB of its 10 GB per-database ceiling (819 B/row,
~794k rows of headroom) while bea needs 912,990 rows and fdic 298,869 — both
arithmetically impossible until noaa's 3,137,871 rows (27.5% of the catalogue for
0.69%% of the library's observations) move out. Cloudflare's cap is per DATABASE;
the account allows 1 TB, so a second D1 bound to the same worker is an internal
shard: same URL, same API, no new domain, no billing change.

ORDER OF OPERATIONS (nothing user-visible may break):
  1. emit    — stream noaa rows out of catalog.db into chunked INSERT OR REPLACE
               files (series + series_fts mirror + source/license parents first).
  2. push    — execute every file against econ-catalog-climate, resumable via a
               done-list so an interrupted push continues instead of restarting.
  3. verify  — shard count == catalog.db count, exact; sample rows compared.
  4. (separate, later) worker routing deploy, live smoke, and only THEN delete
     noaa from the primary. The delete is of re-derivable rows — catalog.db
     remains the source of truth throughout.

Emission reuses core.sync_catalog_d1's literals and core.sync_state_d1's
constants + execute_remote (bounded retries, utf-8-pinned decode — R363/R222
lessons already baked in). D1_DATABASE is monkeypatched to the shard for push.

Memory is bounded: rows stream from a cursor and files flush at MAX_FILE_BYTES;
nothing holds 3.1M rows at once (emit_sql in sync_catalog_d1 would).
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_state_d1 as st  # noqa: E402  (_lit, MAX_FILE_BYTES, execute_remote)
from core import sync_catalog_d1 as cat  # noqa: E402  (_parent_rows)

SHARD = "econ-catalog-climate"
SOURCE = "noaa"
CATALOG_DB = os.path.join(ROOT, "data", "catalog.db")
OUT_DIR = os.path.join(ROOT, "data", "_noaa_shard_sql")
DONE_LIST = os.path.join(OUT_DIR, "_pushed.txt")
COLS = ["series_id", "source_id", "title", "frequency", "unit", "geography",
        "category", "license_id", "start_date", "end_date", "last_updated", "metadata"]
# Per-STATEMENT byte cap, not a row count. The first emit used 400 rows/statement and
# D1 rejected file 0 with "statement too long: SQLITE_TOOBIG" — noaa rows carry long
# station titles + metadata JSON, so a fixed row count that was fine for bare-code
# sources overflows here. 80 KB per statement clears D1's limit with margin.
STMT_BYTES = 80_000
ROWS_PER_STMT = 400  # fetchmany batch size only; statements are cut by STMT_BYTES


def emit() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = sqlite3.connect(CATALOG_DB)
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) FROM series WHERE source_id=?",
                         (SOURCE,)).fetchone()[0]
    print(f"emit: {total:,} {SOURCE} rows -> {OUT_DIR}")

    files: list[str] = []
    buf: list[str] = []
    n = 0

    def flush() -> None:
        nonlocal buf, n
        if not buf:
            return
        p = os.path.join(OUT_DIR, f"noaa_{len(files):05d}.sql")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("\n".join(buf))
        files.append(p)
        buf, n = [], 0

    def add(stmt: str) -> None:
        nonlocal n
        if buf and n + len(stmt) > st.MAX_FILE_BYTES:
            flush()
        buf.append(stmt)
        n += len(stmt)

    # Parents FIRST (source + licence rows) — sync_catalog_d1's measured lesson:
    # series without their source row are fetchable but unlistable.
    for s in cat._parent_rows(conn, [{"source_id": SOURCE}]):
        add(s)

    cur = conn.execute(
        f"SELECT {', '.join(COLS)} FROM series WHERE source_id=? ORDER BY series_id",
        (SOURCE,))
    emitted = 0
    s_vals: list[str] = []
    f_vals: list[str] = []
    s_n = f_n = 0

    def cut_series() -> None:
        nonlocal s_vals, s_n
        if s_vals:
            add(f"INSERT OR REPLACE INTO series ({', '.join(COLS)}) VALUES\n  "
                + ",\n  ".join(s_vals) + ";")
            s_vals, s_n = [], 0

    def cut_fts() -> None:
        nonlocal f_vals, f_n
        if f_vals:
            add("INSERT INTO series_fts (series_id,title,geography) VALUES\n  "
                + ",\n  ".join(f_vals) + ";")
            f_vals, f_n = [], 0

    while True:
        chunk = cur.fetchmany(ROWS_PER_STMT)
        if not chunk:
            break
        for r in chunk:
            sv = "(%s)" % ", ".join(st._lit(r[c]) for c in COLS)
            if s_n + len(sv) > STMT_BYTES:
                cut_series()
            s_vals.append(sv)
            s_n += len(sv)
            fv = "(%s,%s,%s)" % (st._lit(r["series_id"]), st._lit(r["title"]),
                                 st._lit(r["geography"]))
            if f_n + len(fv) > STMT_BYTES:
                cut_fts()
            f_vals.append(fv)
            f_n += len(fv)
        emitted += len(chunk)
        if emitted % 200_000 < ROWS_PER_STMT:
            print(f"  emitted {emitted:,}/{total:,} rows, {len(files)} files",
                  flush=True)
    cut_series()
    cut_fts()
    flush()
    print(f"emit DONE: {emitted:,} rows across {len(files)} files")
    if emitted != total:
        raise SystemExit(f"FATAL: emitted {emitted:,} != counted {total:,}")


def push() -> None:
    done = set()
    if os.path.exists(DONE_LIST):
        done = {ln.strip() for ln in open(DONE_LIST, encoding="utf-8") if ln.strip()}
    all_files = sorted(f for f in os.listdir(OUT_DIR)
                       if f.endswith(".sql"))
    todo = [os.path.join(OUT_DIR, f) for f in all_files if f not in done]
    print(f"push: {len(todo)} of {len(all_files)} files remaining -> {SHARD}")
    st.D1_DATABASE = SHARD  # execute_remote reads the module global
    for p in todo:
        st.execute_remote([p])          # loud abort on final failure, per-file
        with open(DONE_LIST, "a", encoding="utf-8") as fh:
            fh.write(os.path.basename(p) + "\n")
    print("push DONE")


def verify() -> int:
    import json
    import subprocess
    conn = sqlite3.connect(CATALOG_DB)
    local = conn.execute("SELECT COUNT(*) FROM series WHERE source_id=?",
                         (SOURCE,)).fetchone()[0]
    npx = __import__("shutil").which("npx")
    res = subprocess.run(
        [npx, "wrangler", "d1", "execute", SHARD, "--remote", "--yes",
         "--command", f"SELECT COUNT(*) AS n FROM series WHERE source_id='{SOURCE}';",
         "--json"],
        cwd=st.WORKER_DIR, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=600)
    remote = json.loads(res.stdout)[0]["results"][0]["n"]
    print(f"verify: catalog.db={local:,}  shard={remote:,}  "
          f"{'MATCH' if local == remote else 'MISMATCH'}")
    # spot-compare 3 rows end-to-end
    for r in conn.execute("SELECT series_id,title FROM series WHERE source_id=? "
                          "ORDER BY series_id LIMIT 3", (SOURCE,)):
        q = subprocess.run(
            [npx, "wrangler", "d1", "execute", SHARD, "--remote", "--yes",
             "--command",
             "SELECT title FROM series WHERE series_id='" + r[0].replace("'", "''") + "';",
             "--json"],
            cwd=st.WORKER_DIR, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=600)
        got = json.loads(q.stdout)[0]["results"]
        ok = bool(got) and got[0]["title"] == r[1]
        print(f"  {r[0][:60]}: {'ok' if ok else 'DIFFERS'}")
        if not ok:
            return 1
    return 0 if local == remote else 1


def prune_primary() -> int:
    """Delete noaa's series + series_fts rows from the PRIMARY — the LAST step.

    Preconditions (all were proven before this phase was ever run):
      verify() MATCH exact; worker routing deployed; live smoke passed on all four
      surfaces (sources listing, shard-routed browse total 3,137,871, unscoped
      two-DB search, authenticated CSV download).

    The source/license rows STAY — /v1/sources reads them from the primary by
    design. Batched deletes (D1 has per-query limits; one 3.1M-row DELETE is a
    timeout risk), loop-until-zero so the phase is re-runnable and its stopping
    condition is the DATABASE's own count, not our bookkeeping.
    """
    import json
    import shutil
    import subprocess
    npx = shutil.which("npx")

    def run_sql(sql: str) -> dict:
        res = subprocess.run(
            [npx, "wrangler", "d1", "execute", "econ-catalog", "--remote", "--yes",
             "--command", sql, "--json"],
            cwd=st.WORKER_DIR, capture_output=True, text=True, encoding="utf-8",
            errors="replace", timeout=600)
        if res.returncode != 0:
            raise SystemExit(f"FATAL: primary delete failed: {(res.stderr or res.stdout)[-300:]}")
        return json.loads(res.stdout)[0]

    total = 0
    while True:
        r = run_sql("DELETE FROM series WHERE rowid IN "
                    "(SELECT rowid FROM series WHERE source_id='noaa' LIMIT 40000);")
        w = r["meta"].get("rows_written", 0)
        # rows_written counts index churn too; use changes-equivalent via a count probe
        left = run_sql("SELECT COUNT(*) AS n FROM series WHERE source_id='noaa';")
        n = left["results"][0]["n"]
        total += w
        print(f"  series: {n:,} noaa rows remain", flush=True)
        if n == 0:
            break
    while True:
        run_sql("DELETE FROM series_fts WHERE series_id IN "
                "(SELECT series_id FROM series_fts WHERE series_id LIKE 'noaa:%' "
                "LIMIT 20000);")
        left = run_sql("SELECT COUNT(*) AS n FROM series_fts WHERE series_id LIKE 'noaa:%';")
        n = left["results"][0]["n"]
        print(f"  series_fts: {n:,} noaa rows remain", flush=True)
        if n == 0:
            break
    src = run_sql("SELECT COUNT(*) AS n FROM source WHERE source_id='noaa';")
    print(f"prune DONE. source row present (must be 1): {src['results'][0]['n']}")
    return 0 if src["results"][0]["n"] == 1 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["emit", "push", "verify", "prune-primary"])
    a = ap.parse_args()
    if a.phase == "emit":
        emit()
    elif a.phase == "push":
        push()
    elif a.phase == "prune-primary":
        sys.exit(prune_primary())
    else:
        sys.exit(verify())
