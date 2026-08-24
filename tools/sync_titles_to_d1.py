#!/usr/bin/env python3
"""Push the titles that DRIFTED into the live D1, and refresh those sources' FTS rows.

WHY THIS EXISTS. A title written into `data/catalog.db` changes nothing a user sees. The Worker
resolves against D1 and search resolves against D1's `series_fts`. Five sources were titled in
one session and every one still served its raw key — idb, unctad_trademerchgr, noaa, unhcr and
bea all read DRIFT against the live API (R345: a local edit is not a served change; R481: the
FTS index is a fourth place, with no foreign key to remind you).

ONLY THE DRIFTED ROWS. The first version of this tool emitted an UPDATE for every titled row of
each source and produced 158,876 files, because noaa alone has 3,138,159 titled rows while 330
of them changed. D1 can identify the drift itself: a row whose title is still its own raw key is
exactly a row this session titled locally and never pushed. So the id list comes from D1 —

    SELECT series_id FROM series WHERE source_id=?
      AND (title IS NULL OR title='' OR title = substr(series_id, instr(series_id,':')+1))

— and only those ids are updated, from the local catalogue's value.

The per-source FTS refresh deletes by ID PATTERN rather than by joining `series`, because a join
cannot remove an FTS row whose series row is already gone, which is the state a purge leaves
behind (R481).

    python tools/sync_titles_to_d1.py idb noaa unhcr            # plan only
    python tools/sync_titles_to_d1.py idb noaa unhcr --push     # execute
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "data", "catalog.db")
OUTDIR = os.path.join(ROOT, "dist", "d1", "titlesync")
WORKER = os.path.join(ROOT, "api", "worker")
BATCH = 20


def q(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


# noaa does not live in econ-catalog. api/worker/src/util.ts routes SHARDED_SOURCES to the
# CATALOG_CLIMATE binding, so a sync that assumes one database silently reports "0 raw in D1"
# for it - which is what happened on the first run and reads exactly like "nothing to do".
SHARDED = {"noaa": "econ-catalog-climate"}


def db_for(source: str) -> str:
    return SHARDED.get(source, "econ-catalog")


def d1(sql: str, as_json: bool = True, db: str = "econ-catalog"):
    cmd = ["npx", "wrangler", "d1", "execute", db, "--remote", "--command", sql]
    if as_json:
        cmd.append("--json")
    r = subprocess.run(cmd, cwd=WORKER, capture_output=True, text=True, shell=True,
            encoding="utf-8", errors="replace")
    if not as_json:
        return r.returncode == 0
    out = r.stdout
    i = out.find("[")
    if i < 0:
        return None
    try:
        obj, _ = json.JSONDecoder().raw_decode(out[i:])
        return obj[0].get("results")
    except Exception:                                        # noqa: BLE001
        return None


def raw_ids_in_d1(source: str) -> list[str]:
    """Ids D1 still serves as their own key — the exact drift set."""
    sql = ("SELECT series_id FROM series WHERE source_id=" + q(source) +
           " AND (title IS NULL OR title='' OR title = substr(series_id, instr(series_id,':')+1))")
    rows = d1(sql, db=db_for(source))
    return [r["series_id"] for r in (rows or [])]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sources", nargs="+")
    ap.add_argument("--push", action="store_true")
    a = ap.parse_args()

    if os.path.isdir(OUTDIR):
        shutil.rmtree(OUTDIR)
    os.makedirs(OUTDIR, exist_ok=True)

    con = sqlite3.connect("file:%s?mode=ro" % CATALOG.replace(os.sep, "/"), uri=True)
    files: list[str] = []
    for src in a.sources:
        ids = raw_ids_in_d1(src)
        if ids is None:
            print(f"  {src:24} could not read D1 — skipped")
            continue
        pairs = []
        for sid in ids:
            row = con.execute("SELECT title FROM series WHERE series_id=?", (sid,)).fetchone()
            if not row or not row[0]:
                continue
            bare = sid.split(":", 1)[1] if ":" in sid else sid
            if row[0] in (sid, bare):
                continue                    # still raw locally too — nothing to push
            pairs.append((sid, row[0]))
        if not pairs:
            print(f"  {src:24} {len(ids):,} raw in D1, 0 titled locally — nothing to push")
            continue
        for i in range(0, len(pairs), BATCH):
            p = os.path.join(OUTDIR, f"{src}_{i // BATCH:05d}.sql")
            with open(p, "w", encoding="utf-8", newline="\n") as fh:
                for sid, t in pairs[i:i + BATCH]:
                    fh.write(f"UPDATE series SET title={q(t)} WHERE series_id={q(sid)};\n")
            files.append(p)
        p = os.path.join(OUTDIR, f"{src}_zz_fts.sql")
        with open(p, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(f"DELETE FROM series_fts WHERE series_id LIKE {q(src + ':%')};\n")
            fh.write("INSERT INTO series_fts(series_id,title,geography) "
                     f"SELECT series_id,title,geography FROM series WHERE source_id={q(src)};\n")
        files.append(p)
        print(f"  {src:24} {len(ids):,} raw in D1 -> {len(pairs):,} to push, + FTS refresh")
    con.close()

    print(f"  wrote {len(files)} file(s)")
    if not a.push:
        print("  PLAN ONLY — re-run with --push")
        return 0
    failed: list[tuple[str, str]] = []
    for p in files:
        src_of = os.path.basename(p).rsplit("_", 1)[0].replace("_zz", "")
        r = subprocess.run(
            ["npx", "wrangler", "d1", "execute", db_for(src_of), "--remote", "--file", p],
            cwd=WORKER, capture_output=True, text=True, shell=True,
            encoding="utf-8", errors="replace")
        if r.returncode != 0:
            # DO NOT abort the run. One transient "Authentication error [code: 10000]" on file
            # 41 of 211 previously skipped every file after it - unhcr, bea, eia and noaa were
            # all reported as pushed and none of them were. Collect the failures, finish the
            # rest, and report; the whole operation is idempotent, so a re-run picks up only
            # what is still raw.
            failed.append((os.path.basename(p), (r.stderr or r.stdout)[-180:].strip()[:160]))
            continue
    ok = len(files) - len(failed)
    print(f"  pushed {ok} of {len(files)} file(s)")
    for name, err in failed:
        print(f"    FAILED {name}: {err}")
    if failed:
        print(f"  {len(failed)} file(s) failed — re-run to retry only what is still raw")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
