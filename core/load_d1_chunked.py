"""load_d1_chunked.py — load dist/d1/econ_catalog.sql into the REMOTE D1 in
chunks, because the single-file import path fails upstream on a ~1 GB dump
(Cloudflare error 7009 on /d1/database/{id}/import, reproduced on wrangler 3
and 4; 2026-07-14/15).

Splits on complete-statement boundaries (a statement ends at a line ending in
';'), executes each chunk via `wrangler d1 execute --remote --file`, retries
each chunk once, then VERIFIES remote row counts per table against the local
catalog.db. Chunked loading is non-atomic — acceptable pre-launch (near-zero
traffic) — which is why the final count verification is mandatory, not
optional.

Run: python core/load_d1_chunked.py  (CLOUDFLARE_API_TOKEN in env)
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DUMP = os.path.join(ROOT, "dist", "d1", "econ_catalog.sql")
CHUNK_DIR = os.path.join(ROOT, "dist", "d1", "chunks")
WORKER_DIR = os.path.join(ROOT, "api", "worker")
DB_NAME = "econ-catalog"
DB_ID = "1a6d0755-ecef-46d0-a478-46cad1cf064c"
ACCT = "ce51d5c7fe3859098751b89bbebeab7a"
CHUNK_BYTES = 2 * 1024 * 1024    # ~4 MB per chunk: small files route through
                                 # wrangler's QUERY path; larger ones go via the
                                 # /import endpoint, which fails upstream (7009)
                                 # on this database regardless of wrangler version.


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def split_dump() -> list[str]:
    os.makedirs(CHUNK_DIR, exist_ok=True)
    chunks, buf, size, n = [], [], 0, 0

    def flush():
        nonlocal buf, size, n
        if not buf:
            return
        p = os.path.join(CHUNK_DIR, f"chunk_{n:03d}.sql")
        with open(p, "w", encoding="utf-8") as f:
            f.writelines(buf)
        chunks.append(p)
        n += 1
        buf, size = [], 0

    with open(DUMP, encoding="utf-8") as f:
        for line in f:
            # IDEMPOTENCY: the execute channel times out client-side while the
            # statements complete server-side (observed: 'no poll() in 15000ms'
            # followed by SQLITE_CONSTRAINT_PRIMARYKEY on the next chunk). Plain
            # INSERT breaks on retry; OR REPLACE converges to the same state no
            # matter how many attempts actually landed.
            if line.startswith("INSERT INTO "):
                line = "INSERT OR REPLACE INTO " + line[len("INSERT INTO "):]
            buf.append(line)
            size += len(line)
            # statement boundary = line ends with ';' — safe split point
            if size >= CHUNK_BYTES and line.rstrip().endswith(";"):
                flush()
    flush()
    log(f"split into {len(chunks)} chunks in {CHUNK_DIR}")
    return chunks


def run_chunk(path: str) -> bool:
    for attempt in (1, 2, 3):
        r = subprocess.run(
            ["npx", "--yes", "wrangler@4", "d1", "execute", DB_NAME, "--remote",
             "--file", path, "-y"],
            cwd=WORKER_DIR, capture_output=True, text=True, timeout=1800,
            encoding="utf-8", errors="replace",   # Windows default cp1252 decode
                                                  # CRASHED on wrangler's output
            shell=True if os.name == "nt" else False)
        out = (r.stdout or "") + (r.stderr or "")
        if r.returncode == 0 and "ERROR" not in out:
            return True
        log(f"  !! attempt {attempt} failed rc={r.returncode}: {out[-200:].strip()}")
        time.sleep(20)
    return False


def remote_count(token: str, table: str) -> int:
    req = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/accounts/{ACCT}/d1/database/{DB_ID}/query",
        method="POST",
        data=json.dumps({"sql": f"SELECT COUNT(*) AS n FROM {table}"}).encode(),
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        d = json.load(resp)
    return d["result"][0]["results"][0]["n"]


def main() -> None:
    token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not token:
        raise SystemExit("CLOUDFLARE_API_TOKEN not set")
    if not os.path.exists(DUMP):
        raise SystemExit(f"dump missing: {DUMP}")

    chunks = split_dump()
    failed = []
    for i, p in enumerate(chunks, 1):
        log(f"chunk {i}/{len(chunks)}: {os.path.basename(p)} "
            f"({os.path.getsize(p)/1e6:.0f} MB)")
        if not run_chunk(p):
            failed.append(p)
            log(f"  !! GIVING UP on {os.path.basename(p)} after retries — "
                "continuing (verification will catch the gap)")
    # VERIFY: remote vs local counts (R1: outcomes in the data)
    local = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    tables = [r[0] for r in local.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%'")]
    log("VERIFY remote vs local row counts:")
    ok = True
    for t in tables:
        l = local.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        try:
            rmt = remote_count(token, t)
        except Exception as e:  # noqa: BLE001
            log(f"  {t}: remote count FAILED ({e})")
            ok = False
            continue
        mark = "OK" if rmt == l else "MISMATCH"
        if rmt != l:
            ok = False
        log(f"  {t:24} local={l:>10,} remote={rmt:>10,}  {mark}")
    log(f"RESULT: {'VERIFY PASS - live D1 matches the certified dump' if ok and not failed else 'INCOMPLETE - see failures above'}")
    sys.exit(0 if ok and not failed else 1)


if __name__ == "__main__":
    main()
