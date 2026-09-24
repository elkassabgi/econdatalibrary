"""load_d1_rest.py — load dist/d1/econ_catalog.sql into the remote D1 via the
REST /query endpoint, ONE STATEMENT PER CALL, in parallel.

Why this transport (2026-07-15): the 1 GB single-file import fails upstream
(7009) on wrangler 3 AND 4; wrangler's chunked execute times out client-side
('no poll() in 15000ms') while sometimes applying server-side; and the /query
endpoint rejects multi-statement SQL (7500). Single-statement /query calls are
the one channel that has been reliable all session — so we use exactly that,
made fast with a thread pool and safe with idempotent INSERT OR REPLACE.

Phases:
  1. schema statements (DROP/CREATE...) — sequential, in dump order;
  2. INSERT statements — rewritten to INSERT OR REPLACE, 10 workers, 5 retries
     with exponential backoff; failures recorded and retried in a final sweep;
  3. VERIFY per-table row counts remote vs local catalog.db (mandatory: the
     transport is non-atomic, the count check is the real gate).

Run: python core/load_d1_rest.py   (CLOUDFLARE_API_TOKEN in env)
"""
from __future__ import annotations

import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)          # `python core/<this>.py`: core.d1_remote needs the root
DUMP = os.path.join(ROOT, "dist", "d1", "econ_catalog.sql")
DB_ID = "1a6d0755-ecef-46d0-a478-46cad1cf064c"
ACCT = "ce51d5c7fe3859098751b89bbebeab7a"
WORKERS = 10
RETRIES = 5

_done = 0
_lock = threading.Lock()


def log(m: str) -> None:
    print(f"[{time.strftime('%m-%d %H:%M:%S')}] {m}", flush=True)


def statements():
    """Yield complete SQL statements from the dump (a statement ends at a line
    ending in ';'). Comments are dropped; INSERT becomes INSERT OR REPLACE."""
    buf: list[str] = []
    with open(DUMP, encoding="utf-8") as f:
        for line in f:
            if not buf and line.startswith("--"):
                continue
            if not buf and line.startswith("INSERT INTO "):
                line = "INSERT OR REPLACE INTO " + line[len("INSERT INTO "):]
            buf.append(line)
            if line.rstrip().endswith(";"):
                yield "".join(buf)
                buf = []
    if buf:
        yield "".join(buf)


def execute(token: str, sql: str) -> None:
    """One statement through core.d1_remote.query (plan step 1; it reads CLOUDFLARE_API_TOKEN itself, and
    after T0 refuses every write - that refusal is final, never retried). Raise after RETRIES."""
    from core import d1_remote
    from core.cutover import CutoverRefused
    delay = 2
    for attempt in range(1, RETRIES + 1):
        try:
            d1_remote.query("econ-catalog", sql, timeout=90)
            return
        except CutoverRefused:
            raise
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {str(e)[:150]}"
        if attempt == RETRIES:
            raise RuntimeError(err)
        time.sleep(delay)
        delay = min(delay * 2, 45)


def run_insert(token: str, idx_sql):
    global _done
    idx, sql = idx_sql
    execute(token, sql)
    with _lock:
        _done += 1
        if _done % 2000 == 0:
            log(f"  {_done:,} insert statements applied")
    return idx


def main() -> None:
    token = os.environ.get("CLOUDFLARE_API_TOKEN")
    if not token:
        raise SystemExit("CLOUDFLARE_API_TOKEN not set")

    schema, inserts = [], []
    for s in statements():
        (inserts if s.lstrip().startswith("INSERT") else schema).append(s)
    log(f"parsed {len(schema)} schema statements + {len(inserts):,} insert statements")

    log("phase 1: schema (sequential, dump order)")
    for i, s in enumerate(schema, 1):
        execute(token, s)
    log("schema applied")

    log(f"phase 2: inserts ({WORKERS} workers, OR REPLACE, {RETRIES} retries)")
    failed: list[int] = []
    work = list(enumerate(inserts))
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(run_insert, token, w): w[0] for w in work}
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:  # noqa: BLE001
                failed.append(futs[f])
                log(f"  !! stmt {futs[f]} failed after retries: {e}")
    if failed:
        log(f"final sweep: retrying {len(failed)} failed statements sequentially")
        still = []
        for idx in failed:
            try:
                execute(token, inserts[idx])
            except Exception as e:  # noqa: BLE001
                still.append(idx)
                log(f"  !! stmt {idx} STILL failing: {e}")
        failed = still

    # phase 3: VERIFY (R1 — outcomes in the data)
    local = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    tables = [r[0] for r in local.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%'")]
    log("phase 3: VERIFY remote vs local row counts")
    ok = True
    for t in tables:
        lcl = local.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        try:
            from core import d1_remote
            rmt = d1_remote.query("econ-catalog", f"SELECT COUNT(*) AS n FROM {t}", timeout=60)["results"][0]["n"]
        except Exception as e:  # noqa: BLE001
            log(f"  {t}: remote count failed ({e})")
            ok = False
            continue
        mark = "OK" if rmt == lcl else "MISMATCH"
        if rmt != lcl:
            ok = False
        log(f"  {t:24} local={lcl:>10,} remote={rmt:>10,}  {mark}")
    good = ok and not failed
    log(f"RESULT: {'VERIFY PASS - live D1 matches the certified dump' if good else 'INCOMPLETE'}")
    sys.exit(0 if good else 1)


if __name__ == "__main__":
    main()
