"""Delist a source: delete its catalog.db + D1 rows WITHOUT touching any R2 object.

AUTHORIZED by Ahmed 2026-08-06 ("yes, remove hf, owids"): removes catalogue/D1 listings for
sources whose rows are unservable or unserveable-by-licence, while deliberately leaving every
R2 object (store parquets AND series CSVs) alone. That is the difference from
tools/retire_source.py, which purges R2 — owid's gated store must survive its delisting, so
the two operations must never share a code path.

Targets this was built for:
  * hf_equities — 1,391 metadata-only listings econ cannot serve (R29: no metadata-only, ever);
    0 series CSVs and 0 store objects exist, so row deletion IS the whole cleanup.
  * owid       — 64 residual listed rows; licence DISPUTED, store stays gated on R2 untouched.

After running: remove the id from util.ts SUPPORTED_SOURCES, deploy, verify live absence
(with a known-present control, R338), and refresh_r2_catalog --allow-shrink <src>.

Usage:
  python tools/delist_source_rows.py hf_equities          # dry run: counts only
  python tools/delist_source_rows.py hf_equities --apply
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

D1_NAME = "econ-catalog"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    src = a.source

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
    n_series = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    n_source = con.execute("SELECT COUNT(*) FROM source WHERE source_id=?", (src,)).fetchone()[0]
    print(f"{src}: catalog series={n_series:,} source_row={n_source} (R2 objects untouched by design)")

    if not a.apply:
        print("(dry run - pass --apply to delist)")
        return 0

    con.execute("DELETE FROM series WHERE source_id=?", (src,))
    con.execute("DELETE FROM source WHERE source_id=?", (src,))
    con.commit()
    left = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (src,)).fetchone()[0]
    print(f"  catalog.db: deleted; residual rows={left} (must be 0)")
    if left:
        return 1

    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for stmt in (f"DELETE FROM series WHERE source_id='{src}';",
                 f"DELETE FROM source WHERE source_id='{src}';"):
        r = subprocess.run(["npx", "wrangler", "d1", "execute", D1_NAME, "--remote",
                            "--command", stmt],
                           cwd=os.path.join(ROOT, "api", "worker"),
                           capture_output=True, text=True, encoding="utf-8",
                           errors="replace", env=env, shell=(os.name == "nt"))
        ok = r.returncode == 0
        print(f"  D1: {stmt.split(' WHERE')[0]} -> {'ok' if ok else 'FAILED'}")
        if not ok:
            print("   ", (r.stderr or r.stdout)[-300:])
            return 1

    print(f"{src}: DELISTED (catalog + D1). Now: util.ts removal + deploy + live absence check "
          f"+ refresh_r2_catalog --allow-shrink {src}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
