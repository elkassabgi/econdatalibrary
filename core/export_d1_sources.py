"""Emit a D1 delta that publishes SPECIFIC sources (licence + source + series + FTS).

Why this exists alongside export_d1_new_series.py: that script only emits series carrying a
"real" title, i.e. it DROPS any row whose title equals the native key. That is the correct
filter for the adversarial title wave, but it silently excludes everything catalogued by
`broaden_catalog`, which honestly uses the native key as the title rather than inventing one.
The IEP set (gpi/gti/ppi/etr) is exactly that shape, so it could never reach D1 via that path.

  python core/export_d1_sources.py gpi gti ppi etr
  wrangler d1 execute econ-catalog --remote --file=dist/d1/sources/part_000.sql   (then _fts.sql)

D1 rules: no BEGIN/COMMIT/PRAGMA, small statements. Idempotent (INSERT OR REPLACE + a
DELETE-then-INSERT FTS refresh per source). Verified by replay into in-memory SQLite.
"""
from __future__ import annotations
import os, sqlite3, sys

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(_THIS)
CATALOG = os.path.join(ROOT, "data", "catalog.db")
OUT_DIR = os.path.join(ROOT, "dist", "d1", "sources")
SCOLS = ["series_id", "source_id", "title", "frequency", "unit", "geography",
         "category", "license_id", "start_date", "end_date", "last_updated", "metadata"]
ROWS_PER_STMT = 20
MAX_BYTES = 6_000_000


def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    return "'" + str(v).replace("'", "''") + "'"


def main(argv):
    if not argv:
        print("usage: python core/export_d1_sources.py <source> [...]"); raise SystemExit(2)
    os.makedirs(OUT_DIR, exist_ok=True)
    for f in os.listdir(OUT_DIR):
        if f.endswith(".sql"):
            os.remove(os.path.join(OUT_DIR, f))

    conn = sqlite3.connect(f"file:{CATALOG}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    qmarks = ",".join("?" * len(argv))

    lic_cols = [r[1] for r in conn.execute("PRAGMA table_info(license)")]
    src_cols = [r[1] for r in conn.execute("PRAGMA table_info(source)")]
    stmts = []

    lic_ids = {r["license_id"] for r in conn.execute(
        f"SELECT DISTINCT license_id FROM source WHERE source_id IN ({qmarks})", argv) if r["license_id"]}
    for lid in sorted(lic_ids):
        r = conn.execute("SELECT * FROM license WHERE license_id=?", (lid,)).fetchone()
        if r:
            stmts.append(f"INSERT OR REPLACE INTO license ({', '.join(chr(34)+c+chr(34) for c in lic_cols)}) "
                         f"VALUES ({', '.join(_lit(r[c]) for c in lic_cols)});")
    for r in conn.execute(f"SELECT * FROM source WHERE source_id IN ({qmarks})", argv):
        stmts.append(f"INSERT OR REPLACE INTO source ({', '.join(chr(34)+c+chr(34) for c in src_cols)}) "
                     f"VALUES ({', '.join(_lit(r[c]) for c in src_cols)});")

    rows = [tuple(r[c] for c in SCOLS) for r in conn.execute(
        f"SELECT {', '.join(SCOLS)} FROM series WHERE source_id IN ({qmarks}) ORDER BY series_id", argv)]
    conn.close()
    print(f"licence rows: {len(lic_ids)}   source rows: {len(argv)}   series rows: {len(rows):,}")

    collist = ", ".join(f'"{c}"' for c in SCOLS)
    for i in range(0, len(rows), ROWS_PER_STMT):
        chunk = rows[i:i + ROWS_PER_STMT]
        vals = ",\n".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in chunk)
        stmts.append(f"INSERT OR REPLACE INTO series ({collist}) VALUES\n{vals};")

    files, buf, bb, part = [], [], 0, 0
    hdr = "-- Econ Data Library: publish specific sources to D1 (no txn/pragma).\n"
    for s in stmts:
        sb = len(s.encode()) + 1
        if buf and bb + sb > MAX_BYTES:
            p = os.path.join(OUT_DIR, f"part_{part:03d}.sql")
            open(p, "w", encoding="utf-8").write(hdr + "\n".join(buf) + "\n")
            files.append(p); part, buf, bb = part + 1, [], 0
        buf.append(s); bb += sb
    if buf:
        p = os.path.join(OUT_DIR, f"part_{part:03d}.sql")
        open(p, "w", encoding="utf-8").write(hdr + "\n".join(buf) + "\n")
        files.append(p)

    fts = os.path.join(OUT_DIR, "_fts.sql")
    with open(fts, "w", encoding="utf-8") as f:
        f.write("-- FTS refresh per source. Run AFTER the part_*.sql. DELETE-then-INSERT so a\n"
                "-- re-run cannot double the rows.\n")
        for s in argv:
            lit = "'" + s.replace("'", "''") + "'"
            f.write(f"DELETE FROM series_fts WHERE series_id IN "
                    f"(SELECT series_id FROM series WHERE source_id={lit});\n")
            f.write(f"INSERT INTO series_fts(series_id,title,geography) "
                    f"SELECT series_id,title,geography FROM series WHERE source_id={lit};\n")
    print(f"wrote {len(files)} part file(s) + _fts.sql to {OUT_DIR}")

    mem = sqlite3.connect(":memory:")
    mem.execute(f"CREATE TABLE series ({', '.join(c + ' TEXT' for c in SCOLS)}, PRIMARY KEY(series_id))")
    mem.execute(f"CREATE TABLE source ({', '.join(c + ' TEXT' for c in src_cols)}, PRIMARY KEY(source_id))")
    mem.execute(f"CREATE TABLE license ({', '.join(c + ' TEXT' for c in lic_cols)}, PRIMARY KEY(license_id))")
    for p in files:
        mem.executescript(open(p, encoding="utf-8").read())
    n = mem.execute("SELECT COUNT(*) FROM series").fetchone()[0]
    ns = mem.execute("SELECT COUNT(*) FROM source").fetchone()[0]
    mem.close()
    ok = (n == len(rows) and ns == len(argv))
    print(f"verify replay: {n:,} series + {ns} source rows  {'PASS' if ok else 'FAIL'}")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main(sys.argv[1:])
