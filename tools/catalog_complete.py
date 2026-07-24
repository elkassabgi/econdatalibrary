"""Catalogue-complete a source: INSERT catalog rows for parquet series that have none.

broaden_catalog does NOT incrementally add new series to an already-catalogued source, so when a
source fetches genuinely-new series (e.g. insee_bdm's ~80 new INSEE idbanks) they have no catalog
row and the CSV-coherence gate demotes the run to `partial`. This tool reads a source's parquet
key column, finds keys with no '<source>:<key>' catalog row, and INSERTs minimal honest rows
(title = the native key, license copied from the source's existing rows), then adds them to FTS.
Run tools/refresh_r2_catalog.py afterward to push the updated catalog to R2.

Minimal rows mirror broaden_catalog's schema; a later full broaden_catalog re-run backfills real
titles/dates. SAFE: only INSERT OR IGNORE for the NAMED source(s); never touches other sources,
never deletes, never rewrites existing rows.

  python tools/catalog_complete.py insee_bdm scb ssb ...
"""
import os, sys, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
from updater import config, blob

CAT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "catalog.db")
KEY_COLS = ("series_key", "idbank")   # detection order — the parquet's series-identity column


def complete(con, source):
    existing = {r[0] for r in con.execute("SELECT series_id FROM series WHERE source_id=?", (source,))}
    licrow = con.execute("SELECT license_id FROM series WHERE source_id=? AND license_id IS NOT NULL "
                         "LIMIT 1", (source,)).fetchone()
    lic = licrow[0] if licrow else None

    keys = set()
    files = blob.list_parquets(config.source_dir(source))
    key_col = None
    for f in files:
        path = os.path.join(config.source_dir(source), f)
        if key_col is None:
            cols = blob.read_schema(path).names
            key_col = next((c for c in KEY_COLS if c in cols), None)
            if key_col is None:
                print(f"  {source}: NO series_key/idbank column ({cols}) — skip"); return 0
        for v in blob.read_table(path, columns=[key_col]).column(key_col).to_pylist():
            keys.add(str(v))

    missing = sorted(k for k in keys if f"{source}:{k}" not in existing)
    print(f"  {source:14} key_col={key_col:10} parquet_keys={len(keys):>7,}  "
          f"catalogued={len(existing):>7,}  missing={len(missing):>6,}")
    if not missing:
        return 0
    rows = [(f"{source}:{k}", source, k, None, None, None, None, lic, None, None, None, "{}")
            for k in missing]
    con.executemany(
        "INSERT OR IGNORE INTO series (series_id,source_id,title,frequency,unit,geography,"
        "category,license_id,start_date,end_date,last_updated,metadata) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        rows)
    try:
        con.executemany("INSERT INTO series_fts(series_id,title,geography) VALUES (?,?,?)",
                        [(f"{source}:{k}", k, None) for k in missing])
    except sqlite3.OperationalError:
        pass
    con.commit()
    print(f"                 -> inserted {len(missing):,} rows (title=native key, license={lic})")
    return len(missing)


def main(sources):
    con = sqlite3.connect(CAT)
    total = 0
    for s in sources:
        total += complete(con, s)
    con.close()
    print(f"\n  total rows added: {total:,}")
    if total:
        print("  NEXT: python tools/refresh_r2_catalog.py <stamp>   (push the updated catalog to R2)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python tools/catalog_complete.py <source> [...]"); raise SystemExit(2)
    main(sys.argv[1:])
