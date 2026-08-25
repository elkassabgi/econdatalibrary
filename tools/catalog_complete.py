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
# The backend default is set in main(), NOT here. Setting it at module scope means merely
# IMPORTING this file mutates the process environment for everything that runs afterwards.
# tests/test_catalog_file_exclusions.py imports it at module level, pytest imports every
# test module during collection, and so the whole suite flipped to backend=r2 - 12 blob
# tests then failed with 'cannot derive an R2 key ... no /data/ segment' and CI went red
# for 22 consecutive pushes. config.source_dir() does not read BACKEND and blob reads the
# env per call, so deferring it changes nothing for the script itself.
from updater import config, blob

CAT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "catalog.db")
KEY_COLS = ("series_key", "idbank")   # detection order — the parquet's series-identity column

# FILES INSIDE A SOURCE DIRECTORY THAT MUST NOT INHERIT THAT SOURCE'S LICENCE.
#
# This tool COPIES the source's licence onto every key it finds, and it finds keys by
# globbing the whole source directory. That is fine while one directory means one licence,
# and silently wrong the moment it does not. A directory is a filesystem fact; a licence is a
# publisher's grant, and nothing keeps them aligned.
#
# vdem is the measured case. data/clean_full/vdem/ holds vdem.parquet (77,371,121 rows) and
# vparty.parquet (2,218,990 rows). V-Dem states CC BY-SA 4.0 for "The V-Dem Dataset" on two
# official surfaces; V-Party is a separate publication whose own page carries no licence
# language at all and which that statement never names. Cataloguing the directory would have
# stamped 2.2M V-Party observations with a grant nobody gave - the same failure as the FAO
# incident in the comment above, arriving through the directory rather than through the rows.
#
# Entries here are exclusions of EVIDENCE, not of interest: remove one by evidencing that
# file's licence, never by assuming it shares its neighbour's.
SOURCE_FILE_EXCLUSIONS = {
    "vdem": ("vparty.parquet",),
}


def complete(con, source):
    existing = {r[0] for r in con.execute("SELECT series_id FROM series WHERE source_id=?", (source,))}
    licrow = con.execute("SELECT license_id FROM series WHERE source_id=? AND license_id IS NOT NULL "
                         "LIMIT 1", (source,)).fetchone()
    lic = licrow[0] if licrow else None
    if lic is None:
        # No series row to copy from — precisely the case for a source being catalogued
        # for the FIRST time, which is a main reason to run this tool. Falling through
        # with None would insert every row with a NULL licence, publishing hundreds of
        # thousands of series carrying no attribution at all. The source table already
        # holds the verified licence, so use it.
        srow = con.execute("SELECT license_id FROM source WHERE source_id=?",
                           (source,)).fetchone()
        lic = srow[0] if srow and srow[0] else None
    if lic is None:
        # Still nothing: refuse rather than publish unattributed rows. Whoever adds a
        # source records its licence FIRST (DATABASE_LICENSES_VERBATIM.md + the source
        # table); that ordering is the point, not a formality.
        print(f"  {source}: NO licence on any series row OR on the source row — refusing "
              f"to insert unattributed catalog rows. Record the licence first.")
        return 0

    # ANNOUNCE THE TERMS BEING APPLIED, and flag the permissive direction.
    #
    # This tool COPIES a licence onto new rows. That makes an unrelated repair able to
    # relicense a source as a side effect, and it happened: cataloguing seven FAO
    # sources to make them downloadable stamped 211,924 series with cc-by-4.0
    # (commercial_ok=1) because the local rows said so, while
    # DATABASE_LICENSES_VERBATIM.md classifies FAO as
    # "redistributable_attribution_noncommercial ... a non-commercial/anti-endorsement
    # restriction that CC BY 4.0 does not impose". The rows it copied were already
    # wrong; the tool propagated them faithfully and silently, and a later sync carried
    # them to the store users read.
    #
    # A licence id alone tells a reader nothing about what it GRANTS, so print the
    # flags, and say plainly when rows are about to be published as commercially
    # usable — that is the direction where being wrong hands out rights the publisher
    # withheld. Cheap to read, and it is the line that would have stopped this (R117).
    flags = con.execute("SELECT commercial_ok, no_modify, attribution_required, name "
                        "FROM license WHERE license_id=?", (lic,)).fetchone()
    if flags:
        comm, nomod, attrib, lname = flags
        print(f"  {source}: applying licence {lic!r} ({lname}) — "
              f"commercial_ok={comm} no_modify={nomod} attribution={attrib}")
        if comm == 1:
            print(f"     NOTE: these rows will be published as COMMERCIALLY USABLE. "
                  f"If DATABASE_LICENSES_VERBATIM.md says otherwise for {source}, stop "
                  f"and fix the licence BEFORE cataloguing — this tool copies terms, it "
                  f"does not verify them.")
    else:
        print(f"  {source}: licence {lic!r} has NO row in the license table — its terms "
              f"are unknown to the catalog. Record it before cataloguing.")
        return 0

    keys = set()
    files = blob.list_parquets(config.source_dir(source))
    if not files:
        # An empty list is not "nothing to do" — it usually means this source's data
        # is not on the BACKEND being read. wid holds 119 parquets locally and none
        # in R2 (it was gated), so under AQUEDUCT_BACKEND=r2 the loop below never
        # ran, key_col stayed None, and the summary line died with "unsupported
        # format string passed to NoneType.__format__" — a crash that says nothing
        # about the actual problem. Say the actual problem.
        print(f"  {source}: NO parquet files under {config.source_dir(source)} "
              f"(backend={os.environ.get('AQUEDUCT_BACKEND', 'local')}). The data is "
              f"not on this backend — upload it first, or re-run against the backend "
              f"that holds it.")
        return 0
    excluded = SOURCE_FILE_EXCLUSIONS.get(source, ())
    if excluded:
        skipped = [f for f in files if os.path.basename(f) in excluded]
        files = [f for f in files if os.path.basename(f) not in excluded]
        # Say what was dropped. A coverage limit nobody prints reads as full coverage.
        print(f"  {source}: EXCLUDING {len(skipped)} file(s) whose licence is not this "
              f"source's: {[os.path.basename(f) for f in skipped]} — see "
              f"DATABASE_LICENSES_VERBATIM.md")
        if not files:
            print(f"  {source}: every parquet was excluded; nothing to catalogue.")
            return 0
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
    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")   # see the note beside the import
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
