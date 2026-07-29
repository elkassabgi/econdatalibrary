"""tools/catalog_pxweb_flowgrain.py — flow-grain (per-table) catalog for the 9 PxWeb giants.

broaden_catalog DEFERS these sources (each >50k series). Their natural, honest grain is
PER-TABLE: one catalog row per PxWeb table, titled with the source's OWN table title (never
fabricated), one downloadable CSV per table. This tool builds those rows.

Per source: scan the parquet store for distinct table prefixes (series_key minus the =dim
parts), compute each table's real min/max obs_date + row count, join the real title from the
source's cached _catalog.json (id/path match; falls back to the table id — honest, not made
up), and insert one `series` row:
    series_id = "<source>:<full_prefix>"   (derive strips "<source>:" -> the parquet prefix)
    title     = source table title | table id
    start/end = real min/max obs_date ;  frequency = NULL (PxWeb parquets carry no freq col)
    license_id/source_id = the existing source row
Idempotent per source (DELETE+reinsert). Does NOT touch the license table / reservable flags.

  python tools/catalog_pxweb_flowgrain.py --dry-run            # scan + report, write nothing
  python tools/catalog_pxweb_flowgrain.py [--source ssb ...]   # catalog (writes catalog.db)
"""
from __future__ import annotations
import argparse, glob, json, os, sqlite3, time
import pyarrow.compute as pc
import pyarrow.parquet as pq

# DERIVED, NEVER HARDCODED. These were absolute D: paths from the workstation cutover; the
# store now lives on E:, so a re-run would have globbed a directory that does not exist and
# reported "0 tables" for every source — a silent zero, not an error. Deriving from __file__
# is the convention the ingest jobs already state for exactly this reason.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data", "clean_full")
CATALOG = os.environ.get("ECONDL_CATALOG") or os.path.join(ROOT, "data", "catalog.db")
if not os.path.isdir(DATA):                    # fail loudly rather than catalogue nothing
    raise SystemExit(f"parquet store not found at {DATA} — refusing to report empty scans")
SOURCES = ["ssb", "stat_slovenia", "stat_latvia", "dst", "scb", "statfin", "hagstofa", "stat_estonia", "bfs"]
PREFIX_RE = r"^(?P<p>.*?):[^:=]*="   # capture full prefix up to the first ":dimname="


def _norm(tid: str) -> str:
    """Normalize a table id for title matching: drop a trailing .px/.PX, keep the rest."""
    t = tid.strip()
    if t.lower().endswith(".px"):
        t = t[:-3]
    return t


def _last_seg(prefix: str) -> str:
    return _norm(prefix.split(":")[-1])


# (title field, id fields) per catalogue shape. PxWeb ships {"text","id"/"path"}; CSO PxStat
# ships {"MtrTitle","MtrCode"}. Both are the PUBLISHER'S OWN table title — the point of
# reading these at all is that a table's name is never invented here (the fallback is the
# real table id, which is honest if terse).
_TITLE_SHAPES = [("text", ("id", "path")), ("MtrTitle", ("MtrCode",))]


def build_title_map(src: str) -> dict:
    """{normalized_table_id -> title} from the source's _catalog.json (empty if absent)."""
    catp = os.path.join(DATA, src, "_catalog.json")
    if not os.path.exists(catp):
        return {}
    cat = json.load(open(catp, encoding="utf-8"))
    if not isinstance(cat, list):
        return {}
    m = {}
    for e in cat:
        if not isinstance(e, dict):
            continue
        for tfield, keyfields in _TITLE_SHAPES:
            text = (e.get(tfield) or "").strip()
            if not text:
                continue
            for keyfield in keyfields:
                v = e.get(keyfield)
                if not v:
                    continue
                k = _norm(str(v).split("/")[-1])
                m.setdefault(k, text)
                m.setdefault(k.lower(), text)
    return m


def scan_prefixes(src: str) -> dict:
    """{prefix -> [min_iso, max_iso, count]} over all of the source's parquet files."""
    agg: dict[str, list] = {}
    files = sorted(f for f in glob.glob(os.path.join(DATA, src, "*.parquet")))
    for f in files:
        pf = pq.ParquetFile(f)
        for batch in pf.iter_batches(columns=["series_key", "obs_date"], batch_size=500_000):
            keys = batch.column("series_key")
            p = pc.extract_regex(keys, pattern=PREFIX_RE).field("p")
            # extract_regex yields an EMPTY (not null) capture for a key with no "=" dim
            # part (a time-only table, key == the table prefix); treat null-OR-empty as
            # "no usable prefix" and fall back to the whole key, else those tables all
            # collapse into one junk "<source>:" entry.
            usable = pc.and_(pc.invert(pc.is_null(p)), pc.not_equal(p, ""))
            pref = pc.if_else(usable, p, keys)
            import pyarrow as pa
            tbl = pa.table({"p": pref, "d": batch.column("obs_date")})
            grp = tbl.group_by("p").aggregate([("d", "min"), ("d", "max"), ("d", "count")])
            ps = grp.column("p").to_pylist()
            mn = grp.column("d_min").to_pylist()
            mx = grp.column("d_max").to_pylist()
            ct = grp.column("d_count").to_pylist()
            for p, a, b, c in zip(ps, mn, mx, ct):
                ai = a.isoformat() if a is not None else None
                bi = b.isoformat() if b is not None else None
                cur = agg.get(p)
                if cur is None:
                    agg[p] = [ai, bi, c]
                else:
                    if ai and (cur[0] is None or ai < cur[0]): cur[0] = ai
                    if bi and (cur[1] is None or bi > cur[1]): cur[1] = bi
                    cur[2] += c
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--source", action="append")
    a = ap.parse_args()
    srcs = a.source or SOURCES

    conn = sqlite3.connect(CATALOG)
    conn.row_factory = sqlite3.Row
    src_license = {r["source_id"]: r["license_id"]
                   for r in conn.execute("SELECT source_id, license_id FROM source")}

    grand = 0
    for src in srcs:
        t0 = time.time()
        tmap = build_title_map(src)
        agg = scan_prefixes(src)
        lic = src_license.get(src)
        rows, matched = [], 0
        for pref, (mn, mx, _ct) in agg.items():
            tid = _last_seg(pref)
            title = tmap.get(tid) or tmap.get(tid.lower())
            if title:
                matched += 1
            else:
                title = tid  # honest fallback: the real table id, never fabricated
            sid = f"{src}:{pref}"
            rows.append((sid, src, title[:500], None, None, None, None, lic, mn, mx, None, "{}"))
        rate = f"{matched/len(rows)*100:.0f}%" if rows else "n/a"
        print(f"{src:16} tables={len(rows):>7,}  title-match={rate:>4}  "
              f"license={lic}  {round(time.time()-t0,1)}s", flush=True)
        if not a.dry_run and rows:
            conn.execute("DELETE FROM series WHERE source_id=?", (src,))
            conn.executemany(
                "INSERT OR REPLACE INTO series (series_id,source_id,title,frequency,unit,geography,"
                "category,license_id,start_date,end_date,last_updated,metadata) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            conn.commit()
        grand += len(rows)

    if not a.dry_run:
        try:
            conn.execute("DELETE FROM series_fts;")
            conn.execute("INSERT INTO series_fts(series_id,title,geography) "
                         "SELECT series_id,title,geography FROM series;")
            conn.commit()
            print("[fts] rebuilt")
        except sqlite3.OperationalError as e:
            print(f"[fts] skipped: {e}")
    conn.close()
    print(f"\n{'DRY-RUN ' if a.dry_run else ''}TOTAL flow-grain tables: {grand:,}")


if __name__ == "__main__":
    main()
