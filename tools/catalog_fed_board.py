"""Build catalogue rows for fed_board from the store's own series sidecars.

WHY. 52,293 series sit in R2, downloadable by id and invisible to search: fed_board has 21
catalogue rows. That is the hosted-but-invisible defect tools/reconcile_serving.py now reports,
and fed_board is one of five instances of it found on 2026-08-01.

THE ID CARRIES THE FLOW: `fed_board:<FLOW>:<series_key>`, e.g. fed_board:H15:RIFSPFF_N.B.
29 keys live in two observation files - the CP/H15 commercial-paper overlap, `DTCNLRHF_N.M` in
both G19 and G20 - so a bare key names more than one series for those. _resolve_fed_board
refuses an ambiguous bare key rather than answering from whichever flow sorts first, so those
29 are unreachable without the qualifier and the catalogue must supply it. Qualifying ALL of
them rather than only the 29 keeps one id shape across the source; a reader should not have to
know which keys happen to collide.

THE 21 EXISTING ROWS ARE REPLACED, THEIR CSVs ARE NOT. Those rows use the bare form
(`fed_board:RIFSPFF_N.B`) and their derived CSVs are keyed by it in R2. Deleting those objects
would 404 every existing link for no gain, so they stay: the resolver still accepts a bare
unambiguous key, so an old link keeps working, while search now offers the qualified id. The
cost is 21 duplicated objects.

FLOW COMES FROM THE OBSERVATION FILES, NOT FROM THE SIDECAR'S `dataset` COLUMN. For fed_board
that column is a PRESENTATION GROUPING - IP.B50001.A is listed under IP_MAJOR_INDUSTRY_GROUPS,
IP_MARKET_GROUPS and IP_SPECIAL_AGGREGATES while its observations live only in G17.parquet - so
grouping by it answers "how many ways is this listed", not "which file holds it". Reading it
that way is how I first measured 219 ambiguous keys where there are 29 (ledger R214).

--apply writes; without it this prints what it would do and changes nothing.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

SOURCE = "fed_board"
LICENSE_ID = "us-public-domain"
BATCH = 20_000

FREQ_WORD = {"A": "Annual", "Q": "Quarterly", "M": "Monthly", "W": "Weekly",
             "D": "Daily", "B": "Business daily", "SA": "Semiannual"}


def flow_of_key(store: str) -> dict[str, str]:
    """series_key -> flow (observation file stem). Raises on a key in two flows only when
    the caller cannot qualify it; here every id IS qualified, so a collision is expected and
    both flows get their own row."""
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    out: dict[str, list[str]] = {}
    for f in sorted(glob.glob(os.path.join(store, "*.parquet"))):
        if f.endswith("__series.parquet"):
            continue
        flow = os.path.splitext(os.path.basename(f))[0]
        p = f.replace("\\", "/")
        for (k,) in con.execute(
                f"select distinct series_key from read_parquet('{p}')").fetchall():
            out.setdefault(k, []).append(flow)
    con.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=180.0)
    con.execute("PRAGMA busy_timeout = 180000")

    lic = con.execute("select reservable, name from license where license_id=?",
                      (LICENSE_ID,)).fetchone()
    if not lic or not lic[0]:
        print(f"licence {LICENSE_ID!r} missing or not reservable — refusing to create rows")
        return 1
    print(f"licence {LICENSE_ID}: reservable={lic[0]}  ok to catalogue")

    store = os.path.join(ROOT, "data", "clean_full", SOURCE)
    where = flow_of_key(store)
    n_amb = sum(1 for v in where.values() if len(v) > 1)
    print(f"{len(where):,} distinct series_key across the observation files "
          f"({n_amb} in more than one flow)")

    side = [f.replace("\\", "/") for f in glob.glob(os.path.join(store, "*__series.parquet"))]
    q = duckdb.connect()
    lst = "[" + ",".join(f"'{f}'" for f in side) + "]"

    # One metadata row per series_key. The sidecars cross-list, so take the row with the
    # longest long_desc: cross-listed copies should be identical, and if they are not, the
    # fullest description is the honest choice rather than whichever DuckDB returns first.
    meta_rows = q.execute(f'''
        select series_key,
               any_value(short_desc order by length(coalesce(long_desc,'')) desc) short_desc,
               any_value(long_desc  order by length(coalesce(long_desc,'')) desc) long_desc,
               any_value(freq       order by length(coalesce(long_desc,'')) desc) freq,
               any_value(unit       order by length(coalesce(long_desc,'')) desc) unit,
               min("start") start_date, max("end") end_date
        from read_parquet({lst}) group by 1''').fetchall()
    meta = {r[0]: r[1:] for r in meta_rows}
    print(f"{len(meta):,} series with sidecar metadata")

    missing_meta = [k for k in where if k not in meta]
    if missing_meta:
        print(f"WARNING {len(missing_meta):,} keys have observations but NO sidecar row — "
              f"they will be titled with the key: {missing_meta[:3]}")

    citation = json.dumps({
        "citation_short": "Board of Governors of the Federal Reserve System.",
        "citation_long": ("Board of Governors of the Federal Reserve System, statistical "
                          "releases (Data Download Program). Compiled and redistributed by "
                          "the Elkassabgi Data Library."),
        "description_processing": ("Retrieved from the Federal Reserve Board's Data Download "
                                   "Program, normalized to a long {series_key, obs_date, "
                                   "value} schema and stored as zstd Parquet, one file per "
                                   "statistical release."),
    }, ensure_ascii=False)

    rows = []
    untitled = 0
    for key, flows in sorted(where.items()):
        m = meta.get(key)
        for flow in flows:
            if m:
                short, long_d, freq, unit, sd, ed = m
                title = (short or long_d or key).strip() or key
                fw = FREQ_WORD.get((freq or "").strip())
                title = f"{title} — {fw} ({flow})" if fw else f"{title} ({flow})"
            else:
                untitled += 1
                title, freq, unit, sd, ed = f"{key} ({flow})", None, None, None, None
            rows.append((f"{SOURCE}:{flow}:{key}", SOURCE, title, (freq or None), (unit or None),
                         None, "Money & Banking", LICENSE_ID, sd, ed, citation))

    print(f"rows to write: {len(rows):,}   untitled (no sidecar row): {untitled:,}")
    for r in rows[:4]:
        print(f"   {r[0]}")
        print(f"      {r[2][:110]}   {r[8]}..{r[9]}")

    if not a.apply:
        print("\n(dry run — pass --apply to write)")
        return 0

    for i in range(0, len(rows), BATCH):
        con.executemany(
            """INSERT OR REPLACE INTO series
               (series_id,source_id,title,frequency,unit,geography,category,license_id,
                start_date,end_date,metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            rows[i:i + BATCH])
    con.commit()

    # The 21 legacy bare-key rows are superseded by their qualified twins. Their R2 CSVs are
    # LEFT IN PLACE so existing links keep resolving; only the catalogue listing moves.
    n_del = con.execute(
        "delete from series where source_id=? and series_id not in "
        "(select series_id from series where source_id=? and length(series_id) - "
        " length(replace(series_id,':','')) >= 2)", (SOURCE, SOURCE)).rowcount
    con.commit()
    n = con.execute("select count(*) from series where source_id=?", (SOURCE,)).fetchone()[0]
    print(f"\nremoved {n_del} legacy bare-key row(s); their R2 CSVs are kept so old links live")
    print(f"catalogue rows for {SOURCE}: {n:,}")
    try:
        con.execute("INSERT INTO series_fts(series_fts) VALUES('rebuild')")
        con.commit()
        print("series_fts rebuilt")
    except sqlite3.Error as e:
        print(f"series_fts rebuild skipped: {e}")
    print("\nNEXT: derive the CSVs (tools/derive_csv_bulk.py --source fed_board) and sync D1. "
          "A catalogue row without a CSV is a listed series that will not download.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
