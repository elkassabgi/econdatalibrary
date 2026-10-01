"""Find - and with --apply clean - catalogue titles that hold a line break (Ahmed, 2026-10-01; rule = core.titles).

The D1 sync already cleans every title it sends (core/sync_catalog_d1.py), so this tool is for the LOCAL copy - the
catalogue the self-hosted origin will serve - and it is the detector: --check exits 1 when any title holds a CR/LF.

  python tools/clean_catalogue_titles.py --check             # count; exit 1 if any (read-only, ~15-35 s)
  python tools/clean_catalogue_titles.py                     # the plan, writes nothing
  python tools/clean_catalogue_titles.py --apply [--ids-out F] [--queue]
  add --i18n to also check localized titles in series.metadata "titles" (a much slower scan of the JSON text)

--apply is ONE transaction taken with BEGIN IMMEDIATE BEFORE the scan (review R1323: a plan made outside it can go
stale - the re-cataloguers rebuild series_fts and every rowid moves). Inside it, for each title: find its ONE index
row by an FTS MATCH on the title's longest word (index-only, no scan of series_fts) filtered to that series_id and
title; UPDATE series ... WHERE series_id=? AND title=? (exactly 1 row); DELETE the index row WHERE rowid=? AND
series_id=? AND title=? (exactly 1 row); INSERT the cleaned index row; then verify every row BEFORE commit and roll
back on any mismatch. A title whose index row cannot be found that way is refused, never guessed. --ids-out writes
the cleaned ids for `core/sync_catalog_d1.py --ids-file`; --queue appends them to the pending sync queue instead.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import catalog_path  # noqa: E402
from core.titles import clean_title  # noqa: E402

BROKEN = ("SELECT series_id, title, geography FROM series "
          "WHERE instr(title, char(10)) > 0 OR instr(title, char(13)) > 0")
I18N = ("SELECT series_id, metadata FROM series WHERE instr(metadata, '\"titles\"') > 0 "
        "AND (instr(metadata, '\\n') > 0 OR instr(metadata, '\\r') > 0)")


def broken_titles(con) -> list[tuple]:
    return con.execute(BROKEN).fetchall()


def broken_i18n(con) -> list[tuple]:
    """(series_id, metadata, cleaned_metadata) for rows whose localized titles hold a real line break."""
    out = []
    for sid, md in con.execute(I18N):
        try:
            obj = json.loads(md)
        except ValueError:
            continue
        titles = obj.get("titles") if isinstance(obj, dict) else None
        if not isinstance(titles, dict):
            continue
        fixed = {k: (clean_title(v) if isinstance(v, str) else v) for k, v in titles.items()}
        if fixed != titles:
            obj["titles"] = fixed
            out.append((sid, md, json.dumps(obj, ensure_ascii=False)))
    return out


def _index_row(con, sid, title):
    words = re.findall(r"\w{3,}", title)
    if not words:
        return []
    q = '"%s"' % max(words, key=len).replace('"', '""')
    return [r for r in con.execute("SELECT rowid, series_id, title FROM series_fts WHERE series_fts MATCH ?",
                                   (f"title:{q}",)) if r[1] == sid and r[2] == title]


def apply(con, i18n: bool) -> tuple[list[str], list[str]]:
    """Clean inside ONE transaction; returns (cleaned ids, refused ids). Raises (after rollback) on any mismatch."""
    con.execute("BEGIN IMMEDIATE")
    try:
        cleaned, refused = [], []
        for sid, title, geo in broken_titles(con):
            new = clean_title(title)
            hits = _index_row(con, sid, title)
            if len(hits) != 1:
                refused.append(sid)
                continue
            if con.execute("UPDATE series SET title=? WHERE series_id=? AND title=?", (new, sid, title)).rowcount != 1:
                raise RuntimeError(f"{sid}: series row changed under the update")
            if con.execute("DELETE FROM series_fts WHERE rowid=? AND series_id=? AND title=?",
                           (hits[0][0], sid, title)).rowcount != 1:
                raise RuntimeError(f"{sid}: index row changed under the delete")
            con.execute("INSERT INTO series_fts(series_id, title, geography) VALUES (?,?,?)", (sid, new, geo))
            cleaned.append(sid)
        if i18n:
            for sid, old_md, new_md in broken_i18n(con):
                if con.execute("UPDATE series SET metadata=? WHERE series_id=? AND metadata=?",
                               (new_md, sid, old_md)).rowcount != 1:
                    raise RuntimeError(f"{sid}: metadata changed under the update")
                if sid not in cleaned:
                    cleaned.append(sid)
        # verify BEFORE commit
        for sid in cleaned:
            t = con.execute("SELECT title FROM series WHERE series_id=?", (sid,)).fetchone()[0]
            if t != clean_title(t) or "\n" in t or "\r" in t:
                raise RuntimeError(f"{sid}: title still holds a line break")
            if len(_index_row(con, sid, t)) != 1:
                raise RuntimeError(f"{sid}: not exactly one cleaned index row")
        con.commit()
        return cleaned, refused
    except Exception:
        con.rollback()
        raise


def main(argv=None) -> int:
    # allow_abbrev=False: __main__ takes the writer lock on the exact "--apply"; a prefix like "--app" must not
    # reach main() as --apply without the lock (tests/test_catalogue_writers_main.py)
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    ap.add_argument("--check", action="store_true", help="count only; exit 1 if any title holds a line break")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i18n", action="store_true", help="also localized titles in metadata (slow)")
    ap.add_argument("--ids-out", help="write the cleaned ids here (for sync_catalog_d1 --ids-file)")
    ap.add_argument("--queue", action="store_true", help="append the cleaned ids to the pending sync queue")
    a = ap.parse_args(argv)
    con = catalog_path.connect(write=a.apply)
    con.execute("PRAGMA busy_timeout = 300000")
    if not a.apply:
        rows = broken_titles(con)
        i18n = broken_i18n(con) if a.i18n else []
        con.close()
        print(f"titles with a line break: {len(rows):,}" + (f"; localized-title rows: {len(i18n):,}" if a.i18n else ""))
        for sid, t, _g in rows[:10]:
            print(f"  {sid}: {t!r} -> {clean_title(t)!r}")
        if a.check:
            return 1 if rows or i18n else 0
        print("DRY: nothing written (--apply to clean)")
        return 0
    cleaned, refused = apply(con, a.i18n)      # the writer lock is the entry's (see __main__), held around main()
    con.close()
    print(f"cleaned {len(cleaned):,} row(s); refused {len(refused):,} (index row not found by MATCH): {refused[:10]}")
    if a.ids_out:
        open(a.ids_out, "w", encoding="utf-8", newline="\n").write("".join(f"{s}\n" for s in cleaned))
    if a.queue and cleaned:
        from core.sync_catalog_d1 import PENDING  # noqa: PLC0415
        with open(PENDING, "a", encoding="utf-8", newline="\n") as fh:
            fh.write("".join(f"{s}\n" for s in cleaned))
    return 1 if refused else 0


if __name__ == "__main__":
    if "--apply" not in sys.argv[1:]:
        sys.exit(main())                     # reads only (--check / the plan): no lock
    else:
        with catalog_path.write_session():   # after T0: the single-writer lock
            sys.exit(main())
