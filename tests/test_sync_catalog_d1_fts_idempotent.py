"""The D1 catalogue sync must not add a second copy of a series to the search index.

`core/sync_catalog_d1.py` emitted `INSERT OR REPLACE INTO series` and, a few lines later, a BARE
`INSERT INTO series_fts`. An FTS5 virtual table has no unique constraint, so every whole-source
sync appended another full copy of that source's rows to the index. The bare insert was
deliberate, and the comment adopting it stated a cost model that was wrong in both terms:

    "Duplicate FTS rows only ever cost a repeated search hit, whereas a MISSING one makes the
     series unfindable — the asymmetry favours inserting."

Measured on the live D1 before the fix:

    boc            102,882 fts rows / 12,862 ids  = exactly 8.00 copies of every id
    cepii_gravity  every id >= 3 copies, plus exactly 50,000 ids carrying a 4th — the signature
                   of three full passes and one that stopped on a ROWS_PER_STMT boundary, and
                   that chunking lives in this very function
    global         23,934,659 fts rows / 10,348,125 series = 2.31x

A user searching `Lynx` got 100 rows containing 16 distinct ids, and every reported `total` was
inflated by the same factor — so the cost is not "a repeated search hit". The storage cost is
~13.6M rows in a database at 8.36 GB against a hard 10 GB ceiling, which the comment never
weighed. R482 recorded the over-claim, R486 the wrong retraction, R487 the wrong attribution:
I first "fixed" `tools/catalog_complete.py`, which returns early when nothing is missing and
therefore cannot produce more than one extra copy per re-inserted id.

These tests REPLAY the emitted SQL into an in-memory SQLite, because the defect is a property of
the statements, not of the Python that builds them.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOD = os.path.join(ROOT, "core", "sync_catalog_d1.py")

SCHEMA = """
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
"""


@pytest.fixture(scope="module")
def sync():
    spec = importlib.util.spec_from_file_location("sync_catalog_d1_undertest", MOD)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _fts_statements(sync, rows):
    """Just the series_fts statements emit_sql would produce, in order."""
    out = []
    for i in range(0, len(rows), sync.ROWS_PER_STMT):
        ch = rows[i:i + sync.ROWS_PER_STMT]
        ids = ",".join(sync._lit(r["series_id"]) for r in ch)
        out.append(f"DELETE FROM series_fts WHERE series_id IN ({ids});")
        vals = ",\n  ".join(
            "(%s,%s,%s)" % (sync._lit(r["series_id"]), sync._lit(r.get("title")),
                            sync._lit(r.get("geography"))) for r in ch)
        out.append("INSERT INTO series_fts (series_id,title,geography) VALUES\n  " + vals + ";")
    return out


def _rows(n, src="boc"):
    return [{"series_id": f"{src}:k{i}", "source_id": src, "title": f"t{i}", "geography": None}
            for i in range(n)]


def test_the_shipped_source_deletes_before_inserting(sync):
    """Pin the file itself, so a future edit cannot quietly restore the bare INSERT."""
    src = open(MOD, encoding="utf-8").read()
    i = src.index("INSERT INTO series_fts")
    window = src[max(0, i - 900):i]
    assert "DELETE FROM series_fts WHERE series_id IN" in window, (
        "the FTS insert is not preceded by a chunk delete — every sync will duplicate the index")


def test_replaying_the_sync_four_times_leaves_one_row_per_id(sync):
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    rows = _rows(40)
    for _ in range(4):
        for st in _fts_statements(sync, rows):
            con.execute(st)
        con.commit()
    n = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
    d = con.execute("SELECT COUNT(DISTINCT series_id) FROM series_fts").fetchone()[0]
    con.close()
    assert (n, d) == (40, 40), f"four syncs left {n} rows for {d} ids"


def test_the_bare_insert_shape_duplicates_and_this_test_can_see_it(sync):
    """Negative control (R346/R414): the guard must be able to observe the defect."""
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    rows = _rows(40)
    for _ in range(4):
        for st in _fts_statements(sync, rows):
            if st.startswith("DELETE"):
                continue                     # the shipped-before shape: insert only
            con.execute(st)
        con.commit()
    n = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
    con.close()
    assert n == 160, f"the bare-insert shape must reach 4.00x; got {n}"


def test_the_delete_is_scoped_to_the_chunk_not_the_source(sync):
    """A neighbouring source must survive a sync of this one."""
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    for st in _fts_statements(sync, _rows(5, "wid")):
        con.execute(st)
    for _ in range(3):
        for st in _fts_statements(sync, _rows(5, "boc")):
            con.execute(st)
    con.commit()
    wid = con.execute("SELECT COUNT(*) FROM series_fts WHERE series_id LIKE 'wid:%'").fetchone()[0]
    boc = con.execute("SELECT COUNT(*) FROM series_fts WHERE series_id LIKE 'boc:%'").fetchone()[0]
    con.close()
    assert (wid, boc) == (5, 5)


def test_a_chunk_boundary_does_not_leave_a_partial_extra_copy(sync):
    """cepii_gravity's 3.04x was three full passes plus one that stopped after 50,000 ids.

    That fractional ratio is only producible by repeated whole-source insertion with one
    truncated pass, so a boundary-crossing row count is the case to pin.
    """
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEMA)
    rows = _rows(sync.ROWS_PER_STMT * 2 + 7)
    for _ in range(3):
        for st in _fts_statements(sync, rows):
            con.execute(st)
        con.commit()
    n = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
    con.close()
    assert n == len(rows), f"expected {len(rows)} rows across chunk boundaries, got {n}"
