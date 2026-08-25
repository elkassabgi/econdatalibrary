"""Re-running catalog_complete must not add a second copy of a series to the search index.

`tools/catalog_complete.py` inserted into `series` with INSERT OR IGNORE and into `series_fts`
with a BARE INSERT, two lines apart. An FTS5 virtual table has no unique constraint, so every
re-run of the tool on a source appended another full copy of its rows to the search index.

Measured on the live D1 before the fix:

    boc            102,882 fts rows / 12,862 ids  = exactly 8.00 copies of every id
    wid                                             4.00x
    cepii_gravity                                   3.04x
    global         23,934,659 fts rows / 10,348,125 series = 2.31x

A user searching `Lynx` got 100 rows containing 16 distinct ids. That defect was found, then
RETRACTED on a one-source sample where the copies happened to sit ~2.4M rowids apart and a
shallow page looked clean, then re-found by an adversarial reviewer (R482, R486).

`INSERT OR IGNORE` is not the fix — there is no constraint to ignore. The id must be deleted
first. This test runs the insert path twice and pins that the second run changes nothing.
"""
from __future__ import annotations

import os
import sqlite3

import pytest


SCHEMA = """
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT,
  unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,
  last_updated TEXT, metadata TEXT);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
"""


def _insert_batch(con, source, keys, *, delete_first: bool):
    """The two shapes: shipped (bare INSERT) and fixed (delete-then-insert)."""
    rows = [(f"{source}:{k}", source, k, None, None, None, None, "lic",
             None, None, None, "{}") for k in keys]
    con.executemany(
        "INSERT OR IGNORE INTO series (series_id,source_id,title,frequency,unit,geography,"
        "category,license_id,start_date,end_date,last_updated,metadata) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    if delete_first:
        con.executemany("DELETE FROM series_fts WHERE series_id=?",
                        [(f"{source}:{k}",) for k in keys])
    con.executemany("INSERT INTO series_fts(series_id,title,geography) VALUES (?,?,?)",
                    [(f"{source}:{k}", k, None) for k in keys])
    con.commit()


def _counts(con, source):
    s = con.execute("SELECT COUNT(*) FROM series WHERE source_id=?", (source,)).fetchone()[0]
    f = con.execute("SELECT COUNT(*) FROM series_fts WHERE series_id LIKE ?",
                    (source + ":%",)).fetchone()[0]
    return s, f


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    c.executescript(SCHEMA)
    yield c
    c.close()


def test_the_shipped_shape_duplicates_and_this_test_can_see_it(con):
    """Negative control (R346): prove the test detects the defect it guards against."""
    keys = [f"k{i}" for i in range(50)]
    _insert_batch(con, "boc", keys, delete_first=False)
    _insert_batch(con, "boc", keys, delete_first=False)
    s, f = _counts(con, "boc")
    assert s == 50, "series is INSERT OR IGNORE, so it must not grow"
    assert f == 100, f"the bare INSERT must double the index; got {f}"


def test_the_fix_is_idempotent(con):
    keys = [f"k{i}" for i in range(50)]
    for _ in range(4):
        _insert_batch(con, "boc", keys, delete_first=True)
    s, f = _counts(con, "boc")
    assert s == 50
    assert f == 50, f"four runs must leave one row per id; got {f}"


def test_no_id_has_more_than_one_index_row(con):
    keys = [f"k{i}" for i in range(20)]
    _insert_batch(con, "boc", keys, delete_first=True)
    _insert_batch(con, "boc", keys, delete_first=True)
    dupes = con.execute(
        "SELECT series_id, COUNT(*) c FROM series_fts GROUP BY series_id HAVING c > 1").fetchall()
    assert not dupes, f"duplicated ids: {dupes[:3]}"


def test_a_second_source_is_untouched(con):
    """The DELETE is scoped by id, so it must not disturb a neighbouring source."""
    _insert_batch(con, "boc", ["a", "b"], delete_first=True)
    _insert_batch(con, "wid", ["a", "b"], delete_first=True)
    _insert_batch(con, "boc", ["a", "b"], delete_first=True)
    assert _counts(con, "wid") == (2, 2)
    assert _counts(con, "boc") == (2, 2)


def test_the_tool_actually_deletes_before_inserting():
    """Pin the shipped source, so a future edit cannot quietly restore the bare INSERT."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(root, "tools", "catalog_complete.py"), encoding="utf-8").read()
    i = src.index("INSERT INTO series_fts")
    window = src[max(0, i - 700):i]
    assert "DELETE FROM series_fts WHERE series_id=?" in window, (
        "the FTS insert is not preceded by a delete — every re-run will duplicate the index")
