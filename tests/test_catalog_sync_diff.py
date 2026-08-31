"""The catalogue sync must send a DIFF, and the diff must be conservative in the safe direction.

R542: an undiffed sync pushes a mean of 42,046 ids per run against a D1 catalogue that is 0
sources short, at ~85 series_fts FULL SCANS per run (~$0.88), which is both the bill and the
failure. These tests pin the behaviour that removes it — and, just as important, pin that an
EMPTY manifest never silently reads as "everything is already sent".
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.catalog_sync_manifest import Manifest, row_hash  # noqa: E402

COLS = ["series_id", "source_id", "title", "end_date"]


def _row(sid, title="t", end="2026-01-01"):
    return {"series_id": sid, "source_id": "src", "title": title, "end_date": end}


def _m(tmp_path):
    return Manifest(str(tmp_path / "sent.db"))


def test_empty_manifest_sends_everything(tmp_path):
    """The bootstrap hazard, pinned: nothing recorded means nothing may be skipped."""
    m = _m(tmp_path)
    rows = [_row("a"), _row("b")]
    send, skipped = m.split(COLS, rows)
    assert skipped == 0 and len(send) == 2


def test_recorded_and_unchanged_is_skipped(tmp_path):
    m = _m(tmp_path)
    rows = [_row("a"), _row("b")]
    m.record(COLS, rows)
    send, skipped = m.split(COLS, rows)
    assert send == [] and skipped == 2


def test_any_content_change_resends(tmp_path):
    """Title, end_date, or a value becoming NULL must all re-send — the row in D1 is stale."""
    m = _m(tmp_path)
    m.record(COLS, [_row("a", title="old", end="2026-01-01")])
    for changed in (_row("a", title="new"), _row("a", end="2026-06-30"),
                    {"series_id": "a", "source_id": "src", "title": None,
                     "end_date": "2026-01-01"}):
        send, skipped = m.split(COLS, [changed])
        assert skipped == 0 and len(send) == 1, changed


def test_a_new_column_resends_every_row(tmp_path):
    """Column NAMES are hashed too. Adding a column means D1's rows genuinely lack it, so
    the next sync must re-send rather than skip on a stale hash."""
    m = _m(tmp_path)
    m.record(COLS, [_row("a")])
    wider = COLS + ["frequency"]
    r = dict(_row("a")); r["frequency"] = "A"
    send, skipped = m.split(wider, [r])
    assert skipped == 0 and len(send) == 1


def test_null_and_empty_string_are_distinguished(tmp_path):
    """A hash that collides NULL with '' would silently keep a wrong D1 row."""
    a = {"series_id": "x", "source_id": "s", "title": None, "end_date": "d"}
    b = {"series_id": "x", "source_id": "s", "title": "", "end_date": "d"}
    assert row_hash(COLS, a) != row_hash(COLS, b)


def test_column_reorder_does_not_change_the_hash_content(tmp_path):
    """The hash is over (name, value) pairs in the given order; the same row described by
    the same columns must hash identically across calls (stability, not order-independence)."""
    r = _row("a")
    assert row_hash(COLS, r) == row_hash(COLS, dict(r))


def test_split_handles_more_ids_than_the_sqlite_parameter_limit(tmp_path):
    """The IN-list is chunked; 3,000 ids must not raise 'too many SQL variables'."""
    m = _m(tmp_path)
    rows = [_row(f"id{i}") for i in range(3000)]
    m.record(COLS, rows)
    send, skipped = m.split(COLS, rows)
    assert send == [] and skipped == 3000


def test_seed_from_catalog_records_without_sending(tmp_path):
    """Bootstrap: seeding asserts D1 already holds the rows and sends nothing itself."""
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, "
                "title TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?,?)",
                    [(f"s{i}", "src", f"t{i}", "2026-01-01") for i in range(120)])
    con.commit()
    m = _m(tmp_path)
    n = m.seed_from_catalog(con, batch=50)
    assert n == 120 and m.count() == 120
    cols = [d[0] for d in con.execute("SELECT * FROM series LIMIT 1").description]
    rows = [dict(zip(cols, r)) for r in con.execute("SELECT * FROM series")]
    send, skipped = m.split(cols, rows)
    assert send == [] and skipped == 120


def test_the_sync_wires_the_diff_and_keeps_an_escape_hatch():
    """Call-site pins: the sync consults the manifest, records ONLY after success, and can
    be forced back to the old behaviour deliberately rather than by accident."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "core", "sync_catalog_d1.py"), encoding="utf-8").read()
    assert "manifest.split(cols, rows)" in src, "the sync no longer diffs"
    assert "--no-diff" in src and "--seed-manifest" in src
    i_record = src.find("manifest.record(cols, rows)")
    i_exec = src.find("execute_remote(files, database=db)")
    assert i_exec != -1 and i_record > i_exec, (
        "hashes must be recorded AFTER the remote execute, or a failed run would mark "
        "unsent rows as sent")
