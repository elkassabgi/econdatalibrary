"""D1 SQL files carry a title's line breaks byte for byte (2026-09-30, review R1315).

emit_sql wrote its files with open(p, "w", encoding="utf-8") and no newline argument, so on Windows every "\\n" INSIDE
a string literal became "\\r\\n" on disk and was imported that way: D1 held dst:DST:NABP10's title with 0D0A where
the local catalogue has 0A (22 such titles), and three eia titles whose own "\\r\\n" had become "\\r\\r\\n". Every D1 SQL
writer now writes newline="\\n" and every replay reads newline="". The behaviour test runs the real emit_sql and
verify_replay on titles holding LF, CR and CRLF; the ratchet refuses a new text-mode .sql writer without newline=.
"""
from __future__ import annotations

import ast
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_catalog_d1 as sc  # noqa: E402

COLS = ["series_id", "source_id", "title", "geography"]
TITLES = {"src:LF": "Balance of payments\nnet", "src:CR": "Sweet\rgas", "src:CRLF": "Price of \r\nSweetener",
          "src:PLAIN": "no break"}


def _rows():
    return [{"series_id": k, "source_id": "src", "title": v, "geography": None} for k, v in TITLES.items()]


def test_the_emitted_file_carries_each_title_byte_for_byte(tmp_path):
    files = sc.emit_sql(COLS, _rows(), str(tmp_path / "out"))
    raw = b"".join(open(p, "rb").read() for p in files)
    for t in TITLES.values():
        assert t.encode("utf-8") in raw, repr(t)
    assert b"\r\r\n" not in raw and b"net\r\n" not in raw


def test_the_replay_sees_the_same_titles(tmp_path):
    rows = _rows()
    files = sc.emit_sql(COLS, rows, str(tmp_path / "out"))
    sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows})
    mem = sqlite3.connect(":memory:")
    mem.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT)")
    mem.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    mem.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, "
                "attribution TEXT, terms_url TEXT)")
    for p in files:
        with open(p, encoding="utf-8", newline="") as fh:
            mem.executescript(fh.read())
    got = dict(mem.execute("SELECT series_id, title FROM series"))
    assert got == TITLES
    assert dict(mem.execute("SELECT series_id, title FROM series_fts")) == TITLES


def _sql_text_writers_without_newline(path):
    """(line, call) for every open(..., "w"/"a", ...) in a module that writes .sql files, with no newline= keyword."""
    src = open(path, encoding="utf-8").read()
    if ".sql" not in src:
        return []
    bad = []
    for node in ast.walk(ast.parse(src)):
        fn = node.func if isinstance(node, ast.Call) else None
        is_open = fn is not None and (getattr(fn, "id", None) == "open" or (
            isinstance(fn, ast.Attribute) and fn.attr == "open" and getattr(fn.value, "id", None) in ("io", "codecs")))
        if is_open and len(node.args) >= 2:
            mode = node.args[1]
            if isinstance(mode, ast.Constant) and isinstance(mode.value, str) and mode.value[:1] in "wa" \
                    and "b" not in mode.value and not any(k.arg == "newline" for k in node.keywords):
                bad.append((node.lineno, ast.get_source_segment(src, node)[:80]))
    return bad


# writes that are NOT SQL for D1 (empty-file queue clears, JSON receipts, journals, markers) - named, so a new one is
# a decision and not a silent pass. Keyed by the exact call text; one entry covers every identical call in the file.
NOT_SQL = {
    ("core/sync_catalog_d1.py", 'open(path, "w", encoding="utf-8")'),          # the pending-queue clear
    ("tools/migrate_noaa_shard.py", 'open(FAILED_MARK, "w", encoding="utf-8")'),
    ("tools/migrate_noaa_shard.py", 'open(DONE_LIST, "a", encoding="utf-8")'),
    ("tools/rebuild_series_fts.py", 'open(JOURNAL, "a", encoding="utf-8")'),
    ("tools/refresh_flowgrain_dates.py", 'open(rpath, "w", encoding="utf-8")'),  # JSON receipts
    ("tools/refresh_sec_edgar.py", 'open(rpath, "w", encoding="utf-8")'),        # JSON receipts
    ("tools/selfhost/swap.py", 'open(tmp, "w", encoding="utf-8")'),              # instances.json
}


def _all_findings():
    found = []
    for d in ("core", "tools", "updater", "jobs"):
        for base, _dirs, names in os.walk(os.path.join(ROOT, d)):
            for name in sorted(names):
                if name.endswith(".py"):
                    rel = os.path.relpath(os.path.join(base, name), ROOT).replace("\\", "/")
                    found += [(rel, ln, seg) for ln, seg in _sql_text_writers_without_newline(os.path.join(ROOT, rel))
                              if (rel, seg) not in NOT_SQL]
    return found


def test_every_text_writer_in_a_sql_emitting_module_names_its_newline():
    found = _all_findings()
    assert not found, "\n".join(f"{r}:{ln}: {s}" for r, ln, s in found)


def test_the_ratchet_can_fail(tmp_path):
    p = tmp_path / "m.py"
    p.write_text('X = "a.sql"\nwith open(X, "w", encoding="utf-8") as fh:\n    fh.write("x")\n', encoding="utf-8")
    assert _sql_text_writers_without_newline(str(p))
    q = tmp_path / "n.py"
    q.write_text('import io\nX = "a.sql"\nio.open(X, "w", encoding="utf-8").write("x")\n', encoding="utf-8")
    assert _sql_text_writers_without_newline(str(q)), "io.open must be caught too"
