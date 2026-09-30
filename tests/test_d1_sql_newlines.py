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


def test_the_replay_refuses_a_damaged_row_anywhere_not_only_in_the_first_50(tmp_path):
    """Review R1316 finding 1: verify_replay compared the titles of rows[:50] only - a title damaged the old Windows
    way at row 55 passed as 'verified'. Every row and every column is compared now, the index rows too."""
    import pytest
    rows = [{"series_id": f"src:K{i:03d}", "source_id": "src", "title": f"t{i}\nline two", "geography": None}
            for i in range(60)]
    for at in (0, 55):
        files = sc.emit_sql(COLS, rows, str(tmp_path / f"out{at}"))
        sid = rows[at]["series_id"]
        hit = [p for p in files if f"'{sid}'" in open(p, encoding="utf-8", newline="").read()]
        for p in hit:                                          # damage that row's title as Windows used to
            body = open(p, encoding="utf-8", newline="").read()
            body = body.replace(f"'{rows[at]['title']}'", f"'{rows[at]['title'].replace(chr(10), chr(13) + chr(10))}'")
            open(p, "w", encoding="utf-8", newline="").write(body)
        with pytest.raises(SystemExit, match="altered"):
            sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows})


def test_control_an_undamaged_60_row_replay_passes(tmp_path):
    rows = [{"series_id": f"src:K{i:03d}", "source_id": "src", "title": f"t{i}\nline two", "geography": None}
            for i in range(60)]
    files = sc.emit_sql(COLS, rows, str(tmp_path / "out"))
    sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows})


def _mode(node, pos):
    """The call's mode string, positional at `pos` or keyword mode=; None when absent or not a constant."""
    m = node.args[pos] if len(node.args) > pos else next((k.value for k in node.keywords if k.arg == "mode"), None)
    return m.value if isinstance(m, ast.Constant) and isinstance(m.value, str) else None


def _no_newline(node):
    return not any(k.arg == "newline" for k in node.keywords)


def _is_open(fn):
    return getattr(fn, "id", None) == "open" or (
        isinstance(fn, ast.Attribute) and fn.attr == "open" and getattr(fn.value, "id", None) in ("io", "codecs"))


def _sql_text_writers_without_newline(path):
    """(line, call) in a module that mentions .sql, for every TEXT-mode write with no newline= keyword:
    open/io.open/codecs.open with mode w/a (positional or mode=), os.fdopen(fd, "w"), tempfile.NamedTemporaryFile
    in text mode, and any .write_text(...) - plus every READ that feeds executescript without newline=""
    (a replay that translates line breaks checks something other than what is sent - review R1316)."""
    src = open(path, encoding="utf-8").read()
    if ".sql" not in src:
        return []
    bad = []
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        seg = (ast.get_source_segment(src, node) or "")[:80]
        name = getattr(fn, "attr", None) or getattr(fn, "id", None)
        if _is_open(fn):
            m = _mode(node, 1)
            if m and m[:1] in "wa" and "b" not in m and _no_newline(node):
                bad.append((node.lineno, seg))
        elif name == "fdopen":
            m = _mode(node, 1)
            if m and m[:1] in "wa" and "b" not in m and _no_newline(node):
                bad.append((node.lineno, seg))
        elif name == "NamedTemporaryFile":
            m = _mode(node, 0) or "w+b"
            if "b" not in m and _no_newline(node):
                bad.append((node.lineno, seg))
        elif name == "write_text" and _no_newline(node):
            bad.append((node.lineno, seg))
        elif name == "executescript" and node.args:
            # executescript(open(...).read()) with a translating read
            for sub in ast.walk(node.args[0]):
                if isinstance(sub, ast.Call) and _is_open(sub.func) and (_mode(sub, 1) or "r")[:1] == "r" \
                        and _no_newline(sub):
                    bad.append((sub.lineno, (ast.get_source_segment(src, sub) or "")[:80]))
    # `with open(p, ...) as fh:` whose body runs executescript(fh.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                call = item.context_expr
                if isinstance(call, ast.Call) and _is_open(call.func) and (_mode(call, 1) or "r")[:1] == "r" \
                        and _no_newline(call) and isinstance(item.optional_vars, ast.Name):
                    var = item.optional_vars.id
                    body = ast.Module(body=node.body, type_ignores=[])
                    if any(isinstance(c, ast.Call) and getattr(c.func, "attr", None) == "executescript"
                           and any(isinstance(x, ast.Name) and x.id == var for x in ast.walk(c)) for c in ast.walk(body)):
                        bad.append((call.lineno, (ast.get_source_segment(src, call) or "")[:80]))
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
    shapes = {
        "io.open": 'import io\nio.open(X, "w", encoding="utf-8").write("x")\n',
        "mode=": 'open(X, mode="w", encoding="utf-8").write("x")\n',
        "write_text": 'import pathlib\npathlib.Path(X).write_text("x", encoding="utf-8")\n',
        "fdopen": 'import os\nos.fdopen(3, "w", encoding="utf-8").write("x")\n',
        "NamedTemporaryFile": 'import tempfile\ntempfile.NamedTemporaryFile("w", suffix=".sql")\n',
        "replay read": 'import sqlite3\nsqlite3.connect(":memory:").executescript(open(X, encoding="utf-8").read())\n',
        "with replay": 'import sqlite3\nm = sqlite3.connect(":memory:")\nwith open(X, encoding="utf-8") as fh:\n'
                       '    m.executescript(fh.read())\n',
    }
    for label, body in shapes.items():
        q = tmp_path / "n.py"
        q.write_text('X = "a.sql"\n' + body, encoding="utf-8")
        assert _sql_text_writers_without_newline(str(q)), f"{label} must be caught"
    ok = tmp_path / "ok.py"
    ok.write_text('X = "a.sql"\nimport sqlite3\nm = sqlite3.connect(":memory:")\n'
                  'with open(X, encoding="utf-8", newline="") as fh:\n    m.executescript(fh.read())\n'
                  'open(X, "w", encoding="utf-8", newline="\\n").write("x")\n', encoding="utf-8")
    assert not _sql_text_writers_without_newline(str(ok)), "the fixed forms must pass"


# ---- review R1317: each replay check, and each loader read, killed ALONE -------------------------------------------

COLS_U = ["series_id", "source_id", "title", "unit", "geography"]


def _rows_u(n=60):
    return [{"series_id": f"src:K{i:03d}", "source_id": "src", "title": f"t{i}", "unit": f"u{i}\nx", "geography": None}
            for i in range(n)]


def test_a_damaged_non_indexed_column_at_row_55_is_refused(tmp_path):
    """Only the series compare can see this: `unit` is not in the index, so the index check cannot cover for it."""
    import pytest
    rows = _rows_u()
    files = sc.emit_sql(COLS_U, rows, str(tmp_path / "out"))
    for p in files:
        body = open(p, encoding="utf-8", newline="").read()
        if "'u55\nx'" in body:
            open(p, "w", encoding="utf-8", newline="").write(body.replace("'u55\nx'", "'u55\r\nx'"))
    with pytest.raises(SystemExit, match=r"altered src:K055 \(columns \['unit'\]\)"):
        sc.verify_replay(COLS_U, rows, files, fts_ids={r["series_id"] for r in rows})


def test_a_damaged_index_row_alone_is_refused(tmp_path):
    """Only the index check can see this: the series row is intact, only its index INSERT literal is damaged."""
    import pytest
    rows = [dict(r, title=f"t{i}\nline") for i, r in enumerate(_rows_u())]
    files = sc.emit_sql(COLS_U, rows, str(tmp_path / "out"))
    done = False
    for p in files:
        body = open(p, encoding="utf-8", newline="").read()
        at = body.find("INSERT INTO series_fts")
        lit = "'t55\nline'"
        if at >= 0 and body.find(lit, at) >= 0:
            j = body.find(lit, at)
            body = body[:j] + "'t55\r\nline'" + body[j + len(lit):]
            open(p, "w", encoding="utf-8", newline="").write(body)
            done = True
    assert done, "fixture: the index literal of row 55 must exist"
    with pytest.raises(SystemExit, match="index row for src:K055"):
        sc.verify_replay(COLS_U, rows, files, fts_ids={r["series_id"] for r in rows})


DUMP_BYTES = (b"-- a comment line\r\n"
              b"INSERT INTO t VALUES('lone\rcr');\n"
              b"INSERT INTO t VALUES('crlf\r\nin a literal');\n"
              b"INSERT INTO t VALUES('lf\nin a literal');\n")


def test_load_d1_rest_statements_keep_the_dump_bytes(tmp_path, monkeypatch):
    from core import load_d1_rest
    p = tmp_path / "dump.sql"
    p.write_bytes(DUMP_BYTES)
    monkeypatch.setattr(load_d1_rest, "DUMP", str(p))
    got = list(load_d1_rest.statements())
    assert got == ["INSERT OR REPLACE INTO t VALUES('lone\rcr');\n",
                   "INSERT OR REPLACE INTO t VALUES('crlf\r\nin a literal');\n",
                   "INSERT OR REPLACE INTO t VALUES('lf\nin a literal');\n"], got


def test_load_d1_chunked_split_dump_keeps_the_dump_bytes(tmp_path, monkeypatch):
    from core import load_d1_chunked
    p = tmp_path / "dump.sql"
    p.write_bytes(DUMP_BYTES)
    monkeypatch.setattr(load_d1_chunked, "DUMP", str(p))
    monkeypatch.setattr(load_d1_chunked, "CHUNK_DIR", str(tmp_path / "chunks"))
    monkeypatch.setattr(load_d1_chunked, "CHUNK_BYTES", 10)
    chunks = load_d1_chunked.split_dump()
    got = b"".join(open(c, "rb").read() for c in chunks)
    assert got == DUMP_BYTES.replace(b"INSERT INTO t", b"INSERT OR REPLACE INTO t"), got
