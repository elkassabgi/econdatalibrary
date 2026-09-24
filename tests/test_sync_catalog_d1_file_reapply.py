"""Every file the daily catalogue sync retries must be safe to apply twice (R1191 finding 2).

wrangler 3.114 can exit nonzero AFTER the server took the file (a failure in the poll step that follows the
import), and a retry then re-applies it. `series` rows are INSERT OR REPLACE, but series_fts is fts5 with
no unique constraint: a re-applied bare INSERT adds a second copy (R1185, the 2.31x index). So:
  - the id-list form keeps each DELETE in the same FILE as the INSERTs it covers - every file re-applies
    to the same state, and keeps its retries;
  - the whole-source range form has ONE DELETE; the files after it hold bare INSERTs and are sent ONCE.
These tests re-apply real emitted files to an in-memory database and count the index rows."""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import sync_catalog_d1 as sc  # noqa: E402
from core import sync_state_d1  # noqa: E402
from core import d1_remote  # noqa: E402

COLS = ["series_id", "source_id", "title"]
SCHEMA = """
CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT);
CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
CREATE TABLE source_counts(source_id TEXT PRIMARY KEY, n INTEGER NOT NULL);
"""


def _rows(src, n):
    return [{"series_id": f"{src}:K{i:05d}", "source_id": src, "title": f"a title for series {i}",
             "geography": None} for i in range(n)]


def _emit(tmp_path, rows, cap, monkeypatch, fts_range_source=None):
    monkeypatch.setattr(sc, "MAX_FILE_BYTES", cap)          # several files from a small test set
    return sc.emit_sql(COLS, rows, str(tmp_path / "out"), None, fts_range_source=fts_range_source)


def _apply(db, path):
    with open(path, encoding="utf-8") as fh:
        db.executescript(fh.read())


def _fts_rows(db):
    return db.execute("SELECT COUNT(*), COUNT(DISTINCT series_id) FROM series_fts").fetchone()


def test_the_id_list_form_keeps_each_delete_with_its_inserts(tmp_path, monkeypatch):
    rows = _rows("boc", 1300)                                  # three 500-id blocks
    files = _emit(tmp_path, rows, 40_000, monkeypatch)
    fts_files = [p for p in files if "INSERT INTO series_fts" in open(p, encoding="utf-8").read()]
    assert len(fts_files) >= 3, f"precondition: the blocks landed in several files ({len(files)} files)"
    for p in files:
        text = open(p, encoding="utf-8").read()
        assert text.count("DELETE FROM series_fts") == text.count("DELETE FROM series_fts WHERE series_id IN")
        assert sc.reapplicable(p), f"{os.path.basename(p)} holds FTS inserts whose DELETE is in another file"


def test_every_id_list_file_applied_twice_leaves_one_index_row_per_id(tmp_path, monkeypatch):
    rows = _rows("boc", 1300)
    files = _emit(tmp_path, rows, 40_000, monkeypatch)
    for again in files:                                        # each file re-applied once, as a retry would
        db = sqlite3.connect(":memory:")
        db.executescript(SCHEMA)
        for p in files:
            _apply(db, p)
            if p == again:
                _apply(db, p)
        assert _fts_rows(db) == (1300, 1300), f"re-applying {os.path.basename(again)} duplicated the index"
        db.close()


def test_the_range_form_only_the_file_with_the_delete_reapplies(tmp_path, monkeypatch):
    rows = _rows("idb", 1300)
    files = _emit(tmp_path, rows, 40_000, monkeypatch, fts_range_source="idb")
    texts = [open(p, encoding="utf-8").read() for p in files]
    with_delete = [i for i, t in enumerate(texts) if "DELETE FROM series_fts" in t]
    after = [i for i, t in enumerate(texts) if "INSERT INTO series_fts" in t and i > with_delete[0]]
    assert len(with_delete) == 1 and after, "precondition: the range form spilled bare INSERTs into later files"
    for i, p in enumerate(files):
        assert sc.reapplicable(p) is (i not in after), os.path.basename(p)
    # and the classification is right: re-applying a file judged unsafe DOES duplicate
    db = sqlite3.connect(":memory:")
    db.executescript(SCHEMA)
    for p in files:
        _apply(db, p)
    _apply(db, files[after[0]])
    n, d = _fts_rows(db)
    assert d == 1300 and n > 1300, "a bare-INSERT file re-applied must duplicate - else the test sees nothing"


def test_a_block_over_the_file_cap_is_refused_not_split(tmp_path, monkeypatch):
    with pytest.raises(SystemExit, match="over the"):
        _emit(tmp_path, _rows("boc", 600), 5_000, monkeypatch)


def test_a_file_that_does_not_reapply_is_sent_once(tmp_path, monkeypatch):
    rows = _rows("idb", 1300)
    files = _emit(tmp_path, rows, 40_000, monkeypatch, fts_range_source="idb")
    sent = []
    monkeypatch.setattr(sc, "execute_remote", lambda fs, database=None, **kw: sent.append((fs, database, kw)))
    sc.execute_plans([("econ-catalog-climate", rows, files)])
    assert [s[0] for s in sent] == [[p] for p in files], "one call per file, in order"
    for (fs, database, kw) in sent:
        safe = sc.reapplicable(fs[0])
        assert database == "econ-catalog-climate"
        assert kw == {"idempotent": safe, "tries": 4 if safe else 1}, (os.path.basename(fs[0]), kw)
    assert any(kw["tries"] == 1 for _, _, kw in sent) and any(kw["tries"] == 4 for _, _, kw in sent)


def test_a_failed_file_after_the_range_delete_restarts_once_from_the_delete(tmp_path, monkeypatch, capsys):
    """R1195 finding 1: a bare-INSERT file after the whole-source DELETE ran once and, on failure, left the
    index partly rebuilt with no word on how to recover. Now it goes back ONCE to the DELETE file."""
    rows = _rows("idb", 1300)
    files = _emit(tmp_path, rows, 40_000, monkeypatch, fts_range_source="idb")
    unsafe = [p for p in files if not sc.reapplicable(p)]
    delete_file = next(p for p in files if sc._has_fts_delete(p))
    sent, fail_on = [], {unsafe[0]: 1}

    def execute_remote(fs, database=None, **kw):
        sent.append(fs[0])
        if fail_on.get(fs[0]):
            fail_on[fs[0]] -= 1
            raise SystemExit("FATAL: wrangler failed")
    monkeypatch.setattr(sc, "execute_remote", execute_remote)
    sc.execute_plans([("econ-catalog", rows, files)])
    first = sent.index(unsafe[0])
    assert sent[first + 1] == delete_file and sent[-1] == files[-1], "back to the DELETE file, then to the end"
    # a replay of exactly what was sent leaves one index row per id
    import sqlite3 as _sq
    db = _sq.connect(":memory:")
    db.executescript(SCHEMA)
    for k, p in enumerate(sent):
        if k == first:
            continue                                            # the failed send applied nothing
        _apply(db, p)
    assert _fts_rows(db) == (1300, 1300)
    fail_on = {unsafe[0]: 2}
    sent.clear()
    monkeypatch.setattr(sc, "execute_remote", execute_remote)
    with pytest.raises(SystemExit):
        sc.execute_plans([("econ-catalog", rows, files)])
    assert "Re-run the same command" in capsys.readouterr().err


def test_main_sends_through_execute_plans():
    src = open(sc.__file__, encoding="utf-8").read()
    body = src[src.index("def main("):]
    assert "execute_plans(plans)" in body and "execute_remote(files" not in body


def test_execute_remote_passes_tries_through(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(d1_remote, "execute_file", lambda db, path, **kw: seen.append(kw["tries"]) or "ok")
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    js = tmp_path / "wrangler.js"
    js.write_text("")
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", str(js))
    sync_state_d1.execute_remote([str(tmp_path / "a.sql")], tries=1)
    sync_state_d1.execute_remote([str(tmp_path / "a.sql")])
    assert seen == [1, 4]


# ---- the finished noaa shard migration: never retried, never resumed after a failure ----------------------
@pytest.fixture
def noaa(tmp_path, monkeypatch):
    sys.path.insert(0, os.path.join(os.path.dirname(sc.__file__), "..", "tools"))
    import migrate_noaa_shard as m
    out = tmp_path / "shard_sql"
    out.mkdir()
    for i in range(3):
        (out / f"part_{i:03d}.sql").write_text("SELECT 1;\n")
    monkeypatch.setattr(m, "OUT_DIR", str(out))
    monkeypatch.setattr(m, "DONE_LIST", str(out / "_pushed.txt"))
    monkeypatch.setattr(m, "FAILED_MARK", str(out / "_failed.txt"))
    monkeypatch.setattr(m, "_shard_fts_count", lambda: 0)
    return m


def test_the_noaa_push_sends_each_file_once_and_marks_a_failure(noaa, monkeypatch):
    calls = []

    def execute_remote(files, *a, **kw):
        calls.append((os.path.basename(files[0]), kw.get("tries")))
        if files[0].endswith("part_001.sql"):
            raise SystemExit("FATAL: wrangler failed")
    monkeypatch.setattr(noaa.st, "execute_remote", execute_remote)
    with pytest.raises(SystemExit):
        noaa.push()
    assert calls == [("part_000.sql", 1), ("part_001.sql", 1)]
    assert open(noaa.FAILED_MARK, encoding="utf-8").read().strip() == "part_001.sql"
    with pytest.raises(SystemExit, match="--force-wipe"):
        noaa.push()                                            # no resume after a failure
    assert len(calls) == 2, "the refused resume sent nothing"
    monkeypatch.setattr(noaa.st, "execute_remote", lambda files, *a, **kw: calls.append(
        (os.path.basename(files[0]), kw.get("tries"))))
    noaa.push(force_wipe=True)
    assert calls[2:] == [("part_000.sql", 1), ("part_001.sql", 1), ("part_002.sql", 1)], "from the top"
    assert not os.path.exists(noaa.FAILED_MARK)
