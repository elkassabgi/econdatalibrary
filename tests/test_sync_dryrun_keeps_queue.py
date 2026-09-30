"""`--dry-run` never empties the pending queue (R1304, 2026-09-30).

main() cleared the pending file in its "nothing to send" branch BEFORE it reached the dry-run return, so a dry
run over a queue whose rows were all unchanged since the last sync emptied the production file (54,619 lines,
no copy). These tests drive the REAL main() against a scratch catalogue and a manifest that already records every
row, which is exactly that branch. The control proves the same fixture DOES clear the queue on a real run, so the
dry-run assertion cannot pass by never reaching the branch.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_catalog_d1 as cat  # noqa: E402
from core.catalog_sync_manifest import Manifest  # noqa: E402

QUEUE = "src:a\nsrc:b\nsrc:a\n"


@pytest.fixture
def unchanged_queue(tmp_path, monkeypatch):
    db = str(tmp_path / "catalog.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?,?)",
                    [("src:a", "src", "t", "2026-01-01"), ("src:b", "src", "t", "2026-01-01")])
    con.commit()
    manifest_path = str(tmp_path / "sent.db")
    # every row recorded as already sent with its current content: the diff skips them all
    m = Manifest(manifest_path)
    m.seed_from_catalog(con)
    m.close()
    con.close()
    pending = tmp_path / "pending_catalog_sync.txt"
    pending.write_text(QUEUE, encoding="utf-8")
    monkeypatch.setattr(cat, "CATALOG_DB", db)
    monkeypatch.setattr(cat, "PENDING", str(pending))
    monkeypatch.setattr(cat, "_manifest_path", lambda root: manifest_path)
    monkeypatch.setattr(cat, "_gated_ids", lambda: set())

    def no_remote(*a, **k):
        raise AssertionError("nothing may be executed remotely")
    monkeypatch.setattr(cat, "execute_remote", no_remote)
    return pending


def test_dry_run_leaves_the_pending_queue_intact(unchanged_queue, capsys):
    cat.main(["--dry-run"])
    out = capsys.readouterr().out
    assert "nothing to send" in out, "the fixture must reach the all-unchanged branch"
    assert unchanged_queue.read_text(encoding="utf-8") == QUEUE
    assert "(dry-run) would clear" in out


def test_dry_run_leaves_an_ids_file_intact(unchanged_queue, tmp_path, capsys):
    ids = tmp_path / "ids.txt"
    ids.write_text("src:a\n", encoding="utf-8")
    cat.main(["--ids-file", str(ids), "--dry-run"])
    assert "nothing to send" in capsys.readouterr().out
    assert ids.read_text(encoding="utf-8") == "src:a\n"


def test_control_a_real_run_does_clear_it(unchanged_queue, capsys):
    """Without --dry-run the same fixture clears the queue - the branch under test is really reached."""
    cat.main([])
    assert "cleared" in capsys.readouterr().out
    assert unchanged_queue.read_text(encoding="utf-8") == ""
