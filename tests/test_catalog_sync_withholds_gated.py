"""The catalogue sync never publishes a gated source's rows (2026-09-17).

core/sync_catalog_d1.py reads the catalogue (in CI, the R2 coherence copy) and upserts series, series_fts,
source, license and source_counts into D1. It had no gate consult, so unfreezing it would re-publish rows a
reviewed delete had removed. These tests drive the REAL main() in --dry-run against a scratch catalogue, with
the emitter replaced by a recorder so nothing is written and the rows that WOULD be sent can be inspected.
Each can fail: a non-gated control must still be sent, and a named gated source must be refused.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_catalog_d1 as cat  # noqa: E402

GATED, KEPT = "srcgated", "srckept"


@pytest.fixture
def scratch(tmp_path, monkeypatch):
    db = str(tmp_path / "catalog.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?,?)",
                    [(f"{GATED}:a", GATED, "t", "2026-01-01"), (f"{KEPT}:a", KEPT, "t", "2026-01-01")])
    con.commit()
    con.close()
    ids = tmp_path / "ids.txt"
    ids.write_text(f"{GATED}:a\n{KEPT}:a\n", encoding="utf-8")
    monkeypatch.setattr(cat, "CATALOG_DB", db)
    monkeypatch.setattr(cat, "_manifest_path", lambda root: str(tmp_path / "sent.db"))
    monkeypatch.setattr(cat, "_gated_ids", lambda: {GATED})
    sent = []

    def record(cols, grp, out_dir, conn=None, **kw):
        sent.extend(grp)
        return []
    monkeypatch.setattr(cat, "emit_sql", record)
    monkeypatch.setattr(cat, "verify_replay", lambda *a, **k: None)

    def no_remote(*a, **k):
        raise AssertionError("dry run must not execute")
    monkeypatch.setattr(cat, "execute_remote", no_remote)
    return str(ids), sent


def test_gated_rows_are_withheld_from_the_queue(scratch, capsys):
    ids, sent = scratch
    cat.main(["--ids-file", ids, "--dry-run", "--no-diff"])
    got = {r["source_id"] for r in sent}
    assert GATED not in got
    assert KEPT in got, "positive control: the non-gated row must still be sent"
    assert "withheld 1 row(s) of gated sources" in capsys.readouterr().out


def test_without_a_gate_both_are_sent(scratch, monkeypatch):
    """Negative control on the fixture: the gated row IS queued and IS sent when nothing is gated."""
    ids, sent = scratch
    monkeypatch.setattr(cat, "_gated_ids", lambda: set())
    cat.main(["--ids-file", ids, "--dry-run", "--no-diff"])
    assert {r["source_id"] for r in sent} == {GATED, KEPT}


def test_a_gated_source_argument_is_refused(scratch):
    with pytest.raises(SystemExit, match="gated"):
        cat.main(["--source", GATED.upper(), "--dry-run"])


def test_a_gated_refresh_counts_argument_is_refused(scratch):
    with pytest.raises(SystemExit, match="gated"):
        cat.main(["--refresh-counts", f"{KEPT},{GATED}", "--dry-run"])


def test_it_runs_as_a_script_the_way_ci_calls_it(tmp_path):
    """updater-daily runs `python core/sync_catalog_d1.py`; the gate reader must import there."""
    import subprocess
    p = subprocess.run([sys.executable, os.path.join("core", "sync_catalog_d1.py"), "--help"],
                       cwd=ROOT, capture_output=True, text=True, env=dict(os.environ, PYTHONPATH=""), timeout=120)
    assert p.returncode == 0, p.stderr[-600:]
