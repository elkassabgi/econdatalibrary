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


# R1305: the queue was not the only thing a dry run wrote. `--seed-manifest --dry-run` seeded the manifest (a changed,
# queued row then read as already sent), and every dry run opened the manifest read-write (WAL switch, DDL, a new
# file when absent). The sweep below snapshots the WHOLE state directory - catalogue, manifest, its -wal/-shm, the
# queue, an ids file - and requires it byte-identical after every dry-run flag combination, with a row that
# genuinely changed since the last sync so the send path is reached too.

def _snapshot(d):
    return {os.path.relpath(os.path.join(r, f), d): open(os.path.join(r, f), "rb").read()
            for r, _, fs in os.walk(d) for f in fs}


@pytest.fixture
def changed_row(unchanged_queue, tmp_path, monkeypatch):
    con = sqlite3.connect(str(tmp_path / "catalog.db"))
    con.execute("UPDATE series SET title='changed' WHERE series_id='src:b'")
    con.commit()
    con.close()
    (tmp_path / "ids.txt").write_text("src:a\nsrc:b\n", encoding="utf-8")
    sent = []

    def record(cols, grp, out_dir, conn=None, **kw):
        sent.extend(grp)
        return []
    monkeypatch.setattr(cat, "emit_sql", record)
    monkeypatch.setattr(cat, "verify_replay", lambda *a, **k: None)
    return sent


DRY_RUNS = [
    [], ["--no-diff"], ["--keep-pending"], ["--source", "src"], ["--source", "src", "--no-diff"],
    ["--ids-file", "IDS"], ["--seed-manifest"], ["--refresh-counts", "src"],
    ["--seed-manifest", "--refresh-counts", "src"], ["--seed-manifest", "--source", "src"],
]


@pytest.mark.parametrize("extra", DRY_RUNS, ids=lambda e: " ".join(e) or "plain")
def test_every_dry_run_leaves_the_state_dir_byte_identical(changed_row, tmp_path, extra):
    args = ["--dry-run"] + [str(tmp_path / "ids.txt") if x == "IDS" else x for x in extra]
    before = _snapshot(tmp_path)
    cat.main(args)
    assert _snapshot(tmp_path) == before, args


def test_seed_under_dry_run_does_not_hide_a_changed_row(changed_row, capsys):
    """The damage R1305 found: after a seeding dry run, the changed row must still be reported as to send."""
    cat.main(["--seed-manifest", "--dry-run"])
    capsys.readouterr()
    cat.main(["--dry-run"])
    assert "1 to send" in capsys.readouterr().out
    assert [r["series_id"] for r in changed_row] == ["src:b"]


def test_dry_run_creates_no_manifest_when_none_exists(changed_row, tmp_path, monkeypatch):
    absent = str(tmp_path / "nested" / "absent.db")
    monkeypatch.setattr(cat, "_manifest_path", lambda root: absent)
    before = _snapshot(tmp_path)
    cat.main(["--dry-run"])
    assert _snapshot(tmp_path) == before
    assert not os.path.exists(os.path.dirname(absent))


def _changed_b(tmp_path):
    con = sqlite3.connect(str(tmp_path / "catalog.db"))
    cur = con.execute("SELECT * FROM series WHERE series_id='src:b'")
    cols = [d[0] for d in cur.description]
    row = dict(zip(cols, cur.fetchone()))
    con.close()
    return cols, row


def test_dry_run_refuses_a_manifest_a_live_writer_holds(changed_row, tmp_path):
    """A writer holds the manifest open with a row only in its -wal. An unlocked copy of db + -wal is not a snapshot
    (R1306 measured torn and silently wrong reads), so the dry run refuses - and still leaves every file identical,
    the -wal and -shm included, and nothing in the system temp dir."""
    import tempfile
    cols, row = _changed_b(tmp_path)
    writer = Manifest(str(tmp_path / "sent.db"))
    writer.db.execute("PRAGMA wal_autocheckpoint = 0")
    writer.record(cols, [row])
    try:
        assert os.path.exists(str(tmp_path / "sent.db-wal")), "fixture must leave the row in the -wal"
        before, tmp_before = _snapshot(tmp_path), set(os.listdir(tempfile.gettempdir()))
        with pytest.raises(SystemExit, match="being written|writing it"):
            cat.main(["--dry-run"])
        assert _snapshot(tmp_path) == before
        assert not {d for d in set(os.listdir(tempfile.gettempdir())) - tmp_before if "manifest" in d}
    finally:
        writer.close()


def test_a_write_during_the_dry_run_read_withdraws_its_numbers(changed_row, tmp_path, monkeypatch):
    """The immutable read holds no lock. A sync that records while the dry run reads must make it refuse, not report
    numbers from a state that no longer exists."""
    cols, row = _changed_b(tmp_path)
    real_split = Manifest.split

    def split_then_a_sync_records(self, c, rows):
        out = real_split(self, c, rows)
        w = Manifest(str(tmp_path / "sent.db"))
        w.record(cols, [row])
        w.close()
        return out
    monkeypatch.setattr(Manifest, "split", split_then_a_sync_records)
    with pytest.raises(SystemExit, match="changed while the dry run read it"):
        cat.main(["--dry-run"])


def test_control_an_untouched_manifest_is_stable(changed_row, capsys):
    cat.main(["--dry-run"])
    assert "1 to send" in capsys.readouterr().out


def test_control_a_real_seed_does_write_the_manifest(changed_row, tmp_path):
    """The sweep's negative control: without --dry-run the seed changes the state dir."""
    before = _snapshot(tmp_path)
    cat.main(["--seed-manifest"])
    assert _snapshot(tmp_path) != before
