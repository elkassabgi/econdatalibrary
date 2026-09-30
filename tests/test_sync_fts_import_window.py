"""The catalogue sync keeps every import inside wrangler's poll window, and leaves index rows alone that D1 already
holds (R1308, 2026-09-30).

The yale_epi sync's catalog_0002.sql held 8 `DELETE FROM series_fts WHERE series_id IN (...)` statements. series_fts
is fts5(series_id UNINDEXED, ...), so each is a full scan (~16.8 s measured that night); the import was cancelled 4
times with "no poll() received in 15000ms", while a file with ONE such scan imported. Three changes, each tested:
  1. at most one FTS id-list DELETE per emitted file;
  2. a DELETE block closes at FTS_DELETE_PER_STMT ids or FTS_DELETE_MAX_BYTES of statement (2,000 ids / 84 KB
     failed with APIError 7009; 1,000 / 42 KB passed);
  3. the manifest records each row's index-row hash (series_id, title, geography) after a real send; a later sync
     leaves out the FTS delete+insert of a row whose index row is unchanged (e.g. only end_date moved). Unknown
     (NULL: pre-column entries, --seed-manifest) is treated as changed.
Every test drives the real emit_sql / Manifest / main(); each has a control that shows the check can fail.
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_catalog_d1 as sc  # noqa: E402
from core.catalog_sync_manifest import Manifest, fts_hash  # noqa: E402

COLS = ["series_id", "source_id", "title", "geography", "end_date"]
DEL = re.compile(r"DELETE FROM series_fts WHERE series_id IN \(([^)]*)\)")


def _rows(src, n, title="a title for series", end="2026-01-01", idlen=0):
    return [{"series_id": f"{src}:K{i:05d}{'x' * idlen}", "source_id": src, "title": f"{title} {i}",
             "geography": None, "end_date": end} for i in range(n)]


def _emit(tmp_path, rows, **kw):
    return sc.emit_sql(COLS, rows, str(tmp_path / "out"), None, **kw)


# ---- 1. one scan per file -------------------------------------------------------------------------------------

def test_no_emitted_file_holds_more_than_one_fts_delete(tmp_path):
    files = _emit(tmp_path, _rows("boc", 5_500))            # 6 DELETE blocks at 1,000 ids
    per_file = [open(p, encoding="utf-8").read().count("DELETE FROM series_fts") for p in files]
    assert max(per_file) == 1, per_file
    assert sum(per_file) == 6, per_file


def test_control_the_old_splitter_would_pack_several(tmp_path, monkeypatch):
    """Negative control: allowing 8 scans per file packs several DELETEs into one file (the yale failure)."""
    monkeypatch.setattr(sc, "FTS_DELETES_PER_FILE", 8)
    files = _emit(tmp_path, _rows("boc", 5_500))
    assert max(open(p, encoding="utf-8").read().count("DELETE FROM series_fts") for p in files) > 1


def test_every_file_still_reapplies_and_replays(tmp_path):
    rows = _rows("boc", 2_300)
    files = _emit(tmp_path, rows)
    assert all(sc.reapplicable(p) for p in files)
    sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows})


# ---- 2. arity by bytes ------------------------------------------------------------------------------------------

def test_a_delete_closes_at_the_id_cap(tmp_path):
    files = _emit(tmp_path, _rows("boc", 2_500))
    sizes = [len(m.group(1).split(",")) for p in files for m in DEL.finditer(open(p, encoding="utf-8").read())]
    assert sizes == [1_000, 1_000, 500], sizes


def test_a_delete_closes_at_the_byte_cap_for_long_ids(tmp_path):
    files = _emit(tmp_path, _rows("boc", 2_000, idlen=80))
    stmts = [m.group(0) for p in files for m in DEL.finditer(open(p, encoding="utf-8").read())]
    assert all(len(s) <= sc.FTS_DELETE_MAX_BYTES + 60 for s in stmts), [len(s) for s in stmts]
    assert len(stmts) > 2, "long ids must close blocks before 1,000 ids"


# ---- 3. unchanged index rows are left alone --------------------------------------------------------------------

def test_emit_leaves_out_the_index_rows_of_skipped_ids(tmp_path):
    rows = _rows("boc", 30)
    skip = {r["series_id"] for r in rows[:20]}
    files = _emit(tmp_path, rows, fts_skip=skip)
    body = "".join(open(p, encoding="utf-8").read() for p in files)
    for r in rows:
        in_fts = re.search(r"INSERT INTO series_fts[^;]*'%s'" % re.escape(r["series_id"]), body) is not None
        assert in_fts == (r["series_id"] not in skip), r["series_id"]
    assert body.count("INSERT OR REPLACE INTO series") >= 1
    sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows} - skip)


def test_the_range_form_ignores_the_skip(tmp_path):
    rows = _rows("boc", 30)
    files = _emit(tmp_path, rows, fts_range_source="boc", fts_skip={r["series_id"] for r in rows})
    sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows})


def test_replay_refuses_a_file_set_that_writes_the_wrong_index_rows(tmp_path):
    """verify_replay's new index check can fail: files that write all 30 index rows are refused when only 10
    were meant to be written."""
    rows = _rows("boc", 30)
    files = _emit(tmp_path, rows)
    with pytest.raises(SystemExit, match="index rows"):
        sc.verify_replay(COLS, rows, files, fts_ids={r["series_id"] for r in rows[:10]})


def test_manifest_fts_current_needs_a_recorded_equal_hash(tmp_path):
    m = Manifest(str(tmp_path / "sent.db"))
    rows = _rows("boc", 4)
    m.record(COLS, rows)                                      # a real send: index hashes recorded
    moved = [dict(rows[0], end_date="2026-12-31"),            # date only: index row unchanged
             dict(rows[1], title="renamed"),                  # title: index row changed
             rows[2]]
    assert m.fts_current(moved) == {rows[0]["series_id"], rows[2]["series_id"]}
    m.record(COLS, [rows[3]], fts_sent=False)                 # seeded: index hash unknown
    assert m.fts_current([rows[3]]) == set()
    m.close()


def test_a_pre_column_manifest_is_migrated_in_place_and_reads_unknown(tmp_path):
    p = str(tmp_path / "old.db")
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE sent(series_id TEXT PRIMARY KEY, row_hash TEXT NOT NULL)")
    con.execute("INSERT INTO sent VALUES('boc:K00000','h')")
    con.commit()
    con.close()
    ro = Manifest(p, read_only=True)                          # dry run: no migration, all unknown
    assert ro.fts_current(_rows("boc", 1)) == set()
    ro.close()
    assert "fts_hash" not in [r[1] for r in sqlite3.connect(p).execute("PRAGMA table_info(sent)")]
    m = Manifest(p)                                           # real run: ALTER TABLE, existing row NULL
    assert m.db.execute("SELECT fts_hash FROM sent").fetchone() == (None,)
    assert m.db.execute("SELECT row_hash FROM sent").fetchone() == ("h",)
    m.close()


def test_seed_records_row_hashes_but_leaves_index_hashes_unknown(tmp_path):
    cat = sqlite3.connect(str(tmp_path / "catalog.db"))
    cat.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, "
                "end_date TEXT)")
    cat.executemany("INSERT INTO series VALUES (?,?,?,?,?)", [tuple(r[c] for c in COLS) for r in _rows("boc", 3)])
    cat.commit()
    m = Manifest(str(tmp_path / "sent.db"))
    assert m.seed_from_catalog(cat) == 3
    assert m.db.execute("SELECT count(*) FROM sent WHERE fts_hash IS NULL").fetchone() == (3,)
    m.close()


# ---- end to end through main() ----------------------------------------------------------------------------------

@pytest.fixture
def world(tmp_path, monkeypatch):
    db = str(tmp_path / "catalog.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, "
                "end_date TEXT)")
    rows = _rows("src", 3)
    con.executemany("INSERT INTO series VALUES (?,?,?,?,?)", [tuple(r[c] for c in COLS) for r in rows])
    con.commit()
    con.close()
    manifest = str(tmp_path / "sent.db")
    m = Manifest(manifest)
    m.record(COLS, rows)                                      # all three sent before, index hashes known
    m.close()
    con = sqlite3.connect(db)                                 # then: a date move on :0, a retitle on :1
    con.execute("UPDATE series SET end_date='2026-12-31' WHERE series_id='src:K00000'")
    con.execute("UPDATE series SET title='new title' WHERE series_id='src:K00001'")
    con.commit()
    con.close()
    ids = tmp_path / "ids.txt"
    ids.write_text("src:K00000\nsrc:K00001\nsrc:K00002\n", encoding="utf-8")
    monkeypatch.setattr(sc, "CATALOG_DB", db)
    monkeypatch.setattr(sc, "_manifest_path", lambda root: manifest)
    monkeypatch.setattr(sc, "_gated_ids", lambda: set())
    sent = {}

    def record(cols, grp, out_dir, conn=None, **kw):
        sent["rows"] = [r["series_id"] for r in grp]
        sent["skip"] = set(kw.get("fts_skip") or ())
        return []
    monkeypatch.setattr(sc, "emit_sql", record)

    def replay(cols, grp, files, **kw):
        sent["fts_ids"] = kw.get("fts_ids")
    monkeypatch.setattr(sc, "verify_replay", replay)
    monkeypatch.setattr(sc, "execute_remote", lambda *a, **k: None)
    sent["db"], sent["manifest"] = db, manifest
    return str(ids), sent


def test_main_sends_both_changed_rows_but_only_the_retitled_index_row(world, capsys):
    ids, sent = world
    sc.main(["--ids-file", ids, "--dry-run"])
    assert sorted(sent["rows"]) == ["src:K00000", "src:K00001"]
    assert sent["skip"] == {"src:K00000"}
    assert "of which 1 keep their index row -> 1 index row(s) rewritten" in capsys.readouterr().out


def test_control_no_diff_rewrites_every_index_row(world):
    ids, sent = world
    sc.main(["--ids-file", ids, "--dry-run", "--no-diff"])
    assert sent["skip"] == set()


def test_main_checks_the_replay_against_the_index_rows_it_means_to_write(world):
    """R1312 mutant M2: main must hand verify_replay the ids whose index rows it rewrites."""
    ids, sent = world
    sc.main(["--ids-file", ids, "--dry-run"])
    assert sent["fts_ids"] == {"src:K00001"}


def test_the_whole_source_range_path_rewrites_every_index_row_even_when_some_are_current(world):
    """R1312 mutant M1: with --source, every row changed (skipped == 0) takes the range form, which deletes the
    source's whole index - so no row may be skipped, although two of them are index-current."""
    ids, sent = world
    con = sqlite3.connect(sent["db"])
    con.execute("UPDATE series SET end_date='2026-12-31' WHERE series_id='src:K00002'")
    con.commit()
    con.close()
    sc.main(["--source", "src", "--dry-run"])
    assert sorted(sent["rows"]) == ["src:K00000", "src:K00001", "src:K00002"]
    assert sent["skip"] == set()
    assert sent["fts_ids"] == {"src:K00000", "src:K00001", "src:K00002"}


def test_a_real_run_forgets_the_index_rows_it_rewrites_before_sending(world, monkeypatch):
    """R1312 finding 7: if the send dies partway, the rewritten rows' index hashes must read UNKNOWN (NULL), not the
    old vouched value; the index-current row keeps its hash."""
    ids, sent = world

    def die(*a, **k):
        raise SystemExit("simulated import failure")
    monkeypatch.setattr(sc, "execute_plans", die)
    with pytest.raises(SystemExit, match="simulated"):
        sc.main(["--ids-file", ids])
    got = dict(sqlite3.connect(sent["manifest"]).execute("SELECT series_id, fts_hash IS NULL FROM sent"))
    assert got == {"src:K00000": 0, "src:K00001": 1, "src:K00002": 0}


def test_a_row_hash_rewritten_by_pre_column_code_voids_the_index_hash(tmp_path):
    """R1312 finding 4: code from before the column (a rollback) rewrites row_hash only. The index hash is bound to
    the row_hash it was recorded with, so it stops vouching instead of approving an index row that code replaced."""
    m = Manifest(str(tmp_path / "sent.db"))
    rows = _rows("boc", 1)
    m.record(COLS, rows)
    assert m.fts_current(rows) == {rows[0]["series_id"]}           # control: a fresh record vouches
    m.db.execute("UPDATE sent SET row_hash='written-by-old-code'")  # what the old record() does
    m.db.commit()
    assert m.fts_current(rows) == set()
    m.close()


# ---- a complete import that wrangler reports as failed -----------------------------------------------------------

def _file_of(tmp_path, key="cat-abc-0000"):
    p = tmp_path / "f.sql"
    body = "".join(f"INSERT OR REPLACE INTO t(a) VALUES\n  ({i});\n" for i in range(7))
    p.write_text(body + (sc.receipt_sql(key) + "\n" if key else ""), encoding="utf-8")
    return str(p)


def _fake_d1(monkeypatch, stdout, receipt_rows, rc=1):
    """wrangler --file returns `stdout` with exit `rc`; the receipt read returns `receipt_rows`."""
    from core import d1_remote
    calls, reads = [], []

    def run(args, **kw):
        calls.append(args)
        return type("R", (), {"returncode": rc, "stdout": stdout, "stderr": ""})()

    def read(database, sql, timeout=900):
        reads.append(sql)
        asked = re.search(r"k = '([^']+)'", sql).group(1)
        rows = [{"k": asked}] if receipt_rows == "echo" else receipt_rows    # "echo": THIS send's receipt exists
        return [{"results": rows, "success": True, "meta": {}}]
    monkeypatch.setattr(d1_remote, "_wrangler", run)
    monkeypatch.setattr(d1_remote, "run_json", read)
    monkeypatch.setattr(d1_remote, "is_cut_over", lambda: False)
    return d1_remote, calls, reads


POLL_GONE = ('\U0001f300 Starting import...\n\U0001f300 Processed 10 queries.\n'
             '{"error": {"text": "Not currently importing anything."}}\n')


def test_a_complete_import_reported_as_failed_counts_as_done_when_its_receipt_reads_back(tmp_path, monkeypatch):
    """Measured 2026-09-30 21:02Z: 'Not currently importing anything', exit 1, and the file HAD committed. Its
    receipt row reads back, so it counts as done - with no re-application."""
    p = _file_of(tmp_path)
    d1_remote, calls, reads = _fake_d1(monkeypatch, POLL_GONE, "echo")
    d1_remote.execute_file("econ-catalog", p, tries=4, retry_timeouts=True)
    assert len(calls) == 1 and len(reads) == 1 and "cat-abc-0000.a" in reads[0]
    assert os.listdir(tmp_path) == ["f.sql"], "the per-send copy is removed"


def test_a_receipt_from_an_earlier_send_of_the_same_file_does_not_vouch(tmp_path, monkeypatch):
    """R1314: the FILE's key committed by an earlier send (a whole-source restart re-sends the DELETE file) must not
    make a LATER send that rolled back count as done. Each send carries its own nonce, so only that send's row
    counts."""
    p = _file_of(tmp_path)
    d1_remote, calls, reads = _fake_d1(monkeypatch, POLL_GONE, [{"k": "cat-abc-0000"}])   # the stale, per-file row
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", p, tries=2)
    assert len(calls) == 2
    sent_keys = {re.search(r"k = '([^']+)'", s).group(1) for s in reads}
    assert len(sent_keys) == 2 and all(k.startswith("cat-abc-0000.a") for k in sent_keys), sent_keys
    assert os.listdir(tmp_path) == ["f.sql"]


def test_without_its_receipt_row_the_same_output_is_a_failure(tmp_path, monkeypatch):
    """R1313: the same text follows a rollback. No receipt row = not committed = a failure, retried as before."""
    p = _file_of(tmp_path)
    d1_remote, calls, _ = _fake_d1(monkeypatch, POLL_GONE, [])
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", p, tries=2)
    assert len(calls) == 2


def test_a_file_without_a_receipt_cannot_be_called_done(tmp_path, monkeypatch):
    p = _file_of(tmp_path, key=None)
    d1_remote, _, reads = _fake_d1(monkeypatch, POLL_GONE, [{"k": "anything"}])
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", p, tries=1)
    assert reads == []


def test_any_other_failure_is_still_a_failure(tmp_path, monkeypatch):
    p = _file_of(tmp_path)
    d1_remote, _, reads = _fake_d1(monkeypatch, "Processed 10 queries.\nX [ERROR] Cancelled due to no poll() received "
                                                "in 15000ms.\n", "echo")
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", p, tries=1)
    assert reads == []


def _run_restart_scenario(tmp_path, monkeypatch, plan):
    """Review R1314's scenario, end to end and offline: the REAL range-form emit_sql, execute_plans and
    execute_file against an in-memory SQLite standing in for D1. `plan` maps (file index, nth send) to an outcome."""
    import json as _json
    from core import d1_remote, sync_state_d1
    rows = [{"series_id": f"src:K{i:04d}", "source_id": "src", "title": f"title {i}", "geography": None,
             "end_date": None} for i in range(120)]
    monkeypatch.setattr(sc, "MAX_FILE_BYTES", 3_000 + sc.RECEIPT_RESERVE)
    files = [os.path.abspath(p) for p in _emit(tmp_path, rows, fts_range_source="src")]
    d1 = sqlite3.connect(":memory:")
    d1.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, "
               "end_date TEXT)")
    d1.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    d1.executemany("INSERT INTO series_fts VALUES (?,?,?)", [(r["series_id"], r["title"], None) for r in rows])
    sends = {}

    def wrangler(args, timeout, retries=2):
        if "--command" in args:
            cur = d1.execute(args[args.index("--command") + 1])
            names = [c[0] for c in cur.description]
            return type("R", (), {"returncode": 0, "stderr": "", "stdout": _json.dumps(
                [{"results": [dict(zip(names, r)) for r in cur], "success": True}])})()
        path = [a for a in args if a.startswith("--file=")][0][7:]
        i = files.index(re.sub(r"\.a[0-9a-f]{12}\.sql$", "", path))
        sends[i] = sends.get(i, 0) + 1
        what = plan.get((i, sends[i]), "ok")
        if what in ("ok", "commit_other_error"):
            d1.executescript(open(path, encoding="utf-8").read())
        if what == "ok":
            return type("R", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()
        if what == "commit_other_error":
            return type("R", (), {"returncode": 1, "stdout": "",
                                  "stderr": "X [ERROR] Cancelled due to no poll() received in 15000ms."})()
        return type("R", (), {"returncode": 1, "stdout": POLL_GONE, "stderr": ""})()      # rolled back
    monkeypatch.setattr(d1_remote, "_wrangler", wrangler)
    monkeypatch.setattr(d1_remote, "is_cut_over", lambda: False)
    monkeypatch.setattr(d1_remote, "WRANGLER_JS", os.path.abspath(__file__))
    monkeypatch.setattr(sync_state_d1.shutil, "which", lambda n: "node")
    import time as _time
    monkeypatch.setattr(_time, "sleep", lambda s: None)
    try:
        sc.execute_plans([(None, rows, files)])
        outcome = "success"
    except SystemExit:
        outcome = "failed"
    n, d = d1.execute("SELECT count(*), count(DISTINCT series_id) FROM series_fts").fetchone()
    return outcome, n, d


def test_a_restart_whose_resend_rolls_back_never_reports_success_over_duplicates(tmp_path, monkeypatch):
    """R1314: file 2 (bare INSERTs) commits but reports another error -> restart at the DELETE file 1, whose re-send
    ROLLS BACK with the ambiguous text. The first send's receipt must not vouch for it."""
    outcome, n, d = _run_restart_scenario(tmp_path, monkeypatch,
                                          {(2, 1): "commit_other_error", (1, 2): "rollback_not_importing"})
    assert (n, d) == (120, 120) or outcome == "failed", (outcome, n, d)
    assert not (outcome == "success" and n != d), f"success reported over {n - d} duplicate index rows"


def test_control_the_restart_scenario_without_a_rollback_is_clean(tmp_path, monkeypatch):
    outcome, n, d = _run_restart_scenario(tmp_path, monkeypatch, {(2, 1): "commit_other_error"})
    assert (outcome, n, d) == ("success", 120, 120)


def test_the_per_send_copy_is_byte_identical_but_for_the_key(tmp_path):
    """R1315 finding 2: a CRLF file (what emit_sql writes on Windows) stays CRLF in the copy; only the key changes."""
    from core import d1_remote
    p = tmp_path / "crlf.sql"
    p.write_bytes(b"INSERT OR REPLACE INTO t(a) VALUES\r\n  ('x\ry');\r\n" + sc.receipt_sql("cat-k-0000").encode() + b"\r\n")
    send = d1_remote._attempt_copy(str(p))
    try:
        got = open(send, "rb").read()
        assert got.replace(re.search(rb"cat-k-0000\.a[0-9a-f]{12}", got).group(0), b"cat-k-0000") == p.read_bytes()
    finally:
        d1_remote._drop_copy(send, str(p))
    assert os.listdir(tmp_path) == ["crlf.sql"]


def test_a_failed_copy_write_leaves_no_partial_file_and_fails_clearly(tmp_path, monkeypatch):
    """R1315 finding 1: disk full / permission while writing the copy - nothing sent, nothing left, a RuntimeError."""
    from core import d1_remote
    p = _file_of(tmp_path)
    real_open = open

    class Full:
        def __init__(self, fh):
            self.fh = fh

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.fh.close()

        def write(self, s):
            self.fh.write(s[:10])
            raise OSError(28, "No space left on device")

    def fake_open(f, mode="r", *a, **k):
        fh = real_open(f, mode, *a, **k)
        return Full(fh) if "w" in mode and str(f).endswith(".sql") and ".a" in os.path.basename(str(f)) else fh
    monkeypatch.setattr("builtins.open", fake_open)
    d1_remote_mod, calls, _ = _fake_d1(monkeypatch, POLL_GONE, "echo")
    with pytest.raises(RuntimeError, match="per-send copy"):
        d1_remote_mod.execute_file("econ-catalog", p, tries=3)
    monkeypatch.setattr("builtins.open", real_open)
    assert calls == [] and os.listdir(tmp_path) == ["f.sql"]


def test_a_missing_file_is_passed_through_for_wrangler_to_report(tmp_path, monkeypatch):
    """R1315 blocking finding: callers (and their tests) pass paths the helper may not read; that is wrangler's
    error to report, not a FileNotFoundError from the copy step."""
    d1_remote, calls, reads = _fake_d1(monkeypatch, "X [ERROR] no such file", [], rc=1)
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", str(tmp_path / "absent.sql"), tries=1)
    assert len(calls) == 1 and reads == []


def test_without_a_per_send_copy_no_receipt_can_confirm_the_send(tmp_path, monkeypatch):
    """Round 5 residual: if the copy step is skipped (the file could not be read here) the send carries the FILE's
    key, which an earlier send may have committed - so the ambiguous exit stays a failure, with no receipt read."""
    from core import d1_remote
    p = _file_of(tmp_path)
    d1_remote_mod, calls, reads = _fake_d1(monkeypatch, POLL_GONE, [{"k": "cat-abc-0000"}])
    monkeypatch.setattr(d1_remote, "_attempt_copy", lambda path: path)
    with pytest.raises(RuntimeError):
        d1_remote_mod.execute_file("econ-catalog", p, tries=1)
    assert reads == []


def test_json_mode_never_takes_the_receipt_path(tmp_path, monkeypatch):
    """R1313 (LOW): the json road returns statement results the receipt path cannot supply - it stays a failure."""
    p = _file_of(tmp_path)
    d1_remote, _, reads = _fake_d1(monkeypatch, POLL_GONE, "echo")
    with pytest.raises(RuntimeError):
        d1_remote.execute_file("econ-catalog", p, tries=1, json_out=True)
    assert reads == []


def test_every_emitted_file_ends_with_its_own_receipt(tmp_path):
    from core import d1_remote
    files = _emit(tmp_path, _rows("boc", 2_300))
    keys = [d1_remote.receipt_key(p) for p in files]
    assert all(keys) and len(set(keys)) == len(files)
    for p in files:
        assert open(p, encoding="utf-8").read().rstrip().endswith(f"VALUES('{d1_remote.receipt_key(p)}', datetime('now'));")


def test_replay_refuses_a_file_without_its_receipt(tmp_path):
    rows = _rows("boc", 30)
    files = _emit(tmp_path, rows)
    body = open(files[0], encoding="utf-8").read()
    open(files[0], "w", encoding="utf-8").write(body[:body.index("CREATE TABLE IF NOT EXISTS sync_receipt")])
    with pytest.raises(SystemExit, match="receipt"):
        sc.verify_replay(COLS, rows, files)


def test_a_second_writer_migrating_first_is_not_an_error(tmp_path, monkeypatch):
    """R1312 finding 7: two new-code writers opening a pre-column manifest together - the loser's ALTER sees a
    duplicate column and carries on."""
    p = str(tmp_path / "old.db")
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE sent(series_id TEXT PRIMARY KEY, row_hash TEXT NOT NULL)")
    con.commit()
    con.close()
    monkeypatch.setattr(Manifest, "_fts_column", lambda self: False)   # it looked before the other writer
    Manifest(p).close()
    Manifest(p).close()                                                 # its ALTER now hits the column
