"""tools/sync_source_rows_d1_to_local.py (t0_ready legacy-catalogue; sec_edgar repair review C2), hermetic: a fake
D1 answer (core.d1_remote.rows) and a temporary catalogue. Pins:
  * the catalogue opens through core.catalog_path: reads write=False, the apply write=True, timeout 120 (each
    followed by PRAGMA busy_timeout=120000, as before);
  * --update-differing detects a difference in ANY column both sides carry and copies EVERY such column from D1;
    a negative control where nothing differs writes nothing;
  * the receipt goes to --receipt-dir (default: the checkout's logs/), never D:, and keeps its old keys;
  * a missing catalogue is exit 2 before D1, and nothing is created; after T0 the tool refuses first;
  * review R1919: a D1 NULL never overwrites a local value unless --allow-null-overwrite (reported either way); the
    receipt's before_after restores every updated cell; a title/geography change replaces the FTS row by rowid
    (the new title MATCHes, the old does not) and is refused before any write without exactly one index row;
    --apply refuses on logs/local_heavy.lock; a row changed after the read is refused in the transaction; the
    post-write check (same D1 rows) fails when a write does not hold; a retried COMMIT counts each row once."""
import hashlib
import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))
from core import catalog_path, cutover, d1_remote  # noqa: E402

import sync_source_rows_d1_to_local as S  # noqa: E402

COLS = ["series_id", "source_id", "title", "frequency", "unit", "geography", "category", "license_id",
        "start_date", "end_date", "last_updated", "metadata"]


def _row(sid, **over):
    r = {"series_id": sid, "source_id": "src", "title": "T " + sid, "frequency": "A", "unit": "USD",
         "geography": "US", "category": "c", "license_id": "pd", "start_date": "2000-01-01",
         "end_date": "2020-01-01", "last_updated": "2026-01-01", "metadata": '{"k": 1}'}
    r.update(over)
    return r


@pytest.fixture
def cat(tmp_path, monkeypatch):
    p = tmp_path / "data" / "catalog.db"
    p.parent.mkdir()
    c = sqlite3.connect(p)
    c.execute(f"CREATE TABLE series ({', '.join(c_ + ' TEXT' for c_ in COLS)}, PRIMARY KEY(series_id))")
    c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    for r in (_row("src:a"), _row("src:b")):
        c.execute(f"INSERT INTO series VALUES ({','.join('?' * len(COLS))})", [r[k] for k in COLS])
        c.execute("INSERT INTO series_fts VALUES (?,?,?)", (r["series_id"], r["title"], r["geography"]))
    c.commit()
    c.close()
    monkeypatch.setattr(S, "DB", str(p))
    monkeypatch.setattr(S, "RECEIPT_DIR", str(tmp_path / "receipts"))
    return p


@pytest.fixture
def opens(monkeypatch):
    seen = []
    real = catalog_path.connect_path

    def spy(path, *, write, **kw):
        seen.append((os.path.normcase(os.path.realpath(os.fspath(path))), write, kw.get("timeout")))
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", spy)
    return seen


def _d1(monkeypatch, rows):
    calls = []

    def fake(db, sql, **kw):
        calls.append((db, sql))
        return [dict(r) for r in rows], 7
    monkeypatch.setattr(d1_remote, "rows", fake)
    return calls


def _run(monkeypatch, *argv):
    monkeypatch.setattr(sys, "argv", ["sync_source_rows_d1_to_local.py", "--source", "src", *argv])
    return S.main()


def _local(p):
    c = sqlite3.connect(p)
    try:
        return {r[0]: dict(zip(COLS, r)) for r in c.execute(f"SELECT {', '.join(COLS)} FROM series")}
    finally:
        c.close()


def _receipt(tmp_path):
    files = os.listdir(tmp_path / "receipts")
    assert len(files) == 1, files
    return json.loads((tmp_path / "receipts" / files[0]).read_text(encoding="utf-8"))


def _sha(p):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()


OLD_KEYS = {"utc", "source", "d1_rows", "d1_rows_read", "local_rows_before", "missing", "differing", "apply",
            "update_differing"}
NEW_KEYS = {"differing_columns", "allow_null_overwrite", "null_over_local", "before_after", "fts_to_replace",
            "fts_unfound"}


def test_a_dry_run_reports_and_writes_nothing(cat, tmp_path, monkeypatch, opens):
    d1 = [_row("src:a"), _row("src:b", metadata='{"k": 2}'), _row("src:c")]
    calls = _d1(monkeypatch, d1)
    before = _sha(cat)
    assert _run(monkeypatch) == 0
    assert _sha(cat) == before
    rc = _receipt(tmp_path)
    assert set(rc) == OLD_KEYS | NEW_KEYS
    assert rc["missing"] == ["src:c"] and rc["differing"] == ["src:b"]
    assert rc["differing_columns"] == {"src:b": ["metadata"]}
    assert rc["d1_rows"] == 3 and rc["d1_rows_read"] == 7 and rc["local_rows_before"] == 2 and rc["apply"] is False
    assert calls == [("econ-catalog", "SELECT * FROM series WHERE series_id >= 'src:' AND series_id < 'src;'")]
    assert {(w, t) for _p, w, t in opens} == {(False, 120)}


def test_apply_inserts_the_missing_rows_and_their_fts_rows_but_updates_nothing(cat, tmp_path, monkeypatch, opens):
    _d1(monkeypatch, [_row("src:a"), _row("src:b", unit="EUR"), _row("src:c")])
    assert _run(monkeypatch, "--apply") == 0
    loc = _local(cat)
    assert loc["src:c"] == _row("src:c")
    assert loc["src:b"]["unit"] == "USD", "no --update-differing: the differing row is reported, not written"
    c = sqlite3.connect(cat)
    assert c.execute("SELECT COUNT(*) FROM series_fts WHERE series_id='src:c'").fetchone()[0] == 1
    c.close()
    rc = _receipt(tmp_path)
    assert rc["inserted"] == 1 and rc["fts_inserted"] == 1 and rc["local_rows_after"] == 3
    assert [(w, t) for _p, w, t in opens].count((True, 120)) == 1
    assert all(t == 120 for _p, _w, t in opens)


@pytest.mark.parametrize("col,val", [("metadata", '{"k": 9}'), ("last_updated", "2026-11-01"),
                                     ("frequency", "M"), ("unit", "EUR"), ("geography", "CA"),
                                     ("category", "z"), ("license_id", "cc-by"), ("source_id", "src2")])
def test_update_differing_catches_and_copies_a_non_span_column(cat, tmp_path, monkeypatch, col, val):
    """Review C2: a planted mismatch in ONE non-span column is detected and copied, and the receipt names it."""
    d1b = _row("src:b", **{col: val})
    _d1(monkeypatch, [_row("src:a"), d1b])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    loc = _local(cat)
    assert loc["src:b"] == d1b and loc["src:a"] == _row("src:a")
    rc = _receipt(tmp_path)
    assert rc["differing"] == ["src:b"] and rc["differing_columns"] == {"src:b": [col]} and rc["updated"] == 1


def test_update_differing_copies_every_column_not_only_the_one_that_differs(cat, tmp_path, monkeypatch):
    d1b = _row("src:b", title="New", start_date="1990-01-01", metadata='{"k": 3}', last_updated="2026-11-17")
    _d1(monkeypatch, [_row("src:a"), d1b])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _local(cat)["src:b"] == d1b
    assert _receipt(tmp_path)["differing_columns"] == {"src:b": ["title", "start_date", "last_updated", "metadata"]}


def test_negative_control_nothing_differs_nothing_is_written(cat, tmp_path, monkeypatch, opens):
    _d1(monkeypatch, [_row("src:a"), _row("src:b")])
    before = _sha(cat)
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _sha(cat) == before
    rc = _receipt(tmp_path)
    assert rc["missing"] == [] and rc["differing"] == [] and rc["differing_columns"] == {} and "updated" not in rc
    assert all(w is False for _p, w, _t in opens), "nothing to write: no read-write open"


def test_text_comparison_of_values():
    assert S._same(None, None) and S._same(1, "1") and S._same("a", "a")
    assert not S._same(None, "") and not S._same("", None) and not S._same("a", "b")


def test_the_receipt_default_is_the_checkout_s_logs_never_d(monkeypatch, tmp_path):
    assert S.RECEIPT_DIR == os.path.join(S.ROOT, "logs")
    src = open(S.__file__, encoding="utf-8").read()
    assert "D:/" not in src and "D:\\\\" not in src
    assert os.path.join("data", "_aqueduct") not in os.path.relpath(S.RECEIPT_DIR, S.ROOT)


def test_the_receipt_dir_argument_is_used(cat, tmp_path, monkeypatch):
    _d1(monkeypatch, [_row("src:a"), _row("src:b")])
    out = tmp_path / "elsewhere"
    assert _run(monkeypatch, "--receipt-dir", str(out)) == 0
    assert len(os.listdir(out)) == 1 and not (tmp_path / "receipts").exists()


def test_a_missing_catalogue_is_exit_2_before_d1_and_nothing_is_created(tmp_path, monkeypatch):
    monkeypatch.setattr(S, "DB", str(tmp_path / "nowhere" / "catalog.db"))
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 reached"))
    assert _run(monkeypatch) == 2
    assert not (tmp_path / "nowhere").exists()


def test_after_t0_it_refuses_before_d1(cat, tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 reached"))
    with pytest.raises(cutover.CutoverRefused):
        _run(monkeypatch, "--apply")


# ---- review of PR #274 (R1919): D1 NULL, before values, FTS refresh, heavy lock, re-checks -----------------------
def _set_local(p, sid, **cols):
    """Change a local row AND its FTS row (by rowid, as the tool does) - a planted local state."""
    c = sqlite3.connect(p)
    old = c.execute("SELECT title FROM series WHERE series_id=?", (sid,)).fetchone()[0]
    c.execute(f"UPDATE series SET {', '.join(k + '=?' for k in cols)} WHERE series_id=?", [*cols.values(), sid])
    t, g = c.execute("SELECT title, geography FROM series WHERE series_id=?", (sid,)).fetchone()
    rid = [r[0] for r in c.execute("SELECT rowid, series_id, title FROM series_fts") if r[1] == sid and r[2] == old]
    assert len(rid) == 1
    c.execute("DELETE FROM series_fts WHERE rowid=?", (rid[0],))
    c.execute("INSERT INTO series_fts VALUES (?,?,?)", (sid, t, g))
    c.commit()
    c.close()


def _match(p, word):
    c = sqlite3.connect(p)
    try:
        return sorted(r[0] for r in c.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH ?",
                                              (f'title:"{word}"',)))
    finally:
        c.close()


def _fts(p, sid):
    c = sqlite3.connect(p)
    try:
        return [r[1:] for r in c.execute("SELECT series_id, title, geography FROM series_fts") if r[0] == sid]
    finally:
        c.close()


def test_a_d1_null_over_a_local_value_is_kept_and_reported(cat, tmp_path, monkeypatch):
    _set_local(cat, "src:b", metadata='{"cik": 2110119}')
    d1b = _row("src:b", metadata=None, unit="EUR")
    _d1(monkeypatch, [_row("src:a"), d1b])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    loc = _local(cat)["src:b"]
    assert loc["metadata"] == '{"cik": 2110119}', "a D1 NULL never overwrites a local value by default"
    assert loc["unit"] == "EUR"
    rc = _receipt(tmp_path)
    assert rc["null_over_local"] == {"src:b": {"metadata": ['{"cik": 2110119}', None]}}
    assert rc["before_after"] == {"src:b": {"unit": ["USD", "EUR"]}}
    assert rc["verify"]["ok"] is True and rc["updated"] == 1 and rc["allow_null_overwrite"] is False


def test_a_row_that_differs_only_by_a_d1_null_is_not_written(cat, tmp_path, monkeypatch, opens):
    _d1(monkeypatch, [_row("src:a"), _row("src:b", metadata=None)])
    before = _sha(cat)
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _sha(cat) == before
    rc = _receipt(tmp_path)
    assert rc["differing"] == ["src:b"] and rc["null_over_local"] == {"src:b": {"metadata": ['{"k": 1}', None]}}
    assert rc["before_after"] == {} and all(w is False for _p, w, _t in opens)


def test_allow_null_overwrite_copies_the_null_and_records_the_before_value(cat, tmp_path, monkeypatch):
    _d1(monkeypatch, [_row("src:a"), _row("src:b", metadata=None)])
    assert _run(monkeypatch, "--apply", "--update-differing", "--allow-null-overwrite") == 0
    assert _local(cat)["src:b"]["metadata"] is None
    rc = _receipt(tmp_path)
    assert rc["before_after"] == {"src:b": {"metadata": ['{"k": 1}', None]}}
    assert rc["null_over_local"] == {"src:b": {"metadata": ['{"k": 1}', None]}} and rc["verify"]["ok"] is True


def test_the_receipt_alone_restores_every_updated_cell(cat, tmp_path, monkeypatch):
    original = _local(cat)
    _d1(monkeypatch, [_row("src:a", start_date="1990-01-01", category="z"),
                      _row("src:b", title="Other name", metadata='{"k": 5}', end_date="2026-11-01")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _local(cat) != original
    ba = _receipt(tmp_path)["before_after"]
    c = sqlite3.connect(cat)
    for sid, cells in ba.items():
        c.execute(f"UPDATE series SET {', '.join(k + '=?' for k in cells)} WHERE series_id=?",
                  [v[0] for v in cells.values()] + [sid])
    c.commit()
    c.close()
    assert _local(cat) == original


def test_a_title_change_replaces_the_fts_row_new_title_matches_old_does_not(cat, tmp_path, monkeypatch):
    _set_local(cat, "src:b", title="Oldname Corp")
    assert _match(cat, "Oldname") == ["src:b"] and _match(cat, "Newname") == []
    _d1(monkeypatch, [_row("src:a"), _row("src:b", title="Newname Holdings")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _match(cat, "Newname") == ["src:b"], "the new title is searchable"
    assert _match(cat, "Oldname") == [], "the old title is gone from the index"
    assert _fts(cat, "src:b") == [("Newname Holdings", "US")] and _fts(cat, "src:a") == [("T src:a", "US")]
    rc = _receipt(tmp_path)
    assert rc["fts_replaced"] == 1 and rc["fts_to_replace"] == ["src:b"] and rc["verify"]["fts_not_found"] == []


def test_a_geography_change_replaces_the_fts_row(cat, tmp_path, monkeypatch):
    _d1(monkeypatch, [_row("src:a"), _row("src:b", geography="CA")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _fts(cat, "src:b") == [("T src:b", "CA")]
    assert _receipt(tmp_path)["fts_replaced"] == 1


def test_a_change_outside_title_and_geography_leaves_the_fts_row(cat, tmp_path, monkeypatch):
    _d1(monkeypatch, [_row("src:a"), _row("src:b", unit="EUR")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    rc = _receipt(tmp_path)
    assert rc["fts_to_replace"] == [] and rc["fts_replaced"] == 0 and _fts(cat, "src:b") == [("T src:b", "US")]


@pytest.mark.parametrize("plant", ["none", "two"])
def test_a_title_change_without_exactly_one_fts_row_is_refused_before_any_write(cat, tmp_path, monkeypatch, plant):
    c = sqlite3.connect(cat)
    if plant == "none":
        c.execute("DELETE FROM series_fts WHERE rowid IN (SELECT rowid FROM series_fts WHERE title='T src:b')")
    else:
        c.execute("INSERT INTO series_fts VALUES ('src:b', 'T src:b', 'US')")
    c.commit()
    c.close()
    _d1(monkeypatch, [_row("src:a", unit="EUR"), _row("src:b", title="Newname"), _row("src:c")])
    assert _run(monkeypatch, "--update-differing") == 0, "the dry run reports it"
    assert _receipt(tmp_path)["fts_unfound"] == ["src:b"]
    os.remove(os.path.join(tmp_path / "receipts", os.listdir(tmp_path / "receipts")[0]))
    before = _sha(cat)
    # --chunk 1: src:a's update would COMMIT on its own before src:b's chunk if only the transaction checked
    assert _run(monkeypatch, "--apply", "--update-differing", "--chunk", "1") == 1
    assert _sha(cat) == before, "refused before ANY write (src:a's unit and src:c's insert too)"
    rc = _receipt(tmp_path)
    assert rc["fts_unfound"] == ["src:b"] and "refused" in rc


def test_apply_refuses_while_the_heavy_lock_is_present(cat, tmp_path, monkeypatch):
    lock = tmp_path / "local_heavy.lock"
    lock.write_text("")
    monkeypatch.setattr(S, "HEAVY_LOCK", str(lock))
    assert S.HEAVY_LOCK and os.path.join("logs", "local_heavy.lock") in os.path.relpath(
        os.path.join(S.ROOT, "logs", "local_heavy.lock"), S.ROOT)
    monkeypatch.setattr(d1_remote, "rows", lambda *a, **k: pytest.fail("D1 reached"))
    before = _sha(cat)
    assert _run(monkeypatch, "--apply", "--update-differing") == 2
    assert _sha(cat) == before


def test_the_heavy_lock_default_is_the_checkout_s_logs():
    assert S.HEAVY_LOCK == os.path.join(S.ROOT, "logs", "local_heavy.lock")


def test_a_dry_run_is_allowed_while_the_heavy_lock_is_present(cat, tmp_path, monkeypatch):
    lock = tmp_path / "local_heavy.lock"
    lock.write_text("")
    monkeypatch.setattr(S, "HEAVY_LOCK", str(lock))
    _d1(monkeypatch, [_row("src:a"), _row("src:b")])
    assert _run(monkeypatch) == 0


def test_a_local_write_between_the_read_and_the_apply_is_not_overwritten(cat, tmp_path, monkeypatch):
    real = catalog_path.connect_path

    def racing(path, *, write, **kw):
        if write:                                  # another writer lands between the read and the apply
            c = sqlite3.connect(path)
            c.execute("UPDATE series SET unit='GBP' WHERE series_id='src:b'")
            c.commit()
            c.close()
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", racing)
    _d1(monkeypatch, [_row("src:a"), _row("src:b", unit="EUR")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 1
    assert _local(cat)["src:b"]["unit"] == "GBP"
    rc = _receipt(tmp_path)
    assert "changed after the read" in rc["refused"] and rc["updated"] == 0


def test_the_post_write_check_fails_when_an_update_does_not_hold(cat, tmp_path, monkeypatch):
    c = sqlite3.connect(cat)
    c.execute("CREATE TRIGGER revert AFTER UPDATE ON series BEGIN "
              "UPDATE series SET unit='USD' WHERE series_id=NEW.series_id; END")
    c.commit()
    c.close()
    calls = _d1(monkeypatch, [_row("src:a"), _row("src:b", unit="EUR")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 1
    rc = _receipt(tmp_path)
    assert rc["verify"] == {"ok": False, "missing": [], "differing": ["src:b"], "fts_not_found": []}
    assert len(calls) == 1, "the check reuses the same D1 rows"


def test_the_post_write_check_passes_and_reads_d1_once(cat, tmp_path, monkeypatch):
    calls = _d1(monkeypatch, [_row("src:a"), _row("src:b", unit="EUR"), _row("src:c")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    assert _receipt(tmp_path)["verify"] == {"ok": True, "missing": [], "differing": [], "fts_not_found": []}
    assert len(calls) == 1


class _FlakyCommit:
    """A connection whose every other COMMIT fails busy (the first attempt of each chunk)."""
    def __init__(self, con):
        self._c, self.commits = con, 0

    def execute(self, sql, *a):
        if sql == "COMMIT":
            self.commits += 1
            if self.commits % 2:
                raise sqlite3.OperationalError("database is locked")
        return self._c.execute(sql, *a)

    def __getattr__(self, name):
        return getattr(self._c, name)


def test_a_failed_commit_that_is_retried_counts_each_row_once(cat, tmp_path, monkeypatch):
    real = catalog_path.connect_path
    monkeypatch.setattr(catalog_path, "connect_path",
                        lambda path, *, write, **kw: _FlakyCommit(real(path, write=write, **kw)) if write
                        else real(path, write=write, **kw))
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    _d1(monkeypatch, [_row("src:a"), _row("src:b", title="Newname"), _row("src:c")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 0
    rc = _receipt(tmp_path)
    assert (rc["updated"], rc["fts_replaced"], rc["inserted"], rc["fts_inserted"]) == (1, 1, 1, 1)
    assert _fts(cat, "src:b") == [("Newname", "US")] and _fts(cat, "src:c") == [("T src:c", "US")]


def test_the_docstring_warns_about_numeric_json_values():
    assert "numeric JSON values" in S.__doc__


def _race(monkeypatch, sql):
    """Run `sql` on the catalogue between the read and the apply (when the tool opens it read-write)."""
    real = catalog_path.connect_path

    def racing(path, *, write, **kw):
        if write:
            c = sqlite3.connect(path)
            c.execute(sql)
            c.commit()
            c.close()
        return real(path, write=write, **kw)
    monkeypatch.setattr(catalog_path, "connect_path", racing)


def test_a_second_fts_row_appearing_after_the_read_is_refused_in_the_transaction(cat, tmp_path, monkeypatch):
    _race(monkeypatch, "INSERT INTO series_fts VALUES ('src:b', 'T src:b', 'US')")
    _d1(monkeypatch, [_row("src:a"), _row("src:b", title="Newname")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 1
    assert _local(cat)["src:b"]["title"] == "T src:b", "the chunk was rolled back"
    assert "FTS index row" in _receipt(tmp_path)["refused"]


def test_a_local_row_appearing_before_its_insert_is_refused(cat, tmp_path, monkeypatch):
    _race(monkeypatch, "INSERT INTO series (series_id, title) VALUES ('src:c', 'someone else')")
    _d1(monkeypatch, [_row("src:a"), _row("src:b"), _row("src:c")])
    assert _run(monkeypatch, "--apply") == 1
    assert _local(cat)["src:c"]["title"] == "someone else" and _fts(cat, "src:c") == []
    assert "appeared after the read" in _receipt(tmp_path)["refused"]


def test_the_post_write_check_fails_when_a_kept_cell_is_lost(cat, tmp_path, monkeypatch):
    c = sqlite3.connect(cat)
    c.execute("CREATE TRIGGER lose AFTER UPDATE ON series BEGIN "
              "UPDATE series SET metadata=NULL WHERE series_id=NEW.series_id; END")
    c.commit()
    c.close()
    _d1(monkeypatch, [_row("src:a"), _row("src:b", metadata=None, unit="EUR")])
    assert _run(monkeypatch, "--apply", "--update-differing") == 1
    assert _receipt(tmp_path)["verify"]["differing"] == ["src:b"]


def test_no_fts_delete_by_series_id_in_the_source():
    import re
    src = open(S.__file__, encoding="utf-8").read()
    assert not re.search(r"DELETE\s+FROM\s+series_fts\s+WHERE\s+series_id", src, re.I), "a full scan per row"
    assert "DELETE FROM series_fts WHERE rowid=?" in src
