"""tools/sync_source_rows_d1_to_local.py (t0_ready legacy-catalogue; sec_edgar repair review C2), hermetic: a fake
D1 answer (core.d1_remote.rows) and a temporary catalogue. Pins:
  * the catalogue opens through core.catalog_path: reads write=False, the apply write=True, timeout 120 (each
    followed by PRAGMA busy_timeout=120000, as before);
  * --update-differing detects a difference in ANY column both sides carry and copies EVERY such column from D1;
    a negative control where nothing differs writes nothing;
  * the receipt goes to --receipt-dir (default: the checkout's logs/), never D:, and keeps its old keys;
  * a missing catalogue is exit 2 before D1, and nothing is created; after T0 the tool refuses first."""
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


def test_a_dry_run_reports_and_writes_nothing(cat, tmp_path, monkeypatch, opens):
    d1 = [_row("src:a"), _row("src:b", metadata='{"k": 2}'), _row("src:c")]
    calls = _d1(monkeypatch, d1)
    before = _sha(cat)
    assert _run(monkeypatch) == 0
    assert _sha(cat) == before
    rc = _receipt(tmp_path)
    assert set(rc) == OLD_KEYS | {"differing_columns"}
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
