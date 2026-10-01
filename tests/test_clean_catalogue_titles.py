"""tools/clean_catalogue_titles.py and the D1 sync's title guard (Ahmed 2026-10-01; review R1323).

Behaviour tests on a scratch catalogue: the tool cleans exactly the broken titles and their index rows - and never
another series' index row, even when two series share the word it searches by; it refuses (and changes nothing) when
an index row cannot be found; it cleans localized titles with --i18n; --check is a detector that can fail. The sync
test drives the real main(): a title with a line break reaches the emitter cleaned and is counted.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import clean_catalogue_titles as cct  # noqa: E402
from core import sync_catalog_d1 as sc  # noqa: E402

ROWS = [
    ("dst:A", "1-2.1.1 Production\nand  generation of income", "Denmark", "{}"),
    ("eia:B", "\r\nU.S. Net Imports from Curacao", None, "{}"),
    ("dst:C", "Production and generation of income, clean", "Denmark", "{}"),   # shares "generation" - must survive
    ("x:D", "plain", None, json.dumps({"titles": {"da": "Produktion\nog indkomst", "de": "ok"}})),
]


def _db(path, rows=ROWS, with_fts=True):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, "
                "metadata TEXT)")
    con.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
    for sid, t, g, md in rows:
        con.execute("INSERT INTO series VALUES (?,?,?,?,?)", (sid, sid.split(":")[0], t, g, md))
        if with_fts:
            con.execute("INSERT INTO series_fts VALUES (?,?,?)", (sid, t, g))
    con.commit()
    return con


def test_apply_cleans_the_broken_titles_and_their_index_rows_only(tmp_path):
    con = _db(str(tmp_path / "c.db"))
    cleaned, refused = cct.apply(con, i18n=False)
    assert sorted(cleaned) == ["dst:A", "eia:B"] and refused == []
    titles = dict(con.execute("SELECT series_id, title FROM series"))
    assert titles["dst:A"] == "1-2.1.1 Production and  generation of income"
    assert titles["eia:B"] == "U.S. Net Imports from Curacao"
    fts = {}
    for sid, t in con.execute("SELECT series_id, title FROM series_fts"):
        fts.setdefault(sid, []).append(t)
    assert fts == {"dst:A": [titles["dst:A"]], "eia:B": [titles["eia:B"]],
                   "dst:C": ["Production and generation of income, clean"], "x:D": ["plain"]}


def test_apply_refuses_and_changes_nothing_when_an_index_row_is_missing(tmp_path):
    con = _db(str(tmp_path / "c.db"), with_fts=False)
    before = con.execute("SELECT * FROM series ORDER BY series_id").fetchall()
    cleaned, refused = cct.apply(con, i18n=False)
    assert cleaned == [] and sorted(refused) == ["dst:A", "eia:B"]
    assert con.execute("SELECT * FROM series ORDER BY series_id").fetchall() == before


def test_i18n_titles_are_cleaned_in_the_metadata(tmp_path):
    con = _db(str(tmp_path / "c.db"))
    cleaned, _ = cct.apply(con, i18n=True)
    assert "x:D" in cleaned
    md = json.loads(con.execute("SELECT metadata FROM series WHERE series_id='x:D'").fetchone()[0])
    assert md["titles"] == {"da": "Produktion og indkomst", "de": "ok"}


def _snapshot(con):
    return (con.execute("SELECT * FROM series ORDER BY series_id").fetchall(),
            sorted(con.execute("SELECT rowid, series_id, title FROM series_fts").fetchall()))


def test_a_wrong_index_row_rolls_everything_back(tmp_path, monkeypatch):
    """Fault injection (R1324 N3): the index lookup hands back ANOTHER series' rowid. The guarded DELETE matches no
    row, the transaction rolls back, and nothing - not the other series, not the target - is changed."""
    # ONE broken title only: with two, the second one's failure would mask a missing guard on the first (R1324 N3)
    con = _db(str(tmp_path / "c.db"), rows=[ROWS[0], ROWS[2]])
    other = con.execute("SELECT rowid FROM series_fts WHERE series_id='dst:C'").fetchone()[0]
    before = _snapshot(con)
    monkeypatch.setattr(cct, "_index_row", lambda c, sid, title: [(other, sid, title)])
    with pytest.raises(RuntimeError, match="index row changed"):
        cct.apply(con, i18n=False)
    assert _snapshot(con) == before


def test_a_failed_verification_rolls_everything_back(tmp_path, monkeypatch):
    """Fault injection (R1324 N5): if the cleaning leaves a break, the check before COMMIT refuses and rolls back."""
    con = _db(str(tmp_path / "c.db"))
    before = _snapshot(con)
    monkeypatch.setattr(cct, "clean_title", lambda t: t.replace("\r", ""))       # leaves the "\n"
    with pytest.raises(RuntimeError, match="still holds a line break"):
        cct.apply(con, i18n=False)
    assert _snapshot(con) == before


def test_apply_exits_1_when_it_refuses_a_title(tmp_path, monkeypatch):
    """R1324 N6: a refused id (index row not found) makes the run fail, so it cannot be read as a clean result."""
    path = str(tmp_path / "c.db")
    _db(path, with_fts=False).close()
    monkeypatch.setattr(cct.catalog_path, "connect", lambda write=False: sqlite3.connect(path))
    assert cct.main(["--apply"]) == 1


def test_check_is_a_detector_that_can_fail(tmp_path, monkeypatch):
    path = str(tmp_path / "c.db")
    _db(path).close()
    monkeypatch.setattr(cct.catalog_path, "connect", lambda write=False: sqlite3.connect(path))
    assert cct.main(["--check"]) == 1
    con = sqlite3.connect(path)
    cct.apply(con, i18n=True)
    con.close()
    assert cct.main(["--check", "--i18n"]) == 0


@pytest.mark.parametrize("raw, want", [("  T  ", ("T", False)), ("   ", ("14100360", True)), (None, ("14100360", True)),
                                       ("A\nB", ("A B", False))])
def test_statcan_cube_title_strips_and_falls_back(raw, want):
    """R1325: the statcan re-cataloguer kept its strip - edge spaces go, a blank title falls back to the Product ID."""
    import catalog_statcan_tables as cst
    assert cst.cube_title({"title": raw} if raw is not None else {}, "14100360") == want


def test_apply_series_names_strips_and_skips_a_blank_title(tmp_path, monkeypatch):
    """R1325: an edge-space CSV title is stored stripped (and is no change against the same stripped title); a blank
    one is not a new title."""
    import apply_series_names as asn
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT, "
                "unit TEXT, metadata TEXT)")
    con.executemany("INSERT INTO series VALUES (?, 's', ?, NULL, NULL, '{}')",
                    [("s:1", "Same"), ("s:2", "Old"), ("s:3", "Keep"), ("s:4", "Was")])
    con.commit()
    csv_rows = [{"series_id": "s:1", "title": "  Same  "}, {"series_id": "s:2", "title": "  New  "},
                {"series_id": "s:3", "title": "   "}, {"series_id": "s:4", "title": "Line\nbreak"}]
    monkeypatch.setattr(asn, "stream_rows", lambda src: iter(csv_rows))
    monkeypatch.setattr(asn, "PENDING", str(tmp_path / "pending.txt"))
    out = asn.process(con, "s", apply=True)
    assert dict(con.execute("SELECT series_id, title FROM series")) == {
        "s:1": "Same", "s:2": "New", "s:3": "Keep", "s:4": "Line break"}
    assert out["title_diff"] == 2


def test_refresh_sec_edgar_writes_d1_titles_cleaned():
    """refresh_sec_edgar writes D1 directly (it bypasses the sync's guard), so its own statements must carry the
    cleaned title - for a new id (INSERT + index row) and for a changed title (UPDATE)."""
    import refresh_sec_edgar as rse
    spans = [("NEWCO", "2020-01-01", "2024-12-31", "New\r\nCo (NEW)", "1"),
             ("OLDCO", "2020-01-01", "2024-12-31", "Old\nCo (OLD)", "2")]
    stmts, _n_new, _n_title = rse.d1_catalog_statements(spans, {"sec_edgar:OLDCO": "Old Co (OLD) before"})
    sql = "\n".join(stmts)
    assert "New Co (NEW)" in sql and "Old Co (OLD)" in sql
    assert "\r" not in sql.replace("\r\n", "") and "New\r\nCo" not in sql and "Old\nCo" not in sql


def test_the_sync_diff_hashes_the_cleaned_row(tmp_path, monkeypatch, capsys):
    """R1324 finding 4: cleaning happens BEFORE the diff, so a manifest that recorded the CLEANED row skips the raw
    local row as unchanged - a raw local title cannot make it re-send forever."""
    from core.catalog_sync_manifest import Manifest
    db = str(tmp_path / "catalog.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT)")
    con.execute("INSERT INTO series VALUES ('src:A', 'src', 'Production\nand income', NULL)")
    con.commit()
    con.close()
    m = Manifest(str(tmp_path / "sent.db"))
    m.record(["series_id", "source_id", "title", "geography"],
             [{"series_id": "src:A", "source_id": "src", "title": "Production and income", "geography": None}])
    m.close()
    ids = tmp_path / "ids.txt"
    ids.write_text("src:A\n", encoding="utf-8")
    monkeypatch.setattr(sc, "CATALOG_DB", db)
    monkeypatch.setattr(sc, "_manifest_path", lambda root: str(tmp_path / "sent.db"))
    monkeypatch.setattr(sc, "_gated_ids", lambda: set())
    sc.main(["--ids-file", str(ids), "--dry-run"])
    assert "1 of 1 row(s) unchanged" in capsys.readouterr().out


def test_the_selfhost_copy_cleans_titles_and_its_check_refuses_a_break():
    """R1324 finding 2: the copy the self-hosted origin serves is cleaned before its index is rebuilt."""
    sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
    import origin_copies as oc
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT, title TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [("a", "x\ny"), ("b", "  keep  "), ("c", "\r\nz")])
    assert oc._clean_titles(con) == 2
    assert dict(con.execute("SELECT series_id, title FROM series")) == {"a": "x y", "b": "  keep  ", "c": "z"}


def test_the_sync_sends_a_broken_title_cleaned_and_counts_it(tmp_path, monkeypatch, capsys):
    db = str(tmp_path / "catalog.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, geography TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?,?)",
                    [("src:A", "src", "Production\nand income", None), ("src:B", "src", "fine", None)])
    con.commit()
    con.close()
    ids = tmp_path / "ids.txt"
    ids.write_text("src:A\nsrc:B\n", encoding="utf-8")
    monkeypatch.setattr(sc, "CATALOG_DB", db)
    monkeypatch.setattr(sc, "_manifest_path", lambda root: str(tmp_path / "sent.db"))
    monkeypatch.setattr(sc, "_gated_ids", lambda: set())
    sent = {}

    def record(cols, grp, out_dir, conn=None, **kw):
        sent.update({r["series_id"]: r["title"] for r in grp})
        return []
    monkeypatch.setattr(sc, "emit_sql", record)
    monkeypatch.setattr(sc, "verify_replay", lambda *a, **k: None)
    sc.main(["--ids-file", str(ids), "--dry-run", "--no-diff"])
    assert sent == {"src:A": "Production and income", "src:B": "fine"}
    assert "1 title(s) held a line break - sent cleaned" in capsys.readouterr().out
