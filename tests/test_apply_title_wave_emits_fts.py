"""A title wave must emit the search-index refresh, not print a suggestion to do it.

`core/apply_title_wave.py::emit_delta` wrote `UPDATE series SET title=...` files and stopped.
`main()` ended with:

    print("DONE. Next: push dist/d1/titles/part_*.sql to D1, then rebuild D1 series_fts.")

A next step that lives in a print statement is a next step that does not happen. Measured on the
live database on 2026-08-25: `dist/d1/titles/` held 109 files and 162,769 `UPDATE` statements for
fao_fo (68,508), fao_qcl (59,077) and fao_pp (35,184), and `grep -rl series_fts dist/d1/titles`
returned nothing at all.

The consequence is invisible on the page and total on the query path, because the two read
different tables. `/v1/catalog` DISPLAYS `s.title` from `series` (sql.ts::SEARCH_FTS), while
matching runs against `series_fts.title`:

    series.title = 'Stocks, Sheep - Vanuatu'
    MATCH 'Sheep' AND series_id='fao_qcl:FAO_QCL:5111.155.976'   ->  0
    fts rows for that id                                          ->  2, both titled with the code

Earlier waves left the same wound: 1,606 bea series and 330 noaa series carry their raw code in
the index while `series` holds the published name.

Two properties are load-bearing and are pinned separately:

  * DELETE before INSERT, per INDIVIDUAL id. An FTS5 virtual table has no unique constraint, so
    a bare INSERT appends a second copy (R487, R489) and `INSERT OR IGNORE` is a no-op on it.
    A whole-source `DELETE ... LIKE 'src:%'` is also wrong here — it opens a window in which the
    entire source is unfindable, which is what AR-004 stopped for wid's 2.47M series.
  * INSERT ... SELECT, never a title literal. 34,670 bea titles and 10,570 noaa titles contain
    an apostrophe ("Prince George's, MD") and titles carry U+2014. Selecting the title from
    `series` means it is never re-quoted, and means this file cannot write a title that `series`
    does not already hold.
"""
from __future__ import annotations

import importlib.util
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOD = os.path.join(ROOT, "core", "apply_title_wave.py")


@pytest.fixture(scope="module")
def wave():
    spec = importlib.util.spec_from_file_location("apply_title_wave_undertest", MOD)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture()
def emitted(wave, tmp_path, monkeypatch):
    """Run emit_delta into a temp dir and return (part_files, fts_files) contents."""
    monkeypatch.setattr(wave, "OUT_DIR", str(tmp_path))
    landed = [
        ("fao_qcl:FAO_QCL:5111.155.976", "Stocks, Sheep - Vanuatu"),
        ("bea:A150RC:A", "Manufacturing (A)"),
        ("noaa:gsom:AM000037880:HD", "KOCHBEC - Heating Degree Days"),
        # the case that breaks naive quoting
        ("bea:APOSTROPHE:A", "Prince George's, MD — level"),
    ]
    wave.emit_delta(landed)
    parts, fts = {}, {}
    for f in sorted(os.listdir(tmp_path)):
        txt = (tmp_path / f).read_text(encoding="utf-8")
        (fts if f.startswith("fts_") else parts)[f] = txt
    return parts, fts


def test_an_fts_refresh_file_is_emitted_at_all(emitted):
    _parts, fts = emitted
    assert fts, ("emit_delta produced no fts_*.sql — the title wave updates `series` and leaves "
                 "the search index holding the old value, so the new titles match nothing")


def test_every_updated_id_gets_an_index_refresh(emitted):
    parts, fts = emitted
    updated = set()
    for txt in parts.values():
        updated |= set(re.findall(r"UPDATE series SET title=.* WHERE series_id='([^']+)';", txt))
    refreshed = set()
    for txt in fts.values():
        refreshed |= set(re.findall(r"DELETE FROM series_fts WHERE series_id='([^']+)';", txt))
    assert updated, "no UPDATE statements were emitted at all"
    assert updated == refreshed, f"ids updated but not re-indexed: {sorted(updated - refreshed)}"


def test_each_insert_is_preceded_by_a_delete_for_the_same_id(emitted):
    """A bare INSERT appends a second copy — FTS5 has no unique constraint to lean on."""
    _parts, fts = emitted
    for name, txt in fts.items():
        stmts = [x.strip() for x in txt.splitlines() if x.strip() and not x.startswith("--")]
        for i, st in enumerate(stmts):
            if not st.startswith("INSERT INTO series_fts"):
                continue
            assert i > 0 and stmts[i - 1].startswith("DELETE FROM series_fts"), (
                f"{name}: an INSERT with no DELETE before it — every run adds another copy")
            sid_del = re.search(r"series_id=('[^']+')", stmts[i - 1]).group(1)
            sid_ins = re.search(r"series_id=('[^']+')", st).group(1)
            assert sid_del == sid_ins, f"{name}: DELETE and INSERT disagree on the id"


def test_the_delete_is_never_whole_source(emitted):
    """AR-004: a whole-source delete makes the source unfindable until the insert lands."""
    _parts, fts = emitted
    for name, txt in fts.items():
        assert "LIKE" not in txt.upper(), f"{name}: a LIKE-scoped delete would clear a whole source"
        assert not re.search(r"DELETE FROM series_fts\s*;", txt), f"{name}: unscoped DELETE"


def test_the_title_is_selected_not_quoted(emitted):
    """A title literal would have to be re-escaped; SELECT means it never leaves `series`."""
    _parts, fts = emitted
    for name, txt in fts.items():
        assert "SELECT series_id,title,geography FROM series" in txt, (
            f"{name}: the refresh does not SELECT the title from `series`")
        assert "VALUES" not in txt.upper(), (
            f"{name}: a VALUES literal reintroduces quoting of titles that contain apostrophes")


def test_an_apostrophe_title_does_not_appear_in_the_index_sql(emitted):
    """The apostrophe case, explicitly: it must not be present to be mis-escaped."""
    _parts, fts = emitted
    joined = "\n".join(fts.values())
    assert "Prince George" not in joined, (
        "a title reached the index SQL as a literal; INSERT...SELECT exists so it cannot")
    assert "bea:APOSTROPHE:A" in joined, "that id should still be refreshed"


def test_negative_control_the_checks_can_see_a_bare_insert(emitted):
    """R346/R414: a guard that cannot fail is not a guard.

    Build the shipped-before shape by hand — INSERT with no DELETE — and assert the same rule
    used above rejects it. If this ever passes, the rule above has stopped testing anything.
    """
    bad = ["INSERT INTO series_fts(series_id,title,geography) "
           "SELECT series_id,title,geography FROM series WHERE series_id='x:1';"]
    offending = [i for i, st in enumerate(bad)
                 if st.startswith("INSERT INTO series_fts")
                 and (i == 0 or not bad[i - 1].startswith("DELETE FROM series_fts"))]
    assert offending == [0], "the bare-insert detector no longer detects a bare insert"
