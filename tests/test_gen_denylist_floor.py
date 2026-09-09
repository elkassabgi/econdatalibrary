"""A regeneration of denylist.ts can never gate LESS than the committed file — the floor.

WHY THIS EXISTS. The floor used to be a hand-typed list of ids inside core/gen_denylist.py.
On 2026-09-08 the ids were removed from the repository, and the floor became a property
instead of a list: the generator reads the COMMITTED denylist.ts and refuses to write a set
that drops anything it gated, unless a human put the id in RELEASED on purpose. A floor that
is a list can be edited by accident; a floor that is a property fails loudly.

These tests are two-sided (R64): the first version of a guard like this could pass because
the parser returned an empty set and "nothing dropped" was vacuously true. So the fixture
plants ids in a committed file and in an EMPTY reservable=0 scan and checks they survive;
then checks that a released id really leaves, that a newly restricted id really arrives, that
a granted id is never gated, and that a template which lost a carve-out refuses to write at
all — with the previous file left byte-identical.

No real catalog.db, no real denylist.ts: everything runs on temp files, so it runs in CI.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

COMMITTED = """// header of the committed file
export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([
  "aaa_gated",
  // legacy/phantom ids (not currently in the catalog; kept as a safety floor):
  "zzz_phantom",
]);

/** helpers carried verbatim */
export const SERIES_CARVEOUTS: Readonly<Record<string, readonly string[]>> = {
  worldbank: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  worldbank_wdi: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  // a hand-added written refusal
  some_src: ["gold", "copper"],
};
export const SERIES_CARVEOUT_EXACT: readonly string[] = [];
"""


def _mk_db(path, reservable0, all_ids):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, reservable INTEGER)")
    con.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT)")
    con.execute("INSERT INTO license VALUES ('open', 1), ('closed', 0)")
    for s in all_ids:
        con.execute("INSERT INTO source VALUES (?, ?)", (s, "closed" if s in reservable0 else "open"))
    con.commit()
    con.close()


@pytest.fixture
def gen(tmp_path, monkeypatch):
    from core import gen_denylist as G
    out = tmp_path / "denylist.ts"
    out.write_text(COMMITTED, encoding="utf-8")
    monkeypatch.setattr(G, "OUT", str(out))
    monkeypatch.setattr(G, "DB", str(tmp_path / "catalog.db"))
    monkeypatch.setattr(G, "RELEASED", set())
    return G, out, str(tmp_path / "catalog.db")


def _gated(G, out):
    return G.committed_gate(out.read_text(encoding="utf-8"))


def test_the_parser_sees_the_planted_ids(gen):
    """The control: a parser that returns nothing would make every test below vacuous."""
    G, out, _ = gen
    assert _gated(G, out) == {"aaa_gated", "zzz_phantom"}
    assert G.committed_carveouts(COMMITTED)["some_src"] == ["gold", "copper"]


def test_floor_survives_an_empty_reservable0_scan(gen):
    G, out, db = gen
    _mk_db(db, set(), {"aaa_gated", "bbb_open"})
    G.main()
    got = _gated(G, out)
    assert {"aaa_gated", "zzz_phantom"} <= got, got
    assert "bbb_open" not in got


def test_a_newly_restricted_source_is_added(gen):
    G, out, db = gen
    _mk_db(db, {"ccc_new"}, {"aaa_gated", "ccc_new"})
    G.main()
    got = _gated(G, out)
    assert "ccc_new" in got and "aaa_gated" in got and "zzz_phantom" in got


def test_release_is_the_only_way_out(gen, monkeypatch):
    G, out, db = gen
    _mk_db(db, set(), {"aaa_gated"})
    monkeypatch.setattr(G, "RELEASED", {"zzz_phantom"})
    G.main()
    got = _gated(G, out)
    assert "zzz_phantom" not in got and "aaa_gated" in got


def test_a_release_that_does_not_take_effect_is_refused(gen, monkeypatch):
    """Releasing an id the scan still restricts would leave a dead entry that lies."""
    G, out, db = gen
    _mk_db(db, {"aaa_gated"}, {"aaa_gated"})
    monkeypatch.setattr(G, "RELEASED", {"aaa_gated"})
    before = out.read_text(encoding="utf-8")
    with pytest.raises(AssertionError):
        G.main()
    assert out.read_text(encoding="utf-8") == before


def test_a_granted_source_is_never_gated(gen):
    G, out, db = gen
    granted = sorted(G.GRANTED_EXCEPTIONS)[0]
    _mk_db(db, {granted}, {granted, "aaa_gated"})
    G.main()
    assert granted not in _gated(G, out)


def test_carveouts_and_helpers_are_carried_verbatim(gen):
    G, out, db = gen
    _mk_db(db, set(), {"aaa_gated"})
    G.main()
    src = out.read_text(encoding="utf-8")
    assert 'some_src: ["gold", "copper"]' in src
    assert "// a hand-added written refusal" in src
    assert "SERIES_CARVEOUT_EXACT" in src
    assert src.count("export const NON_REDISTRIBUTABLE") == 1


def test_a_template_that_lost_a_carveout_refuses_to_write(gen, monkeypatch):
    """The 5fc56cea1/be939627f regression: a template that no longer carries a hand-added
    refusal must not be allowed to overwrite the file that has it."""
    G, out, db = gen
    _mk_db(db, set(), {"aaa_gated"})
    monkeypatch.setattr(G, "committed_tail", lambda src=None: G.DEFAULT_TAIL)
    before = out.read_text(encoding="utf-8")
    with pytest.raises(AssertionError):
        G.main()
    assert out.read_text(encoding="utf-8") == before


def test_default_tail_is_a_usable_template(gen, monkeypatch):
    """With no committed file at all, the generator still emits the worker's helper exports."""
    G, out, db = gen
    out.unlink()
    _mk_db(db, {"ccc_new"}, {"ccc_new"})
    G.main()
    src = out.read_text(encoding="utf-8")
    assert _gated(G, out) == {"ccc_new"}
    for name in ("seriesSource", "isGated", "SERIES_CARVEOUT_LIKE", "SERIES_CARVEOUT_EXACT", "likeEscape"):
        assert name in src, name
