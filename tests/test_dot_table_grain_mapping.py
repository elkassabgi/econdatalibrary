"""The UNCTAD DOT table-grain mapper derives from ONE source of truth: the resolver's set.

The resolver (clients/python/econdl/_resolve.py, `_DOT_TABLE_GRAIN`) serves id X for every store
key k with  k == X  or  k startswith X + "." .  The §5.7 mapper (`orchestrate._catalog_ids_for`)
must answer the inverse question - which catalogued ids does a changed key k make stale - for
EVERY member of that set. It used to keep its own `{source: depth}` map (`_UNCTAD_DOT_DEPTH`),
which held 9 of the resolver's 18 members; the other nine mapped 0% of their changed keys and
their served CSVs never re-derived (unctad_criticalmineralstradebypart, 2026-10-05: 8,589,597
changed keys, 0 mapped, 16,103 served CSVs left behind the store). The old test checked only
"map members are in the resolver set", the direction that cannot catch a missing member.

This file is the RATCHET: every case is parametrised from the LIVE resolver set, so a source
added there gets a mapping case automatically, and a mapper that does not cover it goes red.
"""
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from econdl import _resolve  # noqa: E402
from updater import config, orchestrate  # noqa: E402

MEMBERS = sorted(_resolve._DOT_TABLE_GRAIN)


def _catalogue(tmp_path, monkeypatch, rows):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(r, r.split(":", 1)[0]) for r in rows])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    # r2 semantics: no derive-all rescue, so "mapped" means the keys really mapped
    monkeypatch.setattr(config, "BACKEND", "r2")


def test_the_resolver_set_is_the_full_eighteen_measured_today():
    """A tripwire on the input, not the logic: if the set shrinks by accident, every source
    removed from it stops being served AND mapped, and nothing else here would notice."""
    assert len(MEMBERS) >= 18
    assert "unctad_criticalmineralstradebypart" in MEMBERS


def test_no_hand_kept_depth_map_survives():
    """The second list is gone; a reintroduced one would be the drift this file exists for."""
    assert not hasattr(orchestrate, "_UNCTAD_DOT_DEPTH")
    assert orchestrate._dot_table_members() == frozenset(_resolve._DOT_TABLE_GRAIN)


@pytest.mark.parametrize("src", MEMBERS)
def test_every_resolver_member_maps_through_the_shipped_mapper(src, tmp_path, monkeypatch):
    """THE RATCHET. Series-grain keys collapse onto their table id at depth 1, 2 and 3, an
    at-grain key maps to itself, duplicates derive once, an uncatalogued table stays unmapped."""
    _catalogue(tmp_path, monkeypatch, [f"{src}:A1", f"{src}:B2.X", f"{src}:C3.Y.Z", f"{src}:D4.Q"])
    ids, unmapped = orchestrate._catalog_ids_for(src, [
        "A1.01.156.M0100",          # depth-1 table
        "B2.X.01.156.M0100",        # depth-2 table
        "B2.X.02.156.M0100",        # same table again -> derived once
        "C3.Y.Z.156.M0100|SPAN=3Y",  # depth-3 table (creativegoodsgr's span suffix shape)
        "D4.Q",                     # a key that IS a catalogue id (the equality arm)
        "E5.R.01.156.M0100",        # table not catalogued
    ])
    assert sorted(ids) == sorted([f"{src}:A1", f"{src}:B2.X", f"{src}:C3.Y.Z", f"{src}:D4.Q"])
    assert len(ids) == len(set(ids))
    assert unmapped == ["E5.R.01.156.M0100"]


@pytest.mark.parametrize("src", MEMBERS)
def test_dot_boundary_never_over_claims(src, tmp_path, monkeypatch):
    """`0000.1` must not claim `0000.10...` - the resolver's trailing '.' (_resolve.py)."""
    _catalogue(tmp_path, monkeypatch, [f"{src}:0000.1"])
    ids, unmapped = orchestrate._catalog_ids_for(src, ["0000.10.01.M0100", "0000.1.01.M0100"])
    assert ids == [f"{src}:0000.1"]
    assert unmapped == ["0000.10.01.M0100"]


def test_nested_prefixes_claim_every_id_the_resolver_serves(tmp_path, monkeypatch):
    """If both `A` and `A.B` were catalogued the resolver would serve `A.B.C` under BOTH, so a
    change to it makes both CSVs stale. 0 such cases exist today (measured over all 18 stores);
    the rule must stay correct if one appears."""
    src = MEMBERS[0]
    _catalogue(tmp_path, monkeypatch, [f"{src}:A", f"{src}:A.B"])
    ids, unmapped = orchestrate._catalog_ids_for(src, ["A.B.C"])
    assert sorted(ids) == [f"{src}:A", f"{src}:A.B"]
    assert unmapped == []


def test_a_new_resolver_member_is_mapped_with_no_second_edit(tmp_path, monkeypatch):
    """Adding a source to the resolver set is the ONLY edit a new DOT source needs."""
    monkeypatch.setattr(_resolve, "_DOT_TABLE_GRAIN", set(_resolve._DOT_TABLE_GRAIN) | {"unctad_zz_new"})
    _catalogue(tmp_path, monkeypatch, ["unctad_zz_new:1.2"])
    ids, unmapped = orchestrate._catalog_ids_for("unctad_zz_new", ["1.2.3.4.M0100"])
    assert ids == ["unctad_zz_new:1.2"] and unmapped == []


def test_non_members_keep_series_grain(tmp_path, monkeypatch):
    """A source outside the set must NOT collapse onto a prefix: it is served at series grain,
    so a prefix id would derive from a predicate that never selects the changed key."""
    assert "unctad_tariff" not in MEMBERS
    _catalogue(tmp_path, monkeypatch, ["unctad_tariff:1.2"])
    ids, unmapped = orchestrate._catalog_ids_for("unctad_tariff", ["1.2.3.4.M0100"])
    assert ids == [] and unmapped == ["1.2.3.4.M0100"]


@pytest.mark.parametrize("src", MEMBERS)
def test_round_trip_against_the_real_resolver(src, tmp_path, monkeypatch):
    """Mapper == inverse of the SERVING code, checked by running the real resolver.

    A one-file store holds every key; for each catalogued id, `_resolve.resolve` + `read_native`
    say which keys it serves. A changed key must map to exactly the ids that serve it."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    keys = ["A1.01.156.M0100", "B2.X.01.156.M0100", "B2.X", "B2.Xa.01.M0100",
            "C3.Y.Z.156.M0100", "0000.10.01.M0100", "0000.1.01.M0100", "E5.R.01.M0100"]
    cat = ["A1", "B2.X", "C3.Y.Z", "0000.1"]
    store = tmp_path / "store"
    (store / src).mkdir(parents=True)
    pq.write_table(pa.table({"series_key": keys, "obs_date": ["2024-12-31"] * len(keys),
                             "value": [1.0] * len(keys)}), store / src / f"{src}.parquet")
    served_by = {k: set() for k in keys}
    for nat in cat:
        res = _resolve.resolve(f"{src}:{nat}", root=str(store))
        try:
            got = _resolve.read_native(res).column(res.key_col).to_pylist()
        except _resolve.ResolveError:
            got = []
        for k in got:
            served_by[k].add(f"{src}:{nat}")
    _catalogue(tmp_path, monkeypatch, [f"{src}:{n}" for n in cat])
    for k in keys:
        ids, unmapped = orchestrate._catalog_ids_for(src, [k])
        assert set(ids) == served_by[k], (k, ids, served_by[k])
        assert (unmapped == [k]) == (not served_by[k]), (k, unmapped)


def test_dot_table_ids_unit():
    nat = {"1601.3", "200551", "203.02.ICT03", "X"}
    assert orchestrate._dot_table_ids("1601.3.01.897.M5019", nat) == ["1601.3"]
    assert orchestrate._dot_table_ids("200551.380.M6042", nat) == ["200551"]
    assert orchestrate._dot_table_ids("203.02.ICT03.096.M0100", nat) == ["203.02.ICT03"]
    assert orchestrate._dot_table_ids("1601.3", nat) == ["1601.3"]     # equality arm
    assert orchestrate._dot_table_ids("1601.30.1", nat) == []          # boundary
    assert orchestrate._dot_table_ids("Y.1", nat) == []
    assert orchestrate._dot_table_ids("X", nat) == ["X"]


def test_the_old_nine_keep_the_ids_they_mapped_before():
    """The nine members the deleted depth map covered, with its own test vectors: same answers."""
    old = {"unctad_intratrade": ("1601.3.01.897.M5019", "1601.3"),
           "unctad_biotrademerchrca": ("200551.380.M6042", "200551"),
           "unctad_ictgoods": ("203.02.ICT03.096.M0100", "203.02.ICT03"),
           "unctad_creativegoodsgr": ("156.CER070.1412.01.M4017|SPAN=3Y", "156.CER070")}
    for src, (key, want) in old.items():
        assert src in MEMBERS
        assert orchestrate._dot_table_ids(key, {want}) == [want]
