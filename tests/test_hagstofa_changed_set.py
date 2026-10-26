"""hagstofa Route B: the merge-measured changed set, and the scheme guard on it (review AR-378, ledger R1668).

finalize() raises DefinitiveError whenever any table is refused, and the orchestrator maps a raise to
`partial` with NO Result: no CSV phase and no series_cursors. Hagstofa has had refused tables on every
pass since 2026-07-14, so the updater never derived a hagstofa CSV while every pass merged rows into the
store (228 of 1,068 served CSVs differed from the R2 store on 2026-10-23). Now:
  - the merge reports which TABLES changed (merge_and_write(report_changed_keys=True), folded to the
    table prefix = the catalogue grain);
  - when finalize raises and something changed, update() returns `partial` WITH that set and the raise's
    own text (the _giant.run_giant pattern); when nothing changed it re-raises unchanged;
  - a table whose stored rows hold more than one key scheme - two dimension-name schemes, or a second
    value-code system under the same names - is never in the set: it is dropped and named.
The real update() and merge run; only _fetch_table is faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

D0 = dt.date(2024, 12, 31)
D1 = dt.date(2025, 12, 31)
DB = "Samfelag"


def _p(path):
    return H._table_prefix(DB, path)


def _store(tmp_path, rows):
    """rows: [(series_key, date, value)] -> Samfelag.parquet"""
    pq.write_table(pa.table({"series_key": pa.array([r[0] for r in rows], pa.string()),
                             "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                             "value": pa.array([r[2] for r in rows], pa.float64())}),
                   str(tmp_path / f"{DB}.parquet"))


def _keys(tmp_path):
    p = tmp_path / f"{DB}.parquet"
    return set(pq.read_table(str(p)).column("series_key").to_pylist()) if p.exists() else set()


def _run(tmp_path, monkeypatch, paths, fetch):
    """fetch: {path: (rows, outcome)} for _fetch_table; a path missing from it answers ([], 'quiet')."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": DB, "path": p, "id": p.rsplit("/", 1)[-1],
                                                     "text": "x"} for p in paths])
    monkeypatch.setattr(H, "_fetch_table", lambda sess, db, path, prefix, since, **k:
                        fetch.get(path, ([], "quiet")))
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    return H.update(unit, None)


A, R, M = "x/A01.px", "x/R01.px", "x/M01.px"          # a clean table, a refused table, a mixed table


# ---- 1-3: the raise path -------------------------------------------------------------------------

def test_a_structural_pass_that_merged_returns_partial_with_the_changed_set(tmp_path, monkeypatch):
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    res = _run(tmp_path, monkeypatch, [A, R], {A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"),
                                               R: ([], "structural")})
    assert res.status == "partial", res
    assert res.changed_keys == {_p(A): D1.isoformat()}, res.changed_keys
    assert R in res.error and "structural" in res.error, "the refusal stays named: nothing is hidden"
    assert res.series_cursors and res.series_cursors[_p(A)] == D1.isoformat(), res.series_cursors
    assert f"{_p(A)}:Kyn=0" in _keys(tmp_path)


def test_a_structural_pass_that_merged_nothing_re_raises(tmp_path, monkeypatch):
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    with pytest.raises(H.DefinitiveError) as ei:
        _run(tmp_path, monkeypatch, [A, R], {R: ([], "structural")})
    assert R in str(ei.value)


def test_an_idempotent_merge_on_the_raise_path_re_raises(tmp_path, monkeypatch):
    """The boundary re-fetch brings back exactly what is stored: rows flowed, nothing changed."""
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    with pytest.raises(H.DefinitiveError):
        _run(tmp_path, monkeypatch, [A, R], {A: ([(f"{_p(A)}:Kyn=0", D0, 1.0)], "data"),
                                             R: ([], "structural")})


# ---- the normal path -------------------------------------------------------------------------------

def test_an_idempotent_merge_gives_an_empty_changed_set(tmp_path, monkeypatch):
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    res = _run(tmp_path, monkeypatch, [A], {A: ([(f"{_p(A)}:Kyn=0", D0, 1.0)], "data")})
    assert res.changed_keys == {}, res.changed_keys     # {} is a statement ("nothing changed"), not None
    assert res.series_cursors == {_p(A): D0.isoformat()}


def test_a_revised_value_is_a_change_and_an_untouched_table_is_not(tmp_path, monkeypatch):
    """The cursors are SEEDED with every stored table; the changed set must name only what merged."""
    B = "x/B01.px"
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0), (f"{_p(B)}:Kyn=0", D0, 5.0)])
    res = _run(tmp_path, monkeypatch, [A, B], {A: ([(f"{_p(A)}:Kyn=0", D0, 1.5)], "data")})
    assert res.status in ("ok", "partial") and res.changed_keys == {_p(A): D0.isoformat()}, res
    assert _p(B) in res.series_cursors, "the untouched table keeps its cursor (health needs it)"


def test_changed_keys_fold_to_the_table_and_keep_the_newest_date(tmp_path, monkeypatch):
    """Value codes can hold ':' (THJ11002 'A: 01'); the fold cuts at the '.px' segment, never at a colon."""
    _store(tmp_path, [(f"{_p(A)}:Grein=A: 01:Kyn=0", D0, 1.0), (f"{_p(A)}:Grein=A: 01:Kyn=0", D1, 1.0)])
    # the merge reports the NEW series (Kyn=1, at the older date) before the REVISED one (Kyn=0, at the
    # newer date), so "first key wins" would fold to the older date
    rows = [(f"{_p(A)}:Grein=A: 01:Kyn=0", D1, 9.0), (f"{_p(A)}:Grein=A: 01:Kyn=1", D0, 3.0)]
    res = _run(tmp_path, monkeypatch, [A], {A: (rows, "data")})
    assert res.changed_keys == {_p(A): D1.isoformat()}, res.changed_keys


# ---- 4: a refused table is never in the set ---------------------------------------------------------

def test_a_refused_table_is_never_in_the_changed_set(tmp_path, monkeypatch):
    """R is RESTRUCTURED (a key scheme it never stored): refused, not merged, not derived."""
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0), (f"{_p(R)}:Kyn=0", D0, 1.0)])
    res = _run(tmp_path, monkeypatch, [A, R], {A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"),
                                               R: ([(f"{_p(R)}:Sex=0", D1, 2.0)], "data")})
    assert res.status == "partial" and "RESTRUCTURED" in res.error, res
    assert set(res.changed_keys) == {_p(A)}, res.changed_keys
    assert f"{_p(R)}:Sex=0" not in _keys(tmp_path), "the refused rows are not in the store either"


def test_guard_drops_a_table_refused_in_this_pass_even_if_it_was_reported():
    out, held = H._guard_changed({_p(A): "2025-12-31", _p(R): "2025-12-31"}, {}, {_p(R)})
    assert out == {_p(A): "2025-12-31"} and held == {_p(R): "refused in this pass"}


# ---- 5: the scheme guard: two-scheme tables are dropped with a note --------------------------------

def test_a_table_with_two_name_schemes_is_dropped_with_a_note(tmp_path, monkeypatch):
    """SJA04903's shape: the stored rows hold two dimension-name schemes. The DOMINANT scheme still
    merges (the store gains the 2025 row), but the table must not be derived: its CSV would serve both."""
    stored = [(f"{_p(M)}:Tegund={i}:Land=0", D0, 1.0) for i in range(5)] + [(f"{_p(M)}:Species=0:Country=0", D0, 1.0)]
    _store(tmp_path, stored + [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    res = _run(tmp_path, monkeypatch, [A, M], {A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"),
                                               M: ([(f"{_p(M)}:Tegund=0:Land=0", D1, 2.0)], "data")})
    assert set(res.changed_keys) == {_p(A)}, res.changed_keys
    assert "NOT derived" in res.error and "M01" in res.error and "dimension-name" in res.error, res.error
    assert res.status == "partial"
    assert f"{_p(M)}:Tegund=0:Land=0" in _keys(tmp_path)


def test_a_table_with_a_second_value_code_system_is_dropped_with_a_note(tmp_path, monkeypatch):
    """VIN00900's shape: positional codes since 2003, then label text under the SAME dimension names. The
    stored boundary is already label-coded, so the fetch passes every merge guard and keeps merging."""
    stored = ([(f"{_p(M)}:Kyn={c}", dt.date(y, 12, 31), 1.0) for c in "012" for y in (2003, 2023)]
              + [(f"{_p(M)}:Kyn={c}", D0, 1.0) for c in ("Alls", "Karlar", "Konur")])
    _store(tmp_path, stored + [(f"{_p(A)}:Kyn=0", D0, 1.0)])
    fetched = [(f"{_p(M)}:Kyn={c}", d, 2.0) for c in ("Alls", "Karlar", "Konur") for d in (D0, D1)]
    res = _run(tmp_path, monkeypatch, [A, M], {A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"), M: (fetched, "data")})
    assert set(res.changed_keys) == {_p(A)}, res.changed_keys
    assert "NOT derived" in res.error and "value-code" in res.error, res.error


def test_a_table_that_becomes_two_scheme_in_this_merge_is_dropped(tmp_path, monkeypatch):
    """The re-code whose fetch no longer carries the stored boundary period (R1153 (v)): no stored series
    is fetched at the boundary, so the RE-CODED rule has nothing to compare and the rows merge. The guard
    reads the store AFTER the merge, so the table never reaches its CSV."""
    stored = [(f"{_p(M)}:Kyn={c}", dt.date(y, 12, 31), 1.0) for c in "012" for y in (2003, 2024)]
    _store(tmp_path, stored)
    fetched = [(f"{_p(M)}:Kyn={c}", D1, 2.0) for c in ("Alls", "Karlar", "Konur")]
    with pytest.raises(H.DefinitiveError) as ei:          # nothing else changed: re-raised, named
        _run(tmp_path, monkeypatch, [M, R], {M: (fetched, "data"), R: ([], "structural")})
    assert "NOT derived" in str(ei.value) and "M01" in str(ei.value), str(ei.value)
    assert f"{_p(M)}:Kyn=Alls" in _keys(tmp_path), "precondition: the re-coded rows did merge"


def test_negative_control_a_single_scheme_table_with_a_new_member_is_derived(tmp_path, monkeypatch):
    stored = [(f"{_p(M)}:Land={c}", D0, 1.0) for c in "0123456"] + [(f"{_p(M)}:Land=Alls", D0, 9.0)]
    _store(tmp_path, stored)
    res = _run(tmp_path, monkeypatch, [M], {M: ([(f"{_p(M)}:Land={c}", D1, 2.0) for c in "01234567"], "data")})
    assert res.changed_keys == {_p(M): D1.isoformat()} and res.status == "ok", res


# ---- the rule itself, on a scheme profile ----------------------------------------------------------

def _rule(rows, p):
    return H._second_key_scheme(H._scheme_profile(rows, p))


def _d(y):
    return dt.date(y, 12, 31)


def test_rule_flags_digit_codes_then_a_text_system():
    p = _p(M)
    why = _rule([(f"{p}:Kyn={c}", _d(y)) for c in "012" for y in (2003, 2025)]
                + [(f"{p}:Kyn={c}", _d(2025)) for c in ("Alls", "Karlar", "Konur")], p)
    assert why and "'Kyn'" in why and "3 text code(s)" in why, why


def test_rule_flags_text_codes_then_a_digit_system():
    p = _p(M)
    why = _rule([(f"{p}:Kyn={c}", _d(y)) for c in ("Alls", "Karlar", "Konur") for y in (2003, 2025)]
                + [(f"{p}:Kyn={c}", _d(2025)) for c in "012"], p)
    assert "3 digit code(s)" in (why or ""), why


def test_rule_flags_two_name_schemes():
    p = _p(M)
    why = _rule([(f"{p}:Kyn=0", _d(2020)), (f"{p}:Sex=0", _d(2020))], p)
    assert "dimension-name" in (why or "") and "[" not in why, why


@pytest.mark.parametrize("rows", [
    # born mixed: digit and text codes from the first day (HS-like codes)
    [("Vara={}".format(c), y) for c in ("01", "02", "0201A", "0201B") for y in (2003, 2025)],
    # a new total: two text members beside many digit codes
    [("Land={}".format(c), y) for c in "0123456789" for y in (2003, 2025)]
    + [("Land=Alls", 2025), ("Land=Annad", 2025)],
    # a full cube landing after aggregated history: new digit AND new text codes on the same day
    [("Aldur=0", 1990), ("Aldur=0", 2022)] + [("Aldur={}".format(c), 2022) for c in ("16", "25", "67+", "Y16-64")],
    # the same with a two-code history: only "no new code of the other kind that day" separates it
    [("Aldur={}".format(c), y) for c in "01" for y in (1990, 2022)]
    + [("Aldur={}".format(c), 2022) for c in ("16", "25", "67+", "Y16-64")],
    # aggregated history (one total code) then the cube's text codes: one digit code is not a system
    [("Aldur=0", 1990), ("Aldur=0", 2023)] + [("Aldur={}".format(c), 2023) for c in ("16-24", "25-54", "55+")],
    # a time-only table (no dimensions)
    [("", 2003), ("", 2025)],
    # new members of the kind that already DOMINATES (FYR04200: 55 digit codes and 2 text codes, then
    # 2 more digit codes) - a legit release, held for good without the minority clause (review AR-380)
    [("Grein={}".format(c), y) for c in [str(i) for i in range(55)] + ["A", "B"] for y in (1998, 2007)]
    + [("Grein={}".format(c), 2008) for c in ("55", "56")],
])
def test_rule_negative_controls_are_one_scheme(rows):
    p = _p(M)
    assert _rule([((f"{p}:{k}" if k else p), _d(y)) for k, y in rows], p) is None


def test_the_profile_read_from_the_store_matches_the_one_built_from_rows(tmp_path):
    """_per_table_profile(codes=...) is what update() uses; _scheme_profile is what the census uses."""
    rows = ([(f"{_p(M)}:Kyn={c}", _d(y), 1.0) for c in "012" for y in (2003, 2023)]
            + [(f"{_p(M)}:Kyn={c}", _d(2024), 1.0) for c in ("Alls", "Karlar", "Konur")])
    _store(tmp_path, rows)
    codes = {}
    H._per_table_profile(str(tmp_path / f"{DB}.parquet"), codes=codes)
    assert codes[_p(M)] == H._scheme_profile([(k, d) for k, d, _v in rows], _p(M))
    assert "value-code" in H._second_key_scheme(codes[_p(M)])


def test_guard_holds_a_key_it_cannot_cut_to_a_table():
    out, held = H._guard_changed({"ICE:Samfelag:no-table-here": None}, {}, set())
    assert out == {} and "no '.px'" in held["ICE:Samfelag:no-table-here"]


def test_the_note_names_every_held_table_up_to_the_bound():
    held = {_p(f"x/T{i:02d}.px"): "stored rows hold 2 dimension-name schemes" for i in range(12)}
    msg = H._with_held("hagstofa: 1/2 sub-unit(s) refused [x/R01.px]", held)
    assert "12 changed table(s) NOT derived" in msg and "+2 more" in msg, msg
    assert all(f"T{i:02d}.px" in msg for i in range(H.HELD_NAMED_MAX)), msg
    assert not any(f"T{i:02d}.px" in msg for i in range(H.HELD_NAMED_MAX, 12)), "the bound holds"
    assert msg.index("NOT derived") < msg.index(" [x/R01.px]"), "before the offender list"


def test_rule_a_tie_between_the_two_kinds_is_not_a_second_system():
    """THE CHOICE AT EQUALITY (review AR-380 N4): a dimension born with 2 digit and 2 text codes that later
    gains 2 more text codes is not held - the new kind must be strictly the MINORITY so far."""
    p = _p(M)
    rows = ([(f"{p}:Vara={c}", _d(y)) for c in ("1", "2", "A", "B") for y in (2003, 2025)]
            + [(f"{p}:Vara={c}", _d(2025)) for c in ("C", "D")])
    assert _rule(rows, p) is None


# ---- the orchestrator: a merge-measured changed set never widens to derive-all (review AR-380) ------
# Adapted from the review's probe: real update(), real _derive_changed_csvs and _catalog_ids_for; only
# derive_and_put is faked. U is a changed table with NO catalogue row; on any backend but r2 one unmapped
# key used to make _catalog_ids_for derive EVERY catalogued id (hagstofa's 1,068 is under the 5,000 cap),
# which re-published the mixed table M the fetcher holds back.

U = "x/U01.px"


def _catalog(tmp_path, monkeypatch, backend, tables):
    from updater import orchestrate
    monkeypatch.setattr(orchestrate.config, "BACKEND", backend)
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"hagstofa:{_p(t)}", "hagstofa") for t in tables])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    monkeypatch.setattr(orchestrate, "_REG_ENTRIES", None)


def _derive_asked(tmp_path, monkeypatch, res, backend):
    from updater import derive, orchestrate
    _catalog(tmp_path, monkeypatch, backend, [A, M])
    asked = []
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="hagstofa/_all", source_id="hagstofa", unit_id="_all")
    orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    return sorted(asked)


def _pass_with_uncatalogued(tmp_path, monkeypatch):
    stored = ([(f"{_p(M)}:Kyn={c}", _d(y), 1.0) for c in "012" for y in (2003, 2023)]
              + [(f"{_p(M)}:Kyn={c}", D0, 1.0) for c in ("Alls", "Karlar", "Konur")])
    _store(tmp_path, stored + [(f"{_p(A)}:Kyn=0", D0, 1.0), (f"{_p(U)}:Kyn=0", D0, 1.0)])
    res = _run(tmp_path, monkeypatch, [A, M, U, R], {
        A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"),
        U: ([(f"{_p(U)}:Kyn=0", D1, 2.0)], "data"),
        M: ([(f"{_p(M)}:Kyn={c}", D1, 2.0) for c in ("Alls", "Karlar", "Konur")], "data"),
        R: ([], "structural")})
    assert set(res.changed_keys) == {_p(A), _p(U)}, "precondition: M held, U kept"
    return res


@pytest.mark.parametrize("backend", ["local", "selfhost", "r2"])
def test_a_merge_measured_set_derives_only_its_mapped_ids_on_every_backend(tmp_path, monkeypatch, backend):
    res = _pass_with_uncatalogued(tmp_path, monkeypatch)
    assert _derive_asked(tmp_path, monkeypatch, res, backend) == [f"hagstofa:{_p(A)}"]


@pytest.mark.parametrize("merge_measured", [True, False])
@pytest.mark.parametrize("mapper_raises", [False, True])
def test_map_changed_sets_the_switch_for_the_call_and_always_restores_it(monkeypatch, merge_measured,
                                                                         mapper_raises):
    """Review AR-380 N3: a switch left off would silently take derive-all from every cursor-reported source
    off r2 for the rest of the process - including after a mapper exception."""
    from updater import orchestrate
    seen = []

    def fake(source_id, keys):
        seen.append(orchestrate._DERIVE_ALL_FALLBACK)
        if mapper_raises:
            raise OSError("catalog unreadable")
        return [], list(keys)

    monkeypatch.setattr(orchestrate, "_catalog_ids_for", fake)
    if mapper_raises:
        with pytest.raises(OSError):
            orchestrate._map_changed("hagstofa", ["k"], merge_measured=merge_measured)
    else:
        orchestrate._map_changed("hagstofa", ["k"], merge_measured=merge_measured)
    assert seen == [not merge_measured]
    assert orchestrate._DERIVE_ALL_FALLBACK is True


def test_control_a_cursor_reported_set_still_derives_all_off_r2(tmp_path, monkeypatch):
    """The fallback is kept for un-migrated fetchers (changed_keys None): same keys, as cursors."""
    res = _pass_with_uncatalogued(tmp_path, monkeypatch)
    cur = types.SimpleNamespace(status="partial", obs=res.obs, changed_keys=None, merged_rows=None,
                                series_cursors={_p(A): "2025-12-31", _p(U): "2025-12-31"})
    assert _derive_asked(tmp_path, monkeypatch, cur, "local") == sorted([f"hagstofa:{_p(A)}",
                                                                         f"hagstofa:{_p(M)}"])


def test_table_of_cuts_at_the_px_segment():
    assert H._table_of("ICE:Samfelag:a:b:VIN00900.px:Kyn=0:Eining=A: 1") == "ICE:Samfelag:a:b:VIN00900.px"
    assert H._table_of("ICE:Samfelag:a:VIN00900.px") == "ICE:Samfelag:a:VIN00900.px"
    assert H._table_of("no table here") is None and H._table_of("") is None


# ---- the half threshold of the value rule (review AR-379 M17) --------------------------------------

@pytest.mark.parametrize("n_digit,n_text,held", [(4, 2, True), (3, 2, True), (5, 2, False)])
def test_rule_new_codes_must_be_at_least_half_of_the_other_kind(n_digit, n_text, held):
    """2 new text codes beside 4 (exactly half) or 3 digit codes are a second system; beside 5 they are
    new members. `new >= other_seen` would miss the first two."""
    p = _p(M)
    rows = ([(f"{p}:Kyn={c}", _d(y)) for c in map(str, range(n_digit)) for y in (2003, 2025)]
            + [(f"{p}:Kyn=T{i}", _d(2025)) for i in range(n_text)])
    assert bool(_rule(rows, p)) is held


# ---- the QUARANTINE: measured store damage no fetch guard can see (review AR-379) ------------------

def test_the_quarantine_holds_a_listed_table_with_its_evidence(tmp_path, monkeypatch):
    """A single-scheme table (no rule fires) that is on the list merges, but never reaches the changed set,
    and the note names it with its evidence."""
    _store(tmp_path, [(f"{_p(A)}:Kyn=0", D0, 1.0), (f"{_p(M)}:Kyn=0", D0, 1.0)])
    monkeypatch.setattr(H, "QUARANTINE", {_p(M): "OFFSET 9 of 9 at code -1 (test)"})
    res = _run(tmp_path, monkeypatch, [A, M], {A: ([(f"{_p(A)}:Kyn=0", D1, 2.0)], "data"),
                                               M: ([(f"{_p(M)}:Kyn=0", D1, 2.0)], "data")})
    assert set(res.changed_keys) == {_p(A)}, res.changed_keys
    assert res.status == "partial" and "quarantined: OFFSET 9 of 9" in res.error, res.error
    assert f"{_p(M)}:Kyn=0" in _keys(tmp_path), "the merge itself is unchanged"


def test_negative_control_an_unlisted_table_is_derived(tmp_path, monkeypatch):
    _store(tmp_path, [(f"{_p(M)}:Kyn=0", D0, 1.0)])
    monkeypatch.setattr(H, "QUARANTINE", {_p(A): "listed elsewhere"})
    res = _run(tmp_path, monkeypatch, [M], {M: ([(f"{_p(M)}:Kyn=0", D1, 2.0)], "data")})
    assert res.changed_keys == {_p(M): D1.isoformat()} and res.status == "ok", res


def test_the_shipped_quarantine_lists_the_37_measured_tables():
    from updater.strategies.fetchers._hagstofa_quarantine import QUARANTINE
    assert H.QUARANTINE is QUARANTINE and len(QUARANTINE) == 37
    by_table = {p.split(":")[-1]: (p, ev) for p, ev in QUARANTINE.items()}
    for t in ("SKO02102.px", "UMH31220.px", "UMH31230.px", "UMH31101.px", "UMH31107.px"):
        assert t in by_table, t
    assert "OFFSET" in by_table["SKO02102.px"][1] and "RESCALE" in by_table["UMH31230.px"][1]
    for p, ev in QUARANTINE.items():
        assert H._table_of(p) == p and p.startswith("ICE:"), p           # a table prefix, as the fold makes
        assert ev.strip() and "E3 2026-10-24" in ev, (p, ev)              # every entry carries its evidence


# ---- merge_measured at the csv-fence and crash call sites (review AR-380 N1/N2, AR-379) ------------

def _record_map(monkeypatch, raises=False):
    from updater import orchestrate
    seen = []

    def fake(source_id, keys, merge_measured):
        seen.append(merge_measured)
        if raises:
            raise RuntimeError("mapper down")
        return [], list(keys)

    monkeypatch.setattr(orchestrate, "_map_changed", fake)
    monkeypatch.setattr(orchestrate, "_csv_misses", lambda sid: "desktop_owed")
    monkeypatch.setattr(orchestrate, "_note_unmapped_debt", lambda *a, **k: True)
    return seen


@pytest.mark.parametrize("merge_measured", [True, False])
def test_the_fence_site_passes_merge_measured(monkeypatch, merge_measured):
    from updater import orchestrate
    seen = _record_map(monkeypatch)
    res = types.SimpleNamespace(changed_keys=({"k": None} if merge_measured else None),
                                series_cursors={"k": "2025-12-31"})
    unit = types.SimpleNamespace(key="x/_all", source_id="x", unit_id="_all")
    orchestrate._book_fence_trip(unit, res, store=None, fence_min=60)
    assert seen == [merge_measured]


@pytest.mark.parametrize("merge_measured", [True, False])
def test_the_crash_site_passes_merge_measured(monkeypatch, merge_measured):
    """The first mapping raises (the crash); the csv_misses branch maps again: both calls carry it."""
    from updater import orchestrate
    seen = _record_map(monkeypatch, raises=True)
    res = types.SimpleNamespace(status="partial", obs=1, merged_rows=None,
                                changed_keys=({"k": None} if merge_measured else None),
                                series_cursors={"k": "2025-12-31"})
    unit = types.SimpleNamespace(key="x/_all", source_id="x", unit_id="_all")
    orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert seen == [merge_measured, merge_measured], seen
