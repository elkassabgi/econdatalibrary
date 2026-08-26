"""TABLE-GRAIN coherence mapping: `_catalog_ids_for` must reduce store keys the same way
the SERVING resolver does.

Why this file exists. Fourteen `imf_*_direct` sources are catalogued at TABLE grain while
their stores are at SERIES grain, and `_catalog_ids_for` had no rule for that shape — so
100% of every changed key missed, §5.7 booked "csv coherence unmet", the run demoted to
`partial`, `partial` never sets last_success_utc (R231), and the served CSVs never
re-derived. Measured on run 32970841711 (2026-08-26): imf_mfsma_direct reported 3,016
unmapped keys against exactly 3,016 distinct store keys — the R221/R245 fingerprint where
`unmapped == the source's own key count` means GRAIN mismatch, not a missing catalogue.

The hazard the tests actually guard is NOT "does it map". It is DRIFT. `updater/orchestrate.py`
and `clients/python/econdl/_resolve.py` now hold the same key-encoding knowledge twice, which
is the R192 class (worker and derive disagreeing on encoding 502'd 60,993 series). A reduction
that is WEAKER than the resolver's predicate is the dangerous direction: it maps a key the
resolver would refuse, so the id re-derives from a predicate that never selects that key and
the changed series stays stale while `unmapped` reports clean (R380's shape).

So the property test does not compare two tables of positions — it round-trips each entry
through the REAL resolver predicate against a real parquet, which is the only check that
fails when a resolver regex moves underneath the spec.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import types

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater.orchestrate import (_TABLE_GRAIN, _table_grain_native,  # noqa: E402
                                 _catalog_ids_for)

# One REAL store key per table-grain source, copied from the live R2 stores on 2026-08-26,
# paired with the catalog native id it must reduce to (also verified present in catalog.db).
# Real keys, not invented ones: a fixture built from my own hypothesis can only confirm it
# (R269), and an invented identifier lands its 404 exactly on the code under test (R220).
REAL_KEYS = {
    "imf_mfsma_direct":  ("MFS_MA:AFG.A.true.BM_MAI.XDC", "MFS_MA:AFG.A"),
    "imf_mfsir_direct":  ("MFS_IR:AFG.A.true.MFS162_RT_PT_A_PT", "MFS_IR:AFG.A"),
    "imf_mfsfmp_direct": ("MFS_FMP:ARG.A.true.EQTS.PA_IX", "MFS_FMP:ARG.A"),
    "imf_mfsdc_direct":  ("MFS_DC:AFG.A.true.FASMB_XDC.XDC", "MFS_DC:AFG.A"),
    "imf_mfsofc_direct": ("MFS_OFC:AGO.A.true.FOSAF_XDC.XDC", "MFS_OFC:AGO.A"),
    "imf_bopagg_direct": ("BOP_AGG:ABW.A.x.CAB_NETCD.BPM6.POGDP_PT", "BOP_AGG:ABW.A"),
    "imf_psbs_direct":   ("PSBS:ARG.A.true.GAXCE_G01_XDC.PGDP_PT", "PSBS:ARG.A"),
    "imf_ctot_direct":   ("CTOT:AFG.A.true.XM_IX.IX", "CTOT:AFG.A"),
    "imf_er_direct":     ("ER:AFG.A.true.ENDA_XDC_USD_RATE.RATE", "ER:AFG.A"),
    "imf_imts_direct":   ("IMTS:ABW.AFG.A.MG_CIF_USD.IMTS", "IMTS:ABW.A.MG_CIF_USD"),
    "imf_pip_direct":    ("PIP:A.ABW.S1.AFG.A.IAPD_BP6_USD.S1", "PIP:AFG.A.IAPD_BP6_USD"),
    "imf_dip_direct":    ("DIP:ABW.AIA.DO.A.IADD_BP6_USD", "DIP:AIA.A.IADD_BP6_USD"),
    "imf_gsli_direct":   ("GS_LI:Y10T14.NOT_AGGREGATED.ABW.A.F._Z._Z._Z._Z._Z.LLFPRT_RT",
                          "GS_LI:ABW.A"),
    "imf_qgfs_direct":   ("QGFS:BS.AUS.Q.true.GAL_G01_XDC.GFSM2014.S1311",
                          "QGFS:AUS.Q"),
}


def test_every_table_grain_source_has_a_real_key_fixture():
    """The fixtures must cover the constant — otherwise adding a source silently skips it."""
    assert set(REAL_KEYS) == set(_TABLE_GRAIN), (
        "REAL_KEYS and _TABLE_GRAIN disagree; add a MEASURED store key for any new source"
    )


@pytest.mark.parametrize("src", sorted(REAL_KEYS))
def test_reduction_matches_the_expected_catalog_native(src):
    key, want = REAL_KEYS[src]
    assert _table_grain_native(src, key) == want


@pytest.mark.parametrize("src", sorted(REAL_KEYS))
def test_reduction_round_trips_through_the_real_resolver(tmp_path, src):
    """THE test. Reduce the store key, then ask the SERVING resolver for that catalog id and
    assert its predicate selects the key we started from.

    This is what a positions-vs-positions comparison cannot do: it fails if a resolver regex,
    a part count or a wildcard class changes underneath the spec, without anyone remembering
    this file exists.
    """
    from econdl import _resolve

    key, _expected = REAL_KEYS[src]
    # Resolve the REDUCTION, not the fixture's expected native. Using the fixture here would
    # only prove the fixture is self-consistent — the test could never fail on a wrong
    # position in _TABLE_GRAIN, which is the single thing it exists to catch (R64/R346).
    native = _table_grain_native(src, key)
    assert native is not None, f"{src}: real store key did not reduce at all"
    cat_id = f"{src}:{native}"

    # A real parquet at the layout every one of these resolvers hardcodes: <root>/<src>/<src>.parquet
    d = tmp_path / src
    d.mkdir()
    decoy = "ZZZ:ZZZ.Z.decoy.DECOY.ZZZ"        # must NOT be selected
    tbl = pa.table({"series_key": [key, decoy],
                    "obs_date": [None, None],
                    "value": [1.0, 2.0]})
    pq.write_table(tbl, d / f"{src}.parquet")

    res = _resolve.resolve(cat_id, root=str(tmp_path))
    got = ds.dataset(res.parquet_path).to_table(filter=res.predicate)
    keys = got.column("series_key").to_pylist()

    assert key in keys, (
        f"{src}: reduced {key!r} -> {cat_id!r}, but the resolver predicate does NOT select "
        f"that key. The mapper is now WEAKER than the resolver and would derive a stale CSV."
    )
    assert decoy not in keys, f"{src}: predicate over-matched and selected the decoy"


# --------------------------------------------------------------------------- #
# Negative controls — a guard ships with a case it must BLOCK and one it must
# let through (R414). The positives above are the let-through half.
# --------------------------------------------------------------------------- #

def test_specs_do_not_leak_across_sources():
    """The mfs PREFIX rule applied to a mid-key source would silently produce a wrong id.

    imf_gsli_direct puts COUNTRY.FREQ at positions 2-3, so reducing its key with the mfs
    (0,1) rule yields `GS_LI:Y10T14.NOT_AGGREGATED` — a plausible-looking id that is not this
    series' table. That is the exact leak a shape test or an `imf_*_direct` glob would allow;
    a per-source spec makes it unreachable.
    """
    gsli_key = REAL_KEYS["imf_gsli_direct"][0]
    assert _table_grain_native("imf_gsli_direct", gsli_key) == "GS_LI:ABW.A"
    # the same key under the mfs spec would have produced the wrong table
    assert _table_grain_native("imf_mfsma_direct", gsli_key) != "GS_LI:ABW.A"


@pytest.mark.parametrize("src,bad", [
    ("imf_mfsma_direct", "MFS_MA:AFG"),              # no tail at all
    ("imf_mfsma_direct", "MFS_MA:AFG.A"),            # the catalog id itself is not a key
    ("imf_mfsma_direct", "no_colon_key"),            # not even flow-shaped
    ("imf_imts_direct",  "IMTS:ABW.AFG.A.MG_CIF_USD.NOTTHEFLOW"),   # tail != flow
    ("imf_pip_direct",   "PIP:A.ABW.S1.AFG.A..S1"),  # empty part, regex uses [^.]+
    ("imf_dip_direct",   "DIP:ABW.AIA.DO.A"),        # 4 parts, spec pins 5
    ("imf_qgfs_direct",  "QGFS:BS.AUS.Q.true.GAL_G01_XDC.GFSM2014"),  # 6 parts, pins 7
])
def test_wrong_shapes_do_not_map(src, bad):
    assert _table_grain_native(src, bad) is None


def test_unlisted_source_never_reduces():
    assert _table_grain_native("statfin", "MFS_MA:AFG.A.true.BM_MAI.XDC") is None


# --------------------------------------------------------------------------- #
# Drift guards, with the default INVERTED: a new source in this family must FAIL
# CI until a human declares its positions, rather than silently mapping nothing.
# --------------------------------------------------------------------------- #

def _resolver_fns():
    from econdl._resolve import _RESOLVERS
    return _RESOLVERS


def test_no_source_binds_a_table_grain_resolver_without_a_spec():
    """A 10th source binding to an EXISTING table-grain resolver (e.g. _resolve_imf_mfs_tables)
    fails here instead of quietly reporting 100% unmapped for weeks.

    Scope, stated honestly: this catches drift in an existing binding ONLY. It cannot see a
    brand-new table-grain resolver, because it derives its function set from _TABLE_GRAIN —
    proven by mutation, not assumed. `test_no_new_resolver_appears_without_being_classified`
    is the guard that covers that case."""
    R = _resolver_fns()
    table_grain_fns = {R[s] for s in _TABLE_GRAIN if s in R}
    offenders = sorted(s for s, fn in R.items()
                       if fn in table_grain_fns and s not in _TABLE_GRAIN)
    assert not offenders, (
        f"these sources are SERVED at table grain but have no _TABLE_GRAIN spec, so every "
        f"changed key of theirs will miss: {offenders}"
    )


# Every resolver function `_RESOLVERS` binds, pinned. THE DEFAULT IS INVERTED ON PURPOSE:
# adding ANY new resolver fails this test until a human puts its name here, and the act of
# doing so is the moment to ask "is this source TABLE grain? does it need a _TABLE_GRAIN
# spec?". Anything keyed off _TABLE_GRAIN itself cannot ask that question, because a resolver
# no spec references is invisible to it.
#
# THIS REPLACES A GUARD THAT DID NOT WORK. The first version asserted the names of the
# resolvers _TABLE_GRAIN's own members bind to and claimed a seventh table-grain resolver
# would fail CI. It would not: the adversarial reviewer added a genuinely new seventh
# resolver plus a source bound to it, with no spec, and all 46 tests passed silently. A guard
# whose population is derived from the thing it is guarding has no independent grip (R414 —
# it needs a case it must BLOCK, and this one had none).
_PINNED_RESOLVERS = [
    "_resolve_abs", "_resolve_bea", "_resolve_bis", "_resolve_bls", "_resolve_boe",
    "_resolve_census_any", "_resolve_cepii_baci", "_resolve_dbnomics", "_resolve_defillama",
    "_resolve_ecb", "_resolve_eia", "_resolve_ember", "_resolve_eurostat", "_resolve_faostat",
    "_resolve_fed_board", "_resolve_fhfa", "_resolve_file_grain", "_resolve_frankfurter",
    "_resolve_hf_equities", "_resolve_ilostat_any", "_resolve_imf",
    "_resolve_imf_dip_direct", "_resolve_imf_gsli_direct", "_resolve_imf_imts_direct",
    "_resolve_imf_mfs_tables", "_resolve_imf_pip_direct", "_resolve_imf_qgfs_direct",
    "_resolve_istat", "_resolve_noaa", "_resolve_oecd", "_resolve_owid", "_resolve_pwt",
    "_resolve_sec_edgar", "_resolve_statcan_any", "_resolve_treasury", "_resolve_usda",
    "_resolve_wdi", "_resolve_wikidata", "_resolve_worldbank", "_resolve_worldbank_esg",
    "_resolve_worldbank_pink", "_resolve_zillow",
]


def test_no_new_resolver_appears_without_being_classified():
    """A NEW resolver of any kind fails here — including a seventh TABLE-GRAIN one, which is
    precisely the case the previous version of this guard let through."""
    R = _resolver_fns()
    names = sorted({fn.__name__ for fn in R.values()})
    new = sorted(set(names) - set(_PINNED_RESOLVERS))
    gone = sorted(set(_PINNED_RESOLVERS) - set(names))
    assert not new, (
        f"new resolver(s) {new}: decide whether each serves a TABLE-GRAIN source and needs a "
        f"_TABLE_GRAIN spec in updater/orchestrate.py, then add the name to _PINNED_RESOLVERS"
    )
    assert not gone, f"resolver(s) {gone} disappeared; update _PINNED_RESOLVERS deliberately"


def test_every_spec_names_a_source_the_resolver_knows():
    R = _resolver_fns()
    missing = sorted(s for s in _TABLE_GRAIN if s not in R)
    assert not missing, f"_TABLE_GRAIN names sources with no resolver: {missing}"


# --------------------------------------------------------------------------- #
# End-to-end through the REAL call site. A helper-only test passes even when the
# helper is never wired in (R346), which is how a fix ships doing nothing.
# --------------------------------------------------------------------------- #

def _catalog(tmp_path, ids):
    db = tmp_path / "catalog.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [(i, i.split(":", 1)[0]) for i in ids])
    con.commit()
    con.close()
    return str(db)


def _r2(monkeypatch):
    """Pin the r2 branch. Under the default `local` backend `_catalog_ids_for` falls through
    to DERIVE-ALL, which returns EVERY catalog id for the source and empties `unmapped` — so
    a local-backend test cannot see the mapping at all, and would pass with the rule deleted.
    Same reason test_csv_coverage_note.py pins it."""
    from updater import config as C
    monkeypatch.setattr(C, "BACKEND", "r2", raising=False)


def test_catalog_ids_for_maps_table_grain_keys(monkeypatch, tmp_path):
    """The wiring test: three real mfsma keys collapsing onto two catalog tables."""
    src = "imf_mfsma_direct"
    _r2(monkeypatch)
    monkeypatch.setenv("ECONDL_CATALOG", _catalog(
        tmp_path, [f"{src}:MFS_MA:AFG.A", f"{src}:MFS_MA:AFG.M"]))
    keys = ["MFS_MA:AFG.A.true.BM_MAI.XDC",
            "MFS_MA:AFG.A.true.CIC1311_MAI.XDC",
            "MFS_MA:AFG.M.true.BM_MAI.XDC"]
    ids, unmapped = _catalog_ids_for(src, keys)
    assert sorted(ids) == [f"{src}:MFS_MA:AFG.A", f"{src}:MFS_MA:AFG.M"]
    assert unmapped == []


def test_catalog_ids_for_reports_a_genuinely_uncatalogued_table(monkeypatch, tmp_path):
    """The other half of the discriminating pair: a table with no catalogue row must still
    be REPORTED, not silently absorbed. MFS_IR:SYR.A is a real one (108 rows ending 2010,
    Syria, no catalogued siblings) — measured 2026-08-26."""
    src = "imf_mfsir_direct"
    _r2(monkeypatch)
    monkeypatch.setenv("ECONDL_CATALOG", _catalog(tmp_path, [f"{src}:MFS_IR:AFG.A"]))
    keys = ["MFS_IR:AFG.A.true.MFS162_RT_PT_A_PT",
            "MFS_IR:SYR.A.true.MFS162_RT_PT_A_PT"]
    ids, unmapped = _catalog_ids_for(src, keys)
    assert ids == [f"{src}:MFS_IR:AFG.A"]
    assert unmapped == ["MFS_IR:SYR.A.true.MFS162_RT_PT_A_PT"]


def test_without_the_rule_the_keys_would_all_miss(monkeypatch, tmp_path):
    """Pins the DEFECT, so this file fails if the reduction is ever removed or bypassed.

    Same catalogue and keys as the mapping test, with _TABLE_GRAIN emptied: every key must
    go unmapped, which is the pre-fix behaviour that demoted the run every night."""
    import updater.orchestrate as O
    src = "imf_mfsma_direct"
    _r2(monkeypatch)
    monkeypatch.setenv("ECONDL_CATALOG", _catalog(
        tmp_path, [f"{src}:MFS_MA:AFG.A", f"{src}:MFS_MA:AFG.M"]))
    monkeypatch.setattr(O, "_TABLE_GRAIN", {}, raising=True)
    keys = ["MFS_MA:AFG.A.true.BM_MAI.XDC", "MFS_MA:AFG.M.true.BM_MAI.XDC"]
    ids, unmapped = O._catalog_ids_for(src, keys)
    assert ids == []
    assert sorted(unmapped) == sorted(keys)


# --------------------------------------------------------------------------- #
# The COST guard. Mapping these ids is what queues them for the D1 catalog sync,
# and that sync's FTS delete is the project's most expensive recorded shape.
# --------------------------------------------------------------------------- #

def test_fts_delete_arity_is_far_above_the_row_chunk():
    """`series_fts` is fts5(series_id UNINDEXED, ...), so `WHERE series_id IN (...)` full-scans
    the table: MEASURED 23,843,482 rows_read for a 20-id list on live D1 (2026-08-26). The
    cost is per STATEMENT, so arity is the only lever (R492 — a 164,705-statement plan priced
    at ~$2,500). At 20 ids/stmt this fix's own 20,783 newly-mapped ids would read 2.48e10
    rows per run, recurring; at 500 it is 1.0e9.

    Pinned as a PROPERTY (a large multiple of the row chunk), not as the constant copied back
    (R319 — a test that restates the implementation cannot fail for the right reason)."""
    from core.sync_catalog_d1 import FTS_DELETE_PER_STMT
    from core.sync_state_d1 import ROWS_PER_STMT
    assert FTS_DELETE_PER_STMT >= 20 * ROWS_PER_STMT, (
        "the FTS delete must batch far more ids per statement than the INSERT chunk; "
        "each statement is one full scan of series_fts regardless of list length"
    )


def test_emit_sql_deletes_every_id_it_inserts(tmp_path):
    """The FTS delete now spans a BLOCK of insert chunks. Prove no id is inserted without
    first being deleted — an FTS row inserted over a surviving old one is the duplication
    this delete exists to stop (R487: boc reached 8.00 copies of every id)."""
    import re
    from core.sync_catalog_d1 import emit_sql, FTS_DELETE_PER_STMT
    rows = [{"series_id": f"src:{i:05d}", "source_id": "src",
             "title": f"t{i}", "geography": None}
            for i in range(FTS_DELETE_PER_STMT + 37)]     # deliberately not a whole block
    # emit_sql WRITES .sql files and returns their PATHS, not the statements.
    paths = emit_sql(["series_id", "source_id", "title", "geography"], rows, str(tmp_path))
    stmts = []
    for p in paths:
        with open(p, encoding="utf-8") as fh:
            stmts.extend(s + ";" for s in fh.read().split(";\n") if s.strip())

    deleted_before_insert, deleted = set(), set()
    # ADJACENCY, not merely order. `open_block` is the ids deleted by the MOST RECENT delete;
    # every FTS insert must draw only from it. Asserting "deleted at some earlier point"
    # instead would pass just as happily if every DELETE were hoisted into one leading pass —
    # which is exactly the wide deleted-but-not-reinserted window the code comment says it
    # closes, so the test has to be able to fail on it (R487, R414).
    open_block: set[str] = set()
    for s in stmts:
        s = s.lstrip()
        if s.startswith("DELETE FROM series_fts"):
            ids = set(re.findall(r"'(src:\d+)'", s))
            deleted |= ids
            open_block = ids
        elif s.startswith("INSERT INTO series_fts"):
            ins = set(re.findall(r"'(src:\d+)'", s))
            assert ins <= open_block, (
                "an FTS insert carried ids outside the most recent DELETE block — the delete "
                "is no longer adjacent to the inserts it covers"
            )
            deleted_before_insert |= ins
    want = {r["series_id"] for r in rows}
    assert deleted == want, "some ids were inserted into series_fts with no matching DELETE"
    assert deleted_before_insert == want, "an FTS insert preceded its own DELETE"
