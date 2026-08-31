"""WU-4: one-to-many expansion tiers (dst subjects, treasury endpoint tails,
wikidata group containment), each mirroring its resolver/fetcher predicate."""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402


@pytest.fixture
def cat(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    rows = [("dst:DST:ABST1", "dst"), ("dst:DST:AED01", "dst"), ("dst:DST:AED012", "dst"),
            ("dst:DST:AKU100K", "dst"), ("dst:DST:AKU100A", "dst"),
            # subject-also-a-table: _subj('ABST')=='ABST'=='_subj('ABST1')
            ("dst:DST:ABST", "dst"),
            # REGN10-class: _subj('REGN10A')=='REGN10' but _subj('REGN10')=='REGN'
            ("dst:DST:REGN10", "dst"), ("dst:DST:REGN10A", "dst"),
            ("treasury:debt_to_penny:tot_pub_debt_out_amt", "treasury"),
            ("treasury:debt_to_penny:intragov_hold_amt", "treasury"),
            ("treasury:avg_interest_rates:marketable:treasury_bills", "treasury"),
            ("wikidata:Q1", "wikidata"), ("wikidata:Q2", "wikidata")]
    con.executemany("INSERT INTO series VALUES (?,?)", rows)
    con.commit(); con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    from updater import config
    monkeypatch.setattr(config, "BACKEND", "r2")
    return p


def test_dst_subject_expansion(cat):
    """'DST:AED' (the fetcher's cursor key) must claim EVERY table in subject AED —
    today's name-coincidence hits returned 1 id where groups average 2.78 tables."""
    ids, unmapped = orchestrate._catalog_ids_for(
        "dst", ["DST:AED", "DST:AKU100", "DST:ZZZZZ"])
    assert sorted(ids) == ["dst:DST:AED01", "dst:DST:AED012",
                           "dst:DST:AKU100A", "dst:DST:AKU100K"]
    assert unmapped == ["DST:ZZZZZ"]
    # the digit-ending subject is THE discriminating case: the cursor key is the
    # subject verbatim; re-applying _subj would strip 'AKU100'->'AKU' and miss
    # (exactly 24 of 376 real keys broke that way in the first cut)


def test_dst_subject_also_a_table_claims_both(cat):
    """Order pin 1 (the review's REQUIRED fixture): subject 'ABST' is ALSO a
    catalogued table id. Exact-first claimed only dst:DST:ABST and starved ABST1;
    index-first claims the whole group — table ABST included, because
    _subj('ABST')=='ABST' puts it in its own group's expansion list."""
    ids, unmapped = orchestrate._catalog_ids_for("dst", ["DST:ABST"])
    assert sorted(ids) == ["dst:DST:ABST", "dst:DST:ABST1"]
    assert unmapped == []


def test_dst_regn10_class_claims_the_group_not_the_name_twin(cat):
    """Order pin 2 (REQUIRED fixture): subject 'REGN10' groups REGN10A, while
    TABLE REGN10 belongs to subject 'REGN' (_subj strips its trailing digits) —
    its rows live in a DIFFERENT subject file. Exact-first wrong-claimed REGN10
    (7 real ids in this class) and starved REGN10A (16 starved)."""
    ids, unmapped = orchestrate._catalog_ids_for("dst", ["DST:REGN10"])
    assert ids == ["dst:DST:REGN10A"], ids
    assert unmapped == []


def test_dst_non_subject_key_still_reaches_the_exact_tier(cat):
    """The fall-through half of the reorder: an index MISS must not swallow the
    key — 'DST:REGN10A' is no subject, but it IS a catalogued id."""
    ids, unmapped = orchestrate._catalog_ids_for("dst", ["DST:REGN10A"])
    assert ids == ["dst:DST:REGN10A"]
    assert unmapped == []


def test_dst_uses_the_fetchers_own_subject_rule():
    """R191/R192: the grouping predicate is IMPORTED from dst.py, never retyped."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__), "orchestrate.py"),
               encoding="utf-8").read()
    assert "from .strategies.fetchers.dst import _subj as _dst_subj" in src


def test_treasury_endpoint_tail_expansion(cat):
    ids, unmapped = orchestrate._catalog_ids_for(
        "treasury", ["v2/accounting/od/debt_to_penny",
                     "v2/accounting/od/avg_interest_rates",
                     "v1/accounting/dts/unknown_endpoint"])
    assert sorted(ids) == ["treasury:avg_interest_rates:marketable:treasury_bills",
                           "treasury:debt_to_penny:intragov_hold_amt",
                           "treasury:debt_to_penny:tot_pub_debt_out_amt"]
    assert unmapped == ["v1/accounting/dts/unknown_endpoint"]


def test_wikidata_companies_claims_all_and_others_do_not(cat):
    ids, unmapped = orchestrate._catalog_ids_for(
        "wikidata", ["companies", "currencies", "stock_exchanges"])
    assert sorted(ids) == ["wikidata:Q1", "wikidata:Q2"]
    assert sorted(unmapped) == ["currencies", "stock_exchanges"]


def test_treasury_and_wikidata_keep_exact_first(cat, tmp_path):
    """Drift pin (review recommendation): ONLY dst was moved ahead of the exact
    tier. If a treasury/wikidata key ever equals a catalogue id, the exact tier
    must claim it — a silent reorder of those two would change this result."""
    con = sqlite3.connect(os.environ["ECONDL_CATALOG"])
    con.execute("INSERT INTO series VALUES (?,?)",
                ("treasury:v2/accounting/od/debt_to_penny", "treasury"))
    con.execute("INSERT INTO series VALUES (?,?)", ("wikidata:companies", "wikidata"))
    con.commit(); con.close()
    ids, _ = orchestrate._catalog_ids_for("treasury", ["v2/accounting/od/debt_to_penny"])
    assert ids == ["treasury:v2/accounting/od/debt_to_penny"]
    ids, _ = orchestrate._catalog_ids_for("wikidata", ["companies"])
    assert ids == ["wikidata:companies"]


def test_subset_scopes_are_pinned():
    for sid in ("treasury", "wikidata", "worldbank_pink", "statcan"):
        assert orchestrate._catalog_scope(sid) == "subset", sid


def test_non_expansion_sources_unaffected(cat):
    ids, unmapped = orchestrate._catalog_ids_for("dst2", ["DST:AED"])
    assert ids == [] and unmapped == ["DST:AED"]
