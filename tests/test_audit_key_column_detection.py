"""The store audit must not exclude a source because its key column is not called `series_key`.

`tools/audit_store_vs_catalog.py` tested `if "series_key" not in schema` and booked anything else
"not a series store". bls keys on `series_id` and holds 154,190,127 distinct series; eia likewise
at 3,862,801. Both vanished from every total the tool printed — **157,784,417 series of real gap,
larger than most of what it did report** — and nothing in the output said so. That is the defect
the review named (R825/R821): a guard keyed on one column name silently excludes whole sources.

Two things are pinned here, and the second matters as much as the first:

  1. the candidate list is the one `core/broaden_catalog.py::_key_col` already uses, so the two
     agree by construction rather than by both happening to be edited together;
  2. a store with NO recognised key column is REPORTED, not silently skipped — the same rule
     `--max-gb` already follows, and the reason `worldbank_esg` (which keys on `country`) must
     not read as a clean pass.
"""
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "audit_store_vs_catalog.py")
SRC = open(TOOL, encoding="utf-8").read()


def test_the_audit_tries_series_id_not_only_series_key():
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core.broaden_catalog import KEY_COL_CANDIDATES
    assert KEY_COL_CANDIDATES == ("series_key", "series_id", "idbank"), KEY_COL_CANDIDATES
    assert "KEY_COL_CANDIDATES" in SRC, (
        "the audit must use the shared candidate list; keying only on series_key silently "
        "excluded bls (154,190,127 series) and eia (3,862,801)"
    )


def test_there_is_exactly_ONE_definition_of_the_candidate_list():
    """This used to compare THREE hand-copied literals by scraping source text - and a review
    found the third copy, in `core/measure_uncataloged.py`, short by `idbank`. The comparison is
    the weaker guard: it can only catch drift that has already happened, in the files it happens
    to know about. One definition, imported, cannot drift at all - so what is pinned now is that
    no file re-types the tuple."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core.broaden_catalog import KEY_COL_CANDIDATES

    users = {
        "core/broaden_catalog.py": True,          # the definition itself
        "tools/audit_store_vs_catalog.py": False,
        "core/measure_uncataloged.py": False,
    }
    literal = re.compile(r'\(\s*"series_key"\s*,\s*"series_id"')
    for rel, is_owner in users.items():
        text = open(os.path.join(ROOT, *rel.split("/")), encoding="utf-8").read()
        hits = literal.findall(text)
        assert len(hits) == (1 if is_owner else 0), (
            f"{rel} re-types the key-column tuple ({len(hits)} time(s)); import "
            f"KEY_COL_CANDIDATES from core.broaden_catalog instead"
        )
        if not is_owner:
            assert "KEY_COL_CANDIDATES" in text, f"{rel} does not use the shared list"
    # ...and the shared list still holds every column a served store actually keys on
    for c in ("series_key", "series_id", "idbank"):
        assert c in KEY_COL_CANDIDATES, c


def test_a_store_with_no_key_column_is_reported_not_silent():
    assert "nokey" in SRC, "stores with no key column must be collected"
    assert "NOT MEASURED" in SRC, (
        "stores with no recognised key column must appear in the summary — silence reads as a "
        "clean pass (worldbank_esg keys on `country`)"
    )
    assert "not a series store" not in SRC, (
        "'not a series store' asserted something about the DATA; the tool only knows it did not "
        "recognise a column name"
    )


def test_the_queries_use_the_detected_key():
    """A detected key that the count query ignores would be decoration."""
    assert 'count(distinct "{key}")' in SRC, "the exact count must use the detected key"
    assert 'approx_count_distinct("{key}")' in SRC, "the HLL fallback must use the detected key"
    assert '"{key}")) from read_parquet' in SRC, (
        "the shard-qualified recount must use the detected key"
    )
    # The EXECUTABLE form only. Two comments legitimately quote `count(distinct series_key)`
    # while describing the tool's history and the shard-undercount, and a test that forbade the
    # phrase outright would forbid explaining the bug it exists to prevent.
    assert "select count(distinct series_key)" not in SRC, (
        "a query still hardcodes series_key"
    )
    assert "approx_count_distinct(series_key)" not in SRC, (
        "the HLL fallback still hardcodes series_key"
    )


def test_a_catalogued_source_with_no_store_directory_is_named():
    """`names` comes from os.listdir(STORE), so such a source is invisible to every verdict.

    Measured 2026-09-06: exactly one, sec_edgar at 17,467 catalogue rows — 75x the orphan total
    the tool did report, and it could never appear even as an ORPHAN.
    """
    assert "nostore" in SRC, "catalogued sources with no store directory must be collected"
    assert "no directory under" in SRC, (
        "they must appear in the summary; being absent from os.listdir is not a verdict"
    )


def test_a_missing_clean_full_dir_is_not_called_missing_data():
    """R289: serving reads clean_grouped/, so an empty clean_full prefix is a false darkness signal."""
    assert "clean_grouped" in SRC, (
        "the check must look in clean_grouped before implying the data is absent — sec_edgar "
        "lives there and is served from there"
    )
    assert "R289" in SRC, "cite the rule, so the next reader knows why the second tree is checked"


def test_the_tool_still_parses():
    import ast
    ast.parse(SRC)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
