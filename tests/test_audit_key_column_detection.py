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
    assert '"series_key", "series_id", "idbank"' in SRC, (
        "the audit must try the same key columns broaden_catalog does; keying only on "
        "series_key silently excluded bls (154,190,127 series) and eia (3,862,801)"
    )


def test_the_candidate_list_matches_broaden_catalog():
    """Two definitions that must not drift — assert they are literally the same tuple."""
    bc = open(os.path.join(ROOT, "core", "broaden_catalog.py"), encoding="utf-8").read()
    m = re.search(r'for c in \(([^)]*)\)', bc)
    assert m, "could not find broaden_catalog._key_col's candidate tuple"
    theirs = [x.strip().strip('"\'') for x in m.group(1).split(",") if x.strip()]
    mine = re.search(r'for c in \("series_key", "series_id", "idbank"\)', SRC)
    assert mine, "the audit's candidate list is not in the expected form"
    assert theirs == ["series_key", "series_id", "idbank"], (
        f"broaden_catalog now tries {theirs}; the audit must be updated to match"
    )


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


def test_the_tool_still_parses():
    import ast
    ast.parse(SRC)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
