"""`audit_store_vs_catalog` must not call its ORPHAN count a 404 count (R825).

The tool compares the CATALOGUE against the LOCAL PARQUET STORE. Neither is what a user
receives — the worker serves pre-derived CSVs from R2 — so "catalogued but not hosted", which is
what the summary line used to say, is a claim the measurement cannot support. Measured
2026-09-06 on fed_board, the largest orphan set this tool has ever reported: 60 of 60 sampled ids
HAD a live CSV, with a present control at 20/20 and a fabricated id correctly 404ing.

The number is also a FLOOR, because `gap = n - cat` is a net: a source with as many uncatalogued
store keys as uncatalogued catalogue rows never reaches the ORPHAN branch. fed_board's real split
is 638 / 406, and the tool can only ever print their difference, 232 — a number matching neither
half, whose halves have opposite fixes.

These tests pin the WORDING, which is unusual and deliberate: the defect was never in the
arithmetic. The count was right and its label was wrong, and a label is what a later session
reads.
"""
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOL = os.path.join(ROOT, "tools", "audit_store_vs_catalog.py")
SRC = open(TOOL, encoding="utf-8").read()


def test_the_summary_does_not_claim_not_hosted():
    """"catalogued but not hosted" asserts a 404 this tool never checked."""
    assert "catalogued but not hosted" not in SRC, (
        "the summary line claims the orphans are not hosted; measured, they are served "
        "(fed_board 60/60 present) — say what was compared instead"
    )


def test_the_summary_names_what_was_compared():
    assert "LOCAL STORE KEY" in SRC, (
        "the ORPHAN summary must name the artefact it compared against (the local parquet "
        "store), not imply the served surface"
    )


def test_the_summary_says_it_is_a_floor():
    """`gap` is a net, so the printed number under-counts and must say so."""
    assert re.search(r"FLOOR", SRC), "the ORPHAN line must declare itself a floor"
    assert "638" in SRC and "406" in SRC, (
        "keep fed_board's real split in the file — it is the worked example that shows the "
        "printed number matches neither half"
    )


def test_the_header_no_longer_calls_orphans_undownloadable():
    assert "listed and undownloadable" not in SRC, (
        "the module docstring still asserts orphans are undownloadable (R825 refuted it)"
    )


def test_the_tool_still_parses():
    import ast
    ast.parse(SRC)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
