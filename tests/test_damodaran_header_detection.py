"""A group-header row with an empty first column must not be mistaken for the real header.

Damodaran's margin.xls 'Industry Averages' sheet has TWO header-ish rows: row 7 carries the
column-group labels ("Gross Income Based", "Net Income Based", "EBITDA Based") with an EMPTY
column 0, and row 8 is the real header starting "Industry Name".

The shipped rule's comment said "col 0 must be a non-empty string" and its code tested
`non_null[0]` — the first non-null cell ANYWHERE in the row, which for row 7 is "Gross Income
Based". So row 7 passed, `entity_ci` resolved to the first column carrying a label (a VALUE
column), and all 384 margins series were keyed by a gross-margin number:

    DAMODARAN:margins:Net_Income_Based:0_36242944995377313

0.36242944995377313 is row 9's Gross Margin. The industry name was lost outright, so those
series were unidentifiable rather than merely untitled — no title composer could have repaired
them, because the identity was gone from the key.

Fixed, the same sheet yields 1,728 series keyed DAMODARAN:margins:<metric>:<industry>. Measured
across every Damodaran dataset, exactly one sheet's header row moved; 37 others were unaffected.
"""
from __future__ import annotations

import importlib.util
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JOB = os.path.join(ROOT, "jobs", "ingest_damodaran.py")

NUMERIC = re.compile(r"^[0-9][0-9_.eE+-]*$")


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("ingest_damodaran_undertest", JOB)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _margin_like_rows():
    """The shape of margin.xls 'Industry Averages', trimmed to what the parser reads."""
    return [
        ("Date updated:", 46027.0, None, None, None),
        ("Created by:", "Aswath Damodaran", None, None, None),
        ("What is this data?", "Profit margins", None, None, "US companies"),
        ("Home Page:", "http://www.damodaran.com", None, None, None),
        (None, None, "Gross Income Based", "Net Income Based", "EBITDA Based"),
        ("Industry Name", "Number of firms", "Gross Margin", "Net Margin", "EBITDA/Sales"),
        ("Advertising", 52.0, 0.3624294499, -0.0030178781, 0.0930538640),
        ("Aerospace/Defense", 79.0, 0.1748461763, 0.0498912133, 0.1040914638),
    ]


def test_the_group_header_row_is_not_chosen(m):
    rows = _margin_like_rows()
    keys, _dates, _vals = m._parse_rows(rows, "margins", "Industry Averages")
    assert keys, "the parser returned nothing"
    entities = {k.split(":")[-1] for k in keys}
    assert "Advertising" in entities
    assert not any(NUMERIC.match(e) for e in entities), (
        f"a value is being used as the series identity: {sorted(entities)[:4]}")


def test_the_metric_name_is_the_column_label_not_the_group_label(m):
    rows = _margin_like_rows()
    keys, _d, _v = m._parse_rows(rows, "margins", "Industry Averages")
    labels = {k.split(":")[2] for k in keys}
    assert "Gross_Margin" in labels or "Gross Margin" in labels, sorted(labels)
    assert "Gross_Income_Based" not in labels, (
        "the group-header label leaked into the key — the wrong header row was used")


def test_every_industry_appears(m):
    rows = _margin_like_rows()
    keys, _d, _v = m._parse_rows(rows, "margins", "Industry Averages")
    entities = {k.split(":")[-1] for k in keys}
    assert "Advertising" in entities and "Aerospace_Defense" in entities, sorted(entities)


def test_a_normal_single_header_sheet_is_unchanged(m):
    """The fix must not move the header on the 37 sheets that were already right."""
    rows = [
        ("Date updated:", 46027.0, None),
        ("Industry Name", "Beta", "D/E Ratio"),
        ("Advertising", 1.2, 0.4),
        ("Apparel", 0.9, 0.3),
    ]
    keys, _d, _v = m._parse_rows(rows, "betas", "Industry Averages")
    entities = {k.split(":")[-1] for k in keys}
    assert entities == {"Advertising", "Apparel"}, sorted(entities)


def test_the_rule_tests_column_zero_not_the_first_non_null_cell(m):
    """Pin the actual defect, so a future edit cannot quietly reintroduce it."""
    src = open(JOB, encoding="utf-8").read()
    i = src.index("# COLUMN 0 must be a non-empty string")
    window = src[i:i + 1400]
    assert "c0 = row[0] if row else None" in window
    assert "isinstance(c0, str)" in window
    assert "isinstance(non_null[0], str)" not in window, (
        "the first-non-null test is back — that is the exact bug")
