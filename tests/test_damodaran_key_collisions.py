"""A damodaran series_key must identify a row (R516's surviving defect).

`_parse_rows` builds `DAMODARAN:<dataset>:<label>:<entity>` from ONE header row, and that label
was not unique. Measured in the served store 2026-09-06: 721 of 24,687 keys hold two DIFFERENT
values at one obs_date, so a user gets two numbers for one date and which one they see depends
on row order. evmultiples 353 of 462 (76.4%), taxrate 179, divfcfe 72, ctryprem 87,
ctryprem_old 30; the other fifteen datasets are clean.

Two shapes produce it, and both are pinned below:

  * a TWO-LEVEL header — vebitda.xls 'Industry Averages' row 7 is a sparse group row
    ('Only positive EBITDA firms' … 'All firms') over a row 8 repeating EV/EBITDA once per
    group, so Advertising's EV/EBITDA is both 11.998 and 15.118;
  * the 25-character truncation — 'Net Cash Returned/FCFE (pre-debt)' and '(post-debt)' both
    become 'Net_Cash_Returned_FCFE__p'.

THE THIRD TEST IS THE IMPORTANT ONE. A keying change is a RE-GRAIN needing a clean re-pull
(R22/R333), so every id that moves has a cost, and a fix that silently re-keys the already-clean
datasets is worse than the defect it repairs. My first draft did exactly that — it raised the
label cap to 80 characters and terminated the table at the first blank header column, both
global changes, and measured over all 20 datasets it moved 6,656 ids across 13 datasets that had
NO conflict. I did not catch it because I compared conflict COUNTS (0 → 0 on the controls)
instead of key SETS, which is a metric that cannot see the harm (R820). So `test_a_sheet_with_no
_repeated_label_keeps_byte_identical_keys` asserts the exact key strings, and it is the test that
fails if this fix ever goes global again.
"""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

sys.path.insert(0, os.path.join(ROOT, "jobs"))
import importlib.util                                                 # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "_damo", os.path.join(ROOT, "jobs", "ingest_damodaran.py"))
damo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(damo)


def keys_of(rows, dataset="ds"):
    k, d, v = damo._parse_rows(rows, dataset, "Sheet1")
    return k, d, v


def pairs(k, d):
    return len(set(zip(k, d)))


# ── the two-level header (the evmultiples shape) ────────────────────────────────
TWO_LEVEL = [
    (None, None, "Only positive EBITDA firms", None, "All firms", None),
    ("Industry Name", "Number of firms", "EV/EBITDA", "EV/EBIT", "EV/EBITDA", "EV/EBIT"),
    ("Advertising", 52.0, 11.998645, 20.908715, 15.118160, 21.152055),
    ("Air Transport", 23.0, 7.575257, 19.318689, 9.993465, 21.019567),
]


def test_a_repeated_label_under_a_group_header_does_not_collide():
    k, d, v = keys_of(TWO_LEVEL)
    assert len(k) == pairs(k, d), (
        "two columns still share one (series_key, obs_date) — this is the defect: "
        f"{len(k)} rows into {pairs(k, d)} pairs"
    )


def test_both_group_values_survive_and_are_distinguishable():
    k, d, v = keys_of(TWO_LEVEL)
    adv = {kk: vv for kk, vv, dd in zip(k, v, d) if kk.endswith(":Advertising")}
    ebitda = {kk: vv for kk, vv in adv.items() if "EV_EBITDA" in kk}
    assert len(ebitda) == 2, f"expected both EV/EBITDA columns, got {sorted(ebitda)}"
    assert sorted(round(x, 6) for x in ebitda.values()) == [11.998645, 15.11816]
    # the qualifier must name the group, not merely be unique
    assert any("Only_positive" in kk for kk in ebitda), sorted(ebitda)
    assert any("All_firms" in kk for kk in ebitda), sorted(ebitda)


# ── the truncation collision (the divfcfe shape), with NO group row ─────────────
TRUNCATED = [
    ("Industry name", "Number of firms",
     "Net Cash Returned/FCFE (pre-debt)", "Net Cash Returned/FCFE (post-debt)"),
    ("Advertising", 52.0, 0.706848, -6.786517),
]


def test_two_labels_that_truncate_to_the_same_25_chars_do_not_collide():
    raw = ["Net Cash Returned/FCFE (pre-debt)", "Net Cash Returned/FCFE (post-debt)"]
    import re
    assert len({re.sub(r"[^a-zA-Z0-9_]", "_", h)[:25].strip("_") for h in raw}) == 1, (
        "fixture no longer exercises the truncation collision"
    )
    k, d, v = keys_of(TRUNCATED)
    assert len(k) == pairs(k, d), f"{len(k)} rows into {pairs(k, d)} pairs — still colliding"
    # three value columns: 'Number of firms' plus the two that truncate together
    assert len(set(k)) == 3, sorted(set(k))
    coll = sorted(kk for kk in k if "Net_Cash_Returned" in kk)
    assert len(coll) == 2 and coll[0] != coll[1], coll
    # with no group row above, the fallback qualifier is the column index
    assert all("__col" in kk for kk in coll), coll


# ── THE CONTROL: a clean sheet's ids must not move (R820) ───────────────────────
CLEAN = [
    ("Industry Name", "Number of firms", "Beta", "Cost of Equity"),
    ("Advertising", 52.0, 1.24, 0.0912),
    ("Air Transport", 23.0, 1.08, 0.0855),
]


def test_a_sheet_with_no_repeated_label_keeps_byte_identical_keys():
    """The fix must be invisible to every dataset that never had the defect.

    Asserting the exact strings, not a count: "0 conflicts before, 0 after" is satisfied by a
    no-op AND by a fix that re-keys the whole sheet, which is how 6,656 ids nearly moved.
    """
    k, d, v = keys_of(CLEAN, dataset="betas")
    assert sorted(set(k)) == [
        "DAMODARAN:betas:Beta:Advertising",
        "DAMODARAN:betas:Beta:Air_Transport",
        "DAMODARAN:betas:Cost_of_Equity:Advertising",
        "DAMODARAN:betas:Cost_of_Equity:Air_Transport",
        "DAMODARAN:betas:Number_of_firms:Advertising",
        "DAMODARAN:betas:Number_of_firms:Air_Transport",
    ], sorted(set(k))


def test_the_qualifier_fires_ONLY_on_the_colliding_label():
    """A sheet with one repeated label must leave its OTHER columns untouched."""
    rows = [
        (None, None, "Group A", "Group B", None),
        ("Industry Name", "Number of firms", "Ratio", "Ratio", "Margin"),
        ("Advertising", 52.0, 1.1, 2.2, 0.33),
    ]
    k, _d, _v = keys_of(rows, dataset="ds")
    assert "DAMODARAN:ds:Margin:Advertising" in k, "an unrelated column was re-keyed"
    assert "DAMODARAN:ds:Number_of_firms:Advertising" in k, "an unrelated column was re-keyed"
    assert not any(kk.startswith("DAMODARAN:ds:Ratio:") for kk in k), (
        "the colliding label was left ambiguous"
    )


def test_group_span_returns_empty_when_there_is_no_group_row():
    """No group row -> {} -> the caller falls back to the column index, never crashes."""
    assert damo._group_span(CLEAN, 0, 4) == {}


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))
