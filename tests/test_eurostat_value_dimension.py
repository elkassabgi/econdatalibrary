"""A eurostat flow may carry a real DIMENSION named `value`, and both rules broke on it.

MEASURED 2026-09-01 on sbs_pen_7b1's live SDMX-CSV header:

    DATAFLOW, LAST UPDATE, freq, value, nace_r1, geo, TIME_PERIOD, OBS_VALUE, OBS_FLAG, ...

Two separate failures, both silent:
  * obs_col was `next(c for c in fields if _norm(c) in ("OBS_VALUE","VALUE"))`, and the
    DIMENSION sits first in DSD order — so the observation was read from a dimension column,
    float() rejected every code, and the flow parsed to ZERO rows.
  * dim_cols was a _NON_KEY blacklist, which deleted that dimension from the key, collapsing
    ~5.8 source rows onto one public id (ledger R544).

THESE TESTS DRIVE THE SHIPPED `_parse_csv`. The first version of this file re-typed the two
rules and asserted against its own copies — the adversarial review proved it could not fail by
replacing `_parse_csv` with a stub that returns (None, None, None) and watching all three tests
pass. That is exactly R544's lesson committed a second time, in the test written to pin R544.
"""
import glob
import gzip
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import eurostat  # noqa: E402
from updater.strategies.fetchers.eurostat import _STRUCTURAL, _norm  # noqa: E402

RAW = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "data", "raw", "eurostat")


def _csv(dims, rows):
    """An SDMX-CSV body in eurostat's real column layout."""
    head = ["DATAFLOW", "LAST UPDATE"] + list(dims) + ["TIME_PERIOD", "OBS_VALUE",
                                                      "OBS_FLAG", "CONF_STATUS"]
    out = [",".join(head)]
    for dim_vals, period, obs in rows:
        out.append(",".join(["ESTAT:X(1.0)", "01/01/26 00:00:00"] + list(dim_vals)
                            + [period, obs, "", ""]))
    return ("\n".join(out) + "\n").encode("utf-8")


def test_a_value_dimension_no_longer_eats_the_observation_column():
    """THE regression, driven through the shipped parser. Before the fix this returned 0 rows
    because float('ME2501-5000') fails; the dimension had been taken as the observation."""
    body = _csv(["freq", "value", "nace_r1", "geo"],
                [(["A", "ME2501-5000", "J6602", "AT"], "2020", "12.5"),
                 (["A", "ME5001-MAX", "J6602", "AT"], "2020", "34.0")])
    keys, dates, vals = eurostat._parse_csv(body)
    assert keys is not None, "_parse_csv returned nothing — the parser rejected a valid body"
    assert len(keys) == 2, f"expected 2 rows, got {len(keys)}"
    assert vals == [12.5, 34.0], f"observations came from the wrong column: {vals}"
    # and the two size bands must remain DISTINCT series, which is what the old key collapsed
    assert len(set(keys)) == 2, f"the two size bands collapsed onto one id: {keys}"
    assert "value=ME2501-5000" in keys[0]


def test_an_ordinary_flow_is_completely_unchanged():
    body = _csv(["freq", "unit", "geo"],
                [(["A", "NR", "AT"], "2020", "7.0"),
                 (["A", "NR", "BE"], "2020", "8.0")])
    keys, dates, vals = eurostat._parse_csv(body)
    assert keys == ["freq=A:unit=NR:geo=AT", "freq=A:unit=NR:geo=BE"]
    assert vals == [7.0, 8.0]


def test_rows_are_unique_on_key_and_date():
    """The property the seeder's guard checks and the fetcher never did (review SHOULD-FIX 5).
    A collapsing key shows up here as a duplicate (key, date)."""
    body = _csv(["freq", "value", "geo"],
                [(["A", "S1", "AT"], "2020", "1.0"),
                 (["A", "S2", "AT"], "2020", "2.0")])
    keys, dates, vals = eurostat._parse_csv(body)
    pairs = list(zip(keys, dates))
    assert len(pairs) == len(set(pairs)), f"duplicate (key, date) after parsing: {pairs}"


def test_the_layout_assumption_holds_or_we_would_key_on_an_attribute():
    """SHOULD-FIX 4: the positional rule takes every column before TIME_PERIOD, so an
    attribute appearing THERE would enter the key. Not seen in any live header, but assert the
    consequence explicitly so the hazard is visible rather than implied."""
    body = _csv(["freq", "geo"], [(["A", "AT"], "2020", "5.0")])
    keys, _d, _v = eurostat._parse_csv(body)
    assert keys == ["freq=A:geo=AT"]
    assert "OBS_FLAG" not in keys[0] and "CONF_STATUS" not in keys[0]


def test_the_change_moves_no_key_in_any_real_flow_except_the_known_seven():
    """The safety property, over the REAL dimension list of every raw flow.

    Driven through the shipped parser: for each flow, build a one-row SDMX-CSV body from its
    true dimension list and compare the key the parser produces against the key the OLD
    blacklist rule would have produced. Only the seven known collisions may differ.
    """
    files = sorted(glob.glob(os.path.join(RAW, "*.tsv.gz")))
    if len(files) < 100:
        pytest.skip("raw eurostat mirror not present")
    from updater.strategies.fetchers.eurostat import _NON_KEY
    changed, checked = [], 0
    for p in files:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                head = f.readline()
        except Exception:
            continue
        dims = head.rstrip("\n").split("\t")[0].split("\\")[0].split(",")
        if not dims or not dims[0]:
            continue
        checked += 1
        vals = [f"v{n}" for n in range(len(dims))]
        keys, _d, _v = eurostat._parse_csv(_csv(dims, [(vals, "2020", "1.0")]))
        assert keys, f"{os.path.basename(p)}: parser returned nothing"
        fields = ["DATAFLOW", "LAST UPDATE"] + dims + ["TIME_PERIOD", "OBS_VALUE",
                                                       "OBS_FLAG", "CONF_STATUS"]
        row = dict(zip(fields, ["ESTAT:X(1.0)", "01/01/26"] + vals
                       + ["2020", "1.0", "", ""]))
        old_key = ":".join(f"{c}={row[c]}" for c in fields
                           if _norm(c) not in _NON_KEY and row.get(c))
        if keys[0] != old_key:
            changed.append(os.path.basename(p)[: -len(".tsv.gz")])
    assert checked > 7000, f"only {checked} flows read; the mirror looks incomplete"
    assert sorted(changed) == sorted([
        "sbs_cre_esc", "sbs_ins_5d1", "sbs_ins_5d2", "sbs_part_wtsct",
        "sbs_pen_7b1", "sbs_sc_3ctrn_tr", "sbs_sctrn_dt_r2",
    ]), f"the change is not confined to the known collisions: {changed}"


def test_structural_names_are_never_dimensions_in_any_real_flow():
    """The positional rule removes only _STRUCTURAL. If a real flow ever named a dimension
    one of those, the seeder and fetcher would silently drop it."""
    files = sorted(glob.glob(os.path.join(RAW, "*.tsv.gz")))
    if len(files) < 100:
        pytest.skip("raw eurostat mirror not present")
    offenders = []
    for p in files:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                head = f.readline()
        except Exception:
            continue
        dims = head.rstrip("\n").split("\t")[0].split("\\")[0].split(",")
        if any(_norm(c) in _STRUCTURAL for c in dims):
            offenders.append(os.path.basename(p))
    assert offenders == [], f"flows whose DIMENSION collides with _STRUCTURAL: {offenders}"


def test_an_attribute_before_the_time_column_is_REFUSED_not_keyed_on():
    """Review SHOULD-FIX 4, driven. The positional cut takes every column before TIME_PERIOD,
    so an attribute there would enter the key — and OBS_FLAG changes between releases, which
    is the `LAST UPDATE` duplication class this module exists to prevent. It must refuse."""
    head = ["DATAFLOW", "LAST UPDATE", "freq", "geo", "OBS_FLAG",
            "TIME_PERIOD", "OBS_VALUE", "CONF_STATUS"]
    body = (",".join(head) + "\n"
            + "ESTAT:X(1.0),01/01/26,A,AT,e,2020,5.0,\n").encode("utf-8")
    keys, dates, vals = eurostat._parse_csv(body)
    assert keys is None, (
        f"an attribute before the time column was keyed on instead of refused: {keys}")
    # and fetch_flow must be able to unpack it — the 2-tuple form would ValueError there
    assert (dates, vals) == (None, None)


def test_a_collapsing_key_is_REFUSED_by_the_property_guard():
    """Review SHOULD-FIX 5, driven. `rows == distinct(series_key, obs_date)` is what caught
    R544; it lived only in the seeder. Simulate a key that loses a dimension by giving two
    rows that differ ONLY in a column the parser cannot see as a dimension."""
    # `period` is a _NON_KEY name that is NOT VALUE, so the layout assertion catches it first;
    # to exercise the PROPERTY guard specifically, use two identical dimension tuples.
    body = _csv(["freq", "geo"],
                [(["A", "AT"], "2020", "1.0"),
                 (["A", "AT"], "2020", "2.0")])
    keys, dates, vals = eurostat._parse_csv(body)
    assert keys is None, (
        f"two rows collapsing to one (key, date) were published instead of refused: {keys}")
    assert (dates, vals) == (None, None)
