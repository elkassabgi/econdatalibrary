"""ember per-dataset route() overrides — the under-keyed quartet (run 32816867502).

Three files sealed duplicate (series_key, obs_date) pairs into their parquets because
their branch-assigned keying carried no identity for them, so every later snapshot
merge collapsed below the 97% never-shrink floor and transient-failed forever:

  methane_chart_satellite_emissions  event-grain plumes, key had no LATITUDE/LONGITUDE
  necp_Ember_NECP_data_2024          E1's keys[:6] cap dropped COUNTRY_NAME + WEM_WAM
  tur_data_tool_srmc_chart           F2 keyed nothing — every key was the string "VALUE"

The fixes are per DATASET id, never per branch (widening a branch's keys re-grains
every other dataset it routes — R333). Discriminating pairs pinned here (R414): each
override produces fully-distinct keys on its measured shape, and a NON-override frame
still routes through its old branch with its old keying.

Measured on the real 2026-08-26 CSVs: methane 9,325/9,325 distinct, necp 4,813/4,813,
srmc 195/195, unlicensed (no override, replace-only) 498/498.
"""
from __future__ import annotations

import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from jobs import ingest_ember as ig  # noqa: E402


def _pairs(rows):
    return [(r["series_key"], r["obs_date"]) for r in rows]


def test_methane_override_keys_on_plume_identity():
    # two detections, same country/sensor/platform/region/day, different coordinates —
    # the OLD keying (no lat/lon) collapsed these to one key
    df = pd.DataFrame({
        "CODE": ["AUS", "AUS"], "SENSOR": ["Tanager", "Tanager"],
        "DATE": ["13/07/2025", "13/07/2025"],
        "EMISSION_RATE__KGH": [1182.6, 900.0], "UNCERTAINTY__KGH": [120.5, 80.0],
        "LATITUDE": [-21.659, -22.001], "LONGITUDE": [147.968, 148.100],
        "PLATFORM": ["Carbon Mapper", "Carbon Mapper"],
        "REGIONNAME": ["Queensland", "Queensland"],
    })
    label, rows = ig.route("methane_chart_satellite_emissions", df)
    assert label == "satellite_events"
    p = _pairs(rows)
    assert len(p) == 4 and len(set(p)) == 4, \
        "each (plume, measure) must be its own key — collapse is the 2026-08-25 defect"
    assert any("-21.659" in k for k, _ in p) and any("-22.001" in k for k, _ in p)
    assert str(rows[0]["obs_date"]) == "2025-07-13"      # file is day-first (measured)


def test_necp_override_keeps_country_and_scenario():
    # same category/kpi/unit/sector/fuel, two countries x two scenarios — the OLD E1
    # keys[:6] cap dropped both discriminators and collapsed all four
    base = dict(CATEGORY="Supply", KPI="Electricity generation", UNIT="TWh",
                SECTOR="Electricity generation", FUEL_GROUP="TOTAL", FUEL_CODE="TOTAL",
                FUEL_LOWER="TOTAL", YEAR=2030)
    df = pd.DataFrame([
        {**base, "VALUE": 70.0, "COUNTRY_NAME": "Austria", "SHORT_COUNTRY_CODE": "AT",
         "WEM_WAM": "WAM"},
        {**base, "VALUE": 71.0, "COUNTRY_NAME": "Austria", "SHORT_COUNTRY_CODE": "AT",
         "WEM_WAM": "WEM"},
        {**base, "VALUE": 55.0, "COUNTRY_NAME": "Belgium", "SHORT_COUNTRY_CODE": "BE",
         "WEM_WAM": "WAM"},
        {**base, "VALUE": None, "COUNTRY_NAME": "Belgium", "SHORT_COUNTRY_CODE": "BE",
         "WEM_WAM": "WEM"},
    ])
    label, rows = ig.route("necp_Ember_NECP_data_2024", df)
    assert label == "year_value"
    p = _pairs(rows)
    assert len(p) == 3 and len(set(p)) == 3          # null VALUE dropped, no collisions
    assert any("Austria" in k and "WAM" in k for k, _ in p)
    assert any("Belgium" in k for k, _ in p)


def test_srmc_override_keys_on_measure():
    df = pd.DataFrame({
        "DATETIME": ["01/21", "01/21", "01/21"],
        "MEASURE_ENG": ["Natural Gas", "Imported Coal", "Lignite"],
        "MEASURE_TUR": ["Doğalgaz", "İthal Kömür", "Linyit"],
        "VALUE": [37.19, 30.0, 25.0],
    })
    label, rows = ig.route("turkiye_data_tool_tur_data_tool_srmc_chart", df)
    assert label == "tidy_measures"
    p = _pairs(rows)
    assert len(p) == 3 and len(set(p)) == 3
    keys = {k for k, _ in p}
    assert keys == {"Natural Gas", "Imported Coal", "Lignite"}, \
        "the 2026-08-25 defect: every key was the literal string 'VALUE'"


def test_necp_missing_pinned_column_raises():
    # schema drift must be a LOUD refusal, never a silent re-grain (R333): the pinned
    # key list raises when upstream drops a column, which the orchestrator books as a
    # visible permanent transient until a human re-grains deliberately.
    import pytest
    df = pd.DataFrame({"CATEGORY": ["Supply"], "KPI": ["x"], "UNIT": ["TWh"],
                       "SECTOR": ["y"], "FUEL_GROUP": ["TOTAL"], "FUEL_CODE": ["TOTAL"],
                       "FUEL_LOWER": ["TOTAL"], "COUNTRY_NAME": ["Austria"],
                       # SHORT_COUNTRY_CODE and WEM_WAM absent
                       "YEAR": [2030], "VALUE": [70.0]})
    with pytest.raises(ValueError, match="pinned key column"):
        ig.route("necp_Ember_NECP_data_2024", df)


def test_non_override_frames_keep_their_old_branch():
    # negative control (R414): a methane-SHAPED frame under a DIFFERENT dataset id must
    # still route through the old branches with the old keying — the overrides are
    # dataset-scoped, not shape-scoped.
    df = pd.DataFrame({
        "CODE": ["AUS"], "SENSOR": ["Tanager"], "DATE": ["13/07/2025"],
        "EMISSION_RATE__KGH": [1182.6], "UNCERTAINTY__KGH": [120.5],
        "LATITUDE": [-21.659], "LONGITUDE": [147.968],
        "PLATFORM": ["Carbon Mapper"], "REGIONNAME": ["Queensland"],
    })
    label, rows = ig.route("some_other_chart", df)
    assert label != "satellite_events"
    if rows:                       # old keying: coordinates NOT in any key
        assert not any("-21.659" in r["series_key"] for r in rows)

    # and the unlicensed_capacity file keeps F2 wide_melt untouched (replace-only fix)
    df2 = pd.DataFrame({
        "DATETIME": ["01/16", "02/16"],
        "Installed capacity (GW)": [0.35, 0.40],
        "Monthly increase (MW)": [61.4, 50.0],
    })
    label2, rows2 = ig.route("turkiye_data_tool_tur_data_tool_unlicensed_capacity_chart", df2)
    assert label2 == "wide_melt" and len(rows2) == 4
