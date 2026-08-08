"""FAOSTAT Emissions Totals — DIRECT, served by GT (#19).

10,506 series. 2,858 native-gas ids (27.2%: CH4 7225 / N2O 7230, no GWP
dependence) reproduce exactly and auto-update via restrict_to_published,
Source='FAO TIER 1' pinned. The planned AR5 re-key was REFUTED by value
verification: the R180 crosswalk maps keys 1:1 but the CO2eq GWP basis changed
(SAR/AR4 -> AR5), so old CO2eq series are a prior-GWP vintage — frozen rather
than mislabeled (only 7/7,650 candidates value-verified).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gt.json.
"""
from . import _faostat as _f

SOURCE = "fao_gt"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
