"""FAOSTAT Emissions - Agriculture (crop residues) — DIRECT, served by GCE (#19).

15,018 series. Template Element.Item.Area reproduces 51.4% (7,712 ids) exactly —
those auto-update via restrict_to_published, filtered to Source='FAO TIER 1' (GCE
also ships UNFCCC-methodology rows that collide on 14,123 (key, year) points). The
7,306-id tail is exactly 4 element classes FAOSTAT DROPPED in the emissions-suite
restructure: per-crop CO2eq (72312/72352/72372) and implied emission factors
(72292) — no successor exists at any grain, frozen as historical (R180/R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_ga.json.
"""
from . import _faostat as _f

SOURCE = "fao_ga"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
