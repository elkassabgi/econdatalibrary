"""FAOSTAT Emissions - Rice Cultivation — DIRECT, served by GCE (#19).

617 series; 323 (52.4%) reproduce (Area.Item.Element, Source='FAO TIER 1' pinned)
and auto-update; dropped implied-EF/CO2eq tail frozen (R403 class).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gr.json.
"""
from . import _faostat as _f

SOURCE = "fao_gr"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
