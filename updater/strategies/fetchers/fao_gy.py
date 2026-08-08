"""FAOSTAT Emissions - Synthetic Fertilizers — DIRECT, served by GCE (#19).

2,491 series; 1,485 (59.6%) reproduce (Area.Item.Element, Source='FAO TIER 1' pinned)
and auto-update; dropped CO2eq/implied-EF tail frozen (R403 class).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gy.json.
"""
from . import _faostat as _f

SOURCE = "fao_gy"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
