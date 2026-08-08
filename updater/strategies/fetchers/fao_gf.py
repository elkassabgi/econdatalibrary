"""FAOSTAT Emissions - Forest Land — DIRECT from its own GF domain (#19).

2,591 series. 819 ids (31.6%) reproduce and auto-update (796 raw + 23
value-verified re-keys), filtered to Source='FAO TIER 1'. The frozen tail is a
methodology break, not a key break: FAOSTAT recalculated forest-carbon
accounting, so most old series' values no longer match any current key — they
stay frozen as a prior-methodology vintage rather than splice two accountings
into one series (R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gf.json.
"""
from . import _faostat as _f

SOURCE = "fao_gf"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
