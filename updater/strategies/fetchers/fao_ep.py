"""FAOSTAT Pesticides Indicators — DIRECT, served by RP (#19).

519 series; CLEAN repair — 98.7% of ids reproduce (Item.Element.Area) and auto-update
via restrict_to_published.

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_ep.json.
"""
from . import _faostat as _f

SOURCE = "fao_ep"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
