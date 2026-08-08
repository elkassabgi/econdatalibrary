"""FAOSTAT Pesticides Use — DIRECT from its own still-live RP domain (#19).

5,440 series. Template Element.Area.Item reproduces 82.8% (4,504 ids) exactly —
those auto-update via restrict_to_published. The 936-id tail is DROPPED ITEMS:
FAOSTAT stopped publishing seed-treatment breakdowns and Mineral Oils; all four
elements survive, so this is an item-level restructure with no successors —
frozen as historical (R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_rp.json.
"""
from . import _faostat as _f

SOURCE = "fao_rp"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
