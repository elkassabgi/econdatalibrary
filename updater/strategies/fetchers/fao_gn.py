"""FAOSTAT Emissions - Energy Use in Agriculture — DIRECT from its own GN domain (#19).

4,761 series. Template Element.Area.Item reproduces 83.5% (3,976 ids) exactly —
those auto-update via restrict_to_published. The 785-id tail is AREA-level:
47 dropped/dissolved area codes plus discontinued combos, with every element and
item surviving — frozen as historical (R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gn.json.
"""
from . import _faostat as _f

SOURCE = "fao_gn"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
