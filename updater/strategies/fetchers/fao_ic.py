"""FAOSTAT Credit to Agriculture — DIRECT from its own still-live IC domain (#19).

2,468 series. The first RE-KEY case of the approved restructure: FAOSTAT re-coded
local-currency elements to STANDARD local currency (6109->6224, 6183->6225); the
403 series whose values verify identical were re-keyed in place and now
auto-update alongside the 1,103 raw-reproducing ids (61.0% total). Frozen tail:
redenominated-country LC series (SLC diverges — splicing would mix units) and
dropped current-price elements (61132/6159).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_ic.json.
"""
from . import _faostat as _f

SOURCE = "fao_ic"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
