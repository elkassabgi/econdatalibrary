"""FAOSTAT Emissions - Enteric Fermentation — DIRECT, served by GLE (#19).

11,813 series. Template Area.Item.Element reproduces 52.6% (6,209 ids) exactly —
those auto-update via restrict_to_published (upstream is a 9.5x superset), filtered
to Source='FAO TIER 1'. The 5,604-id tail is 2 element classes FAOSTAT DROPPED in
the emissions-suite restructure: per-animal CO2eq (72314) and implied emission
factors (72244) — no successor at any grain, frozen as historical (R180/R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_ge.json.
"""
from . import _faostat as _f

SOURCE = "fao_ge"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
