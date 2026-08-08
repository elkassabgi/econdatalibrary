"""FAOSTAT Livestock Primary (now inside QCL) — DIRECT, served by the QCL bulk dataset.

The largest of the 18 frozen fao_* sources (#19, Ahmed-approved 2026-08-07): 20,179
series. Template Item.Area.Element reproduces 84.3% (16,997 ids) exactly — those
auto-update in place via restrict_to_published. The 3,182-id remainder is FAOSTAT's
QCL-merge restructure (old per-product Yield elements + (number)-variant egg items,
re-coded with NEW units); their successors are already served live under fao_qcl,
so re-keying would mint duplicates (R91). They stay frozen-served as historical.
The value-verified successor map (580 mappings, floor 0.90, uniqueness margin) is
recorded in _faostat_maps/fao_ql.crosswalk.json for documentation.

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_ql.json.
"""
from . import _faostat as _f

SOURCE = "fao_ql"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
