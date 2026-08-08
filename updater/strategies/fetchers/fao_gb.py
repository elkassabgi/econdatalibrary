"""FAOSTAT Emissions - Burning Crop Residues — DIRECT, served by GCE (#19).

6,980 series with FOUR-part ids (Area.Source.Item.Element — the methodology
Source Code is inside the key, so both FAO TIER 1 and UNFCCC series are distinct
and no row filter applies). 2,899 ids (41.5%) reproduce exactly and auto-update
via restrict_to_published. The 4,081-id tail is 5 element classes FAOSTAT
DROPPED: per-crop CO2eq (72317/72437/72447) and implied emission factors
(72247/72297) — no successor, frozen as historical (R180/R91).

Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_gb.json.
"""
from . import _faostat as _f

SOURCE = "fao_gb"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
