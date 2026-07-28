"""IMF Fiscal Decentralization — DIRECT from api.imf.org (flow FD, agency IMF.STA).

Same story as imf_hpdd: 8,398 series served and downloadable with no updater entry,
because the flow was renamed (FISCALDECENTRALIZATION -> FD) and the exact-id miss
read as "discontinued".

Code map in _imf_maps/imf_fiscaldecentralization.json, derived by value agreement,
not typed. Proven: 8,398 of 8,398 ids preserved.
"""
from . import _imf_mapped as _m

SOURCE = "imf_fiscaldecentralization"


def current_vintage(unit):
    return _m.vintage(SOURCE)


def update(unit, us):
    return _m.run(SOURCE)
