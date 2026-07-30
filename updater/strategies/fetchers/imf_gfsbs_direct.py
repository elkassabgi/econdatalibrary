"""IMF Government Finance Statistics — Balance Sheet — DIRECT from api.imf.org (flow GFS_BS, agency IMF.STA).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset needs its own
module. All behaviour lives in _imf_direct.py — see that file for why these are NEW source ids
rather than replacements for the DBnomics-era ones.

MAPPING, established by evidence rather than by name (ledger, 2026-07-29). Our six legacy
imf_gfs* sources (213,200 series) had no 1:1 name match against the six flows IMF now publishes,
because the pipeline had SPLIT one flow into several source ids by indicator group. Extracting
every distinct INDICATOR from each flow settled it:
    GFS_SOO   -> imf_gfse  (its G26* codes)  AND  imf_gfsmab (its G11*/G12* codes)
    GFS_COFOG -> imf_gfscofog     GFS_SSUC -> imf_gfsssuc
    GFS_BS    -> imf_gfsibs       GFS_SFCP -> imf_gfsfalcs
    GFS_SOEF  -> nothing we carry (12,720 series of Other Economic Flows: NEW coverage)
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "GFS_BS", "IMF.STA", "imf_gfsbs_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
