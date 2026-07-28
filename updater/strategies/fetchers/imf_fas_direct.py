"""IMF Financial Access Survey — DIRECT from api.imf.org (flow FAS, agency IMF.STA).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset
needs its own module. All behaviour lives in _imf_direct.py — see that file for why
these are NEW source ids rather than replacements for the DBnomics-era imf_fas.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "FAS", "IMF.STA", "imf_fas_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
