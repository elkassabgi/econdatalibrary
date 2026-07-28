"""IMF Western Hemisphere Regional Economic Outlook — DIRECT from api.imf.org (flow WHDREO, agency IMF.WHD).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset
needs its own module. All behaviour lives in _imf_direct.py — see that file for why
these are NEW source ids rather than replacements for the DBnomics-era imf_whdreo.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "WHDREO", "IMF.WHD", "imf_whdreo_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
