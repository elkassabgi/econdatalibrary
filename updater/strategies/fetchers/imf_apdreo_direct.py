"""IMF Asia and Pacific Regional Economic Outlook — DIRECT from api.imf.org (flow APDREO, agency IMF.APD).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset
needs its own module. All behaviour lives in _imf_direct.py — see that file for why
these are NEW source ids rather than replacements for the DBnomics-era imf_apdreo.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "APDREO", "IMF.APD", "imf_apdreo_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
