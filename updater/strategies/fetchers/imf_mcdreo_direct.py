"""IMF Middle East & Central Asia Regional Economic Outlook — DIRECT from api.imf.org (flow MCDREO, agency IMF.MCD).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset
needs its own module. All behaviour lives in _imf_direct.py — see that file for why
these are NEW source ids rather than replacements for the DBnomics-era imf_mcdreo.

The direct feed carries ~623 series vs the frozen relay's 1,095 (57%) — Ahmed's
2026-08-06 ruling ("refresh to match publisher... I need a clean database") makes
the publisher's current scope the target, so this is deliberate, not a regression.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "MCDREO", "IMF.MCD", "imf_mcdreo_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
