"""IMF Financial Soundness Indicators — Core and Additional Indicators — DIRECT from api.imf.org (flow FSIC, agency IMF.STA).

Thin wrapper: the registry resolves fetchers/<source_id>.py, so each IMF dataset needs its own
module. All behaviour lives in _imf_direct.py — see that file for why these are NEW source ids
rather than replacements for the DBnomics-era ones.

WHY THREE MODULES FOR "FSI". Our legacy `imf_fsi` (73,288 series) keys on flow `FSI`, which IMF
no longer publishes as a single dataflow: on api.imf.org the FSI family is FSIC (core and
additional indicators), FSIBSIS (balance sheet, income statement) and FSICDM (concentration and
distribution measures), plus two metadata tables that carry no series. That split is why
`imf_fsi` had no fetcher at all and sat frozen — there was no `FSI` flow left to fetch.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "FSIC", "IMF.STA", "imf_fsic_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
