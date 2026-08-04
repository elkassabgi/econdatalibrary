"""IMF International Reserves and Foreign Currency Liquidity — DIRECT from api.imf.org
(flow IRFCL, agency IMF.STA).

Thin wrapper; all behaviour lives in _imf_direct.py, including why these are NEW source ids
rather than replacements for the DBnomics-era ones.

`imf_irfcl` holds 54,126 relay-era series with no fetcher, so it has never auto-updated. IRFCL is
an EXACT dataflow id on api.imf.org (agency IMF.STA), read from IMF's own /dataflow catalogue
rather than guessed — agency ids are not uniform across IMF datasets and assuming IMF.STA has
produced spurious 404s before.

Excluded from this batch for the same reason recorded in imf_bop_direct: flows where IMF's direct
copy is SMALLER than our relay copy (MCDREO 57%, FM 9%) are a reserved decision, not a build.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "IRFCL", "IMF.STA", "imf_irfcl_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
