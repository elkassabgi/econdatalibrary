"""IMF Consumer Price Index — DIRECT from api.imf.org (flow CPI, agency IMF.STA).

Thin wrapper; all behaviour lives in _imf_direct.py, including why these are NEW source ids
rather than replacements for the DBnomics-era ones.

`imf_cpi` holds 28,420 relay-era series with no fetcher, so it has never auto-updated. CPI is an
EXACT dataflow id on api.imf.org (agency IMF.STA), read from IMF's own /dataflow catalogue rather
than guessed.

NOTE ON THE NAME COLLISION. IMF's live catalogue also carries `CPIS` — Coordinated Portfolio
Investment Survey — which is a DIFFERENT dataset and our separate `imf_cpis` (100,783 series).
`CPI` here is the Consumer Price Index and nothing else; the two must not be conflated, which is
why the flow is pinned as a literal rather than derived from the source id.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "CPI", "IMF.STA", "imf_cpi_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
