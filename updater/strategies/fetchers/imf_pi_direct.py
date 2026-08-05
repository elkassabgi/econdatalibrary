"""IMF Production Indexes (PI) — DIRECT from api.imf.org (agency IMF.STA).
NEW COVERAGE: the successor home of dismembered IFS's industrial-production family
(the AIP-type series), found by the IFS-families keyword sweep.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

current_vintage() returned PI:2.0.0 live 2026-08-05 (cycle 19 of the econ-updater loop).
Size and key shape are measured at the proof run; grain by the #45 arithmetic, run
explicitly per R356.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "PI", "IMF.STA", "imf_pi_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
