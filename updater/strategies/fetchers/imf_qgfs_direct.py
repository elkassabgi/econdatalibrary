"""IMF Quarterly Government Finance Statistics (QGFS) — DIRECT from api.imf.org
(agency IMF.STA). NEW COVERAGE: the quarterly GFS companion, discovered in the gfsr
coverage comparison (its INDICATOR codelist carries the revenue G-codes). The dated
QGFS_*_VINTAGE snapshot flows are deliberately ignored.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

current_vintage() returned QGFS:12.0.0 live 2026-08-05 (cycle 21 of the econ-updater
loop). Size and key shape are measured at the proof run; grain by the #45 arithmetic,
run explicitly per R356.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "QGFS", "IMF.STA", "imf_qgfs_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
