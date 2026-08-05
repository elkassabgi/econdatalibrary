"""IMF Effective Exchange Rate (EER) — DIRECT from api.imf.org (agency IMF.STA).
NEW COVERAGE: no legacy counterpart id — real/nominal effective exchange-rate indices,
found by the IFS-families keyword sweep alongside ER.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

current_vintage() returned EER:6.0.0 live 2026-08-05 (cycle 17 of the econ-updater loop).
Size and key shape are measured at the proof run; grain by the #45 arithmetic, run
explicitly per R356.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "EER", "IMF.STA", "imf_eer_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
