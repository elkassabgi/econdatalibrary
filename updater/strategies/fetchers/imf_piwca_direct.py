"""IMF Production Indexes, World and Country Group Aggregates (PI_WCA) — DIRECT from
api.imf.org (agency IMF.STA). Sibling of imf_pi_direct: the aggregate companion.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

current_vintage() returned PI_WCA:1.0.0 live 2026-08-05 (cycle 20 of the econ-updater
loop). Size and key shape are measured at the proof run; grain by the #45 arithmetic,
run explicitly per R356.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "PI_WCA", "IMF.STA", "imf_piwca_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
