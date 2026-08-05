"""IMF Gender Statistics — labor and income (GS_LI) — DIRECT from api.imf.org (agency
IMF.STA). One of the five GS flows the relay-era GENDER_* datasets were split into.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_gender_equality` (295 series) and `imf_gender_budgeting`
(288 series) are relay-era stores — catalogued, SERVED, frozen. IMF restructured the
gender statistics into the GS_* family (GS_LGRGHTS, GS_LEPM, GS_SDO, GS_ATF, GS_LI —
all IMF.STA), probe-confirmed in the R-ledger rename audit and by the IFS-families sweep.
current_vintage() returned GS_LI:1.0.0 live 2026-08-05 (cycle 15 of the econ-updater
loop). One source id per flow, the FSI-trio/MFS precedent.

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_gsli_direct` takes nothing from the legacy gender stores; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "GS_LI", "IMF.STA", "imf_gsli_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
