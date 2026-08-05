"""IMF Public Sector Balance Sheet (PSBS) — DIRECT from api.imf.org (agency IMF.FAD).
Successor to the relay-era PSBSFAD dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_psbsfad` holds 14,018 relay-era series — catalogued, SERVED,
frozen: no fetcher, no registry entry. IMF RENAMED the dataset: no PSBSFAD flow exists;
PSBS (IMF.FAD) carries it, probe-confirmed in the R-ledger rename audit with an IDENTICAL
series count (14,018 = 14,018, the same-dataset proof of R75). current_vintage() returned
PSBS:2.0.0 live 2026-08-05 (cycle 7 of the econ-updater loop). NOTE the agency: IMF.FAD,
not IMF.STA.

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_psbs_direct` takes nothing from `imf_psbsfad`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "PSBS", "IMF.FAD", "imf_psbs_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
