"""IMF Historical Public Debt (HPD) — DIRECT from api.imf.org (agency IMF.FAD).
Successor to the relay-era HPDD (Historical Public Debt Database) dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_hpdd` holds 191 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF RENAMED the dataset: HPD (IMF.FAD) carries it, matched
in the R-ledger rename audit at an IDENTICAL series count (191 = 191, the R75 same-dataset
proof). current_vintage() returned HPD:1.0.0 live 2026-08-05 (cycle 14 of the econ-updater
loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_hpd_direct` takes nothing from `imf_hpdd`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "HPD", "IMF.FAD", "imf_hpd_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
