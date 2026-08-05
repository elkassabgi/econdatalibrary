"""IMF Fiscal Decentralization (FD) — DIRECT from api.imf.org (agency IMF.STA).
Successor to the relay-era FISCALDECENTRALIZATION dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_fiscaldecentralization` holds 8,398 relay-era series —
catalogued, SERVED, frozen: no fetcher, no registry entry. IMF RENAMED the dataset: FD
(IMF.STA) carries it, matched in the R-ledger rename audit at an IDENTICAL series count
(8,398 = 8,398, the R75 same-dataset proof). current_vintage() returned FD:6.0.0 live
2026-08-05 (cycle 13 of the econ-updater loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_fd_direct` takes nothing from `imf_fiscaldecentralization`; supersession is
#46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "FD", "IMF.STA", "imf_fd_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
