"""IMF Commodity Terms of Trade (CTOT) — DIRECT from api.imf.org (agency IMF.RES).
Successor to the relay-era PCTOT dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_pctot` holds 4,320 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF RENAMED the dataset (R74's rename): no PCTOT flow
exists; CTOT carries it at agency IMF.RES (the Research department — NOT IMF.STA), with an
identical series count in the R75 rename audit (4,320 = 4,320, the same-dataset proof).
current_vintage() returned CTOT:5.0.1 live 2026-08-05 (cycle 9 of the econ-updater loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_ctot_direct` takes nothing from `imf_pctot`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "CTOT", "IMF.RES", "imf_ctot_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
