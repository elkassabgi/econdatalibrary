"""IMF Reported SDG Data (SDG) — DIRECT from api.imf.org (agency IMF.STA).
Successor to the relay-era UNSDG "IMF inputs" dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_unsdg_imf_inputs` holds 2,515 relay-era series — catalogued,
SERVED, frozen: no fetcher, no registry entry. IMF RENAMED the dataset: the /dataflow
catalogue carries no UNSDG flow; it carries SDG (IMF.STA, "IMF Reported SDG Data"),
probe-confirmed 2026-08-05, and current_vintage() returned SDG:2.0.1 live the same day
(cycle 10 of the econ-updater loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_sdg_direct` takes nothing from `imf_unsdg_imf_inputs`; supersession is #46,
reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "SDG", "IMF.STA", "imf_sdg_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
