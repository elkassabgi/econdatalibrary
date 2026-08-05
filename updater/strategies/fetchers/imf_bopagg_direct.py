"""IMF Balance of Payments and IIP Statistics aggregates (BOP_AGG) — DIRECT from
api.imf.org (agency IMF.STA). Successor to the relay-era BOPAGG dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_bopagg` holds 7,801 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF RENAMED the dataset: the /dataflow catalogue carries no
BOPAGG flow; it carries BOP_AGG (IMF.STA, "BOP and IIP Statistics (BOP/IIP)") — the rename
was probe-confirmed by NAME search in the R-ledger's 10-dataset rename audit, and
current_vintage() returned BOP_AGG:9.0.1 live 2026-08-05 (cycle 6 of the econ-updater loop).
DISTINCT from the already-served BOP flow (detailed accounts); BOP_AGG carries the headline
aggregates.

Size and key shape are measured at the proof run, never assumed; serving grain is decided by
the #45 D1 arithmetic when the counts land.

Adding `imf_bopagg_direct` takes nothing from `imf_bopagg`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "BOP_AGG", "IMF.STA", "imf_bopagg_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
