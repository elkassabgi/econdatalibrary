"""IMF National Accounts Main Aggregates (NA_MAIN) — DIRECT from api.imf.org
(agency IMF.STA). Successor to the relay-era NAMAIN_IDC_N dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_namain_idc_n` holds 1,926 relay-era series — catalogued, SERVED,
frozen: no fetcher, no registry entry. IMF RENAMED the dataset: the /dataflow catalogue
carries no NAMAIN flow; it carries NA_MAIN (IMF.STA, "National Accounts Main Aggregates
(SDMX)"), probe-confirmed 2026-08-05, and current_vintage() returned NA_MAIN:1.0.0 live the
same day (cycle 11 of the econ-updater loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_namain_direct` takes nothing from `imf_namain_idc_n`; supersession is #46,
reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "NA_MAIN", "IMF.STA", "imf_namain_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
