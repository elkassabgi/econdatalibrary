"""IMF Investment and Capital Stock Dataset (ICSD) — DIRECT from api.imf.org
(agency IMF.FAD). Successor to the relay-era PGCS (private/government capital stock) dataset.

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. `imf_pgcs` holds 2,262 relay-era series — catalogued, SERVED, frozen:
no fetcher, no registry entry. IMF carries no PGCS flow; ICSD (IMF.FAD, "Investment and
Capital Stock Dataset") is the successor by name AND content — pgcs's kpriv/kgov/kppp
capital-stock indicators are exactly the ICSD product. Probe-confirmed 2026-08-05, and
current_vintage() returned ICSD:1.0.0 live the same day (cycle 12 of the econ-updater
loop). Coverage comparison lands at the proof run per R75 (identical-count is the
same-dataset proof; a shortfall is a finding, not a silent accept).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.

Adding `imf_icsd_direct` takes nothing from `imf_pgcs`; supersession is #46, reserved.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "ICSD", "IMF.FAD", "imf_icsd_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
