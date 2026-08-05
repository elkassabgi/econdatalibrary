"""IMF Exchange Rates (ER) — DIRECT from api.imf.org (agency IMF.STA).
NEW COVERAGE: no legacy counterpart id — this is the successor home of the exchange-rate
content that lived inside the dismembered IFS (its E* family, 5,918 frozen series).

Thin wrapper over _imf_direct.py, the imf_bop_direct pattern.

WHY THIS ONE EXISTS. imf_ifs was dismembered with semantic re-coding (no mechanical
supersession, per the 2026-08-05 mapping); its exchange-rate family had no successor until
the IFS-families keyword sweep found ER v4.0.1 live in the /dataflow catalogue.
current_vintage() returned ER:4.0.1 live 2026-08-05 (cycle 16 of the econ-updater loop).

Size and key shape are measured at the proof run, never assumed; serving grain is decided
by the #45 D1 arithmetic when the counts land.
"""
from . import _imf_direct as _base

FLOW, AGENCY, SOURCE = "ER", "IMF.STA", "imf_er_direct"


def current_vintage(unit):
    return _base.vintage(FLOW)


def update(unit, us):
    return _base.run(FLOW, AGENCY, SOURCE)
