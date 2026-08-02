"""WHO Health Workforce — from WHO's OWN Global Health Observatory API (ghoapi.azureedge.net).

Migrated off the DBnomics mirror 2026-08-02: DBnomics is banned (CLAUDE.md §0, ledger R251)
and every source now comes from its publisher. Behaviour lives in _who_gho.py, including the
key grammar and the proof that it reconstructs our published ids exactly.
"""
from . import _who_gho as _base

SOURCE, PREFIX = "who_hwf", "WHO_HWF"


def current_vintage(unit):
    return _base.current_vintage(unit, SOURCE)


def update(unit, since):
    return _base.run(SOURCE, PREFIX)
