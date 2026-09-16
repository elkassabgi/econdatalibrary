"""WHO SDG health indicators — from WHO's OWN Global Health Observatory API (ghoapi.azureedge.net).

Migrated off the relay mirror 2026-08-02: the relay aggregator is banned (CLAUDE.md §0, ledger R251)
and every source now comes from its publisher. Behaviour lives in _who_base.py, including the
key grammar and the proof that it reconstructs our published ids exactly.
"""
from . import _who_base as _base

SOURCE, PREFIX = "who_sdg", "WHO_SDG"


def current_vintage(unit):
    return _base.current_vintage(unit, SOURCE)


def update(unit, since):
    return _base.run(SOURCE, PREFIX)
