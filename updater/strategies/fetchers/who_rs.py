"""WHO Reproductive health / population — from DBnomics dataset WHO/RS.

Thin wrapper; all behaviour lives in _dbnomics.py, including WHY DBnomics is a legitimate
upstream for WHO specifically (its WHO index was re-run 2026-07-24) and not for UNCTAD,
UNESCO or FAO, whose indexes stopped years ago.
"""
from . import _dbnomics as _base

SOURCE, PROVIDER, DATASET = "who_rs", "WHO", "RS"


def current_vintage(unit):
    return _base.vintage(PROVIDER, DATASET)


def update(unit, since):
    return _base.run(SOURCE, PROVIDER, DATASET)
