"""FAOSTAT Temperature change on land (ET) — DIRECT from FAOSTAT's bulk API.

One of 25 fao_* sources that were SERVED and downloadable with no registry entry at
all — 561 series in the catalog, never once attempted, because the family hid
behind the registry's separate `faostat` entry (a different source).

Behaviour lives in _faostat.py. The key template is DISCOVERED by
tools/prove_faostat_repair.py — scored on exact reproduction of our published ids,
never guessed — and read from _faostat_maps/fao_et.json: 100.0% of 561 ids
reproduced. Upstream carries 576 series.
"""
from . import _faostat as _f

SOURCE = "fao_et"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
