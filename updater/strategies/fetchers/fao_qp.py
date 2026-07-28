"""FAOSTAT Crops Primary (now inside QCL) — DIRECT, served by the QCL bulk dataset.

One of 25 fao_* sources that were SERVED and downloadable with no registry entry —
2,271 series, never attempted. This one looked unrecoverable twice over: its own
FAOSTAT domain no longer exists (FAOSTAT merged QL, QP and QA into QCL), and a
literal key comparison against QCL matched 0% because the components are in a
DIFFERENT ORDER — our ids are item.area.element where QCL's own are
element.area.item. Same three codes, read backwards; absence was a shape mismatch,
not missing data (ledger R75).

The permutation search in tools/prove_direct_repair's FAOSTAT counterpart finds
that automatically: template Item.Area.Element reproduces 98.5% of our 2,271 ids.
Behaviour lives in _faostat.py; the map is read from _faostat_maps/fao_qp.json.
"""
from . import _faostat as _f

SOURCE = "fao_qp"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
