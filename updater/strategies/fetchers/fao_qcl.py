"""FAOSTAT Crops and livestock products (QCL) — DIRECT from FAOSTAT's bulk API.

Repairs a source that was SERVED and downloadable with no registry entry at all:
20,238 series in the catalog and in the worker's supported list, never once
attempted. All 25 fao_* sources are in that state (136,754 series) — the registry's
`faostat` entry is a different source, which is how the family went unnoticed.

Behaviour lives in _faostat.py. The key template (Element Code . Area Code .
Item Code) is DISCOVERED by tools/prove_faostat_repair.py, scored on exact
reproduction of our published ids, and read from _faostat_maps/fao_qcl.json —
98.2% of 20,238 ids reproduced, values agreeing 92.22% across 988,719 shared
points (the rest FAO's routine revisions). Upstream carries 78,974 series against
our 20,238 and runs to 2024 rather than 2022.
"""
from . import _faostat as _f

SOURCE = "fao_qcl"


def current_vintage(unit):
    return _f.vintage(SOURCE)


def update(unit, us):
    return _f.run(SOURCE)
