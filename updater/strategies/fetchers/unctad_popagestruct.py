"""S1 bulk fetcher — UNCTADstat US.PopAgeStruct (successor batch, #70).

1,949,112 obs / 20,988 series (population age structure, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PopAgeStruct", "unctad_popagestruct")
