"""S1 bulk fetcher — UNCTADstat US.SDG_PORFVOL (successor batch, #70).

2,780 obs / 194 series (SDG port freight volumes, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.SDG_PORFVOL", "unctad_sdgporfvol")
