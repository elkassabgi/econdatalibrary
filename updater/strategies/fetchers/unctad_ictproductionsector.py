"""S1 bulk fetcher — UNCTADstat US.IctProductionSector (successor batch, #70).

919 obs / 125 series (ICT production sector, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctProductionSector", "unctad_ictproductionsector")
