"""S1 bulk fetcher — UNCTADstat US.IctUseLocation (successor batch, #70).

1,076 obs / 478 series (ICT use by location, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctUseLocation", "unctad_ictuselocation")
