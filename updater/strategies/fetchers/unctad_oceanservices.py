"""S1 bulk fetcher — UNCTADstat US.OceanServices (successor batch, #70).

68,565 obs / 5,348 series (ocean services trade). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.OceanServices", "unctad_oceanservices")
