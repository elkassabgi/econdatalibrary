"""S1 bulk fetcher — UNCTADstat US.VesselValueByOwnership (successor batch, #70).

1,885 obs / 248 series (vessel value by ownership, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.VesselValueByOwnership", "unctad_vesselvaluebyownership")
