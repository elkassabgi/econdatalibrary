"""S1 bulk fetcher — UNCTADstat US.VesselValueByRegistration (successor batch, #70).

1,869 obs / 250 series (vessel value by flag of registration, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.VesselValueByRegistration", "unctad_vesselvaluebyregistration")
