"""S1 bulk fetcher — UNCTADstat US.PortCallsArrivals_S (successor batch, #70).

8,810 obs / 751 series (port calls arrivals, semiannual '2018S01' codes). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PortCallsArrivals_S", "unctad_portcallsarrivalss")
