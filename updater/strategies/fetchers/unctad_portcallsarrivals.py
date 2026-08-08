"""S1 bulk fetcher — UNCTADstat US.PortCallsArrivals (successor batch, #70).

4,424 obs / 751 series (port calls arrivals, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PortCallsArrivals", "unctad_portcallsarrivals")
