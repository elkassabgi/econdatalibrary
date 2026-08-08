"""S1 bulk fetcher — UNCTADstat US.PortCalls (successor batch, #70).

6,704 obs / 1,167 series (port calls, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PortCalls", "unctad_portcalls")
