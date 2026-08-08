"""S1 bulk fetcher — UNCTADstat US.ShipBuilding (successor batch, #70).

2,940 obs / 248 series (ship building deliveries, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ShipBuilding", "unctad_shipbuilding")
