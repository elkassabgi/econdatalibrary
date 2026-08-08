"""S1 bulk fetcher — UNCTADstat US.ShipScrapping (successor batch, #70).

2,482 obs / 226 series (ship scrapping by country of demolition, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ShipScrapping", "unctad_shipscrapping")
