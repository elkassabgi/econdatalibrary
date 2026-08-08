"""S1 bulk fetcher — UNCTADstat US.MerchantFleet (successor batch, #70).

245,665 obs / 9,800 series (merchant fleet by flag/type). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.MerchantFleet", "unctad_merchantfleet")
