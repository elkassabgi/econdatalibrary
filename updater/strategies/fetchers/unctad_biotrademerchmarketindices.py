"""S1 bulk fetcher — UNCTADstat US.BioTradeMerchMarketIndices (successor batch, #70).

133,255 obs / 8,888 series (biotrade market indices). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BioTradeMerchMarketIndices", "unctad_biotrademerchmarketindices")
