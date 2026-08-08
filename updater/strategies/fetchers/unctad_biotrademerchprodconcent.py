"""S1 bulk fetcher — UNCTADstat US.BioTradeMerchProdConcent (successor batch, #70).

7,248 obs / 534 series (biotrade product concentration, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BioTradeMerchProdConcent", "unctad_biotrademerchprodconcent")
