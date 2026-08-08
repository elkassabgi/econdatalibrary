"""S1 bulk fetcher — UNCTADstat US.BioTradeMerchGDPShare (successor batch, #70).

3,719 obs / 272 series (biotrade merchandise share of GDP, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BioTradeMerchGDPShare", "unctad_biotrademerchgdpshare")
