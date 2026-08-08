"""S1 bulk fetcher — UNCTADstat US.MerchTheilIndices (successor batch, #70).

51,654 obs / 1,800 series (Theil concentration indices, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.MerchTheilIndices", "unctad_merchtheilindices")
