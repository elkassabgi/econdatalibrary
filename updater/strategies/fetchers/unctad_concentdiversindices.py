"""S1 bulk fetcher — UNCTADstat US.ConcentDiversIndices (successor batch, #70).

54,124 obs / 1,824 series (product concentration/diversification indices, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ConcentDiversIndices", "unctad_concentdiversindices")
