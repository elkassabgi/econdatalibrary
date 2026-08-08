"""S1 bulk fetcher — UNCTADstat US.RCA (successor batch, #70).

1,379,898 obs / 57,535 series (Revealed Comparative Advantage by economy x product, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.RCA", "unctad_rca")
