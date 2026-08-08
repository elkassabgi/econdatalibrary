"""S1 bulk fetcher — UNCTADstat US.UCPI_M (successor batch, #70).

15,694 obs / 42 series (unit commodity price indices, monthly '1995M01' codes). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.UCPI_M", "unctad_ucpim")
