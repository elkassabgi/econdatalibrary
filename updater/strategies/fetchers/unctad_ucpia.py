"""S1 bulk fetcher — UNCTADstat US.UCPI_A (successor batch, #70).

854 obs / 28 series (unit commodity price indices, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.UCPI_A", "unctad_ucpia")
