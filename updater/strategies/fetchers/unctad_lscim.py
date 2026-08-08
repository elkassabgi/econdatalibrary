"""S1 bulk fetcher — UNCTADstat US.LSCI_M (successor batch, #70).

44,100 obs / 563 series (liner shipping connectivity index, monthly). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.LSCI_M", "unctad_lscim")
