"""S1 bulk fetcher — UNCTADstat US.FdiFlowsStock (successor batch, #70).

66,554 obs / 2,150 series (FDI flows and stock, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.FdiFlowsStock", "unctad_fdiflowsstock")
