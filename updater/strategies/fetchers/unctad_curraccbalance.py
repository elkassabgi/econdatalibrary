"""S1 bulk fetcher — UNCTADstat US.CurrAccBalance (successor batch, #70).

22,450 obs / 558 series (current account balance, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CurrAccBalance", "unctad_curraccbalance")
