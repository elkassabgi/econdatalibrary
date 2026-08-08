"""S1 bulk fetcher — UNCTADstat US.Remittances (successor batch, #70).

14,497 obs / 741 series (personal remittances, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Remittances", "unctad_remittances")
