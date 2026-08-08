"""S1 bulk fetcher — UNCTADstat US.SDG_LULFRG (successor batch, #70).

7,252 obs / 300 series (SDG land use / forest indicators, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.SDG_LULFRG", "unctad_sdglulfrg")
