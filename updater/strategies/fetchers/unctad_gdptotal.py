"""S1 bulk fetcher — UNCTADstat US.GDPTotal (successor batch, #70).

91,005 obs / 1,785 series (GDP totals, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GDPTotal", "unctad_gdptotal")
