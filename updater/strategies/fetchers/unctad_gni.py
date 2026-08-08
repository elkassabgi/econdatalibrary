"""S1 bulk fetcher — UNCTADstat US.GNI (successor batch, #70).

26,890 obs / 577 series (gross national income, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GNI", "unctad_gni")
