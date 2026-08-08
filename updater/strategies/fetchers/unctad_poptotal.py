"""S1 bulk fetcher — UNCTADstat US.PopTotal (successor batch, #70).

88,278 obs / 954 series (total population, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PopTotal", "unctad_poptotal")
