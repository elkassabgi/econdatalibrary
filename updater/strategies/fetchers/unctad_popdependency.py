"""S1 bulk fetcher — UNCTADstat US.PopDependency (successor batch, #70).

88,596 obs / 954 series (population dependency ratios, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PopDependency", "unctad_popdependency")
