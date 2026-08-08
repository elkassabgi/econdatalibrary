"""S1 bulk fetcher — UNCTADstat US.IctUseEconActivity (successor batch, #70).

12,006 obs / 7,335 series (ICT use by economic activity). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctUseEconActivity", "unctad_ictuseeconactivity")
