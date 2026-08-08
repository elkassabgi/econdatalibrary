"""S1 bulk fetcher — UNCTADstat US.IctUseEconActivity_Isic4 (successor batch, #70).

88,665 obs / 20,708 series (ICT use by ISIC Rev.4 activity). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctUseEconActivity_Isic4", "unctad_ictuseeconactivityisic4")
