"""S1 bulk fetcher — UNCTADstat US.TotAndComServicesQuarterly (successor batch, #70).

812,977 obs / 11,553 series (services trade by main category, quarterly). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TotAndComServicesQuarterly", "unctad_totandcomservicesquarterly")
