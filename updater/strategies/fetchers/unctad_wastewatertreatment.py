"""S1 bulk fetcher — UNCTADstat US.WastewaterTreatment (successor batch, #70).

49,165 obs / 2,039 series (wastewater treatment indicators). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.WastewaterTreatment", "unctad_wastewatertreatment")
