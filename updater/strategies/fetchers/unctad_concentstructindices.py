"""S1 bulk fetcher — UNCTADstat US.ConcentStructIndices (successor batch, #70).

47,492 obs / 1,532 series (market concentration/structural change indices, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ConcentStructIndices", "unctad_concentstructindices")
