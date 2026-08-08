"""S1 bulk fetcher — UNCTADstat US.IFF_CrimesRelated (successor batch, #70).

130 obs / 36 series (crime-related illicit financial flows, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IFF_CrimesRelated", "unctad_iffcrimesrelated")
