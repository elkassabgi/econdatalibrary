"""S1 bulk fetcher — UNCTADstat US.GDPComponent (successor batch, #70).

833,468 obs / 16,653 series (GDP components, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.GDPComponent", "unctad_gdpcomponent")
