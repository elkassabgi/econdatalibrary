"""S1 bulk fetcher — UNCTADstat US.ContPortThroughput (successor batch, #70).

2,900 obs / 202 series (container port throughput, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ContPortThroughput", "unctad_contportthroughput")
