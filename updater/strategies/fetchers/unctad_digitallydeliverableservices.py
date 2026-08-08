"""S1 bulk fetcher — UNCTADstat US.DigitallyDeliverableServices (successor batch, #70).

281,579 obs / 20,876 series (digitally deliverable services). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.DigitallyDeliverableServices", "unctad_digitallydeliverableservices")
