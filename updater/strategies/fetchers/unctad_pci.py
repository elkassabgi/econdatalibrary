"""S1 bulk fetcher — UNCTADstat US.PCI (successor batch, #70).

59,085 obs / 2,403 series (productive capacities index). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.PCI", "unctad_pci")
