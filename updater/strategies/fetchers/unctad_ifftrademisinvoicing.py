"""S1 bulk fetcher — UNCTADstat US.IFF_TradeMisinvoicing (successor batch, #70).

392 obs / 28 series (illicit financial flows from trade misinvoicing, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IFF_TradeMisinvoicing", "unctad_ifftrademisinvoicing")
