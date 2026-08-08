"""S1 bulk fetcher — UNCTADstat US.TradeFoodProcCat_Cat_RCA (successor batch, #70).

648,241 obs / 17,617 series (food processing trade RCA by category). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeFoodProcCat_Cat_RCA", "unctad_tradefoodproccatcatrca")
