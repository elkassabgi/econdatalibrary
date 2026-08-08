"""S1 bulk fetcher — UNCTADstat US.TradeFoodProcCat_Proc_RCA (successor batch, #70).

712,550 obs / 19,087 series (food processing trade RCA by process). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeFoodProcCat_Proc_RCA", "unctad_tradefoodproccatprocrca")
