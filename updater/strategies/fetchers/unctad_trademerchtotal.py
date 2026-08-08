"""S1 bulk fetcher — UNCTADstat US.TradeMerchTotal (first successor source, #70).

81,760 obs / 1,220 series at first ingest (Economy.Flow.Mmeasure keys, annual). All
machinery is shared — see _unctad.py for the contract and jobs/ingest_unctad_ds.py
for the single parse/key implementation both this fetcher and the ingest call.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeMerchTotal", "unctad_trademerchtotal")
