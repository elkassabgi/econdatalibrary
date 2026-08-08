"""S1 bulk fetcher — UNCTADstat US.TradeServCatTotal (gate case at DOT-prefix table grain, #70).

1,695,966 obs / 101,079 series (services trade by category). Served at depth-2 TABLE grain (562 catalog ids; see _DOT_TABLE_GRAIN in the
resolver). All machinery shared via _unctad.py; parse/key logic solely in
jobs/ingest_unctad_ds.py — the size-cap chunker handles the pull automatically.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.TradeServCatTotal", "unctad_tradeservcattotal")
