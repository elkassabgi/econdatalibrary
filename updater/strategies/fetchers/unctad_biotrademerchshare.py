"""S1 bulk fetcher — UNCTADstat US.BiotradeMerchShare (gate case at DOT-prefix table grain, #70).

1,705,660 obs / 144,552 series (biotrade merchandise shares). Served at depth-2 TABLE grain (544 catalog ids; see _DOT_TABLE_GRAIN in the
resolver). All machinery shared via _unctad.py; parse/key logic solely in
jobs/ingest_unctad_ds.py — the size-cap chunker handles the pull automatically.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BiotradeMerchShare", "unctad_biotrademerchshare")
