"""S1 bulk fetcher — UNCTADstat US.IntraTrade (gate case at DOT-prefix table grain, #70).

11,571,843 obs / 376,909 series (intra/extra-trade by partner group). Served at depth-2 TABLE grain (247 catalog ids; see _DOT_TABLE_GRAIN in the
resolver). All machinery shared via _unctad.py; parse/key logic solely in
jobs/ingest_unctad_ds.py — the size-cap chunker handles the pull automatically.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IntraTrade", "unctad_intratrade")
