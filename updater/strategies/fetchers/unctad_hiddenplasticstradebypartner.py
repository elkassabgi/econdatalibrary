"""S1 bulk fetcher — UNCTADstat US.HiddenPlasticsTradebyPartner (gate case, #70).

9,449,860 obs / 756,314 series; depth-2 DOT-prefix table grain (1,076 catalog ids). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.HiddenPlasticsTradebyPartner", "unctad_hiddenplasticstradebypartner")
