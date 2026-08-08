"""S1 bulk fetcher — UNCTADstat US.AssociatedPlasticsTradebyPartner (gate case, #70).

5,456,976 obs / 461,427 series; depth-2 DOT-prefix table grain (809 catalog ids). All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.AssociatedPlasticsTradebyPartner", "unctad_associatedplasticstradebypartner")
