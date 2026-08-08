"""S1 bulk fetcher — UNCTADstat US.ECommerceTotal (gate case, #70).

181,558 obs / 33,818 series (4% of dense estimate); ordinary series grain. All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ECommerceTotal", "unctad_ecommercetotal")
