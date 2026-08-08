"""S1 bulk fetcher — UNCTADstat US.ECommerceInternational (gate case, #70).

33,218 obs / 9,316 series (0.8% of dense estimate); ordinary series grain. All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.ECommerceInternational", "unctad_ecommerceinternational")
