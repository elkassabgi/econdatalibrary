"""S1 bulk fetcher — UNCTADstat US.CreativeServ_Group_E (successor batch, #70).

2,772 obs / 189 series (creative services by economy group, annual). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CreativeServ_Group_E", "unctad_creativeservgroupe")
