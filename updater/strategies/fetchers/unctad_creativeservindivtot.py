"""S1 bulk fetcher — UNCTADstat US.CreativeServ_Indiv_Tot (successor batch, #70).

11,427 obs / 955 series (creative services, individual economies). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.CreativeServ_Indiv_Tot", "unctad_creativeservindivtot")
