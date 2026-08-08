"""S1 bulk fetcher — UNCTADstat US.IctUseEnterprSize (successor batch, #70).

30,282 obs / 6,155 series (ICT use by enterprise size). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.IctUseEnterprSize", "unctad_ictuseenterprsize")
