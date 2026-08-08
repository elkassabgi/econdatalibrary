"""S1 bulk fetcher — UNCTADstat US.Cpi_A (successor batch, #70).

21,340 obs / 560 series (consumer price indices, annual). Source id is
unctad_cpi_annual, NOT the mechanical slug: source_id_for("US.Cpi_A") produced
"unctad_cpia", which is a LEGACY DBnomics-era source with 637 live series — the
collision overwrote its store before the OVERRIDES map + guard existed (R399).
All machinery shared via _unctad.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.Cpi_A", "unctad_cpi_annual")
