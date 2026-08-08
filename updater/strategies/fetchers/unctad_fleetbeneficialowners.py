"""S1 bulk fetcher — UNCTADstat US.FleetBeneficialOwners (successor batch, #70).

226,044 obs / 22,680 series (fleet by beneficial ownership). All machinery shared via _unctad.py; the single parse/key implementation
lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.FleetBeneficialOwners", "unctad_fleetbeneficialowners")
