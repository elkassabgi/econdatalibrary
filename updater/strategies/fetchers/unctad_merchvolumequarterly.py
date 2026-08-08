"""S1 bulk fetcher — UNCTADstat US.MerchVolumeQuarterly (successor #4, #70).

139,358 obs / 1,680 series at first ingest (5 measure groups; Quarter axis with
isTime=true, codes '2005Q01' -> quarter-start dates). All machinery shared via
_unctad.py; the single parse/key implementation lives in jobs/ingest_unctad_ds.py.
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.MerchVolumeQuarterly", "unctad_merchvolumequarterly")
