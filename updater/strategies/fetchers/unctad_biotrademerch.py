"""S1 bulk fetcher — UNCTADstat US.BiotradeMerch (giant #13, #127).

1,063,192,830 obs / 112,266,300 series (M4023) — the family's largest store.
Depth-2 DOT-prefix table grain (6,666 catalog ids, _DOT_TABLE_GRAIN) — measured
2026-08-12 over the FULL store with duckdb, no sampling: depth-1 is 2,222
prefixes (~478k obs each, unusable as tables), depth-3 is 1,362,065 — alone
172% of D1's entire remaining headroom — and series grain (112.3M) is 141x it.
Depth-2 sits at ~159k obs/table.

Keys are Product.Flow.Economy.Partner.Measure; the catalogue id is
Product.Flow. The M4023 (value) measure was assembled from the spill cache by
tools/_merge_unctad_spills.py (count gated exactly against the pull's own
1,063,192,830); the M0100 (quantity) campaign appends under the SAME table ids
when it lands. All machinery shared via _unctad.py; the vintage is the
publisher's version|lastUpdated stamp (proven live 2026-08-12:
'10003|2026-07-15T11:40:26').
"""
from __future__ import annotations

from ._unctad import make

current_vintage, update = make("US.BiotradeMerch", "unctad_biotrademerch")
