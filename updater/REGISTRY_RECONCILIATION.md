# Registry reconciliation (UPDATER_BUILD_PLAN.md §1.3, resolves O-1 / G6)

**Measured:** 2026-07-03, on-disk files, not documents.
**Procedure run** (reproduce any time):

```python
import yaml, json
from collections import Counter
reg = yaml.safe_load(open('updater/registry.yaml', encoding='utf-8'))
mat = json.load(open('UPDATE_CAPABILITY_MATRIX.json', encoding='utf-8'))
rlist = [e['source_id'] for e in reg['sources']]          # step 1
mlist = [p['source_id'] for p in mat['profiles']]         # step 2 (profiles is a LIST)
# step 3: diff the sets; step 4: reasons below; step 5: pin EXPECTED_SOURCE_COUNT
```

## Measured numbers

| What | Count |
|---|---|
| `updater/registry.yaml` `sources` entries | **130** (130 unique source_ids, zero duplicates) |
| `UPDATE_CAPABILITY_MATRIX.json` `profiles` entries | 133 list rows |
| ... of which unique `source_id`s | **129** |
| Matrix metadata `profiled` / `expected` fields | 133 / 133 (counts script-profile ROWS, not sources — misleading, see below) |

## Set diff and add-or-drop decisions (one line each)

**In registry, NOT in matrix (1):**

- `sec_edgar_xbrl` — **KEEP in registry.** Real, distinct product (XBRL companyfacts/submissions bulk zips → `clean_grouped/sec_edgar/`), deliberately split out of `sec_edgar` on 2026-06-25 per its own `strategy_reason`; the matrix predates the split and profiles both EDGAR products under the single `sec_edgar` row.

**In matrix, NOT in registry (0):** none — every profiled source has a registry entry.

## Why the matrix says 133 but contains 129 sources

Four sources were profiled once **per legacy ingest script**, producing two profile rows each (not a set-diff issue; each pair belongs to one registry source, no add/drop needed):

- `bis` — `jobs/ingest_bis_cbs_lbs.py` + `jobs/ingest_bis_full.py` (two scripts, one source).
- `bls` — `jobs/ingest_bls.py` + `jobs/ingest_bls_full.py` (two scripts, one source).
- `insee_sirene` — `jobs/ingest_insee_sirene.py` + `jobs/ingest_insee_sirene_bulk.py` (two scripts, one source).
- `GATED` — `jobs/ingest_irena.py` + `jobs/ingest_irena_country.py` (two scripts, one source).

So: 129 unique + 4 doubled rows = 133 rows; 129 unique + `sec_edgar_xbrl` (post-split, never profiled) = 130 registry sources. Every prior number now traces: "133" = script-profile rows, "129" = unique profiled sources, "130" = registry sources.

## Reconciled result

**`EXPECTED_SOURCE_COUNT = 130`** — pinned in `updater/config.py` and enforced by
`registry.validate(reg, expected_count=...)` in `updater/orchestrate.py` (per honesty
rule §5.6: measured, never copied from a doc). Adding or retiring a source requires
re-running the procedure above and updating `config.py` + this file in the same commit.

## Follow-ups (outside this change's file ownership)

- `UPDATE_CAPABILITY_MATRIX.json` metadata `profiled: 133` / `expected: 133` counts
  script rows, not sources — correct to per-source counts (or rename the field) in the
  Phase-1 doc pass (§5.6).
- `CONTINUOUS_UPDATE_DESIGN.md:66,112` "133 sources" matches only the script-row count —
  correct in the same doc pass (D-2).
