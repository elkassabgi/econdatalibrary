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
| `updater/registry.yaml` `sources` entries | measured on the date above (unique source_ids, zero duplicates) |
| `UPDATE_CAPABILITY_MATRIX.json` `profiles` entries | list rows, counted on the date above |
| ... of which unique `source_id`s | measured on the date above |
| Matrix metadata `profiled` / `expected` fields | count script-profile ROWS, not sources — misleading, see below |

## Set diff and add-or-drop decisions (one line each)

**In registry, NOT in matrix (1):**

- `sec_edgar_xbrl` — **KEEP in registry.** Real, distinct product (XBRL companyfacts/submissions bulk zips → `clean_grouped/sec_edgar/`), deliberately split out of `sec_edgar` on 2026-06-25 per its own `strategy_reason`; the matrix predates the split and profiles both EDGAR products under the single `sec_edgar` row.

**In matrix, NOT in registry (0):** none — every profiled source has a registry entry.

## Why the matrix's row count is not its source count

Some sources were profiled once **per legacy ingest script**, producing two profile rows each (not a set-diff issue; each pair belongs to one registry source, no add/drop needed).

So: the unique sources plus their doubled rows make up the matrix rows, and the unique sources plus `sec_edgar_xbrl` (post-split, never profiled) are the registry sources. Every earlier figure traces to script-profile rows, to unique profiled sources or to registry sources.

## Reconciled result

**`EXPECTED_SOURCE_COUNT`** — pinned in `updater/config.py` and enforced by
`registry.validate(reg, expected_count=...)` in `updater/orchestrate.py` (per honesty
rule §5.6: measured, never copied from a doc). Adding or retiring a source requires
re-running the procedure above and updating `config.py` + this file in the same commit.

## Follow-ups (outside this change's file ownership)

- `UPDATE_CAPABILITY_MATRIX.json` metadata `profiled` / `expected` counts
  script rows, not sources — correct to per-source counts (or rename the field) in the
  Phase-1 doc pass (§5.6).
- the source count at `CONTINUOUS_UPDATE_DESIGN.md:66,112` matches only the script-row count —
  correct in the same doc pass (D-2).
