# econdl resolver coverage (as of 2026-06-26)

All 33 series-cataloged sources have an at-rest resolver. Resolvers were generated
+ adversarially verified by the resolver-coverage workflow; cross-cutting concerns
(dedup, stamp_id, tidy_ok) are applied centrally in `_resolve.resolve()`.

A multi-source acceptance bundle of 30 sources (82,381 tidy rows) round-trips
`bundle()` → `pull()` **row-for-row identical** (sha256-verified).

## Native-only (relational/wide — shipped verbatim, excluded from the tidy frame)
`wikidata`, `fhfa`, `census`, `treasury`, `hf_equities`. These have no canonical
`value` column; `bundle()` includes them as native parquet + warns; read them from
the bundle's `data/<source>.parquet` directly.

## Store-coverage gaps surfaced during integration (DATA/ingest issues, NOT resolver bugs)
The resolver is correct; these series resolve to a path but the at-rest store is
missing/renamed. They error LOUDLY (never silently skipped). Backfill or re-catalog:

| source | resolvable | gap |
|---|---|---|
| `hf_equities` | 0 / 1391 | R2-only; not mirrored to the local store. Resolver targets `<root>/hf_equities/<TICKER>.parquet`; sync the R2 `clean/<TICKER>.parquet` (bucket `hfdatalibrary-data`) or point `$ECONDL_DATA` at a mirror. |
| `eurostat` | 7196 / 7637 | 441 catalog flows have no on-disk file in this snapshot (e.g. avia_*, bop_c6_*). |
| `boe` | 17 / 21 | 4 IUD series never fetched (IUDSOIA, IUDSIZC, IUDMIZC, IUDLIZC — SONIA + real zero-coupon yields). |
| `defillama` | 17 / 24 | 6 `protocol_tvl` slugs are versioned on disk (e.g. `aave-v3` not `aave`): aave, compound-finance, eigenlayer, makerdao, pancakeswap, uniswap; plus `tvl:total` (v2/historicalChainTvl) not materialized. |

## Known data-correctness fixes applied in the resolver layer
- `ecb`, `bea`: the same observation is replicated byte-identically across mirror/
  table files. `_DEDUP_ON` drops duplicate (series_key, obs_date) after the read so
  neither the native copy nor the tidy frame is inflated (ecb USD/EUR 21101→7034,
  bea A191RC:Q 2219→317).
- `worldbank_esg`, `worldbank`, `hf_equities`: identity is in the FILENAME (no in-file
  key column). `_STAMP_ID` stamps the catalog series_id onto every projected row so the
  bundle is self-describing and round-trippable.

## Known multiplicity NOT fixed in the resolver: `bls` vintage rows (GATED data-op)
`bls` ships **multiple rows per (series_id, obs_date)** from `cu.parquet` — e.g.
`bls:CUUR0000SA0` resolves to 4,796 rows over 1,472 distinct dates (most dates carry
3 rows, the rest 4). This is **deliberately left as-is** and is NOT in `_DEDUP_ON`.

Why: `cu.parquet` has 1,656,726 distinct (series_id, obs_date) groups, of which
**96,634 carry DIFFERING `value`s** — genuine BLS revisions across release vintages,
not byte-identical mirror dups. A blanket keep-first dedup (as used for `ecb`/`bea`,
which are *byte-identical*) would silently DROP real revised observations. Removing the
multiplicity correctly requires a **keep-latest-by-vintage** pass (the gated BLS dedup
data-op), not a resolver change. Until that data-op runs, both the local bundle and the
HTTP `.csv` ship the vintage rows verbatim and **identically** (the API and the client
share this resolver, so they cannot disagree). For a clean single-vintage example in
docs, prefer an already-deduped series such as `oecd:GDP_GROWTH_QOQ:USA` or
`penn_world_table:rgdpe:USA`.
