# Updater remediation plan — systematic, per-source

**Date:** 2026-07-23 · **Base:** `main` @ `fcae3eb` · **Evidence:** production health run
[30036391239](https://github.com/elkassabgi/econdatalibrary/actions/runs/30036391239) (every
source's cadence + newest observation, computed against live R2 state).

## The reframe (why "no new rows" is usually NOT a bug)

Two independent things were being conflated:

1. **Is the data current?** — `newest_obs` vs the source's cadence. A monthly source whose
   provider last published in June is *correct* to show June and add nothing today.
2. **Will it auto-update?** — is the source `live: true` (runs in the daily CI)?

**Finding:** most sources are **current but frozen** — their data is fresh from a bulk load, but
only **2 of 105** (`cnb`, `frankfurter`) actually auto-update. The health report's RED flags are
partly **false alarms** for quiet annual/static data (e.g. `ppi` "RED-DATA @2022" is IEP's genuine
latest edition). So the job is mostly **enabling auto-update on sources whose data is already
current**, not repairing stale data — plus a short list of genuinely broken things.

## Scorecard (105 registry sources)

| Bucket | # | Meaning |
|---|--:|---|
| **A. Working & current** | 34 | Data current for cadence. 2 live (auto-update); 32 current-but-frozen. |
| **B. Fetcher works, needs promotion** | 18 | Runs (or will, post-patch); data mostly current; not live yet. |
| **C. PxWeb parser (Layer 2)** | 6 | Run `partial`: some tables 200-but-0-rows. Benign-or-real, unverified. |
| **D. Derive/catalog (Layer 3)** | 2 | Fetch adds rows, but CSV-derive can't map new series_keys. |
| **E. Genuinely stale** | 2 | `imf_commodity` (real, 14mo); `ppi` (likely false alarm — verify). |
| **F. No fetcher** | 43 | No incremental adapter; ~20 have bulk data on disk, most annual/irregular. |

Only **2** are `live: true`. That single fact — not stale data — is why "nothing updates."

---

## The path (ordered; each source proven before promotion)

The promotion contract for EVERY source (learned the hard way — ledger R35/R36):
> `workflow_dispatch` with `source: <id>` → read the log for `N unit(s) processed` and a
> per-unit `ok/added/no_change` (NOT `partial`/`DefinitiveError`) → only then flip `live: true`.
> A green badge or a local run proves nothing.

### Phase 0 — CI-safety (DONE, proven)
The 12 store-backed fetchers (`abs adb bls ecb eurostat insee_bdm insee_melodi istat scb GATED
stat_estonia treasury`) read the store via the R2-routed `blob` layer. Proven live: `scb` processed
2,741 sub-units in CI where pre-patch it died at "source dir missing". `fcae3eb`.

### Phase 1 — Promote the clean, current sources (the bulk win)
Sources whose fetcher works and whose data is current — flip live after a clean dispatch, **cadence
order (daily/weekly first, they benefit most)**:

- **Daily/weekly:** `bcrp`, `ofr`, `defillama`, `bcb`, `treasury`, `statcan`, `bls`(after D),
  `wikidata`.
- **Monthly:** `bundesbank`, `epu`, `nasa_giss`, `dst`, `eurostat`, `oecd`, `faostat`,
  `insee_melodi`, `worldbank_wdi`.
- **Annual/irregular (lowest urgency — rarely change):** `damodaran`, `gcb`, `wgi`,
  `transparency_ti`, `undp_hdr`, `kof_globalization`, `GATED`, `swiid`, `ei_statreview`,
  `gpi/gti/etr`, `harvard_atlas`, `edgar_jrc`, `fsi_fundforpeace`, `penn_world_table`, `ggdc`,
  `yale_epi`, `sec_edgar`, `adb`, `ksh`.
- **Static (flip live harmlessly; they self-report no_change):** `barro_lee`, `pwt`, `gppd`,
  `oxcgrt`, `cepii_gravity`.

Expected `no_change` for most annual/static on any given day — that is SUCCESS, not silence.

### Phase 2 — The genuinely stale (verify against provider, then fix)
- **`imf_commodity`** — monthly, stuck at 2025-06. **ROOT CAUSE VERIFIED 2026-07-23 (live probe):**
  NOT our bug. It mirrors IMF PCPS *via DBnomics* (`api.db.nomics.world/v22/series/IMF/PCPS`), and
  DBnomics's IMF/PCPS mirror is itself frozen — dataset metadata reads `updated: 2025-07-15,
  indexed_at: 2025-07-16T02:22Z`, i.e. ~a year stale. Our data equals what DBnomics still serves;
  the upstream link died (IMF migrated PCPS to its new data portal in 2025, deprecating the old
  mirror). FIX = repoint the fetcher to IMF's current PCPS feed (data.imf.org / new IMF SDMX API) —
  a fetcher rewrite against a new endpoint, not a delta tweak. Until then it is honestly frozen at
  the last vintage DBnomics published.
  UPDATE 2026-07-24: the repoint TARGET is confirmed LIVE — `api.imf.org/external/sdmx/3.0/data/dataflow/IMF.RES/PCPS/~/<key>` returns the PCPS dataflow (v9.0.0). BUT the new v9.0.0 has a DIFFERENT dimension structure than the old {FREQ}.{REF_AREA}.{COMMODITY}.{UNIT}; my guessed keys + `c[TIME_PERIOD]=ge:` time filter returned ZERO observations (a query-format issue, NOT proof of data absence). So current data past 2025-06 is NOT yet confirmed. The fetcher rewrite must first pull the DSD/codelists (dims INDICATOR/COMMODITY_CF/DATA_TRANSFORMATION/UNIT…), derive valid keys + the SDMX-3.0 time-filter syntax, THEN map to our series_key. Not a delta tweak; a real rewrite.
- **`ppi`** — annual @2022. **Verify** IEP hasn't published 2023+; if not, it is CURRENT →
  reclassify A and silence the RED-DATA false alarm (raise its SLA or mark edition-final).
- Spot-check `bcrp`/`ofr` (daily, ~1 month back): provider-quiet or a real freeze?

### Phase 3 — PxWeb parser (Layer 2): `scb bfs hagstofa statfin stat_slovenia pip`
All run `partial` from "200 but 0 rows" on some tables. Root-cause ONE (scb) end-to-end: pull the
failing table ids from a dispatch, fetch one live, decide per ledger R25–R27 whether it's the
time-axis misclassification (real) or a quiet-table false alarm (tighten the classifier). Fix the
shared cause, then all six clear together.

### Phase 4 — Derive/catalog (Layer 3): `bls insee_bdm norgesbank`
Fetch works and adds rows, but the CSV-derive reports "changed series_keys have no catalog mapping".
The new series exist upstream but aren't in the catalog. Fix = extend the per-source catalog
mapping so derive can place them. (`bls` ALSO gated on its legacy-inflation data-op, ledger R18 —
do that first.)

### Phase 5 — Build the 43 missing fetchers, cadence-prioritised
`bea bis boe census cbs_nl cepii_baci cepii_gravity cftc comtrade edgar_13f eia ember fdic fed_board
fhfa gii gleif gus_dbw idb ilostat imf imf_fsi insee_sirene ipea ksh_stadat maddison noaa nyfed
ons_uk GATED pxweb rba riksbank sec_edgar_xbrl stats_nz ucdp un_wpp unhcr usda worldbank_esg
worldbank_extra GATED zillow`

- **Daily/weekly first** (`eia fed_board gleif nyfed riksbank cftc fdic sec_edgar_xbrl
  worldbank_esg`) — they go stale fastest.
- **Monthly next** (`bea bis boe census ember fhfa ilostat imf imf_fsi noaa ons_uk GATED rba usda
  GATED zillow gus_dbw ipea insee_sirene`).
- **Annual/irregular/static last** (`comtrade cepii_* gii idb ksh_stadat maddison ucdp un_wpp unhcr
  edgar_13f cbs_nl stats_nz worldbank_extra`) — many change once a year.
- ~20 already have bulk data on disk (20.5 GB) but are un-catalogued — those also need the
  flow-grain catalog step so their existing data is even visible.

---

## Honest current state

- **Auto-updating in production:** 2 (`cnb`, `frankfurter`).
- **Data current but frozen (not live):** ~50 (Bucket A minus 2, plus current members of B).
- **Genuinely stale:** 1 confirmed (`imf_commodity`), 1 to verify (`ppi` likely fine).
- **Cannot yet run / no fetcher:** 43.
- **Infra blocker (Layer 1):** FIXED + proven; unblocks promotion of the 12 store-backed fetchers.

The next concrete milestone is **one source promoted end-to-end** — a clean dispatch → `live: true`
→ confirmed on the following cron. Recommended first: `bcb` or `wikidata` (monthly, currently `OK`,
non-PxWeb, no derive issue) — a genuine clean win, then replicate down Phase 1.

---

## Verified diagnosis (2026-07-23, workflow wf_fc88e6a3 — 5 diagnose + 2 adversarial-verify agents)

Every root cause code-grounded; the two hard classes independently refuted-or-confirmed. Key
refutations: the PxWeb time-axis resolver is **correct** (not the culprit), and a first-draft
CSV-coherence fix would have **corrupted the live `frankfurter`** source. Both were caught by
**this workflow's own `verify:csv_coherence` adversarial agent (wf_fc88e6a3)** — an internal
verification loop, NOT either external human/AI reviewer (record corrected 2026-07-24 per the
second reviewer's note; attribution matters for knowing which loop caught what).

| Class | Sources | Verdict | Root cause | Fix status |
|---|---|---|---|---|
| PxWeb "0 rows" | **scb** | real | Far-future ceiling (today+2) drops legit population **projections to 2070** → false break every tick | ✅ FIXED (scb.py, status-only, `3304ea5`) |
| | **bfs** | real | Parse-branch missing the `since_max` guard the other 3 carry → flags date-less census tables | ✅ FIXED (shared helper, `3304ea5`) |
| | hagstofa, statfin, stat_estonia | **stale** | Old state from before the R25 fixes; current code reproduces **0** structural | re-dispatch to clear |
| | pip | separate | Not PxWeb — World Bank poverty-line bad body | separate triage |
| treasury "catalog missing" | **treasury** | real | `_load_catalog` raw local open + catalog not on R2 (same 2-part bug as scb) | ✅ FIXED + catalog uploaded (`3304ea5`) |
| CSV-coherence | insee_bdm, ssb | real | Cursor key ≠ catalog series-id (grain mismatch) | align cursor keys (careful: keep derive-all cap — `frankfurter` depends on it) |
| | bls | real | `finalize()` called without `series_cursors=` | populate series_cursors |
| | stat_latvia | real | Grain-aligned but catalog **never uploaded to R2** (R28) | upload its catalog to R2 |
| | norgesbank, unsdg | stale | Already deleted/denylisted | clear stale state |
| Transient | bundesbank, cso, defillama, GATED, stat_slovenia | **by design** | Self-healing; data preserved, retries next tick | none (auto-retry once live) |
| Memory | vdem | real | 77M-row OOM, mislabeled "transient" | overwrite-mode + keep OFF CI (giant → workstation) |
| "dir missing" | abs, adb | **stale** | Already fixed by fcae3eb; stale recorded state | re-dispatch to clear |

**Landmine noted:** `hagstofa.py:398`, `ssb.py:472`, `stat_latvia.py:382` still carry the raw
`os.path.isdir` "source dir missing" pattern — they will fail in CI the moment they run there.
Patch (blob.list_parquets) when each is promoted.

**Net:** of ~20 "failing" sources, **most are stale state or by-design self-healing.** The genuine
code defects were treasury + scb + bfs (now fixed) and the CSV-coherence grain-alignment (careful,
next). The full agent transcripts: workflow wf_fc88e6a3 journal.

---

## 2026-07-24 — CSV-COHERENCE CLASS RESOLVED (the pivotal blocker)

All three root causes fixed and PROVEN end-to-end on bfs:

1. **Parser** (false structural "200 but 0 rows") — scb (ceiling-vs-projection), bfs (shared
   `structural_on_zero_rows` guard). Committed 3304ea5.
2. **Grain mismatch** (cursor key ≠ catalog series_id) — ssb (`SSB:<tid>`, 325f63b), insee_bdm
   (idbank not flow_id, changed-set separated from frontier, 447f21e). Locally verified: aligned
   keys map to the catalog (ssb 3/3, insee_bdm 95/95).
3. **Stale R2 coherence catalog** (THE root) — `_aqueduct/catalog.db.zst` was months stale:
   missing the 13 sources catalogued since, still carrying the ~20 purged. Refreshed via
   `tools/refresh_r2_catalog.py` (02dc950), superset-verified, backup kept. [ledger R38]

**PROOF (bfs, run 30068599472):** its error walked forward across three dispatches —
"90/648 parsed 0 rows" (parser) → "coherence unmet: 582 keys unmapped" (catalog) → "9/648
transient-failed; will retry" (benign self-healing). The coherence-unmet error is GONE.

**bfs data-op:** trimmed 75 corrupt far-future rows (one table `px-x-0102020300_102`, year>2075
to 2150); 5,337,546 legit projection rows (to 2055/2075) preserved; backup kept (58295d8 tool).

**Remaining to promote each coherence source:** a clean `--source` dispatch (ok/no_change) then
`live:true`. Order (after the 06:00 cron soak validates the 5-tier): insee_bdm, stat_latvia,
stat_estonia, ssb (bfs holds until its transients clear + a clean run). bls stays gated (R18).

## Live tier: 5 (bcb, cnb, frankfurter, scb, treasury). Gate before #6: the 06:00 UTC cron soak.

---

## 2026-07-24 session — live tier 5 → 7, batch-dispatch + first bulk template

**Promoted to live (CI-proven `ok`, data through today):** `nyfed` (NY Fed SOFR/OBFR/TGCR via FRED
date-tail), `riksbank` (SWEA ~117 series, /Series freshness pre-filter + per-series date-tail).
Run 30101500855 batch-proved both. **Live tier now 7:** bcb, cnb, frankfurter, nyfed, riksbank, scb, treasury.

**New fetchers built + pushed (all CI-safe, store I/O via `blob`):**
- `nyfed`, `riksbank` — promoted (above).
- `unhcr` (annual refugee stats, 3 endpoints, recent-years window), `boe` (BoE IADB, per-3char-prefix
  files, real server-side Datefrom/Dateto date-tail) — proving in batch #2 (run 30104047711).
- `rba` — **first bulk_snapshot template**: no server-side date filter, but RBA CSVs carry
  Last-Modified and honour If-Modified-Since (verified 304+0 bytes). Per-file conditional GET against a
  **blob-routed** Last-Modified sidecar; 304=skip cheap, 200=parse+merge. Template for the
  scraped/fixed-URL + Last-Modified bulk sub-family. Proving in batch #2. Not yet live.

**Infra / fixes:**
- workflow `source` input now accepts a comma/space LIST → repeated `--source` (injection-safe:
  env var, no-glob, `[a-z0-9_]`-validated). One run batch-proves many sources.
- `catalog_complete boe` (+30,653 rows) + `refresh_r2_catalog 20260724c` → R2 coherence catalog 1,260,376.
- **faostat CI-safety (R36):** its vintage sidecar + os.listdir scans used raw runner paths → would
  re-download all 68 domains every CI run. Blob-routed sidecar (read_bytes/write_bytes_atomic) +
  list_parquets + obs_date-projected max scan. Establishes the CI-safe bulk-sidecar pattern.

**Concurrency lesson:** the `aqueduct-updater` group is `cancel-in-progress: false` → when 3 runs
pile up it cancels the MIDDLE pending one. So CI proving is serial (1 in-progress + 1 pending); batch
many sources per dispatch. Killed a 28h/99GB runaway bare-`updater.run` (vdem-OOM, R39).

**Coverage inventory (fetcher present? by strategy):** MISSING = 41 (13 extend_by_date, 26
bulk_snapshot, 1 overwrite [cbs_nl], 1 manual [gii]). Built 5 of the 13 date-tail this session
(nyfed, riksbank, unhcr, boe + rba is bulk). Remaining date-tail clean clones exhausted; deferred:
- `ipea` (OData $filter IGNORED → needs SERATUALIZACAO freshness-gate, 2899 series),
- `comtrade` (preview API hard-capped 500 records/query → needs reporter/commodity pagination),
- `imf_fsi` (data.imf.org/api/SDMX/BI = 404; migrated to SDMX 3.0 like imf_commodity — repoint+DSD).

**Big uncatalogued sources (statcan ~4M, worldbank_wdi 289k, boe was 30k):** do NOT need full
catalog_complete to auto-update — the coherence gate's `_DERIVE_ALL_CAP=5000` rescues small changed-sets
and annual/current sources report no_change (coherence never triggers). statcan uses getChangedCubeList
(only changed cubes processed/run) so it's CI-viable. Test empirically in batch #3; catalog only if a
run actually reports coherence-unmet with >5000 changed series.

**Next:** promote batch #2 passers (unhcr, boe, bcrp, ofr, rba); batch #3 = worldbank_wdi + statcan +
insee_bdm re-prove; then the bulk_snapshot family (manifest sub-family via faostat pattern; conditional-GET
sub-family via rba pattern).
