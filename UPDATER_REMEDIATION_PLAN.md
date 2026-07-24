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
The 12 store-backed fetchers (`abs adb bls ecb eurostat insee_bdm insee_melodi istat scb sdmx_nso
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
  `transparency_ti`, `undp_hdr`, `kof_globalization`, `sipri_polity`, `swiid`, `ei_statreview`,
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
ons_uk owid pxweb rba riksbank sec_edgar_xbrl stats_nz ucdp un_wpp unhcr usda worldbank_esg
worldbank_extra worldbank_pink zillow`

- **Daily/weekly first** (`eia fed_board gleif nyfed riksbank cftc fdic sec_edgar_xbrl
  worldbank_esg`) — they go stale fastest.
- **Monthly next** (`bea bis boe census ember fhfa ilostat imf imf_fsi noaa ons_uk owid rba usda
  worldbank_pink zillow gus_dbw ipea insee_sirene`).
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
| Transient | bundesbank, cso, defillama, fred_releases, stat_slovenia | **by design** | Self-healing; data preserved, retries next tick | none (auto-retry once live) |
| Memory | vdem | real | 77M-row OOM, mislabeled "transient" | overwrite-mode + keep OFF CI (giant → workstation) |
| "dir missing" | abs, adb | **stale** | Already fixed by fcae3eb; stale recorded state | re-dispatch to clear |

**Landmine noted:** `hagstofa.py:398`, `ssb.py:472`, `stat_latvia.py:382` still carry the raw
`os.path.isdir` "source dir missing" pattern — they will fail in CI the moment they run there.
Patch (blob.list_parquets) when each is promoted.

**Net:** of ~20 "failing" sources, **most are stale state or by-design self-healing.** The genuine
code defects were treasury + scb + bfs (now fixed) and the CSV-coherence grain-alignment (careful,
next). The full agent transcripts: workflow wf_fc88e6a3 journal.
