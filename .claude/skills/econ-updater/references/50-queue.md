# Work queue + standing constraints — measured 2026-08-04

> The queue SNAPSHOT below ages. The live number is always
> `python tools/audit_schedule_coverage.py` — re-run it at the start of a cycle and
> refresh this file when the delta is material. The constraints are verbatim from the
> canonical files and do NOT age without an explicit decision from Ahmed.

## Standing constraints (verbatim from E:/research/econfindatalibrary/CLAUDE.md)

### §0 — DBnomics is BANNED (CLAUDE.md:3-28)

> **Do not fetch from DBnomics. Do not probe api.db.nomics.world. Do not build, keep or "temporarily" rely on a DBnomics-backed fetcher, relay, mirror or vintage signal. Do not run the DBnomics staleness audit as if it described a supported path.** Every source must come from ITS OWN PUBLISHER.

Why (CLAUDE.md:15-20): 98 of the 101 datasets ever taken from DBnomics have not been re-indexed in over 180 days (UNCTAD: 1,581 days), and its vintage signal is DBnomics' own hash, so a frozen dataset reports `no_change` forever while the health gate sees daily success.

What it means in practice (CLAUDE.md:22-28, quoted):
> - Existing DBnomics-derived data STAYS until migrated to the publisher — nothing is deleted by this rule. `who_hwf`, `who_rs`, `who_sdg` are the last three live relay fetchers and are to be MIGRATED to WHO directly, not refreshed via DBnomics.
> - Never add a new one. If a publisher has no usable API, say so and ask — do not reach for the aggregator.
> - Do not cite DBnomics coverage as evidence of anything about a source's freshness.

### §1 — Do not end a turn to report (CLAUDE.md:31-50)

> **Definition of done for any task here:** the change is committed, pushed, deployed where applicable, and VERIFIED against the live system — not "the code is written".

Only three things end a turn (CLAUDE.md:41-46, quoted):
> 1. A decision in the RESERVED list below (§2) that I am not authorised to make.
> 2. A hard external blocker — a credential I do not have, an account only Ahmed can create, a service that is down.
> 3. The queue is genuinely empty.

### §2 — Pre-authorised (do without asking) (CLAUDE.md:54-66, quoted)

> - Adding NEW source ids, catalog rows, fetchers, ingesters, registry entries
> - Deriving and uploading CSVs to R2; syncing catalog rows to D1
> - Flipping `SUPPORTED_SOURCES` **after** verifying the CSVs exist (derive → sync → flip; never flag-first, which turns a 501 into a 404)
> - Promoting a source to `live: true` once it proves in CI
> - Building/committing/pushing/deploying fixes to econdatalibrary + its worker
> - Restarting local crawlers, dispatching CI runs, adding audits and instrumentation
> - Fetching a source directly instead of via an aggregator, when measured to be equal-or-better coverage

### §2 — RESERVED (stop and ask) (CLAUDE.md:67-81, quoted)

> **RESERVED — stop and ask.** Each of these can destroy something a user has:
>
> - **Re-keying or retiring existing series ids** (breaks saved links, notebooks, MCP configs). Adding a parallel id is pre-authorised; changing an existing one is not.
> - **Deleting data, catalog rows, or R2 objects** — including "phantom" rows, until their absence upstream is verified.
> - **Auth / security / billing policy** (token lifetimes, key rotation, rate limits).
> - **Publishing anything to a PUBLIC repo that is internal** — MISTAKES.md, licence negotiations, operational notes.
> - **Sending email** or any outward communication under Ahmed's name.
> - **Switching a source to a feed that serves LESS than the current one** (e.g. IMF MCDREO direct has 57% of the relay's series; FM 9%).
>
> If a task is mostly pre-authorised with one reserved step, do the whole pre-authorised part first and surface only the reserved step.

### Other standing constraints

- **Reporting filter** (CLAUDE.md:83-89): report decisions needed, blockers, verified completions, and model-changing findings; never intermediate investigation, plans about to be executed, restated status, or a proposal that could simply be done.
- **Runbook-first diagnosis** (CLAUDE.md:91-115): a stopped source starts at `docs/runbook/<source_id>.md` (248 generated pages, index `docs/runbook/README.md`; regenerate with `python tools/gen_runbook.py --with-store`, never hand-edit). Three traps on every page: a `partial` never sets `last_success_utc` (R231); `obs_count` means "rows this run" on a productive run and "whole store" on a quiet one (R326); a FUTURE date is usually a legitimate projection — a defect is a SENTINEL (9999/2999) or a COUNTER, never judged by size (R327). In the 2026-08-04 all-sources audit the causes were `budget_deferral`, `code_bug`, `rate_limited`, `gated_by_design` — **zero expired credentials or dead endpoints**; `deferred (budget N min)` means nothing failed (R303).
- **Verification rules** (CLAUDE.md:117-137): read the MISTAKES.md Rules Digest, especially R0, before trusting any number you produced (R328 — sixteen entries with zero digest lines were invisible the same night). A green run is not a proof — read what it DID (R50); announce work BEFORE starting it (R70); verify at the surface the user touches — local catalog ≠ live D1 ≠ R2 (R60); run a known-good control before believing a negative (R52/R67); a test that cannot fail proves nothing (R64); names are an interface — grep every consumer (R66); a budget bounds only the failure mode it measures (R72).

## Licence gates & disputes (canonical file: E:/research/econfindatalibrary/DATABASE_LICENSES_VERBATIM.md — do NOT re-derive)

Header rule (line 5): single source of truth; a database is only cleared to re-host when terms **explicitly permit redistribution** AND the adversarial verifier CONFIRMED it; anything ambiguous stays gated.

Summary (lines 13-21): CLEARED-attribution 144, RESTRICTED-keep-gated 18, NEEDS HUMAN REVIEW 11, CLEARED-open 9, CLEARED-NC-only 6, CLEARED by written permission 2 (+1 scoped), verdicts CONFIRMED=184 / DISPUTED=7.

**The 7 DISPUTED verdicts** (needs-attention table, lines 37-58; per-provider detail at cited lines):
| Source | Verdict summary | Lines |
|---|---|---|
| bundesbank | non_redistributable use-only grant; metadata-only/link-out unless written permission | 38, 799-823 |
| faostat | NC + anti-endorsement + third-party carve-out (not plain CC BY) | 45, 1107-1125 |
| freedomhouse | data gated behind "FIW Data Request"; open re-hosting not authorized | 47, 1282-1294 |
| idb | CC BY-NC-ND, and ~86% of IDB datasets carry NO licence at all | 48, 1580-1609 |
| owid | mixed/source-dependent — majority is third-party data, not blanket CC BY | 51, 1991-2008 |
| worldbank | CC BY with third-party exceptions — embedded UN/IMF/WHO/ILO/IEA/UNESCO series may NOT be redistributed | 56, 2769-2788 |
| worldbank_pink | restricted — LME/Cotlook/SICOM/ICCO/ICO proprietary series; NEEDS-REVIEW pending per-series clearance | 57, 2994-3016 |

**RESTRICTED/permission_required (CONFIRMED, stay gated)** (lines 37-58): WTO (8 dbs), cboe, cow, damodaran (unclear_not_found), defillama, Energy Institute, Kenneth French, frankfurter (unclear), irena (unclear), nbp, polity5, shiller (unclear), sipri, tcmb, zillow. Plus **fred — RESTRICTED, keep gated** (3519-3566): mirroring and "essential experience" prohibitions apply to ALL use including non-commercial; serving only the public-domain tier does NOT rescue it; 48.2M local obs must stay uncatalogued/underived — "It must STAY that way. Do not catalogue, do not derive CSVs, do not add a fetcher or a registry entry" (3563-3564); deleting the local copy is Ahmed's call (3564-3566). Also an ML/LLM-training clause binding even internal use (3537, 3557-3559).

**Written permissions on file** (lines 25-31): comtrade (holdings must stay ≤100,000 records), kof_globalization (NC academic, cite KOF/ETH), whr — **OPEN gate**: "GRANTED in writing (Gallup/WHR) but SCOPED to the Figure 2.1 summary ONLY; currently re-gated pending trim to that scope" (line 31).

**Open / pending items:**
- **fdic** — NEEDS HUMAN REVIEW, keep gated (3441-3479): no licence stated anywhere; §105 public-domain inference explicitly rejected ("'probably fine' is not a quote", 3473); answerable by writing webmaster@fdic.gov (reserved: sending email).
- **istat, fdic, gleif** — RESEARCHER-ASSESSED single pass, "verdict field as pending that second reader" (3404-3407, 3450-3451, 3490-3491). istat itself CLEARED CC BY 4.0 (3397-3437).
- **gleif** — licence CLEARED (CC0, 3483-3517) but "the blocker here is SHAPE, not licence": entity registry, no series model fits; serving needs an entity-lookup surface = product decision (3512-3517).
- **un_wpp** — CLEARED CC BY 3.0 IGO locally, but **D1 still records NEEDS-REVIEW**; D1 must be aligned before un_wpp is ever hosted (3311-3314).
- **wid** — CC BY-NC-SA 4.0 (3113-3148): the CC BY text in the page source is COMMENTED OUT — do not cite it (3137-3139); SA means own licence row; plus a **currency condition** — Alice asked we "keep the most updated data sources", so WID must be wired to refresh, not snapshotted (3147-3148).
- **yale_epi** — CC BY-NC-SA; flags were corrected 2026-07-28 after being copied from another source's row (3151-3178).
- **iep (gpi/gti/ppi/etr)** — CLEARED CC BY-NC-SA via Ahmed's 2026-07-06 form grant (3215-3229); the earlier NEEDS-REVIEW verdict is superseded (3235-3264).
- **adb** — CLEARED via KIDB's own broader grant; attribution must carry the PRESCRIBED verbatim form "Asian Development Bank: Key Indicators Database Online (https://kidb.adb.org). Accessed on [insert date of access]." plus pass-through (3369-3393). ons_uk carve-out closed — exemptions are images/video only (3355-3367).

**etalab-2.0 date-condition precedent** (1541-1561): Etalab imposes THREE limbs — source attribution, **the date of last update of the data when known**, and no meaning-alteration; "Any future source under Etalab must satisfy all three limbs, not just attribution" (1555). Class sweep 2026-07-29 (1557): insee_bdm 101,789/101,848 dated, insee_melodi 134/139. **cepii_gravity — GATE CLOSED (1559): 1,143,250/1,143,250 dated (100%)** using the `Last-Modified` header CEPII serves on the exact hosted file (`Gravity_csv_V202211.zip`) → `2024-04-15`, "an observed publisher fact, not our fetch time and not a day invented out of a month" — writing "2022-11-01" from the V202211 stamp would have fabricated precision. Keep both facts: dataset version V202211, file re-issued 2024-04-15. Currency check (1561): V202211 is CEPII's newest release.

## Coverage headline (re-measured 2026-08-05 after loop cycles 1-2)

**127 of 224 sources / 10,021,703 of 11,388,693 series scheduled (56.7% of sources, 88.0% of
series).** cepii_baci SERVED + scheduled 2026-08-05 (90,582 pair-grain series, verify exit 0).

**D1 CAPACITY (measured 2026-08-05, supersedes every older figure): 9.31 GB of the ~10 GB hard
ceiling, 11,378,473 series rows, ~818 B/row effective. Practical budget for ALL remaining
cataloguing: ~400-500k rows. EVERY grain decision starts from this number now (task #45 holds
the next-tier options for Ahmed). The 6.9 GB / 1,647,600-row figures cited anywhere else are
DEAD.**

Tool's own caveat: "'scheduled' is a registry fact — live, adapter built, offered a turn. It is
NOT sub-unit coverage"; for sub-unit coverage run `tools/audit_untouched_files.py --live`.

## Work queue (97 served, licence-cleared-enough, not auto-updating sources / 1,366,990 series)

Key structural fact for ALL imf_* rows (updater/registry.yaml:5527-5543): the relay-era ids are DBnomics-shaped and IMF re-keyed its datasets, so no first-hand refresh preserves the old ids. The supported path is **parallel `imf_<flow>_direct` ids from api.imf.org** (pre-authorised: new ids alongside), never overwriting — "Overwriting imf_<flow> would re-key thousands of live series to buy freshness" (registry.yaml:5538-5540). 19 `imf_*_direct` registry entries already exist (registry.yaml:5544-5998).

### NEW-COVERAGE CANDIDATES (probe-confirmed live 2026-08-05, no legacy counterpart id)

Found by the IFS-families keyword sweep; each is a build (fetcher + registry + count bump +
serve), none supersedes anything. Ignore every dated *_VINTAGE snapshot flow.

| Flow | Version | Covers |
|---|---|---|
| IMF.STA:ER | 4.0.1 | DONE 2026-08-05 (cycle 16): `imf_er_direct` SERVED — 681 C.F table ids / 10,474 series / 2,427,666 obs (15x collapse, R356 arithmetic run explicitly), verify exit 0 |
| IMF.STA:EER | 6.0.0 | DONE 2026-08-05 (cycle 17): `imf_eer_direct` SERVED — SERIES grain (2x collapse makes tables pointless), 732 series / 179,736 obs, verify exit 0 |
| IMF.STA:LS | 9.0.0 | DONE 2026-08-05 (cycle 18): `imf_ls_direct` SERVED — SERIES grain, 4,160 series / 339,635 obs (supersedes IFS's 3,049 frozen L*), verify exit 0 |
| IMF.STA:PI | 2.0.0 | DONE 2026-08-05 (cycle 19): `imf_pi_direct` SERVED — SERIES grain, 3,100 series / 447,095 obs, verify exit 0 |
| IMF.STA:PI_WCA | 1.0.0 | DONE 2026-08-05 (cycle 20): `imf_piwca_direct` SERVED — 16 aggregate series / 2,168 obs, verify exit 0 |
| IMF.STA:QGFS | 12.0.0 | DONE 2026-08-05 (cycle 21 — the LAST actionable item): `imf_qgfs_direct` SERVED — TABLE grain COUNTRY x FREQ (mid-key positions 2-3 of 7), 267 ids / 20,502 series / 1,243,439 obs (77x collapse, #45 arithmetic), verify exit 0, worker 7d5c3e76. Sidecar auto-carried — `_carry_dims_sidecar`'s first live proof |
| IMF.STA:GS_LI | 1.0.0 | DONE 2026-08-05 (cycle 15): `imf_gsli_direct` SERVED — TABLE grain (mid-key positions 3-4 of 11), 233 ids / 80,394 series (345x collapse), verify exit 0; row left open by mistake when the ACTIONABLE entry closed |
| IMF.STA:MFS_IR | 9.0.0 | REGISTERED 2026-08-05 (cycle 5) — fifth MFS flow, Interest Rates |

### ACTIONABLE (26 sources / 996,987 series) — build/promote the publisher-direct sibling; legacy relay ids stay frozen

| Source | Series | Known notes |
|---|---|---|
| imf_dot | 101,000 | IMF RENAMED the flow: successor is IMTS (IMF.STA v1.0.0, "formerly Direction of Trade Statistics"). `imf_imts_direct` registered + heavy-matrix 2026-08-05 (cycle 2, #104); NO flow id contains DOT — do not search for one |
| imf_cpis | 100,783 | RENAMED: successor IMF.STA:PIP v5.0.0 'Portfolio Investment Positions by Counterpart Economy (formerly CPIS)'. imf_pip_direct registered 2026-08-05 (cycle 3) — NOT the World Bank's `pip`, never abbreviate |
| imf_ifs | 100,706 | MAPPED at topic level 2026-08-05: BOP 15,539 / MFS 31,156 / reserves 6,916 / trade 2,638 / GFS 4,343 / prices 1,339 all have SERVED direct successors, but IFS's own mnemonics (BCAXF, RAFA, TMG) were re-coded semantically — 0 root-code overlap in every family, so NO mechanical supersession is possible; legacy stays served-frozen. Families with NO successor flow yet: labor L* 3,049, production/GDP A*/N* 6,903, housing H* 7,558, exchange rates E* 5,918, numeric-prefix legacy 15,347 — probe IMF for ER/labor flows before calling them retired |
| imf_bop | 99,636 | `imf_bop_direct` already registered (registry.yaml:5664) — prove/promote |
| imf_cdis | 97,723 | RENAMED: successor IMF.STA:DIP v12.0.1 'Direct Investment Positions by Counterpart Economy (formerly CDIS)'. `imf_dip_direct` SERVED 2026-08-05 (cycle 4, #106): 776,752 series via 5,180 table ids, key is 5-part with counterpart FIRST (table dims at positions 2/4/5) |
| imf_mfs | 88,271 | DONE 2026-08-05 (cycle 5, #107): ALL FIVE successors SERVED — MFS_DC (539 ids/36,506 series), MFS_MA (468/3,016), MFS_OFC (231/4,704), MFS_FMP (207/276), MFS_IR (510/3,382) — 1,955 catalog ids / 47,884 series total, COUNTRY x FREQ grain (C.F.I saves nothing here: TYPE_OF_TRANSFORMATION ~1.02/combo). All five in the heavy matrix; legacy stays served-frozen (#46) |
| imf_irfcl | 54,126 | `imf_irfcl_direct` added 2026-08-04, vintage IRFCL:12.0.0 verified live (registry.yaml:5694-5715) |
| imf_gfsr | 52,055 | RESOLVED by coverage comparison 2026-08-05: gfsr is GFS REVENUE (not the stability report) and 61 of 74 of its bare G-codes are in the served imf_gfssoo_direct store as _T-suffixed successors (G111->G111_T). No separate revenue flow exists; the 13 unmatched are deep grant/memo detail SOO no longer carries. The gfse pattern: covered, no build. NOTE: QGFS v12.0.0 (Quarterly GFS, IMF.STA) is live and UNREGISTERED — candidate new build, ignore its *_VINTAGE snapshots |
| imf_gfse | 48,750 | Covered by `imf_gfssoo_direct` — GFS_SOO carries legacy gfse G26* codes; 475,049 series, live:false, **needs its own runner** (registry.yaml:5798-5825) |
| imf_gfsmab | 43,179 | Also covered by `imf_gfssoo_direct` (G11*/G12* codes; same runner blocker) |
| imf_gfsssuc | 36,901 | `imf_gfsssuc_direct` registered, live:false — needs own runner, too big for daily job's ceiling (registry.yaml:5854) |
| imf_gfscofog | 34,731 | `imf_gfscofog_direct` registered, live:false — same runner blocker (registry.yaml:5826) |
| imf_gfsibs | 29,390 | Corresponds to `imf_gfsbs_direct` (GFS_BS), live:false (registry.yaml:5889) |
| imf_cpi | 28,420 | `imf_cpi_direct` added 2026-08-04, vintage CPI:5.0.0 verified (registry.yaml:5716-5737) |
| imf_gfsfalcs | 20,249 | Corresponds to `imf_gfssfcp_direct` (GFS_SFCP), live:false (registry.yaml:5917) |
| imf_fsire | 18,620 | MEASURED 2026-08-05: 0 of 68 fsire indicator codes appear in any FSI-trio store at ANY decomposition level (exact, tail, segment) — NOT absorbed, and no matching flow among the 222 advertised. Joins imf_pgi in the no-successor class; legacy stays served-frozen |
| imf_psbsfad | 14,018 | DONE 2026-08-05 (cycle 7): `imf_psbs_direct` SERVED — 86 C.F table ids / 14,018 series (EXACTLY the legacy count, R75's same-dataset proof) / 209,229 obs, verify exit 0. Agency IMF.FAD. Legacy stays served-frozen (#46) |
| imf_pgi | 8,891 | NO matching flow in the 222 advertised (probed by name+keywords 2026-08-05) — probable full retirement (the G20 PGI initiative); candidate for the RESERVED/retired class, not a build |
| imf_bopagg | 7,801 | DONE 2026-08-05 (cycle 6): `imf_bopagg_direct` SERVED — 208 C.F table ids / 7,839 series / 140,907 obs, verify exit 0. 6-part keys with a sometimes-EMPTY phantom part; 2 GX aggregate codes unnamed (DIP class). Legacy stays served-frozen (#46) |
| imf_pctot | 4,320 | DONE 2026-08-05 (cycle 9): `imf_ctot_direct` SERVED — 360 C.F table ids / 4,320 series (EXACTLY the legacy count, R75) / 1,264,128 obs, verify exit 0. Agency IMF.RES. Legacy stays served-frozen (#46) |
| imf_unsdg_imf_inputs | 2,515 | DONE 2026-08-05 (cycle 10): `imf_sdg_direct` SERVED — SERIES grain, 2,577 series / 42,789 obs, verify exit 0. 16-part UN-SDMX keys; titles via the IAEG-SDGs codelists (dimension-level enumeration fallback + _T skip added to imf_direct_titles). Sidecar rename bug found: publish strands _staging_*.dims.json — fixed for SDG by R2 copy; class check pending |
| imf_pgcs | 2,262 | DONE 2026-08-05 (cycle 12): `imf_icsd_direct` SERVED — SERIES grain, 3,195 series / 155,840 obs (a coverage SUPERSET of the frozen 2,262), verify exit 0. First source whose dims sidecar arrived canonically via the carry fix. Legacy stays served-frozen |
| imf_namain_idc_n | 1,926 | DONE 2026-08-05 (cycle 11): `imf_namain_direct` SERVED — SERIES grain, 1,969 series / 86,928 obs, verify exit 0. 22-part ESA/SNA keys decoded via the carried dims sidecar + the _Z placeholder skip. Legacy stays served-frozen |
| imf_fiscaldecentralization | 8,398 | DONE 2026-08-05 (cycle 13): `imf_fd_direct` SERVED — SERIES grain, 8,398 series (EXACT legacy count) / 160,957 obs, 0 title fallbacks, verify exit 0 |
| imf_hpdd | 191 | DONE 2026-08-05 (cycle 14): `imf_hpd_direct` SERVED — SERIES grain, 191 series (EXACT legacy count) / 9,628 obs, verify exit 0 |
| imf_gender_equality | 295 | DONE 2026-08-05 (cycle 15): ALL FIVE GS flows SERVED — LGRGHTS 8,084 + LEPM 973 + SDO 7,599 + ATF 12,057 (series grain) + LI 80,394 via 233 C.F table ids (mid-key positions 3/4, the #45 arithmetic at a 345x collapse) = 109,107 series total, verify exit 0 each. Legacy pair stays served-frozen |
| imf_gender_budgeting | 288 | VERDICT 2026-08-05: NO successor — 0 of 24 GB_* codes in ANY of the five GS family stores (prefix-tolerant, positive control passed) and no flow matches by name. Joins fsire/pgi in the no-successor class; legacy stays served-frozen |
| imf | 131 | RECONCILED 2026-08-05 (cycle 21 close): the audit is CORRECT — the registry entry (registry.yaml:2581) has NO `live:` key (parsed and verified), is absent from the heavy matrix, and so was registered but never promoted. Promoting it would have jobs/ingest_imf_full.py re-pull whole dataflows over the store the 131 retained legacy ids resolve against — the #46 re-key class, RESERVED. Joins the served-frozen legacy set |

### HEALTH-GATE TRIAGE (classified 2026-08-05, cycle 22 — after the last ACTIONABLE build closed)

The daily runs had been red for 2+ days on 36 ATTENTION sources. Classified by NOTE (one
query), not source-by-source: 5 designed budget-slices (abs/dst/ecb/ssb/stat_estonia —
healthy, in-progress), 12 coherence, 19 assorted. Root causes fixed this cycle:

| Item | State |
|---|---|
| §5.7 punished partial catalogue coverage harder than zero (R359) | FIXED b7bee0d9/8dc5cbec: proven-uncatalogued residue = non-demoting `csv coverage note:`; zero-mapped-with-rows + derive failures still demote. Clears statfin/snb/imf_fas_direct/unesco_natmon/unesco_sdg/who_sdg as they re-attempt |
| defillama: NC grant advertised commercial_ok=1 for 20d (R358) + bare cursor keys froze all 24 served CSVs | **PROVEN GREEN** 2026-08-05 run 31054705327: `ok defillama/_all`, gate `OK defillama`, first CI success ever; coverage note carries the 2,645 dark-family keys honestly |
| bfs: cursor keys dropped the `BFS:` store prefix — 582/582 unmapped every run | **PROVEN GREEN** run 31054927114: `ok bfs/_all`, 581/582 mapped+derived, gate `OK bfs`; the 1 residual is a genuinely-new uncatalogued table |
| census: cursors were per-series store keys ('eits/advm3\|dim=val\|…') vs a table-grain catalog ('census:eits__advm3', split parts '#part') — 12,019/12,019 unmapped | FIXED cb051654 (cycle 23). The cloud "proof" was INVALID (R362 — keyless, 45 false breaks); the REAL proof ran 2026-08-06 on its designed route (run_local_heavy -Only census -Force): 45 flows walked WITH the key, ~47k tail rows merged+published (+23,283 statenaics, +17,075 enduse…), the new cursor code clean throughout — ended transient_fail on a census.gov connection reset at a late flow after 117 min. Merges stood; next pass's tails are tiny and complete the green. Follow-up CLOSED: the 22 legacy series-grain ids re-derived 22/22 |
| adb 8,686 / fhfa / imf_fsi* / imf_gfssoef/ssuc "unmet" verdicts | MEASURED stale: adb maps 3,000/3,000 against today's catalog — the 08-03 verdicts predate the R2 refreshes; self-clear on re-attempt |
| eia "49,998 unmapped" | The 7-row sliver catalog vs 3.8M-series store is #37's RESERVED table-grain decision; workstation route derive-all keeps its 7 served CSVs coherent locally |
| Stale-verdict cohort (adb, fhfa, imf_fsi*, imf_gfssoef/ssuc, usda, worldbank_wdi) | Self-clears: their catalog gaps were closed by the 08-04/08-05 R2 refreshes (R271 class); verdicts age out as the rotation re-attempts each (R277) |
| dst (#110, the R355 manifest fix) | DRAINED + MEASURED LEVEL 2026-08-06: backlog 2,317 → 73 due/day; publisher AUS07 newest = 2026M06 = our store max (66d obs_age is honest publication lag under its monthly data_cadence). Residual "1/73 transient" is ordinary retry churn — goes green on the first 0-transient run. #110 CLOSED |
| insee_bdm 201/201 + insee_melodi 129/144 transient-failed (verdicts 07-31) | RE-VERDICTED 2026-08-06 run 31058253388 (2h11m, within its 250-min budget): BOTH RECOVERED — the outage passed (endpoints probed alive). bdm merged +254,629 rows; its derive was budget-cut at 43,354/77,501 → which exposed R361 (below). melodi is pure budget-rotation now (15 attempted, none failed, 130 deferred) |
| stat_slovenia 1/2 "returned 200 but parsed 0 rows" (08-04) | ALL 4 CLASSIFIED from live SURS metadata + a reproduced boundary POST (cycle 28, ee9f6fe0): **2221405S** = REAL FIX — SURS pre-lists the next period ('2024') before publishing; the boundary body returns all-null values and `bool([None]*36)` read as "non-empty" → false structural every sweep; `_body_has_data()` now requires a non-null value (3 tests). **1506815S** (2002/2007 agri census) + **1012308S** (single-year 2012; category dim mis-flagged time=True) = archival, correctly 'confirmed'. **1517309S** = DELIBERATELY DECLINED: the fix would be the flagged-axis fall-through that core/pxweb.py refuses BY DESIGN — scb's Region codes (0114..2584, many inside the 1500-2100 sanity window) became years across 87,358 rows through exactly that door (R331), and no cheap guard reliably excludes that shape. One pig-forecast table is not worth re-opening it; if ever wanted, a per-table override, never a resolver rule |
| R361: csv_retry_queue was WRITE-ONLY — "retried next run instead of lost" had no reader; insee_bdm parked 43,354 | FIXED 2f868fa9 (cycle 27): run_once drains per source after the fresh derive (cap 20k/run), successes cleared + catalog-synced, refailures stay queued WITHOUT demoting (R359 must not return through this door). Under r2 an id drains when its file is next on the runner |
| cso: up to 222 matrices "no subject mapping" retried forever, ~45% of every budget (verdict 08-03) | **PROVEN** run 31073851494 (2026-08-06): mapping churn GONE — zero unroutable prints, +6,004,966 rows merged in one 34-min pass. Residual partial = 22 CIA*/CIS* derives "zero rows matched in 12 files" (the r2 file-locality class): queued, drains via the R361 reader when their subject parquets next rewrite |
| hagstofa 7/1096 perpetual "structural" (KOS*/CEN* archival event cross-tabs) | FIXED a35713fd (cycle 25): probed live — KOS03190 has NO time variable (Municipality/Age/Sex, year in title); stored max >=2y old now classifies frozen-archive 'quiet', recent max still structural |
| worldbank_wdi 10,255 unmapped every run | **PROVEN GREEN** run 31073851494: `ok worldbank_wdi/_all` (37-min pass) — indicator-grain cursors map |
| ~~WATCH: 06:00Z cron missed 2026-08-06~~ RESOLVED: the cron FIRED at 08:12:40Z (event=schedule, run 31083964702) — ALIVE, lagging 2h12m (heavy GitHub cron delay, not death) | The arriving scheduled run cancelled the manual replacement (31081010360) per the concurrency policy and performs the identical catch-up. Cycle 29's ofr diagnosis stands: its RED-DATA was the gap's measured cost (publisher 08-04 vs store 08-03, fetcher healthy) and clears in this run. Residual lesson: a 2h+ cron lag on a daily-cadence gate briefly reddens fast daily sources — expected, self-healing |
| fhfa bare-key cursors (07-30 verdict) | ESCALATED to cycle 30 (6ccab725): today's rotation DID run with the prefix fix, but the 50k cursor cap filled with annual_* series (2025-12-31) before hpi_master's monthly ones — a 218d phantom age on data measured LEVEL with the publisher (store 2026-05-01 = master yr2026 p5). Cursors now collect hpi_master-first. The forced rebuild went no_change (correctly — all 9 upstream files unchanged; --force overrides the cadence gate, not the vintage check), so the red persists as a DOCUMENTED PHANTOM (data proven level with the publisher) until FHFA's next release (~Aug HPI / Aug-26 quarterly) triggers the real rebuild and the fixed cursor order clears it permanently |
| owid 25,358 unmapped + GATED | Question: gated sources' coherence semantics — after R359 lands it goes green-with-note; the real question (should a gated store keep refreshing?) is Ahmed's |
| defillama per-chain + per-entity families (23 chain series data, protocols/yields) | The S1 fetcher refreshes only bulk aggregates; jobs/ingest_defillama.py owns per-entity and nothing schedules it — per-chain series data frozen ~June. Candidate: schedule it on the workstation route, or catalogue the bulk families (headroom decision #45) |
| eurostat "UNSTABLE 'LAST UPDATE=' series_key — run the one-time re-key" | #71/#80: the re-key is a SERVED-id change = RESERVED (#46 class) |
| Transient cohort (worldbank_esg 8/9, ipea 298/1491, idb 10/40, ember 4/48, ksh 1/60, sec_edgar 1/8) | ALL upstreams probed ALIVE 2026-08-06 (idb via its REAL CKAN endpoint data.iadb.org — a guessed URL 404'd first, the R61 trap): passed outages/throttles; rotation clears them under the R359-fixed green path. No work |

### RESERVED (71 sources / 370,003 series) — decision belongs to Ahmed; do not work

| Source | Series | Why reserved |
|---|---|---|
| unctad_* (38 ids: tabbapotta 29,358; rfia 24,720; gdpgbtoevbkoeatasa 21,158; sbtisvsaga 7,920; gasbtoia 6,776; fdiiaofasa 5,107; gasbeaiogasa 5,076; tabmcioeaiopa 4,250; tabmscioeaiopa 4,250; gasbtbia 3,402; sbeaiotsvsaga 3,010; gdptapccac2pa 1,734; soigapotta 1,226; taupa 898; rgdptapcgra 867; bopcaba 842; mpcadioeaia 816; lsciq 760; mttasa 704; cpia 637; mtba 584; srbca 414; sotwmfvbcoboa 373; reericba 352; mttgra 351; lscia 344; reerigdba 333; tabpcioeaia 308; neera 280; cpta 177; mfbcoboa 155; mmcascioeaiopa 86; fmcpa 50; cpa 50; cioiuibbicoeair4a 15; fmcpia21 14; wstbtocabgoea 8; ciocgeaia 8) | 127,413 total | Upstream (UNCTAD Data Hub) re-coded ids; DBnomics relay was 1,581 days stale (CLAUDE.md:18) and is banned; no unctad entry exists in registry.yaml; refreshing under new ids = re-key, RESERVED (CLAUDE.md:69-70) |
| imf_fsi | 73,288 | IMF publishes NO "FSI" dataflow; measured 2026-08-01 all 73,288 ids are DBnomics-shaped; three `imf_fsi{c,bsis,cdm}_direct` are the supported path — "Retiring or re-keying 73,288 live ids is the owner's call, not a build task" (registry.yaml:2678-2691) |
| imf_fas | 13,960 | Direct sibling LIVE (registry.yaml:5564); crosswalk ~0% (registry.yaml:5536-5538) — only the retire/re-key decision remains |
| imf_world | 2,268 | Direct LIVE (registry.yaml:5584); crosswalk ~0%; same retire decision |
| imf_fdi | 1,728 | Direct LIVE (registry.yaml:5544); crosswalk 95.3%; same retire decision |
| imf_afrreo | 1,654 | Direct LIVE (registry.yaml:5604); ~100% coverage; same retire decision |
| imf_whdreo | 322 | Direct LIVE (registry.yaml:5979); crosswalk 56%; same retire decision |
| imf_apdreo | 265 | Direct LIVE (registry.yaml:5624); crosswalk 100% by code; same retire decision |
| imf_cofer | 154 | Direct LIVE (registry.yaml:5644); crosswalk ~0% (currency moved into its own dimension); same retire decision |
| imf_fm | 1,356 | Direct feed has **9%** of relay's series — switching to a thinner feed is RESERVED (CLAUDE.md:77-78; registry.yaml:5540-5543) |
| imf_mcdreo | 1,095 | Direct feed has **57%** of relay's series — same reserved class (CLAUDE.md:77-78; registry.yaml:5540-5543) |
| unesco_clte | 23,868 | UNESCO culture/innovation 4 — Ahmed's call. Precedent: unesco_sci stays out because only 12/1,230 legacy indicator codes exist in the current UIS API (api/worker/src/util.ts:191-192); same currency question applies |
| unesco_inno | 18,909 | Same |
| unesco_film | 8,527 | Same |
| unesco_cltt | 6,226 | Same |
| fao_* (18 ids: ql 20,179; ga 15,018; ge 11,813; gt 10,506; gb 6,980; rp 5,440; gn 4,761; gl 3,057; gf 2,591; gy 2,491; ic 2,468; gr 617; es 595; ep 519; ew 169; ae 164; af 162; ec 49) | 87,579 total | FAOSTAT restructure question — Ahmed's. Precedent to reuse when he decides: fao_qcl went direct 2026-07-28 and DBnomics-era ids turned out to BE FAOSTAT's own codes, 98.2% reproducing exactly (registry.yaml:6061-6075) |
| hf_equities | 1,391 | HF equities family — not in registry, no fetcher, no state ever (docs/runbook/hf_equities.md); belongs to Ahmed's hfdatalibrary pipeline decision |
| insee_sdmx | — (not in queue; not served) | Needs full re-crawl: store unusable — 10.8M rows under 817 keys, all built from observation attributes (tools/derive_statcan_tables.py:53-55) |
| unsdg | — (not in queue; denylisted) | Licence CLEARED 2026-07-21 (DATABASE_LICENSES_VERBATIM.md:3106) but sits on the denylist safety floor (api/worker/src/denylist.ts:72); un-gating a denylist entry is Ahmed's |
| norgesbank | — (not in queue; denylisted) | Same: CLEARED NLOD 2.0 (DATABASE_LICENSES_VERBATIM.md:3105) but on denylist floor (api/worker/src/denylist.ts:64) |

Cross-check: 26 actionable + 71 reserved = 97 queue sources; 996,987 + 370,003 = 1,366,990 series ✓ (matches audit total).

COVERAGE: read lines 1-137 (complete file, 137 lines) of E:/research/econfindatalibrary/CLAUDE.md, last line read: `- **A budget bounds only the failure mode it measures** (time ≠ memory). R72.`
COVERAGE: read lines 9-63 and 3087-3566 (end of file) of E:/research/econfindatalibrary/DATABASE_LICENSES_VERBATIM.md in full, plus grep-targeted excerpts of lines 1541-1565 (etalab/cepii), 799-823, 1107-1125, 1282-1294, 1580-1609, 1991-2008, 2769-2788, 2994-3016 (the 7 DISPUTED details) and all `^##`/`^###` headers; the middle (lines 64-3086) was NOT read line-by-line — per task instructions only headers/GATE/DISPUTED/etalab were extracted from it. Last line read: `needed under a different arrangement.` (line 3566, end of file).
## IMF LEGACY RETIREMENT — EXECUTED IN FULL 2026-08-07 (permission granted ~21:40, all done by ~23:55)

**Class A COMPLETE: all 33 legacy sources retired archive-first, zero failures** (archives at
r2://econ-data/archive/retired/<src>/). Registry -4 total (hpdd, fiscaldecentralization, fsi,
imf; count 176→172), util.ts -33, deploy 1fd30232, all retired ids live-absent (present +
successor controls), coherence refresh 2026-08-07b with every shrink declared. whr UN-GATED in
the same deploy after its 178 tainted CSVs purged (SERVED, verify exit 0). **B2 ANSWERED: KEEP
ALL FOUR** (fsire, pgi, gender_budgeting, ifs remainder stay served-frozen — Ahmed: "just keep
them.. no need to crawl"). gfsfalcs still held pending its successor check. MEASURED RESULT:
D1 9.42 GB → 8.71 GB (10,535,584 rows) — the #45 split DEFERRED; coverage 70.5% of sources /
96.0% of series auto-updating. Original plan below for the record.

## (original plan) IMF LEGACY RETIREMENT PLAN (AUTHORIZED by Ahmed 2026-08-06: "no bookmarks... refresh to match publisher... I need a clean database")

Inventory measured 2026-08-06: 40 legacy (non-direct) imf sources, 1,122,144 catalog rows.
Retiring frees ~1M D1 rows — likely DEFERS the #45 split entirely (D1 hard cap is 10 GB/db;
storage past 5 GB bills $0.75/GB-mo ≈ $3.20/mo today; shrinks after cleanup).

**Class A — RETIRE NOW (full/superset successor live and proven):** dot→imts, cpis→pip,
cdis→dip, mfs→MFS×5, fsi→FSI trio, irfcl→irfcl_direct, bop→bop_direct, cpi→cpi_direct,
psbsfad→psbs (EXACT 14,018), pctot→ctot (EXACT 4,320), fiscaldecentralization→fd (EXACT
8,398), hpdd→hpd (EXACT 191), unsdg_imf_inputs→sdg, namain_idc_n→namain, pgcs→icsd,
gender_equality→GS×5, fas→fas_direct, bopagg→bopagg_direct, fdi→fdi_direct,
gfsr/gfse/gfsmab→gfssoo (61/74 measured; the 13 unmatched are detail SOO no longer carries
= publisher's current scope), gfsssuc→direct, gfscofog→direct, gfsibs→direct.
(gfsfalcs: verify its direct successor exists before including.) ≈ 25 sources / ~1.02M rows.

**Class B1 — BUILD DIRECT FIRST, then retire (live IMF dataflows, no direct built yet):**
CORRECTED 2026-08-06 (cycle 32, the R343 label-vs-system check): **weo and commodity need NO
build** — `imf_weo` (DataMapper API, live, no_change 2026-08-03) and `imf_commodity` (live, ok
+9,788 rows 2026-07-28) are ALREADY publisher-direct fetchers maintaining their own catalogued
ids; they retire nothing and nothing supersedes them. cofer/world/afrreo/apdreo/whdreo/fdi/fas
directs were already LIVE. The genuinely-missing builds were mcdreo + fm:
**mcdreo — DONE 2026-08-06 (cycle 32): `imf_mcdreo_direct` SERVED — 623 series, verify exit 0,
worker 21a17009, confirming run no_change.** **fm — DONE 2026-08-06 (cycle 34): `imf_fm_direct`
SERVED — 128 series / 5,077 obs (FM v5.0.0, IMF.FAD), verify exit 0, worker 0d011df0; adopted
from a concurrent session that died uncommitted, validated line-by-line before commit
(count 172→173). CLASS B1 IS COMPLETE.** Remaining non-IMF builds: whr primary-provenance
rebuild, unsdg + norgesbank rebuilds from upstream (their R2 residue was purged 2026-07-23 —
un-gating = a build, not a toggle), unctad 38 (surveyed cycle 33, blocked on Ahmed's free
UNCTADstat API key — see scratchpad survey), unesco 4. Also: imf (131, never-promoted entry)
folds into the retirement wave.

**Class B2 — PUBLISHER-DISCONTINUED, one-line keep/delete list for Ahmed (NOT re-crawlable):**
fsire (18,620), pgi (8,891), gender_budgeting (288), the ifs remainder (subset of 100,706
after the ER/EER/LS/PI families are subtracted). Deleting loses data that exists nowhere
upstream; keeping contradicts "clean". Awaiting his one line.

**unsdg REBUILD — IN PROGRESS (cycle 38, surveyed 2026-08-06; next cycle executes):**
norgesbank's twin (Ahmed's answer #5 authorizes the serve; UNdata licence CLEARED, ledger
line 771). Store purged 07-23; API probed live: Series/List = exactly 713 codes, current
release 2026.Q2.G.01 (the release tag is the vintage — the fetcher's content-hash design is
right). GRAIN DECIDED by #45 arithmetic: store keys are <seriesCode>:<geo>|dims (~353k
distinct — the old digest's "353,081 unmapped" number); catalogue at SERIES-CODE grain =
713 rows (~0.15% of headroom), the ilostat pattern; the key shape fits _resolve.py's
_FLOW_GRAIN prefix mechanism (flow id = seriesCode prefix). BEFORE re-registering, the
dead-code fetcher needs TWO fixes: (1) `codes = codes[:budget]` is an R190 fixed-prefix
truncation — add load_rotation/save_rotation + rotate_after (statfin pattern) so bounded
runs cover all 713 across ticks; (2) accumulate-then-merge is the R249 kill=discard class
under the 45-min cap (~713 GETs at 7-9s ≈ 95 min) — merge in chunks inside the loop, and
set a max_series default that self-bounds ≈30 min (~200 codes/run → 4 runs = full backfill).
Also label the bare tally calls. THEN: registry re-add (count 175→176 same commit), CI
backfill runs (~4 forced), flow-grain catalogue tool pass (713 rows + _FLOW_GRAIN entry),
derive at flow grain, refresh, D1, util.ts, denylist floor pin removal (unsdg is pinned —
same barro_lee precedent, Ahmed's authorization), deploy, verify.

**AUTHORIZED + EXECUTED 2026-08-06 (Ahmed: "yes, remove hf, owids, and trim whr" / "yes match
publisher for unctad unesco"):**
- hf_equities DELISTED: 1,391 metadata-only rows deleted from catalog.db + D1
  (tools/delist_source_rows.py — deletes rows, NEVER touches R2; distinct from retire_source.py),
  removed from util.ts, deployed 21a17009, live /v1/sources absence verified with present
  control. 0 R2 objects ever existed for it.
- owid DELISTED: 64 residual rows deleted from catalog.db + D1, removed from util.ts (deployed
  same version), live absence verified. The DISPUTED gated store on R2 stays UNTOUCHED; denylist
  entry kept. RESIDUE: its 40 orphaned series/owid%3A CSVs on R2 — the delete_objects call is
  classifier-blocked (same permission class as retire_source --apply); they are unreachable
  (source 501s) so cosmetic; sweep them when the retirement permission opens.
- whr REBUILT from PRIMARY provenance 2026-08-06 (cycle 35) and READY — un-gate BLOCKED on the
  R2-deletion permission. Fetcher rewritten to files.worldhappiness.report (newest Figure-2.1
  listing link, real ETag/CL vintage, NO OWID fallback); publisher 403s GitHub runners so whr is
  run_location: local (proven: local pass merged WHR26 13,397 obs / 1,749 series, state pushed).
  Catalogued 1,749 (whr-granted licence: reservable, NC, attribution), derived, D1-synced.
  RESIDUE (R364): 178 OWID-era CSVs on R2 under series/whr%3AWHR%3A (the derive walked both
  shards) — unreachable behind the 451 denylist; legacy shard quarantined at
  data/_quarantine/whr_owid_era.parquet. WHEN THE DELETION PERMISSION OPENS: purge the 178 +
  owid's 40, THEN remove whr from denylist.ts, deploy, verify 451→200 + verify_source_served
  exit 0. Serving before the purge would expose ungranted OWID-provenance ids.
- unctad (38 legacy ids): "match the publisher" CONFIRMED — build new-id successors from the
  UNCTAD Data Hub at current scope (surveyed cycle 33; blocked on Ahmed's free UNCTADstat API
  key); legacy ids then retire via Class A.
- unesco culture/innovation (4) — CLOSED 2026-08-06 (cycle 36) as PUBLISHER-DISCONTINUED,
  measured twice: (a) the current UIS API carries clte 21/408, film 1/76, cltt 0/34, inno 0/638
  of our indicator codes (unesco_dem.py's own measurement); (b) the publisher's bulk page
  (databrowser.uis.unesco.org/resources/bulk) lists NO culture/film/innovation file in the
  current 202602 release — only dated archives (CLTEARCHIVE-JUN2019, CLTTARCHIVE-JUN2021,
  FILM archive, INNOARCHIVE-APR2017, SCIARCHIVE-MAR2021). Our served 2022-era snapshots are
  equal-or-newer than every archive, so a re-ingest adds nothing. Verdict: the four stay
  SERVED-FROZEN as archival data (matching the publisher's own archival posture); no fetcher
  is built (a fetcher on a frozen archive transfers zero information — the R73 class). They
  join the no-successor set; deletion, if ever wanted, is re-crawlable from the archive zips.

**Retirement pipeline per source (tools/retire_imf_legacy.py, to build on the
purge_unpermitted_r2.py pattern):** archive primary parquet → delete catalog.db rows →
delete D1 rows → remove from util.ts SUPPORTED_SOURCES → registry retire + count bump
(same commit, R347) → R2 purge (series/ CSVs + clean_full/ store, terminated prefixes,
guard imf_*_direct!) → wrangler deploy → live /v1/sources absence check → refresh_r2_catalog
→ coverage re-measure. First batch: the EXACT quartet (psbsfad, pctot,
fiscaldecentralization, hpdd) to prove the pipeline.

## cso / ons_uk / insee_melodi — resolver FIXED; the residue is staleness, not absence

**2026-08-07 (cycle 38). Read the correction before acting on the commit message**: commit
5e938746 said these three sources' "downloads were broken". That is WRONG and is logged as
R371. What was measured, and what is true:

- The client resolver DID return 0 rows for all three (cso 7,896 ids, ons_uk 42,
  insee_melodi 139; positive control stat_latvia returned 42). cso had no resolver entry at
  all; ons_uk/insee_melodi put dataset identity in the FILENAME. Both fixed — cso joins
  `_FLOW_GRAIN` (7,606 of 7,896 natives now resolve against the FULL R2 store),
  ons_uk/insee_melodi get `_resolve_file_grain` (42/42 and 139/139).
- **Users were never cut off.** The Worker serves PRE-DERIVED CSVs from R2, not the
  resolver. All 7,896 / 42 / 139 CSVs are present with real content (medians 14 KB /
  2.9 MB / 1.2 MB, zero header-only), last written 2026-07-29.
- **The real defect is FROZEN DATA.** The daily derive is what goes through the resolver, so
  "csv_derive failed 22/22" meant those CSVs could not be refreshed — stale since 2026-07-29,
  not missing. The fix restores refreshability; confirm on the next scheduled cso run that
  the csv_derive failure count drops to 0 and the CSVs get a newer LastModified.

Open, measured, not yet actioned:

- **cso: 290 catalogue natives have no rows in the R2 store** (7,606 of 7,896 resolve). They
  still have CSVs on R2, so they serve a snapshot the store can no longer regenerate —
  orphaned relics of the pre-#78 matrix repair. Decide per matrix: re-fetch from PxStat, or
  delist the row AND purge its CSV. Do not leave a served CSV whose provenance is a
  superseded store.
- **insee_melodi coverage: we hold 79 of the publisher's 145 dataflows** (live
  `/dataflow/all`, measured 2026-08-07 — 132 DS_*, 13 DD_*). 66 are a fetchable gap; the
  fetcher enumerates every flow each run but is budget-bounded, so confirm it is actually
  rotating toward them rather than re-tailing the same 79. 5 store files name flows the
  publisher no longer lists (frozen archive, fine).
- The earlier "insee_melodi: 55 catalogue rows with no store data" was FALSE — a LOCAL disk
  listing (84 files) for a cloud-backend source whose R2 store holds all 139. R366/R371.

### cso batch ordering + the 290 holes (cycle 38, 2026-08-07)

Measured, all against the live publisher and the real store:

- CSO publishes **12,908** matrices. Our store holds **7,608**. The revision cursor
  (`_collupd.json`) held **61** — it restarted empty when `_write_cursor` was blob-routed on
  2026-08-03 — so nearly everything looked "changed" and newest-first re-pulled matrices we
  already had.
- A cso run is **2,017.8 s for 60 matrices** (~34 s each) against a 45-min cap, so the bound
  is TIME. Raising `MAX_TABLES` cannot help.
- Of the **290** catalogued matrices with ZERO store rows, only **4** were in the next 60 and
  the last sat at queue position **11,945** (~200 runs).

SHIPPED: `_held.json` (what the store can serve, seeded 7,608 by `tools/seed_cso_held.py`,
confirmed on R2, extended per run from `pulled_ok`) + `order_changed()` — unheld first,
newest within group. Re-measured: orphans in the next 60 go **4 → 31**, last orphan
**11,945 → 5,142** (~86 runs). A 2.3x improvement, NOT a fix.

RUNNING: targeted backfill of the **263** orphans CSO still publishes, via
`CSO_ONLY_MATRICES` (~2.5 h in one pass). The other **27** are gone from ReadCollection
entirely and cannot be re-fetched: `A0207 A0208 A0209 B0207 B0208 B0209 B0212 C0424 C0427
C0429 C0438 CD820 E1004 E1018 E1033 E1036 E1037 E1038 E1039 E1042 E1043 E7043 NAA02 NAA03
NAA04 NQQ34 NQQ38`. Their CSVs still hold real data on R2, so they are served-frozen
archival (the unesco-culture verdict) — but their parquet download has no rows behind it,
which is a product inconsistency to settle: either delist the 27 or accept CSV-only.

### insee_melodi coverage (cycle 38, 2026-08-07)

The publisher's `/dataflow/all` lists **145** flows (132 `DS_*`, 13 `DD_*`); we hold **79**.
So **66** are a fetchable coverage gap, and 5 of our files name flows the publisher no
longer lists. The fetcher enumerates every flow each run but is budget-bounded — confirm it
is actually rotating toward the 66 rather than re-tailing the same 79 before assuming it
self-heals.
