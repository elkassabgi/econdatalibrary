# ECONLIB COMPLETION PLAN
## A systematic, mistake-proof programme to finish econdatalibrary.com — prepared for Claude Code

**Prepared from:** `ECONDATALIBRARY_COMPLETE.md` (the complete system document, 8,513 lines, dated 2026-08-30).
**Target repos:** `E:\research\econfindatalibrary` (econ), `D:\research\hfdatalibrary` (hf — holds the ledger, NUMBERS.md, hooks, and the adversarial-review skill).
**Owner:** Ahmed Elkassabgi. Anything marked **RESERVED** is his decision. Never decide it for him.
**Companion artefact:** the `econ-completion` skill (`econ-completion-skill/`) — install it into `.claude/skills/` in BOTH repos before starting. This plan says **what** to do; the skill says **how** you are required to work while doing it. The skill wins where they differ.

---

# PART 0 — READ ME FIRST (the three sentences that govern everything)

1. **The database is mostly right; the recurring danger was the previous AI.** 519 recorded mistakes (R0–R519) show a consistent pattern: measurements whose *shape* did not fit the question, reported as facts. Every protocol below exists to stop that pattern, not because the data is broken.
2. **Prose rules did not hold; a second agent and a runnable check are the difference.** A rule that exists only as a sentence has zero enforcement value in this project (R58, R279, R374, R383). Every rule you add must ship with a mechanical check in the same commit — a discriminating pair, a hook, a test, or a reviewer.
3. **"Done" has one definition, enforced in the skill:** catalogued + served (live 200 from the running endpoint) + verified against the publisher where a value is claimed + on a schedule + licence-verified + recorded in the ledger/NUMBERS.md. A green run is not a proof (R50). A commit is not a deploy (R345). A local file is not the serving surface (R461).

---

# PART 1 — CURRENT STATE (the baseline you are completing from)

Every figure below is dated 2026-08-30 and has a named instrument. Re-measure before acting on any of them; treat them as the *map*, not the *territory* (ledger R509: never build on a previous session's conclusion without re-measuring).

## 1.1 Scale

| Fact | Value | Instrument |
|---|---|---|
| Served sources | **322** | `py tools/audit_schedule_coverage.py`; cross-checked `SELECT COUNT(*) FROM source_counts` on both D1 DBs (321 + 1 noaa shard) |
| Catalogued series | **13,486,342** | audit + independent PK-range sweep (both agree exactly) |
| Served observations | **33,908,707,379** | `logs/stats-2026-08-26.json` (census over the served store) |
| Sources scheduled for auto-update | **270 of 322** = 83.9% of sources, **97.5% of series** | audit |
| Unscheduled = archival (publisher retired) | 52 | audit — each with a dated per-source finding |
| **Actionable scheduling work** | **0** | audit |
| Registry entries | 282 (229 `live: true` — NOT a count of anything scheduled; four scheduler paths exist) | `updater/registry.yaml` |
| Local catalogue | 349 `source` rows (322 with >=1 series + 27 empty), 71 licence rows | `catalog.db` |
| Local store | 345 GB, 98,785 files, 430 source dirs | os.walk sweep |
| D1 shard | `noaa` lives in `econ-catalog-climate` (primary hit 9.35/10 GB) | `util.ts`, `wrangler.toml` |

## 1.2 The open work — the actual reasons this plan exists

### W1 — The key-collision family (largest open data defect)
A `series_key` that drops a dimension the publisher varies, so distinct series collapse onto one id. Health gates cannot see it (a collision looks like a healthy source with more observations per series). Confirmed, served, measured:

| Source | Served ids | Dropped dimension | Worst evidence |
|---|---|---|---|
| `eia` | 268,502 | frequency (`.A/.M/.Q/.D/.D.H`) | **142,073 ids (52.9%)** gather >1 publisher series; `ELEC.PLANT.GEN` serves a **566 MB** object bundling ~172k series; `EBA.AEC-ALL` mixes hourly demand with day-ahead forecast |
| `idb` | 18,838 | everything but dataset+country | one id returns 2,805 rows over 13 dates |
| `unctad_tradefoodproccatprocrca` | 19,087 | `Flow` (Imports/Exports) | publisher `$metadata` names `Flow` in the Fact key |
| `unctad_tradefoodproccatcatrca` | 17,617 | `Flow` | 286,038 publisher two-flow cells = 286,038 duplicated pairs, zero residue |
| `damodaran` | 24,687 (721 collided) | worksheet name | publisher workbook: India Adj. Default Spread = 0.0209, **we serve 0.3** (its corporate tax rate); rating ladder breaks monotonicity at 9/19 steps |
| `bea` | 913,230 | (minor: 49,856 pairs = 0.074%) | per-file, cross-file is by design |
| `defillama`, `istat`, `GATED` | served | smaller | see §10.3 of source doc |
| `GATED`, `GATED` | 0 (gated) | various | — |

**The five giants unswept** by the fleet duplicate sweep: `statcan`, `eurostat`, `cbs_nl`, `oecd`, `ilostat`. The census is **not final** until they are measured. 13 stores are unmeasurable by the generic sweep (key on `series_id` or custom columns) and were partially measured separately (`bls` 282,931 conflicting pairs but only 9 catalogued ids; `ofr` clean).

**Systemic cause:** 146 of 212 ingest jobs write parquet directly; **zero** route through `merge_and_write` (the dedup/never-shrink/impossible-date layer every fetcher uses). Stores were born without the invariant the maintenance path assumes.

**Why not fixed yet:** every remedy is a **re-key that changes PUBLIC series ids** → **RESERVED for Ahmed** (precedent R275/R276). Your job is to prepare per-source decision briefs (design, cost, migration plan, rollback, id-stability guarantees) and execute only after his written go.

### W2 — Updates reach the store but never the user
- **73,125 changed series keys across 20 sources map to no catalogue id.** Leaders: `eia` 50,000 (exactly `CURSOR_CAP` — a cap, not a count; true number larger; its latest run banked **+235,050,106 rows** and delivered none), `GATED` 12,192, `GATED` 6,513.
- **231,782 series in `csv_retry_queue`**, oldest 12 days. **Every row is at attempts=1 — nothing has ever reached a second attempt.** 183,735 of them are hard `UnitTimeout` crashes (not the designed budget-deferral path); `abs` at exactly 100,000 and `ilostat` at exactly 50,000 are suspicious round numbers (cap artefacts?) — **establish** with `SELECT enqueued_utc, COUNT(*) … GROUP BY 1`.
- **~56 live+served sources have never returned `ok`** (perpetual `partial`), so their CSVs were never re-derived on schedule (`worldbank_esg` served 2023 values while the store held 2024 — fixed only by forcing derive on partials).
- **11 fetchers (`norgesbank` + 10 more) compute their "changed" set from disk before any network call**, violating the orchestrator's contract — their `series_cursors` mean "everything", so derives run 32x over-cost or never converge.

### W3 — Freshness & coverage gaps
- **26 of 229 live sources hold files their fetcher has not rewritten** (`bea` 591/592, `dst` 594/813, `defillama` 94/112, `adb` 32/54, `cso` 32/57, `abs` 444/1222, `worldbank_esg` 13/93, `ecb` 17/540…). Each needs a dated, evidence-backed attribution: retired flow / ingester-owned tree / static reference / genuine gap. **Not yet attributed — real open work** (`tools/audit_untouched_files.py --live`).
- **`eurostat`: 440 catalogued flows serve nothing, and 540 store files disappeared while the source was frozen 45 days.** Cause NOT ESTABLISHED. Investigate before touching anything.
- **`oecd`: 60 of 131 flows have no `TIME_PERIOD` column** — cross-sectional data outside the series model (same shape as `gleif`). Serve or formally exclude → **RESERVED product decision**.
- Gate has **no tolerance for a bounded known-broken minority** (`bfs` 649/650, `hagstofa` 1538/1568, `stat_slovenia` 95/97 permanently red) → gate policy → **RESERVED**.
- `norgesbank` **un-gating provenance**: its R2 objects were deleted in the 2026-07-23 purge as "gated, 0 catalog series", then catalogued 2026-08-06. Confirm authorisation **before** anything publishes 35,135 new objects → **RESERVED**.
- `GATED`: 26 residual catalogue rows while gated (the one "gated-but-present" source the 07-23 purge policy otherwise eliminated). Check whether they exist in D1; bring to Ahmed with a recommendation (purge vs keep) → **RESERVED**.

### W4 — Catalogue & search integrity
- **`series_fts` holds 2.00x the series count** (26,981,683 rows): 1,052,814 orphans + 12,442,585 surplus duplicates (`wid` 7,395,591; `boc` **8.00x** → a search page is 84% repeats). Repair is known: PK-range statements only, **never** per-id statements on the UNINDEXED `series_id` column (23,843,482 rows/statement; an `IN` of 200 costs the same) — and the survival test must prove the *kept* rows carry the real **titles** (R488: proving on `series_id` is proving on the one column FTS ignores).
- **`source_counts` drift** (R489): `vdem` had no cache row (live `COUNT(*)` of 783,100 rows per page view — the $82/day shape rebuilt), `ilo` advertised `total:1157` with `results:[]`. Reconcile all 322; every direct D1 write must refresh the cache in the same operation.
- Stale strings worth fixing (one-liners, no data risk): `catalog_coverage: "series-level for 33 sources…"` (`catalog.ts:19`, actually 322 served); `SUPPORTED_SOURCES` header comment "The 191 sources" (array holds 323); `updater-daily.yml` comment drift (285/335/270 vs real 305/355/290). Remember: **the Worker deploys manually** — fixing the string in the repo changes nothing until `npx wrangler deploy` + live check.
- `/v1/sources` per-request cost **NOT MEASURED** (349 correlated `EXISTS` probes against unindexed `series.source_id`, not edge-cached). Measure once with `meta.rows_read`; if it's millions, materialise or edge-cache. This is the documented instrument, exactly one query.

### W5 — Ledger & reliability-system defects (fix these FIRST — they protect everything else)
- `ledger_check.py --digest` matches only `^## (R\d+) — ` headings (**100 `###` entries invisible**) and exempts by id cutoff `RULE_FROM=475` while **R435–R466 are dated 2026-08-19→08-24, after the rule existed**. 147 entries lack digest lines. Fix: regex `^#{2,3} (R\d+)\b` + date-based cutoff from the `M-YYYYMMDD` tag. (Named in the source doc as a five-line change; do it, test it, and add the discriminating test that proves it now FAILS on the current file's gap.)
- The doc's own stale-claims list: archive line count "8,600" (real 13,492), `CLAUDE.md` "150+ entries" (real 519 rule ids). Fix the docs when you fix the checks.
- R504's open mechanism, R487's 50,000 boundary cause, R506's duplicate origin — listed as NOT ESTABLISHED. Do not re-litigate them; record them as open questions and move on.

### W6 — Public-facing honesty items (each ends in a decision brief)
- `/v1/stats` serves the **July census (79.8B obs / 7.73B series)**; the measured store is **33.9B / 3.90B**. The census tool's >20% gate refuses to publish without `--force-publish`. Publishing the honest number is **Ahmed's Phase-4 decision** — prepare the one-page brief (both numbers, why they differ, what the site would show).
- Homepage claims "Python and R clients available" — **no econ R client exists** (`clients/r` is hf's). Either build one (then verify live) or change the site copy + `.zenodo.json` + `STRATEGY.md`. Copy change is trivial; claiming is not.
- `GATED`: gated in the Worker but its local licence row says `reservable=1` (cc-by-3.0). One of the two records is stale; settle from `DATABASE_LICENSES_VERBATIM.md` (the single source of truth — do NOT re-derive) and D1, then reconcile.

### W7 — Still-running jobs (do not disturb; verify by artefact only)
- `statcan` derive: ~8,200/8,207 tables; last census tables ~2–3 h each; parquet re-upload queued behind it.
- Fleet duplicate sweep: 416/430 stores measured; five giants outstanding (see W1).
- `cbs_nl` / `gus_dbw` crawlers: long-running by design; watch via `_modified.json` / `_refresh_state.json` artefacts, never by process listing alone (R453).
---

# PART 2 — THE CONSTITUTION (binding rules; each with its enforcement)

These are distilled from 519 ledger entries and the §8/§9 analysis. They are repeated in the skill where they bind an action. **Every rule ships with its enforcement, or it is not a rule.**

| # | Rule | Why (ledger evidence) | Enforcement |
|---|---|---|---|
| C1 | **Every number carries its instrument and date.** A figure without a named command/query/tool is not a measurement. | R0; NUMBERS.md exists for this; headline drift "77B → 30 → 20" had no instrument | Every figure you report gets a `NUMBERS.md` row (claim / number / instrument / date / note) in the same commit. Corrections are appended as new rows naming the retraction — never silently edited. |
| C2 | **A claim about the running system is settled only by the running system.** "The registry says X" ≠ "X"; "the log shows N" ≠ "N rows fetched"; a commit ≠ a deploy. The Worker deploys MANUALLY (`npx wrangler deploy` from `api/worker/`); the site publishes MANUALLY (`workflow_dispatch` of `deploy-site.yml`); pushing to GitHub publishes **nothing**. | R345 (425,462 series called SERVED while the worker was 2 weeks undeployed), R408, R432, R461, R509, Class D | The skill's Claim Ladder: before saying "served/live/gated/complete/verified", name which surface you probed and paste the live response. A hedge goes **inside the sentence**, not in a caveat below it. |
| C3 | **A probe that reports absence is VOID until it has been run against something known PRESENT — and the control must be in the probe list, every time.** | R0 sub-rule 4; R134/R316/R338/R433/R478 (the `id` vs `source` key, five times); R484 (five tools, one session, all reader bugs) | Skill checklist "Null-result protocol": positive control in the same run + print one raw record beside every count. A round number in the all-failed direction (0 of 61, 796,716 of 796,716) is a *reader-bug alarm*, not a finding. |
| C4 | **A green run is not a proof.** Require positive evidence of work: units processed > 0, rows counted, artefact moved, and the consumer's path tested — not the writer's. | R50, R35 (4 days green, 2/113 processed), R380, Group D (48 entries) | Every run you report must quote the run's own work counters; a "0 units processed, exit 0" is a FAILED proof. Verification tests the path the user/pipeline actually reads (R491: titles correct, search index still bare keys). |
| C5 | **A guard ships with a discriminating pair** — one case it MUST block, one it MUST let through — in the same commit, and the guard's `except` branch IS the guard ("cannot measure" must refuse, never pass). | R414 (guard refused every seed), R488 (proof passed identically on the catastrophe), R501→R503→R508 (three fail-opens in one week), R492 (`ledger_check --titles` PASS on a destroyed index) | Every guard/check/test you write carries its two cases, proven failing/letting-through, and a guard without a denial path is rejected in review. |
| C6 | **A reported example is one instance of a class.** Sweep the whole surface; the zero-result check is not confirmation, it is what *defines* the work. | Ahmed's standing rule; R95 (ten more licence rows, 105,301 series), R256 (hand-listed class, grep found 2 more = 40% more work), R390, Group G | Fix the instance, then enumerate by `grep -l '<the exact defective line>'` (or structured query) across **every** executable surface (`*.py,*.ps1,*.cmd,*.yml,*.sh,*.ts`), fix all, and commit the enumeration. |
| C7 | **Destructive operations are a different species.** Plan the DESIRED END STATE, make every writer idempotent, print the delete-set before deleting, guard the POST-state, and the licence/gate authority is `DATABASE_LICENSES_VERBATIM.md` — never a diff between stores (a diff finds disagreement, never shared error). | R10, R107, R263, R503 (863,253 rows deleted outside approved scope), R117, R519 (603,467 rows nearly destroyed) | Skill "Destructive-op protocol" (full checklist in references). Destructive work requires the parallel adversarial review BEFORE the write, and review of the post-state AFTER. |
| C8 | **D1 is metered. Cost is a correctness property.** Every public-request query against a multi-million-row table must be O(page). `series_fts.series_id` is UNINDEXED (23,843,482 rows per statement); cost is per STATEMENT — raise predicate arity (PK ranges `[src+':', src+';')`), never add statements. Measure ONE statement and read `meta.rows_read` before any batch. | R430 ($82/day), R492 (3.93e12 rows ≈ $2,500 planned, caught at $0), R309b, R502/R505/R507/R508 | Existing hooks (`d1_cost_guard.py`, `cost_banner.py`) + skill checklist before every D1 batch. `SELECT SUM(n) FROM source_counts` is instant and uncounted — prefer it for counts. Desktop-first: decide locally, verify remotely. |
| C9 | **A causal story is not a finding.** Report the observation at full confidence and the cause at the confidence its test earned. Before reporting a cause, write down the observation that would refute it, and run that. Corroboration counts independent vantage points, not observations. | R514 (three stories, one session), R512 ("IP-blocked" — host down for everyone), R504 ("what it actually was" refuted at 0/40), Class L | Skill "Diagnosis protocol": observation / hypothesis / refuting test / result — all four written before the claim is spoken. |
| C10 | **The error asymmetry: discount alarming findings ~3:1; treat every green as unmeasured.** Errors that inflate get spoken and corrected; false all-clears stay silent until the owner, a user, or the invoice finds them (R421, R430, R404 — the three most expensive incidents were ALL false all-clears detected outside the system). | §8.4–8.5 analysis (28:10 owner-facing; 17:14 recent block) | When you conclude "X is broken", first ask "what did the instrument actually measure?" When you conclude "X is fine", ask "what check would have gone red, and was it ever fed a failing case?" |
| C11 | **Ledger discipline.** Every mistake → archive entry + digest line in the SAME commit (mechanically enforced by `ledger_check.py --digest`); every correction appended, never rewritten; ledger writes are append-only at an anchor, `git -C D:/research/hfdatalibrary`, never `git add -A` in a repo whose tree doubles as a staging area. | R328/R485 (entries with no digest lines, twice), R247/R352 (concurrent clobber), R135, R370 (37,778 CSVs staged) | `ledger_check.py` + the fix in W5 + the skill's ledger protocol. |
| C12 | **Adversarial review runs in PARALLEL with everything consequential** — brief → challenge → do → verify → record. The reviewer's job is to find the flaw, not approve; FAIL/REDIRECT is a reviewer success. Reviewer-authored ledger entries are adopted without challenge (higher bar, R487's note). | Ahmed's standing orders (2026-08-24, 2026-08-29); 17+ entries reviewer-caught, including the $2,500 plan, the R488 delete, the R519 remedy | A second Claude session on every design/deletion/derive/deploy plan. Details in skill references. Do not proceed on a FAIL without re-briefing. |
| C13 | **One source at a time, end to end.** Read its runbook, grep the ledger for its id, read its fetcher header, its registry entry, its licence verdict — then work. A source is DONE only when: catalogued + served + verified live + scheduled + licence-verified + recorded. | The only thing that has worked (§5.2 of the source doc); R158 (shipped without updater), R268 (task closed on the part being looked at) | Skill "Source protocol". Per-source completion record appended to the plan's work log. |
| C14 | **Reserved decisions stop the work, and only Ahmed releases them.** Deleting non-re-crawlable data · un-gating a DISPUTED licence · auth & billing · sending email as Ahmed · **any change that alters PUBLIC series ids** · the `/v1/stats` publication · gate policy · cross-sectional serving policy. | R460 (asked 3x then decided myself — reverted), R500 (asked to delete 35 live rows on a stale premise), R275/R276 | Skill "Reserved-decisions protocol": prepare a one-page brief (facts re-measured, options, recommendation, cost) and stop. Never un-gate via the permission system (R250). |
| C15 | **Shell, files and channels.** Code goes in files via the file tool (never heredocs into bash). Never pipe a watched long job through `tail`. PowerShell scripts are ASCII-only or UTF-8-with-BOM. Long jobs run with `-u`, `PYTHONIOENCODING=utf-8`, and are judged by the ARTEFACT they move, not by process listings or log silence. `git push` verified by `git rev-list --count origin/main..HEAD == 0`. | R92, R336/R348, R196, R453/R454, R445, R476, Group K (18 entries) | Skill "command-line composition checklist" (syntactic triggers checked at composition time, because that is when the attention is on the shell, not the rulebook). |
| C16 | **A fix to an assumption is a class sweep.** The moment you fix an assumption (a path, a flag, a key name, a backend read), grep for it repo-wide and put the grep in the commit. Fixing one of N paths and reporting it as the total is the single most frequent self-inflicted error (R390, R428, R333, R262, R411). | Group G (16 entries); the "four scheduler" saga | The grep is part of the definition of done for any fix. |
| C17 | **Never build on your own ledger line, digest line, or memory without re-measuring.** Previous-you's conclusion is the citation you are least likely to challenge. | R509 (fix built on own digest line; premise false), R411 (fluent figure, wrong), R230 (compaction summary inherited) | Treat every inherited claim as a hypothesis; the re-measure is the first step of the task that uses it. |
---

# PART 3 — THE COMPLETION PROGRAMME (phases, tasks, gates)

Execute strictly in order. A phase is **not complete** until its exit gate passes. Each phase lists: objective, tasks, outputs, exit gate, and the reserved decisions it feeds. Track every completed task in a `WORKLOG.md` (append-only, one line per task: date / task / instrument / result / ledger ref if any).

> **Global rhythm (every working session, before any task):**
> 1. Load the skill (it auto-loads; if not, `/econ-completion`).
> 2. Re-read this plan's phase header for the phase you are in.
> 3. Run the mechanical checks: `ledger_check.py --digest --counts --numbers`, `git status` both repos, `gh run list` for red workflows in repos you will touch (R421).
> 4. Update `WORKLOG.md` with what you intend to do this session.
> 5. Start the parallel adversarial reviewer for anything consequential before building it.

## PHASE 0 — Install, baseline, and fix the instruments that watch everything else

**Objective.** Put the reliability system on a sound footing and capture a re-measured baseline, so every later phase stands on current numbers.

**Tasks.**
1. Install the `econ-completion` skill into `.claude/skills/` of BOTH repos (see `INSTALL_AND_HANDOFF.md`). Verify it loads.
2. Fix `ledger_check.py --digest` (W5): heading regex `^#{2,3} (R\d+)\b`, date-based cutoff from `M-YYYYMMDD` tags, plus a discriminating test (must FAIL on today's file until the backlog is backfilled or explicitly whitelisted). Backfill digest lines for the 147 missing entries — mechanical, one commit per ~20 lines, checking the count after each append (R247).
3. Fix the stale doc numbers named in W5 (`CLAUDE.md` "150+ entries" → actual; digest header line count).
4. Re-run and record the baseline instruments into `WORKLOG.md` (all read-only, local or the single allowed D1 measurement):
   - `py tools/audit_schedule_coverage.py`
   - `py tools/audit_untouched_files.py --live`
   - PK-range sweep counts per source (the 1.3 s form) — or re-read the shipped tool output
   - `csv_retry_queue` breakdown incl. the `abs`/`ilostat` round-number provenance query (state.db, local)
   - the ONE D1 measurement: `SELECT_SOURCES` verbatim with `meta.rows_read` (closes the §10.2 flag; if rows_read is in the millions, schedule the materialisation/cache fix in Phase 2)
   - `ledger_check.py --numbers` and `test_reliability_system.py`
5. Decide the `GATED` 26-rows question → **brief to Ahmed** (RESERVED). Check D1 presence first (one cheap query, count-only).
6. Reconcile `GATED` licence drift → fix the stale record or brief Ahmed (RESERVED if it means changing a gate).

**Outputs.** Updated `WORKLOG.md` with dated baselines; fixed `ledger_check.py`; the two decision briefs.
**Exit gate.** `ledger_check.py --digest` passes with **zero invisible headings** and a demonstrated FAIL-on-gap test; every baseline row has an instrument + date; briefs filed and their resolution recorded.

## PHASE 1 — Safe repairs and honesty items (no data-plane risk)

**Objective.** Remove the user-visible lies and stale artefacts that cost nothing to fix, and prove the pipeline for small changes end-to-end (repo → deploy → live verification).

**Tasks.**
1. `catalog_coverage` string → real number (single source of truth: count from `SUPPORTED_SOURCES`/audit). Fix `api/CONTRACT.md` in the same commit (they share the constant). **Then `npx wrangler deploy` from `api/worker/` and verify live** — this is the rehearsal for C2. Record the deploy in the log.
2. `SUPPORTED_SOURCES` header comment (323, not 191). Also fix the duplicated `unctad_cpia` literal if it is a duplicate (verify first — 325 literals, 324 ids).
3. `updater-daily.yml` comment drift (285→305, 335→355, 270→290) — prose-only.
4. Homepage "R client" claim → build or change copy (W6): cheapest honest path is copy change + `.zenodo.json` + `STRATEGY.md` step 4 note + `api.html`/`index.html`; building an R client is a later, separate decision (**RESERVED** whether to build).
5. `worldbank_wdi` / geo-alias mapping correctness: verify the 8 legacy aggregates question is *closed* (R509/R500 show a history of wrong claims here — re-measure from the store the fetcher reads: `clean_full/worldbank/worldbank.parquet`; the grouped tier is NOT evidence). If genuinely broken, fix per the fetcher docstring's own `_migrate_legacy` guidance, never by deleting published series.
6. `/v1/sources` cost: execute the Phase-0 measurement plan; if materialisation/caching needed, implement and verify rows_read drops (discriminating pair: before/after measurements).

**Outputs.** Deployed worker with verified live responses (paste them in the log); site copy consistent with reality.
**Exit gate.** Live `GET /v1/catalog?limit=1` shows the corrected `catalog_coverage`; docs match code; every changed claim has a live-response quote in `WORKLOG.md`.

## PHASE 2 — Catalogue and search integrity (data-plane writes, strictly guarded)

**Objective.** Make the five places a series lives agree again, and make search index the titles it must match.

**Tasks.**
1. **`series_fts` repair** (W4). Design first: PK-range statements only; per-source; the survival test proves the kept rows carry real **titles** (sample `MATCH` queries before/after per source, incl. `wid` `q=disposable` baseline 33,390 and `boc` 84%-repeat baseline); orphans (1,052,814) removed loud-and-named; `source_counts` refreshed in the same sync. Run the design through a parallel adversarial review (C12) — this exact family produced R488, R492. Execute one source first, verify live, then batch the rest within the cost guard.
2. **`source_counts` reconciliation** (R489): rebuild the cache from one authoritative query per source (recompute via PK ranges locally, push via the single writer), then a reconciliation check across all 322 (the zero-result check defines completion). The fallback live-`COUNT(*)` path must never fire again — prove it (log instrumentation or cache-completeness assert in the sync).
3. Title/`series` vs `series_fts` consistency sweep: after (1), verify `MATCH` finds what `series.title` says (the R491 instrument: sample 120 ids — title correct 120/120 AND match hits > 0).
4. `eia`-style catalogue-store coherence: reconcile `catalog_scope: subset` sources' mappings so changed keys resolve (feeds Phase 3).

**Outputs.** FTS row count ≈ `series` count (both DBs incl. noaa shard); search spot-checks pass; cache drift zero.
**Exit gate.** The reconciliation check over all 322 sources returns zero drift on two consecutive runs (one immediate, one after the next sync); `ledger_check --titles` passes with a discriminating test that fails on a destroyed index (close the R492 hole).

## PHASE 3 — Update-path completeness (make every fetched row reach a user)

**Objective.** Close the gap between "the updater is green" and "users got the data" — W2 — without touching public ids.

**Tasks.**
1. **Retry-queue drain, root-caused:**
   a. Establish the `abs`=100,000 / `ilostat`=50,000 provenance (the GROUP BY query). If cap artefacts, fix the enqueue caps to record true counts.
   b. The graceful deferrals (`usda` 48,047, budget path): verify the drain actually drains now (the no-`csv_err`-gate fix landed — confirm the reader exists, R361-style: grep the reader before believing). Run and watch the count fall; report per-run progress.
   c. The 183,735 `UnitTimeout` crashes: diagnose per source (window arithmetic: probe ≤ R/2, update ≤ (R−probe)/2; the 45-min default vs actual source cost; `abs`'s 1,222 units). Fixes are per-source budget/derivation changes — do NOT weaken `merge_and_write` or `min_ratio` (it has been right every time it fired, R519).
2. **The 73,125 unmapped keys** (coherence): for each of the 20 sources, establish *why* (cap, no catalog row, flow-vs-series grain, retiree) and fix the mapping ladder or the catalogue. `eia`'s 50,000 is a cap — fixing its cursor rotation + mapping is a Phase-4 dependency; do the non-id-changing parts here.
3. **The 11 fetchers' changed-set contract** (`norgesbank` + 10): move cursor seeding to post-fetch rows actually merged (the documented contract). One fetcher first, with before/after derive counts as the discriminating measurement; then the class sweep (C6/C16: grep for the shape).
4. **~56 perpetual-`partial` sources**: triage to a root cause each (structural vs budget vs real transient). Fix budget/rotation; any source needing a re-key → Phase 4 brief. Track "sources that have returned `ok`" as the phase metric.

**Outputs.** Queue drained or bounded with every row attributable; unmapped-keys count → 0 (or a dated, per-source reason + RESERVED brief where ids would change); the 11 fetchers fixed and their derives measurably correct; `ok` rate climbs and stays.
**Exit gate.** Two consecutive nightly runs with: queue deltas ≤ enqueue rate; zero unexplained coherence notes; zero `UnitTimeout` enqueues for the fixed sources; the per-source reasons for any remainder are written and dated.

## PHASE 4 — The key-collision re-keys (each one a decision brief FIRST)

**Objective.** Eliminate the largest open data defect (W1) with zero silent breakage and with Ahmed's explicit sign-off per source.

**Tasks.**
1. **Finish the measurement.** Sweep the five giants (`statcan`, `eurostat`, `cbs_nl`, `oecd`, `ilostat`) with the corrected per-file instrument (validate against hand-computed answers before use — the v1–v4 history). Include the 13 custom-schema stores with their own instruments. Publish the final table (store / conflicting pairs / % / files / served?) in `WORKLOG.md` and NUMBERS.md. This census is the evidence base for every brief.
2. **One brief per affected source**, in a standard form: the dropped dimension (publisher evidence — `$metadata`, dimension list, workbook sheet), the proposed new id grammar (stable across snapshots — never embed vintage in the key), the migration (store re-key → re-derive → catalogue/D1/FTS/source_counts → denylist unaffected), the compatibility plan (what happens to old ids: alias, 410, or deprecation window — must be loud, never a silent 404), the cost (rows/objects/Class-A PUTs, wall time), the rollback (fixtures + backup), and the verification (publisher-value spot-checks like the Damodaran/UNCTAD confirmations). **RESERVED: no execution before Ahmed's written go.**
3. **Order the executions** by (served ids affected × severity × reversibility): `damodaran` (721 wrong values, publisher-confirmed, smallest blast radius) → UNCTAD ×2 (Flow) → `idb` → `eia` (biggest; needs the Phase-3 cursor fix first) → the minors (`bea`, `defillama`, `istat`, `GATED`) → gated stores (`GATED`, `GATED`, `GATED`) which change nothing user-facing but stop the defect at its source.
4. **Fix the systemic cause**: route ingest jobs through `merge_and_write` (or a shared "ingest publish" wrapper with the same invariants) for all future writes — 146 writers to migrate or retire; each migration proven by a discriminating test (a fixture with duplicate `(key,date)` pairs must come out deduped; a shrink must be refused). Never re-run an ingest against a live store without the guard's protection.
5. After each re-key: re-derive, sync all five places, regenerate the site + runbooks (`python tools/gen_runbook.py --with-store`), verify live (200s + correct values incl. a publisher-confirmed spot-check), log it, ledger it.

**Outputs.** The final collision census; one signed-off brief per source; executed re-keys with publisher-confirmed verification.
**Exit gate.** Zero served sources in the collision table (or a dated, signed decision to keep a specific one); the post-fix census shows zero conflicts for every re-keyed source; old-id behaviour is documented on the site (deprecation note) and tested live.

## PHASE 5 — Freshness, coverage, and the open investigations

**Objective.** W3 and the residual investigations, so nothing scheduled is silently stale.

**Tasks.**
1. `tools/audit_untouched_files.py --live` attribution for all 26 sources — each file gets one of four dated reasons (retired flow / ingester-owned / static reference / genuine gap); genuine gaps get fixed fetchers (Phase-3 patterns) or briefs.
2. **`eurostat` investigation**: 440 catalogued flows serving nothing + 540 vanished store files. Read-only first; the guard history (45-day freeze on a stale file count) says distrust counts until re-verified against R2 with `tools/store_inventory.py`. Publish findings; fix only with a review.
3. `statcan` derive completion: verify by the DERIVE's own log, never the ingest's marker file (R431). Then the parquet re-upload. Then its collision census slot.
4. `comtrade` standing guard: holdings ≤ 100,000 records — add the mechanical check if absent (a guard, with its discriminating pair).
5. Gate policy for bounded broken minorities + `oecd` cross-sectional decision + `norgesbank` provenance → **three RESERVED briefs**.
6. `/v1/stats` publication brief (W6) → **RESERVED**.
7. Re-run the full health gate and coverage audit; the honest headline set (sources / series / observations / scheduled %) goes to NUMBERS.md with instruments.

**Outputs.** A dated reason for every untouched file; eurostat findings; the briefs; the refreshed headline numbers.
**Exit gate.** `audit_untouched_files.py` shows zero unattributed files; the health gate's red set equals exactly the signed, documented exceptions; all briefs filed with dated resolutions.

## PHASE 6 — Final verification and the completion ceremony

**Objective.** Prove the library is complete by the standard that has failed before: every place a series lives agrees, every claim on the public site is true, and the record is clean.

**Tasks.**
1. **Five-place reconciliation** (R2 CSV / D1 `series` / D1 `series_fts` / local `catalog.db` / `source_counts`) across all 322 sources, using PK-range instruments only. Any drift → root-cause and fix (this is the R481/R489 class; expect to find something — finding zero on a first pass is suspicious, not success: control the instrument with a planted drift).
2. Full end-to-end user-journey verification on the LIVE system: search → browse → download (200 with correct header and values) → bundle manifest → MCP tools → status board — one sample per pillar, results pasted into the close-out.
3. Regenerate site + runbooks; confirm 321 pages × 321 index entries × 322 served reconcile.
4. Final ledger pass: `ledger_check.py` all modes; every open item in this plan has a resolution or a filed brief; `WORKLOG.md` summarised into the close-out.
5. Close-out report to Ahmed: what was fixed (with instruments), what was decided and by whom, what remains reserved and why, the honest headline numbers, and the "how this will not rot" section (the standing guards: cost guard, health gate, freshness probes, digest checks, the skill itself).

**Outputs.** The close-out report; a clean audit trail.
**Exit gate.** The five-place reconciliation is zero-drift on two consecutive full passes with a proven instrument; the live user journey is documented; every RESERVED item has a dated decision; the site's public claims are all true.

---

# PART 4 — WHAT "COMPLETE" MEANS (the acceptance definition)

The database is complete when **all** of these hold, measured, not asserted:

1. **Coverage:** 322/322 served sources catalogued, resolvable, and scheduled-or-documented-archival; 0 actionable scheduling gaps.
2. **Correctness:** 0 served sources in the key-collision census (or dated, signed exceptions); publisher-confirmed value spot-checks pass for every re-keyed source.
3. **Freshness:** every live source's newest observation is within its data-clock, every exception is a dated, signed, code-readable declaration (`upstream_verified` or equivalent), and the health gate's red set equals the documented exceptions.
4. **Delivery:** every fetched change reaches a user (0 unexplained coherence notes; retry queue bounded and draining; no row banked without a CSV).
5. **Consistency:** the five places a series lives reconcile to zero drift, verified twice with a planted-drift control.
6. **Honesty:** every public claim (counts, clients, coverage string, status board, stats) is true against the live system, verified by live requests.
7. **Cost:** forward run-rate documented and guarded; the billing guard reconciles to real invoices; no query shape violates the O(page) rule.
8. **Record:** the ledger is mechanically clean (every entry digested), NUMBERS.md carries every reported figure with its instrument, and the skill remains installed so the next work session starts with the constitution.

---

# PART 5 — HOW TO HAND THIS TO CLAUDE CODE

1. Install the skill (see `INSTALL_AND_HANDOFF.md`) in `E:\research\econfindatalibrary\.claude\skills\econ-completion\` and `D:\research\hfdatalibrary\.claude\skills\econ-completion\`.
2. Start Claude Code in `E:\research\econfindatalibrary` and say, verbatim or close to it:

   > "Follow the `econ-completion` skill. Work from `ECONLIB_COMPLETION_PLAN.md` in `D:\research\deepseek econ plan`. You are starting Phase 0. Before any consequential action, run the parallel adversarial review as the skill requires. Do not touch anything RESERVED — prepare briefs and stop. Maintain `WORKLOG.md` in the econ repo root. Report per phase, not per task."

3. Every session boundary: ask Claude Code to re-read the phase header and run the mechanical checks first (the skill makes this automatic if installed correctly).
4. If it claims something is done: demand the live response and the instrument, per C2/C4. If its finding sounds alarming, apply C10 (3:1) before acting.

---

*End of plan. The skill (`econ-completion-skill/`) is the operational companion; this document is the assignment. Both are dated 2026-08-30 and grounded in `ECONDATALIBRARY_COMPLETE.md`.*