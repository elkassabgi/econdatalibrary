# Phase playbooks — per-phase entry and exit

The full task lists live in `ECONLIB_COMPLETION_PLAN.md` Part 3. Load this file at the start of each phase, and the plan's phase section before every task inside it. Record every task and every gate check in `WORKLOG.md`.

## Rules that apply to every phase

1. Re-measure every number from the plan before using it (R509).
2. Anything consequential gets a parallel adversarial review BEFORE building.
3. Exit-gate evidence must be instrumented, not asserted. Paste live responses for anything user-facing.
4. A phase is not complete until its gate PASSES — no exceptions, no "almost".

## Phase 0 — Install, baseline, fix the instruments

Entry checks: skill installed in BOTH repos and loads; plan reachable; `WORKLOG.md` created (append-only).
Key tasks: fix `ledger_check.py --digest`; fix stale doc counts; capture baselines (audit, untouched files, retry-queue provenance incl. the `abs`/`ilostat` GROUP BY, the ONE `SELECT_SOURCES` rows_read measurement); brief Ahmed on several GATED sources.

> **CORRECTED 2026-08-30 — the original wording of the digest fix was refuted by this project's own
> adversarial review the same day (ledger R521). Do NOT implement "date-based cutoff + backfill 147".**
> Four independent faults: (a) a **date-based cutoff is open-ended and exempts what it cannot
> parse** — `## R443 … (2026-08-22/23)` is a date *range*, so the regex misses it, the rule falls
> through to `id >= 475`, and an entry dated four days after the cutoff is **silently excused**
> (R508/R503's shape); only **85 of 294** headings carry a parsable trailing date at all.
> (b) **27 ids each carry TWO different entries**, so `id not in digest` and a one-line-per-id
> backfill would mark both covered and hide one permanently (R488). Key on **(id, heading line)**
> and add a **no-id-reuse** check. (c) The widened regex still misses **171 `### M-YYYYMMDD-NN:`
> entries with no R id**, so it covers **294 of 470 headings = 62.6%** — state coverage as a
> fraction of the population at risk, or bring the M-form into scope (R501). (d) "147" is the
> distinct count; the guard's arithmetic runs over **174 occurrences** — quote both (R341).
>
> **Do instead:** an **enumerated allowlist** of the pre-rule backlog keyed on `(id, title-prefix)`,
> which **FAILS if a listed entry is missing** from the ledger (it must not rot) and **FAILS if it
> would need to grow**; a **seven-case** discriminating suite, each case derived from a measured
> defect; and ship the regex change **and** the backfill in the SAME commit.
>
> **Deadlock warning.** `skill_check.py` treats a non-zero `ledger_check --digest` as a HARD
> failure, so a deliberately-red digest bricks the next session's preflight. Land the backfill with
> the regex change, or give the preflight an advisory tier for this one check — decide before
> starting, not after.

Gate: `--digest` covers every entry heading (or states its coverage fraction explicitly); the
allowlist can only shrink; every baseline row instrumented; briefs filed.

## Phase 1 — Safe repairs (no data-plane risk)

Key tasks: `catalog_coverage` → real number + CONTRACT.md, then MANUAL worker deploy + live verify (the C2 rehearsal); `SUPPORTED_SOURCES` comment; updater-daily.yml comment drift; R-client copy claim (or RESERVED decision to build); re-verify the 8 worldbank legacy aggregates from `clean_full/worldbank/worldbank.parquet` (NOT the grouped tier); execute the /v1/sources rows_read fix if needed.
Gate: live `/v1/catalog?limit=1` shows corrected coverage string; docs match code; every claim has a live-response quote in WORKLOG.md.

## Phase 2 — Catalogue & search integrity (guarded data-plane writes)

Key tasks: series_fts repair (PK-range statements; per-source; survival test proves the KEPT rows carry real titles; `q=disposable&source=wid` baseline 33,390 and `boc` 84% repeat baseline; orphans named loudly; source_counts refreshed in the same sync) — design through adversarial review first (this family produced R488 and R492); source_counts reconciliation for all 322 with the live-`COUNT(*)` fallback proven unreachable; title↔fts consistency sweep (the R491 instrument: 120/120 titles AND match hits > 0); eia-style mapping coherence.
Gate: zero cache drift on two consecutive runs; `ledger_check --titles` has a discriminating test that FAILS on a destroyed index.

## Phase 3 — Update-path completeness (no public-id changes)

Key tasks: retry-queue provenance + drain (graceful first, then per-source UnitTimeout root causes — never weaken `min_ratio`); the 73,125 unmapped keys attributed and fixed per source (eia cap fix is the non-id part; full fix is Phase-4); the 11 fetchers' changed-set contract (one first, then the class sweep by grep); ~56 perpetual-partial sources triaged; metric = sources returning `ok`.
Gate: two consecutive nightly runs with queue deltas ≤ enqueue rate, zero unexplained coherence notes, zero UnitTimeout enqueues from fixed sources; remainder reasons written and dated.

## Phase 4 — Key-collision re-keys (brief FIRST, every source)

Key tasks: finish the census (five giants + 13 custom-schema stores, per-file instrument validated against hand-computed answers); one RESERVED brief per source in the standard form (dimension evidence, new id grammar with vintage-stability, migration across all five places, old-id compatibility — loud, never silent 404 — cost in rows/objects/Class-A, rollback fixtures, publisher spot-check verification); execution only after Ahmed's written go, in order damodaran → unctad ×2 → idb → eia → minors → gated stores; systemic fix: route ingest jobs through `merge_and_write` (discriminating pair: dedup must fire, shrink must refuse); after each re-key regenerate site + runbooks and verify live.
Gate: zero served sources in the collision table (or dated signed exceptions); post-fix census clean; old-id behaviour documented and tested live.

## Phase 5 — Freshness, coverage, open investigations

Key tasks: attribute every untouched file (four allowed reasons, dated); eurostat investigation read-only first (use `tools/store_inventory.py`, distrust local counts); statcan completion verified by the DERIVE's log (R431); comtrade ≤100k guard with discriminating pair; three RESERVED briefs (gate policy, oecd, norgesbank); /v1/stats publication brief; refreshed headline set to NUMBERS.md.
Gate: zero unattributed files; health-gate red set equals the signed documented exceptions; briefs filed with dated resolutions.

## Phase 6 — Final verification & completion ceremony

Key tasks: five-place reconciliation across all 322 with a planted-drift control (zero on a first pass is suspicious — control the instrument); live end-to-end user journey per pillar (paste responses); regenerate site + runbooks; final ledger pass; close-out report to Ahmed (what was fixed + instruments; decisions and by whom; remaining reserved items; the "how this will not rot" section).
Gate: zero drift twice with the proven instrument; the user journey documented; every RESERVED item has a dated decision; all public claims true.

## What to do when something in a phase is blocked

1. Is it blocked on Ahmed? → Reserved-decisions protocol: brief, record, move to the next non-blocked task in the SAME phase.
2. Blocked on a fact you cannot yet measure? → Write the instrument that would settle it into WORKLOG.md and continue; never fill the gap with a guess (R495).
3. Blocked by a guard refusing a write? → The guard is the witness. Re-measure the plan; do not weaken the guard (R519).
4. Everything in the phase is done? → Run the exit gate, paste the evidence, and only then move to the next phase.