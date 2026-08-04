---
name: econ-updater
description: MANDATORY operating system for ALL econdatalibrary updater/serving work. Invoke before touching anything in E:\research\econfindatalibrary — architecture, iron rules, the per-source procedure, and the serve-a-source pipeline. Created 2026-08-04 at Ahmed's direction after 5 weeks of circular regressions.
---

# econ-updater — the operating system for econdatalibrary

## Why this exists

Five weeks of the same failures, each documented and each repeated: working code broken by
fixes made without reading it first; the DBnomics ban violated from memory; sources reported
"live" that no user could reach; the whole updater taken down by a one-line count left stale.
The knowledge existed — a 150-entry ledger, 249 runbook pages — but consulting it was
voluntary. This skill makes it the procedure, and three mechanical layers back it up:

1. **PreToolUse hook** (D:\research\hfdatalibrary\.claude\settings.json) — DENIES any command
   reaching db.nomics.world. It has already fired in production, on the session that wrote it.
2. **CI tests** (this repo, run on every push): `test_dbnomics_ban.py` (the domain in runtime
   code fails the build), `test_registry_count_guard.py` (registry entries vs
   EXPECTED_SOURCE_COUNT), `test_failure_labels_ratchet.py` (unlabelled in-loop failures may
   only decrease). These survive every session, every model, every memory loss.
3. **This skill**, loaded completely — not skimmed:

```bash
node "C:/Users/aelkassabgi/.claude/skills/read-session-log/driver.mjs" skill "E:/research/econfindatalibrary/.claude/skills/econ-updater"
```

Do not proceed to any edit until `skill-verify` prints **ALL PASS**.

## The ten non-negotiables

Violating any of these has cost days. Each is cited to the ledger entry where it did.

1. **DBnomics is BANNED.** No fetching, no probing, no relays, no mirrors (R251). The hook and
   CI enforce it; retired scripts raise at import — leave them defused.
2. **One source at a time, end-to-end.** Never touch a second source before the first meets
   the Definition of Done below. The circular-failure pattern is many half-finished touches.
3. **Read before touching.** For source X, BEFORE any edit: `docs/runbook/X.md` → grep
   `D:\research\hfdatalibrary\.claude\MISTAKES.md` for `X` → X's fetcher header → X's
   `registry.yaml` entry → X's verdict in `DATABASE_LICENSES_VERBATIM.md`. Five reads, always.
4. **A registry.yaml entry add/remove bumps `config.EXPECTED_SOURCE_COUNT` in the SAME
   commit** (R347 — the mismatch stops ALL sources, not the new one: validation refuses the
   run before anything fetches, and it cost a 14-hour total outage).
5. **An edit to `api/worker/src/util.ts` changes NOTHING a user can reach** until
   `cd api/worker && npx wrangler deploy` runs — nothing auto-deploys the worker (R345:
   425,462 series were reported "live" while every id answered 501). Verify on the LIVE
   surface: `/v1/sources` must list the source.
6. **Verify against the running system, never against a file you just edited** (R345, R296,
   R342). Re-running the query that produced a number is reproduction, not verification.
7. **`partial` is not failure; deferral is not failure; a state row is a snapshot of the last
   RUN, not current truth** (R231, R303). Before calling anything broken, check the evidence
   POSTDATES the last fix (R339: compare the newest run timestamp to `git log` on the file).
8. **All store I/O goes through `updater/blob`** — `open()` on a store path works locally and
   silently sees nothing under `AQUEDUCT_BACKEND=r2` (R344, R36 class).
9. **Licence gate BEFORE catalogue.** `DATABASE_LICENSES_VERBATIM.md` is canonical — never
   re-derive a verdict. A catalogue row is an offer to serve. etalab-2.0 requires a
   publisher-observed last-update date (the `Last-Modified` of the exact hosted file — the
   cepii_gravity precedent), never a date invented from a version string.
10. **A new check must be shown it can FAIL before it is trusted** (R346/R338: build a real
    negative control, and verify the control is actually negative — my "unsupported source"
    control turned out to be supported, and my replacement probe passed a source id I invented).

## The cycle (what a work session looks like)

```
pick ONE source  →  the 5 reads (rule 3)  →  measure its current state
  →  smallest change that fixes/builds it  →  full test suite green
  →  if the serving surface changed: the FULL pipeline in references/30-serving-pipeline.md
  →  Definition of Done proven  →  regenerate its runbook page  →  commit + push
  →  only then, the next source
```

Shared infrastructure (`updater/strategies/fetchers/_common.py`, `_giant.py`,
`orchestrate.py`, `merge`, `core/`, `api/worker/`) may only change when the current source
strictly requires it, the full suite passes, AND the change ships with a test that would have
caught its absence.

## Definition of Done for a source

Every row proven with a command whose output you actually read — no row may be asserted:

| claim | proof |
|---|---|
| store coherent | `catalogue rows == R2 objects`, MISSING 0, ORPHANED 0 (`tools/verify_source_served.py --source X --sample 150`) |
| bytes correct | byte-compare sample N/N identical (same tool) |
| discoverable | D1 count == catalogue count, AND the LIVE `/v1/sources` lists X (same tool — its live probe, not the local util.ts) |
| auto-updating | registry entry live (or workstation route), EXPECTED_SOURCE_COUNT matches, and a REAL run (dispatched or scheduled) ended `ok`/`no_change` for X |
| documented | `docs/runbook/X.md` regenerated after the work |

## Reserved for Ahmed — stop, write `.claude/STOP_REASON`, ask

Deleting data that is not re-crawlable · un-gating a DISPUTED licence · re-keying or retiring
SERVED series ids · switching a source to a feed serving LESS · auth/security/billing ·
sending email as Ahmed · publishing internal docs to public repos.

## References (load ALL of them — the driver enforces it)

| file | contents |
|---|---|
| `references/00-architecture.md` | component map, end-to-end dataflow, state semantics, workflows inventory, hard numbers |
| `references/10-iron-rules.md` | the full distilled rulebook from all 150 ledger entries, grouped by pipeline stage |
| `references/20-per-source-procedure.md` | the step-by-step for fixing or building ONE source |
| `references/30-serving-pipeline.md` | exact commands: derive → verify → catalogue → D1 → util.ts → deploy → live check |
| `references/40-source-landmines.md` | per-source warnings mined from the ledger |
| `references/50-queue.md` | the work queue: ACTIONABLE vs RESERVED, with series counts |

## Reporting

Progress is reported ONLY as `N of M sources / X of Y series scheduled` (from
`tools/audit_schedule_coverage.py`) plus the one source completed this cycle. Never a
completion summary, never a claim the coverage tool cannot back. Record every mistake in
`D:\research\hfdatalibrary\.claude\MISTAKES.md` immediately — append at the anchor, verify the
entry count (R247).
