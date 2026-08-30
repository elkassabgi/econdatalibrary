---
name: econ-completion
description: Systematic, mistake-proof working method for completing and maintaining the Elkassabgi econdatalibrary (the econ repo). Use whenever working on the econ data library — when executing ECONLIB_COMPLETION_PLAN.md or any of its phases 0-6, when touching the catalogue, D1, R2, the updater, fetchers, ingesters, licences, derives, deploys, runbooks, or public series ids, when about to report any number, status, or claim about the system to the owner, and whenever the user says "work on the econ database", "continue the econ plan", or "follow the econ-completion skill". Also use it before any destructive operation, any batch of D1 statements, and any deploy in either the econ or the hf repository.
license: MIT
---

# econ-completion — finish the database without repeating the 519 recorded mistakes

## What this skill is

This skill is the mandatory working method for completing econdatalibrary.com. It exists because the project's own record (519 ledger entries, R0–R519) proves that prose rules did not hold: only procedure bound to actions, mechanical checks, and a second agent stopped the repeats. It executes the assignment in `ECONLIB_COMPLETION_PLAN.md` (`D:\research\deepseek econ plan\`). Where this skill and the plan conflict, the skill wins. If the plan is unreachable, STOP and ask for it — do not improvise the work.

Repos: econ `E:\research\econfindatalibrary` · hf `D:\research\hfdatalibrary` (owns the ledger `MISTAKES.md`, `NUMBERS.md`, hooks, and the adversarial-review skill).

## The five non-negotiables (re-read at the start of every session)

1. **Every number carries its instrument and date.** No instrument → not a measurement → do not report it. Every reported figure gets a `NUMBERS.md` row in the same commit; corrections are appended, never silently edited.
2. **A claim about the running system is settled only by the running system.** "The registry says X" ≠ "X"; a commit ≠ a deploy (the Worker and the Pages site both deploy MANUALLY); a local file ≠ what users see. Say which surface you probed and paste the live response.
3. **Prose rules do not hold.** Every rule you add ships with its enforcement in the same commit: a discriminating pair (one case it must block, one it must pass), a hook, a test, or a reviewer.
4. **Adversarial review runs in parallel with everything consequential.** Brief → challenge → do → verify → record. The reviewer's job is to find the flaw; FAIL/REDIRECT is a reviewer success. Never proceed past a FAIL without re-briefing.
5. **RESERVED decisions stop the work.** Only the owner (Ahmed) releases them: deleting non-re-crawlable data; un-gating a DISPUTED licence; auth & billing; sending email as him; ANY change that alters public series ids; the `/v1/stats` publication; gate policy; cross-sectional serving policy. Prepare a one-page brief and stop. Never un-gate via the permission system.

## Session rhythm (run in order, every session)

1. Confirm this skill is loaded and `ECONLIB_COMPLETION_PLAN.md` is reachable.
2. Re-read the plan's current-phase section (check `WORKLOG.md` for the active phase).
3. Run `python <skill>/scripts/skill_check.py`. **Exit 1 = STOP** and fix what it names.
   **Exit 2 = create the named file, then continue.** (These tiers are the script's contract;
   the earlier "non-zero means STOP" wording contradicted the script's own documented exit 2 and
   would have hard-stopped a session over a missing `WORKLOG.md`. Its discriminating cases are in
   `tests/test_skill_check.py` — 8 cases, 5 mutations caught.)
4. Check red workflows in any repo you will touch: `gh run list` (R421: a red daily job in a repo you push to is your outage).
5. Append today's intent to `WORKLOG.md` (econ repo root): date / task / instrument / expected result.
6. Start the parallel adversarial reviewer BEFORE building anything consequential. The project's own adversarial-review skill is in `D:\research\hfdatalibrary\.claude\skills\adversarial-review\`.

## Definition of DONE (per source, per task, per phase)

DONE means ALL of: catalogued + served (live 200 from the deployed endpoint, response pasted into `WORKLOG.md`) + verified against the publisher wherever a value is claimed + on a schedule + licence-verified against `DATABASE_LICENSES_VERBATIM.md` (never re-derived from scratch) + recorded (ledger/NUMBERS.md/`WORKLOG.md`). A green run is not a proof (R50). "0 units processed, exit 0" is a FAILED proof.

## Protocols — load `references/protocols.md` BEFORE the matching act

| Act you are about to perform | Protocol section to load first |
|---|---|
| Trust or report any probe that says "absent / missing / 0 found" | Null-result protocol |
| Delete, purge, re-key, bulk-write, shrink anything | Destructive-operation protocol |
| Run a batch of statements against D1 | D1 batch protocol |
| `npx wrangler deploy` or publish the site | Deploy protocol |
| Tell the owner a number, a status, or "it is done" | Reporting protocol |
| Attach a cause to an observation | Diagnosis protocol |
| Launch or watch a job that runs > 60 s | Long-job protocol |
| Compose any shell command | Command-composition checklist |
| Record a mistake | Ledger protocol |
| Work a source end to end | Source protocol |
| Encounter anything marked RESERVED | Reserved-decisions protocol |

## The failure classes — load `references/failure-classes.md` when a result surprises you

Before believing any result, check it against the 13 failure classes and the 9 seductive assumptions. The two meta-rules that cover most cases: discount alarming findings ~3:1 and re-measure the instrument first; treat every green as unmeasured until you can name the check that would have gone red.

## Phases — load `references/phase-playbooks.md` at each phase start

The plan defines Phases 0–6. Do not start Phase N+1 until Phase N's exit gate has PASSED and its evidence is recorded in `WORKLOG.md`. `references/state-baseline.md` holds the condensed current-state numbers (all dated 2026-08-30) — re-measure before acting on any of them (R509: never build on a previous session's conclusion).

## Required outputs

- `WORKLOG.md` (econ repo root, append-only) — every task: date / task / instrument / result / ledger ref.
- `NUMBERS.md` (hf repo) — every reported figure with instrument and date.
- Ledger entry + digest line in the SAME commit for every mistake (`ledger_check.py --digest` enforces).
- Live-response quotes for every "served / verified / done / live" claim.