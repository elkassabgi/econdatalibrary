# econfindatalibrary — operating rules for Claude

## 1. Do not end a turn to report. Report while working, or when actually done.

The failure mode in this project is not bad work, it is **stopping to narrate
finished work**. A completed investigation is not a deliverable. A written plan is
not a deliverable. The deliverable is data that is hosted, downloadable, and
auto-updating.

**Definition of done for any task here:** the change is committed, pushed, deployed
where applicable, and VERIFIED against the live system — not "the code is written".

Only three things end a turn:

1. A decision in the RESERVED list below (§2) that I am not authorised to make.
2. A hard external blocker — a credential I do not have, an account only Ahmed can
   create, a service that is down.
3. The queue is genuinely empty.

"I found something interesting" is not one of them. Neither is "this is a natural
place to summarise". If the next action is knowable, take it, and put the summary at
the END of the working turn.

## 2. Pre-authorised vs reserved decisions

**PRE-AUTHORISED — do these without asking.** They are additive, reversible, or
plainly within the standing mission (everything auto-updates, hosted fully):

- Adding NEW source ids, catalog rows, fetchers, ingesters, registry entries
- Deriving and uploading CSVs to R2; syncing catalog rows to D1
- Flipping `SUPPORTED_SOURCES` **after** verifying the CSVs exist (derive → sync →
  flip; never flag-first, which turns a 501 into a 404)
- Promoting a source to `live: true` once it proves in CI
- Building/committing/pushing/deploying fixes to econdatalibrary + its worker
- Restarting local crawlers, dispatching CI runs, adding audits and instrumentation
- Fetching a source directly instead of via an aggregator, when measured to be
  equal-or-better coverage

**RESERVED — stop and ask.** Each of these can destroy something a user has:

- **Re-keying or retiring existing series ids** (breaks saved links, notebooks, MCP
  configs). Adding a parallel id is pre-authorised; changing an existing one is not.
- **Deleting data, catalog rows, or R2 objects** — including "phantom" rows, until
  their absence upstream is verified.
- **Auth / security / billing policy** (token lifetimes, key rotation, rate limits).
- **Publishing anything to a PUBLIC repo that is internal** — MISTAKES.md, licence
  negotiations, operational notes.
- **Sending email** or any outward communication under Ahmed's name.
- **Switching a source to a feed that serves LESS than the current one** (e.g. IMF
  MCDREO direct has 57% of the relay's series; FM 9%).

If a task is mostly pre-authorised with one reserved step, do the whole
pre-authorised part first and surface only the reserved step.

## 3. What is worth reporting

Report: decisions needed, blockers, completions with verification, and things that
change Ahmed's model of the system (a bug class, a wrong assumption of his or mine).

Do not report: intermediate investigation, plans about to be executed, restated
status, or a proposal that could simply be done.

## 4. Verification rules earned the hard way

Full detail in `.claude/MISTAKES.md` (R1–R72). The ones that bite most often here:

- **A green run is not a proof** — read what it DID (units processed > 0). R50.
- **Announce work BEFORE starting it** — a killed process prints nothing, so the
  last `>>>` line is the culprit. R70.
- **Verify at the surface the user touches** — local catalog ≠ live D1 ≠ R2. R60.
- **Run a known-good control** before believing a negative result. R52/R67.
- **A test that cannot fail proves nothing.** R64.
- **Names are an interface** — grep every consumer before assuming a convention. R66.
- **A budget bounds only the failure mode it measures** (time ≠ memory). R72.
