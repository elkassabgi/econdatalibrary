# econfindatalibrary — operating rules for Claude

## 0. DBNOMICS IS BANNED. Ahmed's standing instruction. No exceptions.

**Do not fetch from DBnomics. Do not probe api.db.nomics.world. Do not build, keep or
"temporarily" rely on a DBnomics-backed fetcher, relay, mirror or vintage signal. Do not
run the DBnomics staleness audit as if it described a supported path.** Every source must
come from ITS OWN PUBLISHER.

Ahmed has said this at least five times. It was never written down until 2026-08-02, which
is why it kept being violated — a spoken instruction that lives only in one session's
context does not survive compaction. It lives here now, in the file that is loaded every
session, and in the Rules Digest of `.claude/MISTAKES.md`.

Why it keeps recurring: DBnomics is *convenient*. It aggregates hundreds of datasets behind
one API, so it looks like leverage whenever coverage is the goal. It is not leverage, it is
a dependency on a mirror that silently stops: 98 of the 101 datasets we ever took from it
have not been re-indexed in over 180 days (UNCTAD: 1,581 days), and because the vintage
signal is DBnomics' own hash, a frozen dataset reports `no_change` forever while the health
gate sees a source succeeding every day. That is the exact opposite of the mission.

**What this means in practice:**
- Existing DBnomics-derived data STAYS until migrated to the publisher — nothing is deleted
  by this rule. `who_hwf`, `who_rs`, `who_sdg` are the last three live relay fetchers and
  are to be MIGRATED to WHO directly, not refreshed via DBnomics.
- Never add a new one. If a publisher has no usable API, say so and ask — do not reach for
  the aggregator.
- Do not cite DBnomics coverage as evidence of anything about a source's freshness.


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

## 3b. A SOURCE STOPPED UPDATING — start at its runbook, not at the code

`docs/runbook/<source_id>.md` — one file per database, 248 of them, GENERATED from the registry,
`state.db`, `catalog.db`, `util.ts`, the fetcher's own docstring, the licence file and the
ledger. Index at `docs/runbook/README.md`. Regenerate with
`python tools/gen_runbook.py --with-store` after any change; never hand-edit.

Each page carries that source's real state, its adapter contract, a six-step DIAGNOSE section
with runnable commands, **the ledger entries about that specific source** (ons_uk has nine,
three of which record a fix that was shipped and was WRONG), and a store-vs-state comparison.

Read it BEFORE forming a theory. Three things mislead nearly everyone, so they are on every page:

- A `partial` never sets `last_success_utc` (R231), so "last SUCCESS: **never**" is often a
  perfectly healthy source failing one sub-unit.
- `obs_count` is NOT "rows this run". It is whatever the fetcher passed `finalize()` as
  `total_rows`, and measured 2026-09-03 only THREE of ~123 call sites pass a genuine added
  count — the rest pass the store's total. R326's conclusion (not comparable across runs) is
  right; the mechanism this line used to give was backwards, and it was the first thing every
  session read. `docs/runbook/bea.md:41` settles it without any code: obs 251,203 beside a note
  of "+258,223 new rows".
- A FUTURE date is usually a legitimate PROJECTION, not staleness (CSO to 2057, Estonia 2085,
  UN WPP 2101). A defect is a SENTINEL (9999/2999) or a COUNTER (contiguous from year 1). Never
  judge by the size of the number. R327.

From the 2026-08-04 audit of every source that could not be fetched, the causes were
`budget_deferral` (NOT broken — ran out of its time slice), `code_bug`, `rate_limited`,
`gated_by_design`. **Zero were an expired credential or a dead endpoint**, though that is the
usual first guess. If sub-units are named `deferred (budget N min)`, nothing has failed (R303).

## 4. Verification rules earned the hard way

**READ `.claude/MISTAKES.md`'s Rules Digest — especially ⚠ R0 — before trusting any number you
produced.** Not the 8,600-line archive below it; the digest is the read-path. On 2026-08-04 I
wrote sixteen entries and added zero digest lines, so those lessons were invisible the same
night (R328). R0 collects the error that keeps recurring — a measurement whose SHAPE is wrong,
not a question that is wrong — as five checks: compute what the SYSTEM computes rather than
re-implementing its rule; read a long job's ARGV, not its progress; read a sweep's FAILURE count
before its results; when a probe reports ABSENCE, test it against a known PRESENCE; and a
one-sided test on a two-sided failure yields a number that merely looks like a measurement.

Full detail in `.claude/MISTAKES.md`. The ones that bite most often here:

- **A green run is not a proof** — read what it DID (units processed > 0). R50.
- **Announce work BEFORE starting it** — a killed process prints nothing, so the
  last `>>>` line is the culprit. R70.
- **Verify at the surface the user touches** — local catalog ≠ live D1 ≠ R2. R60.
- **Run a known-good control** before believing a negative result. R52/R67.
- **A test that cannot fail proves nothing.** R64.
- **Names are an interface** — grep every consumer before assuming a convention. R66.
- **A budget bounds only the failure mode it measures** (time ≠ memory). R72.
