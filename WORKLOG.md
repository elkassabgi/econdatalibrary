# WORKLOG — econdatalibrary completion programme

Append-only. One entry per task: **date · task · instrument · result · ledger ref**.
Governed by the `econ-completion` skill and `ECONLIB_COMPLETION_PLAN.md`.
A phase is not complete until its exit gate passes and the evidence is recorded here.

---

## 2026-08-30 — PHASE 0 opened

**Intent.** Install the skill, fix the instruments that watch everything else, and capture a
re-measured baseline so later phases stand on current numbers.

### P0.1 — Install the econ-completion skill · DONE

* **Instrument:** `cp -r` into both repos, then `find` to list the installed tree.
* **Result:** installed at `E:\research\econfindatalibrary\.claude\skills\econ-completion\` and
  `D:\research\hfdatalibrary\.claude\skills\econ-completion\`; both hold `SKILL.md`,
  `scripts/skill_check.py`, and all four `references/*.md`. Frontmatter `name: econ-completion`
  verified present.

### P0.2 — Session preflight · PASS (1 warning, now cleared)

* **Instrument:** `py .claude/skills/econ-completion/scripts/skill_check.py`
* **Result:** exit 0. All eight path checks OK except `WORKLOG.md`, which this file creates.
  `ledger_check.py --digest` → "112 headings, 336 digest lines … RESULT: all checks passed".

---

## Carried-forward corrections to the plan's baseline

The plan was prepared from the 8,513-line snapshot of `ECONDATALIBRARY_COMPLETE.md`. Four of its
baseline facts have since been superseded by measurement. Recording them here so no phase acts on
a stale premise (C17: never build on an inherited claim without re-measuring).

| Plan says | Measured 2026-08-30 (later) | Instrument |
|---|---|---|
| W7: `statcan` derive ~8,200/8,207, "parquet re-upload queued behind it" | **Derive COMPLETE**: `[8207/8207] units 466,341 · put 252,425 · skipped 213,916 · errors 0 · 677,658 s`, 5 oversized tables named as refused | the derive's own log tail (R431: the writer's evidence, never a sidecar marker) |
| Phase 5 task 3: "Then the parquet re-upload" | **CANCELLED — this would reverse Ahmed's own order.** He ordered statcan's parquet deleted from R2 on 2026-08-18 (1,548.7 GB, 65% of the bucket, $23.23/mo) to be re-derived compressed. That is now done and verified `ContentEncoding: gzip`. Arithmetic closes: 213,916 objects at order time + 252,425 put = **466,341** live | R2 LIST of `series/statcan%3A`; ledger **R520** |
| W3: the 26 untouched-file sources are "Not yet attributed — real open work" | **Attributed.** 13 ROTATING (healthy by design, R285's scattered-write-block test), 3 wrote today, 1 already attributed (`bea`, R282 ingester-owned tree), **3 that looked stuck all refuted by cadence** (`fdic`/`stats_nz` quarterly, `scb` monthly), **1 genuine fault: `idb`** | `attribute_stale.py` (R2 LastModified per object) + registry `cadence` + latest `runs` row |
| W1: fleet sweep at 416/430 | **421/430**, 5,470,525,630 rows; four giants outstanding (`statcan`, `eurostat`, `cbs_nl`, `oecd`) — `ilostat` now measured | `dupsweep.py` v3 results file |

**New finding not in the plan** — `idb`'s single genuine fault is the key-collision defect wearing
another mask: 10 of 40 sub-units refused identically on every run since 2026-08-06 by the
never-shrink guard (`< 97% of existing`), `obs` frozen at 15,066,444, because idb is 39.83%
conflicting so a clean pull is *smaller* than the collided store. Same mechanism that refused the
two UNCTAD stores three times and was right every time. **`min_ratio` stays as it is** (R519).

**Also new** — 34 sources are invisible to `source_state` (249 with a row / 34 without;
"has a row" ⇔ "has ever recorded `ok` or `no_change`", **zero exceptions across 283**), because a
`partial` run never creates the row. They are disproportionately the largest sources, including
all eleven UNCTAD giants. Any freshness instrument keyed on that table reports nothing for them.

### P0.5 / P0.6 — the two decision briefs · FILED

Written to `docs/briefs/PHASE0_BRIEFS.md`. Both re-measured today; nothing changed.

* **`worldbank_pink` — RESERVED, awaiting Ahmed.** 26 series rows in local `catalog.db` **and** 26
  in live D1, plus a `source_counts` row advertising `n=26`, all behind a 451 gate.
  Instrument: one PK-range D1 query, `rows_read: 28` (index seek — C8's PK-range principle
  confirmed in practice). Canonical verdict is **DISPUTED / NEEDS HUMAN REVIEW**: the Pink Sheet
  carries LME settlement prices, Cotlook, SICOM and ICCO/ICO data, and "LME in particular
  prohibits redistribution of its price data without a license". Recommendation: purge the rows
  and the cache row as defence in depth (a mistaken un-gating would expose LME-derived prices
  instantly; R429 shows a push to `main` silently reverting worker state). Re-crawlable, so
  recoverable. **Not proceeding — C14.**
* **`sdmx_nso` — NOT a reserved item; the plan's premise does not hold.** Re-measured: **0** series
  rows locally, and in live D1 **0 series, 0 `source_counts`, 0 `source` rows — it does not exist
  in D1 at all**. The plan describes it as a live drift between two records; only the local record
  exists. It is a stale local `source` row naming ISTAT, which is separately live as `istat`.
  Nothing exposed, no page rendered (`gen_site.py::load_denylisted()` subtracts the denylist —
  the R490 fix, verified by code read + file listing). Local debris to clear at the next
  catalogue rebuild.

*Transient worth recording:* the first D1 call returned Cloudflare **7403**. Per R222/R363 an
identical call succeeding moments later means transient, not a permission wall — re-probed once
and it succeeded. No auth diagnosis was started.

### P0.2 (in progress) — `ledger_check.py --digest` blind spot, MEASURED

The plan's claims verified with my own instruments before any change (C17):

| Claim | Measured |
|---|---|
| guard sees only `^## (R\d+) — ` | **112** distinct ids |
| all `^## R` headings | **124** distinct |
| `^### R` headings | **166** distinct |
| proposed `^#{2,3} (R\d+)\b` | **267** distinct |
| **ids invisible to the guard** | **155** |
| digest lines | **336** |
| missing a digest line, current scope | **58** |
| missing a digest line, widened scope | **147** (matches the plan) |

Date coverage, which the proposed date-based cutoff depends on: only **85 of 294** headings carry
a trailing `(YYYY-MM-DD)`. Of the heading-occurrences missing a digest line, **31 are dated
2026-08-19→08-24** (all after the digest rule began on 2026-08-04 — the R435–R466 range the plan
names) and **143 are undated** pre-rule entries.

**Held pending adversarial review** (C12), briefed on: whether the widened regex matches prose that
quotes other entries; whether a trailing-date cutoff silently exempts entries it should cover
(the R508/R503 fail-open shape); whether the `[M-YYYYMMDD-NN]` tag is a better date source; whether
writing 31 summary lines for entries I have not read is a fabrication risk; and — the one I raised
myself — that `skill_check.py` treats a non-zero `ledger_check` exit as a **hard session stop**, so
a correctly-failing guard would become a self-inflicted outage.

### P0.7 — independent evaluation of the econ-completion skill, and three fixes · DONE

Ahmed asked whether the skill will actually work. Evaluated by TESTING it, not reading it, and
held it to its own rule C5 ("a guard ships with a discriminating pair; 'cannot measure' must
refuse"). Three defects found in its own preflight, each proven by a run:

| Defect | Evidence |
|---|---|
| **Fail-open on a redirected root.** `--hf D:/nonexistent_repo_xyz` printed `[OK] hf repo -> D:\research\hfdatalibrary`, **skipped the ledger check** (gated on `os.path.exists(args.hf)`), and **exited 0** | ran it |
| **"Cannot measure" passed.** An exception running `ledger_check.py` was downgraded to a warning | code read + mutation |
| **SKILL.md contradicted the script on exit codes** ("non-zero = STOP" vs documented exit 2 = continue); a missing `WORKLOG.md` returns 2 | ran it |

Also: **no test existed for `skill_check.py` anywhere**, it is **not wired into any hook or CI**,
and `PLAN_PATH` pointed at `D:\research\deepseek econ plan\`, which `git status` confirms is **not
a git repository** — an unversioned hard dependency that would hard-stop every session if the
folder were tidied.

**Fixed and verified:**
* paths now resolve from `--econ/--hf/--plan` throughout (the hard list no longer ignores them);
* an unrunnable or failing `ledger_check` is a HARD failure, never a warning;
* the plan is now versioned at `docs/ECONLIB_COMPLETION_PLAN.md` and the preflight prefers a repo
  copy — verified: `[OK] plan -> E:\research\econfindatalibrary\docs\ECONLIB_COMPLETION_PLAN.md`;
* SKILL.md now states the exit tiers that match the script;
* **`tests/test_skill_check.py` — 8 discriminating cases, and a mutation run kills 5 of 5**
  (except-branch warns; hard list ignores the redirected root; ledger_check dropped from the hard
  list; plan check removed; soft warnings silently return 0).

**A defect in my own fix, caught by the mutation run and worth recording:** my first version added
a defensive `else` beside the subprocess call for "ledger_check absent". Mutation M3 SURVIVED,
which showed the branch was unreachable — an absent `ledger_check.py` is already a HARD failure
above, so my test was passing through the hard list, not the branch it claimed to cover. That is
R488's "right answer for the wrong reason". Dead branch removed; the test's docstring now states
exactly which path fires.

**Still open (not fixed, deliberately):** the skill is not wired to a hook. Both repos already have
`SessionStart` / `PreToolUse` / `PostToolUse` / `UserPromptSubmit` wiring and the existing guards
ship with tests (`test_d1_cost_guard.py`, `test_reliability_system.py`). The skill's activation
still depends on the model choosing to load it — which is the judgment it exists to constrain.
That wiring is the single highest-value remaining change and is a behaviour change to every
session, so it goes through review first.

### P0.8 — second, independent evaluation of the skill; four more fixes · DONE

An independent evaluator reached **WILL WORK WITH FIXES**, confirmed all three preflight
fail-opens I had found and fixed, and added four findings I had missed:

1. **The two installed copies had actually DIVERGED** for ~20 minutes today — the econ copy fixed,
   the hf copy still printing `PASS … EXIT=0` for a nonexistent `--hf`. CI only ever exercised the
   econ copy. **Fixed:** `tests/test_skill_check.py` now asserts all six skill files are
   byte-identical across both installs; proven to discriminate by injecting a divergence
   (1 failed) and restoring it (6 passed). Suite is now **14 cases**.
2. **A dead branch inside my own C5 suite** — `_fake_world(ledger_check="raise")` wrote a script
   that exits 0 and raises nothing, and no test used it. Removed.
3. **`state-baseline.md` still taught the stale numbers.** My corrections had gone into WORKLOG,
   not back into the file the skill loads — so the next session would have read the wrong figures.
   **Fixed at source:** W5 (155 invisible ids not 100; 147 distinct / 174 occurrences; the 171
   M-form entries and the 62.6% coverage; the 27 id collisions; the enumerated-allowlist design),
   W3 (26 sources attributed, cadence-blind audit, `idb` the one real fault), W6 (sdmx_nso premise
   withdrawn), W7 (statcan complete; the parquet re-upload CANCELLED per R520).
4. **Phase 0 task 2 self-deadlocks and prescribes a refuted design.** Executing it correctly makes
   `ledger_check --digest` red, which `skill_check.py` treats as a HARD failure, bricking every
   later preflight. **Fixed:** `phase-playbooks.md` now carries the corrected design from R521
   (enumerated allowlist, `(id, heading line)` keying, no-id-reuse check, seven cases, regex +
   backfill in one commit) and an explicit deadlock warning.

**Its top recommendation, NOT done and needing a decision:** wire the skill into
`.claude/hooks/_receipt_rules.py::REQUIRED` so the existing `consequential_gate.py` *denies*
deploys, D1 writes, `--apply` and pushes to main until `SKILL.md` and `references/protocols.md`
have qualifying read receipts. Both evaluations independently reached the same conclusion: the
skill is currently the only part of the reliability system whose activation depends on the model
choosing to load it — and both repos' SessionStart context currently points at a *different*
skill (`econ-updater`). That changes the gate for every session in both repos, so it goes to
Ahmed rather than being done unilaterally.

### P0.9 — oecd "structural breaks": claim proven, proposed fix REDIRECTED, nothing shipped

* **Instrument:** adversarial reviewer re-ran the SHIPPED path against the authoritative flow list
  in `data/clean_full/oecd/_giant_state.json` (1,549 flows; `definitive_fail: 60`).
* **Result — the claim got stronger:** **60/60** return HTTP 200 with a well-formed CSV,
  `OBS_VALUE` present and **`TIME_PERIOD` absent**; zero refutations; 3/3 known-good controls parse
  normally *with* `TIME_PERIOD`. DSD census: **18/18** structures declare no SDMX `TimeDimension`,
  against 3/3 healthy controls that do. Alternative requests (`dimensionAtObservation=TIME_PERIOD`,
  and the publisher default) do not produce one — **our request is not the defect**.
* **REDIRECT, three ways, all mine:**
  1. My "13 of 60 sampled" was not a sample. `_named(cap=20)` + `_clip_err(1400)` mean only **16**
     ids reach the run note, 11 of them from two DSD families. The population was in a sidecar I
     never opened.
  2. **My replacement label would have been a new false claim.** 49 of 60 carry a time-like column
     (45 varying `REF_PERIOD`; **2 carry `PUB_YEAR` as a key DIMENSION over 23 distinct years**);
     only 11 have none. So "cross-sectional, outside the series model" is refuted by the publisher's
     own metadata, and this is **not** the `gleif` shape. Correct wording, and all the evidence
     licenses: *"the publisher's DSD declares no SDMX `TimeDimension`."*
  3. **"Only a text change" was false.** The note is built in `_common.py::finalize()`, which
     **raises**; ceasing to call `structural_unit()` lets `run_giant` stamp a catalogue vintage that
     **short-circuits all 1,545 flows next tick** and sets `last_success_utc`. The health gate is
     indifferent either way (oecd is `live:false`; both gate paths filter on `live`).
* **The correct predicate already exists in the tree:** `abs.py` gates `structural_unit()` on a flow
  that previously had rows. Measured here: **0 of 60** failing flows have a parquet vs **40 of 40**
  controls. Without it, "no `TIME_PERIOD` ⇒ not a break" would turn a genuine future OECD format
  change into a silent no-op across 1,545 flows. `jobs/ingest_oecd.py` has the identical defect and
  books all 59 as `status="full", n_obs=0` — the two-parser rule.
* **Also established:** oecd has run twice ever and has **no `source_state` row**; it is not stale
  from a landed fix but **starved** — `run_local_heavy.ps1` aborted **398 of 478** launches with
  "1 updater-daily run(s) still in flight". Cadence is monthly and it is not due.
* **Status: NOT SHIPPED.** Ledger **R523**. The work is now well-specified but is a behaviour
  change (vintage short-circuit + `last_success_utc`), needs a deliberate `ok`-vs-`partial`
  decision, and cannot be verified end to end while the source is starved.

### P0.2 — `ledger_check --digest` rebuilt to the reviewed design · DONE (Phase 0 exit gate)

Built to the design R521 validated, not the one the plan prescribed.

**Scope, measured from the document's own `## Entries` anchor rather than a line guess:**

| | before | after |
|---|---|---|
| entries the guard can see | **112** (`^## (R\d+) — `) | **468** (297 R-form + 171 M-form) |
| reaching the digest | not measured | **281** |
| not reaching it | 58 "known backlog" | **187, every one enumerated by name** |
| exemption mechanism | `RULE_FROM = 475`, open-ended | an allowlist that can only **shrink** |
| id collisions | unknown | **27 known**, a 28th fails the check |

**Backfilled the 32 post-rule entries** (R435–R466), each digest line composed from that
entry's **own `RULE`/`RULES` sentence** — wording and numbers preserved, 794 body lines read,
nothing paraphrased from a heading. Digest ids 339 → 371. **Post-rule targets now 0.**

**Shipped with its enforcement**, which is what C5 demands and what this guard never had:
`test_ledger_check_digest.py` — **14 cases, and `mutate_digest_guard.py` kills 7 of 7**
(narrow regex · newline-crossing whitespace · missing backlog passing instead of refusing ·
dropped rot check · ignored archive anchor · id-only keying · silent new collision).

**Three defects in my own work, each caught by a mutation SURVIVING:**
1. My guard's `\s*` **crossed newlines**, so a bare `## R221` heading absorbed the blank line
   and stole the next line as its title — giving the guard and the generator different keys for
   the same 14 entries, which surfaced as 14 ids failing as *both* unexcused *and* stale.
   Fixed to `[^\S\n]`, and the generator now **imports the guard's regex and key function** so
   they cannot drift again.
2. Three of my tests passed **for the wrong reason** — they failed on a different check than the
   one under test, so the mutations they were meant to kill survived. Rewritten so only the code
   under test can decide the outcome (R488, third time today).
3. The archive boundary was a heuristic ("first entry past line 2000") that put the start ~1,500
   lines late and reclassified 128 archive entries as digest content. Anchored on `## Entries`,
   and the tool now **refuses rather than guesses** if that marker is absent.

**Deadlock avoided as planned:** the regex change and the backfill landed together, so the
preflight stays green. Verified: `skill_check.py` → all 8 checks OK, `RESULT: all checks passed`.

### P0.4 — baselines captured · DONE (completes the Phase 0 gate)

| baseline | value | instrument |
|---|---|---|
| coverage | 322 served / 270 scheduled / 52 archival / **0 actionable**; 13,486,342 series | `tools/audit_schedule_coverage.py` |
| untouched files | 26 sources flagged, **0 genuinely stuck** (13 rotating, 3 wrote today, 1 pre-attributed, 3 refuted by cadence, 1 real fault: `idb`) | `attribute_stale.py` + registry cadence + latest `runs` |
| retry queue | **225,272** (was 231,782 — draining); `abs` 100,000 = **2 × CURSOR_CAP**, `ilostat` 50,000 = 1 × cap; `usda` 48,047 and `imf_qgfs_direct` 20,502 are real single-run counts; `cso` drained **7,256 → 0**; 3 rows now at attempts=3 | `GROUP BY enqueued_utc` on `csv_retry_queue` |
| sources-endpoint cost | **1,442 rows read, 7.1 ms** — the flag CLOSES, no fix needed | one live run of the exact `sql.ts` query, reading `meta.rows_read` |
| ledger | **468 archive entries**, 281 reach the digest, 187 enumerated, 27 known collisions | `ledger_check.py --digest` (rebuilt) |
| NUMBERS.md | 142 rows, all instrumented, none stale >30d | `ledger_check.py --numbers` |
| reliability system | 33 checks passed; cost-guard suite passed | the two hook test scripts, run directly |
| collision census | **423/430 stores, 6,201,382,580 rows, 12 offenders** | `dupsweep.py` v3 |

**PHASE 0 EXIT GATE: PASSED.** `--digest` now covers every entry heading and states its scope
explicitly; the enumerated backlog can only shrink; every baseline row above carries an instrument
and a date; both briefs are filed (`docs/briefs/PHASE0_BRIEFS.md`), with `sdmx_nso` resolved as
not-a-reserved-item and `worldbank_pink` awaiting Ahmed.

**Two Phase-1/2 tasks are removed by these measurements**, which is worth stating because the plan
still lists them: the sources-endpoint materialisation (the cost is 1,442 rows, not millions), and
the untouched-file attribution (done, and zero sources are stuck).

**Operational note for the next session.** The reading gate refused two commands today by
classifying the *text being written* as a script or as SQL — both were heredocs writing markdown.
It fails closed, which is correct, but the refusal aborts the whole compound command, so earlier
harmless statements in the same line are discarded: two file appends were silently lost and only
surfaced when the following commit reported "no changes added to commit". Keep file writes and
anything SQL-shaped in separate calls.
