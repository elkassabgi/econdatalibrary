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

---

## P1 — string repairs, and the third holder of the constant (2026-08-30)

**The reading gate is satisfied for the deploy.** Instrument: `_receipt_rules.classify()` +
`unread()` evaluated against `.claude/state/reads.json` for this session, 2026-08-30 —
`cd E:/research/econfindatalibrary/api/worker && npx wrangler deploy` classifies as `deploy`,
requires `{adversarial-review SKILL.md, econ CLAUDE.md, ledger (MISTAKES.md digest)}`, and all
three carry qualifying region receipts. The ledger regions `[[1,225],[401,490]]` took six chunked
Reads; no single Read can span them (the 400 dense digest lines exceed the Read tool's 25k cap).

**What changed, and the one I nearly missed.** The plan named two holders of the coverage
constant — `api/worker/src/catalog.ts::COVERAGE` and `api/CONTRACT.md`. There is a **third**:
`api/devserver.py:56::_CATALOG_COVERAGE`, the dev shim, which still read
`"series-level for 33 sources; source-level for the rest"` after I had "finished" Phase 1.
Instrument: `grep -rn "catalog_coverage" clients/ api/ site/ docs/`, 2026-08-30, which I ran to
answer a different question (does any client PARSE this field — none does, so the string is safe
to change). The sweep found the miss; my own edit pass had not.

That is Ahmed's example-means-class rule doing real work: one reported instance is one instance of
a class. The zero-result check that closes it — `Grep "series-level for 33 sources"` over the repo,
2026-08-30 — returns exactly two hits, both legitimate historical quotes: the plan document
describing the defect, and my own explanatory comment in `catalog.ts` quoting the old value. **No
live surface still carries the stale string.**

All three now read `"series-level for every served source"` — deliberately carrying no number,
because the previous value was accurate when written and rotted silently for months.

### The review FAILED it, and it was right — the repair was a new false claim

`"series-level for every served source"` is **false**. Reproduced independently before acting
(R325), 2026-08-30, `sqlite3` read-only over `data/catalog.db`, `SELECT source_id, count(*) FROM
series GROUP BY source_id`, joined against `SUPPORTED_SOURCES` minus `denylist.ts`:

| served source | catalogue rows | real series (per `util.ts`'s own comment) |
|---|---|---|
| `ons_uk` | **42** | 3,897,884 |
| `insee_melodi` | 139 | 139 flows / 36,436,053 rows |
| `istat` | 14,267 | 43,564,079 |

plus the registered sets in `clients/python/econdl/_resolve.py` — `_FLOW_GRAIN` (11) and
`_DOT_TABLE_GRAIN` (13). **A second review FAILED my first version of this table**, which also
listed `statcan` 20, `oecd` 28, `abs` 18 and `bls` 9 as coarse-grain. They are not: all four are
small hand-curated **per-series** catalogues (`bls:CUUR0000SA0` is one series), carrying a scalar
frequency AND geography on 100% of rows where the genuine coarse ones carry 0%. I had inferred
grain from the row count — R141's inversion, committed while writing a fix about grain. The bad
table reached the **published** `api/CONTRACT.md` before it was caught. My follow-up census then
erred the other way (classifying 274 sources coarse on "no scalar attributes"): `wid` has
2,465,197 such rows and every one names a single series. **Row count predicts grain in neither
direction, and only the positive test — scalar attributes on every row ⇒ per-series — is sound.**

Those are **table and flow grain**, and the sources' own generated pages say so
(`catalog/site/istat.html`: "Served at FLOW grain"). So the old string's second clause,
"source-level for the rest", was the *true* half — and my repair deleted it. `catalog.ts:7` states
why the field exists: "so absence is never read as nonexistence". A caller who searches for an
ISTAT indicator, finds nothing, and reads "series-level for every served source" concludes the
series does not exist. It does, inside one of 14,267 flow CSVs. **A stale number is rot; that
would have been a correctness regression, worse than what it replaced.** Not deployed — caught
before shipping, which is the whole point of running the reviewer in parallel.

Two further corrections from the same review, both verified here:
- **Served = 321, not 322.** `SUPPORTED_SOURCES ∩ NON_REDISTRIBUTABLE = {dbnomics,
  worldbank_pink}` — I had missed `worldbank_pink`, which is the very source I filed a RESERVED
  brief about. `docs/ECONLIB_COMPLETION_PLAN.md:78` carries the same 322.
- **`unctad_cpia` is a LIVE array member**, not comment-only as my new `util.ts` comment claimed.
  Only `ksh` is comment-only. Corrected in place.

The string now reads `"mixed grain: some sources are catalogued per series, others per table or
flow — absence from this catalogue does not mean a series is unavailable"` in all **three**
holders. It asserts no count and no uniform grain.

**And it is now pinned mechanically** — `tests/test_catalog_coverage_sync.py`, 4 checks: the three
holders agree, the string embeds no digit, and it keeps the absence caveat. Nothing had ever
mentioned this field in the **98** test files under `tests/`, which is exactly how "33 sources"
survived months of growth past 300. Discriminating pair per R414, now
`tools/mutate_coverage_guard.py` — in the repo, because the test cites it and a harness cited in
shipped code but living in a scratch directory is unreproducible for the next reader.

**11/11 scenarios**, and the harness found **three real guard bugs that the passing tests could
not** — every one of them in the EXTRACTOR, not in the assertions:
1. The old string contains a `;`, so my non-greedy regex truncated inside the string literal,
   extraction returned EMPTY, and the no-count and caveat checks passed **vacuously** (R413's
   cannot-fail comparator, inside the guard written to prevent exactly that).
2. A **decoy** `const COVERAGE` planted in a `/* */` comment was read in preference to the live
   one — so the guard could pass while the deployed value was the original bug.
3. Requiring the word "absence" accepted the sentence's own **inversion**: "absence from this
   catalogue *means* a series is unavailable" contains it and asserts the opposite.
Fixed by a character-scanning comment stripper that never touches string literals, a
uniqueness assertion on the declaration, a non-empty assertion, and a polarity check that
requires the negation in the same clause. **A guard is defeated where it READS, not where it
asserts** — and no passing test can reveal that.

**R-client sweep was incomplete, and one surface reaches users.** Beyond `.zenodo.json` and
`gen_site.py` I had missed `CITATION.cff:31` (published citation metadata — the reviewer missed
this one too; found by my own grep) and `zenodo_README.md:48`, which ships *inside* the Zenodo
deposit whose `.zenodo.json` I had just corrected: the two disagreed within one record. Both
fixed. `STRATEGY.md` is **gitignored** (`.gitignore:78`), so it is a private planning doc and was
never a public claim; annotated locally anyway. **Still outstanding and NOT fixed:**
`catalog/site/index.html:487` and `:526` are GENERATED and still advertise "Python and R clients"
— `gen_site.py` is only the template. The live homepage keeps serving that claim until the site is
regenerated *and* manually Pages-deployed (R345). I am not calling this item done.

**Test suite: 713 passed** (`py -3.14 -m pytest tests/ -q`, 36m57s, 2026-08-30). `tsc --noEmit`
exit 0. `py_compile` on the dev shim OK.

**`catalog/sitemap_state.json`: commit it, separately — the risk runs the opposite way to my
assumption.** The reviewer established that the file has been lagging its own pages since
2026-08-05 while `catalog/site/` was committed twice (2026-08-24, 2026-08-26). Its working-tree
content matches the committed `sitemap.xml` exactly (331 pages, 328 × `2026-08-24`, 3 ×
`2026-08-26`). `gen_site.py:156-160` keeps a page's `lastmod` only when the stored hash matches;
the *committed* state matches none of the current 331 pages, so **reverting or omitting it makes
the next run stamp every page with the run date** — a false site-wide date bump, precisely what
the mechanism exists to prevent. It is not deployed either way (`SITEMAP_STATE` sits outside
`OUT_DIR`). Committed on its own, described as the lagging companion of `beb78ca78`, not folded
into a commit about `catalog_coverage`. One thing remains **UNVERIFIED**: its mtime is today
18:30 but its `generated` field is `2026-08-26`, and a run today would have stamped today — so
its *content* is the 08-26 run's and what touched the file is unexplained.

---

## P1 exit gate MET, and a licence leak found by meeting it (2026-08-30)

**Deployed and verified live.** Worker `econdl-api`, version `000baa89-b800-4ef6-b481-5abde1969266`,
then `0ddc1cf7-5e08-4396-9dec-df302653e0f7`. Pasted, not asserted:

```
GET /v1/catalog?limit=1
"catalog_coverage":"mixed grain: some sources are catalogued per series, others per table or
 flow — absence from this catalogue does not mean a series is unavailable"
```

The response's own first row corroborates the grain correction: `abs:ANA_AGG:M1.GPM.20.AUS.Q`
carries `"frequency":"Q"` and `"geography":"AU"` — `abs` is per-series, the very thing I had
wrongly called table-grain.

### Verifying my own deploy found a redistribution control failure

**`worldbank_esg` was serving 178 ILO-sourced unemployment series ungated**, advertised
cc-by-4.0 / reservable / commercial_ok. Ledger **R32** verbatim — "a carve-out keyed on one
source id does not cover the others" — in a rule that *names* `worldbank_esg`. The 2026-07-22
fix closed `_wdi` and left `_esg` open. Pre-existing; no commit of mine touched `denylist.ts`.

The discriminating instrument (`index.ts` runs `isGated` **before** `requireDownloadAuth`, so a
gated id 451s pre-auth and 401 proves the gate did not fire):

| series | before | after |
|---|---|---|
| `worldbank:SL.UEM.TOTL.ZS:AGO` | 451 | 451 |
| **`worldbank_esg:SL.UEM.TOTL.ZS:AGO`** | **401** | **451** |
| `worldbank_wdi:FP.CPI.TOTL.ZG` | 451 | 451 |
| `worldbank_esg:AG.LND.FRST.ZS:AFG` (control) | 401 | 401 |

The control matters: the fix gates the ILO indicator without over-gating the source.

### The browse defect, fixed and verified

`?source=worldbank` returned **`total=692` with an empty results array** — offsets 0–193 blank,
because `FP.CPI` sorts first and all 195 of its rows are carved. The gate was a JS filter applied
to a page SQL had already cut. Search was worse: `?q=unemployment&source=worldbank` returned
`total=235` and nothing on **every** page.

```
before:  ?source=worldbank&limit=3   -> total=692  returned=0
after :  ?source=worldbank&limit=3   -> total=262  returned=3
                                         worldbank:NY.GDP.MKTP.CD:ABW / :AFE / :AFG
before:  ?q=unemployment&source=worldbank -> total=235  returned=0   (inconsistent)
after :  ?q=unemployment&source=worldbank -> total=0    returned=0   (consistent — all carved)
```

No collateral damage — `abs` 18, `bls` 9, `istat` 14,267, `ons_uk` 42, all unchanged. The
exclusion is built **per source**, so the ~318 sources without carve-outs pay nothing; it is
index-resident (billed `rows_read` unmoved), and only the 2–3 carve-out sources take a bounded
PK-range count instead of the carved-inclusive `source_counts`.

Two latent SQL defects closed with it: the `<src>:<ind>:` prefix could never match a **two-part**
id (so `worldbank_wdi` and `worldbank_pink`'s SQL exclusion had always matched zero rows — the JS
gate covered them, but `worldbank_pink`'s seven REFUSED-in-writing metals would be exposed the
day that source is un-gated), and `_` is a LIKE wildcard present in two of the three carve-out
source ids.

**Still open, unchanged:** `q=gdp` returns `worldbank:NY.GDP.MKTP.CD:XD` first — one of R524's
eight advertised-but-unresolvable ids. Recorded, not fixed; the remedy is update-path work.

---

## RETRACTED: the re-key authorisation was requested on wrong numbers (2026-08-30)

Ahmed authorised re-keying five sources ("go") on five figures I presented as collision counts.
**Every one is a catalogue row count** — `SELECT COUNT(*) FROM series WHERE source_id = ?`
reproduces all six exactly (verified myself after the adversarial review returned STOP). I
showed him how many public ids would *break* and called it how much damage would be *repaired*.

True collisions: eia **145,248,181** (541× my figure) · idb 14,734,403 (782×) · unctad ×2
603,467 (16×) · damodaran **1,849** (13× smaller) · usda 65,122. And the remedy is wrong for
all five — eia needs `period` added to its dedup key (no id changes; 958,244 of 958,293
collisions resolved in one measured row group); idb and both unctad stores hold only
`[series_key, obs_date, value]` so **cannot** be re-keyed from disk; damodaran is already
handled; usda is an ingester-key fix. Unpriced: ~50–57B D1 rows read (52× the daily scan
budget) and no id-alias mechanism exists for the 418,435 ids.

**Nothing was executed.** Full retraction and corrected work plan in
`docs/briefs/PHASE0_BRIEFS.md`; ledger **R527** with the structural rule: a number that
underwrites an authorisation lands in NUMBERS.md with its instrument BEFORE the ask. The
instrument that produced the bad census (`tools/audit_dedup_uniqueness.py`) is fixed in
`3b4cf8ea0` — an assumed key now announces itself and a source where nothing was checked exits
non-zero instead of wearing a pass's face. The four id-preserving fixes await Ahmed's word.

---

## The 7-day local silence, root-caused (2026-08-31)

The health gate on run 33358938352: *"ROUTE 'local' SILENT — 18 live source(s) run there and
NOT ONE has succeeded within 3d (newest success: 7.2d ago)"* — `bea` RED-DATA, `eia` 8d stale
on a daily cadence, `statcan` RED-DATA 9d, plus `census`, `noaa`, `oecd` and the unctad giants.

**Not a dead machine.** The guard loop is alive (heartbeat stamped minutes before the check)
and push-state works (`bis` and `faostat` show 1d in the gate's own table). Three stacked
causes, each measured:

1. **CI-in-flight deference.** The local runner refuses to start while any updater-daily run
   is in flight. On 2026-08-29 the guard launched it **203 times** and the sampled tick says
   `ABORT: 1 updater-daily run(s) still in flight`. With four daily cron windows, free local
   windows are scarce — the design intent is one ~20h-cadence pass per night, which did run
   on 08-28 and 08-30/31.
2. **The one nightly pass gets eaten by a mis-banded giant.** Last night's pass
   (`local_heavy_updater_20260830-234131.log`): budget clamped to 153 min to end before the
   03:00Z CI window; istat 39s (upstream outage, clean skip); `unctad_biotrademerch` 516s;
   then **`unctad_tradefoodcatbyproc` consumed the remaining ~154 min and was hard-killed**.
   `bea, bls, census, eia, noaa, oecd, ons_uk, statcan…` — never attempted.
3. **The kill erases its own evidence, so the loop is self-sustaining.** `store.log_run`
   fires only from inside Python (orchestrate.py:1847); a taskkill from the runner writes NO
   run row. `run_cost_estimate()` is MAX(dur_s) over the last 5 *recorded* runs, so the
   killed giant keeps its stale cheap estimate, re-enters the cheap band of the ladder, and
   leads the queue again tomorrow. Its own docstring calls under-estimation "the failure the
   lane exists to prevent" — the external-kill path produced exactly that, invisibly. This is
   R273's killed-before-bookmark loop one level up, at the scheduler.

**Fix built (under parallel review, uncommitted):** `tools/record_killed_unit.py` — parses
the killed pass's log for the `>>>` without a `<<<`, attributes the unaccounted elapsed
(floored at 60s; over-estimation is the documented safe direction) as one `killed_external`
run row — wired into `run_local_heavy.ps1` between the kill and push-state, because
pull-state replaces local state wholesale (R340) so a row written at any other moment is
lost. 4/4 tests including the end-to-end assertion that the estimator's answer actually
rises. Dry-run against the real log attributes **9,233s** to `unctad_tradefoodcatbyproc`.
En route, R153 twice in one file: I called `State` when the class is `StateStore`, and used
`$updaterStart` before declaring it — both caught by running/parsing, not by reading.

---

## oecd: the 60 no-TimeDimension flows stop poisoning every run (2026-08-31, `f9f529d41`)

`finalize()` raises on any structural count; oecd's 60 flows whose DSD declares no SDMX
TimeDimension (reviewer verified at population: `_giant_state.json` = 1,549 flows, exactly 60
`definitive_fail`, **0 of them with any on-disk parquet**) were misfiled as breaks, so every
oecd run went `definitive_fail` and the 1,545-flow giant starved. Now: a PROVEN SDMX-CSV
header without TIME_PERIOD → `no_time_dimension`; abs.py's had-rows predicate keeps a genuine
vanished-column break loud; never-had-rows flows park at the publisher's vintage (re-probed on
a version bump — pinned against the REAL `select_flows`); `finalize` carries a named,
non-demoting note. Review-hardened: the SDMX marker requirement (a plain-text 200 error body
must fall structural, never park silently for years), the ingester's split path fixed too, and
the tests now drive the REAL `run_giant` (the first branch test ran zero lines of it). 11/11.

## QoG refused; recorded, deleted, and a loaded gun defused (2026-08-31, `5aa5a1d97` + `9217c25f0`)

Written refusal from the publisher recorded VERBATIM in the canonical licence file; trail row
DENIED; reply draft for Ahmed at `docs/briefs/QOG_REPLY_DRAFT.md`. Nothing user-facing changed
(already denylisted, 0 catalogue rows). The dormant holdings (23 MB store, fetcher, ingester)
deleted after their adversarial review — measured clean in every place a series lives first,
including the reviewer's two additions (uppercase R2 prefixes; D1 `source_counts`, the R489
fifth place my brief missed). Resurrection paths closed (capability matrix + classifications
stripped; runbook regenerated). And the reviewer found `tools/purge_unpermitted_r2.py` still
naming SERVED sources (vdem, wid) in a "gated, no permission" purge list — one re-run away
from deleting served data; it now refuses loudly until its list is re-derived and reviewed.

---

## Phase 2 task 1 COMPLETE: series_fts rebuilt, swapped, verified live (2026-08-31)

Built server-side from D1's own `series` in 422 PK-range chunks (journal + stop-not-retry),
**10,348,426 rows == the same-day `series` count exactly**; atomic RENAME swap; post-swap
`sync_catalog_d1` applied 66,721 pending rows / 75 files; the three frozen workflows
re-enabled after it. Live acceptance, every row in the predicted direction:

| query | before | after | prediction |
|---|---|---|---|
| `disposable` | 35,684 | **35,493** | 35,493 |
| `unemployment` | 22,108 | **14,011** | 14,011 |
| `inflation` | 7,524 | **3,534** | 3,531 (+3 = D1's known 243-row lineage lead; none updated today) |
| `wid disposable` | 33,390 | **33,390** | survival, exact |
| `boc q=Lynx` | 252 total / 16 distinct | **14/14/14** | repeats dead |
| zillow orphans | findable | **0 in results** | ghosts gone |

Mid-build the DB hit D1's **10 GB ceiling** (old 23.7M-row index + 7.06M new rows =
9,991,606,272 bytes) — the one constraint neither the design, nor four probes, nor two
adversarial reviews had priced, though the number sat in the plan docs (ledger **R528**).
Every designed safety held: the failed chunk rolled back atomically (journal == table to the
row), the driver stopped, the old index kept serving. Recovery improved the plan: dropped the
old index first (9.99 → **5.95 GB**; the reviewed LIKE fallback carried search live at the
clean predictions), resumed from the journal, swapped with a bare RENAME. Second lesson: the
DROP returned error **7429 (timeout, object reset)** having SUCCEEDED — verify state, never
the error string.

The search index now means what it says: no duplicates (was 2.30× on the primary), no orphans
(was 1,052,814 incl. ghosts of retired zillow), totals == reality.

### Phase 2 tasks 2 + 3, and the exit gate's first half (2026-08-31)

* **Task 3 — title consistency: PASSED AT FULL POPULATION, not the plan's 120-id sample.**
  One statement joins every FTS row to `series`: **0 mismatches** on title OR geography across
  all **10,348,426** joined rows (`rows_read` 41,393,704, ~$0.04). `series == series_fts ==
  10,348,426` exactly.
* **Task 2 — source_counts reconciliation: ZERO DRIFT, ZERO UNCACHED.** One statement, whole
  cache vs per-source truth: 321 == 321, drifted 0, uncached 0. (The cache stores RAW counts
  by design; the three carve-out sources take the bounded visible count live in the worker —
  yesterday's fix — so cache==COUNT(*) is the correct invariant here.)
* `ledger_check --titles wid boc worldbank bls`: PASS — wid covers all 2,465,197 (was 4×
  duplicated), boc all 12,862 (was 8×), worldbank 692, bls 9.
* **Exit gate: HALF met.** "Zero drift on two consecutive runs (one immediate, one after the
  next sync)" — run 1 is the zero above; run 2 is calendar-bound to the next scheduled sync
  (18:00Z drain or tomorrow's 06:00Z). Re-run the one-statement reconciliation after it.

### Phase 3 task 1a — the retry-queue's round numbers, provenance ESTABLISHED (2026-08-31)

`abs` 100,000 and `ilostat` 50,000 are **cap artefacts, twice over**: each cohort is a run's
changed-cursor set truncated at `CURSOR_CAP = 50_000`, whose entire csv phase then hit the
60-minute `UnitTimeout` and enqueued the whole batch. Instrument: the queue rows themselves —
every one carries `attempts=1` and the same error (`csv_derive crashed: UnitTimeout('<src>/_all
(csv phase) exceeded its 60-minute hard limit'`); ilostat's 50,000 enqueued on ONE day
(2026-08-19), abs's 100,000 = two 50k batches (2026-08-18..24, rotation shifted the window).
Queue total 225,272 across 9 sources; the small ones drain (cso reached 0), the two capped
cohorts cannot — 50k CSVs do not fit a 60-minute window, so the same timeout that filled the
queue also guards its drain. The fix is per-source csv budget/pacing (task 1c's territory),
NOT touching `merge_and_write`/`min_ratio` (R519).

### Phase 3 task 2 — the unmapped keys, attributed per class (2026-08-31)

Current state (the plan's 73,125 was an earlier snapshot; today's `unit_state` notes total
**43,528** across 15 sources, plus two false members):

* **cso 39,442 (90.6%)** — flow-grain by design: per-series store keys against a 7,896-flow
  catalogue. The note itself says "served ids coherent"; nothing to fix (R359's non-demoting
  residue doing its job).
* **~3,626 across 12 sources** (defillama 2,731, ipea 257, rba 225, who_sdg 235,
  unctad_cpi_annual 215, imf_* smalls, stat_latvia 9, statfin 22, ksh_stadat 60,
  unctad_rca 55, imf_fas 103, imf_fsibsis 141) — the notes' own classifier: "UNDER the
  5,000 cap — cause is NOT the cap" (or trivial residue on over-cap catalogs). These are
  uncatalogued store series (the dark-series class): the fix is ADDITIVE cataloguing per
  source, queued as ordinary work, or documented residue.
* **insee_bdm and usda are NOT unmapped** — their notes are honest budget DEFERRALS
  ("derive budget spent — 46,363 / 48,047 deferred, none failed"), which drain when their
  runs reach them; usda runs local, so the starvation fix directly helps it.

Task 2's "establish why per source" is therefore done; no mapping-ladder defect found in the
current fleet — the two real classes are by-design flow residue and uncatalogued store keys.

### Phase 3 task 4 — the perpetual-partials, first triage (2026-08-31)

36 units currently `partial`, five causes (one query over `unit_state`, R359's classify-the-
notes method): **15** transient tails (self-healing); **7** honest budget deferrals (drain);
**2** never-shrink refusals = the two unctad collision sources (`refusing shrink
648,241→362,203`) — the guard doing its job, waiting on the Phase-4 decision; **1** timeout =
`ember` (97,334 changed series, csv-phase UnitTimeout — the same fence bug the re-raise fix
under review closes; its crash path queued 0 because ember's colon-free keys are filtered);
**11 "other"** needing per-source reads (e.g. `treasury`: "177 changed series_keys have no
catalog mapping"), queued as individual items.
