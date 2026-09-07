# Protocols — the action-bound checklists

Load the section you need **immediately before** the matching act. These checklists exist because rules indexed by topic do not fire when the violation is a keystroke (R348): the check must be bound to the act.

---

## 1. Null-result protocol (before trusting or reporting any "absent / missing / 0 found")

Every one of these steps is one command. Skip none.

1. **Positive control IN the probe list.** Include at least one item you would bet on being PRESENT (e.g. `penn_world_table` in an absence sweep, a known-live id in a 404 sweep). If the control does not come back present, the run is VOID — do not report anything from it (R338, R329).
2. **Print ONE raw record** beside every count — the whole JSON entry / DB row / API response — and confirm the keying by eye before believing the count (R433, R478). A uniform negative across every key is almost never the data (R478).
3. **Round numbers in the all-failed direction are a reader-bug alarm**, not a finding: `0 of 61`, `0 of 6,776`, `796,716 of 796,716` (R484). Genuine publisher gaps are usually partial.
4. **A denominator must travel with every count** — what was examined AND what could not be reached. "0 defects in 0 files examined is not a result" (R0 sub-rule 3, R330).
5. **Absence from a listing is absence from that listing, never from the world.** Before filing anything as discontinued/deleted/retired, request the DATA endpoint, not the index (R510, R61, R75).

## 2. Destructive-operation protocol (before any delete / purge / re-key / bulk write / shrink)

A destructive operation gets a parallel adversarial review **before the write** and a post-state review **after**. Nothing in the checklist is skippable because the plan "feels" small.

1. **Write the DESIRED END STATE first**, and make every writer idempotent toward it (R107). The plan is the desired state; a diff is the work list, not the plan.
2. **Print the delete-set before deleting.** Name every affected id/object/count. If the tool cannot print it, do not run it (R10).
3. **Guard the POST-state, not just the selection.** State what must be true after (counts, resolvability, licence rows) and verify it with a different instrument than the writer used (R263, R503).
4. **Ground-truth parse must be non-empty and sane** before any destructive fallback; an empty parse is an ABORT, never a "nothing to do" (R10).
5. **Never weaken a guard that has fired.** `merge_and_write` / `min_ratio=0.97` / never-shrink has been right every time it has fired (R519, R305). If a write is refused, the guard is the witness and your plan is the suspect. Re-measure the plan, not the guard.
6. **A shrink or overwrite of served data is a review-level event.** Re-keying changes PUBLIC series ids → RESERVED: brief and stop.
7. **Licence authority is `DATABASE_LICENSES_VERBATIM.md`.** Never re-derive it; a diff between catalog.db and D1 finds disagreement, never shared error (R117). An un-gate lands in catalog.db AND D1 AND the R2 snapshot AND the regenerated site in one session (R358, R21).
8. **Deleting a series touches FIVE places** (R2 CSV, D1 `series`, D1 `series_fts`, local `catalog.db`, `source_counts`) — and the fifth has no foreign key to remind you (R481, R489). Enumerate all five in the plan and verify all five after.

## 3. D1 batch protocol (before any batch of statements against D1)

1. **Measure ONE statement and read `meta.rows_read`.** One query, ~13 seconds. It is the pre-flight rule (R492).
2. **The cost is per STATEMENT when the predicate column is unindexed.** `series_fts.series_id` is UNINDEXED: 23,843,482 rows per statement, and an `IN` of 200 costs the same as 1. **Raise predicate arity, never add statements** (R492).
3. **Use the PK range form for per-source work:** `WHERE series_id >= '<src>:' AND series_id < '<src>;'` rides the primary-key index. Never `WHERE source_id = ?` (full scan), never `LIKE` (also unsafe: `_` is a wildcard) (R430, R513 note).
4. **Counts come from `source_counts`** (`SELECT SUM(n) FROM source_counts`) — instant, uncounted, cached. A live `COUNT(*)` on `series` is the $82/day shape (R430, R489).
5. **Desktop-first.** Explore, count, audit against local `catalog.db` for free; touch D1 only to serve, to write, and to verify user-facing state afterwards. Local and D1 CAN disagree — a claim about what users see still needs the remote check.
6. **Respect the hooks:** `d1_cost_guard.py` refuses full-table scans past 15/hour, 40/day. A refusal is the system working.
7. **Any direct D1 write must refresh `source_counts` in the same operation** — a missing cache row silently restores the live `COUNT(*)` (R489), and a STALE one is worse: it returns a 200 with a plausible number, which is why `sec_edgar` advertised 17,437 against 17,467 rows for two days before a guard caught it (R846). The recount is an index seek — measured `rows_read 17,468` for `n = 17,467` — so run it unconditionally, not only when the write added rows: gating on "did I add anything" means a run that failed halfway can never self-heal. Use `--command`, never `--file`: the file path is the IMPORT endpoint that blocked every origin read for 112 minutes in R709. **This bullet used to say `source_counts` "has exactly one writer"; it does not, and believing so is what let the drift sit** (corrected 2026-09-07).

## 4. Deploy protocol (before and after any `npx wrangler deploy` or site publish)

1. **Know what turns a commit into a deploy for this artefact.** Worker: manual `npx wrangler deploy` from `api/worker/` (no workflow does it). Site: `workflow_dispatch` of `deploy-site.yml` (Git Provider: No). Pushing to GitHub publishes NOTHING.
2. **Deploy from the branch that IS production**, in a clean worktree; diff the artifact you are about to ship against the deployed source and require the delta to be only your change (R405).
3. **Deploy first, destructive steps after** — a resolver/deletion change must be deployed before the deletion is safe (R155).
4. **Verify the LIVE endpoint after the deploy** — a green `wrangler` step means "Cloudflare accepted the upload", not "the site serves". Wait for propagation (retry; cross-check a second endpoint reading the same state, R138, R222b).
5. **Ask what else the deploy ships.** `deploy.yml` on hf deploys Pages AND the worker on every push to `main` — a one-page edit can revert a live fix (R429). Check the deploy set before pushing.
6. After deploying a fix to a list/constant: grep the literal token in the file you edited (R125), then paste the live response into `WORKLOG.md`.

## 5. Reporting protocol (before telling the owner a number, status, or "done")

1. **Instrument + date attached, in the same breath as the number.** No instrument → do not report.
2. **The hedge goes INSIDE the sentence.** "The registry says X" is not "X"; "the log shows N" is not "N rows fetched". If unverified, say which part is unverified in the sentence itself (R410, §5.4 of the source doc).
3. **Claim Ladder — say which rung you are standing on:** (a) a file/doc says so → quote it as a claim, not a fact; (b) local store measured → say "locally"; (c) live endpoint returned → paste the response; (d) publisher confirms → paste the publisher evidence. Only (c)/(d) license "served / live / complete / verified".
4. **Alarming findings:** apply C10 — discount ~3:1; re-measure the instrument before alarming the owner (R518 overstated 440x). **Reassuring findings:** name the check that would have gone red, and whether it was ever fed a failing case.
5. **Costs and ETAs:** enumerate remaining phases from the CODE before estimating; a cost claim needs the bill/allowance model, not a daily rate (R422/R423, R502).
6. **"Done" claims use the Definition of DONE**, and nothing less.

## 6. Diagnosis protocol (before attaching a cause to an observation)

1. Write four lines BEFORE speaking: **Observation** (measured, with instrument) / **Hypothesis** (labelled as such) / **Refuting test** (the observation that would kill the hypothesis) / **Result** (run it). If you cannot name a refuting test, you have a story, not a diagnosis (R514).
2. **Corroboration counts independent vantage points, not observations.** Three readings from one machine are n=1 (R512). A different egress, a different store, a different instrument.
3. **"What it actually was" is the most dangerous sentence in a post-mortem** — only write it when the refuting test has run and passed against the leading alternative (R504).
4. **Before blaming upstream, reproduce our exact request sequence** (R18, R46); before blaming our code, check the publisher (R394, R484).
5. **A red status has a DATE.** Compare the failure timestamp against the last commit touching that code before diagnosing (R339, R277).

## 7. Long-job protocol (launching or watching anything > 60 s)

1. **Never pipe a watched long job through `tail`/`head`** — it shows nothing until exit, a healthy job looks stalled (R336/R348). Use log files, read them separately.
2. **Launch with `-u`** (unbuffered), `PYTHONIOENCODING=utf-8`, output to a dated log file; announce work BEFORE starting each unit (`>>> unit`) so the last line names the culprit (R70, R290).
3. **Judge by the ARTEFACT the job moves** (objects, rows, cursor advance) between two readings — never by process listing, CPU, or log silence alone (R453, R454, R80: a derive that overwrites leaves the object count constant by construction).
4. **Read the ARGV, not the progress line** (`Get-CimInstance Win32_Process | select CommandLine`) before concluding what a running job is (R323).
5. **Ask "what does a kill at hour N cost?"** before launching a multi-hour accumulation; resumability is a property of the INVOCATION (flags), not the tool (R418, R427).
6. **Before killing anything: grep the ledger for the thing being killed** (R458), require two independent stall signals over two ticks (R457's lesson), and confirm the PID is gone after the kill (R90).
7. **A sentinel file is an interface — ask what else reads it** (`.DONE` files, rotation bookmarks) before writing or trusting one (R475, R287).

## 8. Command-composition checklist (at the moment of composing any shell command)

Check at COMPOSITION time, because that is when attention is on the shell:

1. No `| tail` / `| head` after a long-running or backgrounded command.
2. No `&` after a command inside `run_in_background` (one or the other, never both, R131).
3. No heredoc feeding code to a shell — write code with the file tool and run the file (R92, R476). If a heredoc is unavoidable, quote the delimiter (`<<'PY'`).
4. Ledger/git writes use `git -C D:/research/hfdatalibrary` (absolute, no `cd` before `git add`, R135).
5. Every command whose meaning depends on cwd carries its own `cd`/`-C` (R89).
6. Verify a push by STATE, not output: `git rev-list --count origin/main..HEAD` must be 0 (R42, R445). Never `push && echo PUSHED`-style success strings.
7. No `git add -A` in a repo whose tree doubles as a build-artifact staging area (R370).
8. PowerShell scripts: ASCII-only or UTF-8-with-BOM (R196); never generate them through bash heredocs (R203).
9. Pipeline exit status: capture `$?` / `$LASTEXITCODE`; a pipeline exits with its last element (R319).
10. Non-ASCII output: set `PYTHONIOENCODING=utf-8` on every background job and use the pinned interpreter (`C:\Users\aelkassabgi\AppData\Local\Programs\Python\Python314\python.exe` for the workstation guard scripts) (R126, R443).

## 9. Ledger protocol (immediately after any mistake is discovered)

1. **Entry + digest line in the SAME commit**, appended at the anchor (never rewritten, never `-A` staged). Run `ledger_check.py --digest` after (R328, R485, R247).
2. State what was claimed, what was true, the mechanism, and a RULE. Corrections are appended as new entries naming the retraction (R518's pattern).
3. A rule written down today does not protect tomorrow unless it ships with a check, a hook, or a tool (R374, R383) — name the enforcement in the same entry.
4. Before writing code that KILLS or DELETES anything, grep the ledger for the thing being killed (R458).

## 10. Source protocol (one source, end to end, nothing else)

1. Read the source's runbook (`docs/runbook/<id>.md`); grep the ledger for its id; read its fetcher header, its registry entry, its licence verdict in `DATABASE_LICENSES_VERBATIM.md`.
2. **Establish which of the two parsers produced the bytes you are looking at** (ingester vs fetcher — a fix in one is not shipped in the other, R333).
3. Check grain: does the key carry the publisher's own Fact key dimensions? (`$metadata`, dimension list, worksheet names — R515/R519/R480.) `COUNT(*)` vs `COUNT(DISTINCT <candidate key>)` is one query (R387).
4. Work read-only until the diagnosis is tested; verify against the publisher for any "publisher does X" claim (R484).
5. DONE only when: catalogued + served + verified live + scheduled + licence-verified + recorded. Report the fraction scheduled (R158).

## 11. Reserved-decisions protocol (whenever a RESERVED item appears)

1. **STOP the work item.** Do not decide, do not "prepare the way by executing", do not ask twice (R460, R250).
2. Prepare a **one-page brief**: the question, re-measured facts (each with instrument + date), the options, your recommendation, the cost, and the exact authorisation you need. Facts in the brief are re-measured NOW, never copied from the plan or a ledger line (R500, R509).
3. Deliver the brief and record it in `WORKLOG.md` with its date; continue with any non-blocked work.
4. The list: deleting non-re-crawlable data · un-gating a DISPUTED licence · auth & billing · sending email as Ahmed · ANY change that alters PUBLIC series ids (incl. every key-collision re-key in Phase 4) · `/v1/stats` publication · health-gate policy for bounded broken minorities · serving cross-sectional data (`oecd`, `gleif`) · `norgesbank` un-gating provenance · `GATED` 26 rows · `sec_edgar`/`sec_edgar_xbrl` id crossing.