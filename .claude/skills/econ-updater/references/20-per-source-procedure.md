# The per-source procedure — one database, end-to-end

This is the systematic core Ahmed asked for: go database by database, KNOW how it functions
before changing it, finish it completely, prove it, document it, move on. Exact commands for
each step live in `30-serving-pipeline.md`; this file is the order and the discipline.

## Step 0 — choose ONE source

Priority: (a) the in-progress source — FINISH IT FIRST; (b) the top ACTIONABLE item in
`50-queue.md`; (c) the reddest *genuinely failing* live source in the latest updater-daily run.
"Genuinely failing" means the evidence POSTDATES the last fix of that source (R339) and is not
a `partial` that is actually budget deferral or csv-coherence backlog (R303/R231 — read the
`last_error` text, it names its own cause).

Never two sources at once. Never "while I'm here" fixes to a second source.

## Step 1 — the 5 reads (NO edits before all five)

```bash
# 1. the runbook page — generated from the system, cannot drift
cat docs/runbook/<source>.md
# 2. every ledger mention — the mistakes already made on this source
grep -n "<source>" D:/research/hfdatalibrary/.claude/MISTAKES.md
# 3. the fetcher header (if one exists) — its own contract and warnings
head -80 updater/strategies/fetchers/<source>.py
# 4. the registry entry — strategy, cadence, live, run_location, notes
grep -n -A 30 "source_id: <source>$" updater/registry.yaml
# 5. the licence verdict — canonical, never re-derive
grep -n -B 2 -A 15 "<source>" DATABASE_LICENSES_VERBATIM.md
```

If the licence verdict is DISPUTED or the source is on the RESERVED list in `50-queue.md`:
**stop here**, write `.claude/STOP_REASON`, ask Ahmed. Working a reserved source is how a day
gets spent producing something that cannot ship.

## Step 2 — measure the current state (numbers, not impressions)

```bash
# what the store believes
python -c "import sqlite3;c=sqlite3.connect('file:data/_aqueduct/state.db?mode=ro',uri=True);\
print(c.execute(\"select unit_id,status,last_success_utc,last_attempt_utc,last_error \
from unit_state where source_id='<source>'\").fetchall())"
# what the catalogue has
python -c "import sqlite3;c=sqlite3.connect('file:data/catalog.db?mode=ro',uri=True);\
print(c.execute(\"select count(*) from series where source_id='<source>'\").fetchone())"
# what a user can actually reach (LIVE, not local)
curl -s https://econdl-api.elkassabgi.workers.dev/v1/sources | python -c \
"import json,sys;d=json.load(sys.stdin);print([r for r in d.get('sources',d) \
if isinstance(r,dict) and r.get('source')=='<source>'])"
```

Write down the three numbers. They are the before-picture every claim at the end is judged
against.

## Step 3 — plan the smallest change

- New source with no fetcher → pick the strategy by what the PUBLISHER offers (see the
  strategy families in `00-architecture.md`); pattern-match an existing healthy fetcher of the
  same family rather than inventing.
- Grain decision needed → arithmetic first, against measured D1 headroom (~1.65M rows,
  794.4 B/row). eia at series grain was 218% of headroom, cepii_baci 1172% — the number IS the
  answer; do not re-argue it (R330-class).
- Existing source broken → reproduce it TODAY before fixing (R318/R339): a stored verdict is a
  snapshot of the last run, and half the "broken" sources this month were stale verdicts.
- If the fix touches shared infrastructure — `_common.py`, `_giant.py`, `orchestrate.py`,
  merge, `core/`, `api/worker/` — it needs: strict necessity for THIS source, the full suite
  green, and a NEW test that would have caught the defect. Otherwise find a source-local fix.

## Step 4 — implement, with the suite as the net

```bash
python -m pytest tests/ -q          # must be green BEFORE you start (know your baseline)
# ... edit ...
python -m pytest tests/ -q          # and green AFTER; new failure = your change, no exceptions
```

Registry entry added or removed? `config.EXPECTED_SOURCE_COUNT` moves in the SAME commit, and
validate the way production validates (R347):

```bash
python -c "import sys;sys.path.insert(0,'.');from updater import registry,config;\
print(registry.validate(registry.load(),expected_count=config.EXPECTED_SOURCE_COUNT) or 'OK')"
```

Failure paths inside loops get LABELS naming the sub-unit and cause — the ratchet test
enforces the count only falls. YAML prose fields are quoted or `>-` block scalars — a bare
`: ` inside a plain scalar kills registry.load() for the whole fleet (R258, and again
2026-08-04).

## Step 5 — prove the fetcher against the real system

```bash
# a real run, forced, with rows counted — "0 units processed, exit 0" proves NOTHING (R50)
AQUEDUCT_BACKEND=r2 PYTHONIOENCODING=utf-8 python -m updater.run --source <source> --force
# or via CI, which is the environment that matters:
gh workflow run updater-daily.yml -f source=<source>
gh run watch <id> --exit-status && gh run view <id> --log | grep -E "<source>"
```

The proof is `ok`/`no_change` WITH positive evidence the fetcher executed (rows merged,
vintage token called). Remember the run may echo the OLD stored state in its summary — read
the orchestrator lines for THIS run, not the state dump (R347's near-miss).

## Step 6 — if the serving surface changed, the FULL pipeline

Follow `30-serving-pipeline.md` exactly: derive (byte-verified) → verify both directions →
catalogue (licence gate first) → D1 sync (which now carries the parent source+license rows —
without them the source is fetchable but INVISIBLE in /v1/sources) → util.ts →
**`cd api/worker && npx wrangler deploy`** → live `/v1/sources` check. The deploy is manual
and mandatory: an undeployed util.ts edit is the R345 failure, 425,462 series "live" at 501.

## Step 7 — close the loop

```bash
python tools/gen_runbook.py                       # regenerate (fast; regenerates all pages)
git add -A && git commit && git push              # numbers in the message, not adjectives
python tools/audit_schedule_coverage.py | tail -5 # the only progress number worth reporting
```

Report: `N of M sources / X of Y series scheduled` + the one source completed. If anything
surprised you — a mistake, a false assumption, a guard that fired — it goes in the ledger NOW
(append at anchor, verify count, R247), not at session end.

Then, and only then: back to Step 0 for the next source.
