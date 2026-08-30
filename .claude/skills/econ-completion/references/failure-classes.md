# Failure classes and seductive assumptions — the audit lens

Derived from the source document's §8 analysis of all 519 ledger entries. Load this file whenever a result surprises you, before reporting it.

## The two meta-rules

1. **Discount alarming findings ~3:1.** Among claims that reached the owner, inflation outnumbered false all-clears 28:10 (73.7%). But in the recent block it was 17:14 — because inflation is loud and gets corrected, while false all-clears stay silent until the owner, a user, or the invoice finds them (the three most expensive incidents — R421, R430, R404 — were ALL silent false all-clears detected outside the system). Practical reading: when something looks broken, first ask *what did the instrument actually measure?* When something looks fine, ask *what check would have gone red, and was it ever fed a failing case?*
2. **The master pattern (R0): the error is the measurement's SHAPE, not the question.** A reasonable question, an instrument that answers a slightly different question, reported as if it answered the original.

## The 13 failure classes, with their cheap tests

| Class | Definition | The cheap test |
|---|---|---|
| A. The probe that cannot succeed | A check whose positive outcome is unreachable; its negative result is read as a finding (R338: every source read absent from a wrong JSON key). | Put a known-PRESENT control IN the probe list; a failed control VOIDS the run. |
| B. Asserting from an artefact, not the system | A doc/task-list/memory/own-digest-line read as current state (R408, R432, R509). | Query the store or hit the live endpoint; "we currently serve X" needs a catalogue count AND a live response. |
| C. Measurement shape ≠ question | The number is true, about a different quantity (R518: pooled 592 files → 11.2M "conflicts" vs true 49,856; R494: write-telemetry read as downloads). | Name the unit the question is about and count THAT unit; if a definition is a union, compute the union. |
| D. "Deployed" ≠ committed | The repo contains it; the running system does not (R345; the worker deploys manually). | Trace what turns a commit into a deploy; verify at the live endpoint. |
| E. Unanchored matching | Substring/prefix matches that collide (R129: `series/imf_fsi` matches `imf_fsire`; R112: `ppi` inside "shipping"). | Anchor on a delimiter you control (`series/imf_fsi%3A`); match structured data, never formatted sentences; assert `len(keys)==len(set(...))` before name-keyed comparisons. |
| F. The guard that cannot fail | No discriminating pair; fires on everything or nothing; `except` branch passes (R414, R488, R501→R503→R508). | Ship the guard with one case it MUST block and one it MUST pass, proven; "cannot measure" must refuse. |
| G. One of N paths | Measured/fixed one scheduler, workflow, parser, store of several; reported as the total (R411, R428, R390). | Enumerate ALL paths from the code (grep what reads the registry / dispatches work); a fix to an assumption is a class sweep with the grep in the commit. |
| H. The silent empty result | A listing/parse returns `[]` instead of raising; "nothing is there" vs "could not look" become indistinguishable (R330, R264, R261). | Ask what distinguishes the two — if nothing does, that is the bug. Print the denominator; name what was skipped. |
| I. Cost found by the invoice | Green everywhere; the bill is the only detector (R430 $82/day; R492 $2,500 planned). | O(page) queries per public request; measure one statement (`meta.rows_read`); reconcile the model against real invoices. |
| J. Key-grain / dropped dimension | The key omits a publisher dimension; collisions look like health (R515, R519, damodaran, eia). | `COUNT(*)` vs `COUNT(DISTINCT <key>)` — one query; read the publisher's own key declaration (`$metadata`). |
| K. Alive is not working | Health inferred from proxies — process name, CPU, log silence, aggregate status (R417, R453, R454). | Measure the quantity the work MOVES between two readings; enumerate a job's children before acting on an aggregate verdict. |
| L. The causal story outruns its test | Observation solid; cause attached free, at the same confidence (R514, R512, R504). | Write the refuting observation and run that one test before speaking the cause. |
| M. Ledger/process hygiene | Failures of the recording system itself (R328, R485, R352). | Mechanical checks (`ledger_check.py`), append-only writes, digest line same commit. |

## The nine seductive assumptions (why the wrong answer feels right)

| Assumption | Why it feels safe | The cheap refutation |
|---|---|---|
| A field is called what I expect | The parse succeeds and prints a plausible total | Print ONE raw record before counting (R433, R478) |
| A number in a log means its label | It came from the system | Ask *what computed it*; would it look identical under the hypothesis you are ruling out? (R519) |
| A naming convention implies behaviour | Conventions are usually honest | One grep for consumers/imports (R516, R475) |
| My tool measured what I pointed it at | It ran and produced numbers | Reconcile the denominator against an independently known population (R515's "308 of 430") |
| A fix propagated to siblings | It was correct where I made it | `grep -l '<the exact defective line>'` across every executable surface (R390, R256) |
| A doc describes the current system | Written by someone who knew | Query the store / live endpoint; when doc and code disagree, FIX THE DOC in the same pass (R408) |
| Re-running my query verifies it | Same answer twice feels confirmed | A DIFFERENT instrument (parquet footers vs DuckDB scan, R342) |
| A clean result means clean data | Zero findings is good news | Carry a positive control that MUST detect the defect (R220a) |
| The obvious columns are the key | Schema designed by someone competent | `COUNT(*)` vs `COUNT(DISTINCT …)` — "it is obviously the key" is how you delete rows (R387) |

## The visible-tell rule

**The absence of hedging is not evidence of verification — in this ledger it correlates with its absence** (R410). If the sentence is shorter than the measurement that supports it, check whether the missing words were the qualifiers (R514). And when a guard refuses your write for the third time, the guard is the witness and you are the suspect (R519).