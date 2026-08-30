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
