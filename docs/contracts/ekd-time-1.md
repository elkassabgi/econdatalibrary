# Family Time Stamp — contract `ekd-time/1`

Draft 5, 2026-10-02. Status: working contract; the owner's confirmation of "declaration only" (rule 1) is open.
Home of this file: `docs/contracts/ekd-time-1.md` in the repository `elkassabgi/econdatalibrary`, branch `main`.
That copy is the authority. A change is a pull request on this path; each draft carries its number in its
status line, its changes in section 6, and is announced to the bundle builder by a message that gives the
SHA-256 of the committed file (LF line endings).
The econ dates project (Part B of draft 4) is separate work; only period-aligned econ output waits for it.

Scope: every dataset of the HF, Econ and IP data libraries, and every bundle built from them.
This contract is a DECLARATION about data as it is stored. It changes no stored value.

## 1. Three rules

1. **The contract requires no change to stored data.** Version 1 changes no column, label, type or file in any
   library. A library that changes its own data does it under its own process, and the change shows as a new
   `ekd:edition`. A re-label of a stored date is a different, breaking decision.
2. **The truth about a row is its span** - the stretch of time the value describes. The stored stamp is only a
   label for that span. Section 2 says how to compute the span from the label.
3. **What is not known is said to be unknown.** A dataset without a declaration is "undeclared". A join across
   datasets with an undeclared side is REFUSED (section 3, rule 7). A visible warning in place of a refusal is
   allowed for one case only: an export of ONE dataset that shows its labels as published and joins nothing.
   No field is guessed.

## 2. The declaration

The manifest is a Frictionless `datapackage.json`, Data Package version 1 (the `profile` key), the descriptor
format econ's Python client already writes as a lock file. In a family bundle one resource = one library
object, with `bytes` and `hash` under the rules below. The family adds objects prefixed `ekd:`. Field names are
written exactly as in this file (snake_case).

**Who writes them.** The bundle builder writes them into the manifest of the bundle it builds. No library API
changes in version 1. Econ's `/v1/bundle` answer stays as it is: econ's conformance test pins the exact key set
of that answer, so an extra key there is a change to econ's own contract.

**Scope.** One declaration covers one resource and the one column that dates the ROW. Other date-like columns
are attributes and are not declared in version 1. A resource that holds series which do not share one
declaration has `declared: false` and `grain: "mixed"`. Per-series declaration is not in version 1; the names
`ekd:time_by_series` (series id -> the same object) and `grain_column` are reserved for it and not delivered.

**A resource's `path`** is the file's path inside the bundle when the object is delivered in it, and the fully
qualified URL of the library object when it is an input that is not delivered (a merged or derived bundle).

**Versioning.** A reader ignores `ekd:` fields it does not know. A new optional field keeps `ekd-time/1`. A
changed meaning is `ekd-time/2`. This rule binds from the first release of a tool that writes `ekd-time/1`
manifests. Until then the contract is a draft: a draft may change a meaning and says so in section 6.

### `ekd:time` - how to read the time column

| Field | Type | Meaning | Example (hf weekly bars) |
|---|---|---|---|
| `contract` | string | always `ekd-time/1` | `"ekd-time/1"` |
| `declared` | boolean | `false` = the library has not declared this file; every field below may be null | `true` |
| `column` | string | the column that holds the time label | `"datetime"` |
| `kind` | `instant` / `date` / `period` | a moment; one calendar day; a stretch of time | `"period"` |
| `grain` | a word from the list below, or `mixed`, or `unknown` | length of one row's span | `"weekly"` |
| `stamp` | `start` / `end` / `mid` / `date` / `unknown` | which point of the span the stored label names. `date` = the label is the day itself (only with `grain: daily`) | `"end"` |
| `anchor` | string or null | a note for people about what fixes the span. No version 1 computation reads it | `"W-FRI: Saturday to Friday; the label is the Friday, also when it is not a trading day"` |
| `zone` | tz-database name, or null | `UTC` for an instant in UTC. Null = a calendar date or period with no zone stated. A null-zone label is never turned into an instant | `"America/New_York"` |
| `session` | object or null | `{"open": "HH:MM", "close": "HH:MM"}` in `zone` when rows exist only inside an exchange session; null = civil calendar | `{"open": "09:30", "close": "16:00"}` |
| `stored_as` | one of `timestamp_naive`, `timestamp_tz`, `date`, `integer_year`, `text` | the stored type, so a reader does not mis-parse it | `"timestamp_naive"` |
| `observed_through` | date (text `YYYY-MM-DD`) or null | last day that has an actual observation in this file | `null` today (see section 4) |
| `projected_through` | date or null | last label, when the file holds projections beyond observation | `null` |
| `last_period_complete` | boolean or null | see "Completeness" below | `null` today |
| `availability` | `after_session_close` / `release_lag_unknown` / `as_of_edition` | when a value could first be known - see section 3, rule 3 | `"after_session_close"` |

Grain list: `1min`, `5min`, `15min`, `30min`, `hourly`, `daily`, `weekly`, `monthly`, `quarterly`,
`half-yearly`, `annual`, `irregular`. These are the words the hf API already uses, extended; the econ
catalogue's values map onto them (`D`, `W`, `M`, `Q` and the word `quarterly`, `S`, `A`, `irregular`).

**How to compute a row's span (normative).** A span is half-open, [start, end), in the declaration's `zone`.
- Sub-daily grains (`1min`, `5min`, `15min`, `30min`, `hourly`): step = the grain length. `stamp: start` gives
  [label, label + step). `stamp: end` gives [label - step, label).
- `daily` and longer: spans are whole calendar days. Ignore any time of day in the stored label. L = the
  label's calendar date. U = one unit of the grain (1 day; 7 days; 1, 3, 6 or 12 calendar months).
  - `stamp: date` (only with `grain: daily`): [L, L + 1 day).
  - `stamp: start`: L is the first day: [L, L + U).
  - `stamp: end`: L is the LAST day, inclusive: end = L + 1 day, start = end - U.
  - `stamp: mid` or `unknown`, or `grain: irregular`, `mixed` or `unknown`: the span cannot be computed in
    version 1. Treat the resource as undeclared for joins.
- A label stored as an integer year Y is read as Y-01-01.
- `kind: date` always has `grain: daily` and `stamp: date`.
- When `session` is set, the value describes only the part of the span inside the session. The span itself is
  not cut. Early closes are not marked.

**Last calendar day (normative).** The last calendar day of a span is the calendar date, in `zone`, of the last
instant inside the span: `end` minus one day when `end` is at midnight (every daily or longer span), and the
date of `end` when it is not (a sub-daily span that ends inside a day).

**Completeness.** `last_period_complete` looks at the last row whose span starts on or before
`observed_through` (per ticker, in a file that holds several). It is true when the last calendar day of that
row's span is on or before `observed_through`, false when it is after, and null when `observed_through` is
null or the span cannot be computed. When it is null the engine flags the last row of every weekly or coarser
file as "completeness not known"; it never assumes complete. (A finished week whose Friday is a holiday reads
false until the next row appears. That is the safe side.)

Dates are text or date values in the manifest, never nanosecond timestamps (those cannot hold year 0001 or
9999, and both occur in econ).

### `ekd:edition` - which bytes these are

| Field | Type | Meaning |
|---|---|---|
| `sha256` | lower-case hex, no prefix, or null | hash of the library object's bytes as delivered to the user |
| `bytes`, `row_count` | integer or null | size and rows of that object |
| `published_utc` | `YYYY-MM-DDTHH:MM:SSZ` or null | when the library last wrote this object to its store (ip: the `uploaded` value of `/v1/bundles`, cut to the second) |
| `source_version` | string or null | the PUBLISHER's own release name, when it has one (ip: the vintage tag in the object path, verbatim). A library's own counter is not a source version; null here need not be listed in `not_recorded` |
| `not_recorded` | list of names | fields that are null because nobody records them yet, written `time.<field>` or `edition.<field>`. Never filled with a guess |

- When the resource also carries the Frictionless `hash` and `bytes`, they must agree with `ekd:edition`.
- `sha256` and `row_count` say which library object this is. Where a library publishes its own record of an
  object, they come from that record only. hf will publish one (the per-ticker manifest): until it is live a
  builder does not fill hf `sha256` or `row_count` from bytes it downloaded; both stay null and are listed in
  `not_recorded`. Econ and ip publish no such record in version 1: for them the builder may write the SHA-256
  and the row count of the whole object as it received it, as econ's Python client does in its lock file. In
  every library, a file rebuilt from parts and verified by value does not carry the library object's `sha256`:
  `sha256` and `row_count` stay null and are listed in `not_recorded`.
- Reserved names, not delivered in version 1: `library_sequence`, `history_revision`, `value_digest` (in
  `ekd:edition`); `value_state`, `released_utc` (in `ekd:time`). Until they are delivered a builder keeps such
  values in its own state file, outside the `ekd:` objects.

### Package level

- `ekd:generated_utc` - when the bundle was built.
- `ekd:derived` - boolean; required and `true` when the bundle re-writes rows (a workbook, a merged table),
  absent or `false` otherwise. The resources of a derived bundle are the INPUTS, each with its declaration and
  edition.
- `ekd:outputs` - optional list of `{"path", "bytes", "sha256", "description"}` for the files a derived bundle
  delivers.
- A bundle may call itself a "snapshot" or "pinned" only when every resource has a `sha256`.

**Files stay byte-identical to the library's objects.** All of the above lives in the manifest. A bundle with
`ekd:derived: true` is a derived product and says so.

## 3. Join rules

1. **Join on spans, never on equal labels.** Two annual series labelled 01-01 and 12-31 are the same year. A
   daily bar belongs to the month whose span contains its trading date. A pivot across resources of ONE
   dataset under ONE declaration is allowed only when that declaration has `declared: true` and a computable
   span; otherwise it is a join and rule 7 applies.
2. **Different calendars join only by a stated rule** (a fiscal year against a calendar year; a Saturday-Friday
   week against a Monday-Sunday week). The engine names the rule in its output. Never silently. In a
   same-period join the values that are attached (the right side) are never of a finer grain than the rows
   they are attached to (the left side).
3. **Two alignments, and the output says which one was used.**
   - *Same period*: the value for month M sits beside every day of M. This may use information that became
     known after the row's span began (look-ahead) and the output must say so.
   - *As known*: a value may sit beside a row only when the value's availability instant is AT OR BEFORE THE
     START of that row's span. Among the values of one series that qualify, the engine takes the one with the
     latest availability instant; when several have that same instant (the sub-daily values of one day), the
     one whose span ends last. One value per row. In version 1 an availability instant exists only for
     `after_session_close`, and only when `zone` and `session` are not null: it is `session.close` in `zone` on
     the last calendar day of the VALUE's span (section 2; hf: 16:00 New York). For `as_of_edition` and
     `release_lag_unknown` there is no instant. As-known is refused when the value side has no instant, and
     when the ROW's `zone` is null, because its span start is then not an instant. So a daily value for day D
     never sits beside any row of day D - its 15:59 bar starts before 16:00, and its daily bar starts at
     00:00 - and first sits beside the rows of the next day. A sub-daily value of day D is treated as known at
     16:00 on D; that is later than the truth, the safe side. A weekly value is known at 16:00 on its Friday.
     On an early-close day 16:00 is later than the true close; that is the safe side. The engine may bound the
     age of a value; the bound is printed in the output.
   - hf supports as-known for TIMING only. When a split is detected the whole history of that ticker is
     rescaled and re-cleaned, and clean bars pass a centred 50-bar filter. So price and volume LEVELS are as of
     the edition, not as of the bar, and the output must say so. The library publishes day D on D+1.
   - Econ records no release time (`release_lag_unknown`): as-known is refused for econ, or runs on a lag the
     user types in and the output records. IP measures are known only as of an edition (`as_of_edition`).
4. **Drop or flag** a last period that is not complete, and rows after `observed_through`.
5. **Compute hf times in New York wall-clock time.** Convert to UTC only on request, with the tz-database rules,
   never a fixed offset. (Checked for every calendar day 1991-2026 at 09:30 and 15:59: no session time is
   ambiguous or missing.)
6. **One shown date = the span's last calendar day** - for NEW display fields only (rule 1 of section 1).
7. **Refuse** a join across datasets when a side has `declared: false`, when a grain is `unknown` or `mixed`, or
   when as-known alignment is asked for and no availability instant exists (rule 3). An export of one dataset with no join
   needs no declaration and can ship first. hf bars with hf variables is a join across datasets.

Not covered by version 1, so the engine needs its own rule and prints it: a table with no time axis or with
null dates; years below 1500 and sentinel years; inputs from different editions; an hf weekly or monthly label
that is not a trading day; a date window on an undeclared resource (a window widened by twelve months loses no
period of one year or less; a longer period that began earlier can be left out, and the output says so).

## 4. The declarations, library by library

**hf - the rules as the code produces them** (`pipeline/aggregate.py`, `build_bars.py`, `clean_pipeline.py`,
`compute_variables.py` on the hf repository's main branch).

| File | `column` | `grain` | `stamp` | Span of one row |
|---|---|---|---|---|
| 1-minute bars | `datetime` | `1min` | `start` | [label, label + 1 minute); first 09:30, last 15:59 |
| 5 / 15 / 30-minute | `datetime` | `5min` / `15min` / `30min` | `start` | [label, label + step) |
| hourly | `datetime` | `hourly` | `start` | the clock hour; the 09:00 row holds 09:30-09:59 only |
| daily | `datetime` (00:00:00) | `daily` | `date` | that calendar day |
| weekly | `datetime` (00:00:00) | `weekly` | `end` | Saturday to Friday; label = the Friday, also on a holiday |
| monthly | `datetime` (00:00:00) | `monthly` | `start` | the calendar month; label = day 1, also a non-trading day |
| variables, quality | `trade_date` (00:00:00) | `daily` | `date` | that calendar day |

For all hf files: `kind: period`, `zone: America/New_York`, `session: {"open": "09:30", "close": "16:00"}`,
`stored_as: timestamp_naive`, `availability: after_session_close`. (A 1-minute bar is declared as a period of
one minute, not as an instant.) The 1-minute CSV is a separate resource with `stored_as: text`; its print
format was not read from a served file. A half day ends early and no column marks it. The last weekly and
monthly row is usually not complete, and the weekly label can be a future date.

A file whose rows are taken unchanged from per-ticker files of ONE kind (all of them, or a date slice), for
several tickers, with an added `ticker` column (an all-ticker unit, a panel) is declared as that base kind.

The first IEX-built bar is dated 2022-03-07; the last trading day of the earlier segment is 2022-03-04. A
registry that needs one date for the change uses 2022-03-07. (Pages and tools that state another date are defects of
those pages, not of this contract.) The label rule of the earlier segment is not proven by any file.

Evidence limit: the rules for the 5-minute to monthly files come from the code, run on a 1-minute snapshot. No
served aggregated file was read. Some inactive tickers have files the daily job has not rewritten for months,
with more columns than the current files. Before the builder writes `declared: true` for hf, it reads one
served file per timeframe for one active and one inactive ticker and compares the first, the last and one
holiday-week label with this table.

What hf does not expose today: `sha256`, `row_count` and `observed_through` PER TICKER (the API gives only
`size_bytes` and `last_modified`). The site's `end_date` is one date for the whole library and is only an upper
bound for a ticker; a builder may show it under its own name, not as an `ekd:` field. Until a per-ticker
manifest written by the daily pipeline is live, these fields are null and listed in `not_recorded`.

**ip**: `patent_measures.parquet` - `column: grant_date`, `kind: date`, `grain: daily`, `stamp: date`,
`zone: null`, `stored_as: date`. `assignee_year.parquet` - `column: grant_year`, `kind: period`,
`grain: annual`, `stamp: start`, `zone: null`, `stored_as: integer_year`. `filing_year` is an attribute, not
the row's time axis; it has nulls and impossible values (above the current year) and is never a join key
without a filter. `availability: as_of_edition`. `source_version` is the vintage tag in the object path,
verbatim. The publisher's release date differs from it by one day and is not a field in version 1.

**econ**: every econ resource in a bundle carries `{"contract": "ekd-time/1", "declared": false, "column":
"obs_date", "kind": "period", "grain": "mixed", "stamp": "unknown", "zone": null, "availability":
"release_lag_unknown"}` until the econ library delivers a declaration for that source. An econ resource holds
many series. A join across econ series, or between econ and another library, is refused under rule 7; a grid of
econ series "as published" is such a join. The econ client's `to_wide()` aligns series on equal labels, which
rule 1 forbids. It is outside this contract, and a family engine does not call it.

## 5. Open decisions (the owner's; numbered as in draft 4)

- Decision 4. Is version 1 a declaration only, with no stored date changed? Working answer: yes.
- Decision 5. Who writes the per-ticker hf manifest? Working answer: the daily pipeline.

## 6. Changes

- **Draft 5 (2026-10-02)**, from the bundle builder's notes N0-N19 on draft 4:
  - CHANGED MEANING (allowed while the contract is a draft - see Versioning), to remove a contradiction:
    as-known now compares the value's availability instant with the START of the row's span (draft 4 said the
    row's span "ends at or after" the instant, which let the 15:59 bar of day D take day D's daily value while
    the same paragraph forbade it). Effect: a daily or weekly ROW takes the value of the period BEFORE it, not
    of its own period. The pick rule, the tie rule for sub-daily values, and the cases with no availability
    instant are stated. "Last calendar day" is defined for sub-daily spans.
  - `sha256` / `row_count`: from the library's own record where one exists (hf, when its manifest is live);
    for econ and ip the builder may record what it received.
  - Rule 3 of section 1 no longer offers "a visible warning" for a join; rule 7 governs.
  - `last_period_complete` is defined on the last calendar day of the span.
  - Stated: Data Package version 1 and snake_case names; what `path` holds; who may fill `sha256` and
    `row_count`; `source_version` is the publisher's; ip `uploaded` maps to `published_utc`; one date for the
    hf source change; all-ticker files are declared as their base kind; the single-dataset pivot; the
    same-period rule for a finer right side; the window rule for an undeclared resource.
  - New optional package fields `ekd:derived`, `ekd:outputs`. Reserved names listed.
  - The contract has a tracked home (this path).
- Draft 4 (2026-10-02): first draft sent to the bundle builder (SHA-256 of that file `283bbfd9...30638d`).
