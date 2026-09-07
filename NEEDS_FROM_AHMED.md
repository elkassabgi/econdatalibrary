# Running list — what I need from Ahmed (updated 2026-08-07 ~12:30, for the 3-hour check-in)

Ordered by impact. Each item says exactly what to do and what it unblocks.

## 0. unsdg un-gate — one word from you (2.07M rows are built and waiting)

**Say "un-gate unsdg" and it goes live in one deploy.** Everything else is done and proven:
396 SDG series codes catalogued (all 396 titled by the publisher, zero raw-code fallbacks),
396 CSVs / 114 MB derived to R2, the coherence catalog refreshed, the client resolver wired,
and a mapping bug fixed that would otherwise have kept the source unhealthy forever
(37,822 of its 227,955 keys could not resolve — now all 227,955 map, 0 unmapped).

**Why I did not just do it.** unsdg sits on the `LEGACY_KEEP` floor in `core/gen_denylist.py`,
pinned in the 2026-07-23 wave as "no reply to the permission request / never assessed". That
floor exists so an UNCONFIRMED source can never silently un-gate, and every previous removal
from it (barro_lee, unesco_natmon, unesco_sdg, norgesbank) records an explicit decision from
you. I am not going to write your authorization for you.

**Why it qualifies, in your own established pattern.** A permission request is only needed
where the LICENCE does not itself grant redistribution — and unsdg's does. The audit has it
CLEARED (attrib), verbatim: *"may be copied freely, duplicated and further distributed
provided that UNdata is cited as the reference"*, with the explicit finding that the
restrictive un.org WEBSITE terms do not govern the data service
(`DATABASE_LICENSES_VERBATIM.md`). This is the norgesbank case exactly, one day later:
purged in the same 07-23 wave, store since REBUILT from the publisher (so purged data never
resurfaces — freshly-fetched data serves), licence since cleared.

Note it is **396 of 713 codes** today; the rest are still being backfilled by the scheduler
and will be added as they land. Nothing is advertised that we do not hold.

## 1. The deletion permission (one settings edit — unblocks FOUR queued jobs)

Add these two lines to `permissions.allow` in
`E:\research\econfindatalibrary\.claude\settings.local.json`:

    "Bash(python tools/retire_source.py *)",
    "Bash(python tools/delist_source_rows.py *)"

(If the file has no `permissions` block yet, tell me and I'll give the full JSON.)

Unblocks, in execution order:
- **Class A IMF retirement** — ~25 legacy sources / ~1.02M D1 rows (plan committed in
  50-queue.md; tool dry-run-verified with exact counts). D1 hit 9.42 GB today — this
  deletion likely defers the paid split entirely.
- **whr un-gate** — purge the 178 OWID-era CSVs on R2, then remove whr from the denylist,
  deploy, verify 451→200. The clean Figure-2.1 data (1,749 series) is already catalogued,
  derived, and D1-synced, waiting behind the gate.
- **GATED residue** — 40 orphaned CSVs on R2 (unreachable, cosmetic).

Alternative if you prefer not to add the rule: run the commands yourself from
`E:\research\econfindatalibrary` — I'll print the exact list on request.

## 2. UNCTAD — keys are IN, but the observations endpoint needs its documented shape (2 min)

Your keys are saved and verified present in .env. Measured tonight: the catalogue, the
vintage signal and every dimension table answer fine WITHOUT keys, but the observations
endpoint (/Facts) returns 400 to any non-browser client — and returns an EMPTY body headless
while returning a detailed validation error from the browser, which means the bot gate is
answering before UNCTAD's own API does. Parameter guessing cannot converge against that.

**What I need (you are logged in, I am not):** open any dataset page, e.g.
https://unctadstat.unctad.org/datacentre/dataviewer/US.TradeMerchTotal → click
**"Get selected data using the data API"** → paste me the code sample it displays (R or
Python). It carries the real base URL and header names with `<<clientId>>`/`<<apiKey>>`
placeholders — safe to paste; keep the actual key VALUES in .env, never in chat.

Full measurement log: scratchpad/unctad_auth_findings.md

## (superseded) 2. UNCTAD API key (free account — unblocks 38 source builds)

1. https://unctadstat.unctad.org/datacentre/ → Login → register (free).
2. After login: **My Home** → copy **Client ID** and **API key**.
3. Put in `E:\research\econfindatalibrary\.env` as:

       UNCTAD_CLIENT_ID=...
       UNCTAD_API_KEY=...

   (Never paste them in chat — I verify presence without printing.)

The full API survey is done (scratchpad/unctad_survey_20260806.md): catalogue + vintage
endpoints already work keyless; only the observations endpoint needs the key.

## 3. B2 keep/delete — one line from you

The publisher-DISCONTINUED sets, NOT re-crawlable (deletion is permanent):
**imf_fsire** (18,620 series) · **imf_pgi** (8,891) · **imf_gender_budgeting** (288) ·
the **imf_ifs remainder** (~85k series with no successor family).
Say "keep all", "delete all", or name them individually.

## 4. Optional / when convenient

- ~~Stats NZ key~~ **OBSOLETE — removed 2026-08-07.** stats_nz needs NO key: it fetches the
  publisher's public bulk CSVs (fetcher header states it explicitly; no key lookup exists in
  the code path). MEASURED healthy: live, quarterly, 1,320 catalogued series, last run
  2026-08-01 `ok` with +122,203 rows through 2026-03. The key was only ever needed for an
  ALTERNATIVE portal-API route that the bulk-CSV fetcher superseded; the item should have been
  retired when that fetcher shipped.
- **Cloudflare tokens**: already verified — nothing expires, nothing to do (the "July 2
  token" was my error; your Jul-31 `econdatalibrary-ci` token is the live one and healthy).
- **GitHub PAT**: done and verified (push access on both repos). Nothing more needed.

## Decisions parked until after the retirement frees D1 headroom

- eia at table grain (#37: 238k–518k rows) and bea's 912,990 dark series (#65: would eat
  52% of headroom) — both sized, both waiting on the post-retirement D1 measurement.
- fao_* element-re-code family (#19) — 27%→79% is solvable, the last 20% is a restructure.
