# Database license verbatim audit — repeatable process

Produces `DATABASE_LICENSES_VERBATIM.md` (repo root): for every database, the
provider's official terms quoted VERBATIM + URL, classified, and adversarially
verified. **Do NOT re-derive by hand — read the output file first.**

Re-run (refresh / add databases):
1. Rebuild inventory + clusters if the catalog changed (see the two JSON files).
2. Run the workflow `license_research_workflow.js` (88 provider clusters ->
   research + adversarial verify). It returns 88 findings+verdicts.
3. `python assemble_license_file.py <workflow_output.json>` -> writes
   DATABASE_LICENSES_VERBATIM.md with per-database entries + decision tiers.

Decision rule (asymmetric caution): a database is only "cleared to re-host" when
the terms EXPLICITLY permit third-party redistribution AND the adversarial
verifier CONFIRMED it. Restricted / ambiguous / unreachable / DISPUTED => gated.
Written permissions (kof, comtrade, whr, IEP) override public terms — see
`REDISTRIBUTION_COMPLIANCE.md`.
