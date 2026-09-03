// Pure helpers for the series CSV comment header.
//
// They live here rather than inside series.ts so they can be unit-tested: series.ts imports
// "./sql" extensionless, which `node --test` cannot resolve, so nothing in that file is
// reachable from a test. This module imports nothing.
//
// That matters more than tidiness here. Both functions were deployed and could NOT be verified
// live — series CSVs are auth-gated (`auth_required` for every source) and no token exists in
// the shell that shipped them — so a unit test against the exact code the worker runs is the
// only honest check of the rendered output.

// Dataset slugs IDB has RENAMED since we ingested them. The old name 404s at the publisher with
// no redirect, which breaks condition (3) of IDB's written permission (2026-07-15): "a clear,
// permanent link back to the original dataset page".
//
// Kept as a map rather than re-keying the affected series: a re-key would change public series
// ids and break every URL a user already holds, in order to repair a link in a comment header.
//
// Checked, not assumed — tools/cost/idb_backlink_check.py asks CKAN package_show whether every
// slug in the served catalogue still resolves, and names any that stop. As of 2026-09-02, 20 of
// 21 resolved and this was the one that did not.
export const IDB_RENAMED: Record<string, string> = {
  // -> cima-indicators, "Center of Information to Improve Learning (CIMA)", cc-by, 29 series
  "center-for-learning-improvement-information-cima-regional-indicators-2007-2": "cima-indicators",
};

// idb ids are shaped idb:IDB:<dataset-slug>:<indicator>:<country>, so the slug is segment 2.
export function idbDatasetUrl(seriesId: string): string {
  const slug = seriesId.split(":")[2] ?? "";
  return `https://data.iadb.org/dataset/${IDB_RENAMED[slug] ?? slug}`;
}

// Same gutter as the header's row(), but wrapped to its 78-column bar. A notice long enough to
// matter is long enough to need this: unwrapped it prints three bar-widths wide and reads as
// damage rather than as something to act on.
//
// A single token longer than the wrap width is emitted whole and allowed to overflow, because
// silently cutting a URL or an identifier is worse than a long line.
export function headerRows(label: string, val: string | null | undefined): string {
  if (!val) return "";
  const width = 65;                         // 78 minus "#  " and the 11-column label gutter
  const out: string[] = [];
  let line = "";
  for (const word of String(val).replace(/\s+/g, " ").trim().split(" ")) {
    if (line && line.length + 1 + word.length > width) { out.push(line); line = word; }
    else line = line ? line + " " + word : word;
  }
  if (line) out.push(line);
  return out.map((l, i) => `#  ${(i === 0 ? label + ":" : "").padEnd(11)}${l}\n`).join("");
}

// The idb caveat itself, so the text the users see and the text the test checks are one string.
//
// MEASURED 2026-09-03 over the full store (tools/cost/idb_affected_series.py): 11,339 of 18,854
// idb series carry two or more DIFFERENT values on the same date, because the series key is
// indicator+country while the publisher also breaks the same indicator down by sex, area, age,
// quintile, education, ethnicity and survey. Those rows are different populations, not
// duplicates, and nothing in the CSV separates them.
export const IDB_CAVEAT =
  "rows are keyed by indicator and country only; the publisher also breaks these down by " +
  "sex, area, age, quintile, education and survey, so one date may carry several " +
  "different values that this file cannot tell apart";
