// node --test test/seriesHeader.test.ts   (Node >= 22.6 strips types natively)
//
// These two functions were deployed and could NOT be verified live: series CSVs are auth-gated
// (`auth_required` for every source) and no token exists in the shell that shipped them. A unit
// test against the exact functions the worker runs is the only honest check of the rendered
// output, so it stands in for the live probe rather than decorating it.
import { test } from "node:test";
import assert from "node:assert/strict";
import { headerRows, idbDatasetUrl, IDB_CAVEAT } from "../src/seriesHeader.ts";

const BAR = 78;                       // "# " + "=".repeat(76) in buildHeader

test("idbDatasetUrl builds the permission-required backlink from the id", () => {
  // IDB written permission (2026-07-15) condition (3): a permanent link to the dataset page.
  assert.equal(
    idbDatasetUrl("idb:IDB:social-indicators-of-latin-america-and-the-caribbean:subempleo:PER"),
    "https://data.iadb.org/dataset/social-indicators-of-latin-america-and-the-caribbean",
  );
});

test("idbDatasetUrl rewrites a slug IDB has renamed", () => {
  // The old name 404s at the publisher with no redirect, which breaks condition (3) for the 29
  // series that carry it. Verified 2026-09-02 against CKAN package_show.
  assert.equal(
    idbDatasetUrl(
      "idb:IDB:center-for-learning-improvement-information-cima-regional-indicators-2007-2:ARG"),
    "https://data.iadb.org/dataset/cima-indicators",
  );
});

test("idbDatasetUrl does not crash on a malformed id", () => {
  // `split(":")[2]` is undefined for a short id; the `?? ""` must hold.
  assert.equal(idbDatasetUrl("idb:IDB"), "https://data.iadb.org/dataset/");
});

test("headerRows returns nothing for an absent value", () => {
  for (const empty of [null, undefined, ""]) {
    assert.equal(headerRows("Caveat", empty), "");
  }
});

test("headerRows keeps every line inside the 78-column bar", () => {
  // the SHIPPED string, not a copy of it - a copy keeps passing after the real one changes
  const caveat = IDB_CAVEAT;
  const lines = headerRows("Caveat", caveat).split("\n").filter(Boolean);

  assert.ok(lines.length > 1, "a caveat this long must wrap");
  for (const l of lines) {
    assert.ok(l.length <= BAR, `line is ${l.length} columns, bar is ${BAR}: ${l}`);
    assert.ok(l.startsWith("#  "), `every line must stay a comment: ${l}`);
  }
  // the label appears once, on the first line, in the same 11-column gutter row() uses
  assert.ok(lines[0].startsWith("#  Caveat:    "), lines[0]);
  assert.equal(lines.filter((l) => l.includes("Caveat:")).length, 1);
  for (const l of lines.slice(1)) {
    assert.ok(l.startsWith("#  " + " ".repeat(11)), `continuation must be indented: ${l}`);
  }
  // and nothing is lost in the wrapping
  assert.equal(lines.map((l) => l.slice(14).trim()).join(" "), caveat);
});

test("headerRows collapses whitespace the way row() does", () => {
  assert.equal(headerRows("X", "  a\n\tb   c  "), "#  X:         a b c\n");
});

test("headerRows does not break a word that is longer than the wrap width", () => {
  const long = "x".repeat(90);
  const lines = headerRows("X", long).split("\n").filter(Boolean);
  assert.equal(lines.length, 1, "an unbreakable token stays on one line rather than being cut");
  assert.ok(lines[0].includes(long), "the token must survive intact even though it overflows");
});

test("the shipped idb caveat says what the measurement supports", () => {
  // Guards against the caveat drifting into a claim the data does not back. 11,339 of 18,854
  // series carry two or more DIFFERENT values on one date (tools/cost/idb_affected_series.py),
  // and the cause is the dropped dimensions named here.
  for (const term of ["indicator", "country", "sex", "area", "age", "quintile", "education"]) {
    assert.ok(IDB_CAVEAT.includes(term), `the caveat should name ${term}`);
  }
  assert.ok(/several different values/.test(IDB_CAVEAT),
    "it must say a date can carry several DIFFERENT values, not merely several rows");
});
