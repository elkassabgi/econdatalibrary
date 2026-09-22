// node --test test/wideSources.test.ts   (Node >= 22.6 strips types natively)
//
// FOUR SOURCES WERE ADVERTISED AND UNDELIVERABLE FOR ~20 DAYS. The CSV_HEADER guard in
// series.ts refuses any stored object whose first line is not "series_id,obs_date,value".
// wikidata, fhfa, census and treasury are DELIBERATELY stored wide - the resolver marks them
// tidy_ok=False (clients/python/econdl/_resolve.py:1491) because a long projection of a
// relational table would be a lie - so their CORRECT objects were rejected as malformed.
// Measured on the live worker 2026-09-22 with an authenticated key: fhfa 89,706 +
// census 2,993 + wikidata 250 + treasury 14 = 92,963 series answering
//   502 {"error":"data_unavailable","detail":"the at-rest object is malformed
//        (header 'dataset,series_key,obs_date,hpi,...')"}
// while bls and oecd answered 200 in the same minute.
//
// THE GUARD ITSELF IS RIGHT and these tests keep it: a NON-wide source with a wrong header is
// still refused. What was missing is that four sources have another correct header.
//
// WHY FILTERS ARE REFUSED RATHER THAN APPLIED. applyDateWindow reads cols[1] because a
// canonical row is series_id,obs_date,value; on an fhfa row cols[1] is `series_key`. Relaxing
// the header check alone would have filtered on the wrong column and still answered 200 -
// worse than the outage. So a wide object is served whole and from/to/geo are refused, which
// is the rule CONTRACT.md already applies to geo/freq/unit.
import { test } from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import { CSV_HEADER, newStats, peekGzipHeader, LineFilter } from "../src/csvStream.ts";
import { NATIVE_ONLY_SOURCES, isNativeOnly } from "../src/util.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();

// The REAL headers, copied from the live 502 details - not invented shapes.
const FHFA_HEADER = "dataset,series_key,obs_date,hpi,hpi_1990,hpi_2000,annual_change";
const CENSUS_HEADER = "series_key,data_type_code,seasonally_adj,category_code";
const TREASURY_HEADER = "series_key,obs_date,record_date,debt_held_public_amt";
const WIKIDATA_HEADER = "series_key,qid,wikidata_url,name,tickers,primary_ticker";

test("the wide-source list is exactly the resolver's _NATIVE_ONLY set", () => {
  // Mirrors clients/python/econdl/_resolve.py:1491. tests/test_worker_native_only.py fails CI
  // if the two ever drift; this one pins the shape on the worker side.
  assert.deepEqual([...NATIVE_ONLY_SOURCES].sort(),
    ["census", "fhfa", "hf_equities", "treasury", "wikidata"]);
});

test("isNativeOnly is true for every wide source and false for a tidy one", () => {
  for (const s of NATIVE_ONLY_SOURCES) assert.equal(isNativeOnly(s), true, s);
  // CONTROL: if this returned true for everything the guard would be gone, not fixed.
  for (const s of ["bls", "oecd", "worldbank", "ilostat", "eurostat"]) {
    assert.equal(isNativeOnly(s), false, s);
  }
});

test("peekGzipHeader accepts a real wide header ONLY when told the source is wide", () => {
  for (const hdr of [FHFA_HEADER, CENSUS_HEADER, TREASURY_HEADER, WIKIDATA_HEADER]) {
    const gz = [new Uint8Array(gzipSync(Buffer.from(`${hdr}\nx,y,z\n`)))];
    // the bug: refused when not flagged
    assert.equal(peekGzipHeader(gz).headerOk, false, `${hdr} must be refused unflagged`);
    // the fix: accepted when the caller knows it is a wide source
    assert.equal(peekGzipHeader(gz, true).headerOk, true, `${hdr} must be accepted when wide`);
  }
});

test("peekGzipHeader still accepts the canonical header without any flag", () => {
  const gz = [new Uint8Array(gzipSync(Buffer.from(`${CSV_HEADER}\na,2020-01-01,1\n`)))];
  assert.equal(peekGzipHeader(gz).headerOk, true);
});

test("the streaming filter accepts a wide header only under allowAnyHeader", () => {
  const body = `${FHFA_HEADER}\nannual_cbsa,01,2020-12-31,100.0,1,2,3\n`;
  for (const [allow, want] of [[false, false], [true, true]] as [boolean, boolean][]) {
    const stats = newStats();
    const f = new LineFilter({ from: null, to: null, geo: null, allowAnyHeader: allow }, stats);
    f.push(enc.encode(body));
    f.flush();
    assert.equal(stats.headerOk, want,
      `allowAnyHeader=${allow} should give headerOk=${want}`);
  }
});

test("a TIDY source with a wrong header is still refused - the guard is kept, not removed", () => {
  // This is the case the guard was written for (commit e59cce123). If this ever passes,
  // the fix has gone too far and a malformed object would be served as data.
  const stats = newStats();
  const f = new LineFilter({ from: null, to: null, geo: null }, stats);
  f.push(enc.encode("total,rows,whatever\n1,2,3\n"));
  f.flush();
  assert.equal(stats.headerOk, false);
  void dec;
});
