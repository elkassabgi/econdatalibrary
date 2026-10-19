// src/withheld.ts and the SQL it feeds (src/sql.ts): the list's validation, the per-isolate cache and its
// fail-closed rules (AR-339 Part 2 finding 5), the literal NOT IN, the cache-key tag (finding 6), the 404.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import {
  TTL_FAIL_MS, TTL_OK_MS, WITHHELD_MAX, floorWithheld, forSource, isSourceClosed, isWithheld, keyWithWithheld,
  loadWithheld,
  parseWithheld, resetWithheldCache, withheldDetail, withheldReason, withheldResponse, LIST_UNREADABLE_DETAIL,
} from "../src/withheld.ts";
import { WITHHELD_FLOOR } from "../src/withheldFloor.ts";
import {
  browseAllSql, browseSourceSql, browseSourceVisibleCountSql, searchFtsCountSql, searchFtsSourceCountSql,
  searchFtsSourceSql, searchFtsSql, searchLikeCountSql, searchLikeSourceCountSql, searchLikeSourceSql, searchLikeSql,
  seriesIdsForSourceSql, withheldExcl, withheldPresentCountSql,
} from "../src/sql.ts";
import { presentCount } from "../src/catalog.ts";

const A = "insee_melodi:DS_RP_TD_MIGRES_COMP";
const B = "insee_melodi:DS_TOUR_CAP";
const OTHER = "wid:WID:x";
const obj = (ids: Record<string, unknown>) => JSON.stringify({ version: 1, ids });
const ent = { since: "2026-10-17", why: "t", until: "complete" };

test("parse: a valid list", () => {
  assert.deepEqual([...parseWithheld(obj({ [A]: ent, [B]: { ...ent, until: "operator" } }))!].sort(), [A, B]);
  assert.equal(parseWithheld(obj({}))!.size, 0);
});

test("parse: anything malformed is UNREADABLE (null), never a partial set", () => {
  for (const bad of [
    "not json", "[]", JSON.stringify({ version: 2, ids: {} }), JSON.stringify({ version: 1 }),
    JSON.stringify({ version: 1, ids: [] }), obj({ "insee_melodi:x'; DROP": ent }), obj({ "nocolon": ent }),
    obj({ "insee_melodi:a b": ent }), obj({ [A]: { ...ent, until: "never" } }), obj({ [A]: "complete" }),
  ]) assert.equal(parseWithheld(bad), null, bad);
  const many: Record<string, unknown> = {};
  for (let i = 0; i <= WITHHELD_MAX; i++) many[`insee_melodi:F${i}`] = ent;
  assert.equal(parseWithheld(JSON.stringify({ version: 1, ids: many })), null, "over the cap");
});

test("load: a good read is cached 300 s, then re-read", async () => {
  resetWithheldCache();
  let calls = 0;
  let t = 1_000_000;
  const read = async () => { calls++; return { text: obj({ [A]: ent }), etag: "e1" }; };
  const w = await loadWithheld(read, "store", () => t);
  assert.ok(isWithheld(w, A) && !isWithheld(w, B) && w.etag === "store-e1" && w.from === "store");
  t += TTL_OK_MS - 1;
  await loadWithheld(read, "store", () => t);
  assert.equal(calls, 1, "re-read inside the 300 s");
  t += 2;
  await loadWithheld(read, "store", () => t);
  assert.equal(calls, 2, "not re-read after 300 s");
});

test("load: absent, failing or invalid -> the FLOOR when nothing good was ever read; a failure is cached 30 s", async () => {
  for (const read of [async () => null, async () => { throw new Error("R2 down"); },
                      async () => ({ text: "{bad", etag: "x" })]) {
    resetWithheldCache();
    let t = 5_000_000;
    let calls = 0;
    const r = async () => { calls++; return read(); };
    const w = await loadWithheld(r as () => Promise<{ text: string; etag: string } | null>, "store", () => t);
    assert.equal(w.from, "floor");
    assert.deepEqual([...w.ids].sort(), [...WITHHELD_FLOOR].sort());
    t += TTL_FAIL_MS - 1;
    await loadWithheld(r as () => Promise<{ text: string; etag: string } | null>, "store", () => t);
    assert.equal(calls, 1, "a failure is not retried on every request");
    t += 2;
    await loadWithheld(r as () => Promise<{ text: string; etag: string } | null>, "store", () => t);
    assert.equal(calls, 2, "a failure is retried after 30 s");
  }
});

test("load: a failure after a good read keeps the LAST GOOD list, not the floor and not nothing", async () => {
  resetWithheldCache();
  let t = 9_000_000;
  let ok = true;
  const read = async () => {
    if (!ok) throw new Error("down");
    return { text: obj({ [B]: ent }), etag: "e2" };
  };
  await loadWithheld(read, "origin", () => t);
  ok = false;
  t += TTL_OK_MS + 1;
  const w = await loadWithheld(read, "origin", () => t);
  assert.equal(w.from, "last-good");
  assert.deepEqual([...w.ids], [B]);
});

test("load: an EMPTY list is honoured (the operator's way to withhold nothing)", async () => {
  resetWithheldCache();
  const w = await loadWithheld(async () => ({ text: obj({}), etag: "e0" }), "store", () => 1);
  assert.equal(w.ids.size, 0);
  assert.equal(w.from, "store");
});

test("the floor is the committed list (api/worker/withheld.json)", () => {
  const committed = JSON.parse(readFileSync(new URL("../withheld.json", import.meta.url), "utf8"));
  assert.deepEqual([...WITHHELD_FLOOR].sort(), Object.keys(committed.ids).sort());
  assert.ok(WITHHELD_FLOOR.length >= 1 && floorWithheld().etag.startsWith("floor-"));
});

test("withheldExcl: literal, sorted, validated", () => {
  assert.equal(withheldExcl([]), "");
  assert.equal(withheldExcl([B, A]), ` AND series_id NOT IN ('${A}','${B}')`);
  assert.equal(withheldExcl([A], "s.series_id"), ` AND s.series_id NOT IN ('${A}')`);
  assert.throws(() => withheldExcl(["insee_melodi:x' OR '1'='1"]));
  assert.equal(withheldPresentCountSql([B, A]),
               `SELECT COUNT(*) AS n FROM series WHERE series_id IN ('${A}','${B}')`);
  assert.throws(() => withheldPresentCountSql([]));
});

test("every catalogue statement builder applies the exclusion it is given", () => {
  const ids = [A, B];
  const builders: Array<[string, string]> = [
    ["searchFtsSql", searchFtsSql(ids)], ["searchFtsCountSql", searchFtsCountSql(ids)],
    ["searchLikeSql", searchLikeSql(ids)], ["searchLikeCountSql", searchLikeCountSql(ids)],
    ["browseAllSql", browseAllSql(ids)],
    ["searchFtsSourceSql", searchFtsSourceSql("insee_melodi", ids)],
    ["searchFtsSourceCountSql", searchFtsSourceCountSql("insee_melodi", ids)],
    ["searchLikeSourceSql", searchLikeSourceSql("insee_melodi", ids)],
    ["searchLikeSourceCountSql", searchLikeSourceCountSql("insee_melodi", ids)],
    ["browseSourceSql", browseSourceSql("insee_melodi", ids)],
    ["browseSourceVisibleCountSql", browseSourceVisibleCountSql("insee_melodi", ids)],
    ["seriesIdsForSourceSql", seriesIdsForSourceSql("insee_melodi", ids)],
  ];
  for (const [name, sql] of builders) {
    assert.ok(sql.includes(`NOT IN ('${A}','${B}')`), `${name} lost the exclusion`);
    // the exclusion sits in the WHERE clause, before LIMIT / ORDER BY
    const at = sql.indexOf("NOT IN (");
    for (const tail of ["LIMIT", "ORDER BY"]) if (sql.includes(tail)) assert.ok(at < sql.indexOf(tail), name);
  }
});

test("a source with no withheld id gets byte-identical SQL", () => {
  assert.equal(browseSourceSql("wid", []), browseSourceSql("wid"));
  assert.equal(seriesIdsForSourceSql("wid", forSource({ ids: new Set([A]), etag: "x", from: "t", closed: new Set<string>() }, "wid")),
               seriesIdsForSourceSql("wid"));
  assert.equal(searchFtsSql([]), searchFtsSql());
});

test("forSource scopes the ids by source", () => {
  const w = { ids: new Set([A, B, OTHER]), etag: "x", from: "t", closed: new Set<string>() };
  assert.deepEqual(forSource(w, "insee_melodi"), [A, B]);
  assert.deepEqual(forSource(w, "wid"), [OTHER]);
  assert.deepEqual(forSource(w, "ecb"), []);
});

test("the 404 is its own error, names the id, carries the owner's sentence, and is never cached", async () => {
  const r = withheldResponse(A);
  assert.equal(r.status, 404);
  assert.equal(r.headers.get("cache-control"), "no-store");
  const b = await r.json() as { error: string; series_id: string; detail: string };
  assert.equal(b.error, "withheld");
  assert.equal(b.series_id, A);
  assert.equal(b.detail, "Temporarily withheld: our copy is incomplete and is being re-downloaded from INSEE; " +
    "it is listed again when the download is complete.");
});

test("the cache key carries the list's tag and drops api_key", () => {
  const k = keyWithWithheld(new Request("https://x.test/v1/catalog?source=insee_melodi&api_key=SECRET"),
                            { ids: new Set<string>(), etag: "store-abc", from: "store", closed: new Set<string>() });
  const u = new URL(k.url);
  assert.equal(u.searchParams.get("__withheld"), "store-abc");
  assert.equal(u.searchParams.get("api_key"), null);
  assert.equal(u.searchParams.get("source"), "insee_melodi");
});

test("the FLOOR withholds every source it names WHOLE; a good read and the last good copy close nothing (AR-341 #2)",
     async () => {
  resetWithheldCache();
  const f = await loadWithheld(async () => { throw new Error("down"); }, "store", () => 1);
  assert.equal(f.from, "floor");
  assert.ok(isSourceClosed(f, "insee_melodi"), "the floor's source is closed");
  assert.ok(isWithheld(f, "insee_melodi:DS_WITHHELD_AFTER_THE_FLOOR"),
            "an id withheld in the store after the floor was committed is still not shown");
  assert.ok(!isWithheld(f, "ecb:EXR:D.AUD.EUR.SP00.A") && !isSourceClosed(f, "ecb"), "other sources stay open");
  resetWithheldCache();
  const g = await loadWithheld(async () => ({ text: obj({ [A]: ent }), etag: "e" }), "store", () => 1);
  assert.equal(g.closed.size, 0);
  assert.ok(isWithheld(g, A) && !isWithheld(g, "insee_melodi:DD_CNA_APU"), "a good read withholds its ids only");
  const lg = await loadWithheld(async () => { throw new Error("down"); }, "store", () => 1 + TTL_OK_MS + 1);
  assert.equal(lg.from, "last-good");
  assert.equal(lg.closed.size, 0);
});

test("presentCount runs once per database and id set per list load, not per request (AR-341 #5)", async () => {
  let n = 0;
  let fail = false;
  const db = { prepare: (_sql: string) => ({ first: async () => { n++; if (fail) throw new Error("d1"); return { n: 7 }; } }) };
  const d = db as unknown as D1Database;
  const w1 = { ids: new Set([A]), etag: "a", from: "store", closed: new Set<string>() };
  const w2 = { ids: new Set([A]), etag: "b", from: "store", closed: new Set<string>() };
  assert.equal(await presentCount(d, [A], w1, "CATALOG"), 7);
  assert.equal(await presentCount(d, [A], w1, "CATALOG"), 7);
  assert.equal(n, 1, "the second request of the same list load reads nothing");
  await presentCount(d, [A], w1, "CATALOG_CLIMATE");
  assert.equal(n, 2, "the other database is its own count");
  await presentCount(d, [A], w2, "CATALOG");
  assert.equal(n, 3, "a new list load counts again");
  assert.equal(await presentCount(d, [], w2, "CATALOG"), 0);
  assert.equal(n, 3, "no ids, no statement");
  fail = true;
  await assert.rejects(presentCount(d, [B], w2, "CATALOG"));
  fail = false;
  assert.equal(await presentCount(d, [B], w2, "CATALOG"), 7, "a failed count is not memoised");
  assert.equal(n, 5);
});

test("a series withheld only because its source is closed gets the unreadable-list sentence (AR-341 re-review N2)",
     async () => {
  resetWithheldCache();
  const f = await loadWithheld(async () => { throw new Error("down"); }, "store", () => 1);
  const floorId = WITHHELD_FLOOR.find((i) => i.startsWith("insee_melodi:"))!;   // dst ids sort first since 2026-10-19
  const r1 = await withheldResponse(floorId, f).json() as { error: string; detail: string };
  assert.equal(r1.error, "withheld");
  assert.match(r1.detail, /our copy is incomplete and is being re-downloaded from INSEE/, "a floor id IS incomplete");
  const r2 = await withheldResponse("insee_melodi:DD_CNA_APU", f).json() as { error: string; detail: string };
  assert.equal(r2.error, "withheld", "still the fail-closed 404 'withheld'");
  assert.equal(r2.detail, LIST_UNREADABLE_DETAIL);
  assert.equal(LIST_UNREADABLE_DETAIL,
               "Temporarily unavailable: the withheld-tables list could not be read; try again in a minute.");
  assert.doesNotMatch(r2.detail, /incomplete/, "never 'our copy is incomplete' for a table not on the list");
});

// dst (owner decision 2026-10-19): the zero-collapse tables. A dst catalogue id has a SECOND colon.
const D = "dst:DST:DNINTVAL";
const DENMARK = "Temporarily withheld: our copy of this table shows only zeros because it was downloaded at the wrong " +
  "level of detail; it is listed again when it is downloaded again correctly from Statistics Denmark.";

test("dst: the id rule admits a second colon and still refuses what could break the literal", () => {
  assert.deepEqual([...parseWithheld(obj({ [D]: { ...ent, until: "operator" }, [A]: ent }))!].sort(), [D, A].sort());
  for (const bad of ["dst:DST:x'y", "dst:DST:a b", "dst:", "dst:DST:%", "DST:DST:X", "dst:DST:x;DROP", "dst:DST:\"x\""]) {
    assert.equal(parseWithheld(obj({ [bad]: ent })), null, bad);
  }
  assert.equal(withheldExcl([D, A]), ` AND series_id NOT IN ('${D}','${A}')`);
});

test("dst: a withheld id answers 404 with the Denmark sentence; insee keeps its sentence", async () => {
  const b = await withheldResponse(D).json() as { error: string; series_id: string; detail: string };
  assert.equal(b.error, "withheld");
  assert.equal(b.series_id, D);
  assert.equal(b.detail, DENMARK);
  assert.equal(withheldDetail(A), "Temporarily withheld: our copy is incomplete and is being re-downloaded from INSEE; " +
    "it is listed again when the download is complete.");
  assert.equal(withheldDetail("wid:WID:x"), "Temporarily withheld: our copy is incomplete and is being re-downloaded " +
    "from the publisher; it is listed again when the download is complete.", "the default is unchanged");
});

test("dst: the bundle reason is per source; insee's and the default are byte-unchanged", () => {
  assert.equal(withheldReason(D), "withheld: our copy of this table shows only zeros because it was downloaded at the " +
    "wrong level of detail; it is listed again when it is downloaded again correctly from Statistics Denmark (HTTP " +
    "404 'withheld' on direct fetch)");
  const old = "withheld: our copy is incomplete and is being re-downloaded; it is listed again when the download is " +
    "complete (HTTP 404 'withheld' on direct fetch)";
  assert.equal(withheldReason(A), old);
  assert.equal(withheldReason("wid:WID:x"), old);
});

test("dst: the committed floor names dst, so an unreadable list closes dst WHOLE with the unreadable-list sentence",
     async () => {
  assert.ok(WITHHELD_FLOOR.includes(D), "the floor must name the dst tables");
  resetWithheldCache();
  const f = await loadWithheld(async () => { throw new Error("down"); }, "store", () => 1);
  assert.ok(isSourceClosed(f, "dst"), "dst is a floor source");
  assert.ok(isWithheld(f, "dst:DST:FOLK1A"), "a complete dst table is hidden while the list is unreadable");
  const k = await withheldResponse("dst:DST:FOLK1A", f).json() as { error: string; detail: string };
  assert.equal(k.error, "withheld");
  assert.equal(k.detail, LIST_UNREADABLE_DETAIL);
  const h = await withheldResponse(D, f).json() as { detail: string };
  assert.equal(h.detail, DENMARK, "a dst id in the floor keeps its own sentence");
  resetWithheldCache();
  const g = await loadWithheld(async () => ({ text: obj({ [D]: { ...ent, until: "operator" } }), etag: "e" }), "store",
                               () => 1);
  assert.ok(isWithheld(g, D) && !isWithheld(g, "dst:DST:FOLK1A") && !isSourceClosed(g, "dst"),
            "a good read withholds the listed dst ids only");
});
