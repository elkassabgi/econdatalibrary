// src/withheld.ts and the SQL it feeds (src/sql.ts): the list's validation, the per-isolate cache and its
// fail-closed rules (AR-339 Part 2 finding 5), the literal NOT IN, the cache-key tag (finding 6), the 404.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import {
  TTL_FAIL_MS, TTL_OK_MS, WITHHELD_MAX, floorWithheld, forSource, isWithheld, keyWithWithheld, loadWithheld,
  parseWithheld, resetWithheldCache, withheldResponse,
} from "../src/withheld.ts";
import { WITHHELD_FLOOR } from "../src/withheldFloor.ts";
import {
  browseAllSql, browseSourceSql, browseSourceVisibleCountSql, searchFtsCountSql, searchFtsSourceCountSql,
  searchFtsSourceSql, searchFtsSql, searchLikeCountSql, searchLikeSourceCountSql, searchLikeSourceSql, searchLikeSql,
  seriesIdsForSourceSql, withheldExcl, withheldPresentCountSql,
} from "../src/sql.ts";

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
  assert.equal(seriesIdsForSourceSql("wid", forSource({ ids: new Set([A]), etag: "x", from: "t" }, "wid")),
               seriesIdsForSourceSql("wid"));
  assert.equal(searchFtsSql([]), searchFtsSql());
});

test("forSource scopes the ids by source", () => {
  const w = { ids: new Set([A, B, OTHER]), etag: "x", from: "t" };
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
                            { ids: new Set(), etag: "store-abc", from: "store" });
  const u = new URL(k.url);
  assert.equal(u.searchParams.get("__withheld"), "store-abc");
  assert.equal(u.searchParams.get("api_key"), null);
  assert.equal(u.searchParams.get("source"), "insee_melodi");
});
