// The self-hosted origin's rules (src/localMode.ts, docs/ECON_SELF_HOSTING_PLAN.md).
//
// The origin runs the same worker code behind the edge. These tests pin the four differences:
// it refuses without a configured secret and without the right secret header, it never answers the
// edge-only routes, every answer leaves private/no-store, and a length-less 200 is marked for the
// edge to count. Each refusal has a passing control beside it, so a gate that always refuses fails.
import assert from "node:assert/strict";
import { test } from "node:test";

import {
  COUNT_HEADER, EDGE_ONLY_PATHS, ORIGIN_SECRET_HEADER, finalizeLocal, isLocal, originGate, secretsEqual,
} from "../src/localMode.ts";

const URL_OK = "http://127.0.0.1:8799/v1/sources";

function req(url: string, secret?: string): Request {
  const headers = new Headers();
  if (secret !== undefined) headers.set(ORIGIN_SECRET_HEADER, secret);
  return new Request(url, { headers });
}

test("local mode is on only for LOCAL = '1'", () => {
  assert.equal(isLocal({ LOCAL: "1" }), true);
  for (const v of [undefined, "", "0", "true", "yes"]) assert.equal(isLocal({ LOCAL: v }), false, String(v));
});

test("an origin with no ORIGIN_SECRET refuses every request, even one carrying a secret", async () => {
  for (const env of [{}, { ORIGIN_SECRET: "" }]) {
    const r = await originGate(req(URL_OK, "anything"), env);
    assert.ok(r, "must refuse");
    assert.equal(r.status, 503);
    assert.equal(r.headers.get("cache-control"), "private, no-store");
  }
});

test("a missing or wrong secret header is refused; the right one passes (control)", async () => {
  const env = { ORIGIN_SECRET: "s3cret-value" };
  assert.equal((await originGate(req(URL_OK), env))?.status, 403);
  assert.equal((await originGate(req(URL_OK, "s3cret-valuE"), env))?.status, 403);
  assert.equal((await originGate(req(URL_OK, "s3cret-value2"), env))?.status, 403);
  // (header values lose surrounding spaces by HTTP rule, so " s3cret-value " IS the right secret)
  assert.equal(await originGate(req(URL_OK, "s3cret-value"), env), null);
});

test("the edge-only routes are never answered by the origin, even with the right secret", async () => {
  const env = { ORIGIN_SECRET: "k" };
  assert.deepEqual([...EDGE_ONLY_PATHS].sort(), ["/v1/public-stats", "/v1/pv", "/v1/pv/report"]);
  for (const p of EDGE_ONLY_PATHS) {
    const r = await originGate(req(`http://127.0.0.1:8799${p}`, "k"), env);
    assert.equal(r?.status, 404, p);
  }
});

test("every answer leaves private, no-store - including one that said public", () => {
  for (const status of [200, 304, 400, 404, 451, 500, 503]) {
    const body = status === 304 ? null : "x";
    const r = finalizeLocal(new Response(body, { status, headers: { "cache-control": "public, max-age=300" } }));
    assert.equal(r.headers.get("cache-control"), "private, no-store", String(status));
  }
});

test("only a 200 without content-length is marked for the edge to count", () => {
  const streamed = finalizeLocal(new Response(new ReadableStream({ start(c) { c.enqueue(new Uint8Array([1])); c.close(); } }), { status: 200 }));
  assert.equal(streamed.headers.get(COUNT_HEADER), "1");
  const sized = finalizeLocal(new Response("abc", { status: 200, headers: { "content-length": "3" } }));
  assert.equal(sized.headers.get(COUNT_HEADER), null);
  const refused = finalizeLocal(new Response("no", { status: 451, headers: { [COUNT_HEADER]: "1" } }));
  assert.equal(refused.headers.get(COUNT_HEADER), null, "a stray marker on a non-200 is removed");
});

test("secretsEqual is equality, whatever the lengths", async () => {
  assert.equal(await secretsEqual("abc", "abc"), true);
  assert.equal(await secretsEqual("abc", "abd"), false);
  assert.equal(await secretsEqual("", "abc"), false);
  assert.equal(await secretsEqual("abc".repeat(100), "abc"), false);
});
