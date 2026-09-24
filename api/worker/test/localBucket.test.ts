// SERIES_BUCKET for the self-hosted origin (src/localBucket.ts) against a fake of the blob sidecar's
// protocol (tools/selfhost/blob_sidecar.py; the real sidecar has its own pytest). Pins the R2 behaviours
// series.ts relies on: null for a missing key; a range whose size is the FULL size; a failed onlyIf that
// returns a BODYLESS object (not null); httpEtag quoted; gzip metadata from x-blob-content-encoding.
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { test } from "node:test";

import { LocalBucket } from "../src/localBucket.ts";

const OBJECTS: Record<string, { body: Buffer; etag: string; enc?: string; meta?: Record<string, string> }> = {
  "series/a.csv": { body: Buffer.from("series_id,date,value\na,2020-01-01,1\n"), etag: "e-a", meta: { csvmd5: "m" } },
  "series/g.csv": { body: Buffer.from([0x1f, 0x8b, 8, 0, 1, 2, 3]), etag: "e-g", enc: "gzip" },
};

const rangesSeen: string[] = [];

function fakeSidecar(): Promise<{ server: Server; base: string }> {
  const server = createServer((req, res) => {
    const key = decodeURIComponent((req.url ?? "").replace(/^\/o\//, ""));
    const o = OBJECTS[key];
    if (!o) { res.writeHead(404, { "content-length": "0" }); res.end(); return; }
    const h: Record<string, string> = {
      "content-type": "application/octet-stream", "x-blob-size": String(o.body.length), "x-blob-etag": o.etag,
      "x-blob-custom-metadata": Buffer.from(JSON.stringify(o.meta ?? {})).toString("base64"),
    };
    if (o.enc) h["x-blob-content-encoding"] = o.enc;
    const im = req.headers["if-match"];
    if (im && String(im).replace(/"/g, "") !== o.etag) { res.writeHead(412, { ...h, "content-length": "0" }); res.end(); return; }
    if (req.headers["range"]) rangesSeen.push(String(req.headers["range"]));
    const m = /^bytes=(\d+)-(\d*)$/.exec(String(req.headers["range"] ?? ""));
    if (m) {
      const a = Number(m[1]);
      const b = m[2] ? Number(m[2]) : o.body.length - 1;
      const part = o.body.subarray(a, b + 1);
      res.writeHead(206, { ...h, "content-length": String(part.length) });
      res.end(part);
      return;
    }
    res.writeHead(200, { ...h, "content-length": String(o.body.length) });
    res.end(o.body);
  });
  return new Promise((ok) => server.listen(0, "127.0.0.1", () => {
    const addr = server.address();
    ok({ server, base: `http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}` });
  }));
}

test("LocalBucket mirrors the R2 behaviours series.ts relies on", async (t) => {
  const { server, base } = await fakeSidecar();
  t.after(() => server.close());
  const b = new LocalBucket(base);

  assert.equal(await b.get("series/none.csv"), null, "a missing key is null");

  const a = await b.get("series/a.csv");
  assert.ok(a && "body" in a);
  assert.equal(await a.text(), OBJECTS["series/a.csv"].body.toString());
  assert.equal(a.size, OBJECTS["series/a.csv"].body.length);
  assert.equal(a.etag, "e-a");
  assert.equal(a.httpEtag, '"e-a"', "httpEtag is quoted, as R2's is");
  assert.deepEqual(a.customMetadata, { csvmd5: "m" });
  assert.equal(a.httpMetadata.contentEncoding, undefined);

  const g = await b.get("series/g.csv");
  assert.ok(g && "body" in g);
  assert.equal(g.httpMetadata.contentEncoding, "gzip");
  assert.deepEqual(new Uint8Array(await g.arrayBuffer()), new Uint8Array(OBJECTS["series/g.csv"].body),
                   "stored gzip bytes arrive as stored, not inflated");

  const size = OBJECTS["series/a.csv"].body.length;
  const tail = await b.get("series/a.csv", { range: { offset: size - 4, length: 4 } });
  assert.ok(tail && "body" in tail);
  assert.equal(tail.size, size, "a range still reports the FULL size");
  assert.equal(await tail.text(), OBJECTS["series/a.csv"].body.subarray(size - 4).toString());

  // a range in the MIDDLE (R1171: the tail range above cannot see an end off by one - the fake clamps it)
  const mid = await b.get("series/a.csv", { range: { offset: 10, length: 10 } });
  assert.ok(mid && "body" in mid);
  assert.equal(await mid.text(), OBJECTS["series/a.csv"].body.subarray(10, 20).toString(), "exactly bytes 10..19");
  assert.equal(rangesSeen.at(-1), "bytes=10-19", "the Range header is inclusive of the last byte");

  const same = await b.get("series/a.csv", { onlyIf: { etagMatches: "e-a" } });
  assert.ok(same && "body" in same, "a matching onlyIf returns the body");
  await same.body.cancel();
  const changed = await b.get("series/a.csv", { onlyIf: { etagMatches: "stale" } });
  assert.ok(changed !== null, "a failed onlyIf is NOT null");
  assert.equal("body" in changed, false, "a failed onlyIf is a bodyless object");
});

test("a sidecar error is thrown, never read as an object (R1168 M12)", async (t) => {
  const server = createServer((_req, res) => { res.writeHead(500, { "content-length": "0" }); res.end(); });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", () => ok()));
  t.after(() => server.close());
  const addr = server.address();
  const b = new LocalBucket(`http://127.0.0.1:${typeof addr === "object" && addr ? addr.port : 0}`);
  await assert.rejects(() => b.get("series/a.csv"), /answered 500/);
});

test("options the adapter does not implement are refused, not ignored (R1168 (e))", async () => {
  const b = new LocalBucket("http://127.0.0.1:1");
  // deno-lint-ignore no-explicit-any
  const get = (o: unknown) => b.get("k", o as any);
  await assert.rejects(() => get({ range: { suffix: 4 } }), /unsupported range field/);
  await assert.rejects(() => get({ range: new Headers({ range: "bytes=0-1" }) }), /range must be/);
  await assert.rejects(() => get({ onlyIf: { etagDoesNotMatch: "x" } }), /unsupported onlyIf field/);
  await assert.rejects(() => get({ onlyIf: new Headers({ "if-match": "x" }) }), /onlyIf must be/);
  await assert.rejects(() => get({ ssecKey: "k" }), /unsupported option/);
});

test("the origin's bucket refuses every write", async () => {
  const b = new LocalBucket("http://127.0.0.1:1");
  await assert.rejects(() => b.put());
  await assert.rejects(() => b.delete());
});
