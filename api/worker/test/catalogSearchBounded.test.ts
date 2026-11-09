// node --test test/catalogSearchBounded.test.ts
//
// A GLOBAL /v1/catalog SEARCH (no source=) MUST NEVER READ THE WHOLE `series` TABLE.
//
// Until 2026-11-09 a global q that FTS did not match - zero rows, or a MATCH that threw - fell to
// `title LIKE '%q%' OR series_id LIKE '%q%'` over every row of BOTH catalogue databases, plus a COUNT of each:
// up to four full table scans per request. A leading wildcard has no index, and LIMIT does not bound a scan that
// finds nothing. Each distinct no-match q (a partial word typed into a search box; a crawler's or an agent's free
// text) paid the set again.
//
// These cases drive the REAL handleCatalog against a fake D1 that records every statement it is asked to
// prepare, and judge each one on its WHERE clause: a statement over `series` must carry a bounding predicate
// (the FTS join, a PK range, a PK IN-list, or `source_id = ?`), and none may LIKE without a source bound. The
// scoped (source=) LIKE fallback is a CONTROL: it must still run, so the recorder is shown able to see a LIKE
// statement when one is issued. No source list is spelled out here: a gated id, when needed, is taken from the
// denylist at run time.
import assert from "node:assert/strict";
import { register } from "node:module";
import { test } from "node:test";

register(new URL("./tsResolveHooks.mjs", import.meta.url));
const {
  handleCatalog, ftsTermsQuery, ftsPrefixQuery, idPrefixQuery, bareCodeIds, ID_PREFIX_COUNT_CAP, PREFIX_COUNT_CAP,
} = await import("../src/catalog.ts");
const { NON_REDISTRIBUTABLE } = await import("../src/denylist.ts");

type Row = Record<string, unknown>;
type Call = { db: string; sql: string; binds: unknown[] };
/** Answers one statement: rows for .all(), a row for .first(). Throwing simulates D1 raising. */
type Answer = (sql: string, binds: unknown[], db: string) => Row[] | Row | null;

function fakeDb(name: string, calls: Call[], answer: Answer) {
  return {
    prepare(sql: string) {
      const run = (binds: unknown[]) => {
        calls.push({ db: name, sql, binds });
        return answer(sql, binds, name);
      };
      const stmt = (binds: unknown[]) => ({
        all: async () => ({ results: run(binds) as Row[] }),
        first: async () => run(binds) as Row | null,
      });
      return { bind: (...b: unknown[]) => stmt(b), ...stmt([]) };
    },
  };
}

const NO_WITHHELD = { ids: new Set<string>(), etag: "t", from: "store", closed: new Set<string>() };

/** A malformed MATCH (an unbalanced paren, `-`, `.`, or FTS5's `col:` filter syntax outside quotes) throws. */
function ftsThrowsOnSyntax(expr: string) {
  if (/[():.\-]/.test(expr.replace(/"[^"]*"/g, ""))) throw new Error("fts5: syntax error");
}

/** Default catalogue: every statement finds nothing. */
const EMPTY: Answer = (sql, binds) => {
  if (sql.includes("MATCH")) ftsThrowsOnSyntax(String(binds[0]));
  return sql.includes("COUNT(") ? { n: 0 } : [];
};

const isFts = (c: Call) => c.sql.includes("series_fts MATCH ?");
const isPage = (c: Call) => !c.sql.includes("COUNT(");

async function ask(qs: string, answer: Answer = EMPTY, supported = "bea,bls,noaa") {
  const calls: Call[] = [];
  const env = {
    CATALOG: fakeDb("CATALOG", calls, answer),
    CATALOG_CLIMATE: fakeDb("CATALOG_CLIMATE", calls, answer),
    SUPPORTED_SOURCES: supported,
  };
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const res = await handleCatalog(new URL(`https://x/v1/catalog?${qs}`), env as any, NO_WITHHELD);
  const body = JSON.parse(await res.text());
  return { status: res.status, body, calls };
}

/** A statement over `series` with no predicate an index can bound - i.e. a full table scan. */
function unboundedSeriesScan(sql: string): boolean {
  const s = sql.replace(/\s+/g, " ");
  if (!/\bFROM series\b(?!_fts)/.test(s)) return false;           // not a read of series
  if (/series_fts MATCH \?/.test(s)) return false;                  // the FTS index drives the join
  if (/series_id >= \? AND series_id < \?/.test(s)) return false;   // PK range
  if (/\bseries_id (IN \(|= \?)/.test(s)) return false;             // PK lookups
  if (/\bsource_id = \?/.test(s)) return false;                     // idx_series_source
  return true;
}

function assertBounded(calls: Call[], what: string) {
  for (const c of calls) {
    assert.ok(!unboundedSeriesScan(c.sql), `${what}: unbounded scan of series on ${c.db}:\n${c.sql}`);
    assert.ok(!/LIKE \?/.test(c.sql) || /source_id = \?/.test(c.sql),
              `${what}: a LIKE over series with no source bound on ${c.db}:\n${c.sql}`);
  }
}

test("the recogniser itself: the old global LIKE pair is unbounded; the bounded shapes are not", () => {
  // Pinned so the property cannot pass by recognising nothing (a receipt that can fail).
  const oldPage = "SELECT series_id FROM series WHERE (title LIKE ? OR series_id LIKE ?) AND source_id NOT IN ('x') LIMIT ? OFFSET ?";
  const oldCount = "SELECT COUNT(*) AS n FROM series WHERE (title LIKE ? OR series_id LIKE ?) AND source_id NOT IN ('x')";
  assert.equal(unboundedSeriesScan(oldPage), true);
  assert.equal(unboundedSeriesScan(oldCount), true);
  assert.equal(unboundedSeriesScan("SELECT s.x FROM series_fts f JOIN series s ON s.series_id = f.series_id WHERE series_fts MATCH ? LIMIT ?"), false);
  assert.equal(unboundedSeriesScan("SELECT x FROM series WHERE series_id >= ? AND series_id < ? LIMIT ?"), false);
  assert.equal(unboundedSeriesScan("SELECT x FROM series WHERE series_id IN (SELECT value FROM json_each(?))"), false);
});

test("a zero-match global q (a partial word) issues only bounded statements and answers total 0", async () => {
  const r = await ask("q=emplo");
  assert.equal(r.status, 200);
  assert.equal(r.body.total, 0);
  assert.deepEqual(r.body.results, []);
  assertBounded(r.calls, "q=emplo");
  // FTS page x2, bare-code PK seeks x2 (bea,bls on the primary; noaa on the shard), prefix FTS page x2
  assert.equal(r.calls.length, 6, r.calls.map((c) => c.sql).join("\n---\n"));
  assert.deepEqual(r.calls.filter(isFts).map((c) => c.binds[0]), ["emplo", "emplo", '"emplo"*', '"emplo"*']);
  assert.equal(r.body.total_is_lower_bound, undefined, "the field appears only when a cap was reached");
});

test("the same zero-match q under lang= (the site's non-English search) is bounded too", async () => {
  const r = await ask("lang=fr&limit=50&q=ch%C3%B4m");
  assert.equal(r.status, 200);
  assert.equal(r.body.total, 0);
  assertBounded(r.calls, "lang=fr q=chôm");
  assert.ok(r.calls.filter(isFts).some((c) => c.binds[0] === '"chôm"*'));
});

test("a malformed MATCH (covid-19, GDP (nominal) is retried as quoted terms on the index, never as LIKE", async () => {
  const hit = { series_id: "bea:GDP", source_id: "bea", title: "GDP nominal", metadata: null };
  const answer: Answer = (sql, binds) => {
    if (sql.includes("MATCH")) {
      ftsThrowsOnSyntax(String(binds[0]));
      if (sql.includes("COUNT(")) return { n: 1 };
      return binds[0] === '"GDP" "nominal"' ? [hit] : [];
    }
    return sql.includes("COUNT(") ? { n: 0 } : [];
  };
  const r = await ask("q=" + encodeURIComponent("GDP (nominal"), answer);
  assert.equal(r.status, 200);
  assertBounded(r.calls, "q=GDP (nominal");
  const exprs = r.calls.filter((c) => isFts(c) && isPage(c)).map((c) => c.binds[0]);
  assert.deepEqual(exprs, ["GDP (nominal", "GDP (nominal", '"GDP" "nominal"', '"GDP" "nominal"']);
  assert.deepEqual(r.body.results.map((x: Row) => x.series_id), ["bea:GDP", "bea:GDP"]);
  assert.equal(r.body.total, 2, "the two databases' FTS counts, summed");
  const c19 = await ask("q=covid-19");
  assertBounded(c19.calls, "q=covid-19");
  assert.ok(c19.calls.some((c) => c.binds[0] === '"covid" "19"'));
});

test("a second FTS failure (not a syntax problem) is a 500, not a cached empty 200", async () => {
  const broken: Answer = (sql) => { if (sql.includes("MATCH")) throw new Error("D1 down"); return []; };
  await assert.rejects(ask("q=unemployment", broken), /D1 down/);
});

test("a q with no word characters at all searches nothing after the failed MATCH", async () => {
  const r = await ask("q=" + encodeURIComponent("(("));
  assert.equal(r.status, 200);
  assert.equal(r.body.total, 0);
  assertBounded(r.calls, "q=((");
});

test("a bare native code is an exact PK lookup in every served source, typed and upper-cased", async () => {
  const row = { series_id: "bls:CUUR0000SA0", source_id: "bls", title: "CPI-U, all items", metadata: null };
  const answer: Answer = (sql, binds) => {
    if (sql.includes("json_each")) return (JSON.parse(String(binds[0])) as string[]).includes("bls:CUUR0000SA0") ? [row] : [];
    if (sql.includes("MATCH")) ftsThrowsOnSyntax(String(binds[0]));
    return sql.includes("COUNT(") ? { n: 0 } : [];
  };
  const r = await ask("q=cuur0000sa0", answer);
  assertBounded(r.calls, "q=cuur0000sa0");
  const seeks = r.calls.filter((c) => c.sql.includes("json_each"));
  assert.deepEqual(seeks.map((c) => [c.db, JSON.parse(String(c.binds[0]))]), [
    ["CATALOG", ["bea:cuur0000sa0", "bea:CUUR0000SA0", "bls:cuur0000sa0", "bls:CUUR0000SA0"]],
    ["CATALOG_CLIMATE", ["noaa:cuur0000sa0", "noaa:CUUR0000SA0"]],
  ]);
  assert.deepEqual(r.body.results.map((x: Row) => x.series_id), ["bls:CUUR0000SA0"]);
  assert.equal(r.body.total, 1);
  assert.ok(!r.calls.some((c) => c.binds[0] === '"cuur0000sa0"*'), "a hit stops before the prefix retry");
  const dotted = await ask("q=NY.GDP.MKTP.CD");
  assertBounded(dotted.calls, "q=NY.GDP.MKTP.CD");
  assert.ok(dotted.calls.some((c) => c.sql.includes("json_each")
    && (JSON.parse(String(c.binds[0])) as string[]).includes("bls:NY.GDP.MKTP.CD")));
});

test("the prefix retry's count is capped, and a capped total is marked as a lower bound", async () => {
  const many = Array.from({ length: 3 }, (_, i) => ({ series_id: `bls:E${i}`, source_id: "bls", title: "Employment", metadata: null }));
  const answer: Answer = (sql, binds) => {
    if (sql.includes("MATCH")) {
      if (binds[0] !== '"emplo"*') return sql.includes("COUNT(") ? { n: 0 } : [];
      return sql.includes("COUNT(") ? { n: Number(binds[1]) } : many;   // the database saturates the cap
    }
    return sql.includes("COUNT(") ? { n: 0 } : [];
  };
  const r = await ask("q=emplo", answer);
  assertBounded(r.calls, "q=emplo (prefix)");
  const counts = r.calls.filter((c) => isFts(c) && !isPage(c));
  assert.equal(counts.length, 2);
  for (const c of counts) {
    assert.match(c.sql.replace(/\s+/g, " "), /SELECT COUNT\(\*\) AS n FROM \(SELECT 1 FROM series_fts .* LIMIT \?\)$/);
    assert.equal(c.binds[1], PREFIX_COUNT_CAP + 1);
  }
  assert.equal(r.body.total, 2 * PREFIX_COUNT_CAP);
  assert.equal(r.body.total_is_lower_bound, true);
});

test("an id-shaped q is a PK-range seek in its source's database only", async () => {
  const rows = [{ series_id: "bls:CUUR0000SA0", source_id: "bls", title: "CPI", metadata: null }];
  const answer: Answer = (sql, binds) => {
    if (sql.includes("COUNT(")) return { n: 1 };
    return binds[0] === "bls:CUUR0" ? rows : [];
  };
  const r = await ask("q=bls%3ACUUR0", answer);
  assert.equal(r.status, 200);
  assertBounded(r.calls, "q=bls:CUUR0");
  assert.ok(r.calls.every((c) => c.db === "CATALOG"), "bls lives in the primary database");
  assert.ok(r.calls.every((c) => !c.sql.includes("MATCH")), "FTS5 would read `bls:` as a column filter");
  assert.deepEqual(r.calls[0].binds, ["bls:CUUR0", "bls:CUUR1", 50, 0]);
  assert.deepEqual(r.calls[1].binds, ["bls:CUUR0", "bls:CUUR1", ID_PREFIX_COUNT_CAP + 1]);
  assert.deepEqual(r.body.results.map((x: Row) => x.series_id), ["bls:CUUR0000SA0"]);
  assert.equal(r.body.total, 1);
});

test("a sharded source's id goes to the shard, not both databases", async () => {
  const r = await ask("q=noaa%3AGHCND");
  assertBounded(r.calls, "q=noaa:GHCND");
  assert.ok(r.calls.length > 0 && r.calls.every((c) => c.db === "CATALOG_CLIMATE"));
});

test("a gated source's id issues no statement and answers nothing", async (t) => {
  const gated = [...NON_REDISTRIBUTABLE][0];
  if (!gated) { t.skip("the denylist is empty"); return; }
  const r = await ask(`q=${gated}%3AX`);
  assert.equal(r.status, 200);
  assert.equal(r.calls.length, 0);
  assert.equal(r.body.total, 0);
});

test("id-prefix total is capped yet never below the page it returned", async () => {
  const row = (i: number) => ({ series_id: `bls:A${i}`, source_id: "bls", title: "t", metadata: null });
  const answer: Answer = (sql, binds) => (sql.includes("COUNT(") ? { n: Number(binds[2]) } : [row(1), row(2)]);
  const r = await ask(`q=bls%3AA&offset=${ID_PREFIX_COUNT_CAP + 10}&limit=2`, answer);
  assert.equal(r.body.results.length, 2);
  assert.equal(r.body.total, ID_PREFIX_COUNT_CAP + 12, "offset + page, so the page never contradicts its total");
  assert.equal(r.body.total_is_lower_bound, true);
});

test("CONTROL: the scoped (source=) zero-match search still takes its LIKE fallback", async () => {
  const r = await ask("source=bls&q=onfarm");
  const likes = r.calls.filter((c) => /LIKE \?/.test(c.sql));
  assert.equal(likes.length, 2, "the recorder sees the scoped LIKE page and its count");
  assert.ok(likes.every((c) => /source_id = \?/.test(c.sql)));
  assertBounded(r.calls, "source=bls&q=onfarm");
});

test("ftsTermsQuery, ftsPrefixQuery, idPrefixQuery, bareCodeIds", () => {
  assert.equal(ftsTermsQuery("GDP (nominal"), '"GDP" "nominal"');
  assert.equal(ftsTermsQuery('a"b OR c*'), '"a" "b" "OR" "c"');
  assert.equal(ftsTermsQuery("chômage-rate"), '"chômage" "rate"');
  assert.equal(ftsTermsQuery("(( ))"), null);
  assert.equal(ftsPrefixQuery("real emplo"), '"real" "emplo"*');
  assert.equal(ftsPrefixQuery("em"), null, "below the minimum prefix length");
  assert.equal(ftsPrefixQuery("unemployment in"), null, "the LAST word decides");
  assert.equal(ftsPrefixQuery("失业率"), '"失业率"*');
  const sup = new Set(["bea", "bls"]);
  assert.deepEqual(idPrefixQuery(" BLS:CUUR0000SA0 ", sup), { source: "bls", lo: "bls:CUUR0000SA0", hi: "bls:CUUR0000SA1" });
  assert.equal(idPrefixQuery("unknown:X", sup), null, "an unknown source part is free text");
  assert.equal(idPrefixQuery("bls:", sup), null);
  assert.equal(idPrefixQuery("bls:CUUR 0000", sup), null, "whitespace is not an id");
  assert.equal(idPrefixQuery("unemployment", sup), null);
  assert.deepEqual(bareCodeIds("CUUR0000SA0", sup, NO_WITHHELD), { primary: ["bea:CUUR0000SA0", "bls:CUUR0000SA0"], shard: [] });
  assert.deepEqual(bareCodeIds("unemployment rate", sup, NO_WITHHELD), { primary: [], shard: [] });
  assert.deepEqual(bareCodeIds("chômage", sup, NO_WITHHELD), { primary: [], shard: [] });
  const closed = { ...NO_WITHHELD, closed: new Set(["bls"]) };
  assert.deepEqual(bareCodeIds("CUUR0000SA0", sup, closed).primary, ["bea:CUUR0000SA0"], "a closed source is skipped");
});
