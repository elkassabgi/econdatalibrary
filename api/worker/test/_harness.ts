// Shared plumbing for the tests that run the REAL worker in workerd (unstable_dev). Not a test file itself
// (CI runs test/*.test.ts only).
//
// Every path is built from this file's location, never from the working directory: CI runs
// `node --test api/worker/test/*.test.ts` from the REPO ROOT, and tests that opened "wrangler.toml" by a
// relative path passed on a workstation (run from api/worker) and failed there (review R1171/R1172).
//
// Local D1 and R2 state is seeded and read IN-PROCESS through wrangler's getPlatformProxy on the same
// persist folder the worker uses - not by spawning `wrangler d1 execute`, which failed intermittently
// ("fetch failed") when run right after another in-process worker had stopped.
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { getPlatformProxy, unstable_dev } from "wrangler";

export const WORKER_DIR = fileURLToPath(new URL("..", import.meta.url));
export const at = (...parts: string[]) => join(WORKER_DIR, ...parts);
export const EDGE_CONFIG = at("wrangler.toml");
export const ORIGIN_CONFIG = at("wrangler.origin.toml");

export type Dev = Awaited<ReturnType<typeof unstable_dev>>;
// Local simulation bindings, typed loosely on purpose: the proxy hands back the workers-types shapes.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type Bindings = Record<string, any>;

export function newPersist(tag = "econ-test-"): string {
  return mkdtempSync(join(tmpdir(), tag));
}

export async function start(config: string, vars: Record<string, string>,
                            persist: string = newPersist(), testScheduled = false): Promise<Dev> {
  return unstable_dev(at("src", "index.ts"), {
    config, vars, local: true, persistTo: persist, logLevel: "none",
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true, testScheduled },
  });
}

/** Run `fn` with the local bindings of `config` over `persist` (the same state the worker sees). */
export async function withBindings<T>(config: string, persist: string, fn: (env: Bindings) => Promise<T>): Promise<T> {
  const px = await getPlatformProxy({ configPath: config, persist: { path: join(persist, "v3") } });
  try {
    return await fn(px.env as Bindings);
  } finally {
    await px.dispose();
  }
}

/** Execute a SQL script (comments allowed) statement by statement against one D1 binding. */
export async function execSql(db: Bindings[string], script: string): Promise<void> {
  const body = script.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
  for (const stmt of body.split(";").map((s) => s.trim()).filter(Boolean)) await db.prepare(stmt).run();
}

export async function all(db: Bindings[string], sql: string): Promise<Record<string, unknown>[]> {
  return (await db.prepare(sql).all()).results;
}

/** wrangler.toml WITHOUT the econ D1 (CATALOG, CATALOG_CLIMATE) and econ R2 (SERIES_BUCKET) bindings - the
 *  edge as it must work after T0. Any code path that still touches them throws (the binding is undefined)
 *  instead of quietly reading an empty local simulation. `main` is made absolute because the file is
 *  written outside the worker folder. */
export function edgeOnlyConfig(): string {
  const dropped = new Set(['binding = "CATALOG"', 'binding = "CATALOG_CLIMATE"', 'binding = "SERIES_BUCKET"']);
  const out: string[] = [];
  let block: string[] = [];
  const flush = () => {
    if (!block.some((l) => dropped.has(l.trim()))) out.push(...block);
    block = [];
  };
  for (const line of readFileSync(EDGE_CONFIG, "utf8").split(/\r?\n/)) {
    if (/^\s*\[/.test(line)) flush();
    block.push(/^main\s*=/.test(line) ? `main = ${JSON.stringify(at("src", "index.ts").replace(/\\/g, "/"))}` : line);
  }
  flush();
  const text = out.join("\n");
  for (const d of dropped) if (text.includes(d)) throw new Error(`edgeOnlyConfig kept ${d}`);
  if (!text.includes('binding = "USERS"')) throw new Error("edgeOnlyConfig dropped USERS");
  const file = join(newPersist("econ-edgecfg-"), "wrangler.edge-only.toml");
  writeFileSync(file, text);
  return file;
}
