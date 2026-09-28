// Which local D1 file backs which binding of the origin (docs/ECON_SELF_HOSTING_PLAN.md, the swap).
//
// Miniflare stores each local D1 database as <persist>/v3/d1/miniflare-D1DatabaseObject/<hash>.sqlite, the
// hash derived from the database id. The swap must drop the primary and climate copies into the right two
// files, so the mapping is DISCOVERED, not assumed: the origin config's bindings are opened in-process on an
// empty folder, a marker table is written through each binding, and the file that holds it is that binding's.
// A wrangler upgrade that changes the hashing is then found by this run, not by a swap that serves the wrong
// catalogue from the wrong binding.
//
// Run from api/worker:  node ../../tools/selfhost/d1_slots.mjs [config]   -> prints JSON {"CATALOG": "<file>", ...}
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { pathToFileURL } from "node:url";

// wrangler is resolved from the WORKING folder (api/worker), not from this file's folder: tools/ has no
// node_modules, so a static `import "wrangler"` here fails with ERR_MODULE_NOT_FOUND. It is also the pinned
// wrangler the origin itself runs with, which is the one whose hashing matters.
const require = createRequire(join(process.cwd(), "package.json"));
const wrangler = await import(pathToFileURL(require.resolve("wrangler")).href);
const getPlatformProxy = wrangler.getPlatformProxy ?? wrangler.default?.getPlatformProxy;

const config = process.argv[2] ?? "wrangler.origin.toml";
const persist = mkdtempSync(join(tmpdir(), "econ-d1slots-"));
const bindings = ["CATALOG", "CATALOG_CLIMATE"];
const px = await getPlatformProxy({ configPath: config, persist: { path: join(persist, "v3") } });
try {
  for (const b of bindings) await px.env[b].prepare(`CREATE TABLE __slot_${b} (x INTEGER)`).run();
} finally {
  await px.dispose();
}
const dir = join(persist, "v3", "d1", "miniflare-D1DatabaseObject");
const out = {};
for (const f of readdirSync(dir).filter((f) => f.endsWith(".sqlite"))) {
  const db = new DatabaseSync(join(dir, f), { readOnly: true });
  const names = db.prepare("SELECT name FROM sqlite_master WHERE name LIKE '__slot_%'").all().map((r) => r.name);
  db.close();
  for (const n of names) out[n.replace("__slot_", "")] = f;
}
for (const b of bindings) if (!out[b]) throw new Error(`no local D1 file found for ${b}`);
if (out.CATALOG === out.CATALOG_CLIMATE) throw new Error("CATALOG and CATALOG_CLIMATE share one file");
rmSync(persist, { recursive: true, force: true });
console.log(JSON.stringify(out));
