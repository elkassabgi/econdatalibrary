// ---------------------------------------------------------------------------
// src/bundle.ts  --  GET /v1/bundle  ->  bundle MANIFEST (client-side fan-out)
//
// CONTRACT.md [w10]: the Worker NEVER streams a server-assembled zip (subrequest
// + memory limits). It returns a Frictionless-shaped datapackage.json SKELETON:
// one resource per source, `path` = the resource's stable URL, plus econdl:*
// provenance from the registry. The client fetches each resource URL and
// assembles the zip locally.
//
// Params: ids= (repeatable and/or comma-separated) OR source=; snapshot=YYYY-MM-DD
// (default today). Unresolvable ids are returned under econdl:unresolved -- loud,
// never dropped (honesty rule). We respect the Worker 50-subrequest cap by doing
// at most one D1 lookup per distinct source (not per id), and by emitting per-SERIES
// stable URLs the client fans out to -- the Worker itself makes no R2 fan-out.
// ---------------------------------------------------------------------------

import type { Env, SourceRow, LicenseRow, SeriesRow, SeriesIdRow } from "./types";
import { SELECT_SOURCE, SELECT_LICENSE, SELECT_SERIES, SERIES_IDS_FOR_SOURCE } from "./sql";
import { json, badRequest, licenseBlock, supportedSources, sourceOf } from "./util";
import { NON_REDISTRIBUTABLE, isSeriesCarvedOut } from "./denylist";

const PROFILE = "tabular-data-package";
const SCHEMA_VERSION = "1.0";

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

function collectIds(url: URL): string[] {
  const ids: string[] = [];
  for (const raw of url.searchParams.getAll("ids")) {
    for (const part of raw.split(",")) {
      const id = part.trim();
      if (id) ids.push(id);
    }
  }
  return ids;
}

interface ProvenanceBlock {
  source_id: string;
  name: string | null;
  homepage: string | null;
  attribution: string | null;
  terms_url: string | null;
  license: ReturnType<typeof licenseBlock>;
  citation: string;
}

async function provenance(
  source: string, snapshot: string, env: Env,
): Promise<ProvenanceBlock> {
  const src = await env.CATALOG.prepare(SELECT_SOURCE).bind(source).first<SourceRow>();
  const licId = src?.license_id ?? null;
  const lic = licId
    ? await env.CATALOG.prepare(SELECT_LICENSE).bind(licId).first<LicenseRow>()
    : null;
  const name = src?.name ?? source;
  const year = snapshot.slice(0, 4);
  let citation = `${name} (${year}). Accessed via Econ Data Library, snapshot ${snapshot}.`;
  if (src?.homepage) citation += ` ${src.homepage}`;
  return {
    source_id: source,
    name: src?.name ?? null,
    homepage: src?.homepage ?? null,
    attribution: src?.attribution ?? null,
    terms_url: src?.terms_url ?? null,
    license: licenseBlock(lic),
    citation,
  };
}

export async function handleBundle(url: URL, env: Env): Promise<Response> {
  const snapshot = url.searchParams.get("snapshot") ?? today();
  const source = url.searchParams.get("source");
  let ids = collectIds(url);

  if (ids.length === 0 && source) {
    if (NON_REDISTRIBUTABLE.has(source)) {
      return badRequest(`source '${source}' cannot be bundled: its licence forbids re-hosting (HTTP 451)`);
    }
    const res = await env.CATALOG.prepare(SERIES_IDS_FOR_SOURCE).bind(source).all<SeriesIdRow>();
    ids = (res.results ?? []).map((r) => r.series_id);
    if (ids.length === 0) return badRequest(`no catalog series found for source '${source}'`);
  }
  if (ids.length === 0) {
    return badRequest("bundle requires ids= (comma/repeatable) or source=");
  }

  const supported = supportedSources(env);
  const unresolved: Array<{ id: string; reason: string }> = [];
  const bySource = new Map<string, string[]>();

  // Validate each id against the catalog (one D1 read per id; bounded by the
  // client's request size). Group resolvable ids by source for one resource each.
  for (const id of ids) {
    const src = sourceOf(id);
    // Redistribution gate FIRST (compliance before catalog semantics): a series
    // from a denylisted source must never appear in a bundle manifest, even as a
    // stable URL — the /v1/series handler also 451s it, but the manifest itself
    // must not advertise it (same rule as /v1/sources and the site).
    if (NON_REDISTRIBUTABLE.has(src) || isSeriesCarvedOut(id)) {
      const reason = NON_REDISTRIBUTABLE.has(src)
        ? `not_redistributable: source '${src}' licence forbids re-hosting (HTTP 451 on direct fetch)`
        : `not_redistributable: this series embeds third-party data and is gated (HTTP 451 on direct fetch)`;
      unresolved.push({ id, reason });
      continue;
    }
    // Canonical order (CONTRACT.md v1.1, matches the dev shim): catalog membership
    // FIRST -> not_found for an unknown id; THEN resolver support -> not_migrated.
    const row = await env.CATALOG.prepare(SELECT_SERIES).bind(id).first<SeriesRow>();
    if (!row) {
      unresolved.push({ id, reason: "not_found: unknown series id" });
      continue;
    }
    if (!supported.has(src)) {
      unresolved.push({ id, reason: `not_migrated: source '${src}' has no resolver yet` });
      continue;
    }
    const list = bySource.get(src) ?? [];
    list.push(id);
    bySource.set(src, list);
  }

  // One resource per source. `path` lists the stable per-series CSV URLs the
  // client fans out to (it assembles the zip locally; the Worker streams nothing).
  // Each resource carries its provenance (incl. citation) -- CONTRACT.md v1.1
  // "/v1/bundle manifest" pin: {name, profile, format, mediatype, path:[urls],
  // econdl:series_ids, econdl:provenance(incl citation)}.
  const resources: Array<{
    name: string;
    profile: string;
    format: string;
    mediatype: string;
    path: string[];
    "econdl:series_ids": string[];
    "econdl:provenance": ProvenanceBlock;
  }> = [];
  for (const [src, memberIds] of [...bySource.entries()].sort()) {
    memberIds.sort();
    resources.push({
      name: src,
      profile: "tabular-data-resource",
      format: "csv",
      mediatype: "text/csv",
      // The client fetches each of these to build data/<source>.csv locally.
      path: memberIds.map((id) => `/v1/series/${encodeURIComponent(id)}.csv`),
      "econdl:series_ids": memberIds,
      "econdl:provenance": await provenance(src, snapshot, env),
      // bytes/hash are unknown at manifest time (the Worker never reads the rows);
      // omitted here, never faked. The client records sha256 as it fetches.
    });
  }

  const totalUrls = resources.reduce((n, r) => n + r.path.length, 0);

  // Distinct license blocks across the resources (Frictionless `licenses[]`),
  // de-duplicated by id. Mirrors clients/python/econdl/_bundle.py::_distinct_licenses
  // and the dev shim: {name: <license id>, title: <license name>, path: <url>}.
  const seenLic = new Set<string>();
  const licenses: Array<{ name: string; title: string | null; path: string | null }> = [];
  for (const r of resources) {
    const lic = r["econdl:provenance"].license;
    if (lic && lic.id && !seenLic.has(lic.id)) {
      seenLic.add(lic.id);
      licenses.push({ name: lic.id, title: lic.name, path: lic.url });
    }
  }

  // CANONICAL v1.1 top-level key ORDER (CONTRACT.md "Canonical response shapes"),
  // byte-for-byte with api/devserver.py::h_bundle. No `created` field (it is not
  // in the pin; a manifest's reproducibility anchor is econdl:snapshot_date).
  const datapackage = {
    name: "econdl-bundle",
    profile: PROFILE,
    "econdl:schema_version": SCHEMA_VERSION,
    "econdl:client": "econdl-worker-manifest",
    "econdl:snapshot_date": snapshot,
    "econdl:series_requested": [...ids].sort(),
    "econdl:resource_url_count": totalUrls,
    // Honest note on the 50-subrequest cap: the MANIFEST is one response and does
    // no fan-out itself, so it never trips the cap. The CLIENT must batch its own
    // fetches; if a single source's url list exceeds the client's budget it pages.
    "econdl:fanout_note":
      "Client fetches each resource path URL and assembles the zip locally. The " +
      "Worker streams no zip and makes no R2 fan-out (50-subrequest cap respected).",
    licenses,
    resources,
    "econdl:unresolved": unresolved, // loud, never dropped
  };

  return json(datapackage);
}
