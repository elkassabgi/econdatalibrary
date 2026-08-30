// ---------------------------------------------------------------------------
// src/geoProjection.ts — per-geo projection of GROUPED series (pure logic).
//
// WHY. Grouped sources catalogue ONE id per indicator (worldbank_wdi:<CODE>)
// whose CSV object carries every economy's rows (row ids WDI:<CODE>:<GEO>).
// Users address a series as indicator x economy — Ahmed's 2026-08-26 report was
// exactly `worldbank:DT.DOD.DECT.CD:LMY` returning 404 while every byte it
// asked for was already served inside the grouped object. Minting per-geo
// catalog ids was rejected on measured grounds: ~293k D1 rows against the
// ~10 GB ceiling (#45 is Ahmed's reserved capacity call) and `worldbank` is a
// DISPUTED licence whose per-indicator third-party review exists only for its
// 3 legacy indicators. A projection adds ZERO catalog rows and ZERO R2 objects,
// and serves ONLY bytes the CLEARED grouped id already serves in full.
//
// The redistribution gate still runs on BOTH spellings (series.ts): the
// indicator carve (denylist.ts SERIES_CARVEOUTS) keys on the segment between
// the first and second colon, which is the indicator code in both the alias
// and the canonical id — R32: carve-outs must cover sibling ids.
// ---------------------------------------------------------------------------

/** Alias source -> grouped source whose object carries the per-geo rows.
 *  A source mapping to ITSELF is a grouped holder and also honors `?geo=`.
 *  Extend per source ONLY after checking, in this order: (a) the grouped
 *  source's licence verdict is CLEARED in DATABASE_LICENSES_VERBATIM.md,
 *  (b) its served CSV row ids end in the geo segment (measured on a real
 *  object, not assumed), (c) SERIES_CARVEOUTS covers both id spellings.
 *  tests/test_worker_geo_projection.py enforces (a) and (c) at commit time. */
export const GEO_PROJECTION_SOURCES: Record<string, string> = {
  worldbank: "worldbank_wdi",
  worldbank_wdi: "worldbank_wdi",
};

export interface GeoAlias { canonical: string; geo: string; }

/** World Bank 2-char economy codes -> the 3-char form the grouped store actually holds.
 *
 *  WHO THIS SERVES, stated precisely because my first version of this comment claimed a
 *  beneficiary it does not have. The eight CATALOGUED `worldbank:<IND>:{XD,XM,XN,XT}` ids
 *  are NOT helped by this map and never needed it: `series.ts` calls `geoAlias()` only when
 *  the D1 lookup MISSES, and those eight are in the catalogue, so the alias branch is never
 *  entered for them. Measured on the running worker — `worldbank:NY.GDP.MKTP.CD:XD`
 *  returns 200 with rows to 2024-12-31 today, against a `:ZZZ` control that 404s. What this
 *  map actually reaches is (a) UNCATALOGUED 3-part `worldbank:<IND>:<2-char>` ids, which do
 *  miss and do fall through to the alias, and (b) `worldbank_wdi:<IND>?geo=<2-char>`.
 *
 *  MEASURED, not assumed, because a mapping asserted from memory is how R504 happened:
 *  (a) the publisher's /v2/country list (295 entries, fetched 2026-08-30) gives
 *      XD->HIC "High income", XM->LIC "Low income", XN->LMC "Lower middle income",
 *      XT->UMC "Upper middle income";
 *  (b) across ALL 1,486 grouped parquets the store holds HIC/LIC/LMC/UMC (34,703 / 31,498 /
 *      36,775 / 34,475 rows) and holds ZERO rows under XD/XM/XN/XT — so the direction of the
 *      map is confirmed by our data, not only by the publisher's vocabulary.
 *
 *  NOT the fix for AR-019/R500. That defect lives in the FETCHER: worldbank.py reports these
 *  eight as `missing` every run and cannot refresh them, so they freeze at the next World
 *  Bank release. Mapping the geo back to 2-char at worldbank.py's key filter is the repair
 *  that makes `missing` go to zero; a worker-side alias leaves it at eight forever.
 *
 *  Adding an entry requires BOTH measurements above, and the value must itself satisfy
 *  GEO_RE — see canonicalGeo. The publisher derives 18 such codes; the four here are the
 *  four our catalogue actually uses, verified by enumerating all 263 legacy geos. */
export const GEO_CODE_ALIASES: Record<string, string> = {
  XD: "HIC",
  XM: "LIC",
  XN: "LMC",
  XT: "UMC",
};

const GEO_RE = /^[A-Z0-9]{2,3}$/;

/** Uppercase, validate, translate, then VALIDATE THE RESULT. Applied on both entry points
 *  (the 3-part alias and `?geo=`), because a user who hits a 404 and retries with `?geo=XD`
 *  must not get a second wrong answer.
 *
 *  The second validation is not redundant: the first one checks what the USER typed, and a
 *  bad map VALUE would sail past it into the row filter unchecked. Nothing else validates
 *  the map's right-hand side. */
function canonicalGeo(raw: string): string | null {
  const geo = raw.trim().toUpperCase();
  if (!GEO_RE.test(geo)) return null;
  const mapped = GEO_CODE_ALIASES[geo] || geo;
  return GEO_RE.test(mapped) ? mapped : null;
}

/** worldbank:DT.DOD.DECT.CD:LMY -> { canonical: "worldbank_wdi:DT.DOD.DECT.CD",
 *  geo: "LMY" }. Null for anything that is not a 3-part id of a projection
 *  source with a plausible World Bank economy code (2-3 alphanumerics). */
export function geoAlias(seriesId: string): GeoAlias | null {
  const parts = seriesId.split(":");
  if (parts.length !== 3) return null;
  const [src, code, geoRaw] = parts;
  const target = GEO_PROJECTION_SOURCES[src];
  if (!target || !code) return null;
  const geo = canonicalGeo(geoRaw);
  if (!geo) return null;
  return { canonical: `${target}:${code}`, geo };
}

/** Validate a `?geo=` value the same way the alias path does. */
export function normalizeGeoParam(raw: string): string | null {
  return canonicalGeo(raw);
}

/** Keep the header plus rows whose id column's LAST ':'-segment equals geo
 *  (exact, case-sensitive — geo is already uppercased). Returns the filtered
 *  text and its data-row count; when the count is 0, `geos` lists what the
 *  object actually holds so the honest 404 can name real alternatives. */
export function filterGeoRows(
  text: string, geo: string,
): { text: string; rows: number; geos: string[] } {
  const lines = text.split("\n");
  const out: string[] = [lines.length > 0 ? lines[0] : "series_id,obs_date,value"];
  let rows = 0;
  const seen = new Set<string>();
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line.trim() === "") continue;
    const comma = line.indexOf(",");
    if (comma < 0) continue; // malformed row: never serve it, never list its "geo"
    const id = line.slice(0, comma);
    const seg = id.slice(id.lastIndexOf(":") + 1);
    seen.add(seg);
    if (seg === geo) { out.push(line); rows++; }
  }
  const geos = rows === 0 ? [...seen].sort() : [];
  return { text: out.join("\n") + "\n", rows, geos };
}
