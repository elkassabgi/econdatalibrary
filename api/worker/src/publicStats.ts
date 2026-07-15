// ---------------------------------------------------------------------------
// src/publicStats.ts  --  GET /v1/public-stats
//
// Family usage stats for the Econ Data Library "Live Statistics" page.
//
// The USER-side figures (total_users, per-country map, institutions) are read
// from the SHARED ElkassabgiData identity DB (env.USERS = hfdatalibrary-db)
// using the SAME aggregation hf's worker uses, so the user count / world map /
// country list are identical across libraries by construction — one login,
// one user base.
//
// The DOWNLOAD figures are this library's OWN: counted from econ_download_log
// (a separate table in the shared DB, so hf's download counters are never
// inflated by econ traffic and vice-versa). No bytes column exists there, so
// only counts are reported.
//
// Read-only. No auth. Aggregated only — no PII leaves this endpoint. CORS "*".
// ---------------------------------------------------------------------------

import type { Env } from "./types";
import { json } from "./util";

// Full-name / abbreviation -> ISO-3166 alpha-2. Mirrors hf's COUNTRY_TO_ISO so
// the normalized world map matches HF exactly. Keyed by LOWER(TRIM(value)).
const COUNTRY_TO_ISO: Record<string, string> = {
  // North America
  'united states': 'US', 'united states of america': 'US', 'usa': 'US', 'u.s.': 'US', 'u.s.a.': 'US', 'us': 'US', 'america': 'US',
  'canada': 'CA',
  'mexico': 'MX',
  // Europe
  'united kingdom': 'GB', 'uk': 'GB', 'great britain': 'GB', 'britain': 'GB', 'england': 'GB', 'scotland': 'GB', 'wales': 'GB', 'northern ireland': 'GB',
  'ireland': 'IE',
  'germany': 'DE', 'deutschland': 'DE',
  'france': 'FR',
  'spain': 'ES', 'españa': 'ES',
  'portugal': 'PT',
  'italy': 'IT', 'italia': 'IT',
  'netherlands': 'NL', 'holland': 'NL', 'the netherlands': 'NL',
  'belgium': 'BE',
  'switzerland': 'CH',
  'austria': 'AT',
  'sweden': 'SE',
  'norway': 'NO',
  'denmark': 'DK',
  'finland': 'FI',
  'iceland': 'IS',
  'poland': 'PL',
  'czech republic': 'CZ', 'czechia': 'CZ',
  'slovakia': 'SK',
  'hungary': 'HU',
  'romania': 'RO',
  'bulgaria': 'BG',
  'greece': 'GR',
  'turkey': 'TR', 'türkiye': 'TR', 'turkiye': 'TR',
  'russia': 'RU', 'russian federation': 'RU',
  'ukraine': 'UA',
  'belarus': 'BY',
  'lithuania': 'LT',
  'latvia': 'LV',
  'estonia': 'EE',
  'croatia': 'HR',
  'serbia': 'RS',
  'slovenia': 'SI',
  'luxembourg': 'LU',
  // Asia
  'china': 'CN', 'people\'s republic of china': 'CN', 'prc': 'CN', 'mainland china': 'CN',
  'hong kong': 'HK',
  'taiwan': 'TW', 'republic of china': 'TW', 'roc': 'TW',
  'japan': 'JP',
  'south korea': 'KR', 'korea': 'KR', 'republic of korea': 'KR', 'rok': 'KR',
  'north korea': 'KP', 'dprk': 'KP',
  'india': 'IN',
  'pakistan': 'PK',
  'bangladesh': 'BD',
  'sri lanka': 'LK',
  'nepal': 'NP',
  'singapore': 'SG',
  'malaysia': 'MY',
  'indonesia': 'ID',
  'philippines': 'PH', 'the philippines': 'PH',
  'thailand': 'TH',
  'vietnam': 'VN', 'viet nam': 'VN',
  'cambodia': 'KH',
  'laos': 'LA',
  'myanmar': 'MM', 'burma': 'MM',
  'mongolia': 'MN',
  'kazakhstan': 'KZ',
  'uzbekistan': 'UZ',
  'iran': 'IR',
  'iraq': 'IQ',
  'israel': 'IL',
  'palestine': 'PS',
  'lebanon': 'LB',
  'syria': 'SY',
  'jordan': 'JO',
  'saudi arabia': 'SA', 'ksa': 'SA',
  'united arab emirates': 'AE', 'uae': 'AE', 'u.a.e.': 'AE',
  'qatar': 'QA',
  'kuwait': 'KW',
  'bahrain': 'BH',
  'oman': 'OM',
  'yemen': 'YE',
  'afghanistan': 'AF',
  // Oceania
  'australia': 'AU',
  'new zealand': 'NZ',
  // South America
  'brazil': 'BR', 'brasil': 'BR',
  'argentina': 'AR',
  'chile': 'CL',
  'colombia': 'CO',
  'peru': 'PE',
  'venezuela': 'VE',
  'ecuador': 'EC',
  'uruguay': 'UY',
  'paraguay': 'PY',
  'bolivia': 'BO',
  // Africa
  'south africa': 'ZA',
  'egypt': 'EG',
  'nigeria': 'NG',
  'kenya': 'KE',
  'ethiopia': 'ET',
  'morocco': 'MA',
  'algeria': 'DZ',
  'tunisia': 'TN',
  'ghana': 'GH',
  'tanzania': 'TZ',
  'uganda': 'UG',
  'senegal': 'SN',
  'cameroon': 'CM',
  'zimbabwe': 'ZW',
  'angola': 'AO',
  // Caribbean / Central America
  'costa rica': 'CR',
  'panama': 'PA',
  'guatemala': 'GT',
  'honduras': 'HN',
  'el salvador': 'SV',
  'nicaragua': 'NI',
  'cuba': 'CU',
  'dominican republic': 'DO',
  'jamaica': 'JM',
  'haiti': 'HT',
  'puerto rico': 'PR',
  'trinidad and tobago': 'TT',
};

function normalizeCountry(input: string | null | undefined): string | null {
  if (!input || typeof input !== "string") return null;
  const s = input.trim();
  if (s.length === 0) return null;
  // Pure ISO-2 (e.g. "US", "cn") — accept directly.
  if (/^[A-Za-z]{2}$/.test(s)) return s.toUpperCase();
  // Full-name / common-abbreviation lookup; unrecognized free-text is dropped.
  return COUNTRY_TO_ISO[s.toLowerCase()] || null;
}

// Non-institution placeholders users type instead of a real school ("none",
// "self", "student", ...). Dropped BEFORE ranking so junk doesn't consume top
// slots. Mirrors hf's list. Real companies are intentionally NOT blocked.
const INSTITUTION_BLOCKLIST: string[] = [
  'none', 'n/a', 'na', 'n.a.', 'n.a', 'no', 'nil', 'null', 'nan',
  'self', 'myself', 'me', 'private', 'personal', 'home', 'individual',
  'individuals', 'independent', 'independent trader', 'unaffiliated',
  'unknown', 'student', 'retired', 'retail', 'retail trader',
  'retail investor', 'freelance', 'freelancer', 'trader', 'aleppo',
  '-', '--', '.', '..', '...', 'x', 'xx', 'test', 'asdf',
  'non applicable', 'independent researcher', 'private trader', 'private use',
  'privat', 'perso', 'persoonlijk', 'full-time employee', 'company', 'exploring',
  'university', 'labs', 'new in fin', 'test university', 'rebel', 'myass',
  '1qaz2wsx', 'gz', 'berln',
];

// Canonical names so the same school typed different ways merges into one row.
// Keyed by LOWER(TRIM(value)). Mirrors hf's INSTITUTION_ALIASES.
const INSTITUTION_ALIASES: Record<string, string> = {
  'stanford': 'Stanford University',
  'havard': 'Harvard University',
  'hongkong university': 'University of Hong Kong',
  '中国人民大学': 'Renmin University of China',
  'erasmus universiteit rotterdam': 'Erasmus University Rotterdam',
  'michigan': 'University of Michigan',
  'illinois': 'University of Illinois',
  'cambridge': 'University of Cambridge',
  'oxford university': 'University of Oxford',
  'old dominion university': 'Old Dominion University',
  'fordham': 'Fordham University',
};

export async function handlePublicStats(env: Env): Promise<Response> {
  const U = env.USERS;

  // Shared identity: total registered accounts (all rows) — matches admin count.
  const totalUsers = await U.prepare("SELECT COUNT(*) AS c FROM users").first<{ c: number }>();

  // This library's OWN downloads (econ_download_log; separate from hf's counters).
  const totalDl = await U.prepare("SELECT COUNT(*) AS c FROM econ_download_log").first<{ c: number }>();
  const todayDl = await U.prepare(
    "SELECT COUNT(*) AS c FROM econ_download_log WHERE ts > datetime('now','-1 day')",
  ).first<{ c: number }>();
  const weekDl = await U.prepare(
    "SELECT COUNT(*) AS c FROM econ_download_log WHERE ts > datetime('now','-7 days')",
  ).first<{ c: number }>();
  // Data served (bytes). Recorded per-download since byte tracking was added; the
  // column is 0/NULL for downloads logged before then, so this is a rising floor.
  const totalBytes = await U.prepare(
    "SELECT COALESCE(SUM(bytes),0) AS b FROM econ_download_log",
  ).first<{ b: number }>();

  // Per-country DISTINCT active users: self-declared profile country UNION any
  // country they've logged in from. UNION dedupes so a user counts once/country.
  const countries = await U.prepare(
    `WITH user_countries AS (
       SELECT id AS user_id, UPPER(country) AS country FROM users
         WHERE is_active = 1 AND country != ''
       UNION
       SELECT lh.user_id, UPPER(lh.country) FROM login_history lh
         JOIN users u ON lh.user_id = u.id
         WHERE u.is_active = 1 AND lh.country IS NOT NULL
           AND lh.country != '' AND lh.country != 'unknown'
     )
     SELECT country, COUNT(DISTINCT user_id) AS users FROM user_countries
     GROUP BY country ORDER BY users DESC`,
  ).all<{ country: string; users: number }>();

  const userCountryMap: Record<string, number> = {};
  for (const row of countries.results ?? []) {
    const code = normalizeCountry(row.country);
    if (code) userCountryMap[code] = (userCountryMap[code] ?? 0) + row.users;
  }

  // Distinct institutions: drop hidden + placeholder junk, then alias-merge and
  // take the top 50 by user count.
  const ph = INSTITUTION_BLOCKLIST.map(() => "?").join(",");
  const instRaw = await U.prepare(
    `SELECT institution, COUNT(*) AS users FROM users
       WHERE is_active = 1 AND TRIM(institution) != ''
         AND COALESCE(hide_institution, 0) = 0
         AND LOWER(TRIM(institution)) NOT IN (${ph})
       GROUP BY institution`,
  ).bind(...INSTITUTION_BLOCKLIST).all<{ institution: string; users: number }>();

  const instMerged: Record<string, number> = {};
  for (const row of instRaw.results ?? []) {
    const name = (row.institution ?? "").trim();
    if (!name) continue;
    const canon = INSTITUTION_ALIASES[name.toLowerCase()] ?? name;
    instMerged[canon] = (instMerged[canon] ?? 0) + row.users;
  }
  const institutions = Object.keys(instMerged)
    .map((institution) => ({ institution, users: instMerged[institution] }))
    .sort((a, b) => b.users - a.users)
    .slice(0, 50);

  // Most-downloaded sources. The log's source is the series_id prefix before
  // the first ':'. CRITICAL: the log contains HISTORICAL downloads of sources
  // that have since been PURGED from the catalog (e.g. WTO) — naming those
  // would resurrect them. So we WHITELIST strictly against the current catalog
  // `source` table: a downloaded source appears only if it is still catalogued,
  // and is displayed with the catalog's own name. Purged sources vanish.
  const dlBySource = await U.prepare(
    "SELECT substr(series_id, 1, instr(series_id, ':') - 1) AS source, " +
    "COUNT(*) AS downloads FROM econ_download_log WHERE instr(series_id, ':') > 0 " +
    "GROUP BY source ORDER BY downloads DESC LIMIT 200",
  ).all<{ source: string; downloads: number }>();
  const catRows = await env.CATALOG.prepare("SELECT source_id, name FROM source")
    .all<{ source_id: string; name: string }>();
  const catName: Record<string, string> = {};
  for (const r of catRows.results ?? []) catName[r.source_id] = r.name || r.source_id;
  const topSources = (dlBySource.results ?? [])
    .filter((r) => r.source && catName[r.source] !== undefined) // whitelist: still catalogued
    .slice(0, 5)
    .map((r) => ({ source_id: r.source, name: catName[r.source], downloads: r.downloads }));

  // Visitor layer for the map — THIS site's own Cloudflare traffic (light-blue in
  // the two-tone map, distinct from the shared dark-blue user layer). Runs only
  // when analytics creds are configured; any failure is swallowed so an analytics
  // hiccup never breaks the endpoint. countryMap keys are ISO-2 already.
  const visitorCountryMap: Record<string, number> = {};
  let totalVisitors = 0;
  let totalPageViews = 0;
  if (env.CF_API_TOKEN && env.CF_ZONE_ID) {
    try {
      const since = new Date(Date.now() - 30 * 86_400_000).toISOString().slice(0, 10);
      const gql =
        `query { viewer { zones(filter: {zoneTag: "${env.CF_ZONE_ID}"}) { ` +
        `httpRequests1dGroups(limit: 10000, filter: {date_geq: "${since}"}) { ` +
        `sum { pageViews countryMap { clientCountryName requests } } uniq { uniques } } } } }`;
      const cfRes = await fetch("https://api.cloudflare.com/client/v4/graphql", {
        method: "POST",
        headers: { authorization: `Bearer ${env.CF_API_TOKEN}`, "content-type": "application/json" },
        body: JSON.stringify({ query: gql }),
      });
      const cfData = await cfRes.json() as {
        data?: { viewer?: { zones?: Array<{ httpRequests1dGroups?: Array<{
          sum?: { pageViews?: number; countryMap?: Array<{ clientCountryName?: string; requests?: number }> };
          uniq?: { uniques?: number };
        }> }> } };
      };
      const zones = cfData?.data?.viewer?.zones ?? [];
      for (const g of zones[0]?.httpRequests1dGroups ?? []) {
        totalVisitors += g.uniq?.uniques ?? 0;
        totalPageViews += g.sum?.pageViews ?? 0;
        for (const c of g.sum?.countryMap ?? []) {
          const code = (c.clientCountryName ?? "").toUpperCase();
          if (code && code !== "XX") {
            visitorCountryMap[code] = (visitorCountryMap[code] ?? 0) + (c.requests ?? 0);
          }
        }
      }
    } catch { /* analytics optional — never fail the endpoint over it */ }
  }

  return json({
    total_users: totalUsers?.c ?? 0,
    total_downloads: totalDl?.c ?? 0,
    downloads_today: todayDl?.c ?? 0,
    downloads_this_week: weekDl?.c ?? 0,
    total_bytes_served: totalBytes?.b ?? 0,
    countries: userCountryMap,
    country_count: Object.keys(userCountryMap).length,
    visitor_countries: visitorCountryMap,
    visitor_country_count: Object.keys(visitorCountryMap).length,
    total_visitors: totalVisitors,
    total_page_views: totalPageViews,
    institutions,
    institution_count: institutions.length,
    top_sources: topSources,
  });
}
