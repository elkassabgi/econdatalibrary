"""Discoverable static catalog generator.

Reads the central metadata registry (data/catalog.db: source / license / series)
plus the operational sidecar (catalog/catalog.json) and emits, under catalog/site/:

  * <source>.html         one landing page per dataset (registered source), each
                          embedding a VALID schema.org/Dataset JSON-LD block
                          (the thing Google Dataset Search indexes) AND an inline
                          Croissant (schema.org JSON-LD) block.
  * sitemap.xml           lists every generated dataset page + the index.
  * index.html            a simple client-side searchable index of all datasets.

Design rules (ARCHITECTURE.md s3, STRATEGY.md):
  - The registry is the single source of truth. License / attribution / terms
    come from the `source` + `license` tables -- never invented.
  - License is a re-serve gate. For NON-reservable sources (license.reservable=0)
    the page advertises distribution as "metadata only" and the JSON-LD omits any
    downloadable distribution + sets isAccessibleForFree accordingly -- we never
    imply we redistribute restricted data.
  - sameAs carries Hugging Face / Zenodo placeholders (spokes); our own domain is
    the canonical landing URL.
  - No fabricated metadata. Every field traces to the DB or catalog.json. Fields
    we don't have are simply omitted.

Run:  python catalog/gen_site.py
"""
from __future__ import annotations

import html
import json
import os
import re
import sqlite3
from datetime import date, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB_PATH = os.path.join(ROOT, "data", "catalog.db")
CATALOG_JSON = os.path.join(HERE, "catalog.json")
OUT_DIR = os.path.join(HERE, "site")

# --- Canonical publication identity ------------------------------------------
# No production domain is wired yet (see STRATEGY.md: Worker/API not shipped).
# This is the single place to set it; everything below derives from it. It is a
# clearly-marked placeholder, NOT scraped/invented per-source metadata.
SITE_BASE = "https://econdatalibrary.com"
SITE_NAME = "Econ Data Library"
PUBLISHER = {
    "@type": "Organization",
    "name": "Econ Data Library",
    "url": SITE_BASE,
}

# sameAs spokes. Per-source HF/Zenodo handles are not yet minted, so we emit a
# deterministic *placeholder* slug under the org accounts. Marked as placeholder
# in the visible page; in JSON-LD they are valid absolute URLs (sameAs hints).
HF_ORG = "https://huggingface.co/datasets/econdatalibrary"
ZENODO_COMMUNITY = "https://zenodo.org/communities/econdatalibrary"

# Canonical license URLs for well-known license IDs. Used ONLY as a fallback when
# the registry's license.url is blank. This is a fixed, auditable mapping of
# standard licenses -- not per-source guessing.
LICENSE_URL_FALLBACK = {
    "cc0": "https://creativecommons.org/publicdomain/zero/1.0/",
    "cc-by-4.0": "https://creativecommons.org/licenses/by/4.0/",
    "cc-by-3.0": "https://creativecommons.org/licenses/by/3.0/",
    "cc-by-3.0-igo": "https://creativecommons.org/licenses/by/3.0/igo/",
    "cc-by-sa-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
    "cc-by-nc-sa-4.0": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    "cc-by-nc-sa-3.0-igo": "https://creativecommons.org/licenses/by-nc-sa/3.0/igo/",
    "us-public-domain": "https://www.usa.gov/government-works",
    "ogl-uk-3.0": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    "etalab-2.0": "https://www.etalab.gouv.fr/licence-ouverte-open-licence/",
    "odbl-1.0": "https://opendatacommons.org/licenses/odbl/1-0/",
    "nlod-2.0": "https://data.norge.no/nlod/en/2.0",
}

# Human-readable license labels (display only; not used in JSON-LD identifiers).
LICENSE_LABEL = {
    "cc0": "Creative Commons Zero (CC0)",
    "cc-by-4.0": "Creative Commons Attribution 4.0 (CC BY 4.0)",
    "cc-by-3.0": "Creative Commons Attribution 3.0 (CC BY 3.0)",
    "cc-by-3.0-igo": "Creative Commons Attribution 3.0 IGO (CC BY 3.0 IGO)",
    "cc-by-sa-4.0": "Creative Commons Attribution-ShareAlike 4.0 (CC BY-SA 4.0)",
    "cc-by-nc-sa-4.0": "Creative Commons BY-NC-SA 4.0",
    "cc-by-nc-sa-3.0-igo": "Creative Commons BY-NC-SA 3.0 IGO",
    "us-public-domain": "U.S. Government Work (public domain)",
    "ogl-uk-3.0": "UK Open Government Licence v3.0",
    "etalab-2.0": "Etalab Open Licence 2.0",
    "odbl-1.0": "Open Data Commons Open Database License (ODbL) 1.0",
    "nlod-2.0": "Norwegian Licence for Open Government Data 2.0",
    # Post-audit statuses (2026-07-14 verbatim license audit): every with-series
    # source now has a DEFINITIVE class; NEEDS-REVIEW remains only on empty
    # (not-yet-served) sources still being crawled.
    "NEEDS-REVIEW": "License not yet verified (no data served)",
    "verified-attribution": "Redistributable with attribution (provider terms verified)",
    "verified-nc": "Redistributable, non-commercial (provider terms verified)",
    "verified-open": "Freely redistributable (provider terms verified)",
    "audit-restricted": "Not redistributable — restricted provider terms (data available from the original provider)",
    "imf-terms": "IMF Terms of Use (redistribution with attribution)",
    "statcan-open": "Statistics Canada Open Licence",
    "ecb-attrib-nomodify": "ECB terms (attribution required, no modification)",
    "bis-attrib-nc": "BIS terms (attribution, non-commercial)",
    "zillow-research": "Zillow Research terms",
    "defillama-open": "DeFiLlama open terms",
    "whr-granted": "World Happiness Report (written permission, Figure 2.1 scope)",
    "damodaran-granted": "Written permission (A. Damodaran, 2026) — attribution required, non-commercial",
    "spi-embed-2026": "Social Progress Imperative (written permission: official embed only)",
    "custom-terms": "Custom provider terms",
    "dbnomics-passthrough": "Pass-through (see original provider terms)",
}

FREQ_LABEL = {
    "A": "Annual", "Q": "Quarterly", "M": "Monthly", "W": "Weekly",
    "D": "Daily", "1min": "1-minute", "irregular": "Irregular",
}

# schema.org/Repetition values -> ISO-8601 durations for JSON-LD repeatFrequency.
FREQ_ISO = {
    "A": "P1Y", "Q": "P3M", "M": "P1M", "W": "P1W", "D": "P1D",
}

TODAY = date.today().isoformat()

# Dates beyond this are treated as data sentinels (9999-12-31, year 6016, ...)
# and excluded from temporalCoverage so we never publish corrupt coverage.
MAX_SANE_YEAR = date.today().year + 2


# ---------------------------------------------------------------------------- #
#  Helpers
# ---------------------------------------------------------------------------- #
def esc(s) -> str:
    return html.escape("" if s is None else str(s))


def xml_esc(s) -> str:
    return html.escape("" if s is None else str(s), quote=True)


def sane_date(d):
    """Return YYYY-MM-DD if the date is real and not a sentinel, else None."""
    if not d:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", str(d))
    if not m:
        return None
    yr = int(m.group(1))
    if yr < 1000 or yr > MAX_SANE_YEAR:
        return None
    return m.group(0)


def license_url(license_id, registry_url):
    if registry_url:
        return registry_url
    return LICENSE_URL_FALLBACK.get(license_id)


def license_label(license_id):
    return LICENSE_LABEL.get(license_id, license_id)


def first_sentence(text, limit=300):
    """A short, clean description for meta tags / JSON-LD when the operational
    description is a long technical note. Never fabricates -- just truncates."""
    if not text:
        return None
    t = " ".join(str(text).split())
    if len(t) <= limit:
        return t
    cut = t[:limit].rsplit(" ", 1)[0]
    return cut + "…"


# ---------------------------------------------------------------------------- #
#  Load the registry + sidecar
# ---------------------------------------------------------------------------- #
def load_registry():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    licenses = {r["license_id"]: dict(r) for r in con.execute("SELECT * FROM license")}

    sources = {}
    for r in con.execute("SELECT * FROM source ORDER BY source_id"):
        sources[r["source_id"]] = dict(r)

    # Per-source series rollup (coverage facets) -- only from the registry.
    series_roll = {}
    q = """
        SELECT source_id,
               COUNT(*)                       AS n_series,
               MIN(start_date)                AS min_start,
               MAX(end_date)                  AS max_end,
               COUNT(DISTINCT geography)      AS n_geo,
               MAX(last_updated)              AS last_updated
        FROM series
        GROUP BY source_id
    """
    for r in con.execute(q):
        series_roll[r["source_id"]] = dict(r)

    # Distinct frequency / category per source (small sets -> fetch separately).
    freqs, cats, geos = {}, {}, {}
    for r in con.execute(
        "SELECT source_id, frequency FROM series "
        "WHERE frequency IS NOT NULL GROUP BY source_id, frequency"
    ):
        freqs.setdefault(r["source_id"], []).append(r["frequency"])
    for r in con.execute(
        "SELECT source_id, category FROM series "
        "WHERE category IS NOT NULL GROUP BY source_id, category"
    ):
        cats.setdefault(r["source_id"], []).append(r["category"])
    for r in con.execute(
        "SELECT source_id, geography FROM series "
        "WHERE geography IS NOT NULL AND geography != '' "
        "GROUP BY source_id, geography LIMIT 100000"
    ):
        geos.setdefault(r["source_id"], []).append(r["geography"])

    # Source-level human metadata (Task #5): description_key / citation_* / processing.
    # These are baked identically onto every series of a source, so one row suffices.
    source_meta = {}
    for r in con.execute("SELECT source_id, metadata FROM series GROUP BY source_id"):
        try:
            m = json.loads(r["metadata"]) if r["metadata"] else {}
        except (ValueError, TypeError):
            m = {}
        source_meta[r["source_id"]] = {
            "description_key": m.get("description_key"),
            "description_processing": m.get("description_processing"),
            "citation_short": m.get("citation_short"),
            "citation_long": m.get("citation_long"),
        }

    con.close()

    for sid, roll in series_roll.items():
        roll["frequencies"] = sorted(set(freqs.get(sid, [])))
        roll["categories"] = sorted(set(cats.get(sid, [])))
        roll["geographies"] = sorted(set(geos.get(sid, [])))

    return licenses, sources, series_roll, source_meta


def load_sidecar():
    if not os.path.exists(CATALOG_JSON):
        return {}, None
    cat = json.load(open(CATALOG_JSON, encoding="utf-8"))
    by_id = {s["id"]: s for s in cat.get("sources", [])}
    return by_id, cat.get("generated")


# ---------------------------------------------------------------------------- #
#  Build the per-dataset metadata model (the registry-grounded record)
# ---------------------------------------------------------------------------- #
def build_record(sid, src, lic, roll, side, s5=None):
    """Assemble everything we know about one dataset from the registry + sidecar.
    Returns a plain dict; downstream renderers never touch the DB again.
    `s5` carries the source-level Task#5 metadata (description_key / citation_* /
    processing) baked into the series rows."""
    s5 = s5 or {}
    license_id = src.get("license_id")
    lrow = lic.get(license_id, {})
    reservable = bool(lrow.get("reservable", 0))

    # Description: prefer the operational sidecar note; fall back to attribution.
    desc_full = (side or {}).get("description") or src.get("attribution")
    desc_short = first_sentence(desc_full)

    cov_start = sane_date(roll.get("min_start")) if roll else None
    cov_end = sane_date(roll.get("max_end")) if roll else None
    # sidecar last_obs is a real "newest observation" signal when present.
    last_obs = sane_date((side or {}).get("last_obs"))
    if last_obs and (not cov_end or last_obs > cov_end):
        cov_end = last_obs

    rec = {
        "id": sid,
        "name": src.get("name") or sid,
        "homepage": src.get("homepage"),
        "attribution": src.get("attribution"),
        "terms_url": src.get("terms_url"),
        "license_id": license_id,
        "license_label": license_label(license_id),
        "license_url": license_url(license_id, lrow.get("url")),
        "reservable": reservable,
        "commercial_ok": bool(lrow.get("commercial_ok", 0)),
        "attribution_required": bool(lrow.get("attribution_required", 0)),
        "no_modify": bool(lrow.get("no_modify", 0)),
        "desc_full": desc_full,
        "desc_short": desc_short,
        "n_series": (roll or {}).get("n_series", 0),
        "cov_start": cov_start,
        "cov_end": cov_end,
        "frequencies": (roll or {}).get("frequencies", []),
        "categories": (roll or {}).get("categories", []),
        "n_geo": (roll or {}).get("n_geo", 0),
        "last_updated": sane_date((roll or {}).get("last_updated")),
        # operational (sidecar) extras -- display only
        "cadence": (side or {}).get("cadence"),
        "strategy": (side or {}).get("strategy"),
        "storage_layout": (side or {}).get("storage_layout"),
        "scripts": (side or {}).get("scripts") or [],
        "measured_obs": (side or {}).get("measured_obs"),
        "page_url": f"{SITE_BASE}/{sid}.html",
        "hf_url": f"{HF_ORG}-{sid}",
        "zenodo_url": ZENODO_COMMUNITY,
        # Task#5 series-tier metadata (producer-first citation + honest caveats).
        "description_key": s5.get("description_key") or [],
        "description_processing": s5.get("description_processing"),
        "citation_short": s5.get("citation_short"),
        "citation_long": s5.get("citation_long"),
    }
    return rec


# ---------------------------------------------------------------------------- #
#  schema.org/Dataset JSON-LD  (the Google-Dataset-Search payload)
# ---------------------------------------------------------------------------- #
def dataset_jsonld(rec):
    """Build a VALID schema.org/Dataset object. Required-by-Google fields:
    name, description; strongly recommended: license, creator/publisher,
    distribution, identifier, sameAs, temporalCoverage, isAccessibleForFree."""
    obj = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "name": rec["name"],
        # description is required; guarantee a non-empty string.
        "description": rec["desc_short"]
        or rec["attribution"]
        or f"{rec['name']} — dataset catalogued in the {SITE_NAME}.",
        "url": rec["page_url"],
        "identifier": rec["id"],
        "isAccessibleForFree": True,
        "publisher": PUBLISHER,
    }

    # creator = the originating provider (with homepage as sameAs when known).
    creator = {"@type": "Organization", "name": rec["name"]}
    if rec["homepage"]:
        creator["url"] = rec["homepage"]
    obj["creator"] = creator

    # license: prefer a resolvable URL; else the HUMAN label for our internal
    # status ids (audit-restricted / verified-*) so JSON-LD never leaks a bare
    # internal token; omit entirely for unverified so we never assert a fake license.
    if rec["license_url"]:
        obj["license"] = rec["license_url"]
    elif rec["license_id"] and rec["license_id"] != "NEEDS-REVIEW":
        obj["license"] = LICENSE_LABEL.get(rec["license_id"], rec["license_id"])

    # keywords from registry categories + provider id.
    kw = list(rec["categories"]) + [rec["id"]]
    if kw:
        obj["keywords"] = kw

    # temporalCoverage as an ISO-8601 interval, only when both ends are sane.
    if rec["cov_start"] and rec["cov_end"]:
        obj["temporalCoverage"] = f"{rec['cov_start']}/{rec['cov_end']}"
    elif rec["cov_start"]:
        obj["temporalCoverage"] = f"{rec['cov_start']}/.."

    if rec["last_updated"]:
        obj["dateModified"] = rec["last_updated"]

    if rec["frequencies"]:
        isos = [FREQ_ISO[f] for f in rec["frequencies"] if f in FREQ_ISO]
        if isos:
            obj["repeatFrequency"] = isos if len(isos) > 1 else isos[0]

    if rec["attribution_required"] and rec["attribution"]:
        obj["creditText"] = rec["attribution"]

    # producer-first citation (Task#5); schema.org Dataset.citation is free text.
    if rec.get("citation_long") or rec.get("citation_short"):
        obj["citation"] = rec.get("citation_long") or rec.get("citation_short")

    # sameAs spokes (our domain is canonical; HF + Zenodo are mirrors/DOIs).
    obj["sameAs"] = [rec["hf_url"], rec["zenodo_url"]]

    # distribution: ONLY for reservable sources do we advertise a download.
    # Non-reservable -> metadata-only catalog entry; we never imply re-serve.
    if rec["reservable"]:
        dist = {
            "@type": "DataDownload",
            "encodingFormat": "text/csv",
            "contentUrl": f"{SITE_BASE}/download.html?source={rec['id']}",
        }
        if rec["license_url"]:
            dist["license"] = rec["license_url"]
        obj["distribution"] = [dist]
    else:
        # Explicit, honest signal: catalogued metadata only, not redistributed.
        obj["isAccessibleForFree"] = False
        obj["usageInfo"] = (
            "Metadata-only catalog entry. This source's license does not permit "
            "redistribution, so the data itself is not served here; use the "
            "provider link to obtain the data under its terms."
        )

    return obj


# ---------------------------------------------------------------------------- #
#  Croissant JSON-LD  (ML-ready, schema.org + mlcommons context)
# ---------------------------------------------------------------------------- #
def croissant_jsonld(rec):
    """A minimal-but-valid Croissant record. Croissant is schema.org/Dataset
    plus the mlcommons:croissant context and (for reservable data) a parquet
    FileObject distribution. We keep it conservative: no fabricated RecordSet
    field types we can't verify -- just the dataset envelope + distribution."""
    obj = {
        "@context": {
            "@language": "en",
            "@vocab": "https://schema.org/",
            "cr": "http://mlcommons.org/croissant/",
            "data": {"@id": "cr:data", "@type": "@json"},
            "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
            "sc": "https://schema.org/",
            "conformsTo": "dct:conformsTo",
            "dct": "http://purl.org/dc/terms/",
        },
        "@type": "sc:Dataset",
        "conformsTo": "http://mlcommons.org/croissant/1.0",
        "name": re.sub(r"[^A-Za-z0-9_\-]+", "_", rec["id"]),
        "description": rec["desc_short"]
        or rec["attribution"]
        or f"{rec['name']} — catalogued in the {SITE_NAME}.",
        "url": rec["page_url"],
        "creator": {"@type": "sc:Organization", "name": rec["name"]},
        "publisher": {"@type": "sc:Organization", "name": SITE_NAME, "url": SITE_BASE},
    }
    if rec["license_url"]:
        obj["license"] = rec["license_url"]
    elif rec["license_id"] and rec["license_id"] != "NEEDS-REVIEW":
        obj["license"] = LICENSE_LABEL.get(rec["license_id"], rec["license_id"])
    if rec["categories"]:
        obj["keywords"] = list(rec["categories"])
    if rec["cov_start"] and rec["cov_end"]:
        obj["temporalCoverage"] = f"{rec['cov_start']}/{rec['cov_end']}"
    if rec.get("citation_long") or rec.get("citation_short"):
        obj["citation"] = rec.get("citation_long") or rec.get("citation_short")

    if rec["reservable"]:
        obj["distribution"] = [
            {
                "@type": "cr:FileObject",
                "@id": f"{rec['id']}-csv",
                "name": f"{rec['id']}-csv",
                "description": "Per-series CSV, downloadable with a free API key.",
                "contentUrl": f"{SITE_BASE}/download.html?source={rec['id']}",
                "encodingFormat": "text/csv",
            }
        ]
    else:
        # No distribution emitted for non-reservable; flag metadata-only.
        obj["isAccessibleForFree"] = False
        obj["usageInfo"] = (
            "Metadata-only Croissant record; underlying data is not redistributed "
            "under this source's license."
        )
    return obj


# ---------------------------------------------------------------------------- #
#  HTML rendering
# ---------------------------------------------------------------------------- #
PAGE_CSS = """
:root{--navy:#1a2332;--navy-light:#243044;--blue:#2563eb;--blue-pale:#eff6ff;
--gold:#d4a843;--gold-deep:#8a6d27;--g50:#f9fafb;--g100:#f3f4f6;--g200:#e5e7eb;
--g300:#d1d5db;--g500:#6b7280;--g600:#4b5563;--g700:#374151;--g800:#1f2937;
--green:#047857;--red:#b91c1c;--amber:#92600a;--serif:Georgia,serif;
--sans:"Inter",system-ui,sans-serif;--mono:"JetBrains Mono",monospace}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--sans);color:var(--g800);background:#fff;line-height:1.6}
.nav{background:var(--navy);color:#fff;position:sticky;top:0;z-index:50}
.nav-in{max-width:1200px;margin:0 auto;padding:0 1.5rem;height:60px;display:flex;
align-items:center;justify-content:space-between}
.brand{font-family:var(--serif);font-weight:700;font-size:1.2rem}
.brand .d{color:var(--gold)}.brand a{color:#fff;text-decoration:none}
.nav a{color:rgba(255,255,255,.8);text-decoration:none;font-size:.9rem;margin-left:1rem}
.wrap{max-width:920px;margin:0 auto;padding:2rem 1.5rem}
.crumb{font-size:.82rem;color:var(--g500);margin-bottom:1rem}
.crumb a{color:var(--blue);text-decoration:none}
h1{font-family:var(--serif);color:var(--navy);font-size:2rem;line-height:1.2}
.pid{font-family:var(--mono);color:var(--gold-deep);font-size:.85rem;margin-bottom:.3rem}
.badges{margin:.9rem 0 1.2rem;display:flex;gap:.5rem;flex-wrap:wrap}
.badge{display:inline-block;font-size:.74rem;font-weight:600;padding:.2rem .6rem;
border-radius:999px}
.badge.open{background:#ecfdf5;color:var(--green)}
.badge.meta{background:#fffbeb;color:var(--amber)}
.badge.lic{background:var(--blue-pale);color:var(--blue)}
.badge.cat{background:var(--g100);color:var(--g600)}
.lead{font-size:1.02rem;color:var(--g700);margin:.5rem 0 1.5rem;white-space:pre-wrap}
.callout{font-size:.9rem;border-radius:8px;padding:.8rem 1rem;margin:1rem 0}
.callout.meta{background:#fffbeb;border:1px solid #fde68a;color:#7c5e10}
.callout.open{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}
ul.notes{margin:.4rem 0 1rem;padding-left:1.1rem;color:var(--g700)}
ul.notes li{margin:.35rem 0;font-size:.92rem;line-height:1.5}
ul.notes li:first-child{font-weight:600;color:var(--navy)}
blockquote.cite{margin:.4rem 0;padding:.7rem 1rem;border-left:3px solid var(--gold);
background:var(--g50,#f9fafb);font-family:var(--mono);font-size:.85rem;color:var(--g700);white-space:pre-wrap}
p.proc{font-size:.85rem;color:var(--g600);margin:.4rem 0 1rem}
h2{font-family:var(--serif);color:var(--navy);font-size:1.2rem;margin:1.8rem 0 .5rem;
border-bottom:1px solid var(--g200);padding-bottom:.3rem}
.kv{display:grid;grid-template-columns:190px 1fr;gap:.45rem 1rem;font-size:.92rem}
.kv dt{color:var(--g500);font-weight:600}
.kv dd{color:var(--g800);word-break:break-word}
.kv dd a{color:var(--blue)}
.mono{font-family:var(--mono);font-size:.85rem}
details{margin-top:1rem;border:1px solid var(--g200);border-radius:8px;background:var(--g50)}
summary{cursor:pointer;padding:.7rem 1rem;font-weight:600;color:var(--g700);font-size:.9rem}
details pre{margin:0;padding:1rem;overflow:auto;font-family:var(--mono);font-size:.78rem;
line-height:1.5;border-top:1px solid var(--g200);background:#fff;color:var(--g800)}
.foot{color:var(--g500);font-size:.8rem;text-align:center;padding:2.5rem 1rem;
border-top:1px solid var(--g200);margin-top:2.5rem}
.foot a{color:var(--blue)}
.dhero{background:var(--navy);margin:-2rem -1.5rem 1.6rem;padding:2.1rem 1.5rem 1.7rem;
border-bottom:3px solid var(--gold)}
.dhero .crumb{color:rgba(255,255,255,.55);margin-bottom:.7rem}
.dhero .crumb a{color:var(--gold);text-decoration:none}
.dhero .pid{color:var(--gold);margin-bottom:.35rem}
.dhero h1{color:#fff;font-size:1.9rem}
.dhero .badges{margin:.9rem 0 0}
.dhero .lead{color:rgba(255,255,255,.78);margin:.8rem 0 0;font-size:1rem}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.7rem;margin:.5rem 0 1rem}
.metric{background:var(--g50);border:1px solid var(--g200);border-radius:10px;padding:.75rem .9rem}
.metric .mlabel{font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--g500)}
.metric .mval{font-family:var(--serif);font-size:1.15rem;color:var(--navy);margin-top:.25rem;word-break:break-word}
"""

HEAD = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<meta name="description" content="{meta_desc}">
<link rel="canonical" href="{canonical}">
<link rel="icon" type="image/svg+xml" href="assets/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>{css}
/* status bar + nav (hfdatalibrary.com parity) */
.status-bar{{background:var(--g50);border-bottom:1px solid var(--g200);font-size:.8rem;
color:var(--g500);min-height:32px;line-height:32px;padding:0 1.5rem}}
.status-bar .sb-in{{max-width:1200px;margin:0 auto;display:flex;justify-content:space-between;
align-items:center;flex-wrap:wrap;gap:.25rem}}
.nav .signin{{background:var(--gold);color:var(--navy)!important;padding:.4rem .875rem;
border-radius:6px;font-size:.85rem;font-weight:600;white-space:nowrap;margin-left:1rem}}
.nav .brand{{display:inline-flex;align-items:center;gap:.55rem}}
.nav .fam-tag{{font-size:.62rem;color:var(--gold)!important;border:1px solid rgba(212,168,67,.5);
border-radius:999px;padding:.12rem .5rem;white-space:nowrap;font-family:var(--sans);font-weight:600;
letter-spacing:.01em;text-decoration:none;line-height:1.4}}
.nav .fam-tag:hover{{background:rgba(212,168,67,.15)}}
@media (max-width:680px){{.nav .fam-tag{{display:none}}}}
</style>
{jsonld}
<script src="assets/sso.js?v=20260715a"></script>
</head><body>
<div class="status-bar" id="status-bar"><div class="sb-in">
<span><span id="sb-dot" style="color:#9ca3af;font-size:.7rem">&#9679;</span> <span id="sb-text">Checking status&hellip;</span></span>
<span style="display:flex;gap:1.5rem;white-space:nowrap"><span id="sb-site"></span><span id="sb-data"></span></span>
</div></div>
<div class="nav"><div class="nav-in"><div class="brand"><a href="index.html">Econ Data <span class="d">Library</span></a><a href="https://elkassabgidata.com" class="fam-tag" title="Part of the ElkassabgiData family — one account, every library">part of ElkassabgiData</a></div>
<div class="nav-links"><a href="index.html">Catalog</a><a href="docs.html">Documentation</a><a href="api.html">API</a><a href="download.html">Download</a><a href="mcp.html">AI Tools</a><a href="cite.html">Cite</a><a href="stats.html">Stats</a><a href="status.html">Status</a><a href="contact.html">Contact</a><a href="account.html" class="signin">Sign in</a></div></div></div>
<script>
(function(){{
  var API="https://econdl-api.elkassabgi.workers.dev";
  function fdate(s){{try{{
    var m=/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})/.exec(s);
    var d=m?new Date(+m[1],+m[2]-1,+m[3]):new Date(s); /* date-only: local, no UTC shift */
    if(isNaN(d))return s;
    return d.toLocaleDateString('en-US',{{year:'numeric',month:'long',day:'numeric'}});}}catch(e){{return s;}}}}
  fetch(API+"/v1/stats?t="+Date.now()).then(function(r){{if(!r.ok)throw 0;return r.json();}}).then(function(d){{
    document.getElementById('sb-dot').style.color='#059669';
    document.getElementById('sb-text').textContent='All systems operational';
    document.getElementById('sb-site').textContent='Website updated: '+fdate('__SITE_UPDATED__');
    if(d.as_of)document.getElementById('sb-data').textContent='Data measured: '+fdate(d.as_of);
  }}).catch(function(){{
    document.getElementById('sb-dot').style.color='#d97706';
    document.getElementById('sb-text').textContent='Status check unavailable';
  }});
}})();
</script>
"""


# The ElkassabgiData family plate — appended to the VERY BOTTOM of every page by
# _write() (below the page's own footer). Ahmed 2026-07-14: logo plate on all
# pages, linked to the family portal. Asset path is site-root-relative (every
# generated page lives at the site root).
FAMILY_BAND = """
<div style="background:#141c2e;border-top:1px solid rgba(212,168,67,.28);text-align:center;padding:2.5rem 1.5rem">
  <a href="https://elkassabgidata.com" title="ElkassabgiData — one account, every library">
    <img src="assets/elkassabgidata-logo.svg" alt="ElkassabgiData" width="300" height="80" style="max-width:78%;height:auto">
  </a>
  <p style="color:rgba(255,255,255,.82);font-family:Georgia,serif;font-size:1.05rem;margin:1.1rem 0 .35rem">One account. Every library.</p>
  <p style="color:rgba(255,255,255,.5);font-size:.85rem;margin:0">
    <a href="https://hfdatalibrary.com/" style="color:#d4a843;text-decoration:none">HF Data Library</a>
    &nbsp;&middot;&nbsp;
    <a href="https://econdatalibrary.com/" style="color:#d4a843;text-decoration:none">Econ Data Library</a>
    &nbsp;&middot;&nbsp;<span style="color:rgba(255,255,255,.4)">more to come</span>
  </p>
</div>
"""


def jsonld_script(obj):
    payload = json.dumps(obj, ensure_ascii=False, indent=2).replace("</", "<\\/")
    return f'<script type="application/ld+json">\n{payload}\n</script>'


# ---------------------------------------------------------------------------- #
#  Per-source embeds granted by the provider in writing. NEVER add one without a
#  documented permission trail. Each entry: heading, permission note (shown on
#  the page), and the provider-supplied embed HTML (cleaned of mail-relay link
#  mangling; functionally identical to what the provider sent).
# ---------------------------------------------------------------------------- #
SOURCE_EMBEDS = {
    # Social Progress Imperative — written permission from REDACTED
    # (REDACTED, 2026-07-14, "Access for Econ Data Library"):
    # embed of the PUBLIC Tableau of the 2026 Global Social Progress Index,
    # student/academic use only, no charge. The DATASET itself is explicitly NOT
    # licensed for free redistribution -> this source stays metadata-only.
    # (Only change vs the provider's code: UI language es-ES -> en-US.)
    "social_progress": {
        "heading": "Explore the 2026 Global Social Progress Index",
        "note": ("Embedded with written permission from the Social Progress "
                 "Imperative (2026) for student and academic use, free of charge. "
                 "The underlying dataset is not redistributed here — data licensing "
                 "and premium access are available from "
                 '<a href="https://www.socialprogress.org/">socialprogress.org</a>.'),
        "html": """
<div class='tableauPlaceholder' id='viz1784056164874' style='position: relative'><noscript><a href='https://www.socialprogress.org/'><img alt='2026 Global Social Progress Index' src='https://public.tableau.com/static/images/20/2026GlobalSocialProgressIndexPublicAccess/2026SPI/1_rss.png' style='border: none' /></a></noscript><object class='tableauViz' style='display:none;'><param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' /> <param name='embed_code_version' value='3' /> <param name='site_root' value='' /><param name='name' value='2026GlobalSocialProgressIndexPublicAccess&#47;2026SPI' /><param name='tabs' value='yes' /><param name='toolbar' value='yes' /><param name='static_image' value='https://public.tableau.com/static/images/20/2026GlobalSocialProgressIndexPublicAccess/2026SPI/1.png' /> <param name='animate_transition' value='yes' /><param name='display_static_image' value='yes' /><param name='display_spinner' value='yes' /><param name='display_overlay' value='yes' /><param name='display_count' value='yes' /><param name='language' value='en-US' /></object></div>
<script type='text/javascript'>
var divElement = document.getElementById('viz1784056164874');
var vizElement = divElement.getElementsByTagName('object')[0];
if ( divElement.offsetWidth > 800 ) { vizElement.style.width='1000px';vizElement.style.height='1250px';} else if ( divElement.offsetWidth > 500 ) { vizElement.style.width='1000px';vizElement.style.height='1250px';} else { vizElement.style.width='100%';vizElement.style.height='7250px';}
var scriptElement = document.createElement('script');
scriptElement.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
vizElement.parentNode.insertBefore(scriptElement, vizElement);
</script>
""",
    },
}


def render_dataset_page(rec):
    # Honesty transform for GATED sources: the baked metadata sentence
    # "Compiled and redistributed by the Elkassabgi Data Library." is true only
    # for reservable sources. On a gated page it would misstate what we do.
    if not rec["reservable"]:
        _honest = ("Catalogued (metadata only) by the Elkassabgi Data Library; "
                   "the data itself is not redistributed here.")
        for _k in ("desc_short", "desc_full", "description_processing"):
            if rec.get(_k):
                rec[_k] = rec[_k].replace(
                    "Compiled and redistributed by the Elkassabgi Data Library.", _honest)
    ds_ld = dataset_jsonld(rec)
    cr_ld = croissant_jsonld(rec)
    jsonld_block = jsonld_script(ds_ld) + "\n" + jsonld_script(cr_ld)

    meta_desc = rec["desc_short"] or rec["attribution"] or rec["name"]
    head = HEAD.format(
        title=f"{esc(rec['name'])} — {SITE_NAME}",
        meta_desc=esc(meta_desc)[:300],
        canonical=esc(rec["page_url"]),
        css=PAGE_CSS,
        jsonld=jsonld_block,
    )

    badges = []
    if rec["reservable"]:
        badges.append('<span class="badge open">Open &middot; redistributed</span>')
    else:
        badges.append('<span class="badge meta">Metadata only</span>')
    badges.append(f'<span class="badge lic">{esc(rec["license_label"])}</span>')
    for c in rec["categories"][:6]:
        badges.append(f'<span class="badge cat">{esc(c)}</span>')

    if rec["reservable"]:
        callout = (
            '<div class="callout open"><b>Redistributable.</b> This source is served '
            "as canonical Parquet under the license below, with attribution and "
            "provenance preserved.</div>"
        )
    else:
        callout = (
            '<div class="callout meta"><b>Metadata only.</b> This source’s license '
            "does not permit redistribution, so we catalog its metadata but do not "
            "re-serve the data. Use the provider link to obtain it under the original "
            "terms.</div>"
        )

    # Coverage section
    cov_rows = []
    if rec["n_series"]:
        cov_rows.append(("Series catalogued", f"{rec['n_series']:,}"))
    if rec["cov_start"] or rec["cov_end"]:
        span = f"{rec['cov_start'] or '?'} – {rec['cov_end'] or 'present'}"
        cov_rows.append(("Temporal coverage", span))
    if rec["frequencies"]:
        cov_rows.append(
            ("Frequencies", ", ".join(FREQ_LABEL.get(f, f) for f in rec["frequencies"]))
        )
    if rec["n_geo"]:
        cov_rows.append(("Distinct geographies", f"{rec['n_geo']:,}"))
    if rec["categories"]:
        cov_rows.append(("Categories", ", ".join(rec["categories"])))
    if rec["measured_obs"]:
        cov_rows.append(("Measured observations", f"{rec['measured_obs']:,}"))
    if rec["last_updated"]:
        cov_rows.append(("Registry last updated", rec["last_updated"]))

    # Licensing / provenance section
    lic_rows = [("License", esc(rec["license_label"]))]
    if rec["license_url"]:
        lic_rows.append(
            ("License URL", f'<a href="{esc(rec["license_url"])}">{esc(rec["license_url"])}</a>')
        )
    if rec["attribution"]:
        lic_rows.append(("Required attribution", esc(rec["attribution"])))
    lic_rows.append(("Redistribution", "Permitted (served here)" if rec["reservable"] else "Not permitted (metadata only)"))
    lic_rows.append(("Commercial use", "Yes" if rec["commercial_ok"] else "Restricted / no"))
    lic_rows.append(("Modification", "Restricted" if rec["no_modify"] else "Permitted"))
    if rec["homepage"]:
        lic_rows.append(("Provider homepage", f'<a href="{esc(rec["homepage"])}">{esc(rec["homepage"])}</a>'))
    if rec["terms_url"]:
        lic_rows.append(("Provider terms", f'<a href="{esc(rec["terms_url"])}">{esc(rec["terms_url"])}</a>'))

    # Access / mirrors. Download + API rows ONLY for reservable sources — a gated
    # source's page must never advertise a download of data we don't redistribute
    # (the API 451s it anyway; the page must say the same thing).
    if rec["reservable"]:
        acc_rows = [
            ("Download", f'<a href="download.html?source={esc(rec["id"])}">Select &amp; download {esc(rec["id"])} series as CSV &rarr;</a>'),
            ("API", f'<a href="account.html">Get a free API key</a>, then <span class="mono">GET /v1/series/&lt;id&gt;.csv</span>'),
            ("Canonical landing", f'<a href="{esc(rec["page_url"])}">{esc(rec["page_url"])}</a>'),
        ]
    else:
        provider_link = rec["homepage"] or rec["terms_url"] or ""
        acc_rows = [
            ("Download", "Not available here — this provider's terms do not permit redistribution."
             + (f' Obtain the data from the <a href="{esc(provider_link)}">original provider</a>.' if provider_link else " Obtain the data from the original provider.")),
            ("Canonical landing", f'<a href="{esc(rec["page_url"])}">{esc(rec["page_url"])}</a>'),
        ]
    if rec["cadence"]:
        acc_rows.append(("Update cadence", esc(rec["cadence"])))
    if rec["strategy"]:
        acc_rows.append(("Update strategy", esc(rec["strategy"]).replace("_", " ")))
    if rec["storage_layout"]:
        acc_rows.append(("Storage layout", esc(rec["storage_layout"])))

    def kv(rows):
        return "\n".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in rows)

    body = [head]
    body.append('<div class="wrap">')
    body.append('<div class="dhero">')
    body.append(f'<div class="crumb"><a href="index.html">Catalog</a> / {esc(rec["name"])}</div>')
    body.append(f'<div class="pid">{esc(rec["id"])}</div>')
    body.append(f"<h1>{esc(rec['name'])}</h1>")
    body.append(f'<div class="badges">{"".join(badges)}</div>')
    if rec["desc_short"]:
        body.append(f'<p class="lead">{esc(rec["desc_short"])}</p>')
    body.append('</div>')  # /dhero
    body.append(callout)

    # Provider-granted embed (see SOURCE_EMBEDS — written permission required).
    emb = SOURCE_EMBEDS.get(rec["id"])
    if emb:
        body.append(f"<h2>{emb['heading']}</h2>")
        body.append(f'<div class="callout open" style="margin-bottom:1rem">{emb["note"]}</div>')
        body.append(emb["html"])

    # Important notes (Task#5 caveats) — for hf_equities the survivorship-bias
    # disclosure is the first bullet; never fabricated, shown only when present.
    if rec["description_key"]:
        notes = "".join(f"<li>{esc(b)}</li>" for b in rec["description_key"])
        body.append("<h2>Important notes</h2>")
        body.append(f'<ul class="notes">{notes}</ul>')

    if cov_rows:
        body.append("<h2>Coverage</h2>")
        cards = "".join(
            f'<div class="metric"><div class="mlabel">{esc(k)}</div>'
            f'<div class="mval">{v}</div></div>' for k, v in cov_rows)
        body.append(f'<div class="metrics">{cards}</div>')

    body.append("<h2>Licensing &amp; provenance</h2>")
    body.append(f'<dl class="kv">{kv(lic_rows)}</dl>')

    # How to cite — producer FIRST, library second (Task#5).
    if rec["citation_long"] or rec["citation_short"]:
        cite = rec["citation_long"] or rec["citation_short"]
        body.append("<h2>How to cite</h2>")
        body.append(f'<blockquote class="cite">{esc(cite)}</blockquote>')
        if rec["description_processing"]:
            body.append(f'<p class="proc"><b>Processing:</b> {esc(rec["description_processing"])}</p>')

    body.append("<h2>Access &amp; mirrors</h2>")
    body.append(f'<dl class="kv">{kv(acc_rows)}</dl>')

    if rec["desc_full"] and rec["desc_full"] != rec["desc_short"]:
        body.append("<h2>Full description</h2>")
        body.append(f'<p class="lead">{esc(rec["desc_full"])}</p>')

    # Show the embedded structured data so it is inspectable on the page too.
    body.append("<h2>Structured metadata</h2>")
    body.append(
        '<details><summary>schema.org/Dataset (Google Dataset Search)</summary>'
        f'<pre>{esc(json.dumps(ds_ld, ensure_ascii=False, indent=2))}</pre></details>'
    )
    body.append(
        '<details><summary>Croissant (ML-ready, schema.org JSON-LD)</summary>'
        f'<pre>{esc(json.dumps(cr_ld, ensure_ascii=False, indent=2))}</pre></details>'
    )

    body.append(
        f'<div class="foot">Part of the {SITE_NAME} catalog &middot; '
        f'metadata generated {TODAY} from the central registry &middot; '
        '<a href="index.html">browse all datasets</a></div>'
    )
    body.append("</div></body></html>")
    html = "\n".join(body)
    if not rec["reservable"]:
        # Page-level honesty sweep for gated sources: baked per-series sample
        # descriptions also carry the "Compiled and redistributed" sentence.
        html = html.replace(
            "Compiled and redistributed by the Elkassabgi Data Library.",
            "Catalogued (metadata only) by the Elkassabgi Data Library; the data itself is not redistributed here.")
    return html


def _earliest_data_year():
    """Measured 'years of history' for the hero stat (never hardcoded): the
    earliest series start_date in catalog.db. Real: the Maddison Project / GGDC
    historical GDP series genuinely begin in year 1 CE. Floored to a century so
    the claim always understates ('2,000+'). Returns None if unmeasurable."""
    try:
        db = sqlite3.connect(os.path.join(HERE, "..", "data", "catalog.db"))
        row = db.execute(
            "SELECT MIN(start_date) FROM series "
            "WHERE start_date IS NOT NULL AND start_date != '' AND start_date >= '0001'"
        ).fetchone()
        db.close()
        if not row or not row[0]:
            return None
        first_year = int(str(row[0])[:4])
        span = date.today().year - first_year
        return (span // 100) * 100  # floor to century: 2025 -> 2000
    except Exception:
        return None


def render_index(records, generated):
    # Lightweight client-side search over an embedded JSON index.
    idx = [
        {
            "id": r["id"],
            "name": r["name"],
            # No blurb on catalog cards: the operational `description` is an
            # internal ingest note (URLs, "grouped ingest", "License:/Source:")
            # with no clean human lead for most sources. The card is already
            # informative from name + license + category badges + series count;
            # the full description stays on each dataset page.
            "desc": "",
            "license": r["license_label"],
            "reservable": r["reservable"],
            "cats": r["categories"],
            "n_series": r["n_series"],
            "page": f"{r['id']}.html",
        }
        for r in records
    ]
    data = json.dumps(idx, ensure_ascii=False).replace("</", "<\\/")
    n_total = len(records)
    n_open = sum(1 for r in records if r["reservable"])
    n_meta = n_total - n_open
    n_series_total = sum(r.get("n_series", 0) or 0 for r in records)
    n_active = sum(1 for r in records if (r.get("n_series", 0) or 0) > 0)

    # Index JSON-LD: DataCatalog + Organization + WebSite + FAQPage (mirrors the
    # hfdatalibrary.com landing page's structured-data graph).
    faq_pairs = [
        ("Is the data really free?",
         "Yes. Browsing, metadata, and the catalog need no account. Data downloads "
         "use a free API key — no subscription, no paywall. One free ElkassabgiData "
         "account works across the whole family, including hfdatalibrary.com."),
        ("What does the library cover?",
         f"{n_total} economic and financial data sources — national statistics, "
         "central banks, international organizations, trade, development, energy, "
         "and research datasets — indexed in one namespace with billions of "
         "individual series (live counts are measured on the data store and shown "
         "on this page)."),
        ("How are licenses handled?",
         "Every series carries its source's license and attribution requirements. "
         "Sources whose license forbids re-hosting are catalogued as metadata-only "
         "pointers to the original publisher — they are never redistributed."),
        ("How do I cite a series?",
         "Every series and every bundle ships a producer-first citation (the "
         "original statistical agency first, the library second). Download bundles "
         "are snapshot-pinned so a citation reproduces the exact data."),
        ("Is there an API?",
         "Yes — a free REST API for search, metadata, series CSV, and reproducible "
         "bundles, plus an MCP server that lets AI assistants query the library "
         "directly with licenses and citations attached."),
        ("Which languages are supported?",
         "Six (English, Arabic, Spanish, French, Russian, Chinese) using only the "
         "sources' official translations — titles are never machine-translated."),
    ]
    catalog_ld = {
        "@context": "https://schema.org/",
        "@graph": [
            {
                "@type": "DataCatalog",
                "@id": f"{SITE_BASE}/#catalog",
                "name": SITE_NAME,
                "url": f"{SITE_BASE}/index.html",
                "description": (
                    f"Searchable catalog of {n_total} economic and financial data sources "
                    "with license, provenance, and machine-readable Dataset + Croissant metadata."
                ),
                "publisher": PUBLISHER,
            },
            {
                "@type": "Organization",
                "@id": f"{SITE_BASE}/#org",
                "name": SITE_NAME,
                "url": f"{SITE_BASE}/",
                "founder": {
                    "@type": "Person",
                    "name": "Ahmed Elkassabgi",
                    "identifier": "https://orcid.org/0000-0002-5926-7493",
                    "affiliation": {"@type": "Organization",
                                    "name": "University of Central Arkansas"},
                },
                "sameAs": ["https://hfdatalibrary.com/",
                           "https://orcid.org/0000-0002-5926-7493"],
            },
            {
                "@type": "WebSite",
                "@id": f"{SITE_BASE}/#website",
                "name": SITE_NAME,
                "url": f"{SITE_BASE}/",
                "inLanguage": "en",
                "publisher": {"@id": f"{SITE_BASE}/#org"},
            },
            {
                "@type": "FAQPage",
                "@id": f"{SITE_BASE}/#faq",
                "mainEntity": [
                    {"@type": "Question", "name": q,
                     "acceptedAnswer": {"@type": "Answer", "text": a}}
                    for q, a in faq_pairs
                ],
            },
        ],
    }

    head = HEAD.format(
        title=f"Data Catalog — {SITE_NAME}",
        meta_desc=f"Searchable catalog of {n_total} economic & financial data sources with license, provenance, and schema.org Dataset metadata.",
        canonical=f"{SITE_BASE}/index.html",
        css=PAGE_CSS
        + """
/* ── HF-landing replica (mirrors hfdatalibrary.com css/style.css) ── */
.container{max-width:1200px;margin:0 auto;padding:0 1.5rem}
.container-narrow{max-width:920px;margin:0 auto;padding:0 1.5rem}
.section{padding:5rem 0}
.section-alt{background:var(--g50)}
.hero{background:linear-gradient(135deg,var(--navy) 0%,var(--navy-light) 100%);
color:#fff;padding:6rem 0 5rem;text-align:center;border-bottom:3px solid var(--gold)}
.hero h1{font-family:var(--serif);color:#fff;font-size:3rem;line-height:1.2;margin-bottom:.5rem}
.hero h1 span{color:var(--gold)}
.hero .subtitle{font-size:1.25rem;color:rgba(255,255,255,.75);max-width:720px;margin:0 auto 2.5rem;font-weight:400}
.stats-bar{display:grid;grid-template-columns:repeat(4,1fr);gap:1.5rem;max-width:900px;margin:0 auto 3rem}
.stat-item{text-align:center}
.stat-item.hl{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);
border-radius:12px;padding:1.25rem 1rem;box-shadow:0 4px 20px rgba(0,0,0,.3);margin-top:-.75rem}
.stat-number{font-family:var(--mono);font-size:2rem;font-weight:700;color:var(--gold);display:block}
.stat-label{font-size:.85rem;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.05em}
.btn{display:inline-flex;align-items:center;gap:.5rem;padding:.75rem 1.75rem;border-radius:8px;
font-weight:600;font-size:.95rem;cursor:pointer;border:none;transition:all .2s;text-decoration:none}
.btn-primary{background:var(--blue);color:#fff}
.btn-primary:hover{background:#3b82f6;color:#fff;transform:translateY(-1px);box-shadow:0 4px 6px rgba(0,0,0,.07)}
.btn-outline{background:transparent;color:#fff;border:2px solid rgba(255,255,255,.3)}
.btn-outline:hover{border-color:#fff;color:#fff}
.btn-group{display:flex;gap:1rem;justify-content:center;flex-wrap:wrap}
.feature-row{display:grid;grid-template-columns:1fr 1fr;gap:4rem;align-items:center}
.feature-text h2{font-family:var(--serif);color:var(--navy);font-size:1.875rem;margin:0 0 .75rem;border:none;padding:0}
.feature-text p{color:var(--g600);margin-bottom:1rem}
.feature-visual{background:var(--g50);border:1px solid var(--g200);border-radius:12px;padding:2rem}
.feature-visual pre{background:var(--navy);color:#e2e8f0;padding:1.25rem 1.5rem;border-radius:8px;
overflow-x:auto;font-size:.82rem;line-height:1.6;margin:0;font-family:var(--mono)}
.grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:1.5rem}
.acard{background:#fff;border:1px solid var(--g200);border-radius:12px;padding:2rem;transition:all .2s}
.acard:hover{box-shadow:0 4px 6px rgba(0,0,0,.07);border-color:var(--g300)}
.acard .card-icon{width:48px;height:48px;background:var(--blue-pale);border-radius:10px;
display:flex;align-items:center;justify-content:center;font-size:1.5rem;margin-bottom:1rem}
.acard h3{font-family:var(--sans);color:var(--navy);font-size:1.125rem;margin-bottom:.75rem}
.acard p{color:var(--g600);font-size:.95rem;margin-bottom:0}
.section-title{font-family:var(--serif);color:var(--navy);font-size:1.875rem;text-align:center;
margin:0 0 2.5rem;border:none;padding:0}
.table-wrap{overflow-x:auto;margin-bottom:1.5rem}
.cmp{width:100%;border-collapse:collapse;font-size:.9rem}
.cmp thead th{text-align:left;padding:.75rem 1rem;border-bottom:2px solid var(--g300);
font-weight:600;color:var(--g700);white-space:nowrap}
.cmp tbody td{padding:.625rem 1rem;border-bottom:1px solid var(--g200)}
.cmp tbody tr:hover{background:var(--g50)}
.comparison-highlight{background:var(--blue-pale)!important}
.comparison-check{color:var(--green);font-weight:700}
.comparison-x{color:var(--g300)}
.faq-item h3{font-family:var(--serif);color:var(--navy);font-size:1.1rem;margin-bottom:.35rem}
.faq-item p{color:var(--g700);font-size:.95rem}
.faq-item{margin-bottom:1.5rem}
.footer{background:var(--navy);color:rgba(255,255,255,.7);padding:3rem 0 2rem;font-size:.9rem;margin-top:0}
.footer-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:2rem;margin-bottom:2rem}
.footer h4{color:#fff;font-family:var(--sans);font-size:1rem;margin-bottom:.75rem}
.footer a{color:rgba(255,255,255,.7);text-decoration:none}
.footer a:hover{color:#fff}
.footer ul{list-style:none}
.footer li{margin-bottom:.4rem}
.footer-bottom{border-top:1px solid rgba(255,255,255,.1);padding-top:1.5rem;
display:flex;justify-content:space-between;align-items:center}
.footer-bottom .orcid{font-family:var(--mono);font-size:.8rem}
/* catalog search section (existing machinery) */
.controls{display:flex;gap:.75rem;flex-wrap:wrap;margin:0 0 1.2rem}
.controls input,.controls select{padding:.7rem .9rem;border:1px solid var(--g300);
border-radius:10px;font-size:.95rem;font-family:var(--sans);background:#fff}
.controls input{flex:1;min-width:240px}
.controls input:focus,.controls select:focus{outline:none;border-color:var(--blue);
box-shadow:0 0 0 3px var(--blue-pale)}
.card{display:block;border:1px solid var(--g200);border-radius:12px;padding:1.1rem 1.2rem;
margin-bottom:.75rem;text-decoration:none;color:inherit;background:#fff;
transition:box-shadow .14s,border-color .14s,transform .14s}
.card:hover{box-shadow:0 6px 22px rgba(26,35,50,.10);border-color:var(--gold);transform:translateY(-1px)}
.card .cid{font-family:var(--mono);font-size:.76rem;color:var(--gold-deep)}
.card h3{font-family:var(--serif);color:var(--navy);font-size:1.16rem;margin:.15rem 0 .3rem}
.card p{font-size:.88rem;color:var(--g600);margin:.2rem 0 .6rem;line-height:1.5;
display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .row{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center}
.count{color:var(--g500);font-size:.82rem;margin-left:auto;font-family:var(--mono)}
[dir=rtl] .count{margin-left:0;margin-right:auto}
[dir=rtl] .card .row{flex-direction:row-reverse}
[dir=rtl] .hero,[dir=rtl] .card{text-align:right}
[dir=rtl] .card h3{font-family:var(--sans)}
@media (max-width:768px){
.hero h1{font-size:2rem}
.stats-bar{grid-template-columns:repeat(2,1fr);gap:1rem}
.stat-number{font-size:1.5rem}
.grid-3{grid-template-columns:1fr}
.feature-row{grid-template-columns:1fr;gap:2rem}
.footer-grid{grid-template-columns:1fr 1fr}
.footer-bottom{flex-direction:column;gap:.5rem;text-align:center}
}
""",
        jsonld=jsonld_script(catalog_ld),
    )

    tpl = (
        head
        + """
<!-- ── Hero (hfdatalibrary.com landing structure) ── -->
<div role="status" style="background:linear-gradient(90deg,#d4a843,#e8c368);color:#14203a;text-align:center;padding:1rem 1.2rem;font-size:1.08rem;font-weight:600;line-height:1.5;border-bottom:4px solid #14203a;letter-spacing:.01em">&#128679; <strong>Under Construction</strong> &mdash; the Econ Data Library is being finalized. Datasets and their licensing are still being verified and may change.</div>
<section class="hero">
  <div class="container">
    <h1 style="font-size:2.6rem">Econ Data Library: Free Economic &amp; Financial <span>Time&#8209;Series</span> Data</h1>
    <p class="subtitle">Free, research-grade macro &amp; financial data — one namespace over the world's statistical sources. Every series carries its license, provenance, and producer-first citation. Reproducible, snapshot-pinned, and <strong>continuously updated</strong>.</p>

    <div class="stats-bar">
      <div class="stat-item">
        <span class="stat-number" id="live-series">&mdash;</span>
        <span class="stat-label">Individual Series</span>
      </div>
      <div class="stat-item hl">
        <span class="stat-number" id="obs-counter" style="font-size:1.55rem">&mdash;</span>
        <span class="stat-label">Observations</span>
      </div>
      <div class="stat-item">
        <span class="stat-number">__YEARS__</span>
        <span class="stat-label">Years of History</span>
      </div>
      <div class="stat-item">
        <span class="stat-number">__N__</span>
        <span class="stat-label">Sources</span>
      </div>
    </div>

    <div class="btn-group">
      <a href="download.html" class="btn btn-primary">Download Data</a>
      <a href="docs.html" class="btn btn-outline">Read the Docs</a>
      <a href="api.html" class="btn btn-outline">API Access</a>
    </div>
    <p style="font-size:.78rem;color:rgba(255,255,255,.5);margin-top:2rem">Series and observation counts are measured on our data store (as of <span id="live-asof">&mdash;</span>) — never estimated, never hardcoded. Years of history: the earliest catalogued series (Maddison Project / GGDC) begin in year 1&nbsp;CE.</p>
  </div>
</section>

<!-- ── What This Is ── -->
<section class="section">
  <div class="container">
    <div class="feature-row">
      <div class="feature-text">
        <h2>What is this?</h2>
        <p>A single, citable library over __N__ economic and financial data sources — national statistical offices, central banks, international organizations, trade, development, energy, and research datasets.</p>
        <p>Every series lives in one namespace (<code>source:series:geography</code>), resolves over a free REST API, and ships with its license, attribution requirements, and a producer-first citation. Bundles are snapshot-pinned so your results reproduce exactly.</p>
        <p>Sources whose licenses forbid re-hosting are catalogued honestly as metadata-only pointers to the original publisher — never silently redistributed.</p>
        <p>No subscription. No paywall. One free key for the whole ElkassabgiData family, including <a href="https://hfdatalibrary.com/">HF Data Library</a>.</p>
      </div>
      <div class="feature-visual">
        <pre><code># Python — any series in a few lines
import io, requests, pandas as pd

API = "https://econdl-api.elkassabgi.workers.dev"
r = requests.get(
    f"{API}/v1/series/worldbank:NY.GDP.MKTP.CD:USA.csv",
    headers={"X-API-Key": "YOUR_FREE_KEY"})
df = pd.read_csv(io.StringIO(r.text), comment="#")

# -> tidy date,value rows with the license and
#    producer-first citation in the CSV header</code></pre>
      </div>
    </div>
  </div>
</section>

<!-- ── Two access tiers (hf 'Two cleaning versions' parallel) ── -->
<section class="section section-alt">
  <div class="container">
    <h2 class="section-title">Two catalog tiers. Always honest.</h2>
    <div class="grid-2" style="max-width:900px;margin:0 auto;display:grid;grid-template-columns:repeat(2,1fr);gap:1.5rem">
      <div class="acard" style="border-left:4px solid var(--green)">
        <span class="badge open" style="margin-bottom:.5rem;display:inline-block">Redistributed</span>
        <h3>Tier 1: Redistributed</h3>
        <p>__NOPEN__ sources whose licenses permit re-hosting. Full data served from our store — CSV downloads, API access, snapshot-pinned bundles. License and attribution attached to every series.</p>
        <p style="margin-top:.5rem"><strong>Best for:</strong> direct downloads, reproducible research bundles, API pipelines.</p>
      </div>
      <div class="acard" style="border-left:4px solid var(--amber)">
        <span class="badge meta" style="margin-bottom:.5rem;display:inline-block">Metadata only</span>
        <h3>Tier 2: Metadata-only</h3>
        <p>__NMETA__ sources whose licenses forbid re-hosting. Fully catalogued — searchable metadata, machine-readable Dataset/Croissant records, and pointers to the original publisher. The data itself stays with its owner.</p>
        <p style="margin-top:.5rem"><strong>Best for:</strong> discovery, license checking, citing the original source correctly.</p>
      </div>
    </div>
    <p style="text-align:center;margin-top:2rem;color:var(--g500);font-size:.9rem;max-width:700px;margin-left:auto;margin-right:auto">We never silently redistribute restricted data — a direct request for a restricted series returns an honest HTTP 451 with a link to the publisher.</p>
  </div>
</section>

<!-- ── Coverage (hf '25 academic variables' parallel) ── -->
<section class="section">
  <div class="container">
    <h2 class="section-title" style="margin-bottom:.5rem">What the library covers</h2>
    <p style="text-align:center;color:var(--g500);margin-bottom:2.5rem">__N__ sources across the pillars of empirical economics and finance.</p>
    <div class="grid-3">
      <div class="acard"><div class="card-icon">&#128200;</div><h3>Macro &amp; National Accounts</h3><p>GDP, employment, production — national statistical offices (ABS, INSEE, ISTAT, StatCan, Eurostat) and the IMF/World Bank.</p></div>
      <div class="acard"><div class="card-icon">&#128176;</div><h3>Prices, Money &amp; Central Banks</h3><p>Inflation, interest rates, FX — ECB, Fed Board, BIS, Bundesbank, and dozens of national central banks.</p></div>
      <div class="acard"><div class="card-icon">&#128674;</div><h3>Trade &amp; Development</h3><p>Bilateral trade (CEPII BACI), tariffs, development indicators (World Bank WDI, UN SDG, UNDP HDR).</p></div>
      <div class="acard"><div class="card-icon">&#9889;</div><h3>Energy &amp; Environment</h3><p>EIA, IRENA, Ember, Global Carbon Budget, NASA GISS — production, prices, emissions, climate.</p></div>
      <div class="acard"><div class="card-icon">&#127963;</div><h3>Institutions &amp; Society</h3><p>Governance (WGI, V-Dem, Freedom House), conflict (UCDP, COW), inequality (WID, SWIID), well-being (WHR).</p></div>
      <div class="acard"><div class="card-icon">&#128218;</div><h3>Research Datasets</h3><p>Maddison Project (year 1 CE onward), Penn World Table, Shiller, Fama-French, Barro-Lee, and more.</p></div>
    </div>
  </div>
</section>

<!-- ── How to Access ── -->
<section class="section section-alt">
  <div class="container">
    <h2 class="section-title">Multiple ways to access the data</h2>
    <div class="grid-3">
      <div class="acard">
        <div class="card-icon">&#8681;</div>
        <h3>Browser Download</h3>
        <p>Search the catalog, pick series, and download citation-headed CSVs — individually or as a multi-series ZIP bundle built in your browser.</p>
        <a href="download.html" class="btn btn-primary" style="margin-top:1rem;">Browse Downloads</a>
      </div>
      <div class="acard">
        <div class="card-icon">{&thinsp;}</div>
        <h3>REST API</h3>
        <p>Programmatic search, metadata, series CSV, and reproducible snapshot-pinned bundles. Free key. Python and R clients available.</p>
        <a href="download.html#api" class="btn btn-primary" style="margin-top:1rem;">Get API Access</a>
      </div>
      <div class="acard">
        <div class="card-icon">&#129302;</div>
        <h3>MCP for AI Assistants</h3>
        <p>Let Claude or any MCP-capable assistant search and fetch series directly — with licenses, citations, and freshness attached to every answer.</p>
        <a href="mcp.html" class="btn btn-primary" style="margin-top:1rem;">MCP Server</a>
      </div>
    </div>
  </div>
</section>

<!-- ── Catalog search (live) ── -->
<section class="section" id="catalog">
  <div class="container-narrow">
    <h2 class="section-title" style="margin-bottom:.5rem">Browse the catalog</h2>
    <p style="text-align:center;color:var(--g500);margin-bottom:2rem">__N__ sources &middot; search datasets in English, or series in 6 languages via the live API</p>
    <div class="controls">
      <input id="q" placeholder="Search by name, id, description, license…" oninput="render()">
      <select id="f" onchange="render()">
        <option value="">All datasets</option>
        <option value="open">Redistributed only</option>
        <option value="meta">Metadata-only</option>
      </select>
      <select id="lang" onchange="onLang()" title="Search series in another language" aria-label="Language">
        <option value="en">English</option>
        <option value="ar">العربية</option>
        <option value="es">Español</option>
        <option value="fr">Français</option>
        <option value="ru">Русский</option>
        <option value="zh">中文</option>
      </select>
    </div>
    <div class="count" id="count"></div>
    <div id="results"></div>
  </div>
</section>

<!-- ── Comparison ── -->
<section class="section section-alt">
  <div class="container">
    <h2 class="section-title">How this compares</h2>
    <div class="table-wrap">
      <table class="cmp">
        <thead>
          <tr>
            <th>Feature</th>
            <th class="comparison-highlight">Econ Data Library</th>
            <th>FRED</th>
            <th>DBnomics</th>
            <th>Bloomberg</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Price</strong></td>
            <td class="comparison-highlight"><strong>Free</strong></td>
            <td>Free</td>
            <td>Free</td>
            <td>$25,000+/yr</td>
          </tr>
          <tr>
            <td><strong>Individual series</strong></td>
            <td class="comparison-highlight"><strong id="cmp-series">billions</strong></td>
            <td>~800k</td>
            <td>1B+</td>
            <td>Terminal-gated</td>
          </tr>
          <tr>
            <td><strong>License on every series</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Yes</span></td>
            <td>Partial</td>
            <td>Partial</td>
            <td>Proprietary</td>
          </tr>
          <tr>
            <td><strong>Producer-first citations</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Every series</span></td>
            <td><span class="comparison-x">No</span></td>
            <td><span class="comparison-x">No</span></td>
            <td><span class="comparison-x">No</span></td>
          </tr>
          <tr>
            <td><strong>Reproducible bundles</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Snapshot-pinned</span></td>
            <td><span class="comparison-x">No</span></td>
            <td><span class="comparison-x">No</span></td>
            <td><span class="comparison-x">No</span></td>
          </tr>
          <tr>
            <td><strong>AI/MCP access</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Built-in</span></td>
            <td><span class="comparison-x">No</span></td>
            <td><span class="comparison-x">No</span></td>
            <td>Paid add-on</td>
          </tr>
          <tr>
            <td><strong>Machine-readable metadata</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Dataset + Croissant</span></td>
            <td>Partial</td>
            <td>Partial</td>
            <td><span class="comparison-x">No</span></td>
          </tr>
          <tr>
            <td><strong>Multilingual search</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">6 languages</span></td>
            <td>English</td>
            <td>English</td>
            <td>Multiple</td>
          </tr>
          <tr>
            <td><strong>Update transparency</strong></td>
            <td class="comparison-highlight"><span class="comparison-check">Public status board</span></td>
            <td>Partial</td>
            <td>Partial</td>
            <td><span class="comparison-x">Opaque</span></td>
          </tr>
        </tbody>
      </table>
    </div>
    <p style="text-align:center;color:var(--g500);font-size:.85rem">Series counts are approximate for third parties (their own published figures); ours is measured live on the data store.</p>
  </div>
</section>

<!-- ── FAQ ── -->
<section class="section">
  <div class="container-narrow">
    <h2 class="section-title">Frequently asked questions</h2>
    __FAQ__
  </div>
</section>

<!-- ── Footer (hf-style) ── -->
<footer class="footer">
  <div class="container">
    <div class="footer-grid">
      <div>
        <h4>Econ Data Library</h4>
        <p>Econ Data Library is the largest totally free online database in the world — dedicated to bringing all of the world's freely available data into a single, easily accessible location, with the help of cutting-edge AI tools. Built and maintained by Ahmed Elkassabgi at the University of Central Arkansas.</p>
        <p style="margin-top:.75rem">Part of the <a href="https://hfdatalibrary.com/">ElkassabgiData</a> family — one free account for every library.</p>
      </div>
      <div>
        <h4>Data</h4>
        <ul>
          <li><a href="#catalog">Browse the Catalog</a></li>
          <li><a href="download.html">Download</a></li>
          <li><a href="status.html">Source Status</a></li>
        </ul>
      </div>
      <div>
        <h4>Access</h4>
        <ul>
          <li><a href="download.html#api">REST API</a></li>
          <li><a href="mcp.html">MCP Server</a></li>
          <li><a href="account.html">Account</a></li>
        </ul>
      </div>
      <div>
        <h4>About</h4>
        <ul>
          <li><a href="about.html">Our Story</a></li>
          <li><a href="cite.html">How to Cite</a></li>
          <li><a href="https://hfdatalibrary.com/">HF Data Library</a></li>
          <li><a href="sitemap.xml">Sitemap</a></li>
        </ul>
      </div>
    </div>
    <div class="footer-bottom">
      <p>&copy; 2026 Ahmed Elkassabgi. University of Central Arkansas. &middot; Generated __GEN__</p>
      <p class="orcid">ORCID: <a href="https://orcid.org/0000-0002-5926-7493">0000-0002-5926-7493</a></p>
    </div>
  </div>
</footer>
<script>
const IDX=__DATA__;
// Live API: when a non-English language is picked, search the full series index
// and show OFFICIAL localized titles (World Bank /v2/<lang>, IMF SDMX, ILOSTAT).
// English keeps the instant client-side dataset search below, unchanged.
const API="https://econdl-api.elkassabgi.workers.dev";
function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function curLang(){return document.getElementById('lang').value;}
function onLang(){
 const L=curLang(), ar=(L==='ar');
 document.documentElement.dir=ar?'rtl':'ltr';
 document.documentElement.lang=L;
 document.getElementById('f').style.display=(L==='en')?'':'none';
 document.getElementById('q').placeholder=(L==='en')
  ?'Search by name, id, description, license…':'Search series in this language…';
 render();
}
function render(){
 if(curLang()!=='en'){clearTimeout(render._t);render._t=setTimeout(renderApi,250);return;}
 renderLocal();
}
function renderLocal(){
 const q=document.getElementById('q').value.toLowerCase().trim();
 const f=document.getElementById('f').value;
 let rows=IDX.filter(r=>{
  if(f==='open'&&!r.reservable)return false;
  if(f==='meta'&&r.reservable)return false;
  if(q){const h=(r.id+' '+r.name+' '+r.desc+' '+r.license+' '+(r.cats||[]).join(' ')).toLowerCase();
   if(!h.includes(q))return false;}
  return true;});
 rows.sort((a,b)=>a.name.localeCompare(b.name));
 document.getElementById('count').textContent=rows.length+' of '+IDX.length+' datasets';
 const out=rows.map(r=>{
  const badge=r.reservable?'<span class="badge open">redistributed</span>':'<span class="badge meta">metadata only</span>';
  const cats=(r.cats||[]).slice(0,4).map(c=>'<span class="badge cat">'+esc(c)+'</span>').join('');
  const ser=r.n_series?'<span class="count">'+r.n_series.toLocaleString()+' series</span>':'';
  return '<a class="card" href="'+r.page+'"><div class="cid">'+esc(r.id)+'</div>'+
   '<h3>'+esc(r.name)+'</h3>'+
   (r.desc?'<p>'+esc(r.desc)+'</p>':'')+
   '<div class="row">'+badge+'<span class="badge lic">'+esc(r.license)+'</span>'+cats+' '+ser+'</div></a>';
 }).join('');
 document.getElementById('results').innerHTML=out||'<p style="color:#6b7280">No datasets match.</p>';
}
async function renderApi(){
 const L=curLang();
 const q=document.getElementById('q').value.trim();
 const cnt=document.getElementById('count');
 cnt.textContent='Searching…';
 try{
  const u=API+'/v1/catalog?lang='+encodeURIComponent(L)+'&limit=50'+(q?('&q='+encodeURIComponent(q)):'');
  const r=await fetch(u);
  if(!r.ok)throw new Error('http '+r.status);
  const d=await r.json();
  const rows=d.results||[];
  cnt.textContent=(d.total||rows.length).toLocaleString()+' series';
  const out=rows.map(s=>{
   const src=(s.series_id||'').split(':')[0];
   return '<a class="card" href="'+esc(src)+'.html"><div class="cid">'+esc(s.series_id)+'</div>'+
    '<h3>'+esc(s.title)+'</h3>'+
    '<div class="row"><span class="badge lic">'+esc(s.source)+'</span>'+
    (s.geography?'<span class="badge cat">'+esc(s.geography)+'</span>':'')+
    (s.frequency?'<span class="badge cat">'+esc(s.frequency)+'</span>':'')+'</div></a>';
  }).join('');
  document.getElementById('results').innerHTML=out||'<p style="color:#6b7280">No series match.</p>';
 }catch(e){
  cnt.textContent='';
  document.getElementById('results').innerHTML=
   '<p style="color:#6b7280">Live multilingual search is temporarily unavailable. Switch to English for the dataset catalog.</p>';
 }
}
render();
// Animated counter (ported from hfdatalibrary.com js/site.js): counts up over
// ~2s, then shows the FULL written-out number with the billions label beneath.
// Floor, never round up - reported counts must never overstate the store.
function animateCounter(el, target) {
  var duration = 2000, startTime = null;
  function step(ts) {
    if (!startTime) startTime = ts;
    var progress = Math.min((ts - startTime) / duration, 1);
    var eased = 1 - Math.pow(1 - progress, 3);
    el.textContent = Math.floor(eased * target).toLocaleString();
    if (progress < 1) { requestAnimationFrame(step); }
    else {
      var billions = (Math.floor(target / 1e8) / 10).toFixed(1) + "+ Billion";
      el.style.lineHeight = "1.1";
      el.innerHTML = target.toLocaleString() +
        '<br><span style="font-size:0.45em; opacity:0.7; line-height:1;">(' + billions + ")</span>";
    }
  }
  requestAnimationFrame(step);
}
// Live headline counts from /v1/stats - never hardcoded in the page.
fetch(API + "/v1/stats").then(function (r) { return r.json(); }).then(function (d) {
  function fmtB(n) { if (n < 1e9) return Number(n).toLocaleString(); var s = (Math.floor(n / 1e8) / 10).toFixed(1); if (s.slice(-2) === ".0") s = s.slice(0, -2); return s + "B+"; }
  if (d.individual_series) document.getElementById("live-series").textContent = fmtB(d.individual_series);
  if (d.observations) animateCounter(document.getElementById("obs-counter"), d.observations);
  if (d.as_of) document.getElementById("live-asof").textContent = d.as_of;
  if (d.individual_series) { var c = document.getElementById("cmp-series"); if (c) c.textContent = fmtB(d.individual_series); }
}).catch(function () {});
</script>
</body></html>
"""
    )
    faq_html = "\n".join(
        f'<div class="faq-item"><h3>{esc(q)}</h3><p>{esc(a)}</p></div>'
        for q, a in faq_pairs
    )
    # Headline stats are NOT baked in (owner rule: no stale hardcoded counts).
    # The page fetches /v1/stats live; the worker serves R2 _aqueduct/stats.json,
    # refreshed by each census run.
    years = _earliest_data_year()
    return (
        tpl.replace("__DATA__", data)
        .replace("__FAQ__", faq_html)
        .replace("__YEARS__", f"{years:,}+" if years else "&mdash;")
        .replace("__NSERIES__", f"{n_series_total/1e6:.2f}M" if n_series_total >= 1e6 else f"{n_series_total:,}")
        .replace("__NACTIVE__", str(n_active))
        .replace("__N__", str(n_total))
        .replace("__NOPEN__", str(n_open))
        .replace("__NMETA__", str(n_meta))
        .replace("__GEN__", generated or TODAY)
    )


_INFO_CSS = """
.wrap h2{margin-top:2rem}
.wrap pre{background:var(--navy);color:#e2e8f0;padding:1.1rem 1.3rem;border-radius:8px;
overflow-x:auto;font-size:.82rem;line-height:1.6;font-family:var(--mono);margin:.8rem 0 1.2rem}
.wrap table{width:100%;border-collapse:collapse;font-size:.88rem;margin:.8rem 0 1.2rem}
.wrap th{text-align:left;padding:.6rem .8rem;border-bottom:2px solid var(--g300);color:var(--g700)}
.wrap td{padding:.55rem .8rem;border-bottom:1px solid var(--g200);vertical-align:top}
.wrap td code{background:var(--g100);padding:.1em .35em;border-radius:4px;font-family:var(--mono);font-size:.85em}
"""


def _info_page(title, meta_desc, page, body):
    head = HEAD.format(
        title=f"{title} — {SITE_NAME}",
        meta_desc=meta_desc,
        canonical=f"{SITE_BASE}/{page}",
        css=PAGE_CSS + _INFO_CSS,
        jsonld="",
    )
    return (head
            + f'<div class="wrap"><h1>{title}</h1>\n{body}\n'
            + f'<div class="foot">Generated __SITE_UPDATED__ &middot; <a href="index.html">Catalog</a> &middot; <a href="sitemap.xml">sitemap.xml</a></div></div></body></html>')


def render_docs():
    body = """
<p class="lead">How the library works: one namespace, honest licensing, reproducible downloads, and a public update pipeline.</p>
<h2>The namespace</h2>
<p>Every series has a stable id of the form <code>source:series_key[:geography]</code> — for example <code>worldbank:NY.GDP.MKTP.CD:USA</code>. The id is permanent, appears in every download, and resolves over the API.</p>
<h2>Two catalog tiers</h2>
<p><strong>Redistributed</strong> sources have licenses that permit re-hosting: their data is served from our store as citation-headed CSV, over the API, and in bundles. <strong>Metadata-only</strong> sources have licenses that forbid re-hosting: they are fully searchable and carry machine-readable metadata and pointers, but the data stays with the publisher — a direct request returns an honest HTTP 451 with the publisher's link. Nothing restricted is ever silently redistributed.</p>
<h2>Reproducibility</h2>
<p>Bundles are snapshot-pinned: a bundle manifest records the snapshot date and the exact member series, so the same request reproduces the same data. Every CSV carries its license and producer-first citation in a comment header.</p>
<h2>The update pipeline</h2>
<p>Sources are refreshed by a cadence-aware pipeline (daily, weekly, monthly, annual — matching each publisher's own schedule). Freshness is never fabricated: a series' date advances only when observations were actually fetched, and failures surface on the public <a href="status.html">status board</a> rather than being hidden.</p>
<h2>Multilingual titles</h2>
<p>Series search is available in six languages (English, Arabic, Spanish, French, Russian, Chinese) using only the sources' official translations — titles are never machine-translated.</p>
<h2>One account, one family</h2>
<p>The free ElkassabgiData key works across the family — this library and <a href="https://hfdatalibrary.com/">HF Data Library</a> (1-minute U.S. equity data). Get a key from the <a href="download.html">Download page</a>.</p>
"""
    return _info_page("Documentation", "How Econ Data Library works: namespace, licensing tiers, reproducible bundles, update pipeline.", "docs.html", body)


def render_api():
    api = "https://econdl-api.elkassabgi.workers.dev"
    body = f"""
<p class="lead">A free REST API over the full catalog. Search and metadata need no key; data downloads use a free key (<code>X-API-Key</code> header, <code>Authorization: Bearer</code>, or <code>?api_key=</code>).</p>
<h2>Base URL</h2>
<pre>{api}</pre>
<h2>Endpoints</h2>
<table>
<tr><th>Endpoint</th><th>What it returns</th><th>Key</th></tr>
<tr><td><code>GET /v1/catalog</code></td><td>Series search. Params: <code>q</code>, <code>source</code>, <code>limit</code>, <code>offset</code>, <code>lang</code> (en/ar/es/fr/ru/zh).</td><td>No</td></tr>
<tr><td><code>GET /v1/series/{{id}}.csv</code></td><td>The series as tidy <code>date,value</code> CSV with license + citation header. Params: <code>from</code>, <code>to</code>, <code>raw=1</code> (bare CSV).</td><td>Yes</td></tr>
<tr><td><code>GET /v1/series/{{id}}.metadata.json</code></td><td>Full metadata: title, frequency, geography, unit, license (incl. commercial-use flag), attribution, coverage.</td><td>No</td></tr>
<tr><td><code>GET /v1/sources</code></td><td>Every source with license and freshness summary.</td><td>No</td></tr>
<tr><td><code>GET /v1/bundle</code></td><td>Snapshot-pinned bundle manifest (Frictionless datapackage). Params: <code>ids=</code> or <code>source=</code>, <code>snapshot=</code>.</td><td>No</td></tr>
<tr><td><code>GET /v1/stats</code></td><td>Live store-measured counts (series, observations, as-of date).</td><td>No</td></tr>
<tr><td><code>GET /v1/last-updates</code></td><td>Per-source freshness board (the data behind <a href="status.html">Status</a>).</td><td>No</td></tr>
</table>
<p>Requests for series from metadata-only sources return HTTP <code>451</code> with the publisher's link — see <a href="docs.html">Documentation</a>.</p>
<h2>Quick start</h2>
<pre># curl — one series as CSV
curl -H "X-API-Key: $KEY" \\
  "{api}/v1/series/worldbank:NY.GDP.MKTP.CD:USA.csv"

# Python
import io, requests, pandas as pd
r = requests.get("{api}/v1/series/worldbank:NY.GDP.MKTP.CD:USA.csv",
                 headers={{"X-API-Key": KEY}})
df = pd.read_csv(io.StringIO(r.text), comment="#")</pre>
<p>Get a free key on the <a href="download.html">Download page</a> — one key for the whole ElkassabgiData family.</p>
"""
    return _info_page("API Reference", "Free REST API for economic & financial time series: search, metadata, CSV, reproducible bundles.", "api.html", body)


def render_cite():
    body = """
<p class="lead">Citations here are <strong>producer-first</strong>: credit the original statistical agency before the library. Every series and bundle ships its own ready-made citation.</p>
<h2>Citing a series</h2>
<p>Each series' citation (original producer, license, retrieval date, series id) is included in its CSV download header and its <code>metadata.json</code>. Use that citation — it names the agency that actually produced the numbers.</p>
<h2>Citing the library</h2>
<blockquote class="cite">Elkassabgi, A. (2026). Econ Data Library: a citable catalog of economic and financial time series. https://econdatalibrary.com</blockquote>
<h2>BibTeX</h2>
<pre>@misc{econdatalibrary,
  author = {Elkassabgi, Ahmed},
  title  = {Econ Data Library: a citable catalog of economic
            and financial time series},
  year   = {2026},
  url    = {https://econdatalibrary.com}
}</pre>
<h2>Reproducibility note</h2>
<p>For exact reproducibility, cite the <em>bundle snapshot date</em> shown in your download's manifest — the same snapshot always resolves to the same data.</p>
"""
    return _info_page("How to Cite", "Producer-first citations for every series, plus how to cite the Econ Data Library itself.", "cite.html", body)


def render_contact():
    # Mirrors hfdatalibrary.com/pages/contact, adapted to econ (series ids and
    # source requests instead of tickers). Same family contact email.
    body = """
<div style="text-align:center;margin:1.2rem 0 2.2rem">
  <h2 style="border:none;margin:0 0 .3rem;font-size:1.45rem">Ahmed Elkassabgi</h2>
  <p style="color:var(--g500);margin:.1rem 0">Associate Professor of Finance</p>
  <p style="color:var(--g500);margin:.1rem 0">University of Central Arkansas</p>
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:1.2rem;margin-bottom:2.2rem">
  <div style="border:1px solid var(--g200);border-radius:12px;padding:1.2rem;text-align:center">
    <b>Email</b><p style="margin:.4rem 0 0"><a href="mailto:admin@hfdatalibrary.com">admin@hfdatalibrary.com</a></p>
  </div>
  <div style="border:1px solid var(--g200);border-radius:12px;padding:1.2rem;text-align:center">
    <b>ORCID</b><p style="margin:.4rem 0 0"><a href="https://orcid.org/0000-0002-5926-7493">0000-0002-5926-7493</a></p>
  </div>
</div>
<h2>Reporting data issues</h2>
<p>If you find an error in the data &mdash; wrong values, missing observations, a bad unit or label &mdash; please email me with:</p>
<ul class="notes">
  <li>The series id (e.g. <code>worldbank:NY.GDP.MKTP.CD:USA</code>)</li>
  <li>Date(s) or observation(s) affected</li>
  <li>Description of the issue</li>
  <li>How you identified it (comparison source, expected value, etc.)</li>
</ul>
<p>Every reported issue is investigated against the original publisher's data.</p>
<h2>Requesting new sources or series</h2>
<p>The catalog covers 183 sources. If you need a source or series that isn't included, email me the source, what you need from it, and the research use &mdash; sources are added in batches as licensing permits.</p>
<h2>University of Central Arkansas</h2>
<p style="color:var(--g600)">College of Business<br>201 Donaghey Avenue<br>Conway, AR 72035<br>United States</p>
"""
    return _info_page("Contact",
                      "Contact Ahmed Elkassabgi about the Econ Data Library: questions, data issues, and source requests for free economic & financial time-series data.",
                      "contact.html", body)


def render_stats():
    # Live usage. USER figures (count, world map, institutions) are the SHARED
    # ElkassabgiData community — served by the econ worker's /v1/public-stats,
    # which reads the shared identity DB with hf's exact aggregation, so they
    # match the HF Data Library by construction (one login, one user base).
    # DATA volume comes from /v1/stats; DOWNLOADS are this library's own
    # (econ_download_log). Every number is fetched live — nothing hardcoded.
    body = """
<style>
.statgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin:1.4rem 0 .5rem}
.bigstat{background:var(--g50);border:1px solid var(--g200);border-radius:12px;padding:1.3rem .7rem;text-align:center}
.bnum{font-family:var(--mono);font-size:1.85rem;font-weight:700;color:var(--navy);line-height:1.1}
.blabel{font-size:.72rem;text-transform:uppercase;letter-spacing:.05em;color:var(--g500);margin-top:.45rem}
.dlnote{font-size:.82rem;color:var(--g500);margin:.1rem 0 2rem}
#world-map{width:100%;height:460px;margin:.2rem auto 0}
#country-badges{display:flex;flex-wrap:wrap;gap:.5rem;justify-content:center;margin-top:1.3rem}
.cbadge{padding:.28rem .7rem;border-radius:20px;font-size:.82rem;font-weight:500}
.cbadge-u{background:#1e3a8a;color:#fff}
.cbadge-v{background:#dbeafe;color:#1e40af}
.reach-key{text-align:center;color:var(--g500);font-size:.9rem;margin:.2rem 0 1rem}
.twocol{display:grid;grid-template-columns:1fr 1fr;gap:2rem;margin:.5rem 0}
.dlbar{margin-bottom:.85rem}
.dlname{font-size:.87rem;color:var(--navy);font-weight:600;display:block;margin-bottom:.28rem}
.dlrow{display:flex;align-items:center;gap:.6rem}
.dlfill{height:20px;background:var(--blue);border-radius:4px;min-width:3px}
.dlcount{font-family:var(--mono);font-size:.8rem;color:var(--g500);white-space:nowrap}
.inst-list{font-size:.92rem;margin-top:.4rem}
.inst-row{display:flex;align-items:center;padding:.26rem 0;color:var(--navy)}
.inst-ic{display:inline-block;width:20px;margin-right:8px;text-align:center;flex-shrink:0}
.inst-more{cursor:pointer;color:var(--blue);font-weight:600;padding:.55rem 0;border-top:1px solid var(--g200);margin-top:.5rem}
.inst-more:hover{color:var(--gold-deep)}
.inst-sign{display:inline-block;width:16px;font-family:var(--mono)}
.actgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin:.4rem 0}
.actcard{background:var(--g50);border:1px solid var(--g200);border-radius:10px;padding:1.1rem;text-align:center}
.anum{font-family:var(--mono);font-size:1.45rem;font-weight:700;color:var(--navy)}
.alabel{font-size:.78rem;color:var(--g500);margin-top:.2rem}
@media(max-width:640px){.statgrid{grid-template-columns:repeat(2,1fr)}.actgrid{grid-template-columns:1fr}.twocol{grid-template-columns:1fr}}
</style>

<p class="lead">Real-time usage for the Econ Data Library, updated live from the database.</p>

<div class="statgrid">
  <div class="bigstat"><div class="bnum" id="s-visitors">&mdash;</div><div class="blabel">Total Visitors</div></div>
  <div class="bigstat"><div class="bnum" id="s-users">&mdash;</div><div class="blabel">Registered Users</div></div>
  <div class="bigstat"><div class="bnum" id="s-downloads">&mdash;</div><div class="blabel">Total Downloads</div></div>
  <div class="bigstat"><div class="bnum" id="s-bytes">&mdash;</div><div class="blabel">Data Served</div></div>
</div>
<h2>Global Reach</h2>
<p class="reach-key"><span style="color:#1e3a8a;font-weight:700">Dark</span> = registered users (<span id="s-usercountries">&mdash;</span> countries) &middot; <span style="color:#60a5fa;font-weight:700">Light</span> = site visitors (<span id="s-visitorcountries">&mdash;</span>)</p>
<div id="world-map"><p style="text-align:center;color:var(--g500);padding-top:190px">Loading map&hellip;</p></div>
<div id="country-badges"></div>

<div class="twocol">
  <div><h2>Most Downloaded Sources</h2><div id="dl-chart"><p style="color:var(--g500)">Loading&hellip;</p></div></div>
  <div><h2>Institutions Represented</h2><div id="institution-list"><p style="color:var(--g500)">Loading&hellip;</p></div></div>
</div>

<h2>At a Glance</h2>
<div class="actgrid">
  <div class="actcard"><div class="anum" id="s-today">&mdash;</div><div class="alabel">Downloads Today</div></div>
  <div class="actcard"><div class="anum" id="s-week">&mdash;</div><div class="alabel">Downloads This Week</div></div>
  <div class="actcard"><div class="anum" id="s-pageviews">&mdash;</div><div class="alabel">Page Views</div></div>
</div>

<script src="https://www.gstatic.com/charts/loader.js"></script>
<script>
var ECON='https://econdl-api.elkassabgi.workers.dev';
google.charts.load('current',{packages:['geochart']});
var mapData=null, chartsReady=false;
google.charts.setOnLoadCallback(function(){chartsReady=true; if(mapData) drawMap();});
function set(id,v){var e=document.getElementById(id); if(e&&v!=null)e.textContent=v;}
function fmtB(n){ if(n>=1e9){var s=(Math.floor(n/1e8)/10).toFixed(1); if(s.slice(-2)==='.0')s=s.slice(0,-2); return s+'B+';} return Number(n).toLocaleString();}
function fmtBytes(n){ n=Number(n)||0; if(n>=1e9)return (n/1e9).toFixed(1)+' GB'; if(n>=1e6)return (n/1e6).toFixed(1)+' MB'; if(n>=1e3)return (n/1e3).toFixed(1)+' KB'; return n+' B'; }
function flag(c){ if(!c||c.length!==2)return ''; return '<img src="https://flagcdn.com/16x12/'+c.toLowerCase()+'.png" width="16" height="12" alt="'+c+'" style="vertical-align:middle;margin-right:4px">';}
var COUNTRY_NAMES={AF:'Afghanistan',AL:'Albania',DZ:'Algeria',AO:'Angola',AR:'Argentina',AM:'Armenia',AU:'Australia',AT:'Austria',AZ:'Azerbaijan',BH:'Bahrain',BD:'Bangladesh',BY:'Belarus',BE:'Belgium',BO:'Bolivia',BA:'Bosnia and Herzegovina',BR:'Brazil',BN:'Brunei',BG:'Bulgaria',KH:'Cambodia',CM:'Cameroon',CA:'Canada',CL:'Chile',CN:'China',CO:'Colombia',CR:'Costa Rica',HR:'Croatia',CU:'Cuba',CY:'Cyprus',CZ:'Czechia',DK:'Denmark',DO:'Dominican Republic',EC:'Ecuador',EG:'Egypt',SV:'El Salvador',EE:'Estonia',ET:'Ethiopia',FI:'Finland',FR:'France',GE:'Georgia',DE:'Germany',GH:'Ghana',GR:'Greece',GT:'Guatemala',HT:'Haiti',HN:'Honduras',HK:'Hong Kong',HU:'Hungary',IS:'Iceland',IN:'India',ID:'Indonesia',IR:'Iran',IQ:'Iraq',IE:'Ireland',IL:'Israel',IT:'Italy',JM:'Jamaica',JP:'Japan',JO:'Jordan',KZ:'Kazakhstan',KE:'Kenya',KP:'North Korea',KR:'South Korea',KW:'Kuwait',LA:'Laos',LV:'Latvia',LB:'Lebanon',LT:'Lithuania',LU:'Luxembourg',MY:'Malaysia',MX:'Mexico',MN:'Mongolia',MA:'Morocco',MM:'Myanmar',NP:'Nepal',NL:'Netherlands',NZ:'New Zealand',NI:'Nicaragua',NG:'Nigeria',NO:'Norway',OM:'Oman',PK:'Pakistan',PS:'Palestine',PA:'Panama',PY:'Paraguay',PE:'Peru',PH:'Philippines',PL:'Poland',PT:'Portugal',PR:'Puerto Rico',QA:'Qatar',RO:'Romania',RU:'Russia',SA:'Saudi Arabia',SN:'Senegal',RS:'Serbia',SG:'Singapore',SK:'Slovakia',SI:'Slovenia',ZA:'South Africa',ES:'Spain',LK:'Sri Lanka',SY:'Syria',TW:'Taiwan',TZ:'Tanzania',TH:'Thailand',TT:'Trinidad and Tobago',TN:'Tunisia',TR:'Turkey',UG:'Uganda',UA:'Ukraine',AE:'United Arab Emirates',GB:'United Kingdom',US:'United States',UY:'Uruguay',UZ:'Uzbekistan',VE:'Venezuela',VN:'Vietnam',YE:'Yemen',ZW:'Zimbabwe'};
function countryName(c){return COUNTRY_NAMES[c]||c;}
function drawMap(){
  var users=Object.assign({},mapData.users||{});
  var visitors=mapData.visitors||{};
  if(!users['PS'])users['PS']=1;
  var codes=new Set(Object.keys(users).concat(Object.keys(visitors)));
  if(!codes.size)return;
  var rows=[['Country','Type']];
  codes.forEach(function(c){rows.push([c, users[c]?2:1]);});
  var data=google.visualization.arrayToDataTable(rows);
  var opts={colorAxis:{minValue:1,maxValue:2,colors:['#93c5fd','#1e3a8a']},backgroundColor:'#fff',datalessRegionColor:'#e5e7eb',defaultColor:'#e5e7eb',legend:'none'};
  new google.visualization.GeoChart(document.getElementById('world-map')).draw(data,opts);
  var us=Object.entries(users).sort(function(a,b){return b[1]-a[1];});
  var vo=Object.entries(visitors).filter(function(e){return !users[e[0]];}).sort(function(a,b){return b[1]-a[1];});
  document.getElementById('country-badges').innerHTML=
    us.map(function(e){return '<span class="cbadge cbadge-u">'+flag(e[0])+' '+countryName(e[0])+'</span>';}).join('') +
    vo.slice(0,40).map(function(e){return '<span class="cbadge cbadge-v">'+flag(e[0])+' '+countryName(e[0])+'</span>';}).join('');
}
function toggleInst(){var m=document.getElementById('inst-more'),s=document.getElementById('inst-sign'); if(!m||!s)return; var o=m.style.display!=='none'; m.style.display=o?'none':'block'; s.textContent=o?'+':'-';}
// Verified school domains -> favicon (mirrors hf). Only mapped names get an
// icon; unmapped ones render a blank fixed-width slot (never a guessed/wrong logo).
var INST_DOMAINS={'University of Central Arkansas':'uca.edu','Stanford University':'stanford.edu','Renmin University of China':'CUSTOM:https://upload.wikimedia.org/wikipedia/en/thumb/1/11/Renmin_University_of_China_logo.svg/250px-Renmin_University_of_China_logo.svg.png','Georgia Institute of Technology':'gatech.edu','Copenhagen Business School':'cbs.dk','Texas State University':'txst.edu','Texas A&M international University':'tamiu.edu','Konkuk University Graduate School':'www.konkuk.ac.kr','University of Southern Mississippi':'usm.edu','University of Wisconsin-Madison':'wisc.edu','University of Sydney':'sydney.edu.au','Shanghai University of Finance and Economics':'www.shufe.edu.cn','TeleAI':'teleai.com','Moscow Institute of Physics and Technology':'mipt.ru','University of Nottingham':'nottingham.ac.uk','University of Maryland, College Park':'umd.edu','University of Medicine and Pharmacy of Craiova':'umfcv.ro','University of Portsmouth':'port.ac.uk','Toronto Metropolitan University':'torontomu.ca','BITS Pilani':'bits-pilani.ac.in','Central China Normal University':'www.ccnu.edu.cn','Yanan University':'yau.edu.cn','University of North Carolina':'unc.edu','University of Arkansas':'uark.edu','Saint Peter\\'s University':'saintpeters.edu','Erasmus Universiteit Rotterdam':'eur.nl','Stanford':'stanford.edu','University of Manchester':'manchester.ac.uk','Singapore university of technology and design':'sutd.edu.sg','Hongkong university':'hku.hk','HKUST':'hkust.edu.hk','Michigan':'umich.edu','University of Bath':'bath.ac.uk','University of bath':'bath.ac.uk','University of Milan-Bicocca':'CUSTOM:https://upload.wikimedia.org/wikipedia/commons/thumb/7/7b/Milano-Bicocca_University_logo_on_transparent_background.svg/250px-Milano-Bicocca_University_logo_on_transparent_background.svg.png','Salem University':'CUSTOM:https://upload.wikimedia.org/wikipedia/commons/thumb/2/29/Salem_University_logo_green.svg/250px-Salem_University_logo_green.svg.png','École de Technologie Supérieure':'etsmtl.ca','Creative Robots':'creative-robots.com','Harvard University':'harvard.edu','MIT':'mit.edu','Massachusetts Institute of Technology':'mit.edu','Yale University':'yale.edu','Princeton University':'princeton.edu','Columbia University':'columbia.edu','University of Chicago':'uchicago.edu','New York University':'nyu.edu','University of Pennsylvania':'upenn.edu','Duke University':'duke.edu','Northwestern University':'northwestern.edu','University of Michigan':'umich.edu','University of California, Berkeley':'berkeley.edu','UCLA':'ucla.edu','London School of Economics':'lse.ac.uk','University of Oxford':'ox.ac.uk','University of Cambridge':'cam.ac.uk','London Business School':'london.edu','University of Toronto':'utoronto.ca','National University of Singapore':'nus.edu.sg','Peking University':'pku.edu.cn','Tsinghua University':'www.tsinghua.edu.cn','University of Hong Kong':'hku.hk','ETH Zurich':'ethz.ch','University of Texas at Austin':'utexas.edu','University of Illinois':'illinois.edu','Cornell University':'cornell.edu','Carnegie Mellon University':'cmu.edu','University of Wisconsin':'wisc.edu','University of Minnesota':'umn.edu','Ohio State University':'osu.edu','University of Florida':'ufl.edu','University of Washington':'uw.edu','Boston University':'bu.edu','University of Southern California':'usc.edu','Erasmus University Rotterdam':'eur.nl','Fordham University':'fordham.edu','Old Dominion University':'odu.edu','East Carolina University':'ecu.edu','Oregon State University':'oregonstate.edu','Portland State University':'pdx.edu','Queensland University of Technology':'qut.edu.au','Postech':'postech.ac.kr','Erciyes Universitesi':'erciyes.edu.tr','Escuela superior politecnica de chimborazo':'espoch.edu.ec','Faculty of Economics and Business, University of Zagreb':'efzg.hr','Hasso Plattner Institute':'hpi.de','American Public University System':'apus.edu','College of Marin':'marin.edu','IIITG':'iiitg.ac.in','Amazon':'amazon.com','NVIDIA':'nvidia.com','MBBANK':'www.mbbank.com.vn','Heidelberg University':'uni-heidelberg.de','Hanyang':'hanyang.ac.kr','National Chengchi University':'www.nccu.edu.tw','Abertay':'abertay.ac.uk','CMU':'cmu.edu'};
var INST_PRESTIGE={'Stanford University':10,'Stanford':10,'National University of Singapore':20,'Cornell University':30,'University of Hong Kong':40,'Hongkong university':40,'HKUST':45,'University of Sydney':50,'University of Manchester':60,'University of Michigan':70,'Michigan':70,'University of Maryland, College Park':80,'Georgia Institute of Technology':90,'University of Illinois':100,'University of Nottingham':110,'University of Bath':115,'University of bath':115,'University of North Carolina':120,'Erasmus Universiteit Rotterdam':130,'University of Wisconsin-Madison':140,'University of Minnesota':150,'Ohio State University':160,'Moscow Institute of Physics and Technology':170,'Copenhagen Business School':180,'Singapore university of technology and design':190,'Toronto Metropolitan University':200,'Renmin University of China':210,'Shanghai University of Finance and Economics':220,'BITS Pilani':230,'Konkuk University Graduate School':240,'Central China Normal University':250,'University of Portsmouth':260,'University of Arkansas':270,'Texas State University':280,'University of Central Arkansas':290,'University of Southern Mississippi':300,'Yanan University':310,'University of Milan-Bicocca':315,'École de Technologie Supérieure':318,'Saint Peter\\'s University':320,'Texas A&M international University':330,'University of Medicine and Pharmacy of Craiova':340,'Salem University':350,'Harvard University':5,'University of Oxford':8,'University of Cambridge':9,'Columbia University':25,'Northwestern University':35,'Postech':55,'Erasmus University Rotterdam':130,'Oregon State University':165,'Hasso Plattner Institute':175,'Queensland University of Technology':205,'IIITG':235,'Faculty of Economics and Business, University of Zagreb':255,'Fordham University':285,'Portland State University':295,'Old Dominion University':305,'East Carolina University':308,'Erciyes Universitesi':325,'Escuela superior politecnica de chimborazo':345,'American Public University System':360,'Kantonsschule Zug':370,'College of Marin':372,'Amazon':9000,'NVIDIA':9000,'MBBANK':9000,'Soros Fund Management':9000,'Creative Robots':9000};
function instIcon(name){var val=INST_DOMAINS[name]; var inner=''; if(val){var url=val.indexOf('CUSTOM:')===0?val.substring(7):'https://www.google.com/s2/favicons?sz=32&domain='+val; inner='<img src="'+url+'" width="20" height="20" style="vertical-align:middle;border-radius:3px;object-fit:contain" onerror="this.style.display=\\'none\\'" alt="">';} return '<span class="inst-ic">'+inner+'</span>';}
async function load(){
  try{ var r=await fetch(ECON+'/v1/public-stats'); if(r.ok){var d=await r.json();
    set('s-users',(d.total_users||0).toLocaleString());
    if(d.total_visitors!=null)set('s-visitors',Number(d.total_visitors).toLocaleString());
    if(d.total_downloads!=null)set('s-downloads',Number(d.total_downloads).toLocaleString());
    if(d.total_bytes_served!=null)set('s-bytes',fmtBytes(d.total_bytes_served));
    if(d.downloads_today!=null)set('s-today',Number(d.downloads_today).toLocaleString());
    if(d.downloads_this_week!=null)set('s-week',Number(d.downloads_this_week).toLocaleString());
    if(d.total_page_views!=null)set('s-pageviews',Number(d.total_page_views).toLocaleString());
    var cc=d.country_count||Object.keys(d.countries||{}).length;
    set('s-usercountries',cc); set('s-usercountries2',cc);
    var vcc=d.visitor_country_count||Object.keys(d.visitor_countries||{}).length;
    set('s-visitorcountries', vcc+' countries');
    mapData={users:d.countries||{},visitors:d.visitor_countries||{}};
    if(chartsReady)drawMap();
    // Most downloaded sources — endpoint already whitelists against the catalog
    // (purged sources like WTO can never appear); names are the catalog's own.
    if(d.top_sources&&d.top_sources.length){
      var maxDl=d.top_sources[0].downloads||1;
      document.getElementById('dl-chart').innerHTML=d.top_sources.map(function(t){
        return '<div class="dlbar"><span class="dlname">'+t.name+'</span><div class="dlrow"><div class="dlfill" style="width:'+Math.max(3,(t.downloads/maxDl)*100)+'%"></div><span class="dlcount">'+Number(t.downloads).toLocaleString()+'</span></div></div>';
      }).join('');
    } else { document.getElementById('dl-chart').innerHTML='<p style="color:var(--g500)">No downloads yet.</p>'; }
    // Institutions — prestige-ordered, with verified icons (mirrors hf)
    if(d.institutions&&d.institutions.length){
      var sorted=d.institutions.slice().sort(function(a,b){var ar=INST_PRESTIGE[a.institution]||9999, br=INST_PRESTIGE[b.institution]||9999; if(ar!==br)return ar-br; return a.institution.localeCompare(b.institution);});
      var TOP=20, top=sorted.slice(0,TOP), rest=sorted.slice(TOP);
      var row=function(i){return '<div class="inst-row">'+instIcon(i.institution)+'<span>'+i.institution+'</span></div>';};
      var html='<div class="inst-list">'+top.map(row).join('');
      if(rest.length){html+='<div class="inst-more" onclick="toggleInst()"><span id="inst-sign" class="inst-sign">+</span> Other institutions ('+rest.length+')</div><div id="inst-more" style="display:none">'+rest.map(row).join('')+'</div>';}
      html+='</div>';
      document.getElementById('institution-list').innerHTML=html;
    } else { document.getElementById('institution-list').innerHTML='<p style="color:var(--g500)">No institutions yet.</p>'; }
  }}catch(e){}
}
load();
</script>
"""
    return _info_page("Live Statistics",
                      "Live usage for the Econ Data Library: registered users and global reach (shared across the ElkassabgiData family), plus this library's data volume and downloads.",
                      "stats.html", body)


def render_sitemap(records):
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
        "  <url>",
        f"    <loc>{xml_esc(SITE_BASE)}/index.html</loc>",
        f"    <lastmod>{TODAY}</lastmod>",
        "    <changefreq>daily</changefreq>",
        "  </url>",
    ]
    for r in records:
        parts.append("  <url>")
        parts.append(f"    <loc>{xml_esc(r['page_url'])}</loc>")
        lm = r["last_updated"] or TODAY
        parts.append(f"    <lastmod>{xml_esc(lm)}</lastmod>")
        parts.append("    <changefreq>weekly</changefreq>")
        parts.append("  </url>")
    parts.append("</urlset>")
    return "\n".join(parts) + "\n"


# ---------------------------------------------------------------------------- #
#  Main
# ---------------------------------------------------------------------------- #
def main():
    licenses, sources, series_roll, source_meta = load_registry()
    sidecar, generated = load_sidecar()

    os.makedirs(OUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------ #
    # DISPLAY POLICY (owner decision 2026-07-15): the site shows a page ONLY
    # for (a) sources whose data we DIRECTLY HOST (reservable + has series),
    # and (b) sources gated PENDING a permission reply (reference kept, data
    # 451). Metadata-only listings for anything else are misleading — no page,
    # no links, no mention. Refused sources (WTO) are purged entirely.
    # ------------------------------------------------------------------ #
    PENDING_PERMISSION = {
        # permission requested, awaiting reply (see REDISTRIBUTION_EMAIL_TRAIL
        # + PERMISSION_EMAIL_DRAFTS): reference stays, data gated.
        "bundesbank", "cboe", "cow", "defillama", "ei_statreview", "famafrench",
        "freedomhouse", "idb", "irena", "nbp", "polity", "shiller", "sipri",
        "tcmb", "whr", "worldbank_pink", "zillow",
        "owid",  # pending via the Energy Institute request (covers OWID's energy series)
        # written EMBED permission on file (official Tableau embed, no data):
        "social_progress",
    }
    records = []
    for sid in sorted(sources):
        rec = build_record(
            sid,
            sources[sid],
            licenses,
            series_roll.get(sid),
            sidecar.get(sid),
            source_meta.get(sid),
        )
        hosted = bool(rec["reservable"]) and bool(series_roll.get(sid))
        if hosted or sid in PENDING_PERMISSION:
            records.append(rec)

    def _write(path, html):
        # single post-process point: stamp the generation date, and append the
        # ElkassabgiData family plate at the very bottom of EVERY page (below the
        # page's own footer), just before </body>.
        html = html.replace("__SITE_UPDATED__", TODAY)
        html = html.replace("</body>", FAMILY_BAND + "</body>", 1)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

    # Per-dataset pages
    n_pages = 0
    for rec in records:
        _write(os.path.join(OUT_DIR, f"{rec['id']}.html"), render_dataset_page(rec))
        n_pages += 1

    # index.html
    _write(os.path.join(OUT_DIR, "index.html"), render_index(records, generated))

    # docs / api / cite (hf-parity information pages)
    _write(os.path.join(OUT_DIR, "docs.html"), render_docs())
    _write(os.path.join(OUT_DIR, "api.html"), render_api())
    _write(os.path.join(OUT_DIR, "cite.html"), render_cite())
    _write(os.path.join(OUT_DIR, "contact.html"), render_contact())
    _write(os.path.join(OUT_DIR, "stats.html"), render_stats())

    # sitemap.xml
    with open(os.path.join(OUT_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(render_sitemap(records))

    n_open = sum(1 for r in records if r["reservable"])
    print(f"Wrote {n_pages} dataset pages to {OUT_DIR}")
    print(f"  redistributed (with distribution): {n_open}")
    print(f"  metadata-only (no distribution):   {n_pages - n_open}")
    print(f"Wrote index.html and sitemap.xml")
    print(f"Sitemap lists {n_pages + 1} URLs (index + {n_pages} datasets)")


if __name__ == "__main__":
    main()
