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
    "NEEDS-REVIEW": "License under review",
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

    # license: prefer a resolvable URL; else the machine license_id token (NOT the
    # human label); omit entirely for unverified so we never assert a fake license.
    if rec["license_url"]:
        obj["license"] = rec["license_url"]
    elif rec["license_id"] and rec["license_id"] != "NEEDS-REVIEW":
        obj["license"] = rec["license_id"]

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
            "encodingFormat": "application/vnd.apache.parquet",
            "contentUrl": f"{SITE_BASE}/data/{rec['id']}/",
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
        obj["license"] = rec["license_id"]
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
                "@id": f"{rec['id']}-parquet",
                "name": f"{rec['id']}-parquet",
                "description": "Canonical long-format Parquet for this dataset.",
                "contentUrl": f"{SITE_BASE}/data/{rec['id']}/",
                "encodingFormat": "application/vnd.apache.parquet",
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
.nav{background:var(--navy);color:#fff;padding:0 1.5rem;height:60px;display:flex;
align-items:center;justify-content:space-between;position:sticky;top:0;z-index:50}
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
<style>{css}</style>
{jsonld}
</head><body>
<div class="nav"><div class="brand"><a href="index.html">econ<span class="d">datalibrary</span></a></div>
<div><a href="index.html">Catalog</a><a href="sitemap.xml">Sitemap</a></div></div>
"""


def jsonld_script(obj):
    payload = json.dumps(obj, ensure_ascii=False, indent=2).replace("</", "<\\/")
    return f'<script type="application/ld+json">\n{payload}\n</script>'


def render_dataset_page(rec):
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

    # Access / mirrors
    acc_rows = [
        ("Canonical landing", f'<a href="{esc(rec["page_url"])}">{esc(rec["page_url"])}</a>'),
        ("Hugging Face (planned mirror)", f'<a href="{esc(rec["hf_url"])}">{esc(rec["hf_url"])}</a> <span style="color:#9ca3af">(placeholder)</span>'),
        ("Zenodo (planned DOI)", f'<a href="{esc(rec["zenodo_url"])}">{esc(rec["zenodo_url"])}</a> <span style="color:#9ca3af">(placeholder)</span>'),
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
    return "\n".join(body)


def render_index(records, generated):
    # Lightweight client-side search over an embedded JSON index.
    idx = [
        {
            "id": r["id"],
            "name": r["name"],
            "desc": r["desc_short"] or "",
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

    # Index JSON-LD: a DataCatalog describing the whole site.
    catalog_ld = {
        "@context": "https://schema.org/",
        "@type": "DataCatalog",
        "name": SITE_NAME,
        "url": f"{SITE_BASE}/index.html",
        "description": (
            f"Searchable catalog of {n_total} economic and financial data sources "
            "with license, provenance, and machine-readable Dataset + Croissant metadata."
        ),
        "publisher": PUBLISHER,
    }

    head = HEAD.format(
        title=f"Data Catalog — {SITE_NAME}",
        meta_desc=f"Searchable catalog of {n_total} economic & financial data sources with license, provenance, and schema.org Dataset metadata.",
        canonical=f"{SITE_BASE}/index.html",
        css=PAGE_CSS
        + """
.hero{background:var(--navy);color:#fff;margin:-2rem -1.5rem 1.5rem;padding:3.2rem 1.5rem 2.6rem;
border-bottom:3px solid var(--gold)}
.hero .eyebrow{font-family:var(--mono);font-size:.74rem;letter-spacing:.14em;text-transform:uppercase;
color:var(--gold);margin-bottom:.7rem}
.hero h1{font-family:var(--serif);color:#fff;font-size:2.6rem;line-height:1.1;max-width:18ch}
.hero .tag{color:rgba(255,255,255,.74);font-size:1.05rem;margin-top:.8rem;max-width:60ch;line-height:1.55}
.heroStats{display:flex;gap:2.4rem;flex-wrap:wrap;margin-top:2rem}
.heroStats .n{font-family:var(--serif);font-size:1.9rem;color:#fff;line-height:1}
.heroStats .l{font-size:.78rem;color:rgba(255,255,255,.6);margin-top:.3rem;text-transform:uppercase;letter-spacing:.06em}
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
.card p{font-size:.88rem;color:var(--g600);margin:.2rem 0 .6rem;line-height:1.5}
.card .row{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center}
.count{color:var(--g500);font-size:.82rem;margin-left:auto;font-family:var(--mono)}
[dir=rtl] .count{margin-left:0;margin-right:auto}
[dir=rtl] .card .row{flex-direction:row-reverse}
[dir=rtl] .hero,[dir=rtl] .card{text-align:right}
[dir=rtl] .card h3{font-family:var(--sans)}
""",
        jsonld=jsonld_script(catalog_ld),
    )

    tpl = (
        head
        + """
<div class="wrap">
<div class="hero">
<div class="eyebrow">Econ Data Library</div>
<h1>Economic &amp; financial data, free and citable</h1>
<p class="tag">One namespace over the world's macro &amp; financial sources — each series resolvable, reproducible as a snapshot-pinned bundle, and carrying its license, provenance, and producer-first citation.</p>
<div class="heroStats">
<div><div class="n">__NIND__</div><div class="l">individual series</div></div>
<div><div class="n">__NOBS__</div><div class="l">observations</div></div>
<div><div class="n">__N__</div><div class="l">sources catalogued</div></div>
<div><div class="n">Free</div><div class="l">open &amp; reproducible</div></div>
</div>
<p class="tag" style="font-size:.78rem;opacity:.75;margin-top:.6rem">Measured on our data store __MEASURED_ASOF__: individual series = every fully-specified time series (distinct series key per source; ~1% estimation error, conservative floor); observations = exact row counts. The catalog below indexes these at dataset grain for browsing.</p>
</div>
<div class="controls">
<input id="q" placeholder="Search by name, id, description, license…" oninput="render()" autofocus>
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
<div class="foot">Generated __GEN__ &middot; __N__ datasets &middot; metadata from the central registry &middot;
<a href="sitemap.xml">sitemap.xml</a></div>
</div>
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
</script>
</body></html>
"""
    )
    # Store-measured headline stats (series census 2026-07-02, results in
    # _series_census_hll.json: global distinct series keys per source via
    # HyperLogLog ±1% — a conservative floor; observations = exact parquet row
    # counts; worker /v1/stats serves the same figures). Update these ONLY from
    # a fresh census run (_series_census_hll.py), never by hand.
    measured_series = "7.7B+"
    measured_obs = "79.8B"
    measured_asof = "2026-07-02"
    return (
        tpl.replace("__DATA__", data)
        .replace("__NSERIES__", f"{n_series_total/1e6:.2f}M" if n_series_total >= 1e6 else f"{n_series_total:,}")
        .replace("__NIND__", measured_series)
        .replace("__NOBS__", measured_obs)
        .replace("__MEASURED_ASOF__", measured_asof)
        .replace("__NACTIVE__", str(n_active))
        .replace("__N__", str(n_total))
        .replace("__NOPEN__", str(n_open))
        .replace("__NMETA__", str(n_meta))
        .replace("__GEN__", generated or TODAY)
    )


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
        records.append(rec)

    # Per-dataset pages
    n_pages = 0
    for rec in records:
        path = os.path.join(OUT_DIR, f"{rec['id']}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(render_dataset_page(rec))
        n_pages += 1

    # index.html
    with open(os.path.join(OUT_DIR, "index.html"), "w", encoding="utf-8") as f:
        f.write(render_index(records, generated))

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
