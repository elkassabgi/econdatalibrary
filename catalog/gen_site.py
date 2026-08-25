"""Discoverable static catalog generator.

Reads the central metadata registry (data/catalog.db: source / license / series)
plus the operational sidecar (catalog/catalog.json) and emits, under catalog/site/:

  * <source>.html         one landing page per dataset (registered source), each
                          embedding a VALID schema.org/Dataset JSON-LD block
                          (the thing Google Dataset Search indexes) AND an inline
                          Croissant (schema.org JSON-LD) block.
  * sitemap.xml           every indexable page (datasets + hub/info pages), at the
                          extensionless URL the host actually serves, each with a
                          <lastmod> proven against the previous build's content
                          hash (catalog/sitemap_state.json) rather than the run date.
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

import hashlib
import html
import json
import os
import re
import sqlite3
import unicodedata
from datetime import date, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB_PATH = os.path.join(ROOT, "data", "catalog.db")
CATALOG_JSON = os.path.join(HERE, "catalog.json")
OUT_DIR = os.path.join(HERE, "site")

# Files that live in OUT_DIR but are NOT generated here. They are hand-maintained and
# MUST survive the orphan sweep below. Deleting download.html in particular would break
# every dataset page (its Croissant/schema.org distribution points at
# /download.html?source=<id>), and _redirects carries the Pages redirect rules.
KEEP_UNGENERATED = {
    "_redirects", "download.html", "status.html", "mcp.html", "account.html", "404.html",
}
# Basenames written by this run; anything else *.html in OUT_DIR is a leftover from an
# earlier run and gets removed at the end of main().
_WRITTEN: set[str] = set()

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


# --- The URL the host actually SERVES ----------------------------------------
# WHAT WAS WRONG (measured live 2026-08-01):
#   Every canonical, every og:url, every schema.org `url` and all 207 sitemap
#   <loc> values were of the form https://econdatalibrary.com/<page>.html, but
#   Cloudflare Pages strips the extension and answers those with a 308:
#       GET /fao_ge.html   -> 308 -> https://econdatalibrary.com/fao_ge   (200)
#       GET /docs.html     -> 308 -> https://econdatalibrary.com/docs     (200)
#       GET /index.html    -> 308 -> https://econdatalibrary.com/         (200)
#   So the SITEMAP consisted 207/207 of redirects, and every page's "self"-
#   referencing canonical pointed somewhere other than itself.
# WHY IT MATTERED: a sitemap of redirects is the weakest possible crawl signal
#   (Google follows it, then drops the submitted URL as non-canonical), and a
#   canonical that does not resolve to the URL being indexed hands Google a
#   contradiction to resolve on a site already suffering duplicate-collapse.
#   The one hand-maintained page written most recently (mcp.html) had ALREADY
#   switched to the extensionless form on its own — the generator was the outlier.
def site_url(page: str) -> str:
    """Absolute public URL for a generated page, in the form the host serves 200 for.

    `page` is the output basename ("fao_ge.html", "index.html"). index.html is
    served at the bare origin root, everything else extensionless.
    """
    name = page[:-5] if page.endswith(".html") else page
    return f"{SITE_BASE}/" if name == "index" else f"{SITE_BASE}/{name}"


# --- Honest sitemap <lastmod> -------------------------------------------------
# WHAT WAS WRONG: render_sitemap wrote `TODAY` for the two hub URLs and
#   `r["last_updated"] or TODAY` for every dataset URL. `last_updated` is
#   populated for 3 of 203 sources, so 204 of 207 URLs were stamped with the run
#   date on EVERY run. Today's regeneration produced 202 insertions and 202
#   deletions in sitemap.xml with not one <loc> altered — only the date.
# WHY IT MATTERED: Google's documented behaviour is to ignore lastmod from sites
#   whose lastmod is not "consistently and verifiably accurate". A site-wide date
#   bump on every build is the exact pattern that earns that discount, so the
#   site was spending its own credibility to say nothing.
# THE APPROACH: fingerprint each page's CONTENT and only move its date when the
#   content actually moves.
#   * `_track_page` hashes the finished HTML *before* the volatile generation
#     stamp (__SITE_UPDATED__ -> TODAY) is substituted, so a rebuild on a new day
#     with identical data produces an identical digest.
#   * The digest and the date it was first seen are persisted in
#     catalog/sitemap_state.json (NOT under site/, so it is neither deployed nor
#     swept). A page whose digest matches the stored one keeps its stored
#     lastmod; a page whose digest changed — or that we have never seen — gets
#     TODAY, which is then true.
#   * Hand-maintained pages (download.html, mcp.html, ...) are fingerprinted
#     straight off disk by the same function, so they follow the same rule.
#   NOTE: sitemap_state.json must be COMMITTED for the memory to survive; without
#   it the first build resets every date to its run date (announced in the log).
#   That reset is harmless the first time — this very build rewrites the <title>,
#   meta description, social tags and JSON-LD of every page, so 2026-08-01 is a
#   genuine modification date for all of them.
SITEMAP_STATE = os.path.join(HERE, "sitemap_state.json")

# Matches <meta name="robots" ... content="...noindex..."> in either quote style.
# Used to keep noindex pages OUT of the sitemap (submitting a page you have asked
# Google not to index is a self-contradicting signal) — and, because it reads the
# emitted bytes rather than a hard-coded list, a page that stops being noindex
# joins the sitemap automatically on the next build.
_NOINDEX_RE = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]*content=["\'][^"\']*noindex', re.I)

_LASTMOD: dict[str, dict] = {}       # basename -> {sha256, lastmod, noindex}
_LASTMOD_PREV: dict[str, dict] = {}  # same, loaded from the previous build


def _load_lastmod_state() -> dict:
    try:
        with open(SITEMAP_STATE, encoding="utf-8") as f:
            st = json.load(f)
    except (OSError, ValueError):
        return {}
    pages = st.get("pages") if isinstance(st, dict) else None
    return pages if isinstance(pages, dict) else {}


def _track_page(name: str, body: str) -> dict:
    """Fingerprint one page and decide its sitemap <lastmod>.

    `body` must be the page content WITHOUT the per-run generation stamp
    substituted, otherwise every page would look modified every day.
    """
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    prev = _LASTMOD_PREV.get(name) or {}
    keep = prev.get("sha256") == digest and prev.get("lastmod")
    ent = {
        "sha256": digest,
        "lastmod": prev["lastmod"] if keep else TODAY,
        "noindex": bool(_NOINDEX_RE.search(body)),
    }
    _LASTMOD[name] = ent
    return ent


# Non-dataset pages that belong in the sitemap, with the change cadence each one
# genuinely has. Any entry that is missing from OUT_DIR, or that carries a robots
# noindex, is dropped by main() — that is how account.html / 404.html /
# auth/callback.html stay out without a second exclusion list to keep in sync.
SITEMAP_EXTRA = [
    ("index.html", "daily"),
    ("catalog.html", "daily"),
    ("download.html", "weekly"),
    ("status.html", "daily"),
    ("docs.html", "monthly"),
    ("api.html", "monthly"),
    ("mcp.html", "monthly"),
    ("cite.html", "monthly"),
    ("stats.html", "monthly"),
    ("contact.html", "yearly"),
]

# sameAs spokes. Per-source HF/Zenodo handles are not yet minted, so we emit a
# deterministic *placeholder* slug under the org accounts. Marked as placeholder
# in the visible page; in JSON-LD they are valid absolute URLs (sameAs hints).
HF_ORG = "https://huggingface.co/datasets/econdatalibrary"
ZENODO_COMMUNITY = "https://zenodo.org/communities/econdatalibrary"

# Permanent dataset DOI (Zenodo, mirrors hfdatalibrary's 10.5281/zenodo.19501605
# pattern). EMPTY until the deposit is published under Ahmed's Zenodo account —
# while empty, the cite page renders URL-only citations (no placeholder text).
# The moment the DOI is minted: set it here, regenerate, redeploy.
ZENODO_DOI = "10.5281/zenodo.21405120"  # published 2026-07-17

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
    # Added 2026-07-22 when ssb/stat_slovenia/bfs were repointed to their audit-verified
    # licences (the local ids had gone stale against D1 and the verbatim audit).
    "surs-open": "SURS open data (CC BY-equivalent, attribution required)",
    "opendata-swiss-by": "opendata.swiss — open use, source must be provided",
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
    "defillama-granted": "Written permission (DeFiLlama, 2026) — attribution required, non-commercial",
    "whr-granted": "World Happiness Report (written permission, Figure 2.1 scope)",
    "damodaran-granted": "Written permission (A. Damodaran, 2026) — attribution required, non-commercial",
    "bundesbank-granted": "Bundesbank terms, confirmed in writing (2026) — free of charge, unaltered, exact source credit required",
    "idb-granted": "Written permission (IDB Open Data, 2026) — CC BY 4.0 institutional data; attribution + dataset link-back required",
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

# Dates beyond this are treated as data sentinels (9999-12-31, year 6016, ...) and excluded
# from temporalCoverage so we never publish corrupt coverage.
#
# The ceiling used to be `date.today().year + 2`, which is right for an OBSERVATIONS catalogue
# and wrong for this one: a good part of it is PROJECTIONS, and a forecast horizon is not a
# corrupt date. Measured across catalog.db on 2026-08-02, the two populations separate cleanly
# with nothing in between:
#
#   real forecast horizons   26 sources,  636,021 series -- un_wpp 2101, gapminder 2100,
#                            ksh_stadat 2100, boc 2095, fao_* 2050, imf_weo 2031
#   sentinels                 7 sources,   40,132 series -- 9999, 6152, 6016, 3005, 2150
#
# At `year + 2` every one of those 636,021 series lost its real end date, and whole pages lost
# their coverage row entirely (gapminder rendered none at all, despite holding 86,684 series
# spanning year 730 to 2100). 2126 sits in the empty gap: a century of headroom for genuine
# projections, still far below the lowest sentinel.
MAX_SANE_YEAR = 2126


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
#  Page <title> + meta description  (SEO: survive Google's ~60-char truncation)
# ---------------------------------------------------------------------------- #
# WHAT WAS WRONG (measured live 2026-08-01, before this change):
#   The title was literally `source.name + " — Econ Data Library"`. `source.name`
#   is a producer-first string, so 154 of 203 titles ran past 60 characters with
#   the DISTINGUISHING words at the far end. Truncated to 60, 18 prefixes were
#   shared by 2+ pages and covered 83 pages -- e.g. 13 FAO pages all read
#   "Food and Agriculture Organization of the United Nations (FAO" and 6 IMF GFS
#   pages all read "International Monetary Fund — Government Finance Statistics ".
#   A `site:` query returned 5 results plus "we have omitted some entries very
#   similar to the 5 already displayed": Google's near-duplicate filter was
#   collapsing the catalogue because every result LOOKED identical.
#   The meta description was ~70 characters of licence boilerplate, and 57 pages
#   carried a description byte-identical to another page's (the sidecar covers
#   only 79 of 203 sources, so 124 fell back to `attribution`, a credit line).
#
# WHY IT MATTERED: a collapsed SERP means 198 of 203 dataset pages were
#   effectively unfindable, however good the data behind them is.
#
# THE RULE HERE: every character emitted below is a VERBATIM registry string
#   (source.name fragment, the derived/curated subtitle, the licence label) or a
#   number counted from the registry (series count, coverage years). Nothing is
#   estimated, rounded, inferred from a sibling source, or carried over. A field
#   that is missing produces NO clause -- never a guess. That is why country
#   counts and update frequencies are absent: `geography` is empty on 173 of 203
#   sources and `frequency` on 171, so any "N countries / updated monthly" clause
#   would be fabricated for the large majority of pages.
# ---------------------------------------------------------------------------- #
NAME_SPLIT = re.compile(r"\s+(?:—|--|–)\s+|\s+-\s+")
_GLOSS = re.compile(r"\s*\(([^()]*)\)")
_CODE_TAIL = re.compile(r"\(code:\s*[^)]+\)\s*$", re.I)
_JUNK_CLAUSE = re.compile(r"^[\d\s.,;:()–—-]+$")
_RAW_LICENSE_TOKEN = re.compile(r"^[a-z0-9][a-z0-9._+-]*$")
_YEARISH = re.compile(r"^[\d\s./–—-]+$")

PRODUCER_FULL_MAX = 42   # longer than this -> prefer the registry's own acronym
LEAD_MIN_USEFUL = 24     # a shorter dataset phrase cannot carry a title
LEAD_BUDGET = 120        # a dataset phrase from `name` is one coherent title
SUBTITLE_BUDGET = 78     # a subtitle is ';'-joined stems: extra clauses add no
                         # distinguishing power, only length
TITLE_PREFIX_N = 60      # roughly what Google shows / de-duplicates on
DESC_MAX = 160           # hard cap on the rendered meta description


def _fold(ch):
    """Accent-folded uppercase, so 'études économiques' still supplies E for INSEE."""
    d = unicodedata.normalize("NFD", ch)
    return "".join(c for c in d if not unicodedata.combining(c)).upper()


def _short_token(t):
    return bool(t) and " " not in t and len(t) <= 12 and re.fullmatch(r"[A-Za-z][A-Za-z0-9./&+-]*", t)


def _strip_glosses(seg):
    """Drop long parenthetical glosses (native-language names, expansions) but KEEP
    short acronym parentheticals -- '(UNCTAD)' is a search keyword, '(Statistisk
    sentralbyrå, SSB)' is noise in a title."""
    def repl(m):
        inner = m.group(1).strip()
        return f" ({inner})" if _short_token(inner) else ""
    return re.sub(r"\s{2,}", " ", _GLOSS.sub(repl, seg or "")).strip(" ,;-—–")


def acronym_of(seg):
    """Return a parenthetical acronym ONLY if its letters are, in order, the initials
    of the words preceding it -- i.e. the catalogue itself asserts the abbreviation.
    'Food and Agriculture Organization of the United Nations (FAO)' -> FAO;
    'International Monetary Fund — Consumer Price Index (CPI)' -> None for the
    producer segment (CPI abbreviates the dataset, not the producer);
    'UN Trade and Development (UNCTAD)' -> None (UNCTAD is not the initials of that
    shortened form) so the caller keeps the full producer string.
    NEVER invents an abbreviation: it fails safe to None."""
    for m in re.finditer(r"\(([^()]{2,12})\)", seg or ""):
        cand = m.group(1).strip()
        if not _short_token(cand) or not cand[0].isupper():
            continue
        letters = [_fold(c) for c in cand if c.isalpha()]
        if len(letters) < 2:
            continue
        initials = [_fold(w[0]) for w in re.findall(r"[^\W\d_]+", seg[:m.start()], re.UNICODE)]
        i = 0
        for ini in initials:
            if i < len(letters) and ini == letters[i]:
                i += 1
        if i == len(letters):
            return cand
    return None


def split_name(name):
    """(producer_display, dataset_phrase, producer_long_form) from `source.name`.

    `producer_display` is the acronym for long producer names; `producer_long_form`
    is the spelled-out name minus the acronym parenthetical, used when the lead
    already contains the acronym (a title leading with 'GISTEMP (GISS Surface
    Temperature Analysis)' must still say NASA rather than drop the producer)."""
    name = " ".join((name or "").split())
    m = NAME_SPLIT.search(name)
    if m:
        producer_raw, dataset = name[:m.start()].strip(), name[m.end():].strip()
    else:
        producer_raw, dataset = name, ""

    full = _strip_glosses(producer_raw) or producer_raw
    acr = acronym_of(producer_raw)
    long_form = (re.sub(r"\s*\(" + re.escape(acr) + r"\)", "", full).strip(" ,;-—–")
                 if acr else full) or full
    producer = acr if (acr and len(full) > PRODUCER_FULL_MAX) else full

    # Shape "<Database> -- <Dataset ...>, <Producing organisation (ACR)>" (the 7
    # FAOSTAT ASTI/climate rows): the producing body is repeated as the LAST comma
    # clause. Move it out of the dataset phrase so the title leads with the dataset.
    if dataset and ", " in dataset:
        head, _, tail = dataset.rpartition(", ")
        org = acronym_of(tail)
        if org and len(tail) >= 20 and len(head) >= LEAD_MIN_USEFUL:
            dataset = head.strip()
            if org.lower() not in producer.lower():
                producer = f"{producer} ({org})"
                long_form = producer
    return producer, dataset, long_form


def _balance(t):
    """Never leave a title/description with an unclosed '(' after truncation."""
    while t.count("(") > t.count(")"):
        i = t.rfind("(")
        if i <= 0:
            return t
        t = t[:i].rstrip(" ,;:-—–")
    return t


_TRIM_SPLITS = ("; ", " — ", " -- ", ": ", ", ")


def trim_phrase(text, budget, tolerance=1.15):
    """Cut at a clause boundary where possible, else at a word boundary. A boundary
    cut is rejected if it would leave a stub ('06307' out of '06307: Forest
    properties, by size class'). Title leads get a 15% tolerance -- truncating a
    phrase one or two characters over budget costs more (an ugly mid-word ellipsis)
    than the length saves; the description passes tolerance=1.0 because its budget
    is a hard cap on the rendered tag."""
    t = " ".join((text or "").split())
    if len(t) <= budget * tolerance:
        return t
    floor = max(24, int(budget * 0.35))
    for sep in _TRIM_SPLITS:
        idx, best = t.find(sep), -1
        while idx != -1 and idx <= budget:
            best, idx = idx, t.find(sep, idx + len(sep))
        if best > 0:
            cand = _balance(t[:best].rstrip(" ,;:-—–"))
            if len(cand) >= floor:
                return cand
    cut = _balance(t[:budget].rsplit(" ", 1)[0].rstrip(" ,;:-—–"))
    return (cut or t[:budget].rstrip()) + "…"


def _drop_junk_clauses(text):
    """Derived subtitles occasionally open on a bare year or numeric code
    ('2014; 06307: Forest properties...'); do not open a title on a meaningless
    number when a real clause follows."""
    if "; " not in (text or ""):
        return text
    parts = text.split("; ")
    while len(parts) > 1 and _JUNK_CLAUSE.match(parts[0] or ""):
        parts.pop(0)
    return "; ".join(parts)


def _is_junk(text):
    t = (text or "").strip()
    return not t or bool(_JUNK_CLAUSE.match(t))


def _codey(text):
    """First clause is a raw machine identifier ('ETR:Demographic_Pressure:Albania')."""
    first = (text or "").split("; ")[0]
    return bool(first) and " " not in first and sum(first.count(c) for c in ":_") >= 2


def _good_dataset_lead(dataset, shared=frozenset()):
    """Reject dataset phrases that cannot carry a title:
      - too short, or opening on a lowercase filing word
        ('domain QCL (Crops and livestock products)');
      - a generic family label whose only specific content is a machine code
        ('Foreign direct investment dataset (code: FMCPA)');
      - a label shared by other published sources -- 11 UNCTAD rows carry the very
        same 'UNCTAD Data Hub (UNCTADstat)', which by definition differentiates
        nothing and is precisely what Google collapsed."""
    return bool(dataset) and (len(dataset) >= LEAD_MIN_USEFUL
                              and not dataset[:1].islower()
                              and not _CODE_TAIL.search(dataset)
                              and dataset not in shared)


def shared_dataset_labels(records):
    from collections import Counter
    c = Counter()
    for r in records:
        ds = split_name(r["name"])[1]
        if ds:
            c[ds] += 1
    return {ds for ds, n in c.items() if n > 1}


def _lead_candidates(rec, shared):
    """(producer, producer_long, [(lead_text, budget), ...]). Every candidate is a
    verbatim registry string, in descending order of how well it identifies THIS
    dataset: the dataset phrase from `name`, then the subtitle (unique across all
    203 published sources), then the raw name."""
    producer, dataset, long_form = split_name(rec["name"])
    sub = _drop_junk_clauses(rec.get("subtitle") or "")
    out, seen = [], set()

    def add(text, budget):
        if text and text not in seen and not _is_junk(text):
            seen.add(text)
            out.append((text, budget))

    if _good_dataset_lead(dataset, shared):
        add(dataset, LEAD_BUDGET)
    add(sub, SUBTITLE_BUDGET)
    add(dataset, LEAD_BUDGET)
    add(rec["name"], LEAD_BUDGET)
    if not out:
        out.append((rec["name"], LEAD_BUDGET))
    return producer, long_form, out


def _mentions(hay, needle):
    """Word-boundary containment. Substring matching would suppress the producer
    'FAO' on every title that merely contains 'FAOSTAT'."""
    if not needle:
        return True
    return re.search(r"(?<![\w])" + re.escape(needle) + r"(?![\w])", hay or "", re.I) is not None


def short_license(label):
    """A snippet-sized licence phrase taken VERBATIM from the label itself: either its
    compact parenthetical ('CC BY 4.0') or the text before it. Never maps, infers or
    upgrades a licence -- it only shortens the label the registry already carries.
    Returns None when the registry still holds a raw internal id (28 of 203 sources
    carry ids like 'cc-by-sa-4.0-unesco_sdg'), so a machine token can never leak into
    a public snippet; those pages simply get no licence clause."""
    lab = " ".join((label or "").split())
    if not lab or _RAW_LICENSE_TOKEN.match(lab):
        return None
    m = re.search(r"\(([^()]{2,14})\)", lab)
    if m and not _YEARISH.match(m.group(1).strip()):
        return m.group(1).strip()
    head = lab.split("(", 1)[0].strip(" ,;-—–")
    return head if len(head) >= 8 else lab


def _coverage_clause(rec):
    """'1961–2024' when both ends are known, 'from 1961' when only the start is.
    OMITTED entirely when neither is -- 45 of 203 sources have no usable end date
    and 21 no usable start, and an invented range would be the worst kind of error
    on an academic catalogue."""
    s, e = rec.get("cov_start"), rec.get("cov_end")
    if s and e:
        return f"{s[:4]}–{e[:4]}" if s[:4] != e[:4] else s[:4]
    return f"from {s[:4]}" if s else None


def build_meta_desc(rec, shared=frozenset()):
    """~150 characters, unique per page, assembled from real fields only:
    what the series actually measure, the producer, the counted series total,
    the measured coverage years, and the licence -- each clause dropped, not
    guessed, when its field is missing."""
    producer, dataset, _long = split_name(rec["name"])
    sub = _drop_junk_clauses(rec.get("subtitle") or "")
    # Prefer the subtitle (derived from the source's OWN series titles, unique
    # across all 203); fall back to the registry dataset phrase when the subtitle
    # is a raw identifier dump or empty.
    what = dataset if (_codey(sub) and _good_dataset_lead(dataset, shared)) else sub
    if _is_junk(what):
        what = (dataset if not _is_junk(dataset) else "") or rec["name"]

    facts = []
    if rec.get("n_series"):
        facts.append(f"{rec['n_series']:,} series")
    cov = _coverage_clause(rec)
    if cov:
        facts.append(cov)
    lic = short_license(rec.get("license_label"))
    access = "Free download" if rec.get("reservable") else "Catalogued metadata"

    def assemble(head, with_lic):
        bits = []
        if producer and not _mentions(head, producer):
            bits.append(producer)
        if facts:
            bits.append(", ".join(facts))
        bits.append(access + (f" under {lic}" if (with_lic and lic) else ""))
        sep = "" if head.endswith("…") else "."
        return f"{head}{sep} " + ". ".join(bits) + "."

    full = " ".join((what or "").split())
    for with_lic in (True, False):          # substance first: the licence clause is
        out = assemble(full, with_lic)      # the first thing dropped when space is tight
        if len(out) <= DESC_MAX:
            return out
    room = DESC_MAX - len(assemble("", False))
    return assemble(trim_phrase(full, max(room, 40), tolerance=1.0), False)


def build_page_seo(records):
    """{source_id: (title, meta_description)} for every published record.

    Titles lead with what makes THIS page different, then the producer, then the
    site, so the distinguishing words land inside Google's ~60-character budget.
    A final pass GUARANTEES that no two pages share their first 60 characters:
    colliding pages first widen their trim, then fall through to the next honest
    candidate (the unique subtitle), and only as an unreachable last resort append
    their registry id -- which prints a warning if it ever fires."""
    from collections import defaultdict
    suffix = f" — {SITE_NAME}"
    shared = shared_dataset_labels(records)
    state = {}
    for r in records:
        producer, long_form, cands = _lead_candidates(r, shared)
        state[r["id"]] = {"prod": producer, "long": long_form, "cands": cands,
                          "ci": 0, "extra": 0, "id_fb": False}

    def title_for(r):
        st = state[r["id"]]
        text, budget = st["cands"][st["ci"]]
        lead = trim_phrase(text, budget + st["extra"])
        for prod in (st["prod"], st["long"]):
            if prod and not _mentions(lead, prod):
                return f"{lead} — {prod}{suffix}"
        return f"{lead}{suffix}"

    titles = {}
    for _ in range(16):
        titles = {r["id"]: title_for(r) for r in records}
        groups = defaultdict(list)
        for sid, t in titles.items():
            groups[t[:TITLE_PREFIX_N]].append(sid)
        clashes = [g for g in groups.values() if len(g) > 1]
        if not clashes:
            break
        for g in clashes:
            for sid in sorted(g)[1:]:        # deterministic: keep the first id, move the rest
                st = state[sid]
                if st["extra"] < 120:
                    st["extra"] += 40
                elif st["ci"] + 1 < len(st["cands"]):
                    st["ci"] += 1
                    st["extra"] = 0
                elif not st["id_fb"]:
                    st["id_fb"] = True
                    st["cands"].append((f'{st["cands"][st["ci"]][0]} ({sid})', 240))
                    st["ci"] += 1
    else:
        titles = {r["id"]: title_for(r) for r in records}

    dupes = defaultdict(list)
    for sid, t in titles.items():
        dupes[t[:TITLE_PREFIX_N]].append(sid)
    still = {k: v for k, v in dupes.items() if len(v) > 1}
    if still:
        print(f"  WARNING: {len(still)} shared {TITLE_PREFIX_N}-char title prefix(es): "
              + "; ".join(f"{k!r} -> {v}" for k, v in list(still.items())[:5]))
    if any(st["id_fb"] for st in state.values()):
        print("  WARNING: title fell back to the source id for "
              + ", ".join(s for s, st in state.items() if st["id_fb"]))

    return {r["id"]: (titles[r["id"]], build_meta_desc(r, shared)) for r in records}


# Filled once per run by main() (a title must be unique across the WHOLE catalogue,
# so it cannot be computed from a single record in isolation). render_dataset_page
# falls back to a self-contained build when a record is not in the map, so the
# renderer still works if called on its own.
PAGE_SEO: dict = {}


# ---------------------------------------------------------------------------- #
#  Load the registry + sidecar
# ---------------------------------------------------------------------------- #
_FREQ_WORDS = {"annually", "annual", "monthly", "quarterly", "daily", "weekly"}


# ---------------------------------------------------------------------------- #
#  schema.org/variableMeasured source data
# ---------------------------------------------------------------------------- #
# The registry has no "variable" column. What it does have is the series TITLE,
# whose leading segment carries the measured quantity (and, where the producer
# supplies one, its unit): 'Revenue (% of GDP)', 'WTI crude oil spot price,
# Cushing OK ($/bbl)', 'Gini index of disposable (post-tax, post-transfer) income'.
#
# THE RULE: publish the stems only when the COMPLETE distinct set is small enough
# to enumerate. A "top 12 of 1,486" list would be an arbitrary sample presented as
# the dataset's variables -- true strings, false claim. So a source either gets its
# whole, verbatim variable list or gets no variableMeasured at all. Measured
# 2026-08-01 over the 205 published sources: 60 pass the enumerability test, 59
# emit the field (noaa is denied below), 146 emit nothing. main() prints the count
# every run, so a change in that ratio shows up in the build log.
MEASURES_MAX = 12

# Sources whose leading title segment is an ENTITY, not a measured variable. Read
# and verified by hand 2026-08-01 against the store's own titles. noaa's stems are
# weather STATIONS ("New York (Central Park)", "Chicago O'Hare"): publishing them
# would assert that the dataset measures "Chicago O'Hare", which is false, and a
# wrong typed claim in structured data is worse than an absent field. No structural
# test separates this from a source whose titles genuinely ARE variable names
# (bls: "Unemployment rate, 16+ (SA, %)"), so the exclusion is explicit + reviewed
# rather than heuristic. Add an id here after reading that source's series titles.
MEASURES_DENY = {"noaa"}

_MEASURE_CODEY = re.compile(r"^[A-Z0-9:_.,/\-]+$")


def _measure_stems(sid, heads):
    """The complete distinct set of indicator stems for one source, verbatim from
    its own series titles -- or [] when that set is not safe to publish. Fails
    closed: ONE unusable stem drops the whole list, because a partial list would
    misrepresent what the dataset measures."""
    if sid in MEASURES_DENY:
        return []
    stems = list(dict.fromkeys(h for h in heads if h))
    if not stems or len(stems) > MEASURES_MAX:
        return []
    for h in stems:
        # " " not in h and the all-caps/punctuation test reject raw machine keys
        # ("ADB:EGELC:AOMPC_BBL", "GRAVITY:col_dep") that 28 sources carry instead
        # of human titles; _is_junk rejects bare numbers and codes.
        if (_is_junk(h) or _codey(h) or " " not in h
                or not (3 <= len(h) <= 120) or _MEASURE_CODEY.match(h)):
            return []
    return stems


def _derive_subtitles(con):
    """A short human subtitle per source, derived from its own series titles.

    Whole facet families share one source name (38 UNCTAD tables, 30 IMF
    datasets, 25 FAOSTAT domains, 3 WHO themes...), so cards were identical
    and only the source id disambiguated (owner-reported). Per source: split
    titles on dashes, drop frequency words and high-cardinality segments
    (geographies/entities), and show the top remaining indicator stems.
    Derived from the FULL registry, never invented; curated overrides in
    SOURCE_SUBTITLES win where hand wording is better.

    Returns (subtitles, measures). `measures` reuses the SAME already-parsed
    titles to build the schema.org/variableMeasured candidate list (see
    _measure_stems), so adding it costs no extra pass over the 6.2M series rows.
    """
    from collections import Counter, defaultdict
    per = defaultdict(list)
    for sid, title in con.execute("SELECT source_id, title FROM series"):
        t = re.sub(r"\s+[–—]\s+", " - ", (title or "").strip())
        t = re.sub(r"\s*\((?:Annually|Monthly|Quarterly|Daily|Weekly)\)\s*$", "", t, flags=re.I)
        per[sid].append([p.strip() for p in t.split(" - ") if p.strip()])
    subs = {}
    for sid, rows in per.items():
        n = len(rows)
        if n <= 40:
            c = Counter(" - ".join(r) for r in rows)
            top = [s for s, _ in c.most_common(3)]
        else:
            width = max(len(r) for r in rows)
            keep = []
            for i in range(width):
                vals = Counter(r[i] for r in rows if len(r) > i)
                if not vals:
                    continue
                low_card = len(vals) <= 30
                freqish = all(v.lower() in _FREQ_WORDS for v in vals)
                if low_card and not freqish:
                    keep.append(i)
                if len(keep) >= 2:
                    break
            stems = Counter()
            for r in rows:
                s = " - ".join(r[i] for i in keep if len(r) > i)
                stems[s or r[0]] += 1
            top = [s for s, _ in stems.most_common(3) if s]
        out = "; ".join(dict.fromkeys(t for t in top if t))
        if len(out) > 140:
            out = out[:137] + "…"
        subs[sid] = out
    measures = {sid: _measure_stems(sid, [r[0] for r in rows if r])
                for sid, rows in per.items()}
    return subs, measures


# Hand-curated subtitles wherever the derived stems read poorly (raw codes,
# ids, or measure fragments with no dataset identity). Wording is checked
# against the store's actual series — never invented coverage. Full-catalog
# audit 2026-07-15; the derived fallback remains for sources whose own series
# titles already read well (BLS, EIA, Maddison, most FAO domains, ...).
SOURCE_SUBTITLES = {
    # --- Added 2026-07-31 ------------------------------------------------------------
    # These 16 sources carry NO human series titles at all: their `title` column holds the
    # raw key, so the derived fallback could only emit code dumps like
    # "ADB:EGELC:AOMPC_BBL:AUS; ADB:EGELC:AOMPC_BBL:AZE" — published on the catalog cards and,
    # once the Status board grew descriptions, about to be published there too.
    # Every phrase below is read off something observable: the catalogued source name, the
    # homepage, or the dimension codes in the source's own keys (cited after each entry).
    # Nothing here asserts coverage or a date range that was not measured.
    "adb": "Key Indicators for Asia and the Pacific — macroeconomic, national accounts, energy and SDG indicators for ADB member economies",
    # keys: ADB:SDG:*, ADB:EO_NA_CONST:NSDGDP_R_XDC (national accounts), ADB:EGELC (energy)
    "cepii_gravity": "Gravity-model variables for every country pair — colonial ties, common language, contiguity, distance and trade-agreement membership",
    # keys: GRAVITY:col_dep:*, col_dep_ever, wto_d, over ordered country pairs (ABW:AFG ...)
    "fao_et": "Temperature change on land — surface temperature anomalies and their standard deviations by country and month",
    # titles: "Standard Deviation - Armenia"; measured coverage 1961-01-01 .. 2023-12-01
    "gapminder": "Long-run harmonised development indicators by country — health, demography, employment and economy",
    # keys: adults_with_hiv_percent_age_15_49, pneumonia_deaths_in_children_1_59_months, self_employed_percent_of_employment
    "harvard_atlas": "Atlas of Economic Complexity — economic complexity indices plus product-level trade values and global market shares",
    # keys: ATLAS:ECI:eci_hs12, ATLAS:HS12:import_value, ATLAS:HS12:global_market_share
    "imf_afrreo_direct": "Regional Economic Outlook, Sub-Saharan Africa — current account, fiscal balances and public debt as ratios to GDP",
    # keys: BCA_GDP_BP6, GGXWDG_GDP, DG_GDP, BXGS_GDP_BP6
    "imf_apdreo_direct": "Regional Economic Outlook, Asia and Pacific — real growth, inflation, unemployment and fiscal balances by economy",
    # keys: NGDP_RPCH, PCPIE_PCH, LUR.ILO, GGXCNL_GDP
    "imf_cofer_direct": "Currency composition of official foreign exchange reserves — holdings per reserve currency, in US dollars and as shares of allocated reserves",
    # keys: CI_USD/CI_AUD/CI_CAD/CI_CHF with NV_USD (value) and SHRO_PT (share)
    "imf_fas_direct": "Financial Access Survey — supply-side financial-inclusion indicators: branches, ATMs and accounts per adult and per unit area",
    # keys: COMBANK, PHTADLT_NUM (per 100k adults), PTKM2_NUM (per 1,000 km2), PTADLT_NUM
    "imf_fdi_direct": "Financial Development Index — composite indices for financial institutions and markets across depth, access and efficiency",
    # keys: FD_IX, FDA_IX, FDD_IX, FDE_IX, FI_IX, FMA_IX
    "imf_fsi": "Financial Soundness Indicators — banking-sector capital adequacy, asset quality, earnings and liquidity, at annual, quarterly and monthly frequency",
    # keys: FSANL_PT, FSCR_PT, FSCET_PT; A/Q/M frequencies all present in the store
    "imf_whdreo_direct": "Regional Economic Outlook, Western Hemisphere — growth, inflation, fiscal balances and public debt by economy",
    # keys: NGDP_RPCH, PCPIE_PCH, GGXGGEI_GDP, GGXWDG_GDP, BCA_GDP_BP6
    "imf_world_direct": "World Revenue Longitudinal Database — government revenue by category as a share of GDP",
    # keys: G1111/G1112/G113/G13 revenue codes with POGDP_PT (percent of GDP)
    "unesco_natmon": "UIS national monitoring — education indicators reported by national authorities, by country and year",
    # opaque numeric indicator codes (UNESCO_NATMON:10.NA.ABW.A); kept general deliberately
    "unesco_sdg": "UIS SDG 4 education indicators — completion, attendance and learning assessment, with equity breakdowns by country",
    # keys: CR.1 (completion rate), GAR.5T8 (gross attendance ratio), ADMI.ENDOFLOWERSEC.MAT
    "wid": "World Inequality Database — distributional national accounts: income and wealth shares, averages and thresholds by percentile group",
    # keys: acainc* (average), sdiinc* (share), tdiinc* (threshold) over p0p100, p60p70, p0p71
    # --- end 2026-07-31 additions ---------------------------------------------------
    # WHO / health
    "who_hwf": "Health workforce — medical doctors, nurses, midwives and other health workers per 10,000 population",
    "who_rs": "Road safety — traffic mortality, registered vehicles, and related population denominators",
    "who_sdg": "Health-related SDG indicators — mortality and death rates, disease burden, pollution-attributable deaths",
    # majors
    "eurostat": "European official statistics — economy, prices, population, industry, transport, environment",
    "bundesbank": "Exchange rates, interest rates, money and banking, and macroeconomic time series for Germany and the euro area",
    "worldbank_wdi": "World Development Indicators — economy, people, environment and infrastructure for all countries",
    "comtrade": "Merchandise trade — total imports and exports by reporter country, plus bilateral totals for major partner pairs (HS, all commodities)",
    "damodaran": "Valuation datasets — equity risk premiums, betas, margins, costs of capital by industry",
    "social_progress": "Social Progress Index — official interactive embed (dataset not redistributed, by written permission)",
    "bea": "U.S. national accounts (NIPA) — GDP components, trade in goods and services, quarterly and annual",
    "bis": "Central bank policy rates — end-of-period rates across central banks (BIS)",
    "boc": "Canadian key series — consumer prices, core CPI, and the USD/CAD exchange rate (Valet API)",
    "bcrp": "Peruvian macro-financial series — exchange rates, interest rates and prices (BCRPData)",
    "cboe": "Cboe volatility indices — VIX-family gauges for currencies, gold and equity ETFs",
    "cow": "Interstate alliance counts by country, from the Correlates of War alliance data",
    "ipea": "Brazilian macroeconomic series — exports, imports and GDP (Ipeadata)",
    "ksh": "Hungarian official statistics — external trade, enterprises, national accounts (STADAT)",
    "nbp": "Narodowy Bank Polski — złoty reference exchange rates for world currencies",
    "tcmb": "Turkish lira exchange rates — buying/selling rates for world currencies (EVDS)",
    "ofr": "U.S. secured-funding and repo reference rates — broad general collateral rate and percentiles",
    "oxcgrt": "COVID-19 government responses — school and workplace closures, containment and health policies",
    "polity": "Polity5 — regime authority scores on the autocracy–democracy scale",
    "rba": "Australian zero-coupon yield curves — discount factors, forward rates and yields (RBA)",
    "sec_edgar": "Company financial fundamentals extracted from SEC EDGAR filings",
    "snb": "Swiss National Bank data portal — banking, custody holdings and monetary statistics",
    "wikidata": "Company reference data from Wikidata — identifiers and attributes of listed firms",
    "unhcr": "Forced displacement — refugees, asylum-seekers and returnees by country (UNHCR)",
    "usda": "U.S. agricultural statistics — crop production and values by commodity (USDA NASS)",
    "whr": "World Happiness Report — self-reported life satisfaction (Figure 2.1 scope)",
    # IMF datasets (official dataset names)
    "imf_weo": "World Economic Outlook — GDP, prices, fiscal and external balances across economies and country groups",
    "imf_cofer": "Currency composition of official foreign exchange reserves (COFER)",
    "imf_afrreo": "Regional Economic Outlook: Sub-Saharan Africa — growth, fiscal and external balances",
    "imf_apdreo": "Regional Economic Outlook: Asia & Pacific — current accounts and fiscal balances",
    "imf_mcdreo": "Regional Economic Outlook: Middle East & Central Asia — current accounts and fiscal balances",
    "imf_whdreo": "Regional Economic Outlook: Western Hemisphere — fiscal balances and net lending/borrowing",
    "imf_bopagg": "Balance of payments — current account aggregates (BPM6), by country",
    "imf_commodity": "Primary commodity prices — global price indices for energy, food, metals and raw materials",
    "imf_cpi": "Consumer price indices — all items and percentage changes, by country",
    "imf_fas": "Financial Access Survey — ATMs, bank branches and account ownership by country",
    "imf_fdi": "Financial Development Index — depth, access and efficiency of institutions and markets",
    "imf_fiscaldecentralization": "Fiscal decentralization — subnational government shares of revenue and expenditure",
    "imf_fm": "Fiscal Monitor — revenue, expenditure and net lending/borrowing (% of GDP)",
    "imf_fsire": "Financial Soundness Indicators — reporting-entity coverage by sector",
    "imf_gender_budgeting": "Gender budgeting — budget documentation practices by country",
    "imf_gender_equality": "Gender equality — gender development and inequality indices",
    "imf_gfscofog": "Government Finance Statistics — expenditure by function of government (COFOG)",
    "imf_gfse": "Government Finance Statistics — expense by economic type",
    "imf_gfsfalcs": "Government Finance Statistics — financial assets and liabilities by counterpart sector",
    "imf_gfsibs": "Government Finance Statistics — integrated balance sheet",
    "imf_gfsmab": "Government Finance Statistics — main aggregates and balances (revenue, tax, contributions)",
    "imf_gfsssuc": "Government Finance Statistics — statement of sources and uses of cash",
    "imf_hpdd": "Historical Public Debt Database — debt-to-GDP ratios over the long run",
    "imf_namain_idc_n": "National accounts (SNA 2008) — GDP at market prices, current prices",
    "imf_pctot": "Commodity terms of trade — import and export price indices by country",
    "imf_pgcs": "Investment and capital stock — public and private capital (% of GDP)",
    "imf_pgi": "Principal Global Indicators — trade values, exchange rates and key macro series",
    "imf_psbsfad": "Public sector balance sheet — assets and liabilities by instrument",
    "imf_unsdg_imf_inputs": "IMF inputs to the UN SDG indicators — ATMs and bank branches per 100,000 adults",
    "imf_world": "World Revenue Longitudinal Data (WoRLD) — tax and revenue shares of GDP",
    # UNCTADstat tables (named from their own series content)
    "unctad_bopcaba": "Balance of payments — current account balance, annual",
    "unctad_ciocgeaia": "Creative goods trade — concentration indices for exports and imports",
    "unctad_cioiuibbicoeair4a": "ICT use by businesses — computers, internet and networks (core indicators)",
    "unctad_cpa": "Commodity prices — individual products (bananas, beef, …) in physical markets, annual",
    "unctad_cpia": "Consumer price indices — all items, annual average growth rates",
    "unctad_cpta": "Container port throughput — TEU handled by economy, annual",
    "unctad_fdiiaofasa": "Foreign direct investment — inward and outward stocks and flows, annual",
    "unctad_fmcpa": "Free-market commodity prices — individual products, annual",
    "unctad_fmcpia21": "Free-market commodity price indices (2015=100) — food, agricultural raw materials, all groups",
    "unctad_gasbeaiogasa": "Trade in goods — balance, exports and imports, seasonally adjusted",
    "unctad_gasbtbia": "Trade balance — goods, and goods and services, annual",
    "unctad_gasbtoia": "Total trade in goods and services — balance, exports, imports, annual",
    "unctad_gdpgbtoevbkoeatasa": "GDP by expenditure — consumption, construction, exports at constant prices",
    "unctad_gdptapccac2pa": "GDP — total and per capita, current and constant (2015) US dollars",
    "unctad_lscia": "Liner Shipping Connectivity Index — annual index and country rankings",
    "unctad_lsciq": "Liner Shipping Connectivity Index — quarterly index and growth rates",
    "unctad_mfbcoboa": "Merchant fleet by country of beneficial ownership, annual",
    "unctad_mmcascioeaiopa": "Merchandise trade structural change indices — exports and imports",
    "unctad_mpcadioeaia": "Merchandise product diversification — number of products exported and imported",
    "unctad_mtba": "Merchandise trade balance, annual",
    "unctad_mttasa": "Merchandise trade — exports and imports, seasonally adjusted",
    "unctad_mttgra": "Merchandise trade — growth rates of exports and imports, annual",
    "unctad_neera": "Nominal effective exchange rate indices, annual",
    "unctad_reericba": "Real effective exchange rate indices (consumer-price based), annual",
    "unctad_reerigdba": "Real effective exchange rate indices (GDP-deflator based), annual",
    "unctad_rfia": "Revealed factor intensities — exporter counts, human and physical capital indices",
    "unctad_rgdptapcgra": "Real GDP — annual growth rates, total and per capita",
    "unctad_sbeaiotsvsaga": "Services trade — exports and imports incl. commercial services, seasonally adjusted",
    "unctad_sbtisvsaga": "Services trade — balance, exports and imports incl. commercial services",
    "unctad_soigapotta": "ICT goods trade — imports, exports and re-exports as share of total trade",
    "unctad_sotwmfvbcoboa": "Share of world merchant fleet value by country of beneficial ownership",
    "unctad_srbca": "Merchant fleet registrations — gross tonnage by flag of registration",
    "unctad_tabbapotta": "Biotrade — biodiversity-based exports and imports as share of total trade",
    "unctad_tabmcioeaiopa": "Merchandise trade market concentration indices — exports and imports",
    "unctad_tabmscioeaiopa": "Merchandise trade structural change indices — exports and imports, annual",
    "unctad_tabpcioeaia": "Merchandise trade product concentration indices — exports and imports",
    "unctad_taupa": "Total and urban population — absolute and urban share, annual",
    "unctad_wstbtocabgoea": "World seaborne trade — crude oil and dry cargo loaded and unloaded",
}


def load_registry():
    # READ-ONLY, and wait for the writers (2026-08-25). The site generator never writes
    # this database - it has no INSERT/UPDATE/DELETE and no commit() - but it opened it
    # read-write with no busy timeout, so a regen during a live crawl died with
    # "database is locked" and three SERVED sources (vdem, cbs_nl, gus_dbw) had no landing
    # page at all while the API served 783,100 / 5,154 / 194 series. The fleet writes
    # continuously by design, so "run it when nothing is writing" is not a real window.
    # mode=ro also makes it impossible for a page build to corrupt the catalogue.
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
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

    # Recover a true start year that sane_date() would otherwise throw away wholesale.
    #
    # This is NOT a fix for a publication bug -- sane_date has rejected years below 1000 and
    # above MAX_SANE_YEAR since the initial release, so no impossible range has ever reached a
    # page. What it rejects, though, is the whole VALUE: a source whose MIN(start_date) is a
    # single corrupt row loses its coverage clause entirely, including the real coverage sitting
    # one row behind the bad one.
    #
    # Measured 2026-08-02: scb reports 0114, then 1749, 1800, 1851 -- only 5 of its 2,550 series
    # carry the impossible year, and Sweden's statistics office really does have data from 1749.
    # stat_slovenia is the same shape: 0001 on 23 rows of 4,134, then 1857. Both published no
    # coverage at all. maddison and ggdc must NOT be touched by this: their year-1 starts are
    # genuine, corroborated by 0730, 1000, 1300 standing right behind them.
    #
    # So the test is CORROBORATION, not a cutoff. An ancient start is trusted when other ancient
    # values stand near it, and discarded as parse damage when it stands alone -- in which case
    # the source falls back to its first plausible year instead of losing the clause. Effect,
    # measured by regenerating the whole site both ways: exactly 2 pages change, both gaining a
    # true fact. Every other page is byte-identical.
    for sid, roll in series_roll.items():
        try:
            first = int(str(roll.get("min_start"))[:4])
        except (TypeError, ValueError):
            continue
        if first >= 1000:
            continue
        years = [int(y[0]) for y in con.execute(
            "SELECT DISTINCT substr(start_date,1,4) y FROM series "
            "WHERE source_id=? AND start_date IS NOT NULL ORDER BY y LIMIT 4", (sid,))
            if y[0] and y[0].isdigit()]
        if sum(1 for y in years if y < 1500) >= 3:
            continue                      # genuinely ancient coverage (maddison, ggdc): keep it
        plausible = [y for y in years if y >= 1000]
        roll["min_start"] = f"{plausible[0]:04d}-01-01" if plausible else None


    _subs, _measures = _derive_subtitles(con)
    for sid, s in _subs.items():
        if sid in series_roll:
            series_roll[sid]["auto_subtitle"] = s
    # schema.org/variableMeasured candidates (complete enumerations only -- see
    # _measure_stems); an empty list simply means the field is omitted downstream.
    for sid, m in _measures.items():
        if sid in series_roll:
            series_roll[sid]["measures"] = m

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
#  Is this database actually on the automated refresh yet?
# ---------------------------------------------------------------------------- #
_FETCHER_BACKED = {"extend_by_date", "overwrite_if_changed", "sdmx_delta",
                   "manual_vintage", "bulk_snapshot_if_changed"}


def load_wiring():
    """{source_id: True/False} — is this database ACTUALLY refreshed automatically today?

    Read straight from the files that decide it, NOT from catalog.json's sidecar: the sidecar is
    generated on its own cadence and can lag, and this is a claim a visitor will hold us to. A
    page that says "Update cadence: monthly" for a database still on its initial load is a
    promise we are not keeping.

    WIRED = the source would actually run. Three mechanisms refresh things, and using only the
    first got this WRONG on the first attempt — bundesbank, un_wpp and sec_edgar were labelled
    "not yet wired" on the public site when they are refreshed every day:
      1. registry.yaml `live: true`         (the daily updater's live tier)
      2. the updater-heavy.yml matrix       (bundesbank, un_wpp, cepii_gravity, eia)
      3. sec-edgar-daily.yml                (sec_edgar)
    AND, for the fetcher-backed strategies, the fetcher module must exist — cepii_gravity and
    eia are in the matrix with no fetcher, so the orchestrator prints "PENDING — no adapter
    built" and skips them forever. Mirrors tools/audit_schedule_coverage.scheduled_sources() and
    orchestrate._has_adapter, so the site, the runner and the audit cannot disagree.
    """
    root = os.path.dirname(HERE)
    reg_path = os.path.join(root, "updater", "registry.yaml")
    if not os.path.exists(reg_path):
        return {}
    try:
        import yaml
        reg = yaml.safe_load(open(reg_path, encoding="utf-8"))
    except Exception:                                        # noqa: BLE001
        return {}
    fdir = os.path.join(root, "updater", "strategies", "fetchers")
    by_id = {e.get("source_id"): e for e in (reg.get("sources") or []) if e.get("source_id")}

    scheduled = {sid for sid, e in by_id.items() if e.get("live")}
    heavy_path = os.path.join(root, ".github", "workflows", "updater-heavy.yml")
    if os.path.exists(heavy_path):
        m = re.search(r"ALL='(\[[^']*\])'", open(heavy_path, encoding="utf-8").read())
        if m:
            try:
                scheduled |= set(json.loads(m.group(1)))
            except Exception:                                # noqa: BLE001
                pass
    sec_path = os.path.join(root, ".github", "workflows", "sec-edgar-daily.yml")
    if os.path.exists(sec_path):
        scheduled |= set(re.findall(r"sec_edgar(?:_xbrl)?",
                                    open(sec_path, encoding="utf-8").read()))

    out = {}
    for sid, e in by_id.items():
        wired = sid in scheduled
        if wired and e.get("strategy") in _FETCHER_BACKED:
            wired = os.path.exists(os.path.join(fdir, f"{sid}.py"))
        out[sid] = wired
    return out


WIRING = load_wiring()

def load_resolvable():
    """Source ids the WORKER can actually serve — SUPPORTED_SOURCES in api/worker/src/util.ts.

    A page is a promise. cepii_gravity had a full dataset page with a "Download" call to action
    while `/v1/series/cepii_gravity:...csv` returned **404**, because the source is catalogued
    locally but absent from the worker's resolver — the same shape as the IEP sources, which
    went live searchable with zero objects behind them and a Download button that failed on
    every click. Verified 2026-07-30 against the live API with a real key: boc 200, cepii_gravity
    404.

    Comments are stripped before scanning, or prose words get harvested as ids (the R137 shape).
    Empty set => unknown, and callers must then NOT downgrade anything: silence beats a wrong
    "unavailable" badge on a database that works.
    """
    path = os.path.join(os.path.dirname(HERE), "api", "worker", "src", "util.ts")
    if not os.path.exists(path):
        return set()
    src = open(path, encoding="utf-8").read()
    m = re.search(r"SUPPORTED_SOURCES\s*:\s*readonly\s+string\[\]\s*=\s*\[(.*?)\]\s*;",
                  src, re.S)
    if not m:
        return set()
    body = re.sub(r"//.*", "", m.group(1))              # line comments (no DOTALL -> per line)
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)   # then block comments
    return set(re.findall(r'"([^"]+)"', body))


def load_denylisted():
    """Source ids the worker REFUSES with 451 — NON_REDISTRIBUTABLE in api/worker/src/denylist.ts.

    Separate from the resolver list on purpose: a source can be in SUPPORTED_SOURCES (the worker
    knows how to serve it) and still be denylisted (the worker must not). `load_resolvable`
    checked only the first, so `unsdg` shipped a page saying "Redistributable." with seven "Free
    download" buttons while `/v1/catalog?source=unsdg` answered 451. Verified live 2026-08-25.

    Same empty-set contract as load_resolvable: unreadable or unparsable => empty => subtract
    NOTHING. An unreadable gate must never silently strip the download offer from every page.
    """
    path = os.path.join(os.path.dirname(HERE), "api", "worker", "src", "denylist.ts")
    if not os.path.exists(path):
        return set()
    src = open(path, encoding="utf-8").read()
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new\s+Set\s*\(\s*\[(.*?)\]\s*\)", src, re.S)
    if not m:
        m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*\[(.*?)\]\s*;", src, re.S)
    if not m:
        return set()
    body = re.sub(r"//.*", "", m.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    return set(re.findall(r'"([^"]+)"', body))


DENYLISTED = load_denylisted()

# A page is a promise, and the promise is only true if the DATA PLANE agrees. The resolver says
# "I know how to serve this"; the denylist says "I must not". Both have to allow it.
RESOLVABLE = load_resolvable()


# ---------------------------------------------------------------------------- #
#  Build the per-dataset metadata model (the registry-grounded record)
# ---------------------------------------------------------------------------- #
# ------------------------------------------------------------------ #
# Human-facing topic labels. The per-series `category` column carries raw
# ingest tags (gilt_yield, treasury_cmt, fx, ...). Everywhere a human sees
# them — catalog cards, the topic filter, dataset-page badges, keywords —
# they are consolidated into these professional groups. Covers ALL 33
# distinct values in the registry (2026-07-15); an unmapped value falls
# back to Title Case and prints a warning at generation, never silently.
TOPIC_LABELS = {
    "Environmental": "Environment & Climate",
    "climate": "Environment & Climate",
    "Governance": "Governance & Institutions",
    "Social": "Society & Well-Being",
    "agriculture": "Agriculture & Food",
    "commodities": "Commodities",
    "crypto": "Digital Assets",
    "demography": "Population & Demographics",
    "deposit_rate": "Interest Rates",
    "lending_rate": "Interest Rates",
    "policy_rate": "Interest Rates",
    "reference_rate": "Interest Rates",
    "rates": "Interest Rates",
    "energy": "Energy",
    "equities": "Equities & Markets",
    "fundamentals": "Company Fundamentals",
    "exchange_rate": "Exchange Rates",
    "fx": "Exchange Rates",
    "fiscal": "Government Finance",
    "gilt_yield": "Government Bonds & Yields",
    "gilt_yield_real": "Government Bonds & Yields",
    "treasury_bill": "Government Bonds & Yields",
    "treasury_cmt": "Government Bonds & Yields",
    "treasury_tips": "Government Bonds & Yields",
    "housing": "Housing & Real Estate",
    "labour": "Labour Markets",
    "macro": "Macroeconomics",
    "monetary_aggregate": "Money & Banking",
    "national-accounts": "National Accounts",
    "output": "National Accounts",
    "prices": "Prices & Inflation",
    "reference": "Reference Data",
    # ABS 'RT' retail turnover is the ONLY series tagged 'trade' — it is the
    # ABS Retail Trade survey, not international trade. Labeling it "Trade"
    # made the Trade topic show one Australian retail series (owner-reported).
    "trade": "Retail Trade",
}
_TOPIC_WARNED = set()

# Source-level topics: most sources carry NO per-series category tags, so a
# tags-only topic filter hid them (e.g. every international-trade source —
# owner-reported as "I only have Australian trade numbers"). These labels are
# ADDED to a source's series-derived topics; any source still untagged falls
# back to its pillar's default topic, so EVERY source appears under at least
# one topic. Keyed by exact id or prefix ("unctad_" covers every facet).
_SOURCE_TOPICS = {
    # international trade & development
    "comtrade": ["International Trade"],
    "cepii_baci": ["International Trade"],
    "cepii_gravity": ["International Trade"],
    "harvard_atlas": ["International Trade"],
    "unctad_": ["International Trade"],
    "imf_bop*": ["International Trade"],
    "bea": ["International Trade", "National Accounts"],
    "idb": ["Development Indicators"],
    "adb": ["Development Indicators"],
    "worldbank_wdi": ["Development Indicators"],
    "gapminder": ["Development Indicators"],
    "worldbank_pink": ["Commodities"],
    "fao_": ["Agriculture & Food"],
    "faostat": ["Agriculture & Food"],
    # markets, money & finance
    "cboe": ["Equities & Markets"],
    "cftc": ["Equities & Markets"],
    "hf_equities": ["Equities & Markets"],
    "famafrench": ["Equities & Markets"],
    "shiller": ["Equities & Markets", "Housing & Real Estate"],
    "damodaran": ["Company Fundamentals", "Equities & Markets"],
    "sec_edgar": ["Company Fundamentals", "Equities & Markets"],
    "edgar_13f": ["Company Fundamentals", "Equities & Markets"],
    "edgar_insider": ["Company Fundamentals", "Equities & Markets"],
    "edgar_pointers": ["Company Fundamentals"],
    "defillama": ["Digital Assets"],
    "frankfurter": ["Exchange Rates"],
    "fdic": ["Money & Banking"],
    "ofr": ["Money & Banking"],
    "global_findex": ["Money & Banking", "Development Indicators"],
    "treasury": ["Government Finance", "Government Bonds & Yields"],
    "imf_gfs*": ["Government Finance"],
    "imf_commodity": ["Commodities"],
    "imf_cpi": ["Prices & Inflation"],
    "zillow": ["Housing & Real Estate"],
    "fhfa": ["Housing & Real Estate"],
    # institutions & society
    "vdem": ["Governance & Institutions"],
    # freedomhouse REMOVED 2026-07-30 — Freedom House declined re-hosting (their director of
    # research: "our preference is for the library to direct users to request the data
    # directly from us"). Owner decision the same day: no hosting AND no mention of the data
    # anywhere on the econ site. Any classification entry here is a route back onto a page,
    # so the id is removed rather than left mapped-but-unused.
    "polity": ["Governance & Institutions"],
    "wgi": ["Governance & Institutions"],
    "transparency_ti": ["Governance & Institutions"],
    "fsi": ["Governance & Institutions"],
    "fsi_fundforpeace": ["Governance & Institutions"],
    "ucdp": ["Conflict & Security"],
    "cow": ["Conflict & Security"],
    "sipri": ["Conflict & Security"],
    "sipri_polity": ["Conflict & Security", "Governance & Institutions"],
    "gpi": ["Conflict & Security", "Society & Well-Being"],
    "gti": ["Conflict & Security"],
    "wid": ["Inequality & Poverty"],
    "swiid": ["Inequality & Poverty"],
    "pip": ["Inequality & Poverty", "Development Indicators"],
    "whr": ["Society & Well-Being"],
    "social_progress": ["Society & Well-Being"],
    "ppi": ["Society & Well-Being"],
    "etr": ["Society & Well-Being"],
    "oxcgrt": ["Health", "Society & Well-Being"],
    "who_": ["Health"],
    "unhcr": ["Migration & Refugees"],
    "usda": ["Agriculture & Food"],
    "wikidata": ["Reference Data"],
    "undp_hdr": ["Development Indicators", "Society & Well-Being"],
    "unesco_clte": ["Culture & Media"],
    "unesco_cltt": ["Culture & Media", "International Trade"],
    "unesco_film": ["Culture & Media"],
    "unesco_dem": ["Population & Demographics"],
    "unesco_inno": ["Science & Innovation"],
    "un_wpp": ["Population & Demographics"],
    "ilo": ["Labour Markets"],
    "ilostat": ["Labour Markets"],
    "gleif": ["Reference Data"],
    "barro_lee": ["Education"],
    "kof_globalization": ["International Trade", "Society & Well-Being"],
    # energy & environment
    "eia": ["Energy"],
    "irena": ["Energy"],
    "ember": ["Energy"],
    "ei_statreview": ["Energy"],
    "gppd": ["Energy"],
    "owid": ["Energy", "Environment & Climate"],
    "gcb": ["Environment & Climate"],
    "nasa_giss": ["Environment & Climate"],
    "noaa": ["Environment & Climate"],
    "edgar_jrc": ["Environment & Climate"],
    "yale_epi": ["Environment & Climate"],
    # long-run research datasets
    "maddison": ["Long-Run & Historical", "Macroeconomics"],
    "pwt": ["Long-Run & Historical", "Macroeconomics"],
    "penn_world_table": ["Long-Run & Historical", "Macroeconomics"],
    "ggdc": ["Long-Run & Historical"],
    "epu": ["Macroeconomics"],
}

# Untagged sources take their pillar's headline topic (classify_pillar only
# needs id+name, so build_record can call it with a stub record).
_PILLAR_DEFAULT_TOPIC = {
    "macro": "Macroeconomics",
    "money": "Money & Banking",
    "trade": "International Trade",
    "energy": "Energy",
    "society": "Society & Well-Being",
    "research": "Long-Run & Historical",
}


def source_topics(sid):
    out = []
    for key, labels in _SOURCE_TOPICS.items():
        prefix = key.endswith("_") or key.endswith("*")
        if sid == key or (prefix and sid.startswith(key.rstrip("*"))):
            out.extend(labels)
    return out


def topic_labels(cats):
    out = []
    for c in cats or []:
        label = TOPIC_LABELS.get(c)
        if label is None:
            label = c.replace("_", " ").replace("-", " ").title()
            if c not in _TOPIC_WARNED:
                _TOPIC_WARNED.add(c)
                print(f"  WARNING: unmapped topic tag {c!r} -> fallback {label!r} (add to TOPIC_LABELS)")
        if label not in out:
            out.append(label)
    return sorted(out)


def build_record(sid, src, lic, roll, side, s5=None):
    """Assemble everything we know about one dataset from the registry + sidecar.
    Returns a plain dict; downstream renderers never touch the DB again.
    `s5` carries the source-level Task#5 metadata (description_key / citation_* /
    processing) baked into the series rows."""
    s5 = s5 or {}
    license_id = src.get("license_id")
    lrow = lic.get(license_id, {})
    # TWO independent gates, and the page needs both. `license.reservable` says the LICENCE
    # permits re-serving; the worker's denylist.ts says the WORKER will refuse it regardless.
    # unsdg passed the first and failed the second, so its page advertised seven "Free download"
    # buttons while GET /v1/catalog?source=unsdg answered 451. A page is a promise, and the
    # promise is only true if the data plane agrees.
    #
    # This line, not RESOLVABLE: `rec["reservable"]` is what gates the access label (:622), the
    # distribution block (:1655), the croissant distribution (:1719) and the gated-page notice
    # (:2160). My first attempt subtracted the denylist from RESOLVABLE, which has one unrelated
    # consumer, and the rebuilt page was unchanged.
    reservable = bool(lrow.get("reservable", 0)) and sid not in DENYLISTED

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
        "categories": (sorted(set(topic_labels((roll or {}).get("categories", []))
                                  + source_topics(sid)))
                       or [_PILLAR_DEFAULT_TOPIC[classify_pillar(
                           {"id": sid, "name": src.get("name") or sid})]]),
        "subtitle": SOURCE_SUBTITLES.get(sid) or (roll or {}).get("auto_subtitle") or "",
        # Complete, verbatim indicator list for schema.org/variableMeasured; [] when
        # the source's variable set is too large to enumerate honestly.
        "measures": (roll or {}).get("measures") or [],
        "n_geo": (roll or {}).get("n_geo", 0),
        "last_updated": sane_date((roll or {}).get("last_updated")),
        # operational (sidecar) extras -- display only
        "cadence": (side or {}).get("cadence"),
        "strategy": (side or {}).get("strategy"),
        "storage_layout": (side or {}).get("storage_layout"),
        "scripts": (side or {}).get("scripts") or [],
        "measured_obs": (side or {}).get("measured_obs"),
        # The 200 URL, not the 308 one (see site_url): this single field feeds the
        # canonical, og:url, schema.org Dataset.url, the Croissant url, the visible
        # "Canonical landing" row and the sitemap <loc>.
        "page_url": site_url(f"{sid}.html"),
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
# Google Dataset Search is a separate vertical: these pages compete against other
# DATASETS, not against the whole finance web, which makes this block the highest-
# value structured data on the site. Google validates it, and one bad field can
# disqualify the page -- so every value below is a verbatim registry string or a
# number counted from the registry, and anything the catalogue does not have is
# OMITTED rather than guessed.
#
# The DataCatalog the homepage and catalog page already publish under this @id.
# Emitting it here as includedInDataCatalog is what links 203 orphan Dataset nodes
# into one catalogue for Dataset Search; it costs no new data (see render_index).
CATALOG_REF = {
    "@type": "DataCatalog",
    "@id": f"{SITE_BASE}/#catalog",
    "name": SITE_NAME,
    "url": site_url("catalog.html"),
}


def jsonld_license(rec):
    """A licence value that is a resolvable URL or a HUMAN label -- never a raw
    internal token, and never omitted-by-accident.

    WAS WRONG: `LICENSE_LABEL.get(license_id, license_id)` fell back to the id
    itself, so bundesbank.html published `"license": "custom-terms-bundesbank"` and
    damodaran.html `"license": "custom-terms-damodaran"` (those two ids are absent
    from LICENSE_LABEL, which carries "custom-terms" and "bundesbank-granted").
    WHY IT MATTERED: licence is the single field on this site that must never be
    approximated. A machine token asserts a licence no one can read, and guessing
    that "custom-terms-bundesbank" means the same as the "bundesbank-granted" label
    would be inferring terms -- exactly what the verbatim licence audit forbids.
    So: omit, and let main() print a warning naming the ids that need a label."""
    if rec["license_url"]:
        return rec["license_url"]
    lid = rec.get("license_id")
    if not lid or lid == "NEEDS-REVIEW":
        return None
    return LICENSE_LABEL.get(lid)          # no id fallback -- None means "omit"


def jsonld_creator(rec):
    """The PRODUCING ORGANISATION.

    WAS WRONG: `creator = {"name": rec["name"]}` on all 203 pages, but `rec["name"]`
    is the whole producer-plus-dataset string, so fao_ge published a creator
    organisation literally named "Food and Agriculture Organization of the United
    Nations (FAO) — FAOSTAT, Emissions — Agriculture: Energy Use (domain GE)".
    WHY IT MATTERED: Dataset Search reads `creator` as the producer and groups /
    attributes datasets by it; a per-dataset "organisation" name groups nothing.
    split_name() already isolates the producer segment for the <title> work, and
    its long form is the spelled-out body ("Deutsche Bundesbank", "Bureau of Labor
    Statistics"). alternateName carries the registry's OWN acronym only when the
    catalogue itself asserts it (acronym_of never invents one)."""
    producer, _dataset, long_form = split_name(rec["name"])
    name = long_form or producer or rec["name"]
    out = {"@type": "Organization", "name": name}
    if producer and producer != name and _short_token(producer):
        out["alternateName"] = producer
    if rec["homepage"]:
        out["url"] = rec["homepage"]
    return out


def jsonld_description(rec, page_desc=None):
    """Dataset.description -- one of the two fields Google Dataset Search REQUIRES,
    and the text it SHOWS in a result.

    WAS WRONG: `desc_short`, i.e. the first 300 characters of `desc_full`, which is
    the operational sidecar note when there is one and the licence `attribution`
    line otherwise. Both are wrong for a public description, in different ways:

      * `attribution` (124 of 203 sources) is a credit line, and 57 pages carried
        one byte-identical to another page's -- 11 x "Source: United Nations Trade
        and Development Data Hub (UNCTADstat)".
      * the sidecar note is CRAWLER DOCUMENTATION. Measured 2026-08-01: 115 of 115
        sidecar descriptions contain internal build detail -- bls read "FULL-COVERAGE
        ingest of the U.S. BLS flat files ... GROUPED storage (mirrors
        jobs/ingest_eurostat.py) ... flushed to the Parquet writer in row-group
        batches". Publishing internal file paths and RAM notes as the dataset
        description is worse than the duplicate credit line, not better.

    So the description is the page's OWN meta description: assembled per page from
    counted registry facts (what the series measure, the producer, the series total,
    the measured coverage years, the licence), unique across the catalogue, and
    already the text this page shows search engines. The old values remain only as
    a fallback so the required field can never be empty."""
    return (page_desc or rec.get("desc_short") or rec.get("attribution")
            or f"{rec['name']} — dataset catalogued in the {SITE_NAME}.")


def dataset_jsonld(rec, page_desc=None):
    """Build a VALID schema.org/Dataset object. Required-by-Google fields:
    name, description; strongly recommended: license, creator/publisher,
    distribution, identifier, sameAs, temporalCoverage, isAccessibleForFree."""
    obj = {
        "@context": "https://schema.org/",
        "@type": "Dataset",
        "name": rec["name"],
        # description is required; guarantee a non-empty string.
        "description": jsonld_description(rec, page_desc),
        "url": rec["page_url"],
        "identifier": rec["id"],
        "isAccessibleForFree": True,
        "publisher": PUBLISHER,
        # Ties this Dataset to the DataCatalog node the hub pages already publish.
        "includedInDataCatalog": dict(CATALOG_REF),
    }

    obj["creator"] = jsonld_creator(rec)

    # license: a resolvable URL, or a human label; omitted when the registry holds
    # only an unverified status or a raw id (see jsonld_license).
    _lic = jsonld_license(rec)
    if _lic:
        obj["license"] = _lic

    # keywords from registry categories + provider id.
    kw = list(rec["categories"]) + [rec["id"]]
    if kw:
        obj["keywords"] = kw

    # variableMeasured: the source's COMPLETE indicator list, verbatim from its own
    # series titles, or nothing at all (see _measure_stems / MEASURES_MAX).
    if rec.get("measures"):
        obj["variableMeasured"] = list(rec["measures"])

    # spatialCoverage: DELIBERATELY NOT EMITTED. Measured 2026-08-01: `series.geography`
    # is non-empty on only 30 sources, and where it IS populated it is not a resolvable
    # place. The same token means different things in different sources -- statcan's
    # only geography is "CA" (Canada) while fhfa's "CA" is one of 61 US STATE codes --
    # and un_wpp's geography column carries "ADB region: East Asia"-style aggregates.
    # No rule over that column can tell a country from a state from a regional
    # aggregate, so any spatialCoverage would be a coin flip on an academic catalogue.
    # It stays omitted until the registry carries a coded place column.

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
            "name": f"{rec['id']} — CSV",
            "encodingFormat": "text/csv",
            "contentUrl": f"{SITE_BASE}/download.html?source={rec['id']}",
        }
        # contentSize is NOT emitted: the registry counts series, not served bytes.
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
def croissant_jsonld(rec, page_desc=None):
    """A minimal-but-valid Croissant record. Croissant is schema.org/Dataset
    plus the mlcommons:croissant context and (for reservable data) a parquet
    FileObject distribution. We keep it conservative: no fabricated RecordSet
    field types we can't verify -- just the dataset envelope + distribution.

    Shares jsonld_creator / jsonld_license / jsonld_description with the Dataset
    block above: the wrong-creator and raw-licence-token defects were identical in
    both blocks, and two copies of the same rule drift apart."""
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
        "description": jsonld_description(rec, page_desc),
        "url": rec["page_url"],
        "creator": dict(jsonld_creator(rec), **{"@type": "sc:Organization"}),
        "publisher": {"@type": "sc:Organization", "name": SITE_NAME, "url": SITE_BASE},
    }
    _lic = jsonld_license(rec)
    if _lic:
        obj["license"] = _lic
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
:root{--navy:#1a2332;--navy-light:#243044;--blue:#2563eb;--blue-light:#3b82f6;
--blue-pale:#eff6ff;--gold:#d4a843;--gold-light:#f0d78c;--gold-deep:#8a6d27;
--g50:#f9fafb;--g100:#f3f4f6;--g200:#e5e7eb;--g300:#d1d5db;--g400:#9ca3af;
--g500:#6b7280;--g600:#4b5563;--g700:#374151;--g800:#1f2937;--g900:#111827;
--green:#047857;--red:#b91c1c;--amber:#92600a;--max-width:1200px;
--serif:"Georgia","Times New Roman",serif;
--sans:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
--mono:"JetBrains Mono","Fira Code","Consolas",monospace}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:var(--sans);color:var(--g800);background:#fff;line-height:1.7;
-webkit-font-smoothing:antialiased}
/* Base typography copied from hfdatalibrary.com css/style.css:49-79 so EVERY page
   (not just the homepage) carries the canon's heading leading, link colour, code
   chip, <pre> panel, table spec and button set. */
h1,h2,h3,h4,h5,h6{font-family:var(--serif);color:var(--navy);line-height:1.3;font-weight:700}
h3{font-size:1.375rem;margin-bottom:.75rem}
h4{font-size:1.125rem;margin-bottom:.5rem}
p{margin-bottom:1rem}
a{color:var(--blue);text-decoration:none;transition:color .15s}
a:hover{color:var(--blue-light)}
code{font-family:var(--mono);font-size:.875em;background:var(--g100);padding:.15em .4em;
border-radius:4px}
pre{background:var(--navy);color:#e2e8f0;padding:1.25rem 1.5rem;border-radius:8px;
overflow-x:auto;font-family:var(--mono);font-size:.875rem;line-height:1.6;margin:0 0 1.5rem}
pre code{background:none;padding:0;color:inherit}
.table-wrap{overflow-x:auto;margin-bottom:1.5rem}
table{width:100%;border-collapse:collapse;font-size:.9rem}
thead th{text-align:left;padding:.75rem 1rem;border-bottom:2px solid var(--g300);
font-weight:600;color:var(--g700);white-space:nowrap}
tbody td{padding:.625rem 1rem;border-bottom:1px solid var(--g200)}
tbody tr:hover{background:var(--g50)}
td.mono{font-family:var(--mono);font-size:.85rem}
.citation-block{background:var(--g50);border:1px solid var(--g200);border-radius:8px;
padding:1.25rem 1.5rem;position:relative;font-size:.9rem;margin-bottom:1rem}
.citation-block p{margin:0;white-space:pre-wrap}
.citation-block pre{margin:0}
.citation-block .copy-btn{position:absolute;top:.75rem;right:.75rem;background:var(--blue);
color:#fff;border:none;padding:.35rem .75rem;border-radius:4px;font-size:.8rem;cursor:pointer}
.citation-block .copy-btn:hover{background:var(--blue-light)}
.btn{display:inline-flex;align-items:center;gap:.5rem;padding:.75rem 1.75rem;border-radius:8px;
font-weight:600;font-size:.95rem;cursor:pointer;border:none;transition:all .2s;text-decoration:none}
.btn-primary{background:var(--blue);color:#fff}
.btn-primary:hover{background:var(--blue-light);color:#fff;transform:translateY(-1px);
box-shadow:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.06)}
.btn-outline{background:transparent;color:#fff;border:2px solid rgba(255,255,255,.3)}
.btn-outline:hover{border-color:#fff;color:#fff}
.btn-gold{background:var(--gold);color:var(--navy)}
.btn-gold:hover{background:var(--gold-light);color:var(--navy)}
.btn-group{display:flex;gap:1rem;justify-content:center;flex-wrap:wrap}
.nav{background:var(--navy);color:#fff;position:sticky;top:0;z-index:100;
box-shadow:0 2px 8px rgba(0,0,0,.15)}
.nav-in{max-width:1200px;margin:0 auto;padding:0 1.5rem;height:64px;display:flex;
align-items:center;justify-content:space-between}
.brand{font-family:var(--serif);font-weight:700;font-size:1.2rem}.brand,.nav .brand{white-space:nowrap;flex-shrink:0}
.brand .d{color:var(--gold)}.brand a{color:#fff;text-decoration:none}
/* nav links mirror hfdatalibrary.com exactly: padded pills, hover + active
   highlight (rgba white .1 background), same rhythm and bar height (64px). */
.nav-links{display:flex;gap:.1rem;list-style:none;align-items:center;white-space:nowrap}
.nav-links a{color:rgba(255,255,255,.8);text-decoration:none;padding:.5rem .75rem;
border-radius:6px;font-size:.875rem;font-weight:500;transition:all .15s;white-space:nowrap}
.nav-links a:hover,.nav-links a.active{color:#fff;background:rgba(255,255,255,.1)}
@media (max-width:1240px){.nav-links a{padding:.5rem .45rem;font-size:.82rem}}
@media (max-width:1160px){.nav-links a{padding:.5rem .3rem;font-size:.8rem}}
.nav-toggle{display:none;background:none;border:none;color:#fff;font-size:1.5rem;cursor:pointer}
/* hf css/style.css:382-400 -- collapse the link row before it crowds the bar. */
@media (max-width:1120px){
.nav-links{display:none}
.nav-links.open{display:flex;flex-direction:column;position:absolute;top:64px;left:0;right:0;
background:var(--navy);padding:1rem;box-shadow:0 10px 15px rgba(0,0,0,.1),0 4px 6px rgba(0,0,0,.05)}
.nav-toggle{display:block}
}
.container{max-width:var(--max-width);margin:0 auto;padding:0 1.5rem}
.container-narrow{max-width:920px;margin:0 auto;padding:0 1.5rem}
.section{padding:5rem 0}
.section-alt{background:var(--g50)}
.wrap{max-width:920px;margin:0 auto;padding:2rem 1.5rem}
.crumb{font-size:.82rem;color:var(--g500);margin-bottom:1rem}
.crumb a{color:var(--blue);text-decoration:none}
h1{font-size:2.5rem}
.pid{font-family:var(--mono);color:var(--gold-deep);font-size:.85rem;margin-bottom:.3rem}
.badges{margin:.9rem 0 1.2rem;display:flex;gap:.5rem;flex-wrap:wrap}
.badge{display:inline-block;font-size:.75rem;font-weight:700;letter-spacing:.05em;
padding:.2em .6em;border-radius:4px}
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
p.proc{font-size:.85rem;color:var(--g600);margin:.4rem 0 1rem}
h2{font-size:1.875rem;margin:1.8rem 0 1rem}
.kv{display:grid;grid-template-columns:190px 1fr;gap:.45rem 1rem;font-size:.92rem}
.kv dt{color:var(--g500);font-weight:600}
.kv dd{color:var(--g800);word-break:break-word}
.kv dd a{color:var(--blue)}
.mono{font-family:var(--mono);font-size:.85rem}
details{margin-top:1rem;border:1px solid var(--g200);border-radius:8px;background:var(--g50)}
summary{cursor:pointer;padding:.7rem 1rem;font-weight:600;color:var(--g700);font-size:.9rem}
details pre{margin:0;font-size:.78rem;line-height:1.5;border-radius:0 0 8px 8px}
.dhero{background:var(--navy);padding:3rem 0;margin:0 0 1.6rem}
.dhero>*{max-width:920px;margin-left:auto;margin-right:auto;padding:0 1.5rem}
.dhero .crumb{color:rgba(255,255,255,.55);margin-bottom:.7rem}
.dhero .crumb a{color:var(--gold);text-decoration:none}
.dhero .pid{color:var(--gold);margin-bottom:.35rem}
.dhero h1{color:#fff;font-size:2.5rem;margin-bottom:.5rem}
.dhero .badges{margin:.9rem 0 0}
.dhero .lead{color:rgba(255,255,255,.7);margin:.8rem 0 0;font-size:1.1rem}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.7rem;margin:.5rem 0 1rem}
.metric{background:var(--g50);border:1px solid var(--g200);border-radius:10px;padding:.75rem .9rem}
.metric .mlabel{font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--g500)}
.metric .mval{font-family:var(--mono);font-weight:700;font-size:1.15rem;color:var(--navy);
margin-top:.25rem;word-break:break-word}
/* hf css/style.css:402-416 -- the one shared mobile block, now on every page. */
@media (max-width:768px){
h1,.hero h1,.dhero h1{font-size:2rem}
.stats-bar{grid-template-columns:repeat(2,1fr);gap:1rem}
.stat-number{font-size:1.5rem}
.grid-2,.grid-3,.grid-4{grid-template-columns:1fr}
.feature-row,.feature-row.reverse{grid-template-columns:1fr;gap:2rem;direction:ltr}
.kv{grid-template-columns:1fr}
.metrics{grid-template-columns:1fr 1fr}
}
"""

HEAD = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="google-site-verification" content="JPQLV9lmydtD2e7IQ62JihpAvow7pUjLlTVUyAaKlSo">
<title>{title}</title>
<meta name="description" content="{meta_desc}">
<meta name="author" content="Ahmed Elkassabgi">
<link rel="canonical" href="{canonical}">
{social}
<link rel="icon" type="image/svg+xml" href="assets/favicon.svg">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>{css}
/* status bar + nav (hfdatalibrary.com parity) */
.status-bar{{background:var(--g50);border-bottom:1px solid var(--g200);font-size:.8rem;
color:var(--g500);min-height:32px;line-height:32px}}
.status-bar .sb-in{{max-width:var(--max-width);margin:0 auto;padding:0 1.5rem;height:100%;
display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:.25rem}}
.nav .signin{{background:var(--gold);color:var(--navy)!important;padding:.4rem .875rem;
border-radius:6px;font-size:.85rem;font-weight:600;white-space:nowrap;margin-left:.75rem}}
.nav .signin:hover{{background:#e0b955}}
.nav .brand{{display:inline-flex;align-items:center;gap:.55rem;white-space:nowrap;flex-shrink:0}}
.nav .fam-tag{{font-size:.62rem;color:var(--gold)!important;border:1px solid rgba(212,168,67,.5);
border-radius:999px;padding:.12rem .5rem;white-space:nowrap;font-family:var(--sans);font-weight:600;
letter-spacing:.01em;text-decoration:none;line-height:1.4}}
.nav .fam-tag:hover{{background:rgba(212,168,67,.15)}}
@media (max-width:680px){{.nav .fam-tag{{display:none}}}}
/* family band + footer (hfdatalibrary.com parity, R438: on EVERY page) */
.ekd-band{{background:linear-gradient(135deg,var(--navy) 0%,var(--navy-light,#243044) 100%);
text-align:center;padding:3.5rem 0}}
.ekd-band .in{{max-width:1200px;margin:0 auto;padding:0 1.5rem}}
.ekd-band .tag{{color:rgba(255,255,255,.85);font-size:1.15rem;margin:1.2rem 0 .3rem;font-family:var(--serif)}}
.ekd-band .fam{{color:rgba(255,255,255,.55);font-size:.9rem;max-width:640px;margin:0 auto}}
.ekd-band .fam a{{color:var(--gold);text-decoration:none}}
.site-footer{{background:var(--navy);color:rgba(255,255,255,.7);padding:3rem 0 2rem;font-size:.9rem}}
.site-footer .in{{max-width:1200px;margin:0 auto;padding:0 1.5rem}}
.site-footer .grid{{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:2rem;margin-bottom:2rem}}
.site-footer h4{{color:#fff;font-family:var(--sans);margin-bottom:.75rem}}
.site-footer a{{color:rgba(255,255,255,.7);text-decoration:none;transition:color .15s}}
.site-footer a:hover{{color:#fff}}
.site-footer ul{{list-style:none;margin:0;padding:0}}
.site-footer li{{margin-bottom:.4rem}}
.site-footer p{{margin-bottom:.75rem}}
.site-footer .bottom{{border-top:1px solid rgba(255,255,255,.1);padding-top:1.5rem;
display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:.5rem}}
.site-footer .orcid{{font-family:var(--mono);font-size:.8rem}}
@media (max-width:768px){{.site-footer .grid{{grid-template-columns:1fr 1fr}}
.site-footer .bottom{{flex-direction:column;text-align:center}}}}
</style>
{jsonld}
<!-- sso.js pin: bump this string whenever assets/sso.js changes. It is a cache-buster,
     and because this generator rewrites all 216 pages, a stale value here silently
     reverts every page to an older script for returning visitors -- which is how a
     deployed sign-out fix can appear not to work. Last bump: 20260802a, the cross-tab
     sign-out marker. -->
<script src="assets/sso.js?v=20260802a"></script>
</head><body>
<div class="status-bar" id="status-bar"><div class="sb-in">
<span><span id="sb-dot" style="color:#9ca3af;font-size:.7rem">&#9679;</span> <span id="sb-text">Checking status&hellip;</span></span>
<span style="display:flex;gap:1.5rem"><span id="sb-site"></span><span id="sb-data"></span></span>
</div></div>
<nav class="nav"><div class="nav-in"><div class="brand"><a href="index.html">Econ Data <span class="d">Library</span></a><a href="https://elkassabgidata.com" class="fam-tag" title="Part of the ElkassabgiData family — one account, every library">part of ElkassabgiData</a></div>
<button class="nav-toggle" onclick="document.querySelector('.nav-links').classList.toggle('open')" aria-label="Toggle navigation">&#9776;</button>
<ul class="nav-links"><li><a href="catalog.html">Catalog</a></li><li><a href="docs.html">Documentation</a></li><li><a href="api.html">API</a></li><li><a href="download.html">Download</a></li><li><a href="mcp.html">AI Tools</a></li><li><a href="cite.html">Cite</a></li><li><a href="stats.html">Stats</a></li><li><a href="status.html">Status</a></li><li><a href="contact.html">Contact</a></li><li><a href="https://hfdatalibrary.com">HF Data</a></li><li><a href="https://ipdatalibrary.com">IP Data</a></li><li><a href="account.html" class="signin">Sign in</a></li></ul></div></nav>
<script>
(function(){{
  var API="https://econdl-api.elkassabgi.workers.dev";
  function fdate(s){{try{{
    var m=/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})/.exec(s);
    var d=m?new Date(+m[1],+m[2]-1,+m[3]):new Date(s); /* date-only: local, no UTC shift */
    if(isNaN(d))return s;
    return d.toLocaleDateString('en-US',{{year:'numeric',month:'long',day:'numeric'}});}}catch(e){{return s;}}}}
  /* hf js/site.js:125-134 -- the strip's second field is a relative age, not a bare date. */
  function tago(s){{try{{
    var m=/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})/.exec(s);
    var t=m?new Date(+m[1],+m[2]-1,+m[3]):new Date(s);
    if(isNaN(t))return '';
    var diff=Math.floor((new Date()-t)/1000);
    if(diff<60)return 'just now';
    if(diff<3600)return Math.floor(diff/60)+' minutes ago';
    if(diff<86400)return Math.floor(diff/3600)+' hours ago';
    if(diff<604800)return Math.floor(diff/86400)+' days ago';
    return '';}}catch(e){{return '';}}}}
  fetch(API+"/v1/stats?t="+Date.now()).then(function(r){{if(!r.ok)throw 0;return r.json();}}).then(function(d){{
    document.getElementById('sb-dot').style.color='#059669';
    document.getElementById('sb-text').textContent='All systems operational';
    document.getElementById('sb-site').textContent='Website updated: '+fdate('__SITE_UPDATED__');
    if(d.as_of){{var _ag=tago(d.as_of);
      document.getElementById('sb-data').textContent='Data updated: '+fdate(d.as_of)+(_ag?' ('+_ag+')':'');}}
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
<section class="ekd-band">
  <div class="in">
    <a href="https://elkassabgidata.com" title="ElkassabgiData &mdash; one account, every library">
      <img src="assets/elkassabgidata-logo.svg" alt="ElkassabgiData" width="340" height="91" style="max-width:90%;height:auto">
    </a>
    <p class="tag">One account. Every library.</p>
    <p class="fam">Your free ElkassabgiData key unlocks the whole family:
      <strong style="color:#fff">Econ Data Library</strong> (this site) and
      <a href="https://hfdatalibrary.com">HF Data Library</a> &mdash; 1-minute intraday U.S. equity data &mdash; and
      <a href="https://ipdatalibrary.com">IP Data Library</a> &mdash; patent &amp; innovation measures.</p>
  </div>
</section>
"""

# Canonical footer, hf-structured, appended by _write() BELOW the band on EVERY
# page. Before R438 only the homepage had a footer; 243 pages simply ended.
# __SITE_UPDATED__ (not __GEN__) because _write() already substitutes it, after
# the lastmod fingerprint is taken.
FOOTER = """
<footer class="site-footer">
  <div class="in">
    <div class="grid">
      <div>
        <h4>Econ Data Library</h4>
        <p>Econ Data Library is the largest totally free online database in the world &mdash; dedicated to bringing all of the world&rsquo;s freely available data into a single, easily accessible location, with the help of cutting-edge AI tools. Built and maintained by Ahmed Elkassabgi at the University of Central Arkansas.</p>
        <p>Part of the ElkassabgiData family &mdash; one free account for every library.</p>
      </div>
      <div>
        <h4>Data</h4>
        <ul>
          <li><a href="catalog.html">Browse the Catalog</a></li>
          <li><a href="download.html">Download</a></li>
          <li><a href="status.html">Source Status</a></li>
          <li><a href="stats.html">Stats</a></li>
        </ul>
      </div>
      <div>
        <h4>Documentation</h4>
        <ul>
          <li><a href="docs.html">Methodology</a></li>
          <li><a href="api.html">API Reference</a></li>
          <li><a href="mcp.html">MCP Server (AI)</a></li>
          <li><a href="cite.html">How to Cite</a></li>
        </ul>
      </div>
      <div>
        <h4>About</h4>
        <ul>
          <li><a href="https://elkassabgidata.com/about">Our Story</a></li>
          <li><a href="https://hfdatalibrary.com">HF Data Library</a></li>
          <li><a href="https://ipdatalibrary.com">IP Data Library</a></li>
          <li><a href="contact.html">Contact</a></li>
        </ul>
      </div>
      <div>
        <h4>Legal</h4>
        <ul>
          <li><a href="https://hfdatalibrary.com/pages/terms">Terms of Service</a></li>
          <li><a href="https://hfdatalibrary.com/pages/privacy">Privacy Policy</a></li>
        </ul>
      </div>
    </div>
    <div class="bottom">
      <p>&copy; 2026 Ahmed Elkassabgi. University of Central Arkansas.</p>
      <p class="orcid">ORCID: <a href="https://orcid.org/0000-0002-5926-7493">0000-0002-5926-7493</a></p>
    </div>
  </div>
</footer>
"""


def jsonld_script(obj):
    payload = json.dumps(obj, ensure_ascii=False, indent=2).replace("</", "<\\/")
    return f'<script type="application/ld+json">\n{payload}\n</script>'


# ---------------------------------------------------------------------------- #
#  Open Graph / Twitter card
# ---------------------------------------------------------------------------- #
# WHAT WAS WRONG: measured 2026-08-01, ZERO of the 215 generated files carried a
# single og:* or twitter:* tag -- the homepage included, while hfdatalibrary.com
# emits nine. WHY IT MATTERED: without them Slack, Teams, LinkedIn, WhatsApp and
# iMessage render a shared link as a bare URL with whatever text they scrape. This
# catalogue is passed around exactly that way (one academic drops a dataset link in
# a channel), so the unfurl IS the first impression of the page.
#
# Every value comes from the SAME real per-page strings as <title> and
# <meta description>, so an OG tag can never disagree with the page it describes.
#
# og:image is a real PNG (assets/og-image.png, 1200x630). It used to be absent
# on purpose: site/assets held only SVGs and Facebook/LinkedIn/X do not render
# SVG, so pointing og:image at one produces a BROKEN card, worse than none.
# That reasoning stands - the fix was to render a PNG, not to relax it. With a
# real raster card the family can carry summary_large_image honestly.
def social_meta(title, description, canonical, og_type="website"):
    """Open Graph + Twitter card tags for one page. Takes RAW (unescaped) text."""
    og_title = title
    for _sep in (f" — {SITE_NAME}",):
        if og_title.endswith(_sep):
            # og:site_name already carries the brand; repeating it inside og:title
            # just eats the ~60 characters an unfurl shows before it truncates.
            og_title = og_title[: -len(_sep)]
            break
    tags = [
        ("property", "og:title", og_title),
        ("property", "og:description", description),
        ("property", "og:type", og_type),
        ("property", "og:url", canonical),
        ("property", "og:site_name", SITE_NAME),
        ("property", "og:locale", "en_US"),
        ("property", "og:image", f"{SITE_BASE}/assets/og-image.png"),
        ("property", "og:image:width", "1200"),
        ("property", "og:image:height", "630"),
        ("name", "twitter:card", "summary_large_image"),
        ("name", "twitter:image", f"{SITE_BASE}/assets/og-image.png"),
        ("name", "twitter:title", og_title),
        ("name", "twitter:description", description),
    ]
    return "\n".join(f'<meta {attr}="{k}" content="{esc(v)}">'
                     for attr, k, v in tags if v)


def render_head(title, meta_desc, canonical, css, jsonld="", og_type="website"):
    """The ONE way to build a page <head>.

    Every page previously called HEAD.format() itself, which is how the site ended
    up with four head-builders and zero Open Graph tags: a new tag had to be
    remembered in four places. Routing all of them through here means a page cannot
    be published without its social tags.

    Takes RAW (unescaped) title / description / canonical and escapes each exactly
    once, so the same string cannot be double-escaped in one tag and emitted raw in
    another (the index and info pages were passing an unescaped '&' straight into
    the description attribute)."""
    return HEAD.format(
        title=esc(title),
        meta_desc=esc(meta_desc),
        canonical=esc(canonical),
        css=css,
        jsonld=jsonld,
        social=social_meta(title, meta_desc, canonical, og_type),
    )


# ---------------------------------------------------------------------------- #
#  Per-source embeds granted by the provider in writing. NEVER add one without a
#  documented permission trail. Each entry: heading, permission note (shown on
#  the page), and the provider-supplied embed HTML (cleaned of mail-relay link
#  mangling; functionally identical to what the provider sent).
# ---------------------------------------------------------------------------- #
SOURCE_EMBEDS = {
    # Social Progress Imperative — written permission from REDACTED
    # (written permission on file from Social Progress Imperative, 2026-07-14):
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
    # Title + meta description (see build_page_seo above). Previously the title was
    # `source.name + " — Econ Data Library"` -- producer-first, so the words that
    # tell two datasets apart fell past Google's ~60-char truncation and 83 pages
    # shared a visible prefix; and meta_desc was `desc_short`, which is a licence
    # credit line on 124 of 203 pages and byte-identical across 57 of them.
    # PAGE_SEO is computed once over the whole catalogue because uniqueness of the
    # first 60 characters is a cross-page property; the fallback keeps this renderer
    # usable standalone.
    title, meta_desc = PAGE_SEO.get(rec["id"]) or (
        f"{rec['name']} — {SITE_NAME}", build_meta_desc(rec))

    # JSON-LD is built AFTER the title/description because both blocks now fall back
    # to this page's unique meta description when the sidecar has no real prose --
    # Dataset.description is required by Google Dataset Search and used to be a
    # licence credit line shared with up to 10 other pages.
    ds_ld = dataset_jsonld(rec, meta_desc)
    cr_ld = croissant_jsonld(rec, meta_desc)
    jsonld_block = jsonld_script(ds_ld) + "\n" + jsonld_script(cr_ld)

    head = render_head(title, meta_desc, rec["page_url"], PAGE_CSS, jsonld_block)

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
        # Never render a bare "?" for a bound we do not have. It read as a broken page
        # ("Coverage ? – 2018-12-31" on ggdc and maddison, reported 2026-08-02), and it is
        # also the wrong claim: we are not uncertain about the coverage, we simply have no
        # usable START for it. Each shape gets a phrasing that is true on its own terms.
        #
        # The two affected sources are the ironic case: their real start IS year 1 (the
        # Maddison Project genuinely estimates back that far), and sane_date() rejects
        # anything below 1000 because for every other source such a value is parse damage.
        # A true fact is dropped, so say plainly what is left rather than printing a glyph.
        if rec["cov_start"] and rec["cov_end"]:
            span = f"{rec['cov_start']} – {rec['cov_end']}"
        elif rec["cov_start"]:
            span = f"{rec['cov_start']} – present"
        else:
            span = f"through {rec['cov_end']}"
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
            ("Download",
             (f'<a href="download.html?source={esc(rec["id"])}">Select &amp; download '
              f'{esc(rec["id"])} series as CSV &rarr;</a>')
             if (not RESOLVABLE or rec["id"] in RESOLVABLE) else
             ('<em>not downloadable yet</em> &mdash; this database is catalogued and its '
              'per-series files are still being published. The API returns 404 for it until '
              'that completes, so we are not offering a button that would fail.')),
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
    # Default FALSE, not None: a source with no registry entry at all is not merely unknown,
    # it is definitively not refreshed — the orchestrator only ever iterates registered units,
    # so an unregistered source is invisible to it. 88 of the 203 served databases are in that
    # state (unctad_*, fao_*, unesco_*, most imf_*), and they are exactly the ones a visitor
    # most needs told. Treating None as "say nothing" left them silently implying currency.
    _wired = bool(WIRING.get(rec["id"], False)) if WIRING else None
    if rec["cadence"]:
        # Say plainly whether that cadence is running or still a target. A bare
        # "Update cadence: monthly" on a database that has no updater yet reads as a promise.
        if _wired is False:
            acc_rows.append(("Update cadence",
                             esc(rec["cadence"]) + " <em>(target &mdash; not yet automated)</em>"))
        else:
            acc_rows.append(("Update cadence", esc(rec["cadence"])))
    if _wired is True:
        acc_rows.append(("Automated refresh",
                         "<strong>live</strong> &mdash; this database is on the daily "
                         "update run"))
    elif _wired is False:
        acc_rows.append(("Automated refresh",
                         "not yet wired &mdash; the data here is the verified initial load. "
                         "See the <a href=\"status.html\">Source Status board</a>."))
    if rec["strategy"]:
        acc_rows.append(("Update strategy", esc(rec["strategy"]).replace("_", " ")))
    if rec["storage_layout"]:
        acc_rows.append(("Storage layout", esc(rec["storage_layout"])))

    def kv(rows):
        return "\n".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in rows)

    body = [head]
    body.append('<section class="dhero">')
    body.append(f'<div class="crumb"><a href="index.html">Catalog</a> / {esc(rec["name"])}</div>')
    body.append(f'<div class="pid">{esc(rec["id"])}</div>')
    body.append(f"<h1>{esc(rec['name'])}</h1>")
    body.append(f'<div class="badges">{"".join(badges)}</div>')
    if rec["desc_short"]:
        body.append(f'<p class="lead">{esc(rec["desc_short"])}</p>')
    body.append('</section>')  # /dhero -- full-bleed, outside .wrap
    body.append('<div class="wrap">')
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
        body.append(f'<div class="citation-block"><p>{esc(cite)}</p></div>')
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
        _p = os.path.join(HERE, "..", "data", "catalog.db")
        db = sqlite3.connect(f"file:{_p}?mode=ro", uri=True, timeout=120)
        db.execute("PRAGMA busy_timeout=120000")
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


# ------------------------------------------------------------------ #
# Catalog facets. The six pillars mirror the homepage "What the library
# covers" tiles; every visible source gets ONE primary pillar and ONE
# region so catalog.html can filter on them. These are browse/UX facets,
# not licensing statements. Distribution is printed at generation time so
# a bad rule is visible, never silent.
PILLARS = [
    ("macro",    "&#128200;", "Macro & National Accounts"),
    ("money",    "&#128176;", "Prices, Money & Central Banks"),
    ("trade",    "&#128674;", "Trade & Development"),
    ("energy",   "&#9889;",   "Energy & Environment"),
    ("society",  "&#127963;", "Institutions & Society"),
    ("research", "&#128218;", "Research Datasets"),
]
REGIONS = ["Global & International", "Europe", "Americas", "Asia-Pacific"]

_PILLAR_BY_ID = {
    # research datasets (named on the tile or scholar-built)
    "maddison": "research", "pwt": "research", "penn_world_table": "research",
    "shiller": "research", "famafrench": "research", "barro_lee": "research",
    "damodaran": "research", "ggdc": "research", "epu": "research",
    "kof_globalization": "research", "oxcgrt": "research", "gapminder": "research",
    "hf_equities": "research", "qog": "research", "harvard_atlas": "research",
    # institutions & society
    "vdem": "society", "polity": "society",   # freedomhouse removed 2026-07-30, see above
    "cow": "society", "ucdp": "society", "sipri": "society",
    "sipri_polity": "society", "wid": "society", "swiid": "society",
    "whr": "society", "wgi": "society", "transparency_ti": "society",
    "fsi": "society", "fsi_fundforpeace": "society", "gpi": "society",
    "gti": "society", "ppi": "society", "etr": "society",
    "social_progress": "society", "global_findex": "society", "pip": "society",
    "un_wpp": "society", "gleif": "society", "ilo": "society", "ilostat": "society",
    "who_gho": "society", "who_hwf": "society", "who_rs": "society",
    "who_sdg": "society", "oxcgrt": "society", "unesco_clte": "society",
    "unesco_cltt": "society", "unesco_dem": "society", "unesco_film": "society",
    "unesco_inno": "society", "unhcr": "society", "undp_hdr": "society",
    "worldbank_esg": "society",
    # energy & environment
    "eia": "energy", "irena": "energy", "ember": "energy", "gcb": "energy",
    "nasa_giss": "energy", "noaa": "energy", "ei_statreview": "energy",
    "owid": "energy", "gppd": "energy", "edgar_jrc": "energy", "yale_epi": "energy",
    # trade & development
    "comtrade": "trade", "cepii_baci": "trade", "cepii_gravity": "trade",
    "idb": "trade", "adb": "trade", "worldbank_pink": "trade",
    "worldbank_wdi": "trade", "faostat": "trade",
    # prices, money, markets & central banks
    "cboe": "money", "defillama": "money", "frankfurter": "money",
    "cftc": "money", "fdic": "money", "ofr": "money", "treasury": "money",
    "sec_edgar": "money", "edgar_13f": "money", "edgar_insider": "money",
    "edgar_pointers": "money", "zillow": "money", "fhfa": "money",
    "imf_commodity": "money", "imf_cpi": "money",
}
# NOTE: no "monetary" keyword — it would misfile every "International
# Monetary Fund" dataset under the Money pillar; the Macro tile names the IMF.
_MONEY_KEYS = ("central bank", "bank of", "banco", "bundesbank", "reserve bank",
               "federal reserve", "riksbank", "norges", "national bank", "evds")
_MONEY_IDS = {"ecb", "ecb_sdmx", "bis", "rba", "nbp", "snb", "boe", "tcmb",
              "cnb", "bcb", "bcrp", "boc", "nyfed", "fed_board", "bundesbank",
              "norgesbank", "riksbank"}


def classify_pillar(rec):
    sid = rec["id"]
    if sid in _PILLAR_BY_ID:
        return _PILLAR_BY_ID[sid]
    hay = (sid + " " + (rec["name"] or "")).lower()
    if sid in _MONEY_IDS or any(k in hay for k in _MONEY_KEYS):
        return "money"
    if sid.startswith(("unctad", "fao")) or "trade" in hay:
        return "trade"
    # IMF facets, World Bank, OECD, NSOs, Eurostat: the Macro pillar
    # (the tile itself names "the IMF/World Bank" + the NSOs).
    return "macro"


_REGION_EUROPE = {"eurostat", "ecb", "ecb_sdmx", "frankfurter", "bundesbank",
                  "boe", "ons_uk", "insee", "insee_bdm", "insee_melodi",
                  "insee_sdmx", "insee_sirene", "istat", "ine_spain", "cbs_nl",
                  "dst", "ssb", "scb", "statfin", "hagstofa", "stat_estonia",
                  "stat_latvia", "stat_slovenia", "gus", "gus_dbw", "ksh",
                  "ksh_stadat", "cso", "cnb", "nbp", "riksbank", "norgesbank",
                  "snb", "bfs", "tcmb"}
_REGION_AMERICAS = {"bea", "bls", "census", "fred", "fred_releases", "fed_board",
                    "nyfed", "treasury", "cftc", "fdic", "sec_edgar", "edgar_13f",
                    "edgar_insider", "edgar_pointers", "eia", "fhfa", "ofr",
                    "statcan", "boc", "bcb", "bcrp", "ibge", "ipea", "idb",
                    "hf_equities", "cboe", "zillow", "shiller", "famafrench",
                    "damodaran", "noaa", "nasa_giss"}
_REGION_ASIAPAC = {"abs", "rba", "stats_nz", "adb"}


def classify_region(rec):
    sid = rec["id"]
    if sid in _REGION_EUROPE:
        return "Europe"
    if sid in _REGION_AMERICAS:
        return "Americas"
    if sid in _REGION_ASIAPAC:
        return "Asia-Pacific"
    return "Global & International"


def _catalog_idx(records):
    """The embedded JSON the catalog page filters/sorts client-side."""
    idx = [
        {
            "id": r["id"],
            "name": r["name"],
            # Card blurb = the per-source subtitle (curated or derived from the
            # source's own series titles) — NOT the raw ingest `description`,
            # which is an internal note. Disambiguates facet families that
            # share one name (WHO/UNCTAD/IMF/FAO/UNESCO).
            "desc": r.get("subtitle") or "",
            "license": r["license_label"],
            "reservable": r["reservable"],
            "cats": r["categories"],
            "n_series": r["n_series"],
            "page": f"{r['id']}.html",
            "pillar": classify_pillar(r),
            "region": classify_region(r),
            "updated": r.get("last_updated") or "",
        }
        for r in records
    ]
    from collections import Counter
    dist = Counter(x["pillar"] for x in idx)
    print("  catalog pillars:", {k: dist.get(k, 0) for k, _i, _l in PILLARS})
    tdist = Counter(c for x in idx for c in (x["cats"] or []))
    zero = [x["id"] for x in idx if not x["cats"]]
    print(f"  catalog topics: {len(tdist)} groups; untopiced sources: {len(zero)}"
          + (f" {zero}" if zero else ""))
    thin = {t: n for t, n in tdist.items() if n == 1}
    if thin:
        print(f"  single-source topics (fine, but check labels): {thin}")
    return idx


# Card-grid + filter-bar CSS for catalog.html (moved off the homepage).
CATALOG_UI_CSS = """
.wrapc{max-width:var(--max-width);margin:0 auto;padding:2.2rem 1.5rem 4rem}
.controls{display:flex;gap:.75rem;flex-wrap:wrap;margin:0 0 .9rem}
.controls input,.controls select{padding:.7rem .9rem;border:1px solid var(--g300);
border-radius:10px;font-size:.95rem;font-family:var(--sans);background:#fff}
.controls input{flex:1;min-width:240px}
.controls input:focus,.controls select:focus{outline:none;border-color:var(--blue);
box-shadow:0 0 0 3px var(--blue-pale)}
.chips{display:flex;gap:.5rem;flex-wrap:wrap;margin:0 0 1.2rem}
.chip{appearance:none;border:1px solid var(--g300);background:#fff;color:var(--g600);
font-family:var(--sans);font-weight:600;font-size:.84rem;padding:.45rem .85rem;
border-radius:999px;cursor:pointer;transition:all .12s}
.chip:hover{border-color:var(--gold);color:var(--navy)}
.chip.active{background:var(--navy);border-color:var(--navy);color:#fff}
.card{display:block;border:1px solid var(--g200);border-radius:12px;padding:1.1rem 1.2rem;
margin-bottom:.75rem;text-decoration:none;color:inherit;background:#fff;transition:all .2s}
.card:hover{box-shadow:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.06);border-color:var(--g300)}
.card .cid{font-family:var(--mono);font-size:.76rem;color:var(--gold-deep)}
.card h3{font-family:var(--sans);color:var(--navy);font-size:1.125rem;margin:.15rem 0 .3rem}
.card p{font-size:.88rem;color:var(--g600);margin:.2rem 0 .6rem;line-height:1.5;
display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .row{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center}
.count{color:var(--g500);font-size:.82rem;margin-left:auto;font-family:var(--mono)}
/* hf's list-page masthead (pages/tickers.html:77-82): flat navy, left-aligned,
   3rem 0, no gold rule -- identical to every other hf subpage band. */
.cat-hero{background:var(--navy);color:#fff;padding:3rem 0}
.cat-hero h1{color:#fff;margin:0 0 .5rem}
.cat-hero p{color:rgba(255,255,255,.7);font-size:1.1rem;margin:0}
[dir=rtl] .count{margin-left:0;margin-right:auto}
[dir=rtl] .card .row{flex-direction:row-reverse}
[dir=rtl] .card{text-align:right}
"""


def render_index(records, generated):
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
         "We only list sources whose license permits re-hosting; anything we can't "
         "redistribute isn't catalogued at all — never silently served."),
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
                "url": site_url("catalog.html"),
                "description": (
                    f"Searchable catalog of {n_total} economic and financial data sources "
                    "with license, provenance, and machine-readable Dataset + Croissant metadata."
                ),
                "publisher": PUBLISHER,
                # Permanent citable DOI (Zenodo) — lets Google Dataset Search and
                # scholarly indexers tie the catalog to its citation. Only emitted
                # once ZENODO_DOI is set.
                **({"identifier": f"https://doi.org/{ZENODO_DOI}",
                    "sameAs": f"https://doi.org/{ZENODO_DOI}"} if ZENODO_DOI else {}),
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
                "parentOrganization": {
                    "@type": "Organization",
                    "name": "ElkassabgiData",
                    "url": "https://elkassabgidata.com/",
                },
                "sameAs": ["https://orcid.org/0000-0002-5926-7493"],
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

    head = render_head(
        # Formal/official title = the Zenodo DOI title (10.5281/zenodo.21405120) so the
        # <title> tag, search results, and the citation record agree. The visible brand
        # LABEL stays SITE_NAME "Econ Data Library" (nav + per-page titles) — cf. HF /
        # High-Frequency, IHOP / International House of Pancakes.
        title="Economic Data Library: Free Economic and Financial Data",
        meta_desc=f"Free, research-grade economic & financial data: {n_total} sources in one namespace, with licenses, provenance, and producer-first citations on every series.",
        canonical=site_url("index.html"),  # the bare origin; /index.html 308s to it
        css=PAGE_CSS
        + """
/* ── HF-landing replica (mirrors hfdatalibrary.com css/style.css) ── */
.hero{background:linear-gradient(135deg,var(--navy) 0%,var(--navy-light) 100%);
color:#fff;padding:6rem 0 5rem;text-align:center}
.hero h1{font-family:var(--serif);color:#fff;font-size:3rem;margin-bottom:.5rem}
.hero h1 span{color:var(--gold)}
.hero .subtitle{font-size:1.25rem;color:rgba(255,255,255,.75);max-width:700px;margin:0 auto 2.5rem;font-weight:400}
.hero-note{font-size:.78rem;color:rgba(255,255,255,.5);margin-top:2rem}
/* the boxed Observations cell holds a 14-character count; widening its track is
   what lets the number stay at the canonical 2rem instead of being shrunk. */
.stats-bar{display:grid;grid-template-columns:1fr 1.4fr 1fr 1fr;gap:1.5rem;max-width:900px;margin:0 auto 3rem}
.stat-item{text-align:center}
.stat-item.hl{background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);
border-radius:12px;padding:1.25rem 1rem;box-shadow:0 4px 20px rgba(0,0,0,.3);margin-top:-1.3125rem}
.stat-number{font-family:var(--mono);font-size:2rem;font-weight:700;color:var(--gold);display:block}
.stat-label{font-size:.85rem;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.05em}
.feature-row{display:grid;grid-template-columns:1fr 1fr;gap:4rem;align-items:center}
/* without this the code panel sets a ~530px minimum and blows out the mobile track */
.feature-row > *{min-width:0}
.feature-text h2{font-family:var(--serif);color:var(--navy);font-size:1.875rem;margin:0 0 .75rem;border:none;padding:0}
.feature-text p{color:var(--g600);margin-bottom:1rem}
.feature-visual{background:var(--g50);border:1px solid var(--g200);border-radius:12px;padding:2rem}
.feature-visual pre{margin:0;font-size:.82rem}
.grid-3{display:grid;grid-template-columns:repeat(3,1fr);gap:1.5rem}
@media (max-width:768px){
.stats-bar{grid-template-columns:repeat(2,1fr);gap:1rem}
.stat-number{font-size:1.5rem}
.grid-3{grid-template-columns:1fr}
.feature-row{grid-template-columns:1fr;gap:2rem}
.hero h1{font-size:2rem}
.hero .sub,.hero p{font-size:1rem}
pre,code{max-width:100%;overflow-x:auto;word-break:break-word}
.acard,.tile-link{min-width:0}
}

.acard{background:#fff;border:1px solid var(--g200);border-radius:12px;padding:2rem;transition:all .2s}
.acard:hover{box-shadow:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.06);border-color:var(--g300)}
.acard .card-icon{width:48px;height:48px;background:var(--blue-pale);border-radius:10px;
display:flex;align-items:center;justify-content:center;font-size:1.5rem;margin-bottom:1rem}
.acard h3{font-family:var(--sans);color:var(--navy);font-size:1.125rem;margin-bottom:.75rem}
.acard p{color:var(--g600);font-size:.95rem;margin-bottom:0}
.section-title{font-family:var(--serif);color:var(--navy);font-size:1.875rem;text-align:center;
margin:0 0 2.5rem;border:none;padding:0}
.section-sub{text-align:center;color:var(--g500);margin-bottom:2.5rem}
.comparison-highlight{background:var(--blue-pale)!important}
.comparison-check{color:var(--green);font-weight:700}
.comparison-x{color:var(--g300)}
.faq-item{margin-bottom:1.5rem}
/* catalog search section (existing machinery) */
.controls{display:flex;gap:.75rem;flex-wrap:wrap;margin:0 0 1.2rem}
.controls input,.controls select{padding:.7rem .9rem;border:1px solid var(--g300);
border-radius:10px;font-size:.95rem;font-family:var(--sans);background:#fff}
.controls input{flex:1;min-width:240px}
.controls input:focus,.controls select:focus{outline:none;border-color:var(--blue);
box-shadow:0 0 0 3px var(--blue-pale)}
.card{display:block;border:1px solid var(--g200);border-radius:12px;padding:1.1rem 1.2rem;
margin-bottom:.75rem;text-decoration:none;color:inherit;background:#fff;transition:all .2s}
.card:hover{box-shadow:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.06);border-color:var(--g300)}
.card .cid{font-family:var(--mono);font-size:.76rem;color:var(--gold-deep)}
.card h3{font-family:var(--sans);color:var(--navy);font-size:1.125rem;margin:.15rem 0 .3rem}
.card p{font-size:.88rem;color:var(--g600);margin:.2rem 0 .6rem;line-height:1.5;
display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .row{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center}
.count{color:var(--g500);font-size:.82rem;margin-left:auto;font-family:var(--mono)}
[dir=rtl] .count{margin-left:0;margin-right:auto}
[dir=rtl] .card .row{flex-direction:row-reverse}
[dir=rtl] .hero,[dir=rtl] .card{text-align:right}
/* pillar tiles are links into catalog.html */
a.tile-link{display:block;text-decoration:none;color:inherit}
a.tile-link:hover{box-shadow:0 4px 6px rgba(0,0,0,.07),0 2px 4px rgba(0,0,0,.06);border-color:var(--g300)}
a.tile-link .tile-go{display:inline-block;margin-top:.9rem;color:var(--blue);font-weight:600;font-size:.86rem}
""",
        jsonld=jsonld_script(catalog_ld),
    )

    tpl = (
        head
        + """
<!-- ── Hero (hfdatalibrary.com landing structure) ── -->
<div role="status" style="background:linear-gradient(90deg,#d4a843,#e8c368);color:#14203a;text-align:center;padding:1rem 1.2rem;font-size:1.08rem;font-weight:600;line-height:1.5;border-bottom:4px solid #14203a;letter-spacing:.01em">&#128679; <strong>Under Construction</strong> &mdash; the Econ Data Library is being finalized. Datasets and their licensing are still being verified and may change. <strong>Automated updates are being wired database by database</strong>: a source joins the daily refresh only once its updater has been built and proven against the publisher. Until then its data is the verified initial load, not pretended current &mdash; the <a href="status.html" style="color:#14203a;text-decoration:underline">Source Status board</a> says which is which, per database, live.</div>
<section class="hero">
  <div class="container">
    <h1>Economic Data Library: Free <span>Economic &amp; Financial</span> Data</h1>
    <p class="subtitle">Free, research-grade macro &amp; financial data — one namespace over the world's statistical sources. Every series carries its license, provenance, and producer-first citation. Reproducible, snapshot-pinned, and <strong>continuously updated</strong>.</p>

    <div class="stats-bar">
      <div class="stat-item">
        <span class="stat-number" id="live-series">&mdash;</span>
        <span class="stat-label">Individual Series</span>
      </div>
      <div class="stat-item hl">
        <span class="stat-number" id="obs-counter">&mdash;</span>
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
      <a href="api.html" class="btn btn-outline">API Access</a><a class="btn btn-outline" href="mcp.html">AI &amp; MCP</a>
    </div>
    <p class="hero-note">Series and observation counts are measured on our data store (as of <span id="live-asof">&mdash;</span>) — never estimated, never hardcoded. Years of history: the earliest catalogued series (Maddison Project / GGDC) begin in year 1&nbsp;CE.</p>
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
        <p>Everything in the library is real, downloadable data — if we can’t host a source, we don’t list it.</p>
        <p>No subscription. No paywall. One free key for the whole ElkassabgiData family, including <a href="https://hfdatalibrary.com">HF Data Library</a> and <a href="https://ipdatalibrary.com">IP Data Library</a>.</p>
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

<!-- ── Every source is fully hosted ── -->
<section class="section section-alt">
  <div class="container">
    <h2 class="section-title">Every source, fully hosted</h2>
    <div style="max-width:680px;margin:0 auto">
      <div class="acard" style="border-left:4px solid var(--green)">
        <span class="badge open" style="margin-bottom:.5rem;display:inline-block">Redistributed</span>
        <h3>__NOPEN__ sources — all real, all downloadable</h3>
        <p>Every dataset here is served from our store as citation-headed CSV, over the free REST API, and in snapshot-pinned bundles — with license and attribution attached to every series. Licensed for re-hosting, with Python and R clients ready to go.</p>
        <p style="margin-top:.5rem"><strong>Best for:</strong> direct downloads, reproducible research bundles, API pipelines.</p>
      </div>
    </div>
    <p style="text-align:center;margin-top:2rem;color:var(--g500);font-size:.9rem;max-width:700px;margin-left:auto;margin-right:auto">If a source's license doesn't permit re-hosting, we simply don't list it — no dead-end pages, no teasers. The moment a publisher grants permission, its data appears here as a full download.</p>
  </div>
</section>

<!-- ── Coverage (hf '25 academic variables' parallel) ── -->
<section class="section">
  <div class="container">
    <h2 class="section-title" style="margin-bottom:.5rem">What the library covers</h2>
    <p class="section-sub">__N__ sources across the pillars of empirical economics and finance — click a pillar to browse its sources.</p>
    <div class="grid-3">
      <a class="acard tile-link" href="catalog.html?pillar=macro"><div class="card-icon">&#128200;</div><h3>Macro &amp; National Accounts</h3><p>GDP, employment, production — national statistical offices (ABS, INSEE, ISTAT, StatCan, Eurostat) and the IMF/World Bank.</p><span class="tile-go">Browse sources &rarr;</span></a>
      <a class="acard tile-link" href="catalog.html?pillar=money"><div class="card-icon">&#128176;</div><h3>Prices, Money &amp; Central Banks</h3><p>Inflation, interest rates, FX — ECB, Fed Board, BIS, Bundesbank, and dozens of national central banks.</p><span class="tile-go">Browse sources &rarr;</span></a>
      <a class="acard tile-link" href="catalog.html?pillar=trade"><div class="card-icon">&#128674;</div><h3>Trade &amp; Development</h3><p>Bilateral trade (CEPII BACI), tariffs, development indicators (World Bank WDI, UN SDG, UNDP HDR).</p><span class="tile-go">Browse sources &rarr;</span></a>
      <a class="acard tile-link" href="catalog.html?pillar=energy"><div class="card-icon">&#9889;</div><h3>Energy &amp; Environment</h3><p>EIA, IRENA, Ember, Global Carbon Budget, NASA GISS — production, prices, emissions, climate.</p><span class="tile-go">Browse sources &rarr;</span></a>
      <a class="acard tile-link" href="catalog.html?pillar=society"><div class="card-icon">&#127963;</div><h3>Institutions &amp; Society</h3><p>Governance (WGI, V-Dem), conflict (UCDP, COW), inequality (WID, SWIID), well-being (WHR).</p><span class="tile-go">Browse sources &rarr;</span></a>
      <a class="acard tile-link" href="catalog.html?pillar=research"><div class="card-icon">&#128218;</div><h3>Research Datasets</h3><p>Maddison Project (year 1 CE onward), Penn World Table, Shiller, Fama-French, Barro-Lee, and more.</p><span class="tile-go">Browse sources &rarr;</span></a>
    </div>
    <p style="text-align:center;margin-top:2.5rem"><a href="catalog.html" class="btn btn-primary" style="font-size:1.05rem;padding:.85rem 2.2rem">Browse the full catalog &rarr;</a></p>
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

<!-- ── Catalog CTA (the full grid lives on catalog.html) ── -->
<section class="section" id="catalog" style="padding:3.5rem 0">
  <div class="container-narrow" style="text-align:center">
    <h2 class="section-title" style="margin-bottom:.5rem">Browse the catalog</h2>
    <p class="section-sub" style="margin-bottom:1.5rem">All __N__ sources — searchable and filterable by pillar, region, topic, and access tier; series search in 6 languages.</p>
    <a href="catalog.html" class="btn btn-primary" style="font-size:1.05rem;padding:.85rem 2.2rem">Open the Data Catalog &rarr;</a>
  </div>
</section>

<!-- ── Comparison ── -->
<section class="section section-alt">
  <div class="container">
    <h2 class="section-title">How this compares</h2>
    <div class="table-wrap">
      <table>
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

<script>
const API="https://econdl-api.elkassabgi.workers.dev";
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
        tpl
        .replace("__FAQ__", faq_html)
        .replace("__YEARS__", f"{years:,}+" if years else "&mdash;")
        .replace("__NSERIES__", f"{n_series_total/1e6:.2f}M" if n_series_total >= 1e6 else f"{n_series_total:,}")
        .replace("__NACTIVE__", str(n_active))
        .replace("__N__", str(n_total))
        .replace("__NOPEN__", str(n_open))
        .replace("__NMETA__", str(n_meta))
        .replace("__GEN__", generated or TODAY)
    )


def render_catalog(records, generated):
    """The full, filterable source catalog (moved off the homepage).

    Filters: free-text search, the six homepage pillars (chips, deep-linkable
    via ?pillar=), topic (the per-source category tags), region, access tier
    (redistributed vs metadata-only); sorts: name / most series / recently
    updated. Non-English languages switch to the live multilingual series
    search against the API, unchanged from the old homepage behavior.
    """
    idx = _catalog_idx(records)
    data = json.dumps(idx, ensure_ascii=False).replace("</", "<\\/")
    n_total = len(records)
    n_open = sum(1 for r in records if r["reservable"])
    n_meta = n_total - n_open

    catalog_ld = {
        "@context": "https://schema.org/",
        "@type": "DataCatalog",
        "@id": f"{SITE_BASE}/#catalog",
        "name": SITE_NAME,
        "url": site_url("catalog.html"),
        "description": (
            f"Searchable catalog of {n_total} economic and financial data sources "
            "with license, provenance, and machine-readable Dataset + Croissant metadata."
        ),
        "publisher": PUBLISHER,
    }

    chips = '<button class="chip active" data-pillar="">All pillars</button>' + "".join(
        f'<button class="chip" data-pillar="{slug}">{icon} {label}</button>'
        for slug, icon, label in PILLARS
    )
    region_opts = '<option value="">All regions</option>' + "".join(
        f'<option value="{esc(x)}">{esc(x)}</option>' for x in REGIONS
    )

    head = render_head(
        title=f"Data Catalog — {SITE_NAME}",
        meta_desc=(
            f"Browse all {n_total} economic & financial data sources — filter by "
            "pillar, region, topic, and access tier; search series in 6 languages."
        ),
        canonical=site_url("catalog.html"),
        css=PAGE_CSS + CATALOG_UI_CSS,
        jsonld=jsonld_script(catalog_ld),
    )

    body = (
        head
        + """
<section class="cat-hero">
  <div class="container">
    <h1>Data Catalog</h1>
    <p>__N__ sources, all downloadable &middot; search datasets in English, or series in 6 languages via the live API</p>
  </div>
</section>
<div class="wrapc">
  <div class="controls">
    <input id="q" placeholder="Search by name, id, license, topic…" oninput="render()">
    <select id="lang" onchange="onLang()" title="Search series in another language" aria-label="Language">
      <option value="en">English</option>
      <option value="ar">العربية</option>
      <option value="es">Español</option>
      <option value="fr">Français</option>
      <option value="ru">Русский</option>
      <option value="zh">中文</option>
    </select>
  </div>
  <div class="chips" id="chips">__CHIPS__</div>
  <div class="controls" id="fine">
    <select id="topic" onchange="render()"><option value="">All topics</option></select>
    <select id="region" onchange="render()">__REGIONS__</select>
    <select id="sort" onchange="render()">
      <option value="name">Sort: Name A&ndash;Z</option>
      <option value="series">Sort: Most series</option>
      <option value="updated">Sort: Recently updated</option>
    </select>
  </div>
  <div class="count" id="count"></div>
  <div id="results"></div>
</div>
<script>
const IDX=__DATA__;
const API="https://econdl-api.elkassabgi.workers.dev";
let PILLAR='';
function esc(s){return (s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function curLang(){return document.getElementById('lang').value;}
function onLang(){
 const L=curLang(), ar=(L==='ar');
 document.documentElement.dir=ar?'rtl':'ltr';
 document.documentElement.lang=L;
 // pillar/topic/region/access/sort are dataset facets; the multilingual mode
 // searches individual series live, so hide them there.
 document.getElementById('fine').style.display=(L==='en')?'':'none';
 document.getElementById('chips').style.display=(L==='en')?'':'none';
 document.getElementById('q').placeholder=(L==='en')
  ?'Search by name, id, license, topic…':'Search series in this language…';
 render();
}
document.querySelectorAll('.chip').forEach(function(c){c.addEventListener('click',function(){
 document.querySelectorAll('.chip').forEach(function(x){x.classList.remove('active')});
 c.classList.add('active');PILLAR=c.getAttribute('data-pillar');render();
});});
(function(){ // populate the topic dropdown from the per-source category tags
 const seen={};IDX.forEach(function(r){(r.cats||[]).forEach(function(c){seen[c]=1})});
 const sel=document.getElementById('topic');
 Object.keys(seen).sort().forEach(function(c){
  const o=document.createElement('option');o.value=c;o.textContent=c;sel.appendChild(o);});
})();
function render(){
 if(curLang()!=='en'){clearTimeout(render._t);render._t=setTimeout(renderApi,250);return;}
 renderLocal();
}
function renderLocal(){
 const q=document.getElementById('q').value.toLowerCase().trim();
 const f='';  // access-tier filter removed — every source is redistributed/hosted
 const topic=document.getElementById('topic').value;
 const region=document.getElementById('region').value;
 const sort=document.getElementById('sort').value;
 let rows=IDX.filter(r=>{
  if(PILLAR&&r.pillar!==PILLAR)return false;
  if(f==='open'&&!r.reservable)return false;
  if(f==='meta'&&r.reservable)return false;
  if(topic&&!(r.cats||[]).includes(topic))return false;
  if(region&&r.region!==region)return false;
  if(q){const h=(r.id+' '+r.name+' '+r.license+' '+(r.cats||[]).join(' ')).toLowerCase();
   if(!h.includes(q))return false;}
  return true;});
 if(sort==='series'){rows.sort((a,b)=>(b.n_series||0)-(a.n_series||0));}
 else if(sort==='updated'){rows.sort((a,b)=>(b.updated||'').localeCompare(a.updated||''));}
 else{rows.sort((a,b)=>a.name.localeCompare(b.name));}
 // Name the active filters next to the count — filters combine (AND), and an
 // unnamed combination reads as "the filter is broken" when it yields 0.
 const parts=[];
 if(PILLAR){const c=document.querySelector('.chip[data-pillar="'+PILLAR+'"]');
  if(c)parts.push('Pillar: '+c.textContent.replace(/^[^ ]+ /,''));}
 if(topic)parts.push('Topic: '+topic);
 if(region)parts.push('Region: '+region);
 if(f)parts.push('Redistributed only');
 if(q)parts.push('Search: “'+q+'”');
 const desc=parts.join(' · ');
 document.getElementById('count').textContent=rows.length+' of '+IDX.length+' datasets'+(desc?' — '+desc:'');
 const out=rows.map(r=>{
  const badge='<span class="badge open">redistributed</span>';
  const cats=(r.cats||[]).slice(0,4).map(c=>'<span class="badge cat">'+esc(c)+'</span>').join('');
  const ser=r.n_series?'<span class="count">'+r.n_series.toLocaleString()+' series</span>':'';
  return '<a class="card" href="'+r.page+'"><div class="cid">'+esc(r.id)+'</div>'+
   '<h3>'+esc(r.name)+'</h3>'+
   (r.desc?'<p>'+esc(r.desc)+'</p>':'')+
   '<div class="row">'+badge+'<span class="badge lic">'+esc(r.license)+'</span>'+cats+' '+ser+'</div></a>';
 }).join('');
 document.getElementById('results').innerHTML=out||('<p style="color:#6b7280">No datasets match'+
  (desc?' the combined filters ('+esc(desc)+')':'')+'. '+
  '<a href="#" onclick="clearFilters();return false" style="font-weight:600">Clear all filters</a></p>');
}
function clearFilters(){
 PILLAR='';
 document.querySelectorAll('.chip').forEach(function(x){x.classList.remove('active')});
 document.querySelector('.chip[data-pillar=""]').classList.add('active');
 document.getElementById('q').value='';
 document.getElementById('topic').value='';
 document.getElementById('region').value='';
 document.getElementById('f').value='';
 render();
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
// Deep links: catalog.html?pillar=macro (the homepage tiles), plus ?q= and ?f=.
(function(){
 const p=new URLSearchParams(location.search);
 const pl=p.get('pillar');
 if(pl){const c=document.querySelector('.chip[data-pillar="'+pl+'"]');
  if(c){document.querySelectorAll('.chip').forEach(function(x){x.classList.remove('active')});
   c.classList.add('active');PILLAR=pl;}}
 if(p.get('q'))document.getElementById('q').value=p.get('q');
 if(p.get('f'))document.getElementById('f').value=p.get('f');
})();
render();
</script>
</body></html>
"""
    )
    return (
        body.replace("__DATA__", data)
        .replace("__CHIPS__", chips)
        .replace("__REGIONS__", region_opts)
        .replace("__N__", str(n_total))
        .replace("__NOPEN__", str(n_open))
        .replace("__NMETA__", str(n_meta))
    )


# Only what is genuinely page-local: <pre>, <table>, <code> and the headings all
# come from PAGE_CSS now (hf keeps exactly one spec for each, css/style.css:60-79
# and :270-289), which is what removed econ's four rival <pre> designs and its
# second table design.
_INFO_CSS = """
.wrap h2{margin-top:2rem}
.wrap table{margin:.8rem 0 1.2rem}
.wrap td{vertical-align:top}
"""


_LEAD_RE = re.compile(r'\s*<p class="lead">(.*?)</p>\s*', re.S)


def _info_page(title, meta_desc, page, body, og_type="website"):
    """One subpage shell: hf's navy masthead band, then the reading column.

    hfdatalibrary.com opens EVERY subpage with the same band (pages/docs.html:51-54)
    -- `background: var(--navy); color:#fff; padding: 3rem 0`, a white h1 and a
    rgba(255,255,255,.7) 1.1rem lead. econ used to render the h1 navy-on-white
    inside .wrap, which is why its subpages read as a different site. The page's
    own opening `<p class="lead">` becomes the band's subtitle (it is the same
    sentence, just promoted); pages without one fall back to their meta
    description, which is existing hand-written copy for that page.
    """
    head = render_head(
        title=f"{title} — {SITE_NAME}",
        meta_desc=meta_desc,
        canonical=site_url(page),  # /docs, not /docs.html (which 308-redirects)
        css=PAGE_CSS + _INFO_CSS,
        jsonld="",
        og_type=og_type,
    )
    m = _LEAD_RE.match(body)
    if m:
        lead, body = m.group(1), body[m.end():]
    else:
        lead = esc(meta_desc)
    return (head
            + '<section style="background: var(--navy); color: #fff; padding: 3rem 0;">\n'
            + '  <div class="container-narrow">\n'
            + f'    <h1 style="color:#fff; margin-bottom:0.5rem;">{title}</h1>\n'
            + f'    <p style="color: rgba(255,255,255,0.7); font-size:1.1rem; margin:0;">{lead}</p>\n'
            + '  </div>\n</section>\n'
            + f'<div class="wrap">\n{body}\n</div></body></html>')


def render_docs():
    body = """
<p class="lead">How the library works: one namespace, honest licensing, reproducible downloads, and a public update pipeline.</p>
<h2>The namespace</h2>
<p>Every series has a stable id of the form <code>source:series_key[:geography]</code> — for example <code>worldbank:NY.GDP.MKTP.CD:USA</code>. The id is permanent, appears in every download, and resolves over the API.</p>
<h2>Two catalog tiers</h2>
<p>Every source in the library has a license that permits re-hosting: its data is served from our store as citation-headed CSV, over the API, and in bundles, with license and attribution attached to every series. Sources whose licenses forbid re-hosting are not listed at all — we never catalog a dead-end we can't actually serve. Nothing restricted is ever silently redistributed.</p>
<h2>Reproducibility</h2>
<p>Bundles are snapshot-pinned: a bundle manifest records the snapshot date and the exact member series, so the same request reproduces the same data. Every CSV carries its license and producer-first citation in a comment header.</p>
<h2>The update pipeline</h2>
<p>Sources are refreshed by a cadence-aware pipeline (daily, weekly, monthly, annual — matching each publisher's own schedule). Freshness is never fabricated: a series' date advances only when observations were actually fetched, and failures surface on the public <a href="status.html">status board</a> rather than being hidden.</p>
<h2>Multilingual titles</h2>
<p>Series search is available in six languages (English, Arabic, Spanish, French, Russian, Chinese) using only the sources' official translations — titles are never machine-translated.</p>
<h2>One account, one family</h2>
<p>The free ElkassabgiData key works across the family — this library, <a href="https://hfdatalibrary.com">HF Data Library</a> (1-minute U.S. equity data) and <a href="https://ipdatalibrary.com">IP Data Library</a> (patent &amp; innovation measures). Get a key from the <a href="download.html">Download page</a>.</p>
"""
    return _info_page("Documentation", "How Econ Data Library works: namespace, licensing tiers, reproducible bundles, update pipeline.", "docs.html", body)


def render_api():
    api = "https://econdl-api.elkassabgi.workers.dev"
    body = f"""
<p class="lead">A free REST API over the full catalog. Search and metadata need no key; data downloads use a free key (<code>X-API-Key</code> header, <code>Authorization: Bearer</code>, or <code>?api_key=</code>).</p>
<h2>Base URL</h2>
<pre>{api}</pre>
<h2>Endpoints</h2>
<div class="table-wrap">
<table>
<thead><tr><th>Endpoint</th><th>What it returns</th><th>Key</th></tr></thead>
<tbody>
<tr><td><code>GET /v1/catalog</code></td><td>Series search. Params: <code>q</code>, <code>source</code>, <code>limit</code>, <code>offset</code>, <code>lang</code> (en/ar/es/fr/ru/zh).</td><td>No</td></tr>
<tr><td><code>GET /v1/series/{{id}}.csv</code></td><td>The series as tidy <code>date,value</code> CSV with license + citation header. Params: <code>from</code>, <code>to</code>, <code>raw=1</code> (bare CSV).</td><td>Yes</td></tr>
<tr><td><code>GET /v1/series/{{id}}.metadata.json</code></td><td>Full metadata: title, frequency, geography, unit, license (incl. commercial-use flag), attribution, coverage.</td><td>No</td></tr>
<tr><td><code>GET /v1/sources</code></td><td>Every source with license and freshness summary.</td><td>No</td></tr>
<tr><td><code>GET /v1/bundle</code></td><td>Snapshot-pinned bundle manifest (Frictionless datapackage). Params: <code>ids=</code> or <code>source=</code>, <code>snapshot=</code>.</td><td>No</td></tr>
<tr><td><code>GET /v1/stats</code></td><td>Live store-measured counts (series, observations, as-of date).</td><td>No</td></tr>
<tr><td><code>GET /v1/last-updates</code></td><td>Per-source freshness board (the data behind <a href="status.html">Status</a>).</td><td>No</td></tr>
</tbody>
</table>
</div>
<p>Requests for series we are not licensed to redistribute return HTTP <code>451</code> with the publisher's link — see <a href="docs.html">Documentation</a>.</p>
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
__LIB_CITATION__
<h2>Reproducibility note</h2>
<p>For exact reproducibility, cite the <em>bundle snapshot date</em> shown in your download's manifest — the same snapshot always resolves to the same data.</p>
"""
    # DOI-aware library citation: URL-only until ZENODO_DOI is minted, then the
    # permanent DOI becomes the canonical citation (same pattern as hf's cite page).
    if ZENODO_DOI:
        doi_url = f"https://doi.org/{ZENODO_DOI}"
        lib = f"""<div class="citation-block"><button class="copy-btn" onclick="copyBlock(this)">Copy</button><p>Elkassabgi, A. (2026). <em>Economic Data Library: Free Economic and Financial Data</em> (version 1.0) [Data set]. Zenodo. <a href="{doi_url}">{doi_url}</a></p></div>
<h2>BibTeX</h2>
<div class="citation-block"><button class="copy-btn" onclick="copyBlock(this)">Copy</button><pre><code>@dataset{{econdatalibrary,
  author    = {{Elkassabgi, Ahmed}},
  title     = {{{{Economic Data Library: Free Economic and Financial Data}}}},
  year      = {{2026}},
  version   = {{1.0}},
  publisher = {{Zenodo}},
  doi       = {{{ZENODO_DOI}}},
  url       = {{https://econdatalibrary.com}}
}}</code></pre></div>
<h2>Permanent DOI</h2>
<div class="citation-block"><button class="copy-btn" onclick="copyBlock(this)">Copy</button><p><a href="{doi_url}" style="font-family:var(--mono)">{ZENODO_DOI}</a></p></div>"""
    else:
        lib = """<div class="citation-block"><button class="copy-btn" onclick="copyBlock(this)">Copy</button><p>Elkassabgi, A. (2026). Economic Data Library: Free Economic and Financial Data. https://econdatalibrary.com</p></div>
<h2>BibTeX</h2>
<div class="citation-block"><button class="copy-btn" onclick="copyBlock(this)">Copy</button><pre><code>@misc{econdatalibrary,
  author = {Elkassabgi, Ahmed},
  title  = {Economic Data Library: Free Economic and Financial Data},
  year   = {2026},
  url    = {https://econdatalibrary.com}
}</code></pre></div>"""
    body = body.replace("__LIB_CITATION__", lib)
    body += """
<script>
function copyBlock(btn) {
  const block = btn.parentElement;
  const code = block.querySelector('pre code');
  const text = code ? code.innerText : block.querySelector('p').innerText;
  navigator.clipboard.writeText(text).then(() => {
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 2000);
  });
}
</script>
"""
    return _info_page("How to Cite", "Producer-first citations for every series, plus how to cite the Econ Data Library itself.", "cite.html", body, og_type="article")


def render_contact():
    # Mirrors hfdatalibrary.com/pages/contact, adapted to econ (series ids and
    # source requests instead of tickers). Same family contact email.
    body = """
<div style="text-align:center;margin:1.2rem 0 2.2rem">
  <h2 style="border:none;margin:0 0 .3rem;font-size:1.45rem">Ahmed Elkassabgi</h2>
  <p style="color:var(--g500);margin:.1rem 0">Assistant Professor of Finance</p>
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
<p>If you need a source or series that isn't included, email me the source, what you need from it, and the research use &mdash; sources are added in batches as licensing permits.</p>
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
    // (purged sources can never appear); names are the catalog's own.
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


# hfdatalibrary.com sets a <priority> on every sitemap URL; econ set none, so the
# hubs and the 232 source pages were indistinguishable to a crawler. Same scale.
SITEMAP_PRIORITY = {
    "": "1.0",
    "catalog": "0.9", "download": "0.9",
    "docs": "0.8", "api": "0.8", "mcp": "0.8",
    "cite": "0.7",
    "stats": "0.6",
    "contact": "0.5",
}


def sitemap_priority(loc):
    """Priority for one sitemap URL; per-source dataset pages get 0.6."""
    return SITEMAP_PRIORITY.get(loc[len(SITE_BASE):].strip("/"), "0.6")


def render_sitemap(entries):
    """Serialize the sitemap from already-decided (loc, lastmod, changefreq) rows.

    WHAT WAS WRONG: this function used to source the dates itself — `TODAY` for the
    two hub URLs and `r["last_updated"] or TODAY` for the datasets. Because
    `last_updated` (MAX(series.last_updated)) exists for 3 of 203 sources, 204 of
    207 URLs were re-dated on every single build; the last run rewrote 202 dates
    without changing one <loc>. Google discounts lastmod that behaves like this,
    so an unchanged page was paying a credibility cost to say nothing.
    Every date now arrives here already proven against the previous build's
    content hash (see _track_page / SITEMAP_STATE); this function only formats.

    It also used to carry exactly two non-dataset URLs and it hard-coded them with
    a `.html` extension the host 308-redirects. Both are now caller decisions.
    """
    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for loc, lastmod, changefreq in entries:
        parts.append("  <url>")
        parts.append(f"    <loc>{xml_esc(loc)}</loc>")
        parts.append(f"    <lastmod>{xml_esc(lastmod)}</lastmod>")
        parts.append(f"    <changefreq>{xml_esc(changefreq)}</changefreq>")
        parts.append(f"    <priority>{sitemap_priority(loc)}</priority>")
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
    # DISPLAY POLICY (owner decision 2026-07-22): the site shows a page ONLY
    # for sources whose data we DIRECTLY HOST (reservable + has series). There
    # are NO metadata-only listings — "if we can't host it, we don't mention
    # it." Sources awaiting a permission reply are simply ABSENT; each returns
    # as a full download page automatically the moment its license flips to
    # granted (reservable=1). Permission tracking is held privately,
    # not on the public site. (Supersedes the 2026-07-15 "keep pending as a
    # metadata-only reference" rule; refused sources like WTO stay purged.)
    # ------------------------------------------------------------------ #
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
        if bool(rec["reservable"]) and bool(series_roll.get(sid)):
            records.append(rec)

    _LASTMOD_PREV.update(_load_lastmod_state())
    if not _LASTMOD_PREV:
        print("  sitemap lastmod: no catalog/sitemap_state.json found -- every page "
              "gets today's date this run; commit the state file so the NEXT build "
              "can tell changed pages from unchanged ones")

    def _write(path, html):
        # single post-process point: stamp the generation date, and append the
        # ElkassabgiData family plate at the very bottom of EVERY page (below the
        # page's own footer), just before </body>.
        html = html.replace("</body>", FAMILY_BAND + FOOTER + "</body>", 1)
        # Fingerprint HERE -- after the family plate (a change to it IS a change to
        # the page) but BEFORE __SITE_UPDATED__ is substituted. That placeholder is
        # the one piece of every page that moves with the calendar rather than with
        # the content; hashing after substitution would mark all 200+ pages modified
        # every day and put us straight back to the meaningless lastmod this fixes.
        # This is what the spec asks for, not a loophole: lastmod is defined as the
        # date of the last SIGNIFICANT change, and a re-rendered "generated on"
        # stamp in the footer is the textbook example of an insignificant one.
        _track_page(os.path.basename(path), html)
        html = html.replace("__SITE_UPDATED__", TODAY)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        _WRITTEN.add(os.path.basename(path))

    # Per-dataset <title> / meta description. Computed ONCE over the full published
    # set because "no two pages share their first 60 characters" cannot be decided
    # from a single record; render_dataset_page reads the result out of PAGE_SEO.
    PAGE_SEO.update(build_page_seo(records))
    _t = {v[0][:TITLE_PREFIX_N] for v in PAGE_SEO.values()}
    print(f"  page titles: {len(PAGE_SEO)} pages, {len(_t)} distinct "
          f"{TITLE_PREFIX_N}-char prefixes")

    # --- structured-data coverage (so a silent regression is visible in the log) ---
    _vm = [r["id"] for r in records if r.get("measures")]
    print(f"  schema.org variableMeasured: {len(_vm)}/{len(records)} pages "
          f"(complete verbatim enumerations only; {len(records) - len(_vm)} omitted)")
    # A licence id with neither a URL nor a LICENSE_LABEL row makes jsonld_license
    # return None, so those pages publish NO licence rather than a raw token. Name
    # them: the fix is one label in LICENSE_LABEL, quoted from the verbatim audit.
    _nolic = sorted({r["license_id"] for r in records
                     if not jsonld_license(r) and r["license_id"]
                     and r["license_id"] != "NEEDS-REVIEW"})
    if _nolic:
        print(f"  WARNING: schema.org license OMITTED for license id(s) {_nolic} "
              "-- no license.url and no LICENSE_LABEL entry (add the label, quoted "
              "from DATABASE_LICENSES_VERBATIM.md; never publish the raw id)")

    # Per-dataset pages
    n_pages = 0
    for rec in records:
        _write(os.path.join(OUT_DIR, f"{rec['id']}.html"), render_dataset_page(rec))
        n_pages += 1

    # sources_meta.json -- the Status board's title + description lookup.
    #
    # WHY A SIDECAR AND NOT THE API. status.html reads /v1/last-updates, whose rows carry
    # source_id and nothing human: the board showed "abs", "imf_fsi", "cepii_gravity" and
    # left the reader to guess. The API's own /v1/sources does carry `name`, but the one-line
    # DESCRIPTION is the curated `subtitle` computed right here (SOURCE_SUBTITLES, falling
    # back to one derived from the source's own series titles) and it exists nowhere in D1.
    # Emitting it as a static file keeps the fix to the catalog build: no D1 migration, no
    # schema change to a pinned response shape, no worker deploy in the serving path.
    #
    # Scope is deliberately `records` -- the sources we actually HOST -- so this never becomes
    # a metadata-only listing of data we do not serve (display policy above). A ledger row
    # with no entry here simply keeps showing its id, exactly as it does today.
    meta_path = os.path.join(OUT_DIR, "sources_meta.json")
    meta = {r["id"]: {"name": r["name"], "desc": r["subtitle"] or ""} for r in records}
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    _no_desc = [k for k, v in meta.items() if not v["desc"]]
    print(f"  sources_meta.json: {len(meta)} source(s); "
          f"{len(_no_desc)} without a description" + (f" {_no_desc[:8]}" if _no_desc else ""))

    # index.html
    _write(os.path.join(OUT_DIR, "index.html"), render_index(records, generated))
    _write(os.path.join(OUT_DIR, "catalog.html"), render_catalog(records, generated))

    # docs / api / cite (hf-parity information pages)
    _write(os.path.join(OUT_DIR, "docs.html"), render_docs())
    _write(os.path.join(OUT_DIR, "api.html"), render_api())
    _write(os.path.join(OUT_DIR, "cite.html"), render_cite())
    _write(os.path.join(OUT_DIR, "contact.html"), render_contact())
    _write(os.path.join(OUT_DIR, "stats.html"), render_stats())

    # ---- sitemap.xml --------------------------------------------------------
    # WHAT WAS WRONG: the sitemap held the two hub URLs plus the dataset pages and
    # NOTHING else, so eight real, indexable pages (docs, api, cite, contact,
    # stats, download, mcp, status) were invisible to it. Every URL it did hold
    # ended in .html, which the host 308-redirects (see site_url).
    # The candidate list is filtered against two facts measured from the emitted
    # bytes, not from a maintained exclusion list: the file has to exist, and it
    # must not carry a robots noindex. That is what keeps /account.html, /404.html
    # and (today) /status.html out, and what will let status.html back in by
    # itself if its noindex is ever removed.
    entries, skipped = [], []
    for name, changefreq in SITEMAP_EXTRA:
        path = os.path.join(OUT_DIR, name)
        if not os.path.isfile(path):
            skipped.append(f"{name} (absent)")
            continue
        if name not in _LASTMOD:
            # Hand-maintained page (KEEP_UNGENERATED): fingerprint it off disk so
            # it obeys exactly the same "unchanged content keeps its date" rule.
            with open(path, encoding="utf-8") as f:
                _track_page(name, f.read())
        if _LASTMOD[name]["noindex"]:
            skipped.append(f"{name} (noindex)")
            continue
        entries.append((site_url(name), _LASTMOD[name]["lastmod"], changefreq))
    for r in records:
        entries.append((r["page_url"], _LASTMOD[f"{r['id']}.html"]["lastmod"], "weekly"))

    with open(os.path.join(OUT_DIR, "sitemap.xml"), "w", encoding="utf-8") as f:
        f.write(render_sitemap(entries))

    # Persist the fingerprints for the next build. Lives in catalog/ (not site/) so
    # it is neither deployed nor removed by the orphan sweep. COMMIT IT: it is the
    # only memory of which pages were unchanged.
    with open(SITEMAP_STATE, "w", encoding="utf-8") as f:
        json.dump({"generated": TODAY, "pages": _LASTMOD}, f,
                  indent=1, sort_keys=True, ensure_ascii=False)

    # Audit line: a healthy steady-state build reports a SMALL changed count. If it
    # ever reports "changed on all N", something volatile has crept back into the
    # page body and lastmod has quietly gone back to being worthless.
    _changed = [n for n, e in _LASTMOD.items()
                if (_LASTMOD_PREV.get(n) or {}).get("sha256") != e["sha256"]]
    print(f"  sitemap: {len(entries)} URLs, all extensionless (the form the host "
          f"serves 200 for); content changed on {len(_changed)}/{len(_LASTMOD)} "
          f"tracked pages -> those get lastmod {TODAY}, the rest keep theirs"
          + (f"; skipped {', '.join(skipped)}" if skipped else ""))

    # robots.txt — allow crawlers, keep the account page out of the index, and
    # point them at the sitemap (hfdatalibrary parity; previously the SPA served
    # index.html for /robots.txt, so there was no Sitemap directive for crawlers).
    # WHAT WAS WRONG: the rule read `Disallow: /account.html`, but the host serves
    # the sign-in page at /account and 308-redirects the .html form, so the only
    # URL a crawler ever sees was NOT covered by the rule. robots.txt matching is
    # by path prefix, so `/account` covers both /account and /account.html.
    with open(os.path.join(OUT_DIR, "robots.txt"), "w", encoding="utf-8") as f:
        f.write("User-agent: *\nAllow: /\nDisallow: /account\n\n"
                f"Sitemap: {SITE_BASE}/sitemap.xml\n")

    # ---- orphan sweep -------------------------------------------------------------
    # gen_site historically never cleaned OUT_DIR, so pages for sources that are no longer
    # generated (e.g. one we stopped hosting) lingered and got deployed with the rest. That
    # is exactly how "Metadata only" pages stayed LIVE after the display gate changed (163
    # stale strings across 36 orphan pages, 2026-07-22). The sweep runs LAST, so if any
    # render above raised, nothing has been deleted.
    orphans = sorted(
        fn for fn in os.listdir(OUT_DIR)
        if fn.endswith(".html")
        and fn not in _WRITTEN
        and fn not in KEEP_UNGENERATED
        and os.path.isfile(os.path.join(OUT_DIR, fn))
    )
    for fn in orphans:
        os.remove(os.path.join(OUT_DIR, fn))
    if orphans:
        print(f"  removed {len(orphans)} orphaned page(s) from a previous run: "
              + ", ".join(orphans[:8]) + (" ..." if len(orphans) > 8 else ""))
    else:
        print("  no orphaned pages (output dir clean)")

    n_open = sum(1 for r in records if r["reservable"])
    print(f"Wrote {n_pages} dataset pages to {OUT_DIR}")
    print(f"  redistributed (with distribution): {n_open}")
    print(f"  metadata-only (no distribution):   {n_pages - n_open}")
    print(f"Wrote index.html and sitemap.xml")
    print(f"Sitemap lists {len(entries)} URLs "
          f"({len(entries) - n_pages} hub/info + {n_pages} datasets)")


if __name__ == "__main__":
    main()
