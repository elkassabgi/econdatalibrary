"""Map our DBnomics-era IMF series ids onto IMF's modern direct-API keys.

WHY THIS EXISTS: IMF retired IFS and rebuilt its datasets with new vocabularies for
BOTH country and indicator, so no string transform relates the two. A first attempt
that simply reordered our dotted key scored 0 of 1,728. But IMF's SDMX codelists
carry English names, and our catalog titles are "<indicator name> - <country name>",
so the two can be joined on MEANING.

That join is what decides whether going direct costs us our series identities. With
it we keep every existing id and change only where the bytes come from; without it a
migration re-keys 333,947 series and breaks every saved link, notebook and MCP config
pointing at them.

Deliberately reports THREE outcomes, never two — the distinction is the whole point:
  mapped              our id has a direct-API counterpart
  unmapped_naming     we could not resolve a name (OUR bug — fixable here)
  absent_upstream     the name resolved fine but IMF does not publish that series
                      (NOT our bug; switching would genuinely lose it)
Collapsing the last two into "failed" is how a migration silently drops data.
"""
from __future__ import annotations

import collections
import re
import urllib.request
import xml.etree.ElementTree as ET

UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
XL = "{http://www.w3.org/XML/1998/namespace}lang"
BASE = "https://api.imf.org/external/sdmx/2.1"

# Aggregate wordings that differ between the DBnomics-era titles and IMF's codelist.
# Evidence-based: every entry below was observed in our imf_fdi titles failing to
# resolve against CL_COUNTRY. Kept as data, not buried in branching logic, so the
# next dataset can extend it without touching the algorithm.
AGGREGATE_ALIASES = {
    "all countries": "world",
    "advanced markets": "advanced economies",
    "emerging markets": "emerging market and developing economies",
    "low-income and developing countries": "low-income developing countries (lidc)",
    "emerging and developing asia": "emerging asia",
}

_PAREN = re.compile(r"\s*\([^)]*\)")
_SUFFIX = re.compile(
    r",\s*(republic of the|republic of|union of the|kingdom of the|kingdom of|"
    r"state of|islamic republic of|people's republic of|federal republic of|"
    r"the)\s*$", re.I)


def norm(name: str) -> str:
    """Normalise a country/indicator name for joining.

    Handles the variants actually seen: "Comoros, Union of the" vs "Comoros",
    "Korea, Republic of" vs "Korea", parenthetical qualifiers, and punctuation.
    """
    s = (name or "").strip().lower()
    s = _SUFFIX.sub("", s)
    s = _PAREN.sub("", s)
    s = s.replace("&", "and").replace(".", "").replace("'", "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def fetch_codelists(agency: str, dsd: str) -> dict:
    """{codelist_id: {code: english_name}} for one DSD, references=all."""
    url = f"{BASE}/datastructure/{agency}/{dsd}?references=all"
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=240) as r:
        root = ET.fromstring(r.read())
    out: dict = collections.defaultdict(dict)
    cur = None
    for e in root.iter():
        t = e.tag.split("}")[-1]
        if t == "Codelist":
            cur = e.get("id")
        elif t == "Code" and cur:
            en = ""
            for c in e:
                if c.tag.split("}")[-1] == "Name" and c.get(XL) == "en":
                    en = (c.text or "").strip()
                    break
            if en:
                out[cur][e.get("id")] = en
    return dict(out)


def build_lookup(codes: dict) -> dict:
    """{normalised name: code}, with aggregate aliases folded in."""
    by_name = {}
    for code, name in codes.items():
        by_name.setdefault(norm(name), code)
    for ours, theirs in AGGREGATE_ALIASES.items():
        c = by_name.get(norm(theirs))
        if c:
            by_name.setdefault(norm(ours), c)
    return by_name


def crosswalk(catalog_rows, direct_keys, flow, country_cl, indicator_cl):
    """catalog_rows: [(series_id, title)]  ->  (mapping, stats)

    mapping: our series_id -> direct series_key, only where the direct key EXISTS.
    """
    ctry = build_lookup(country_cl)
    ind = build_lookup(indicator_cl)
    mapping = {}
    unmapped_naming, absent_upstream = [], []
    for sid, title in catalog_rows:
        if not title or " - " not in title:
            unmapped_naming.append((sid, title, "no '<indicator> - <country>' title"))
            continue
        iname, cname = title.rsplit(" - ", 1)
        ic, cc = ind.get(norm(iname)), ctry.get(norm(cname))
        if not ic or not cc:
            miss = "indicator" if not ic else "country"
            unmapped_naming.append((sid, title, f"unresolved {miss} name"))
            continue
        freq = sid.split(":")[-1].split(".")[0]
        cand = f"{flow}:{cc}.{freq}.{ic}"
        if cand in direct_keys:
            mapping[sid] = cand
        else:
            # Names resolved; IMF simply does not publish this combination.
            absent_upstream.append((sid, title, cand))
    return mapping, {
        "mapped": len(mapping),
        "unmapped_naming": unmapped_naming,
        "absent_upstream": absent_upstream,
        "total": len(catalog_rows),
    }
