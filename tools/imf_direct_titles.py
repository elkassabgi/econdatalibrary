"""Human titles for the imf_*_direct family, decoded from IMF's own codelists.

WHY. 21,382 catalogued imf_*_direct series carry a title that IS the key
(`AFRREO:AGO.A.BCA_GDP_BP6.BPM6`), and the six GFS direct sources hold data with no catalogue
rows at all. A catalogue whose titles are codes is searchable by nobody, and the publisher
already ships the fix: `dataflow/<agency>/<flow>?references=all` is a few MB carrying the DSD
and every codelist the flow uses.

THE KEY IS POSITIONAL, AND THE ORDER IS THE WHOLE GAME. jobs/ingest_imf_direct.pull() builds
it from `sorted()` over the attributes the DATA carries. That is NOT the DSD's declared order,
and the DSD also omits attributes that appear in the data - METHODOLOGY is absent from the GFS
datastructure yet present in every GFS key. Read the order off the DSD and every part after
the missing one shifts: `S1311B`, a SECTOR code, gets labelled TYPE_OF_TRANSFORMATION and the
title comes out confidently wrong while looking fine.

So the order comes from, in preference:
  1. <source>.parquet.dims.json, written by the ingest at the moment it knew (358f37a);
  2. failing that, a SEARCH over candidate orderings, accepting one only if it resolves EVERY
     non-blank part of a sample of real keys. A partial match is rejected rather than used,
     because a decoder that resolves most parts produces titles that are wrong in a way nobody
     notices.

--sample IS FOR ESTABLISHING THE KEY ORDER, NOT FOR PRODUCING TITLES. The order is a property
of the flow, so a handful of real keys settles it and the same order then applies to every
series. When this feeds the catalogue build, titles are generated for ALL series - the sample
never becomes the deliverable.

Usage:
    python tools/imf_direct_titles.py --source imf_afrreo_direct --flow AFRREO --agency IMF.AFR
    python tools/imf_direct_titles.py --source imf_gfssoef_direct --flow GFS_SOEF --sample 20
"""
from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

UA = {"User-Agent": "Econ-Fin Data Library admin@econdatalibrary.com"}
BASE = "https://api.imf.org/external/sdmx/2.1"
NON_IDENTITY = {"TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "SCALE", "UNIT_MULT",
                "COMMENT", "TIME_FORMAT"}


def _t(el):
    return el.tag.split("}")[-1]


def _name(el):
    for ch in el:
        if _t(ch) == "Name":
            lang = ch.get("{http://www.w3.org/XML/1998/namespace}lang")
            if lang in (None, "en"):
                return (ch.text or "").strip()
    return ""


def load_structure(flow: str, agency: str):
    """-> (dsd_dims, {dim_id: {code: name}}). Codelists come via the CONCEPT, not the
    Dimension: Dimension -> ConceptIdentity/Ref -> Concept -> CoreRepresentation ->
    Enumeration -> Ref. Looking directly under Dimension finds nothing and reads as
    'this flow has no codelists'."""
    req = urllib.request.Request(f"{BASE}/dataflow/{agency}/{flow}?references=all",
                                 headers=UA)
    root = ET.fromstring(urllib.request.urlopen(req, timeout=300).read())

    codelists = {}
    for cl in root.iter():
        if _t(cl) != "Codelist":
            continue
        codes = {c.get("id"): _name(c) for c in cl if _t(c) == "Code" and c.get("id")}
        if codes:
            codelists[cl.get("id")] = codes

    concept_cl = {}
    for con in root.iter():
        if _t(con) != "Concept":
            continue
        for sub in con.iter():
            if _t(sub) == "Ref" and sub.get("class") == "Codelist":
                concept_cl[con.get("id")] = sub.get("id")

    dims, dim_codes = [], {}
    for d in root.iter():
        if _t(d) not in ("Dimension", "TimeDimension") or not d.get("id"):
            continue
        did = d.get("id")
        dims.append(did)
        cref = did
        for sub in d:
            if _t(sub) == "ConceptIdentity":
                for r in sub.iter():
                    if _t(r) == "Ref" and r.get("id"):
                        cref = r.get("id")
        cl = concept_cl.get(cref) or concept_cl.get(did)
        if not (cl and cl in codelists):
            # STANDARD-SDMX FALLBACK (found via imf_sdg_direct, cycle 10): flows built on
            # external structures (SDG references IAEG-SDGs codelists) enumerate on the
            # DIMENSION's LocalRepresentation, not the concept's CoreRepresentation — the
            # concept path above finds nothing and every codelist reads as empty. Read the
            # Enumeration ref directly under the dimension; concept path stays first so
            # the existing IMF-native flows resolve exactly as before.
            for r in d.iter():
                if (_t(r) == "Ref" and r.get("class") == "Codelist"
                        and r.get("id") in codelists):
                    cl = r.get("id")
                    break
        if cl and cl in codelists:
            dim_codes[did] = codelists[cl]
    return sorted(set(dims) - NON_IDENTITY), dim_codes


def load_dims(source_id: str, store_path: str) -> list[str] | None:
    """The authoritative key order, if the ingest recorded it.

    READ IT THE WAY IT IS WRITTEN. This tested os.path.exists and open()ed the local file, so
    under AQUEDUCT_BACKEND=r2 - the mode the catalogue actually runs in - it never saw a sidecar
    published to R2, returned None, and handed the job to infer_dims, which GUESSES the order
    the ingest had recorded exactly. That guess is only as good as the codelists: it costs
    imf_cpi_direct a dimension (CPI read as a METHODOLOGY, not an INDEX_TYPE) and it cannot
    place imf_bop_direct at all - 7 key parts against 5 codelisted dims, so no ordering resolves
    every part and all 260,931 series fall back to their raw key.

    Falls back to the local path so a store written under the local backend still decodes.
    """
    p = store_path + ".dims.json"
    try:
        from updater import blob
        raw = blob.read_bytes(p)
        if raw:
            return json.loads(raw.decode("utf-8")).get("key_dims")
    except Exception:                                            # noqa: BLE001
        pass                                                     # local fallback below
    if not os.path.exists(p):
        return None
    try:
        return json.load(open(p, encoding="utf-8")).get("key_dims")
    except Exception:                                            # noqa: BLE001
        return None


def infer_dims(sample_keys, dsd_dims, dim_codes, extra_pool=("METHODOLOGY", "SECTOR",
                                                             "UNIT", "COUNTERPART_SECTOR")):
    """Find a key order that resolves EVERY non-blank part of every sample key.

    Only used when the dims sidecar is absent (stores written before the ingest recorded it).
    Accepts a candidate ONLY on total resolution: a partial match is how a decoder ends up
    labelling a SECTOR code as a TYPE_OF_TRANSFORMATION.
    """
    n_parts = {len(k.split(":", 1)[1].split(".")) for k in sample_keys}
    if len(n_parts) != 1:
        return None
    want = n_parts.pop()
    pool = sorted(set(dsd_dims) | {e for e in extra_pool})

    # SCORE, do not take the first that passes. Accepting the first valid ordering picks
    # whichever candidate sorts earliest, and a dimension with NO codelist is never checked -
    # so `COUNTERPART_SECTOR` (no codelist) beat `COUNTRY` alphabetically, AGO was assigned to
    # it, went unverified, and the title read "AGO" where it should have read "Angola". The
    # ordering was wrong and every test still passed.
    #
    # Ranking by how many parts actually RESOLVE fixes that: the correct assignment resolves
    # AGO against COUNTRY's codelist and scores higher. A tie means two orderings explain the
    # data equally well, and guessing between them is exactly how a key silently mislabels -
    # so a tie refuses.
    scored = []
    for combo in itertools.combinations(pool, want):
        order = sorted(combo)
        if not all(_resolves_fully(k, order, dim_codes) for k in sample_keys):
            continue
        score = sum(title_for(k, order, dim_codes)[1] for k in sample_keys)
        scored.append((score, order))
    if not scored:
        return None
    best_score = max(s for s, _ in scored)
    top = [o for s, o in scored if s == best_score]

    # A TIE ONLY MATTERS IF IT CHANGES THE ANSWER. The candidates tie precisely on the
    # dimensions that have NO codelist, and those emit their raw code as the label whichever
    # slot they occupy - so several orderings can be indistinguishable from the data yet
    # produce byte-identical titles. Refusing there would block a title that is not in doubt.
    # Compare the OUTPUT, not the ordering: identical titles -> take the first; genuinely
    # different titles -> refuse, because choosing between them would be a guess about what
    # the data means.
    titles = {tuple(title_for(k, o, dim_codes)[0] for k in sample_keys) for o in top}
    if len(titles) > 1:
        print(f"  {len(top)} orderings fit the sample and produce {len(titles)} DIFFERENT "
              f"titles - refusing to choose between them", flush=True)
        return None
    if len(top) > 1:
        print(f"  {len(top)} orderings fit, but all produce identical titles - proceeding",
              flush=True)
    return top[0]


def _resolves_fully(key, order, dim_codes) -> bool:
    """Does this ordering resolve every part that COULD be resolved?

    The test is deliberately not "every part gets a name". Some dimensions carry no codelist
    at all - AFRREO's METHODOLOGY is one, where the code IS the label (BPM6) - so demanding a
    name for them rejects the correct order along with the wrong ones, which is exactly what
    happened on the first attempt.

    Requiring every CODELIST-BACKED dimension to resolve is still a strong test: under a wrong
    ordering a COUNTRY code lands on the INDICATOR codelist and fails, so bad orders are
    rejected. Dimensions with no codelist are accepted as-is and their raw code is used as the
    label - never invented.
    """
    parts = key.split(":", 1)[1].split(".")
    if len(parts) != len(order):
        return False
    checked = 0
    for did, code in zip(order, parts):
        if code == "" or did not in dim_codes:
            continue
        if not dim_codes[did].get(code):
            return False
        checked += 1
    # an ordering that verified nothing has proved nothing
    return checked > 0


def title_for(key, order, dim_codes) -> tuple[str, int, int]:
    parts = key.split(":", 1)[1].split(".")
    bits, hit, tot = [], 0, 0
    for did, code in zip(order, parts):
        # "_T" is the SDMX total/no-breakdown placeholder (found on imf_sdg_direct, whose
        # 16-part keys carry ~12 of them): decoding each to "No breakdown" buries the
        # country and indicator under noise. Skip it like the empty part — IMF-native
        # flows never use "_T" as a code, so this is inert for every existing source.
        if code in ("", "_T"):
            continue
        if did in dim_codes:
            tot += 1                                             # only count what CAN resolve
        nm = dim_codes.get(did, {}).get(code)
        if nm:
            hit += 1
            bits.append(nm)
        else:
            bits.append(code)                                    # never invent a label
    return " — ".join(bits), hit, tot


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--flow", required=True)
    ap.add_argument("--agency", default="IMF.STA")
    ap.add_argument("--sample", type=int, default=10)
    a = ap.parse_args()

    from updater import config
    store = os.path.join(config.source_dir(a.source), f"{a.source}.parquet")

    import sqlite3
    con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"))
    keys = [r[0].split(":", 1)[1] for r in con.execute(
        "select series_id from series where source_id=? limit ?", (a.source, a.sample))]
    if not keys:
        # BLOB-ROUTED, not os.path/pq direct. The sources that most need titles are exactly the
        # ones whose store lives only in R2 - the six GFS flows were written by a CI runner and
        # have no local copy - and a raw local read reports "no store" for data that plainly
        # exists (ledger R36). blob.exists/read_table honour AQUEDUCT_BACKEND and work in both
        # places.
        from updater import blob
        if not blob.exists(store):
            print(f"no catalogue rows and no store for {a.source} "
                  f"(looked via blob at {store}; set AQUEDUCT_BACKEND=r2 if it is in R2)")
            return 1
        t = blob.read_table(store, columns=["series_key"])
        keys = list(dict.fromkeys(t.column("series_key").to_pylist()))[:a.sample]
    print(f"{len(keys)} sample key(s), e.g. {keys[0]}")

    dsd_dims, dim_codes = load_structure(a.flow, a.agency)
    print(f"DSD dims: {dsd_dims}")
    print(f"dims with a codelist: {sorted(dim_codes)}")

    order = load_dims(a.source, store)
    if order:
        print(f"key order from the ingest sidecar: {order}")
    else:
        order = infer_dims(keys, dsd_dims, dim_codes)
        print(f"key order INFERRED (sidecar absent): {order}")
    if not order:
        print("could not establish the key order - refusing to guess a title")
        return 1

    print()
    ok = 0
    for k in keys:
        t, hit, tot = title_for(k, order, dim_codes)
        full = hit == tot
        ok += full
        print(f"  {'OK ' if full else 'PARTIAL'} {k}")
        print(f"      {t}   ({hit}/{tot} parts resolved)")
    print(f"\n{ok} of {len(keys)} fully resolved")
    return 0 if ok == len(keys) else 1


if __name__ == "__main__":
    sys.exit(main())
