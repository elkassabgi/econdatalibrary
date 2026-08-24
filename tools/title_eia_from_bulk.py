#!/usr/bin/env python3
"""Compose dist/titles/eia.json from EIA's OWN bulk files.

All 268,502 eia catalogue rows carry a title identical to their id (a fallback, not a
label), so none can be found by name. EIA publishes the official name for every v1-style
series id in api.eia.gov/bulk/<FAMILY>.zip (JSON Lines: `series_id`, `name`).

THE JOIN IS ON THE ID MINUS ITS FREQUENCY SEGMENT. Bulk ids end in a frequency
("PET.WGTIMUS2.W"); our catalogue id is the base ("PET.WGTIMUS2"), because ONE catalogue
row is a container for that base's frequency variants - the served CSV holds them all and
its series_id column keeps them apart (verified on PET.WGTIMUS2: rows for .4 and .W, each
tagged). Measured: every one of our 93,154 coded PET ids is a store base, and no store
series carries two frequencies.

SO AN "AMBIGUOUS" BASE IS NOT A CONFLICT, IT IS A GROUP. Its variants are the publisher's
same label differing only in the frequency qualifier:
    PET.WGTIMUS2.4  'U.S. Imports of Total Gasoline, 4 Week Avg'
    PET.WGTIMUS2.W  'U.S. Imports of Total Gasoline, Weekly'
The title for the group is their SHARED PREFIX - still EIA's own words, just the part all
variants agree on, never a merged or invented phrase. A prefix under 12 characters is
refused rather than padded, and a single name is used verbatim. Measured: 140,271 of
140,272 grouped bases yield a usable prefix.
"""
import json, os, zipfile, collections, sqlite3
import requests

ROOT = r"E:\research\econfindatalibrary"
CACHE = r"D:\temp\claude\eia"
FAMS = ["INTL","PET","ELEC","PET_IMPORTS","NG","EMISS","STEO","TOTAL","SEDS","EBA","IEO","COAL"]
MIN_PREFIX = 12

def lcp(strs):
    s1, s2 = min(strs), max(strs)
    i = 0
    while i < len(s1) and i < len(s2) and s1[i] == s2[i]: i += 1
    return s1[:i]

os.makedirs(CACHE, exist_ok=True)
con = sqlite3.connect(os.path.join(ROOT, "data", "catalog.db"), timeout=300)
ours = {r[0].split(":",1)[1]: r[0] for r in
        con.execute("SELECT series_id FROM series WHERE source_id='eia'")}
print("catalogue eia ids: %s" % format(len(ours), ","), flush=True)

titles, refused = {}, 0
for fam in FAMS:
    p = os.path.join(CACHE, fam + ".zip")
    if not os.path.exists(p):
        r = requests.get("https://api.eia.gov/bulk/%s.zip" % fam, timeout=1800, stream=True)
        if r.status_code != 200:
            print("  %-12s HTTP %s - skipped" % (fam, r.status_code), flush=True); continue
        with open(p, "wb") as fh:
            for chunk in r.iter_content(1 << 20): fh.write(chunk)
    base = collections.defaultdict(set)
    full = {}
    try: z = zipfile.ZipFile(p)
    except Exception as e:
        print("  %-12s bad zip: %s" % (fam, str(e)[:60]), flush=True); continue
    for member in z.namelist():
        if not member.endswith(".txt"): continue
        with z.open(member) as fh:
            for line in fh:
                s = line.decode("utf-8", "replace").strip()
                if not s.startswith("{"): continue
                try: d = json.loads(s)
                except Exception: continue
                sid, nm, un = d.get("series_id"), d.get("name"), d.get("units")
                if not (sid and nm): continue
                # UNITS DISAMBIGUATE WHAT EIA'S NAME ALONE DOES NOT. INTL.11-1-BRA-MT,
                # -MTOE, -QBTU and -TJ are one measure in four units and EIA names all
                # four "Anthracite production, Brazil, Annual" - ten catalogue rows would
                # share a title. The unit is EIA's own field, so appending it stays
                # verbatim; it is skipped when the name already carries it.
                if un and un.strip() and un.strip().lower() not in nm.lower():
                    nm = "%s (%s)" % (nm, un.strip())
                base[sid.rsplit(".",1)[0]].add(nm)
                full[sid] = nm
    exact = grouped = short = 0
    for b, names in base.items():
        cid = ours.get(b)
        if not cid: continue
        if len(names) == 1:
            titles[cid] = next(iter(names)); exact += 1
        else:
            pre = lcp(list(names)).rstrip(" ,;:-(")
            if len(pre) >= MIN_PREFIX: titles[cid] = pre; grouped += 1
            else: short += 1; refused += 1

    # THIRD PASS - CATALOGUE IDS THAT ARE A COARSER GRAIN THAN THE BULK FILE. Some eia rows
    # are a PREFIX of a whole family rather than a series: `SEDS.ABICB` fronts 52 bulk ids
    # (SEDS.ABICB.AK.A, .AL.A, ...), one per state, all named the same thing bar the state.
    # The first two passes key on the id minus its frequency segment and never see these, so
    # 2,364 rows kept a bare code. Title them with the shared prefix of the family's names -
    # still EIA's words, just the part every member agrees on.
    prefixed = 0
    remaining = [b for b in ours if ours[b] not in titles]
    if remaining:
        by_prefix = {}
        for sid, nm in full.items():
            for cut in range(2, sid.count(".") + 1):
                by_prefix.setdefault(sid.rsplit(".", cut)[0], set()).add(nm)
        for b in remaining:
            names = by_prefix.get(b)
            if not names:
                continue
            pre = (next(iter(names)) if len(names) == 1
                   else lcp(list(names)).rstrip(" ,;:-("))
            if len(pre) >= MIN_PREFIX:
                titles[ours[b]] = pre
                prefixed += 1
    if prefixed:
        print("  %-12s prefix-matched a further %s coarser-grain id(s)" % (fam, format(prefixed, ",")), flush=True)
    print("  %-12s bulk_bases=%-9s exact=%-8s grouped=%-8s refused=%s"
          % (fam, format(len(base),","), format(exact,","), format(grouped,","), short), flush=True)

out = os.path.join(ROOT, "dist", "titles", "eia.json")
json.dump(titles, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=0, sort_keys=True)
print("wrote %s with %s titles (refused: %s)" % (out, format(len(titles), ","), refused), flush=True)
