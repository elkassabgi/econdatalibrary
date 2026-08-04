#!/usr/bin/env python3
"""Deterministically title raw UNCTADstat source unctad_cioiuibbicoeair4a.

Authoritative label source: DBnomics faithful mirror of UNCTADstat labels for
dataset UNCTAD/CIOIUIBBICOEAIR4A
("Core indicators on ICT use in business by industrial classification of
economic activity (ISIC Rev. 4), annual").

Dimensions (order from DBnomics dimensions_codes_order):
  1. frequency   -> {"A": "Annual", "Q": "Quarterly"}  (constant "A"; dropped from title, per sibling style)
  2. use-of-ict  -> the ICT-use indicator (the varying / leading dimension)

Each catalog series_id has the form:
  unctad_cioiuibbicoeair4a:UNCTAD_CIOIUIBBICOEAIR4A:<frequency>.<use-of-ict>

Title style (mirrors sibling unctad_rfia.json): lead with the indicator label.
Here the indicator IS the use-of-ict dimension, so the title is exactly its
official VERBATIM label. No unit dimension exists. Every code-derived token is
copied verbatim from DBnomics dimensions_values_labels; any series whose code
lacks an official label is omitted (left raw).
"""
import json
import os
import sqlite3
import sys
import urllib.request

# THIS SCRIPT IS DISABLED. It sources its labels from api.db.nomics.world, and DBnomics is
# banned outright by CLAUDE.md §0 — no fetching, no probing, no "just for labels". It predates
# that rule. Nothing it previously wrote is deleted by the ban (§0 is explicit that existing
# DBnomics-derived data stays until migrated), but it must not RUN again, and leaving a working
# banned-path script lying around is how a rule gets violated by someone who never read it.
# To re-enable: re-point DBNOMICS_URL at UNCTADstat itself. Note task #70 — UNCTAD re-coded
# every dataset id and 0 of 38 still match, so that is a real migration, not a URL swap.
if os.environ.get("ECONDL_ALLOW_DBNOMICS") != "i-have-read-CLAUDE-md-section-0":
    sys.exit("title_unctad_cioiuibbicoeair4a: DISABLED — sources labels from DBnomics, "
             "banned by CLAUDE.md §0. Re-point at UNCTADstat instead (see task #70).")

# Derived from this file, never a drive letter (R330).
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_DB = os.path.join(_ROOT, "data", "catalog.db")
SOURCE_ID = "unctad_cioiuibbicoeair4a"
DBNOMICS_URL = (
    "https://api.db.nomics.world/v22/series/UNCTAD/CIOIUIBBICOEAIR4A"
    "?limit=0&observations=false"
)
OUT_PATH = os.path.join(_ROOT, "dist", "titles", "unctad_cioiuibbicoeair4a.json")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


def fetch_dbnomics():
    req = urllib.request.Request(DBNOMICS_URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    meta = fetch_dbnomics()
    ds = meta["dataset"]
    order = ds["dimensions_codes_order"]
    assert order == ["frequency", "use-of-ict"], order
    dvl = ds["dimensions_values_labels"]
    ict_labels = dvl["use-of-ict"]  # code -> VERBATIM official label

    con = sqlite3.connect(CATALOG_DB)
    series_ids = sorted(
        r[0]
        for r in con.execute(
            "SELECT series_id FROM series WHERE source_id=?", (SOURCE_ID,)
        )
    )
    con.close()

    titles = {}
    for sid in series_ids:
        key = sid.split(":", 2)[2]
        segs = key.split(".")
        if len(segs) != 2:
            continue  # unexpected shape -> leave raw
        _freq_code, ict_code = segs
        label = ict_labels.get(ict_code)
        if not label:
            continue  # no official label -> omit (leave raw)
        # VERBATIM: only trim pure leading/trailing whitespace.
        titles[sid] = label.strip()

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(titles, f, ensure_ascii=False, indent=2, sort_keys=True)

    print(f"titled {len(titles)} of {len(series_ids)} series -> {OUT_PATH}")


if __name__ == "__main__":
    main()
