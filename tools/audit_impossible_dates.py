"""Find observations dated beyond any possible publication horizon, across every store.

WHY. Nothing in this system asked whether a value was POSSIBLE — every instrument measures
RECENCY, and a fabricated FUTURE date passes that trivially by making a source look maximally
fresh. health.py deliberately filters forward-dated periods out of the recency signal so real
projections (ABS 2046/2071, UN WPP 2101, IMF WEO 2031) do not cry wolf, which means the same
mechanism conceals fabrication. cso served 434,408 such rows — 272,445 in Census 2016 at
9998-12-31 — until someone read the store by hand (ledger R265).

merge_and_write now announces this at WRITE time, so new data is covered permanently. This tool
is for the BACKLOG: data already on disk, written before that check existed.

READS STATISTICS, NOT ROWS. Parquet keeps per-row-group min/max for obs_date, so a file is
judged from its footer. That is the difference between a scan you will actually run and one you
will not.

WHICH STORE IS AUTHORITATIVE. Under --local it reads $ECONDL_DATA. For a source with
run_location: local that IS the store. For a CLOUD source the local directory is only a scratch
mirror of whatever the last run wrote (see blob.py / orchestrate.py), so a local-only scan can
UNDER-report — it reports the file-count difference so the gap is visible rather than assumed.
Under --r2 it reads the real objects, which costs a GET per file.

    python tools/audit_impossible_dates.py --local              # fast sweep, all sources
    python tools/audit_impossible_dates.py --r2 --source cso    # authoritative, one source
"""
from __future__ import annotations
import argparse
import datetime as dt
import glob
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# MAKE `--r2` MEAN R2 — before updater.config is imported and freezes BACKEND from the env.
#
# It did not, and the flag lied. `--r2` only chose blob.list_parquets() over a local glob, and
# blob honours config.BACKEND, which comes from AQUEDUCT_BACKEND. So
#
#     python tools/audit_impossible_dates.py --r2 --source scb
#
# read the LOCAL tree and printed "(r2)" over it. Not a cosmetic mislabel: under
# AQUEDUCT_BACKEND=r2 the local tree is a scratch mirror of the LAST RUN ONLY (R296/R36), so it
# is systematically CLEANER than the store users download — a repair writes to R2, the audit
# then reads local, and the all-clear describes a directory nobody is served from. This was the
# verification used to confirm a 104,501-row prune, and it was measuring the wrong store.
#
# Setting the env var here keeps ONE source of truth, and the resolved backend is printed in the
# header so it never has to be assumed again. R330/R296.
if "--r2" in sys.argv:
    os.environ["AQUEDUCT_BACKEND"] = "r2"
elif "--local" in sys.argv:
    os.environ["AQUEDUCT_BACKEND"] = "local"

import pyarrow.parquet as pq                                  # noqa: E402
from updater import blob, config, registry                    # noqa: E402

# Matches updater.merge._IMPOSSIBLE_AFTER. Deliberately far above every real horizon: the point
# is to be unarguable. A bound that flagged UN WPP would be switched off and protect nothing.
IMPOSSIBLE_AFTER = dt.date(2200, 1, 1)

# ...AND THE OTHER DIRECTION, which this tool was blind to and which hid the worse half.
#
# The failure mode that motivates it: stat_slovenia's 05W.parquet holds one key with 5,863
# observations dated year 1, 2, 3, ... 6152, every one at 12-31. That is a POSITIONAL COUNTER
# being read as a year, not a date at all. A future-only test sees the tail above 2200 and calls
# it "214,775 of 506,605 bad" — but the counter starts at 1, so 250,876 MORE rows sit below 1900
# and were never examined, and the whole file is fabricated. Measured 2026-08-04:
#
#     year <= 1900   250,876 rows   impossible, and invisible to a future-only test
#     year >  2200   214,638 rows   all the original sweep found
#     1900..2200      41,091 rows   fabricated and INDISTINGUISHABLE from real data
#
# 1500, CALIBRATED AGAINST THE DATA rather than guessed. My first attempt used 1850 and flagged
# 25 sources, nearly all of them genuine: treasury's US debt outstanding from 1790, vdem from
# 1789, wid from 1800, ssb from 1769, noaa weather from 1840, owid from 1840. That is exactly the
# failure this file's upper-bound note warns about — a bound that flags real data gets switched
# off and protects nothing. At 1500 the remaining low-side hits are two: scb BE/HE at year 0114
# and stat_slovenia 05W at year 0001, both unarguable, plus allowlisted deep history below.
IMPOSSIBLE_BEFORE = dt.date(1500, 1, 1)

# Sources whose real data legitimately predates IMPOSSIBLE_BEFORE. Named individually and with a
# reason, never a pattern, so adding one is a decision somebody made rather than a widened net.
DEEP_HISTORY_OK = {
    "maddison":   "Maddison Project reconstructs GDP per capita to AD 1 for some countries",
    "barro_lee":  "educational attainment series start 1870 in the long files",
    "pwt":        "not deep, but keep adjacent to barro_lee for review",
    "penn_world_table": "same",
    "gcb":        "Global Carbon Budget runs from 1750",
    "ei_statreview": "Energy Institute Statistical Review has pre-1900 series",
    "owid":       "book production / literacy series (Buringh & van Zanden) genuinely start ~1475",
    "treasury":   "US historical debt outstanding begins 1790 — real",
    "vdem":       "V-Dem codes regimes from 1789 — real",
    "ssb":        "Norwegian long series reach 1769 — real",
    "wid":        "World Inequality Database distributional series start 1800 — real",
    "noaa":       "GHCN station records begin 1840 — real",
    "scb":        "Swedish long series reach 1800 (MI.parquet); BE/HE at year 0114 are NOT this "
                  "and are a real defect — see task #91",
}


def _max_dates(md):
    """Per-row-group max obs_date from footer statistics; [] when the column is absent."""
    names = md.schema.names
    if "obs_date" not in names:
        return []
    i = names.index("obs_date")
    out = []
    for rg in range(md.num_row_groups):
        st = md.row_group(rg).column(i).statistics
        if st is not None and st.max is not None:
            out.append(str(st.max)[:10])
    return out


def _min_dates(md):
    """Per-row-group MIN obs_date from footer statistics.

    Costs nothing extra — the footer already carries min beside max, and reading only one of
    them is what made this tool half a test. See IMPOSSIBLE_BEFORE.
    """
    names = md.schema.names
    if "obs_date" not in names:
        return []
    i = names.index("obs_date")
    out = []
    for rg in range(md.num_row_groups):
        st = md.row_group(rg).column(i).statistics
        if st is not None and st.min is not None:
            out.append(str(st.min)[:10])
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--local", action="store_true")
    g.add_argument("--r2", action="store_true")
    ap.add_argument("--source", help="one source id (default: every registered source)")
    # THE WRITE-TIME BOUND AND THE AUDIT BOUND SHOULD NOT BE THE SAME NUMBER, and this flag is
    # why. merge_and_write must be unarguable — one false positive on UN WPP's real 2101 and the
    # check gets switched off, protecting nothing — so it sits at 2200. But fabrication does not
    # politely stay above 2200: cso/10_Census_2016.parquet holds 272,445 rows past 2100 and only
    # 268,765 past 2200, so 3,680 fabricated rows live in the ambiguous band. An operator
    # investigating deliberately can look wherever they like; the automated guard cannot.
    # 2102 is the tightest defensible floor — UN WPP genuinely reaches 2101-07-01.
    ap.add_argument("--after", metavar="YYYY-MM-DD", default=IMPOSSIBLE_AFTER.isoformat(),
                    help=f"report obs_date beyond this (default {IMPOSSIBLE_AFTER}, the "
                         f"write-time bound). Lower it to sweep the ambiguous band, where real "
                         f"projections also live.")
    ap.add_argument("--before", metavar="YYYY-MM-DD", default=IMPOSSIBLE_BEFORE.isoformat(),
                    help=f"report obs_date EARLIER than this (default {IMPOSSIBLE_BEFORE}). A "
                         f"counter-as-year starts at 1, so the low side is where most "
                         f"fabrication lives; a future-only sweep never looks.")
    a = ap.parse_args()

    bound = a.after
    lo = a.before
    sources = ([a.source] if a.source
               else sorted(e["source_id"] for e in registry.load().get("sources", [])))
    # Print the RESOLVED backend, not the flag. They disagreed until 2026-08-04 and the header
    # was the thing that made the disagreement invisible.
    print(f"scanning {len(sources)} source(s) for obs_date > {bound} and < {lo}  "
          f"(backend={config.BACKEND})\n")

    hits = []
    for sid in sources:
        d = config.source_dir(sid)
        try:
            if a.local:
                files = [os.path.relpath(p, d).replace(os.sep, "/")
                         for p in glob.glob(os.path.join(d, "**", "*.parquet"), recursive=True)]
            else:
                files = blob.list_parquets(d, recursive=True)
        except Exception:                                     # noqa: BLE001
            continue
        if not files:
            continue
        bad = []
        for rel in files:
            p = os.path.join(d, rel)
            try:
                md = pq.read_metadata(p) if a.local else blob.read_metadata(p)
                worst = max((v for v in _max_dates(md) if v > bound), default=None)
                # BOTH DIRECTIONS. The min costs nothing (it sits beside the max in the same
                # footer) and is where the worse half hid: a positional counter read as a year
                # starts at 1, so it produces year-0001 rows long before it produces year-6152
                # ones. stat_slovenia 05W: 214,638 rows above 2200 (found), 250,876 below 1900
                # (never looked at), 41,091 in between and indistinguishable from real data.
                earliest = min((v for v in _min_dates(md) if v < lo), default=None)
                if worst or earliest:
                    bad.append((rel, worst, earliest))
            except Exception:                                 # noqa: BLE001
                continue
        if bad:
            hits.append((sid, len(files), bad))
            n_hi = sum(1 for _r, w, _e in bad if w)
            n_lo = sum(1 for _r, _w, e in bad if e)
            print(f"  {sid}: {len(bad)} of {len(files)} file(s) out of range "
                  f"({n_hi} past {bound}, {n_lo} before {lo})"
                  + ("   [DEEP_HISTORY_OK: " + DEEP_HISTORY_OK[sid] + "]"
                     if sid in DEEP_HISTORY_OK else ""))
            for rel, worst, earliest in sorted(
                    bad, key=lambda x: (x[1] or "", x[2] or ""), reverse=True)[:5]:
                span = []
                if earliest:
                    span.append(f"min={earliest}")
                if worst:
                    span.append(f"max={worst}")
                print(f"      {' '.join(span):<28} {rel}")

    print(f"\n{len(hits)} source(s) affected"
          f"{'' if hits else ' — nothing beyond the bound'}")
    if a.local and hits:
        print("\nNOTE: a local scan of a CLOUD source sees only the scratch mirror of the last "
              "run.\n  Re-check anything found here with --r2 --source <id> before acting.")
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
