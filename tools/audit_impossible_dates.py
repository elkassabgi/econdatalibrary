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
    "ggdc":       "holds maddison2020.parquet and maddison2023.parquet — the SAME Maddison "
                  "Project data already exempted above, published by the Groningen Growth and "
                  "Development Centre. Exempting `maddison` but not `ggdc` made every run report "
                  "the same dataset as a permanent false positive (found 2026-09-04)",
    "gapminder":  "income_per_person_long_series:chn genuinely starts 0730-12-31 — Gapminder's "
                  "published long income reconstruction for China; the key says long_series. "
                  "1,888 of 3,763,088 rows",
}

# The HIGH side needs its own list, and for a sharper reason than the low side.
#
# A far-future date is usually fabrication (a missing time axis, a code read as a year) — that is
# the cso 9998 defect. But some publishers ship an explicit open-ended sentinel, and there the
# far-future value IS the faithful reproduction of the source. The danger is the obvious "fix":
# inventing plausible dates for those rows would be fabricating data to make a checker green,
# which is the one thing this tool must never encourage.
#
# Same discipline as above: named individually, with the evidence, never a pattern.
PUBLISHER_SENTINEL_OK = {
    "eurostat":   "ENV_WAT_LTAA and TEN00001 are 100% 9999-12-31 (524 and 388 rows). Their keys "
                  "carry freq=NAP — Eurostat's own 'not applicable' frequency code — because "
                  "these are Long-Term Annual Averages with NO time dimension. It is the "
                  "publisher's open-ended sentinel, not our fabrication, and the pipeline already "
                  "guards the one place it leaked: sync_state_d1.py:158 filters end_date >= "
                  "'2900-01-01' out of source_data_through. Do NOT invent dates for these rows",
}


# source_id -> registry `out_dir`, filled in main(). Only 2 entries differ from the source id
# today (sec_edgar -> edgar_13f, sec_edgar_xbrl -> sec_edgar) and both were invisible to this
# tool until 2026-09-03.
_OUT_DIRS = {}


def _show_store(path):
    """Render a store path the way the ACTIVE backend addresses it.

    Under --r2 the listing is an R2 key prefix, not a local directory. Printing
    `E:\\research\\...\\clean_full\\cso` for an --r2 run sends the reader to a directory that run
    never touched, and on a CI runner that path does not exist at all — the R330/R296 class this
    file's header exists to prevent. Used by BOTH the hit line and the NOT-SCANNED block; having
    it in only one of them is how the hit line kept lying after the other was fixed.
    """
    if config.BACKEND != "r2":
        return path
    root = os.path.dirname(config.DATA_ROOT)
    if path.startswith(root):
        rel = path[len(root):].replace(os.sep, "/").lstrip("/")
        return rel + "/   (R2 key prefix)"
    return path


def _store_candidates(sid):
    """Every place this source's parquet store might actually live, in priority order.

    `config.source_dir` assumes clean_full/<source_id>/, which is wrong for two known classes:
    the registry's `out_dir` redirects a source elsewhere (sec_edgar -> edgar_13f), and some
    stores live in the clean_grouped/ tier (sec_edgar's 17,451 files are at
    clean_grouped/sec_edgar/). Returning candidates rather than one path is what lets the
    caller distinguish "looked everywhere and found nothing" from "looked in one wrong place".
    """
    out_dir = _OUT_DIRS.get(sid) or sid
    grouped_root = os.path.join(os.path.dirname(config.DATA_ROOT), "clean_grouped")
    seen, cands = set(), []
    for p in (config.source_dir(out_dir), config.source_dir(sid),
              os.path.join(grouped_root, out_dir), os.path.join(grouped_root, sid)):
        if p not in seen:
            seen.add(p)
            cands.append(p)
    return cands


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
    _reg = registry.load().get("sources", [])
    _OUT_DIRS.update({e["source_id"]: e.get("out_dir")
                      for e in _reg if e.get("out_dir")})
    sources = ([a.source] if a.source
               else sorted(e["source_id"] for e in _reg))
    # Print the RESOLVED backend, not the flag. They disagreed until 2026-08-04 and the header
    # was the thing that made the disagreement invisible.
    print(f"scanning {len(sources)} source(s) for obs_date > {bound} and < {lo}  "
          f"(backend={config.BACKEND})\n")

    hits = []
    # A SOURCE THIS TOOL COULD NOT LOOK AT IS NOT A SOURCE IT FOUND CLEAN (ledger R704).
    #
    # Two separate defects, both found by review after R704 was booked on a WRONG diagnosis:
    #
    # (1) COVERAGE. This tool assumed every source lives at clean_full/<source_id>/. It does
    #     not. The registry declares `out_dir` (sec_edgar -> edgar_13f, sec_edgar_xbrl ->
    #     sec_edgar) and some stores sit in the clean_grouped/ tier instead. sec_edgar's real
    #     store is data/clean_grouped/sec_edgar/ — 17,451 parquet files carrying an obs_date
    #     column that this tool's own footer method reads perfectly (VICR max 6016-06-30,
    #     PAMT 3015-03-31). R704's first draft recorded "sec_edgar has no parquet store, it is
    #     served-CSV only" — that was FALSE, and it was a lookup bug, not blindness.
    # (2) SILENT NON-JUDGEMENT. A file whose footer carries no obs_date statistics returns []
    #     from _max_dates/_min_dates, counts toward len(files), and passes as in-range.
    #     Measured on edgar_13f: 371 of 371 files have NO obs_date column, so the source
    #     reported clean having judged nothing. Counting LISTED files instead of JUDGED files
    #     is how a clean verdict gets minted from zero evidence.
    unscanned, errored = [], []
    for sid in sources:
        # Follow out_dir, then fall back through the tiers, and remember which store answered
        # so the report can name it. A source is only "absent" once every candidate is empty.
        # A CANDIDATE THAT YIELDS FILES BUT NO JUDGEABLE DATES IS NOT THE STORE. Stopping at
        # the first directory that merely CONTAINS parquet is what kept sec_edgar unscanned:
        # its out_dir points at clean_full/edgar_13f (371 files, none carrying an obs_date
        # column) while its real store is clean_grouped/sec_edgar (17,451 files that do).
        # Keep trying candidates until one actually yields a judgement.
        cand = _store_candidates(sid)
        files, d, bad = [], None, []
        n_judged = n_unreadable = 0
        tried, listing_error = [], None
        for path in cand:
            try:
                if a.local:
                    found = [os.path.relpath(p, path).replace(os.sep, "/")
                             for p in glob.glob(os.path.join(path, "**", "*.parquet"),
                                                recursive=True)]
                else:
                    found = blob.list_parquets(path, recursive=True)
            except Exception as exc:                          # noqa: BLE001
                listing_error = type(exc).__name__
                tried.append(f"{path}  (listing failed: {listing_error})")
                continue
            if not found:
                tried.append(f"{path}  (0 parquet file(s))")
                continue
            c_bad, c_judged, c_unreadable = [], 0, 0
            for rel in found:
                p = os.path.join(path, rel)
                try:
                    md = pq.read_metadata(p) if a.local else blob.read_metadata(p)
                    maxes, mins = _max_dates(md), _min_dates(md)
                    # JUDGED, NOT LISTED. A file with no obs_date column (or no footer stats
                    # for it) yields [] here and would otherwise sail through as "in range".
                    if maxes or mins:
                        c_judged += 1
                    else:
                        c_unreadable += 1
                    worst = max((v for v in maxes if v > bound), default=None)
                    # BOTH DIRECTIONS. The min costs nothing (it sits beside the max in the
                    # same footer) and is where the worse half hid: a positional counter read
                    # as a year starts at 1, so it produces year-0001 rows long before it
                    # produces year-6152 ones. stat_slovenia 05W: 214,638 rows above 2200
                    # (found), 250,876 below 1900 (never looked at), 41,091 in between and
                    # indistinguishable from real data.
                    earliest = min((v for v in mins if v < lo), default=None)
                    if worst or earliest:
                        c_bad.append((rel, worst, earliest))
                except Exception:                             # noqa: BLE001
                    c_unreadable += 1
                    continue
            if c_judged:
                files, d, bad = found, path, c_bad
                n_judged, n_unreadable = c_judged, c_unreadable
                break
            tried.append(f"{path}  ({len(found)} file(s) present, "
                         f"0 carried an obs_date statistic)")
        if not n_judged:
            if listing_error and not tried:
                errored.append((sid, listing_error))
            else:
                unscanned.append((sid, tried))
            continue
        if bad:
            hits.append((sid, len(files), bad))
            n_hi = sum(1 for _r, w, _e in bad if w)
            n_lo = sum(1 for _r, _w, e in bad if e)
            print(f"  {sid}: {len(bad)} of {len(files)} file(s) out of range "
                  f"({n_hi} past {bound}, {n_lo} before {lo})"
                  f"  [judged {n_judged}/{len(files)}"
                  + (f", {n_unreadable} unjudgeable" if n_unreadable else "")
                  + f"; store {_show_store(d)}]"
                  + ("   [DEEP_HISTORY_OK: " + DEEP_HISTORY_OK[sid] + "]"
                     if sid in DEEP_HISTORY_OK else "")
                  + ("   [PUBLISHER_SENTINEL_OK: " + PUBLISHER_SENTINEL_OK[sid] + "]"
                     if sid in PUBLISHER_SENTINEL_OK else ""))
            for rel, worst, earliest in sorted(
                    bad, key=lambda x: (x[1] or "", x[2] or ""), reverse=True)[:5]:
                span = []
                if earliest:
                    span.append(f"min={earliest}")
                if worst:
                    span.append(f"max={worst}")
                print(f"      {' '.join(span):<28} {rel}")

    # SAY WHAT WAS NOT LOOKED AT, BEFORE THE VERDICT, so the verdict is never mistaken for a
    # statement about those sources (R704).
    n_seen = len(sources) - len(unscanned) - len(errored)
    if unscanned or errored:
        print(f"\nNOT SCANNED — {len(unscanned) + len(errored)} source(s) yielded NO judgeable "
              f"data. This is not a clean result; it is no result:")
        for sid, cands in unscanned:
            print(f"  {sid}:")
            for c in cands:
                # A candidate line may carry a trailing "  (N file(s) present, ...)" note; keep
                # it while still rendering the PATH part for the active backend.
                head, sep, tail = c.partition("  (")
                print(f"      looked in: {_show_store(head)}{sep}{tail}")
        for sid, exc in errored:
            print(f"  {sid}: listing failed ({exc}) — also no result.")
        print("  A source can be redirected by the registry's `out_dir`, or live in the "
              "clean_grouped/ tier; both are tried above. If every candidate is empty the "
              "store is genuinely elsewhere — find it before calling the source clean.")

    verdict = f"\n{len(hits)} source(s) affected"
    if not hits:
        if n_seen == len(sources):
            verdict += " — nothing beyond the bound"
        elif n_seen:
            # PARTIAL COVERAGE IS NOT CLEAN. The unqualified phrasing here is exactly what got
            # read as "the data is fine" for a source that was never looked at (R704).
            verdict += (f" — nothing beyond the bound IN THE {n_seen} SOURCE(S) SCANNED. "
                        f"{len(sources) - n_seen} were not scanned and are unjudged.")
        else:
            verdict += (" — but ZERO sources were actually scanned, so this says NOTHING about "
                        "the data. Do not read it as clean.")
    verdict += f"   [scanned {n_seen} of {len(sources)} source(s)]"
    print(verdict)

    if a.local and hits:
        print("\nNOTE: a local scan of a CLOUD source sees only the scratch mirror of the last "
              "run.\n  Re-check anything found here with --r2 --source <id> before acting.")
    # Exit 2 covers ANY incomplete coverage, not just total failure. The fleet default always
    # lands in the partial case, so a trichotomy keyed on "every source failed" would have
    # returned 0 on exactly the run shape that produced R704's false clean.
    #
    # MEASURED 2026-09-03 with the candidate resolution above: 3 of 282 registry sources have no
    # local parquet store under ANY candidate (gii, pxweb, sipri_polity). Note the figure moved
    # BECAUSE of this file's own fix -- it was 5 when only clean_full/<source_id> was tried, and
    # sec_edgar + sec_edgar_xbrl came into range once out_dir and the clean_grouped tier were
    # followed. Quote the post-fix number, not the one that motivated the change.
    if hits:
        return 1
    return 0 if n_seen == len(sources) else 2


if __name__ == "__main__":
    raise SystemExit(main())
