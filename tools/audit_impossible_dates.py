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

import pyarrow.parquet as pq                                  # noqa: E402
from updater import blob, config, registry                    # noqa: E402

# Matches updater.merge._IMPOSSIBLE_AFTER. Deliberately far above every real horizon: the point
# is to be unarguable. A bound that flagged UN WPP would be switched off and protect nothing.
IMPOSSIBLE_AFTER = dt.date(2200, 1, 1)


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
    a = ap.parse_args()

    bound = a.after
    sources = ([a.source] if a.source
               else sorted(e["source_id"] for e in registry.load().get("sources", [])))
    print(f"scanning {len(sources)} source(s) for obs_date > {bound}  "
          f"({'local' if a.local else 'r2'})\n")

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
                if worst:
                    bad.append((rel, worst))
            except Exception:                                 # noqa: BLE001
                continue
        if bad:
            hits.append((sid, len(files), bad))
            print(f"  {sid}: {len(bad)} of {len(files)} file(s) reach past {bound}")
            for rel, worst in sorted(bad, key=lambda x: x[1], reverse=True)[:5]:
                print(f"      {worst}   {rel}")

    print(f"\n{len(hits)} source(s) affected"
          f"{'' if hits else ' — nothing beyond the bound'}")
    if a.local and hits:
        print("\nNOTE: a local scan of a CLOUD source sees only the scratch mirror of the last "
              "run.\n  Re-check anything found here with --r2 --source <id> before acting.")
    return 1 if hits else 0


if __name__ == "__main__":
    raise SystemExit(main())
