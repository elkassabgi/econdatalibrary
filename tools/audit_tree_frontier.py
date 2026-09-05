"""Per-DIRECTORY freshness for a source whose store is a tree. Read-only.

WHY THIS EXISTS. `last_obs_date` is a MAX over the whole store tree, so it reports the source's
FRESHEST file and cannot distinguish "every part is current" from "one part is current and the rest
is frozen". On 2026-09-05 that hid a real defect on `bea`: its fetcher iterates 2 of 12 dataset
directories, so 895,531 of 913,230 catalogued series (98.06 %) sat in directories nothing refreshes -
Regional at 2025-12-31, others at 2024-12-31 and 2023-12-31 - while `last_obs_date` read 2026-06-01
from the 1.94 % the fetcher does write. I then proposed suppressing bea's alert on the strength of
that number. The adversarial review refused it. Ledger R762.

A MAX is the wrong statistic for a freshness question. The honest ones are here: the newest date per
directory, and the OLDEST per-file max within each directory, which is where a frozen region shows.

The first version of the tool this grew from was written by a reviewer and lived in a scratch
directory, which is the finding three consecutive reviews recorded about the seam instruments. It
lives in the repo now so a result can be re-derived.

    python tools/audit_tree_frontier.py --source bea
    python tools/audit_tree_frontier.py --source bea --depth 2 --top 20

READS ROW-GROUP STATISTICS ONLY - never a whole parquet file. R690 records unbounded reads here, and
an earlier auditor crashed the machine at 128 GB. Do not "fix" this by adding a memory limit: the
workstation has 382.7 GB, and every OOM on 2026-09-03 was a self-imposed `--memory-limit`.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _file_max(blob, path: str, date_col: str):
    """The max of `date_col` over one parquet's row-group statistics, or None."""
    md = blob.read_metadata(path)
    names = md.schema.names
    if date_col not in names:
        return None, md.num_rows, f"no {date_col} column"
    idx = names.index(date_col)
    fmax = None
    for rg in range(md.num_row_groups):
        st = md.row_group(rg).column(idx).statistics
        if st is None or st.max is None:
            continue
        v = st.max
        v = v if isinstance(v, dt.date) else dt.date.fromisoformat(str(v)[:10])
        if fmax is None or v > fmax:
            fmax = v
    if fmax is None:
        return None, md.num_rows, "no row-group statistics"
    return fmax, md.num_rows, None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--depth", type=int, default=1,
                    help="how many path segments define a group (default 1 = top-level dir)")
    ap.add_argument("--date-col", default="obs_date")
    ap.add_argument("--top", type=int, default=10, help="how many stalest/freshest files to list")
    a = ap.parse_args()

    os.environ.setdefault("AQUEDUCT_BACKEND", "r2")
    from updater import blob, config                                # noqa: E402

    print(f"read at {_stamp()}   backend {config.BACKEND}")
    out_dir = config.source_dir(a.source)
    print(f"store dir {out_dir}")

    rels = list(blob.list_parquets(out_dir, recursive=True))
    print(f"parquet objects listed: {len(rels)}", flush=True)
    if not rels:
        # R316: an absence must not be reported as a finding without a positive control.
        print("NO PARQUETS FOUND. That is not a result yet - run this against a source known to hold "
              "data before believing it, because an empty listing looks identical whether the store "
              "is empty or the backend is misrouted.")
        return 2

    per = collections.defaultdict(lambda: {"files": 0, "rows": 0, "max": None, "min_of_max": None,
                                           "unreadable": 0})
    seen: list = []
    for i, rel in enumerate(rels):
        norm = rel.replace("\\", "/")
        parts = norm.split("/")
        group = "/".join(parts[:a.depth]) if len(parts) > a.depth else "(root)"
        d = per[group]
        d["files"] += 1
        try:
            fmax, nrows, why = _file_max(blob, os.path.join(out_dir, rel), a.date_col)
            d["rows"] += nrows
            if fmax is None:
                d["unreadable"] += 1
                if d["unreadable"] <= 2:
                    print(f"  unreadable {rel}: {why}", flush=True)
                continue
            if d["max"] is None or fmax > d["max"]:
                d["max"] = fmax
            if d["min_of_max"] is None or fmax < d["min_of_max"]:
                d["min_of_max"] = fmax
            seen.append((fmax, rel))
        except Exception as ex:                                     # noqa: BLE001
            d["unreadable"] += 1
            if d["unreadable"] <= 2:
                print(f"  unreadable {rel}: {type(ex).__name__}: {str(ex)[:70]}", flush=True)
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(rels)}", flush=True)

    print()
    print(f"{'group':<32} {'files':>6} {'rows':>15} {'NEWEST':>12} {'oldest file max':>16} {'unread':>7}")
    for g in sorted(per):
        d = per[g]
        print(f"{g:<32} {d['files']:>6} {d['rows']:>15,} {str(d['max']):>12} "
              f"{str(d['min_of_max']):>16} {d['unreadable']:>7}")

    tree_max = max((m for m, _ in seen), default=None)
    tree_min = min((m for m, _ in seen), default=None)
    print()
    print(f"TREE MAX  {tree_max}   <- this is what last_obs_date reports, from the freshest file")
    print(f"TREE MIN  {tree_min}   <- the stalest file's own newest observation")

    # READ THE RIGHT COLUMN. The first version of this tool warned on the SPREAD between the freshest
    # and stalest FILE. On bea that spread is 25,110 days and says almost nothing, because it is
    # dominated by tables the publisher legitimately ENDED decades ago - Regional/SQINC5H stops in
    # 1957, NIPA/T70201A in 1966. A discontinued table having an old max is correct, not frozen.
    # The signal is the per-GROUP NEWEST: a whole directory whose freshest file trails the others by
    # years is a region nothing writes. Same lesson as R762 itself - a max answered the wrong question.
    groups = {g: d["max"] for g, d in per.items() if d["max"]}
    if len(groups) > 1 and tree_max:
        behind = sorted(((tree_max - m).days, g, m) for g, m in groups.items())
        stale = [(days, g, m) for days, g, m in behind if days > 365]
        print()
        if stale:
            print(f"GROUPS MORE THAN A YEAR BEHIND THE TREE MAX ({len(stale)} of {len(groups)}):")
            for days, g, m in sorted(stale, reverse=True):
                rows = per[g]["rows"]
                print(f"  {g:<30} newest {m}  {days:>6,} d behind   {rows:>15,} rows")
            print("  A whole directory trailing by years is the FROZEN-REGION shape: it is written by")
            print("  nothing. The spread is only a hint - CONFIRM by reading the fetcher's own write")
            print("  paths, and by asking the publisher what it currently serves (R762).")
        else:
            print(f"No group is more than a year behind the tree max ({len(groups)} groups compared).")

    seen.sort()
    print(f"\n{a.top} STALEST files by their own newest observation:")
    for m, rel in seen[:a.top]:
        print(f"  {m}  {rel}")
    print(f"\n{a.top} FRESHEST:")
    for m, rel in seen[-a.top:]:
        print(f"  {m}  {rel}")
    print(f"\ndone {_stamp()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
