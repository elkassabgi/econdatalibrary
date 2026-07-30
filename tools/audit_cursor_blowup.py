"""Which fetchers can OOM the runner by reporting an UNBOUNDED cursor set?

THE DEFECT THIS HUNTS (ledger R175, and the outage it actually caused on 2026-07-30).
A fetcher returns Result.series_cursors so orchestrate._derive_changed_csvs knows which
CSVs to re-derive. Most build that dict with one entry PER SERIES and no cap. For a small
source that is free. For a big one it is a runner-killer:

  abs holds 976,632,535 rows across 376,332,763 DISTINCT SERIES. Its cursor fold took one
  entry each -- about 94 GB. It sorts FIRST alphabetically, so it ran first, climbed
  1,211MB -> 15,700MB at ~299 MB/min and destroyed the 16 GB runner 48.5 minutes in. The
  log carried one orchestrator banner and zero completions: NO OTHER SOURCE EVER RAN. The
  daily updater was a total outage, twice (batch 30312217406 did the same, 15,654 MB).

Neither the workflow's rc=137/143 OOM branch nor the orchestrator's between-source run
budget can see this: a destroyed runner reports "cancelled" with no exit code, and a
source that never RETURNS is never checked against the budget. The bound has to exist
inside the fetcher, which is what CURSOR_CAP / merge_cursors / cursors_from_parquet are
for -- so the audit is simply: does a fetcher with a BIG store use one of them?

The store row count is an upper bound on distinct series and is read from parquet
FOOTERS (metadata only, no column scan), so a full sweep of ~293 stores costs minutes.

RUN THIS LOCALLY — DO NOT WIRE IT INTO CI AS-IS. It needs the clean_full store, which a
runner does not have; there it would find no store root, return 0 and report a clean
sweep it never performed. A gate that cannot fail is not a gate (R142), and a vacuously
green one is worse than none because it retires the suspicion that would have caught the
next abs.

Usage:  python tools/audit_cursor_blowup.py [--threshold-rows 20000000]
Exit 1 if any source over the threshold folds cursors PER SERIES without a bound.
"""
from __future__ import annotations
import argparse
import ast
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

FETCHERS = os.path.join(ROOT, "updater", "strategies", "fetchers")
BOUNDED = ("CURSOR_CAP", "merge_cursors", "cursors_from_parquet", "merge_cursor_map")

# A BIG STORE IS NOT THE TRIGGER — THE FOLD SHAPE IS. The first cut of this audit flagged
# any source over the row threshold that reported cursors, and named statcan (56.8 BILLION
# rows), istat and ecb. All three are FALSE POSITIVES: they key cursors by table/flow/file
#   statcan: series_cursors[str(pid)] = md      -> one entry per PID
#   istat:   cursors[flow_id] = md              -> one entry per flow
#   ecb:     cursors[stem] = md                 -> one entry per file
# so their cursor count is bounded by the FILE count (~8k / ~1.1k / ~540), not the row
# count, and capping them would be a fix to nothing. Only a fold that emits one entry per
# SERIES can blow up. Detect that shape directly.
PER_SERIES = (
    "_series_maxes(",          # the shared per-series helper, copied into ~50 fetchers
    "zip(keys, dates)",        # the inline row-stream fold
    "zip(all_keys, all_dates)",
)

# Sources whose fetcher reports cursors but whose store is small enough that an
# unbounded fold cannot matter. Populated by measurement, not by assumption -- anything
# over the threshold must carry a real bound.
EXEMPT: dict[str, str] = {}


def store_rows(d: str) -> tuple[int, int, int]:
    """(total rows, files, LARGEST single file's rows) for a source dir, from footers only.

    The largest FILE matters independently of the total: an unprojected blob.read_table()
    decodes one file at a time, so a store of many small parquets is safe however big the
    sum, while one 962-million-row cube is fatal on its own.
    """
    try:
        import pyarrow.parquet as pq
    except Exception:                                        # noqa: BLE001
        return (-1, 0, 0)
    rows = 0
    files = 0
    biggest = 0
    for dirpath, _dirs, names in os.walk(d):
        for n in names:
            if not n.endswith(".parquet"):
                continue
            files += 1
            try:
                r = pq.read_metadata(os.path.join(dirpath, n)).num_rows
            except Exception:                                # noqa: BLE001
                continue
            rows += r
            biggest = max(biggest, r)
    return (rows, files, biggest)


# Roughly what one row of (series_key str, obs_date date32, value float64) costs once
# DECODED into Arrow. Measured against statcan's cubes; deliberately conservative.
BYTES_PER_ROW = 70


def unprojected_reads(text: str) -> int:
    """Count read_table(...) CALLS that pass no columns= projection.

    THE SECOND OOM CLASS. abs died on an unbounded cursor fold; statcan was one changed
    census cube away from dying on a full-table read — `blob.read_table(path)` on a
    962,150,400-row parquet is ~67 GB of Arrow. A read without a projection costs the
    WHOLE row width, so it scales with the biggest file in the store, not with the work.

    PARSED, NOT GREPPED. The first cut regex-matched the source with `#` comments stripped
    and immediately produced a false positive: statcan's own docstring EXPLAINS the bug by
    quoting `blob.read_table(path)`, and prose inside a docstring is not a comment. An
    audit that flags its own fix's documentation trains you to ignore it. ast sees calls.
    """
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return 0
    n = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = getattr(fn, "attr", None) or getattr(fn, "id", None)
        if name != "read_table":
            continue
        if not any(kw.arg == "columns" for kw in node.keywords):
            n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--threshold-rows", type=int, default=20_000_000,
                    help="a store at least this big must bound its cursor set")
    ap.add_argument("--store", default=None, help="override the clean_full root")
    ap.add_argument("--read-gb", type=float, default=8.0,
                    help="flag an unprojected read_table() whose largest file would decode "
                         "to at least this many GB (runner has 16 GB)")
    args = ap.parse_args()

    try:
        from updater import config
        root = args.store or os.path.dirname(config.source_dir("abs"))
    except Exception:                                        # noqa: BLE001
        root = args.store or os.path.join(ROOT, "data", "clean_full")
    if not os.path.isdir(root):
        print(f"store root not found: {root} — nothing to audit")
        return 0

    reports, bounded, per_series, unproj = {}, {}, {}, {}
    for fn in sorted(os.listdir(FETCHERS)):
        if not fn.endswith(".py") or fn.startswith("__"):
            continue
        src = fn[:-3]
        try:
            with open(os.path.join(FETCHERS, fn), encoding="utf-8") as fh:
                text = fh.read()
        except OSError:
            continue
        # strip comments so a mention in prose counts as neither a bound nor a fold
        code = re.sub(r"#.*", "", text)
        reports[src] = "series_cursors" in code
        bounded[src] = any(b in code for b in BOUNDED)
        per_series[src] = any(m in code for m in PER_SERIES)
        unproj[src] = unprojected_reads(text)

    print(f"store root: {root}")
    print(f"{len(reports)} fetcher module(s); "
          f"{sum(reports.values())} report cursors, "
          f"{sum(1 for s in reports if reports[s] and per_series[s])} fold PER SERIES, "
          f"{sum(1 for s in reports if per_series[s] and bounded[s])} of those are bounded\n")

    rows_by_src = {}
    for name in sorted(os.listdir(root)):
        d = os.path.join(root, name)
        if os.path.isdir(d):
            rows_by_src[name] = store_rows(d)

    def _is_offender(src, rows):
        return (rows >= args.threshold_rows and reports.get(src) and per_series.get(src)
                and not bounded.get(src) and src not in EXEMPT)

    def _read_risk_gb(src):
        """GB a single unprojected read_table() would decode for this store's LARGEST file."""
        if not unproj.get(src):
            return 0.0
        biggest = rows_by_src.get(src, (0, 0, 0))[2]
        return biggest * BYTES_PER_ROW / 1e9

    # EVALUATED OVER EVERY SOURCE, DISPLAYED FOR THE TOP 25. The first cut appended
    # offenders inside the display slice, so only the 25 biggest stores were ever judged —
    # while the threshold is 20M rows and the 25th store holds 72.5M. A source ranked 26th
    # with 60M rows and an unbounded per-series fold would have passed silently. A gate
    # that inspects part of its surface and prints "0" is worse than no gate.
    ranked = sorted(rows_by_src.items(), key=lambda kv: -kv[1][0])
    offenders = [(s, r) for s, (r, _f, _b) in ranked if _is_offender(s, r)]
    # SECOND CLASS: an unprojected read_table() whose worst single file would not fit.
    read_risks = [(s, _read_risk_gb(s)) for s, _v in ranked
                  if _read_risk_gb(s) >= args.read_gb]

    shown = set()
    print(f"{'source':26s} {'store rows':>16s} {'files':>7s}   fold      bound")
    for src, (rows, files, _b) in ranked[:25]:
        shown.add(src)
        psr = per_series.get(src)
        mark = "   <<< UNBOUNDED per-series fold over threshold" if _is_offender(src, rows) else ""
        fold = "per-series" if psr else ("per-file" if reports.get(src) else "-")
        print(f"{src:26s} {rows:16,d} {files:7,d}   {fold:10s} "
              f"{'yes' if bounded.get(src) else ('NO' if psr else '  n/a'):>5s}{mark}")
    # any offender outside the displayed slice must still be shown, or the table would
    # contradict the verdict below it
    for src, rows in offenders:
        if src not in shown:
            files = rows_by_src[src][1]
            print(f"{src:26s} {rows:16,d} {files:7,d}   {'per-series':10s} "
                  f"{'NO':>5s}   <<< UNBOUNDED (below the displayed slice)")
    print(f"\n(evaluated all {len(ranked)} source store(s); displayed the 25 largest)")

    print(f"\nthreshold: {args.threshold_rows:,} store rows, and the fold must be PER-SERIES")
    print("(a per-file fold — statcan by PID, istat by flow, ecb by stem — is bounded by the")
    print(" file count no matter how many rows the store holds, so it is not flagged)")
    print(f"\nCLASS 1 — unbounded per-series cursor folds (must be 0): {len(offenders)}")
    for s, r in offenders:
        print(f"    {s}: {r:,} store rows, folds one cursor PER SERIES with no "
              f"CURSOR_CAP / merge_cursors / merge_cursor_map / cursors_from_parquet")

    print(f"\nCLASS 2 — unprojected read_table() vs the LARGEST single file "
          f"(must be 0): {len(read_risks)}")
    for s, gb in sorted(read_risks, key=lambda kv: -kv[1]):
        print(f"    {s}: {unproj[s]} read_table() call(s) with no columns=; largest file "
              f"{rows_by_src[s][2]:,} rows -> ~{gb:.0f} GB decoded (runner has 16 GB)")
    return 1 if (offenders or read_risks) else 0


if __name__ == "__main__":
    sys.exit(main())
