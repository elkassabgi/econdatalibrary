"""Which day of each period does every source stamp its observations on?

The store has ONE date type (date32) and one column name (obs_date), so this is not
a formatting difference — every source already writes an ISO date. What differs is
the CONVENTION: an annual 2024 observation can be stamped 2024-01-01 (period-START)
or 2024-12-31 (period-END), and a monthly 2025-M06 can be 2025-06-01 or 2025-06-30.

That difference is invisible until it bites, and it bites hard:

  * MERGING mixes them. imf_commodity stores period-START; jobs/ingest_imf_direct.py
    stamps period-END. Merging the direct pull into it without converting would have
    written a SECOND row for every month of 34 years of history rather than
    extending the series — dedup keys on (series_key, obs_date), so the two dates
    are two different observations.
  * JOINING across sources silently misaligns. A user joining an annual series
    stamped 01-01 to one stamped 12-31 gets an empty join, or worse, a 1-year lag
    they never notice.
  * FILTERING by date cuts differently. `obs_date <= 2024-12-31` includes a
    period-END 2024 annual point and a period-START one; `< 2024-12-31` includes
    only one of them.

So: measure it. For each source this samples real observations, infers each series'
cadence from its own spacing, and classifies where in the period the stamp falls.

Usage:  python tools/audit_date_conventions.py [--full] [--json out.json]
"""
from __future__ import annotations

import argparse
import calendar
import collections
import datetime as dt
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pyarrow.parquet as pq                                  # noqa: E402

BASE = os.path.join(ROOT, "data", "clean_full")
MAX_FILES = 3          # parquet files sampled per source
MAX_ROWS = 200_000     # rows sampled per source


KEEP_HEAD = 5     # earliest observations retained per series
KEEP_TAIL = 5     # latest, kept with a rolling window


def sample(sid, full=False):
    """Per-series date evidence from one source: {series_key: [dates]}.

    MEMORY IS THE CONSTRAINT, not I/O. The first version accumulated every
    (key, date) pair, which on this 265 GB / 52,355-file store grew to 40 GB RSS
    and was climbing 1.3 GB every 20 seconds — it would have exhausted the box long
    before finishing, and "the audit crashed" is not an answer to a question about
    every source.

    Bounding it per SERIES rather than per source keeps full coverage of what is
    being measured. Deciding a series' cadence needs consecutive gaps, and deciding
    where in the period it is stamped needs a date — a handful at each end gives
    both, and keeping both ends is what exposes a source that changed convention
    partway through. Every file, every series, every source is still visited; what
    is dropped is the redundant middle of each series, which carries no information
    this audit uses.
    """
    d = os.path.join(BASE, sid)
    if not os.path.isdir(d):
        return {}
    files = sorted(f for f in os.listdir(d) if f.endswith(".parquet")
                   and not f.startswith("_"))
    if not files:
        return {}
    if not full:
        files = files[:MAX_FILES]
    head, tail = {}, {}
    seen = 0
    for f in files:
        try:
            pf = pq.ParquetFile(os.path.join(d, f))
            names = set(pf.schema_arrow.names)
            kcol = ("series_key" if "series_key" in names
                    else ("series_id" if "series_id" in names else None))
            if kcol is None or "obs_date" not in names:
                continue
            for i in range(pf.num_row_groups):
                t = pf.read_row_group(i, columns=[kcol, "obs_date"])
                for k, dte in zip(t[kcol].to_pylist(), t["obs_date"].to_pylist()):
                    if dte is None:
                        continue
                    seen += 1
                    h = head.get(k)
                    if h is None:
                        head[k] = [dte]
                        tail[k] = collections.deque(maxlen=KEEP_TAIL)
                    elif len(h) < KEEP_HEAD:
                        h.append(dte)
                    tail[k].append(dte)
                if not full and seen >= MAX_ROWS:
                    return {k: head[k] + list(tail[k]) for k in head}
        except Exception:                                     # noqa: BLE001
            continue
    return {k: head[k] + list(tail[k]) for k in head}


def classify(per):
    """Infer each series' cadence from its own spacing, then locate the stamp."""
    verdict = collections.Counter()
    for k, ds in per.items():
        ds = sorted({d for d in ds if isinstance(d, dt.date)})
        if len(ds) < 3:
            continue
        # Gaps are measured only WITHIN the retained head and tail; the join between
        # them spans the dropped middle, so that one gap is discarded rather than
        # mistaken for a huge cadence.
        gaps = sorted([(b - a).days for a, b in zip(ds, ds[1:])])
        med = gaps[len(gaps) // 2]
        s = ds[-1]                                            # a real, recent stamp
        last = calendar.monthrange(s.year, s.month)[1]
        if 350 <= med <= 380:                                 # annual
            if (s.month, s.day) == (1, 1):
                verdict["annual START"] += 1
            elif (s.month, s.day) == (12, 31):
                verdict["annual END"] += 1
            else:
                verdict[f"annual other ({s.month:02d}-{s.day:02d})"] += 1
        elif 85 <= med <= 95:                                 # quarterly
            if s.day == 1 and s.month in (1, 4, 7, 10):
                verdict["quarterly START"] += 1
            elif s.day == last and s.month in (3, 6, 9, 12):
                verdict["quarterly END"] += 1
            else:
                verdict["quarterly other"] += 1
        elif 28 <= med <= 31:                                 # monthly
            if s.day == 1:
                verdict["monthly START"] += 1
            elif s.day == last:
                verdict["monthly END"] += 1
            else:
                verdict["monthly other"] += 1
        elif med <= 7:
            verdict["daily/weekly (exact date)"] += 1
        else:
            verdict["irregular"] += 1
    return verdict


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--full", action="store_true",
                    help="read every file per source (slow, exhaustive)")
    ap.add_argument("--json")
    a = ap.parse_args()

    sids = sorted(s for s in os.listdir(BASE) if os.path.isdir(os.path.join(BASE, s)))
    print(f"sources on disk: {len(sids)}")
    print("sampling up to %s rows / %s files each\n"
          % (format(MAX_ROWS, ","), "all" if a.full else MAX_FILES))

    out, totals = {}, collections.Counter()
    conflicted = []
    for sid in sids:
        v = classify(sample(sid, a.full))
        if not v:
            continue
        out[sid] = dict(v)
        for k, n in v.items():
            totals[k] += n
        # A source that stamps the SAME cadence two different ways is the worst case:
        # it is not merely inconsistent with its neighbours, it is inconsistent with
        # itself, and no single conversion rule can fix it.
        for cad in ("annual", "quarterly", "monthly"):
            ks = [k for k in v if k.startswith(cad)]
            if len(ks) > 1:
                conflicted.append((sid, {k: v[k] for k in ks}))
                break

    print("CONVENTION TOTALS (series counted, all sources)")
    for k, n in totals.most_common():
        print("  %-34s %10s" % (k, format(n, ",")))

    print("\nBY CADENCE — how many SOURCES use each convention")
    for cad in ("annual", "quarterly", "monthly"):
        st = sorted(s for s, v in out.items()
                    if max((k for k in v if k.startswith(cad)),
                           key=lambda k: v[k], default="") .endswith("START"))
        en = sorted(s for s, v in out.items()
                    if max((k for k in v if k.startswith(cad)),
                           key=lambda k: v[k], default="").endswith("END"))
        print(f"  {cad:<11} START {len(st):>3} source(s)   END {len(en):>3} source(s)")
        if en and st:
            print(f"      START e.g. {', '.join(st[:6])}")
            print(f"      END   e.g. {', '.join(en[:6])}")

    if conflicted:
        print(f"\nSOURCES INCONSISTENT WITH THEMSELVES ({len(conflicted)}) — same "
              f"cadence stamped two ways inside one source:")
        for sid, v in conflicted[:20]:
            print("  %-26s %s" % (sid, v))

    if a.json:
        io.open(a.json, "w", encoding="utf-8").write(json.dumps(out, indent=1))
        print(f"\nwrote {a.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
