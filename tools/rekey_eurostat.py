"""One-time re-key of the eurostat store: strip the UNSTABLE 'LAST UPDATE=' segment.

WHY THE SOURCE IS FROZEN. eurostat's SDMX-CSV carries a `LAST UPDATE` column (with a SPACE)
that changes on EVERY release. The original ingest folded every column into the series_key as
`k=v`, so each release minted a brand-new key for the same series and the file grew a parallel
history instead of extending one. updater/strategies/fetchers/eurostat.py now builds the key
from DIMENSION columns only and refuses to run incrementally until the existing data is
re-keyed (`_require_rekeyed`), because mixing the two schemes duplicates (series, obs_date)
under two keys — which merge's never-shrink guard cannot catch. So eurostat has not updated at
all: 7,637 catalogued dataflows, 7,754 store files, permanently `partial`.

THE KEY IS NOT SPLITTABLE ON ':'. The unstable segment is
    LAST UPDATE=13/05/26 11:00:00:freq=A:am_item=AM400000:unit=THS_AWU:geo=AT
and its VALUE contains colons. `split(':')` yields ['LAST UPDATE=13/05/26 11','00','00',...] —
it shreds the timestamp and would silently corrupt every key it touched. Segments therefore
start only where a `NAME=` boundary appears, which is what _split_kv finds.

CORRECTNESS COMES FROM REUSING THE FETCHER'S OWN RULES. _NON_KEY and _norm are imported from
the fetcher, not re-typed here: if the two ever disagree the store splits a second time, which
is the exact failure this migration exists to undo.

COLLISIONS ARE THE POINT, AND ALSO THE RISK. Stripping the unstable segment deliberately
collapses the same series' releases onto one key — that is the fix. But two rows can then share
(key, obs_date) with DIFFERENT values (a revision). The dry run REPORTS that count before
anything is written; on write we keep the LAST occurrence per (key, obs_date), matching
merge_and_write's "new wins on revision" dedup.

Usage:
    python tools/rekey_eurostat.py --dry-run [--limit N]
    python tools/rekey_eurostat.py --apply   [--limit N]     (writes via blob -> R2 under AQUEDUCT_BACKEND=r2)
"""
from __future__ import annotations
import argparse
import os
import re
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config                                   # noqa: E402
from updater.strategies.fetchers.eurostat import _NON_KEY, _norm   # noqa: E402

# A segment begins at a `NAME=` token: letters/digits/underscore/space before '='.
_KV = re.compile(r"(?:^|:)([A-Za-z][A-Za-z0-9_ ]*)=")


def split_kv(key: str) -> list[tuple[str, str]]:
    """['LAST UPDATE=13/05/26 11:00:00', 'freq=A', ...] -> [(name, value), ...].

    Boundary-aware: a colon INSIDE a value (the timestamp) does not start a segment.
    """
    out = []
    ms = list(_KV.finditer(key))
    for i, m in enumerate(ms):
        name = m.group(1)
        vstart = m.end()
        vend = ms[i + 1].start() if i + 1 < len(ms) else len(key)
        out.append((name, key[vstart:vend]))
    return out


def stable_key(key: str) -> str:
    """Drop every non-dimension segment, preserving the dimensions' original order."""
    return ":".join(f"{n}={v}" for n, v in split_kv(key) if _norm(n) not in _NON_KEY)


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true")
    g.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()

    out_dir = config.source_dir("eurostat")
    files = blob.list_parquets(out_dir)
    if a.limit:
        files = files[:a.limit]
    print(f"eurostat store: {len(files):,} file(s) under {out_dir}  (backend={config.BACKEND})")

    tot_rows = tot_out = touched = clean = 0
    tot_keys_before = tot_keys_after = 0
    revisions = 0
    for i, fn in enumerate(files, 1):
        path = os.path.join(out_dir, fn)
        try:
            t = blob.read_table(path)
        except Exception as e:                                  # noqa: BLE001
            print(f"  [{i}/{len(files)}] {fn}: UNREADABLE ({type(e).__name__}) — skipped")
            continue
        if t.num_rows == 0 or "series_key" not in t.column_names:
            continue
        keys = t.column("series_key").to_pylist()
        if not any(k and "LAST UPDATE" in k for k in keys):
            clean += 1
            continue
        dates = t.column("obs_date").to_pylist()
        vals = t.column("value").to_pylist()
        new = [stable_key(k) if k else k for k in keys]

        # keep the LAST row per (key, obs_date): merge's "new wins on revision"
        seen: dict[tuple, int] = {}
        for idx, (k, d) in enumerate(zip(new, dates)):
            kk = (k, d)
            if kk in seen and vals[seen[kk]] != vals[idx]:
                revisions += 1
            seen[kk] = idx
        keep = sorted(seen.values())

        tot_rows += t.num_rows
        tot_out += len(keep)
        tot_keys_before += len(set(keys))
        tot_keys_after += len({new[j] for j in keep})
        touched += 1

        if a.apply:
            tbl = pa.table({
                "series_key": pa.array([new[j] for j in keep], pa.string()),
                "obs_date":   pa.array([dates[j] for j in keep], pa.date32()),
                "value":      pa.array([vals[j] for j in keep], pa.float64()),
            })
            blob.write_table_atomic(path, tbl)
        if i % 500 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] touched={touched:,} rows={tot_rows:,} -> {tot_out:,}", flush=True)

    print(f"\nfiles needing re-key : {touched:,}")
    print(f"files already clean  : {clean:,}")
    print(f"rows                 : {tot_rows:,} -> {tot_out:,}  (collapsed {tot_rows - tot_out:,})")
    print(f"distinct series_key  : {tot_keys_before:,} -> {tot_keys_after:,}")
    print(f"(key,date) pairs whose duplicates disagreed on value: {revisions:,}  "
          f"(kept the LAST, matching merge's new-wins-on-revision)")
    if a.dry_run:
        print("\n--dry-run: nothing written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
