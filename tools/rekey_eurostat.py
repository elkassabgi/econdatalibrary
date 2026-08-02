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

import pyarrow.compute as pc                                       # noqa: E402
from updater import blob, config                                   # noqa: E402
from updater.merge import _dedup                                   # noqa: E402
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
    # flush=True on EVERY progress line. Without it stdout is block-buffered when redirected
    # to a file, so a long run emits NOTHING until it exits — 18 minutes of a dry run with no
    # way to tell progress from a hang. A job that says nothing can only be guessed at.
    print(f"eurostat store: {len(files):,} file(s) under {out_dir}  (backend={config.BACKEND})",
          flush=True)

    tot_rows = tot_out = touched = clean = 0
    tot_keys_before = tot_keys_after = 0
    for i, fn in enumerate(files, 1):
        path = os.path.join(out_dir, fn)
        # CHEAP SKIP FIRST: one column, one value. A key scheme is uniform within a file (one
        # ingest wrote it), so a single row answers "does this need re-keying?".
        try:
            probe = blob.read_table(path, columns=["series_key"])
        except Exception as e:                                  # noqa: BLE001
            print(f"  [{i}/{len(files)}] {fn}: UNREADABLE ({type(e).__name__}) — skipped", flush=True)
            continue
        if probe.num_rows == 0:
            continue
        head = probe.column("series_key")[0].as_py()
        if not head or "LAST UPDATE" not in head:
            clean += 1
        else:
            t = blob.read_table(path)
            before_rows = t.num_rows
            # TRANSFORM DISTINCT KEYS, NOT ROWS. AACT_ALI01 holds 3,945 rows across 219
            # distinct keys — running the regex per row is 18x redundant, and over 7,754 files
            # that is the difference between a ~6-hour pass and a short one.
            # Dictionary-encode so the regex runs over the DICTIONARY (219 values in
            # AACT_ALI01) and the row-wise expansion is an Arrow take() — no multi-million-row
            # Python list is ever materialised. col.to_pylist() on the big files was the
            # remaining cost after the per-row regex was removed.
            col = t.column("series_key").combine_chunks()
            enc = pc.dictionary_encode(col)
            dic = enc.dictionary if hasattr(enc, "dictionary") else enc.chunk(0).dictionary
            idx = enc.indices if hasattr(enc, "indices") else enc.chunk(0).indices
            newdict = pa.array([stable_key(k) if k else k for k in dic.to_pylist()], pa.string())
            newcol = pc.take(newdict, idx)
            uniq = dic.to_pylist()
            mapping = {k: stable_key(k) for k in uniq if k}
            t = t.set_column(t.column_names.index("series_key"), "series_key", newcol)
            # Dedup with the SAME routine the merge path uses, so this migration and the
            # fetcher agree on identity — including its null handling (R254).
            t = _dedup(t, ["series_key", "obs_date"])
            tot_rows += before_rows
            tot_out += t.num_rows
            tot_keys_before += len(uniq)
            tot_keys_after += len(set(mapping.values()))
            touched += 1
            if a.apply:
                blob.write_table_atomic(path, t)
        if i % 100 == 0 or i == len(files):
            print(f"  [{i}/{len(files)}] touched={touched:,} clean={clean:,} "
                  f"rows={tot_rows:,} -> {tot_out:,}", flush=True)

    print(f"\nfiles needing re-key : {touched:,}")
    print(f"files already clean  : {clean:,}")
    print(f"rows                 : {tot_rows:,} -> {tot_out:,}  (collapsed {tot_rows - tot_out:,})")
    print(f"distinct series_key  : {tot_keys_before:,} -> {tot_keys_after:,}")
    if a.dry_run:
        print("\n--dry-run: nothing written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
