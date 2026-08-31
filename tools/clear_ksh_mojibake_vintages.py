"""WU-3 step 1: clear the sidecar vintages of every ksh table whose stored keys carry U+FFFD.

WHAT THIS ACTUALLY FIXES — corrected 2026-08-31 after the adversarial review refuted the
plan's premise (ledger R540). WU-3's spec claimed "~1,931 catalogued series are unservable
today ... a mojibake-only store". Measured, ZERO are: of 98,423 catalogued ksh ids, 97,520
are in the store as CLEAN keys and 0 as FFFD; the 13,140 catalogued keys under the affected
tables are all present; and catalogued ids whose FFFD twin exists serve non-empty CSVs today
(5/5 probed). So the spec's exit gate — "a previously-FFFD id derives a NON-EMPTY CSV" —
passes BEFORE this runs and cannot tell success from doing nothing.

The real defect is STALENESS, not emptiness. 2,898 FFFD keys have a clean twin that is both
catalogued and in the store, so the old decode has been posting fresh observations into an
orphan key while the SERVED twin froze — 1,847 catalogued keys are measurably stale. Clearing
these vintages makes the fixed decoder (commit 25178593a; the docstring previously cited
7b8ac3900, which is in no branch) re-fetch those tables so the fresh observations land on the
served key instead.

KNOWN COST, disclosed rather than discovered later: roughly 2,595 of the FFFD keys have no
clean counterpart anywhere, because their "row label" is actually a row of DATA VALUES the
parser mistook for a label. Decoding them correctly still yields garbage, so the re-fetch will
mint ~2,595 clean-but-uncatalogued orphan keys. That is R380's shape in the store rather than
the mapper, and it is a cost of this unit, not a benefit.

EXIT GATE (replaces the vacuous one): for a named table, a CATALOGUED clean id's served CSV
gains observations beyond its pre-clear max obs_date. Record that baseline BEFORE running —
`--baseline gdp0111,gsz0058,ege0020` writes it.

THE POPULATION IS READ FROM R2, NOT FROM THE LOCAL GLOB. The sidecar being rewritten lives on
R2, and 8 of 28 local theme parquets differ from their R2 copies. The first cut refused a local
SIDECAR read and then silently scanned the local STORE — the guard testing the half that is not
the risk, R503's shape. Files whose name starts with '_' are archives
(`_discontinued_from_ksh`, `_migrated_from_ksh_unparsed`) and are excluded.

THE SIDECAR WRITE IS COMPARE-AND-SWAP. `R2Blob.put_atomic` is a bare PUT with no If-Match, and
`update()` rewrites the WHOLE sidecar at the end of a run, so a run that started before this
one can silently restore every cleared entry AFTER the read-back has printed success. This
captures the ETag before reading and refuses if it moved before the PUT.

DRAIN, HONESTLY: clearing these does NOT mean "a few daily ticks". ksh's cadence is `irregular`
(a 7-day TTL) and it has run six times in its life. The queue already holds ~1,091 tables, so
at MAX_PER_RUN=60 the cleared tables land over about 7 runs — roughly 7 WEEKS — and only the
tables that were not already due change anything at all (measured and reported per run below).

Usage:
  AQUEDUCT_BACKEND=r2 py tools/clear_ksh_mojibake_vintages.py --baseline gdp0111,gsz0058,ege0020
  AQUEDUCT_BACKEND=r2 py tools/clear_ksh_mojibake_vintages.py            # dry run
  AQUEDUCT_BACKEND=r2 py tools/clear_ksh_mojibake_vintages.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob, config  # noqa: E402
from tools.prune_series_cursors import runs_in_flight  # noqa: E402

SIDECAR_NAME = "_bulk_vintages.json"
FFFD = "�"
FEFF = "﻿"
MAX_PER_RUN = 60          # mirrors ksh_stadat.MAX_PER_RUN
CADENCE_DAYS = 7          # `irregular` TTL, updater/strategies/base.py


def _store_scan() -> tuple[set[str], dict]:
    """Affected table ids + population stats, read THROUGH blob (i.e. from R2 under r2).

    Returns (table_ids, stats). Archive parquets (names starting '_') are excluded: they
    are retired snapshots, not the live store, and including them inflated the first cut's
    row and key counts by 11,775 / 903.
    """
    store = config.source_dir("ksh_stadat")
    names = [n for n in blob.list_parquets(store) if not n.startswith("_")]
    if not names:
        raise SystemExit(f"no live ksh parquets under {store} — refusing to act on an "
                         f"empty read (that means the backend or path is wrong)")
    rows = 0
    keys: set[str] = set()
    bad_keys: set[str] = set()
    feff_keys: set[str] = set()
    per_table: dict[str, set[str]] = {}
    bad_rows = 0
    for n in names:
        t = blob.read_table(os.path.join(store, n), columns=["series_key"])
        col = t.column("series_key").to_pylist()
        rows += len(col)
        for k in col:
            if k is None:
                continue
            keys.add(k)
            if FFFD in k or FEFF in k:
                bad_rows += 1
                bad_keys.add(k)
                if FEFF in k:
                    feff_keys.add(k)
                parts = k.split(":")
                if len(parts) >= 2:
                    per_table.setdefault(parts[1], set()).add(k)
    stats = {
        "live_store_files": len(names),
        "live_store_rows": rows,
        "live_distinct_keys": len(keys),
        "affected_rows": bad_rows,
        "affected_distinct_keys": len(bad_keys),
        "feff_distinct_keys": len(feff_keys),
        "affected_tables": len(per_table),
    }
    return set(per_table), stats


def _drain_forecast(sidecar_after: dict) -> dict:
    """How many tables the fetcher would actually re-fetch, before vs after the clear."""
    try:
        from jobs import ingest_ksh_stadat as ig
        from updater.strategies.fetchers import ksh_stadat as fetch
        cat = ig.load_catalog()
    except Exception as e:                                       # noqa: BLE001
        return {"todo_before": None, "todo_after": None,
                "note": f"catalogue unavailable ({type(e).__name__}) — drain not forecast"}

    def _todo(sc: dict) -> int:
        n = 0
        store = config.source_dir("ksh_stadat")
        for e in cat:
            tid = fetch._table_id(e)
            if not tid:
                continue
            theme_path = os.path.join(store, f"{tid[:3].lower()}.parquet")
            if sc.get(tid) == fetch._vintage(e) and blob.exists(theme_path):
                continue
            n += 1
        return n
    return {"catalogue_tables": len(cat), "todo_after": _todo(sidecar_after)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--baseline", metavar="T1,T2,...",
                    help="record max obs_date per CATALOGUED clean key for these tables, "
                         "so the exit gate can FAIL (it is the pre-clear watermark)")
    ap.add_argument("--force-unsafe", action="store_true")
    a = ap.parse_args()

    if config.BACKEND != "r2":
        print(f"REFUSED: backend is {config.BACKEND!r}. The authoritative ksh sidecar AND the "
              f"authoritative store both live on R2; the local copies are stale (8 of 28 "
              f"theme parquets differ) and the local sidecar is absent. "
              f"Re-run with AQUEDUCT_BACKEND=r2.")
        return 2

    tables, stats = _store_scan()
    print(json.dumps(stats, indent=1))

    path = os.path.join(config.source_dir("ksh_stadat"), SIDECAR_NAME)
    b = blob.from_env("r2")
    key = blob._path_to_key(path)
    etag_before = b.etag(key)
    raw = blob.read_bytes(path)
    sidecar = json.loads(raw.decode("utf-8")) if raw else {}
    print(f"sidecar entries before: {len(sidecar):,}  (etag {etag_before})")
    if not sidecar:
        print("REFUSED: sidecar is empty — that means the backend or path is wrong, not "
              "that the work is done.")
        return 2

    present = sorted(t for t in tables if t in sidecar)
    missing = sorted(t for t in tables if t not in sidecar)
    print(f"affected tables in store : {len(tables):,}")
    print(f"  ... present in sidecar : {len(present):,}  (these get cleared -> re-fetched)")
    print(f"  ... absent from sidecar: {len(missing):,}  (already unpinned; nothing to do)")
    if missing:
        print(f"      e.g. {missing[:5]}")

    if a.baseline:
        return _write_baseline([t.strip() for t in a.baseline.split(",") if t.strip()])

    after_preview = {k: v for k, v in sidecar.items() if k not in set(present)}
    fc = _drain_forecast(after_preview)
    if fc.get("todo_after") is not None:
        # TWO different questions, both reported because reporting one invites the other's
        # answer to be assumed. The fetcher takes `todo` SORTED by table id, so where a
        # cleared table sits in that order decides when IT lands, while the whole backlog
        # takes far longer to drain.
        all_runs = -(-fc["todo_after"] // MAX_PER_RUN)
        rank = max((sorted(present).index(t) + 1) for t in present) if present else 0
        mine_runs = -(-rank // MAX_PER_RUN) if rank else 0
        print(f"drain forecast: {fc['todo_after']:,} table(s) queued after the clear, of "
              f"{fc.get('catalogue_tables', '?')} catalogued. The 181 CLEARED tables sort "
              f"early (last one at position ~{rank}), so they land in ~{mine_runs} run(s) "
              f"= ~{mine_runs * CADENCE_DAYS} days; the WHOLE backlog needs ~{all_runs} "
              f"run(s) = ~{all_runs * CADENCE_DAYS} days at a {CADENCE_DAYS}-day "
              f"'irregular' cadence. Neither is 'a few daily ticks'.")
    else:
        print("drain forecast:", fc.get("note"))

    if not a.apply:
        print("(dry run — pass --apply to clear and publish)")
        return 0
    if not present:
        print("nothing to clear")
        return 0

    blockers = runs_in_flight()
    if blockers and not a.force_unsafe:
        print("REFUSING — a run may rewrite the whole sidecar underneath this clear:")
        for x in blockers:
            print(f"  - {x}")
        return 2

    backup = os.path.join(ROOT, "data", "_aqueduct",
                          "ksh_bulk_vintages.before_wu3_clear.json")
    os.makedirs(os.path.dirname(backup), exist_ok=True)
    with open(backup, "w", encoding="utf-8") as f:
        json.dump(sidecar, f, indent=1, sort_keys=True)
    print("backed up the pre-clear sidecar to", backup)

    # COMPARE-AND-SWAP. The scan above takes seconds; a run that finished in that window
    # would have republished the whole sidecar, and our PUT would silently discard it.
    etag_now = b.etag(key)
    if etag_now != etag_before:
        print(f"ABORT: the sidecar changed under us ({etag_before} -> {etag_now}). A ksh run "
              f"published while this was scanning. Nothing written; re-run.")
        return 2

    expected = {k: v for k, v in sidecar.items() if k not in set(present)}
    blob.write_bytes_atomic(path, json.dumps(expected, sort_keys=True).encode("utf-8"))

    verify = blob.read_bytes(path)
    after = json.loads(verify.decode("utf-8")) if verify else {}
    if after != expected:
        print(f"MISMATCH after read-back: {len(after):,} entries, expected {len(expected):,} "
              f"and byte-equal content. NOTE the LOCAL sidecar was also written by "
              f"write_bytes_atomic. Restore from {backup} before any re-fetch.")
        return 1
    print(f"cleared {len(present):,} table vintage(s); sidecar now {len(after):,} entries.")
    print("REMINDER: merge is never-shrink, so the re-fetch ADDS clean keys and the "
          f"{stats['affected_rows']:,} mojibake rows STAY in the store until a separate, "
          f"deliberate purge — and ~2,595 clean-but-uncatalogued orphan keys will be minted, "
          f"because those FFFD 'labels' are rows of data values. Verify the clear SURVIVED by "
          f"re-reading this sidecar AFTER the next ksh run, not before.")
    return 0


def _write_baseline(tables: list[str]) -> int:
    """Pre-clear watermark: max obs_date per catalogued clean key, so the gate can FAIL."""
    import sqlite3
    store = config.source_dir("ksh_stadat")
    cat = sqlite3.connect(
        f"file:{os.environ.get('ECONDL_CATALOG') or os.path.join(config.ROOT, 'data', 'catalog.db')}"
        f"?mode=ro", uri=True)
    catalogued = {r[0][len("ksh_stadat:"):] for r in cat.execute(
        "SELECT series_id FROM series WHERE series_id >= 'ksh_stadat:' "
        "AND series_id < 'ksh_stadat;'")}
    out: dict[str, str] = {}
    for t in tables:
        theme = t[:3].lower()
        tbl = blob.read_table(os.path.join(store, f"{theme}.parquet"),
                              columns=["series_key", "obs_date"])
        ks = tbl.column("series_key").to_pylist()
        ds = tbl.column("obs_date").to_pylist()
        for k, d in zip(ks, ds):
            if not k or FFFD in k or f":{t}:" not in f":{k}":
                continue
            if k not in catalogued or d is None:
                continue
            iso = d.isoformat()
            if k not in out or iso > out[k]:
                out[k] = iso
    p = os.path.join(ROOT, "data", "_aqueduct", "ksh_wu3_baseline.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"tables": tables, "catalogued_clean_keys": len(out), "max_obs_date": out},
                  f, indent=1, sort_keys=True)
    print(f"baseline: {len(out):,} catalogued clean key(s) across {tables} -> {p}")
    print("EXIT GATE: after the drain, one of these keys must show a LATER max obs_date. "
          "If none does, the clear achieved nothing and the unit FAILED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
