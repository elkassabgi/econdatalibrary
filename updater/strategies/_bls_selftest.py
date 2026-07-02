"""NON-DESTRUCTIVE isolation test for the S5 bls fetcher.

Seeds an ISOLATED temp data root with copies of a few production BLS survey
parquets (one per on-disk schema variant + one no-Current fallback survey),
redirects config.DATA_ROOT to it, and exercises the REAL fetcher end to end:
  - per-survey Last-Modified vintage gate,
  - download the cheap Current/AllData tail,
  - parse conformed to the existing per-file schema,
  - merge (dedup series_id+obs_date, never-shrink),
  - NO new (series_id,obs_date) duplicates introduced,
  - idempotency: a second run does not grow any file,
  - schema preserved per file,
  - honest status.

PRODUCTION parquet is only READ once to seed the temp copy; never written.
Run:  AQUEDUCT_DATA_ROOT set internally; python -m updater.strategies._bls_selftest
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

import pyarrow.parquet as pq
import pyarrow.compute as pc

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

PROD_BLS = os.path.join(ROOT, "data", "clean_full", "bls")

# (survey, truncate_to_series) — pick one of each schema variant + a no-Current survey.
#   ap : date32 + footnote schema, has ap.data.0.Current  (small, ~1.1MB tail)
#   cu : string obs_date schema, has cu.data.0.Current; carries the KNOWN dups
#   bg : date32+footnote schema, NO Current cut -> AllData fallback (tiny, discontinued 1995)
SEED = [("ap", None), ("cu", 200), ("bg", None)]


def _dup_count(path):
    t = pq.read_table(path, columns=["series_id", "obs_date"])
    n = t.num_rows
    sids = t.column("series_id").to_pylist()
    ods = t.column("obs_date").to_pylist()
    uniq = len(set(zip(sids, ods)))
    return n, uniq, n - uniq


def _seed_truncated(src, dst, keep_series):
    """Copy src->dst, optionally keeping only the first `keep_series` distinct series
    (keeps the test fast for huge surveys while preserving the real schema/keys)."""
    import pyarrow as pa
    t = pq.read_table(src)
    if keep_series is not None:
        sids = t.column("series_id")
        # first keep_series distinct series_ids, in file order
        keep = set()
        for s in sids.to_pylist():
            if s not in keep:
                keep.add(s)
                if len(keep) >= keep_series:
                    break
        mask = pc.is_in(sids, value_set=pa.array(list(keep)))
        t = t.filter(mask)
    pq.write_table(t, dst, compression="zstd")
    return t


def main():
    tmp_root = tempfile.mkdtemp(prefix="bls_selftest_")
    os.environ["AQUEDUCT_DATA_ROOT"] = tmp_root
    # Import AFTER setting the env so config.DATA_ROOT picks up the temp root.
    from updater import config  # noqa: E402
    from updater.strategies.base import Unit  # noqa: E402
    from updater.strategies.fetchers import bls  # noqa: E402

    assert config.DATA_ROOT == os.path.abspath(tmp_root), \
        f"DATA_ROOT not redirected: {config.DATA_ROOT}"
    out_dir = config.source_dir("bls")
    os.makedirs(out_dir, exist_ok=True)
    print(f"isolated temp data root: {tmp_root}")
    print(f"bls dir: {out_dir}\n")

    seeded = {}
    for sv, keep in SEED:
        src = os.path.join(PROD_BLS, f"{sv}.parquet")
        if not os.path.exists(src):
            print(f"  SKIP seed {sv}: not in production")
            continue
        dst = os.path.join(out_dir, f"{sv}.parquet")
        _seed_truncated(src, dst, keep)
        n, uniq, dups = _dup_count(dst)
        sch = [(f.name, str(f.type)) for f in pq.ParquetFile(dst).schema_arrow]
        seeded[sv] = {"rows": n, "uniq": uniq, "dups": dups, "schema": sch}
        print(f"  seeded {sv}: rows={n:,} uniq(series,date)={uniq:,} preexisting_dups={dups:,}")
        print(f"           schema={sch}")
    print()

    unit = Unit(source_id="bls", unit_id="_all", strategy="bulk_snapshot_if_changed",
                cadence="weekly", out_paths=[out_dir], config={})

    # ---- current_vintage probe (cheap) ----
    cv = bls.current_vintage(unit)
    print(f"current_vintage() -> {cv}")
    assert cv is not None, "probe returned None despite seeded surveys"

    # ---- RUN #1 (real download + parse + merge) ----
    print("\n=== RUN #1 ===")
    before = {sv: _dup_count(os.path.join(out_dir, f"{sv}.parquet"))[0] for sv in seeded}
    r1 = bls.update(unit, since=None)
    print(f"  result: status={r1.status} obs={r1.obs} last_obs={r1.last_obs_date}")
    print(f"          {r1.error}")
    assert r1.status in ("ok", "no_change", "partial"), f"unexpected status {r1.status}"

    after1 = {}
    ok = True
    for sv, info in seeded.items():
        path = os.path.join(out_dir, f"{sv}.parquet")
        n, uniq, dups = _dup_count(path)
        after1[sv] = n
        sch = [(f.name, str(f.type)) for f in pq.ParquetFile(path).schema_arrow]
        # never-shrink
        if n < before[sv]:
            print(f"  FAIL {sv}: SHRANK {before[sv]:,} -> {n:,}"); ok = False
        # no NEW duplicates beyond the pre-existing legacy dups
        new_dups = dups - info["dups"]
        # schema preserved
        schema_ok = (sch == info["schema"])
        print(f"  {sv}: rows {before[sv]:,} -> {n:,} (+{n-before[sv]:,}) | "
              f"dups {info['dups']:,} -> {dups:,} (new {new_dups:+,}) | "
              f"schema_preserved={schema_ok}")
        if new_dups > 0:
            print(f"  FAIL {sv}: introduced {new_dups} NEW (series_id,obs_date) duplicates"); ok = False
        if not schema_ok:
            print(f"  FAIL {sv}: schema changed -> {sch}"); ok = False

    # ---- RUN #2 (idempotency: vintage now matches -> should skip; rows unchanged) ----
    print("\n=== RUN #2 (idempotency) ===")
    r2 = bls.update(unit, since=None)
    print(f"  result: status={r2.status} obs={r2.obs}  ({r2.error})")
    for sv in seeded:
        path = os.path.join(out_dir, f"{sv}.parquet")
        n = _dup_count(path)[0]
        if n != after1[sv]:
            print(f"  FAIL {sv}: NON-IDEMPOTENT {after1[sv]:,} -> {n:,}"); ok = False
        else:
            print(f"  {sv}: rows stable at {n:,}  (idempotent)")
    # run#2 must not be 'ok' with added rows (nothing changed upstream within seconds)
    if r2.status == "ok":
        print(f"  NOTE run#2 status=ok (added {r2.obs}); acceptable only if upstream moved between runs")

    shutil.rmtree(tmp_root, ignore_errors=True)
    print("\n" + ("ALL CHECKS PASS" if ok else "FAILURES DETECTED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
