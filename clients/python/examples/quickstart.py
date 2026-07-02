"""econdl quickstart + end-to-end reproducibility check.

Runs the full differentiator loop on REAL local data:

  1. search the catalog
  2. bundle 5 real series spanning 3 sources -> tidy frame + datapackage.json lockfile + zip
  3. pull() the lockfile back and ASSERT the data reproduces exactly (row + value identity)
  4. show that pull() verifies sha256 (tamper -> raises)
  5. show that pull(latest=True) loudly WARNS, never silently skips, an unsatisfiable series

Run:  python clients/python/examples/quickstart.py
"""
from __future__ import annotations

import os
import sys
import warnings

import pandas as pd

# Make the sibling package importable when run from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import econdl  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "_out", "mystudy.zip")
os.makedirs(os.path.dirname(OUT), exist_ok=True)

# 5 real series across 3 sources & 2 license families:
#   bls (us-public-domain), worldbank_wdi (cc-by-4.0), penn_world_table (cc-by-4.0)
SERIES = [
    "bls:LNS14000000",                 # US unemployment rate (SA, %)
    "bls:CUUR0000SA0",                 # CPI-U, all items (NSA)
    "worldbank_wdi:NY.GDP.MKTP.KD.ZG", # GDP growth (annual %), all countries
    "worldbank_wdi:SP.POP.TOTL",       # Population, total, all countries
    "penn_world_table:rgdpe:USA",      # PWT real GDP (expenditure side), USA
]


def main() -> int:
    print("=" * 72)
    print("1) econdl.search('unemployment rate')")
    for row in econdl.search("unemployment rate", limit=3):
        print(f"   {row['series_id']:34s} {row['title']}")

    print("=" * 72)
    print(f"2) econdl.bundle({len(SERIES)} series across "
          f"{len({s.split(':')[0] for s in SERIES})} sources) -> {OUT}")
    df = econdl.bundle(SERIES, out=OUT)
    print(f"   tidy frame: {len(df):,} rows  x  {df.shape[1]} cols   {list(df.columns)}")
    print(f"   sources:    {sorted(df['source'].unique())}")
    print(f"   distinct native series: {df['series_id'].nunique()}")
    print(df.groupby("source").agg(rows=("value", "size"),
                                   series=("series_id", "nunique")).to_string())

    bundle_dir = os.path.splitext(OUT)[0]
    dp = os.path.join(bundle_dir, "datapackage.json")
    print(f"   wrote: {dp}")
    print(f"   wrote: {OUT}")
    for f in sorted(os.listdir(os.path.join(bundle_dir, "data"))):
        print(f"   resource: data/{f}")

    print("=" * 72)
    print("3) econdl.pull(datapackage.json)  -> reproduce EXACT pinned snapshot")
    repro = econdl.pull(dp)
    print(f"   reproduced frame: {len(repro):,} rows")

    # ASSERT exact reproduction (row count, ids, dates, values).
    assert len(repro) == len(df), f"row count differs: {len(repro)} vs {len(df)}"
    a = df.sort_values(["series_id", "obs_date"]).reset_index(drop=True)
    b = repro.sort_values(["series_id", "obs_date"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_like=False)
    # Independent hash of the value vectors as a second witness.
    ha = pd.util.hash_pandas_object(a[["series_id", "obs_date", "value"]], index=False).sum()
    hb = pd.util.hash_pandas_object(b[["series_id", "obs_date", "value"]], index=False).sum()
    assert ha == hb, "value hash mismatch"
    print(f"   ASSERT PASSED: bundle() and pull() are row-for-row identical "
          f"({len(df):,} rows; value-hash {ha} == {hb})")

    print("=" * 72)
    print("4) tamper detection: corrupt a resource, pull() must REFUSE")
    res_path = os.path.join(bundle_dir, "data", "bls.parquet")
    with open(res_path, "ab") as fh:
        fh.write(b"\x00corrupt")
    try:
        econdl.pull(dp)
        print("   FAIL: pull() returned corrupted data without complaint")
        return 1
    except (ValueError, Exception) as e:  # parquet may fail to read, or sha256 mismatch
        print(f"   ASSERT PASSED: pull() refused corrupted bundle -> {type(e).__name__}: "
              f"{str(e).splitlines()[0][:90]}")
    # rebuild the clean bundle for the next step
    econdl.bundle(SERIES, out=OUT)

    print("=" * 72)
    print("5) pull(latest=True) with an unsatisfiable pinned series -> LOUD WARNING, no silent skip")
    # Hand-craft a SEPARATE lockfile (leave the real one pristine) that pins a
    # series with no resolver, to prove the guard.
    import json
    import shutil
    demo_dir = os.path.join(os.path.dirname(OUT), "_warn_demo")
    if os.path.exists(demo_dir):
        shutil.rmtree(demo_dir)
    shutil.copytree(bundle_dir, demo_dir)
    dp = os.path.join(demo_dir, "datapackage.json")
    with open(dp, encoding="utf-8") as fh:
        dp_obj = json.load(fh)
    dp_obj["econdl:series_requested"] = sorted(set(dp_obj["econdl:series_requested"]
                                                   + ["faostat:does-not-resolve:XYZ"]))
    with open(dp, "w", encoding="utf-8") as fh:
        json.dump(dp_obj, fh, indent=2)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        latest = econdl.pull(dp, latest=True)
        warned = [w for w in caught if "could NOT be refreshed" in str(w.message)]
    assert warned, "expected a loud warning for the unsatisfiable series"
    assert "faostat:does-not-resolve:XYZ" in latest.attrs.get("econdl_missing", []), \
        "missing series not reported in attrs"
    print(f"   ASSERT PASSED: {len(warned)} warning(s) raised; "
          f"missing={latest.attrs['econdl_missing']}; "
          f"satisfied={len(latest.attrs['econdl_satisfied'])} series")
    print(f"   latest frame still returned for the satisfiable series: {len(latest):,} rows")

    print("=" * 72)
    print("ALL ASSERTIONS PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
