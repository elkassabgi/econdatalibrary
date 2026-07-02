#!/usr/bin/env python3
"""Run ONE connector end-to-end -- the generalization of HF Data Library's daily script.

LOCAL mode: writes per-series Parquet under data/clean/ and a SQLite catalog
(data/catalog.db) -- faithful stand-ins for R2 + Cloudflare D1. The license gate
runs at both source and per-series level, so restricted data can never be published.

Usage:  python jobs/run_connector.py worldbank
"""
import os
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from core.licenses import assert_reservable          # noqa: E402
from core import catalog, storage                    # noqa: E402


def load_connector(source_id):
    """Auto-discover the Connector subclass in connectors/<source_id>/connector.py."""
    import importlib
    import inspect
    from connectors.base import Connector
    mod = importlib.import_module(f"connectors.{source_id}.connector")
    for _, obj in inspect.getmembers(mod, inspect.isclass):
        if issubclass(obj, Connector) and obj is not Connector and obj.__module__ == mod.__name__:
            return obj()
    raise SystemExit(f"no Connector subclass found in connectors.{source_id}.connector")


def run(source_id):
    t0 = time.time()
    c = load_connector(source_id)
    assert_reservable(c.license_id, context=f"source:{source_id}")     # free-only gate (source)
    db = catalog.connect()
    fts = catalog.init(db)
    catalog.upsert_source(db, c.source_id, getattr(c, "name", source_id),
                          c.license_id, c.attribution, getattr(c, "homepage", None))
    n_series = n_obs = 0
    for meta, obs in c.fetch(None):
        assert_reservable(meta.license_id, context=f"series:{meta.series_id}")  # gate (per-series)
        if not obs:
            continue
        storage.write_series_parquet(meta.series_id, obs)             # -> Parquet (R2 stand-in)
        dates = [o.obs_date for o in obs]
        catalog.upsert_series(db, meta, start=str(min(dates)), end=str(max(dates)))  # -> catalog (D1 stand-in)
        n_series += 1
        n_obs += len(obs)
    db.commit()
    catalog.rebuild_fts(db, fts)
    print(f"[{source_id}] ingested {n_series:,} series / {n_obs:,} observations "
          f"in {time.time()-t0:.1f}s  (search={'FTS5' if fts else 'LIKE-fallback'})")
    return db


def demo(db):
    print("\n--- proving the serve path: search -> series -> observations ---")
    hits = catalog.search(db, "GDP", limit=5)
    print(f"search('GDP') -> {len(hits)} hits; first 3:")
    for r in hits[:3]:
        print(f"   {r['series_id']}  |  {r['title']}")
    sid = "worldbank:NY.GDP.MKTP.CD:USA"
    s = catalog.get_series(db, sid)
    if s:
        obs = storage.read_series(sid)
        print(f"\nGET /v1/series/{sid}")
        print(f"  title:   {s['title']}")
        print(f"  license: {s['license_id']}   span: {s['start_date']} .. {s['end_date']}   points: {len(obs)}")
        print("  last 3 observations (GET /v1/observations):")
        for o in obs[-3:]:
            print(f"     {o['obs_date']}   {o['value']:,.0f}")


if __name__ == "__main__":
    src = sys.argv[1] if len(sys.argv) > 1 else "worldbank"
    database = run(src)
    if src == "worldbank":
        demo(database)
