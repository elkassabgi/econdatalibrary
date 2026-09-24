"""tools/series_census.py after T0 (plan: MOVE series_census): the served parquet store is the local one, so the
census lists local files, reads nothing over s3:// (no R2 client, no credentials handed to DuckDB), and publishes
_aqueduct/stats.json into the self-hosted blob store /v1/stats reads - through updater.blob's single-writer rule.
Before T0 it measures R2 as it always did."""
import datetime as dt
import io
import json
import os
import sqlite3
import sys
import urllib.request

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import series_census  # noqa: E402
from blobstore import BlobStore  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    live = tmp_path / "live"
    full = live / "data" / "clean_full"
    for src, keys in (("eurostat", ["a", "a", "b"]), ("oecd", ["x"])):
        (full / src).mkdir(parents=True)
        pq.write_table(pa.table({"series_key": keys, "value": list(range(len(keys)))}), full / src / "t.parquet")
    (full / "gated_thing").mkdir()
    pq.write_table(pa.table({"series_key": ["g"]}), full / "gated_thing" / "t.parquet")
    build = live / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", [("eurostat:a", "eurostat"), ("oecd:x", "oecd")])
    c.close()
    monkeypatch.setattr(series_census, "ROOT", str(live))
    monkeypatch.setattr(series_census, "ROOTS", [str(full), str(live / "data" / "clean_grouped")])
    monkeypatch.setattr(series_census, "resolvable_sources", lambda: {"eurostat", "oecd", "bls", "bea", "worldbank"})
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(live))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(live))
    monkeypatch.setattr(updater_config, "ROOT", str(live))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))

    def no_r2(*a, **k):
        raise AssertionError("after T0 the census must not touch R2")
    monkeypatch.setattr(blob, "R2Blob", no_r2)
    monkeypatch.setattr(series_census, "_wire_r2", no_r2)
    (tmp_path / "CUTOVER").write_text("")
    yield tmp_path, live, full
    if blob._process_session is not None:
        blob._process_session.__exit__(None, None, None)
        blob._process_session = None


def test_after_t0_the_served_keys_are_the_local_store(world):
    _tmp, _live, full = world
    keys = series_census.served_keys()
    assert keys == {f"clean_full/{s}/t.parquet": os.path.getsize(full / s / "t.parquet")
                    for s in ("eurostat", "gated_thing", "oecd")}


def test_after_t0_every_kept_file_is_read_locally(world):
    kept, dropped = series_census.keep_served(series_census.source_files())
    assert set(kept) == {"eurostat", "oecd"}, "the worker's unresolvable source is not counted"
    assert all(not f.startswith("s3://") for files in kept.values() for f in files)
    assert dropped == {}


def test_after_t0_a_publish_lands_in_the_self_hosted_store(world, monkeypatch):
    tmp, _live, _full = world
    today = dt.date.today().isoformat()
    monkeypatch.setattr(urllib.request, "urlopen",            # the live check; no network in a test
                        lambda req, timeout=60: io.BytesIO(json.dumps({"as_of": today}).encode()))
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    assert series_census.main() == 0
    sb = blob.SelfhostBlob()
    stats = json.loads(sb.get(series_census.KEY))
    assert (stats["observations"], stats["individual_series"], stats["sources_catalogued"]) == (4, 3, 2)
    assert sb.store.head(series_census.KEY)["content_type"] == "application/json"
    assert sb.store.head(series_census.KEY)["content_encoding"] is None, "stats.json stays plain"
    assert catalog_path._held is not None, "the publish took the single-writer lock"


def test_after_t0_a_publish_from_another_checkout_is_refused(world, monkeypatch):
    tmp, _live, _full = world
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    counted = []
    monkeypatch.setattr(series_census, "_rows_and_key_batch", lambda *a: counted.append(a) or (0, [], 0))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        series_census.main()
    assert counted == [], "refused before the counting starts, not after it"
    assert blob.SelfhostBlob().get(series_census.KEY) is None
