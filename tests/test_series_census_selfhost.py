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

_REAL_WIRE_R2 = series_census._wire_r2          # the fixture replaces it with a tripwire


@pytest.fixture
def world(tmp_path, monkeypatch):
    live = tmp_path / "live"
    full = live / "data" / "clean_full"
    for src, keys in (("eurostat", ["a", "a", "b"]), ("oecd", ["x"]), ("bls", ["z"])):
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
    sb = blob.SelfhostBlob()
    for key in ("series/eurostat%3Aa.csv", "series/oecd%3Ax.csv"):     # served CSVs; bls has none (R1221)
        sb.put_atomic(key, b"series_id,obs_date,value\n")
    monkeypatch.setattr(catalog_path, "LIVE_STATE_DIR", str(tmp_path / "live_state"))

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
                    for s in ("bls", "eurostat", "gated_thing", "oecd")}


def test_after_t0_a_source_counts_only_with_csvs_in_the_served_store(world, capsys):
    """R1221 finding 1: after T0 "served" had become "on local disk". A resolvable source whose CSVs the served
    store does not hold (bls here) is not downloadable, so it is not counted."""
    kept, dropped = series_census.keep_served(series_census.source_files())
    assert set(kept) == {"eurostat", "oecd"}, "unresolvable and CSV-less sources are not counted"
    assert dropped == {"bls": 1}
    assert all(not f.startswith("s3://") for files in kept.values() for f in files)
    out = capsys.readouterr().out
    assert "eurostat: 1 local parquet file(s), 1 served CSV(s)" in out and "bls (1 files)" in out


def test_after_t0_nothing_goes_to_r2_even_when_sizes_differ(world, monkeypatch):
    """R1221 finding 2: a file rewritten after served_keys() read a different size, and keep_served sent DuckDB
    to s3:// with the write key. After T0 sizes are not compared, and _wire_r2 itself is refused."""
    monkeypatch.setattr(series_census, "served_keys", lambda: {"clean_full/eurostat/t.parquet": 1})
    kept, _dropped = series_census.keep_served(series_census.source_files())
    assert all(not f.startswith("s3://") for files in kept.values() for f in files)
    with pytest.raises(cutover.CutoverRefused, match="R1221"):
        _REAL_WIRE_R2(object())                               # the real one, not the fixture's stand-in


def test_before_t0_the_served_store_is_r2s_listing(world, monkeypatch, tmp_path):
    """The pre-T0 half (a mutant that always listed local files survived R1221): a local file R2 does not hold
    is dropped, and one R2 holds at another size is read from R2."""
    import pathlib
    (tmp_path / "CUTOVER").unlink()
    _tmp, _live, full = world
    listed = [{"Key": "clean_full/eurostat/t.parquet", "Size": os.path.getsize(full / "eurostat" / "t.parquet")},
              {"Key": "clean_full/oecd/t.parquet", "Size": 1}]

    class Pag:
        def paginate(self, Bucket, Prefix):
            return [{"Contents": [o for o in listed if o["Key"].startswith(Prefix)]}]

    class FakeR2:
        bucket = "econ-data"
        client = type("C", (), {"get_paginator": lambda self, op: Pag()})()
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: FakeR2())
    kept, dropped = series_census.keep_served(series_census.source_files())
    assert kept["eurostat"] == [str(full / "eurostat" / "t.parquet")], "same size: read locally"
    assert kept["oecd"] == ["s3://econ-data/clean_full/oecd/t.parquet"], "different size: R2 wins"
    assert dropped == {"bls": 1}, "absent from R2: not downloadable"
    assert pathlib.Path(full / "bls" / "t.parquet").exists()


def test_after_t0_the_r420_gate_compares_with_the_served_object(world, monkeypatch, capsys):
    """A mutant that never read the current object survived R1221: a >20% move must be refused."""
    sb = blob.SelfhostBlob()
    (world[0] / "CUTOVER").unlink()
    sb.put_atomic(series_census.KEY, json.dumps({"observations": 100, "individual_series": 3,
                                                 "served_rule": series_census.SERVED_RULE_AFTER_T0}).encode())
    (world[0] / "CUTOVER").write_text("")
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    assert series_census.main() == 1
    assert "REFUSING to publish: observations moves 100 -> 4" in capsys.readouterr().out
    assert json.loads(sb.get(series_census.KEY))["observations"] == 100, "the served object is unchanged"


def _fresh_verify(monkeypatch):
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=60: io.BytesIO(
        json.dumps({"as_of": __import__("datetime").date.today().isoformat()}).encode()))


def test_a_publish_under_a_new_rule_is_a_decision_whatever_its_size(world, monkeypatch, capsys):
    """R1226: the post-T0 step is ~0.5%, far under R420's 20% - so the RULE, not the size, is gated. A live
    object counted without the post-T0 rule refuses the publish until --force-publish."""
    sb = blob.SelfhostBlob()
    (world[0] / "CUTOVER").unlink()
    sb.put_atomic(series_census.KEY, json.dumps({"observations": 4, "individual_series": 3}).encode())
    (world[0] / "CUTOVER").write_text("")
    _fresh_verify(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    assert series_census.main() == 1
    assert "counted under served_rule None" in capsys.readouterr().out
    assert "served_rule" not in json.loads(sb.get(series_census.KEY)), "unchanged"
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish", "--force-publish"])
    assert series_census.main() == 0
    published = json.loads(sb.get(series_census.KEY))
    assert published["served_rule"] == series_census.SERVED_RULE_AFTER_T0
    # R1226 mutants: the method sentence and the history file's CSV counts
    assert "a source with any served CSV counts whole" in published["method"]
    hist = json.loads(open(os.path.join(series_census.ROOT, "logs",
                                        f"stats-{__import__('datetime').date.today().isoformat()}.json"),
                           encoding="utf-8").read())
    assert hist["per_source_served_csvs"] == {"eurostat": 1, "oecd": 1}


def test_the_lock_is_waited_for_before_the_object_is_read_or_written(world, monkeypatch):
    """R1226: a wait moved after put_atomic survived. The wait comes first: before the gate reads the live
    object and before anything is written."""
    order = []
    monkeypatch.setattr(series_census, "_own_the_store_waiting", lambda *a, **k: order.append("wait"))
    real_get, real_put = blob.SelfhostBlob.get, blob.SelfhostBlob.put_atomic
    monkeypatch.setattr(blob.SelfhostBlob, "get", lambda self, k: order.append("get") or real_get(self, k))
    monkeypatch.setattr(blob.SelfhostBlob, "put_atomic",
                        lambda self, k, d, plain=False: order.append("put") or real_put(self, k, d, plain=plain))
    _fresh_verify(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish", "--force-publish"])
    assert series_census.main() == 0
    assert order[0] == "wait" and order.index("wait") < order.index("put")


def test_the_publish_waits_for_the_lock_then_takes_it(monkeypatch):
    calls = []

    def refuse_twice(what):
        calls.append(what)
        if len(calls) < 3:
            raise cutover.CutoverRefused("refused: another process holds the catalogue writer lock X")
    monkeypatch.setattr(blob, "_refuse_or_own_store_write", refuse_twice)
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: None)
    series_census._own_the_store_waiting(max_wait_s=60, step_s=0)
    assert len(calls) == 3


def test_any_other_refusal_is_not_waited_for(monkeypatch):
    """R1225: a mutant that waited on EVERY refusal passed - the test allowed 60 s of retries. One call."""
    calls = []

    def refuse(what):
        calls.append(what)
        raise cutover.CutoverRefused("refused: the code's own checkout is elsewhere")
    monkeypatch.setattr(blob, "_refuse_or_own_store_write", refuse)
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: pytest.fail("not a lock wait: no sleeping"))
    with pytest.raises(cutover.CutoverRefused, match="own checkout"):
        series_census._own_the_store_waiting(max_wait_s=60, step_s=0)
    assert len(calls) == 1


def test_the_lock_wait_is_bounded(monkeypatch):
    """R1225: a mutant with no time limit passed. A lock that never frees ends in the refusal, not a hang."""
    calls = []

    def always_held(what):
        calls.append(what)
        if len(calls) > 50:
            pytest.fail("the wait did not end")
        raise cutover.CutoverRefused("refused: another process holds the catalogue writer lock X")
    monkeypatch.setattr(blob, "_refuse_or_own_store_write", always_held)
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: None)
    with pytest.raises(cutover.CutoverRefused, match="another process holds"):
        series_census._own_the_store_waiting(max_wait_s=0.0, step_s=0)
    assert 1 <= len(calls) <= 2


def test_after_t0_the_publish_takes_the_lock_through_the_wait(world, monkeypatch):
    """R1225: a main() that never called the wait passed every test."""
    waited = []
    monkeypatch.setattr(series_census, "_own_the_store_waiting", lambda *a, **k: waited.append(1))
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=60: io.BytesIO(
        json.dumps({"as_of": __import__("datetime").date.today().isoformat()}).encode()))
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    assert series_census.main() == 0
    assert waited == [1]


def test_a_missing_publish_store_fails_before_anything_is_read(world, monkeypatch, tmp_path):
    """R1221 finding 5: the store was opened only after the multi-hour count. R1225: the first version of this
    test passed with the store opened late, because keep_served opened it first - so assert that nothing at
    all is read before the refusal."""
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "no_store_here"))
    monkeypatch.setattr(sys, "argv", ["series_census.py", "--publish"])
    seen = []
    monkeypatch.setattr(series_census, "keep_served", lambda srcs: seen.append("keep_served") or ({}, {}))
    monkeypatch.setattr(series_census, "source_files", lambda: seen.append("source_files") or {})
    with pytest.raises(Exception, match="no_store_here|store"):
        series_census.main()
    assert seen == []


def test_a_source_s_csv_prefix_is_terminated(world, monkeypatch):
    """R1225: counting CSVs under 'series/' + src without the terminator let `ilo` borrow `ilostat`'s CSVs."""
    _tmp, _live, full = world
    import pyarrow as pa
    import pyarrow.parquet as pq
    for src in ("ilo", "ilostat"):
        (full / src).mkdir()
        pq.write_table(pa.table({"series_key": ["k"]}), full / src / "t.parquet")
    (world[0] / "CUTOVER").unlink()
    blob.SelfhostBlob().put_atomic("series/ilostat%3Ak.csv", b"series_id,obs_date,value\n")
    (world[0] / "CUTOVER").write_text("")
    monkeypatch.setattr(series_census, "resolvable_sources",
                        lambda: {"eurostat", "oecd", "bls", "ilo", "ilostat", "bea", "worldbank"})
    kept, dropped = series_census.keep_served(series_census.source_files())
    assert "ilostat" in kept and "ilo" not in kept and dropped.get("ilo") == 1


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
