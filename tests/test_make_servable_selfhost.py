"""tools/make_servable.py after T0 (plan step 1: a self-hosted path; before, it forced the R2 backend and failed
closed). The local store IS the served store, so there is no sync; the catalogue, the derive and the verify run in
ONE process under the single-writer lock (the cataloguer in-process - a child would be refused the lock); the
verify lists the self-hosted store with the same freshness rule; the NEXT line names the swap. No R2, no D1;
refused outside the live checkout. Before T0 unchanged (R2)."""
import datetime as dt
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from tools import make_servable as M  # noqa: E402
from updater import derive  # noqa: E402
from blobstore import BlobStore  # noqa: E402


def _csv(sid):
    return f"series_id,obs_date,value\n{sid},2024-01-01,1\n".encode()


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    full = root / "data" / "clean_full"
    (full / "zz").mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["a", "b", "c"], "obs_date": [dt.date(2024, 1, 1)] * 3,
                             "value": [1.0, 2.0, 3.0]}), full / "zz" / "zz.parquet")
    build = root / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, "
                  "unit TEXT, geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, "
                  "last_updated TEXT, metadata TEXT)")
        c.execute("CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography)")
        c.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, license_id TEXT)")
        c.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, commercial_ok INTEGER, "
                  "no_modify INTEGER, attribution_required INTEGER)")
        c.execute("INSERT INTO license VALUES ('us-public-domain', 'US public domain', 1, 0, 0)")
        c.execute("INSERT INTO series (series_id, source_id, title, license_id) VALUES "
                  "('zz:a', 'zz', 'a', 'us-public-domain')")
    c.close()
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(full))
    monkeypatch.setattr(updater_config, "source_dir", lambda s: str(full / s))
    monkeypatch.setattr(M, "ROOT", str(root))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    # main() sets these for its process (chdir, the backend, the worker count): recorded here so they are put
    # back after each test - left behind, one test moved 41 others onto the R2 backend
    monkeypatch.chdir(str(tmp_path))
    monkeypatch.setenv("AQUEDUCT_BACKEND", "r2")
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "2")
    held = []
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: held.append(catalog_path._held is not None)
                        or _csv(sid))
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 reached after T0"))
    monkeypatch.setattr(blob, "R2Blob", lambda *a, **k: pytest.fail("R2 used after T0"))
    (tmp_path / "CUTOVER").write_text("")
    return root, build, full, held


def _catalogued(build):
    c = sqlite3.connect(f"file:{build}?mode=ro", uri=True)
    try:
        return sorted(r[0] for r in c.execute("SELECT series_id FROM series WHERE source_id='zz'"))
    finally:
        c.close()


def test_after_t0_catalogue_derive_and_verify_run_locally_under_the_lock(live, capsys):
    root, build, full, held = live
    M.main(["zz"])
    out = capsys.readouterr().out
    assert _catalogued(build) == ["zz:a", "zz:b", "zz:c"], "the cataloguer ran in-process, under the lock"
    store = blob.SelfhostBlob()
    assert sorted(store.list_keys("series/")) == ["series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zz%3Ac.csv"]
    assert held and all(held), "every derive ran under the writer lock"
    assert catalog_path._held is None, "and the lock is let go"
    assert "no sync" in out and "MISSING 0  ORPHANED 0  OK" in out, out
    assert "tools/selfhost/swap.py" in out.strip().splitlines()[-1], "the closing NEXT line names the swap"
    assert not [ln for ln in out.splitlines() if ln.strip().startswith("NEXT: python core/sync_catalog_d1.py")], \
        "no bare pre-T0 step is printed after T0"
    assert os.environ["AQUEDUCT_BACKEND"] == "selfhost"


def test_a_csv_older_than_the_parquet_is_derived_again_and_a_sibling_is_not_an_orphan(live, capsys):
    """The freshness rule and the colon-anchored prefix, on the self-hosted store."""
    root, build, full, held = live
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        store.put_atomic("series/zz%3Aa.csv", b"old bytes")
        store.put_atomic("series/zzz%3Asibling.csv", _csv("zzz:sibling"))
    old = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc).isoformat()
    s = store.store
    s._w.execute("UPDATE blobs SET stored_utc=? WHERE key='series/zz%3Aa.csv'", (old,))
    s._w.commit()
    M.main(["zz"])
    out = capsys.readouterr().out
    assert "to derive 3 (1 of them present but OLDER than the parquet)" in out
    assert store.get("series/zz%3Aa.csv") != b"old bytes"
    assert "ORPHANED 0" in out, "zzz:sibling is another source's"


def test_after_t0_another_checkout_is_refused_before_anything(live, monkeypatch, tmp_path):
    root, build, full, held = live
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        M.main(["zz"])
    assert _catalogued(build) == ["zz:a"] and not held


def test_after_t0_a_source_with_no_parquet_serves_nothing(live, capsys):
    M.main(["nothing_here"])
    assert "NO parquet in the store" in capsys.readouterr().out


def test_importing_the_tool_changes_nothing_in_this_process():
    """The import (at the top of this file) left the working directory and the backend alone."""
    import subprocess
    code = ("import os, sys; sys.path.insert(0, %r); cwd = os.getcwd(); os.environ.pop('AQUEDUCT_BACKEND', None); "
            "from tools import make_servable; "
            "print(os.getcwd() == cwd, 'AQUEDUCT_BACKEND' in os.environ, 'AQUEDUCT_DERIVE_WORKERS' in os.environ)"
            % ROOT)
    r = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True, timeout=120,
                       cwd=os.path.dirname(ROOT))
    assert r.returncode == 0, r.stderr[-600:]
    assert r.stdout.split()[-3:] == ["True", "False", "False"], r.stdout


def _set_stored(key, when):
    s = blob.SelfhostBlob().store
    s._w.execute("UPDATE blobs SET stored_utc=? WHERE key=?", (when.isoformat(timespec="seconds"), key))
    s._w.commit()


def test_a_rewritten_identical_parquet_verifies_ok(live, capsys):
    """R1241: the second run re-derives (the parquet is newer), every write is skipped as identical, and the
    skipped objects keep their old stored_utc - VERIFY said MISSING 3. What this run derived is current."""
    root, build, full, held = live
    assert M.main(["zz"]) == 0
    capsys.readouterr()
    p = full / "zz" / "zz.parquet"
    later = dt.datetime.now(dt.timezone.utc).timestamp() + 5
    os.utime(p, (later, later))                                   # rewritten, same rows
    assert M.main(["zz"]) == 0
    out = capsys.readouterr().out
    assert "already current, not re-written: 3" in out and "MISSING 0  ORPHANED 0  OK" in out, out


def test_a_csv_older_than_the_parquet_by_less_than_a_second_is_stale(live, capsys):
    """R1241: the parquet time was floored to the second, so a CSV stored 0.5 s before it counted as current."""
    root, build, full, held = live
    assert M.main(["zz"]) == 0
    capsys.readouterr()
    t = dt.datetime(2030, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    os.utime(full / "zz" / "zz.parquet", (t.timestamp() + 0.5, t.timestamp() + 0.5))
    for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zz%3Ac.csv"):
        _set_stored(k, t)                                         # the same whole second, before the write
    M.main(["zz"])
    assert "to derive 3 (3 of them present but OLDER than the parquet)" in capsys.readouterr().out


def test_the_newest_of_several_parquets_decides(live, capsys):
    """R1241 mutant S8 (the oldest file's time) survived with a one-file fixture."""
    root, build, full, held = live
    old = full / "zz" / "part_old.parquet"
    pq.write_table(pa.table({"series_key": ["a"], "obs_date": [dt.date(2020, 1, 1)], "value": [0.0]}), old)
    assert M.main(["zz"]) == 0
    capsys.readouterr()
    t = dt.datetime(2030, 1, 1, tzinfo=dt.timezone.utc)
    os.utime(old, (t.timestamp() - 86400, t.timestamp() - 86400))
    os.utime(full / "zz" / "zz.parquet", (t.timestamp() + 60, t.timestamp() + 60))
    for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zz%3Ac.csv"):
        _set_stored(k, t)                          # after the OLD file, before the NEW one: stale
    M.main(["zz"])
    assert "to derive 3 (3 of them present but OLDER than the parquet)" in capsys.readouterr().out


def test_one_bad_source_does_not_stop_the_batch(live, capsys):
    """R1241: the cataloguer runs in-process now; a corrupt parquet in the first source raised and the second
    was never served. It is reported, the rest are served, and the run exits 1."""
    root, build, full, held = live
    (full / "broken").mkdir()
    (full / "broken" / "broken.parquet").write_bytes(b"not a parquet file")
    with catalog_path.writer_lock():                     # a licence, so the cataloguer goes on to READ the file
        c = sqlite3.connect(str(build))
        c.execute("INSERT INTO source VALUES ('broken', 'us-public-domain')")
        c.commit()
        c.close()
    assert M.main(["broken", "zz"]) == 1
    out = capsys.readouterr().out
    assert "FAIL broken:" in out and "NOT SERVED CLEANLY: ['broken']" in out
    assert _catalogued(build) == ["zz:a", "zz:b", "zz:c"], "zz was still served"
    assert catalog_path._held is None


def test_before_t0_it_still_goes_to_r2(live, monkeypatch, tmp_path):
    root, build, full, held = live
    (tmp_path / "CUTOVER").unlink()

    class Reached(Exception):
        pass
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: (_ for _ in ()).throw(Reached()))
    with pytest.raises(Reached):
        M.main(["zz"])
