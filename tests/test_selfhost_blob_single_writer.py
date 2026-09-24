"""updater/blob.py SelfhostBlob has the catalogue's single-writer rule (review R1203 finding 2): after T0 a write
to the served store is refused from any checkout but the live one (or with a data root elsewhere), and it holds
core.catalog_path's writer lock - taken for the process on the first write, refused at once when another
process holds it. Before T0 nothing changes."""
import os
import subprocess
import sys
import threading

import pytest

from core import catalog_path, cutover
from updater import blob, config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from blobstore import BlobStore  # noqa: E402

CSV = b"series_id,obs_date,value\nx:1,2020-01-01,1.5\n"
KEY = "series/x%3A1.csv"


@pytest.fixture
def world(tmp_path, monkeypatch):
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "state" / "writer.lock"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "no_build" / "catalog.db"))  # no hot journal
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    # the "live checkout" is THIS checkout, with its own data root: the positive case
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", ROOT)
    monkeypatch.setattr(config, "DATA_ROOT", os.path.join(ROOT, "data", "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    assert catalog_path._held is None
    yield tmp_path, blob.SelfhostBlob(root=str(tmp_path / "blobs"))
    if blob._process_session is not None:                      # let go of a lock a write took
        blob._process_session.__exit__(None, None, None)
        blob._process_session = None         # not monkeypatch: its undo would put the session back
    assert catalog_path._held is None


def _cut_over(tmp_path):
    (tmp_path / "CUTOVER").write_text("")
    assert cutover.is_cut_over()


def test_before_t0_a_write_takes_no_lock(world):
    tmp_path, sb = world
    sb.put_atomic(KEY, CSV)
    assert catalog_path._held is None and blob._process_session is None
    assert sb.exists(KEY)


def test_after_t0_the_live_checkout_writes_and_holds_the_lock(world):
    tmp_path, sb = world
    _cut_over(tmp_path)
    sb.put_atomic(KEY, CSV)
    assert catalog_path._held is not None, "the first write took the writer lock for the process"
    assert sb.exists(KEY)
    sb.put_file("_aqueduct/x.json", __file__)                  # later writes reuse it
    sb.delete("_aqueduct/x.json")


def test_after_t0_a_write_from_another_checkout_is_refused(world, monkeypatch):
    tmp_path, sb = world
    _cut_over(tmp_path)
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(tmp_path / "the_real_checkout"))
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path / "the_real_checkout" / "data" / "clean_full"))
    for write in (lambda: sb.put_atomic(KEY, CSV), lambda: sb.put_file(KEY, __file__),
                  lambda: sb.put_gzip_file(KEY, __file__), lambda: sb.delete(KEY)):
        with pytest.raises(cutover.CutoverRefused, match="the code's own checkout.*R1203"):
            write()
    assert not sb.exists(KEY) and catalog_path._held is None


@pytest.mark.parametrize("override", ["config.DATA_ROOT", "ECONDL_DATA", "config.ROOT", "ECONDL_CATALOG"])
def test_after_t0_a_root_elsewhere_is_refused(world, monkeypatch, override):
    """R1217 finding 5: ECONDL_ROOT is where derive_csv_bulk reads its parquets, so it is checked too."""
    tmp_path, sb = world
    _cut_over(tmp_path)
    elsewhere = str(tmp_path / "worktree" / "data" / "clean_full")
    if override.startswith("ECONDL_"):
        monkeypatch.setenv(override, elsewhere)
    else:
        monkeypatch.setattr(config, override.split(".")[1], elsewhere)
    with pytest.raises(cutover.CutoverRefused, match=override.replace(".", r"\.")):
        sb.put_atomic(KEY, CSV)
    assert not sb.exists(KEY) and catalog_path._held is None


def test_a_copy_goes_through_the_rule(world, monkeypatch):
    """R1217 finding 4: Targets.archive wrote through sb.store.put and skipped the rule; it uses copy() now."""
    tmp_path, sb = world
    sb.put_atomic(KEY, CSV)                                    # before T0
    _cut_over(tmp_path)
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "worktree"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        sb.copy(KEY, "archive/x.csv")
    assert not sb.exists("archive/x.csv")
    monkeypatch.setattr(blob, "_code_root", lambda: ROOT)
    sb.copy(KEY, "archive/x.csv")
    assert sb.get("archive/x.csv") == sb.get(KEY)
    assert sb.store.head("archive/x.csv")["custom_metadata"] == sb.store.head(KEY)["custom_metadata"]


def test_no_code_writes_the_blob_store_behind_the_rule():
    """Every write to the served store goes through SelfhostBlob's methods; `<x>.store.put/delete` is the
    bypass R1217 finding 4 found. BlobStore's own users (tools/selfhost) are checked by name below."""
    import re
    bypass = re.compile(r"\.store\.(put|delete)\(")
    found = []
    for base in ("core", "updater", "tools", "jobs", "scripts", "clients"):
        for dirpath, _dirs, files in os.walk(os.path.join(ROOT, base)):
            for f in files:
                if f.endswith(".py"):
                    p = os.path.join(dirpath, f)
                    rel = os.path.relpath(p, ROOT).replace(os.sep, "/")
                    with open(p, encoding="utf-8-sig") as fh:
                        for i, line in enumerate(fh, 1):
                            if bypass.search(line) and rel != "updater/blob.py":
                                found.append(f"{rel}:{i}")
    assert found == []
    users = set()
    for dirpath, _dirs, files in os.walk(ROOT):
        if any(s in dirpath for s in (".git", "node_modules", "__pycache__", os.sep + "tests")):
            continue
        for f in files:
            if f.endswith(".py"):
                p = os.path.join(dirpath, f)
                with open(p, encoding="utf-8-sig", errors="replace") as fh:
                    if "BlobStore(" in fh.read():
                        users.add(os.path.relpath(p, ROOT).replace(os.sep, "/"))
    # blob_sidecar serves GET/HEAD only (R1217 review); import_from_r2 applies the rule in copy_one
    assert users == {"updater/blob.py", "tools/selfhost/blob_sidecar.py",
                     "tools/selfhost/import_from_r2.py"}, "a new direct BlobStore writer needs the rule"


def test_after_t0_import_from_r2_keeps_the_newer_store_and_obeys_the_lock(world, monkeypatch):
    """R1217 finding 3: import_from_r2 wrote frozen R2 bytes over the served object beside a lock holder."""
    import hashlib
    import import_from_r2
    tmp_path, sb = world
    sb.put_atomic(KEY, CSV)
    before = sb.get(KEY)
    frozen = b"FROZEN R2 BYTES"

    class S3:
        def get_object(self, **kw):
            import io
            return {"Body": io.BytesIO(frozen), "ETag": '"%s"' % hashlib.md5(frozen).hexdigest()}  # noqa: S324

    _cut_over(tmp_path)
    ok, msg = import_from_r2.copy_one(S3(), sb.store, KEY)
    assert ok and "kept" in msg and sb.get(KEY) == before, "the store's newer object stays"
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "worktree"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        import_from_r2.copy_one(S3(), sb.store, "series/new%3A1.csv")
    monkeypatch.setattr(blob, "_code_root", lambda: ROOT)
    ok, _msg = import_from_r2.copy_one(S3(), sb.store, KEY, overwrite=True)   # a deliberate repair
    assert ok and sb.get(KEY) == frozen and catalog_path._held is not None


_HOLDER = """
import sys, time
sys.path.insert(0, sys.argv[1])
from core import catalog_path
catalog_path.LOCK_PATH = sys.argv[2]
fh = open(catalog_path.LOCK_PATH, "a+b")
catalog_path._lock(fh)
print("held", flush=True)
sys.stdin.readline()
"""


def test_after_t0_a_write_is_refused_while_another_process_holds_the_lock(world):
    tmp_path, sb = world
    _cut_over(tmp_path)
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    p = subprocess.Popen([sys.executable, "-c", _HOLDER, ROOT, catalog_path.LOCK_PATH],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        with pytest.raises(cutover.CutoverRefused, match="another process holds"):
            sb.put_atomic(KEY, CSV)
        assert not sb.exists(KEY) and catalog_path._held is None
    finally:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=30)


def test_after_t0_many_threads_take_the_lock_once(world):
    tmp_path, sb = world
    _cut_over(tmp_path)
    errors = []

    def put(i):
        try:
            sb.put_atomic(f"series/x%3A{i}.csv", CSV.replace(b"1.5", str(i).encode()))
        except BaseException as e:           # noqa: BLE001 - the test reports every failure
            errors.append(e)

    threads = [threading.Thread(target=put, args=(i,)) for i in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(sb.list_keys("series/")) == 16


def test_the_updater_already_holding_the_lock_is_enough(world):
    tmp_path, sb = world
    _cut_over(tmp_path)
    with catalog_path.writer_lock():
        sb.put_atomic(KEY, CSV)
        assert blob._process_session is None, "no second acquisition inside the updater's own"
    assert sb.exists(KEY)
