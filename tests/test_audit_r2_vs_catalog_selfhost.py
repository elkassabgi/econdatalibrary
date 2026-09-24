"""tools/audit_r2_vs_catalog.py after T0 (plan step 6d): objects are counted in the SELF-HOSTED store (R2 is a frozen
copy and is not asked), colon-anchored per source, from the live checkout only."""
import os
import sqlite3
import sys

import pytest

from core import catalog_path, cutover, r2_util
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
from tools import audit_r2_vs_catalog as A  # noqa: E402
from blobstore import BlobStore  # noqa: E402


@pytest.fixture
def live(tmp_path, monkeypatch):
    root = tmp_path / "live"
    (root / "data").mkdir(parents=True)
    build = root / "data" / "catalog.db"
    with sqlite3.connect(build) as c:
        c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        c.executemany("INSERT INTO series VALUES (?, ?)", [("zz:a", "zz"), ("zz:b", "zz"), ("zzz:x", "zzz")])
    c.close()
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", str(root))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(build))
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(blob, "_code_root", lambda: str(root))
    monkeypatch.setattr(A, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "ROOT", str(root))
    monkeypatch.setattr(updater_config, "DATA_ROOT", str(root / "data" / "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: pytest.fail("R2 asked after T0"))
    store = blob.SelfhostBlob()
    with catalog_path.writer_lock():
        for k in ("series/zz%3Aa.csv", "series/zz%3Ab.csv", "series/zzz%3Ax.csv", "series/zzz%3Ay.csv"):
            store.put_atomic(k, b"series_id,obs_date,value\n")
    (tmp_path / "CUTOVER").write_text("")
    return tmp_path


def test_after_t0_objects_are_counted_in_the_self_hosted_store(live, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["audit_r2_vs_catalog.py", "zz", "zzz"])
    A.main()
    out = capsys.readouterr().out
    assert "store objects" in out and "R2 objects" not in out
    zz = next(ln for ln in out.splitlines() if ln.strip().startswith("zz ") or ln.strip().split()[:1] == ["zz"])
    zzz = next(ln for ln in out.splitlines() if ln.strip().split()[:1] == ["zzz"])
    assert zz.split()[1:3] == ["2", "2"], zz                          # zzz's objects not counted under zz
    assert zzz.split()[1:4] == ["2", "1", "+1"], zzz
    assert "where the store and the catalogue agree" in out
    # R1243: after T0 there is no D1 to "verify against" - the verdict and the note name the build
    assert "OBJECTS WITH NO CATALOGUE ROW" in zzz and "D1" not in out, out
    assert "THE `catalogue rows` COLUMN IS THE LIVE CATALOGUE BUILD" in out, out


def test_after_t0_another_checkout_is_refused(live, monkeypatch, tmp_path):
    monkeypatch.setattr(blob, "_code_root", lambda: str(tmp_path / "a_worktree"))
    monkeypatch.setattr(sys, "argv", ["audit_r2_vs_catalog.py", "zz"])
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        A.main()


def test_after_t0_a_catalogue_row_with_no_object_names_the_swap_not_d1(live, monkeypatch, capsys):
    """R1249 A9: the post-T0 'rows with no object' verdict had no pin; putting back the D1 wording passed."""
    with catalog_path.writer_lock():
        blob.SelfhostBlob().delete("series/zz%3Ab.csv")
    monkeypatch.setattr(sys, "argv", ["audit_r2_vs_catalog.py", "zz"])
    A.main()
    out = capsys.readouterr().out
    zz = next(ln for ln in out.splitlines() if ln.strip().startswith("zz "))
    assert zz.split()[1:4] == ["1", "2", "-1"], zz
    assert "CATALOGUE ROWS WITH NO OBJECT — once swapped, a user gets 502" in zz and "D1" not in out, out


def _trace_series_reads(monkeypatch):
    """Every SQL statement the tool sends through the catalogue resolver (both roads)."""
    import re as _re
    sql = []
    for name in ("connect", "connect_path"):
        real = getattr(catalog_path, name)

        def traced(*a, _real=real, **k):
            con = _real(*a, **k)
            con.set_trace_callback(sql.append)
            return con
        monkeypatch.setattr(catalog_path, name, traced)

    def whole_table_reads():
        reads = [q for q in sql if "FROM series" in q or "from series" in q]
        return reads, [q for q in reads if "GROUP BY" in q.upper() or _re.search(r"source_id\s*=\s*", q)]
    return whole_table_reads


def test_after_t0_the_live_build_is_never_read_whole(live, monkeypatch, capsys):
    """R1249 G6: after T0 the catalogue counts come from short primary-key chunks - one GROUP BY holds the live
    build's read lock while the writer waits."""
    check = _trace_series_reads(monkeypatch)
    monkeypatch.setattr(sys, "argv", ["audit_r2_vs_catalog.py", "zz", "zzz"])
    A.main()
    reads, bad = check()
    assert reads and not bad, bad
