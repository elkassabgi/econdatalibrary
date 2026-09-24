"""tools/guard_heartbeat.py after T0 (plan: MOVE + A NEW READER, review R1210): the beat is published to the
self-hosted store - a status key, so the live checkout is required but not the updater's writer lock - and an
off-machine check reads it through the public /v1/guard-heartbeat route, which serves only a timestamp and
counts. Before T0 publish and check are as they were (R2)."""
import datetime as dt
import io
import json
import os
import subprocess
import sys

import pytest

from core import catalog_path, cutover
from updater import blob
from updater import config as updater_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
import guard_heartbeat as gh  # noqa: E402
from blobstore import BlobStore  # noqa: E402


@pytest.fixture
def t0(tmp_path, monkeypatch):
    BlobStore(str(tmp_path / "blobs"), create=True)
    monkeypatch.setattr(blob, "SELFHOST_BLOB_ROOT", str(tmp_path / "blobs"))
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))
    monkeypatch.setattr(catalog_path, "LIVE_STORE_ROOT", ROOT)
    monkeypatch.setattr(catalog_path, "LOCK_PATH", str(tmp_path / "lock" / "writer.lock"))
    monkeypatch.setattr(catalog_path, "BUILD_PATH", str(tmp_path / "no_build" / "catalog.db"))
    monkeypatch.setattr(updater_config, "ROOT", ROOT)
    monkeypatch.setattr(updater_config, "DATA_ROOT", os.path.join(ROOT, "data", "clean_full"))
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    monkeypatch.delenv("ECONDL_CATALOG", raising=False)
    monkeypatch.setattr(gh, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(gh, "_emptiness_verdict", lambda: {"ran": True, "fetch_without_write": 0, "unreadable": 0})
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: pytest.fail("R2 used after T0"))
    (tmp_path / "CUTOVER").write_text("")
    yield tmp_path
    if blob._process_session is not None:
        blob._process_session.__exit__(None, None, None)
        blob._process_session = None


_HOLDER = """
import sys
sys.path.insert(0, sys.argv[1])
from core import catalog_path
fh = open(sys.argv[2], "a+b")
catalog_path._lock(fh)
print("held", flush=True)
sys.stdin.readline()
"""


def test_after_t0_the_beat_lands_in_the_self_hosted_store_while_the_updater_holds_the_lock(t0):
    """The updater holds the writer lock for hours; a beat refused for that long would read as a dead
    watchdog. The beat key is exempt from the lock - and takes none."""
    os.makedirs(os.path.dirname(catalog_path.LOCK_PATH), exist_ok=True)
    p = subprocess.Popen([sys.executable, "-c", _HOLDER, ROOT, catalog_path.LOCK_PATH],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert p.stdout.readline().strip() == "held"
        assert gh.publish() == 0
    finally:
        p.stdin.write("\n")
        p.stdin.flush()
        p.wait(timeout=30)
    body = json.loads(blob.SelfhostBlob().get(gh.KEY))
    assert "utc" in body and catalog_path._held is None, "published, and no lock was taken"


def test_after_t0_a_beat_from_another_checkout_is_refused(t0, monkeypatch):
    monkeypatch.setattr(blob, "_code_root", lambda: str(t0 / "a_worktree"))
    with pytest.raises(cutover.CutoverRefused, match="R1203"):
        gh.publish()
    assert blob.SelfhostBlob().get(gh.KEY) is None


def test_the_lock_exemption_is_this_key_only(t0):
    """Any other _aqueduct key still needs the lock (the exemption is not a prefix)."""
    sb = blob.SelfhostBlob()
    sb.put_atomic("_aqueduct/stats.json", b"{}")
    assert catalog_path._held is not None


def _serve(monkeypatch, body=None, error=None):
    def fake_urlopen(req, timeout=60):
        if error:
            raise error
        return io.BytesIO(json.dumps(body).encode())
    import urllib.request
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)


def _now(minutes_ago=0.0):
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)).isoformat()


def test_the_off_machine_check_passes_a_fresh_beat(monkeypatch, capsys):
    _serve(monkeypatch, {"utc": _now(3), "table_ok": True, "jobs_alive": 2, "jobs_tracked": 3,
                         "emptiness_ran": True, "fetch_without_write": 0})
    assert gh.check_url("https://edge.example/v1/guard-heartbeat", 45) == 0
    assert "jobs_alive=2/3" in capsys.readouterr().out


@pytest.mark.parametrize("case", ["stale", "unreachable", "unreadable", "emptiness"])
def test_the_off_machine_check_fails_what_is_not_a_fresh_clean_beat(monkeypatch, case):
    import urllib.error
    body = {"utc": _now(3), "table_ok": True, "jobs_alive": 3, "jobs_tracked": 3, "emptiness_ran": True,
            "fetch_without_write": 0}
    if case == "stale":
        body["utc"] = _now(90)
    elif case == "unreadable":
        body = {"error": "heartbeat_absent"}
    elif case == "emptiness":
        body["fetch_without_write"] = 2
    _serve(monkeypatch, body, urllib.error.URLError("503") if case == "unreachable" else None)
    assert gh.check_url("https://edge.example/v1/guard-heartbeat", 45) == 1
