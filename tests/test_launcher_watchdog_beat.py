"""The launcher watchdog's state rides the guard beat (tools/guard_heartbeat.py) and reaches the daily digest
(updater/send_digest.py). Review AR-481 round 2, finding 3: its GAVE-UP, CANNOT TELL, a hung look and a pinned-hash
mismatch were visible only on the workstation's console. Each state is planted here; the healthy and the
not-installed cases are the negative controls (they must stay silent), every other case must name its problem."""
import datetime as dt
import io
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import guard_heartbeat as GH  # noqa: E402
from updater import send_digest as SD  # noqa: E402

NOW = dt.datetime(2026, 11, 12, 12, 0, tzinfo=dt.timezone.utc)
LOOPS = ("IpGuard", "HfRunner", "HfRunnerDaily", "HfDailyDispatch", "EconSlotDispatch")

# The shape launcher_watchdog.ps1 writes (ConvertTo-Json of its ordered table, UTF-8 without a BOM), as produced by
# its own test suite's status case on 2026-11-12, with the times moved to NOW.
REAL_SHAPE = """{
    "schema":  "launcher-watchdog-status/1",
    "utc":  "2026-11-12T11:58:00Z",
    "pid":  4242,
    "look":  "ok",
    "summary":  "EconOrigin=excluded IpGuard=alive HfRunner=alive HfRunnerDaily=alive HfDailyDispatch=alive EconSlotDispatch=alive",
    "targets":  {
                    "EconOrigin":  {
                                       "state":  "excluded",
                                       "since":  "2026-11-12T09:00:00Z"
                                   },
                    "IpGuard":  {
                                    "state":  "alive",
                                    "since":  "2026-11-12T09:00:00Z"
                                },
                    "HfRunner":  {
                                     "state":  "alive",
                                     "since":  "2026-11-12T09:00:00Z"
                                 },
                    "HfRunnerDaily":  {
                                          "state":  "alive",
                                          "since":  "2026-11-12T09:00:00Z"
                                      },
                    "HfDailyDispatch":  {
                                            "state":  "alive",
                                            "since":  "2026-11-12T09:00:00Z"
                                        },
                    "EconSlotDispatch":  {
                                             "state":  "alive",
                                             "since":  "2026-11-12T09:00:00Z"
                                         }
                }
}"""


def _status(utc="2026-11-12T11:58:00Z", look="ok", states=None):
    states = states or {}
    t = {n: {"state": states.get(n, "alive"), "since": "2026-11-12T09:00:00Z"} for n in LOOPS}
    t["EconOrigin"] = {"state": "excluded", "since": "2026-11-12T09:00:00Z"}
    return {"schema": "launcher-watchdog-status/1", "utc": utc, "pid": 1, "look": look, "summary": "x", "targets": t}


def _plant(tmp_path, monkeypatch, status=None, hook=None, raw_status=None):
    if raw_status is not None:
        (tmp_path / "status.json").write_text(raw_status, encoding="utf-8")
    elif status is not None:
        (tmp_path / "status.json").write_text(json.dumps(status), encoding="utf-8")
    if hook is not None:
        (tmp_path / "hook.status").write_text(hook + "\r\n", encoding="ascii")
    monkeypatch.setattr(GH, "LAUNCHER_WATCHDOG_DIR", str(tmp_path))
    return GH._launcher_watchdog_beat(NOW)


def _digest(lw, utc="2026-11-12T11:59:00+00:00"):
    return SD.launcher_watchdog_lines({"utc": utc, "launcher_watchdog": lw}, NOW)


HOOK_OK = "2026-11-12T11:57:30Z started "


def test_not_installed_is_silent(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch)
    assert lw["installed"] is False and lw["problems"] == []
    assert _digest(lw) == []


def test_healthy_is_silent_and_reads_the_watchdogs_real_shape(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, raw_status=REAL_SHAPE, hook=HOOK_OK)
    assert lw["installed"] is True and lw["problems"] == [], lw["problems"]
    assert lw["status"]["targets"]["IpGuard"]["state"] == "alive"
    assert _digest(lw) == []


def test_gave_up_and_cannot_tell_are_named_with_since(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, status=_status(states={"HfRunner": "GAVE-UP", "IpGuard": "cannot-tell"}),
                hook=HOOK_OK)
    assert "HfRunner GAVE-UP since 2026-11-12T09:00:00Z" in lw["problems"]
    assert "IpGuard cannot-tell since 2026-11-12T09:00:00Z" in lw["problems"]
    got = _digest(lw)
    assert got[0].startswith("  launcher watchdog") and "2 item(s)" in got[0]
    assert any("HfRunner GAVE-UP" in l for l in got) and any("IpGuard cannot-tell" in l for l in got)


def test_every_attention_state_is_named(tmp_path, monkeypatch):
    for state in GH.LW_ATTENTION:
        lw = _plant(tmp_path, monkeypatch, status=_status(states={"HfRunnerDaily": state}), hook=HOOK_OK)
        assert lw["problems"] == [f"HfRunnerDaily {state} since 2026-11-12T09:00:00Z"], (state, lw["problems"])
    for quiet in ("alive", "RELAUNCHED", "rate-limited", "grace", "stop", "skip-file", "not-installed", "no-script"):
        lw = _plant(tmp_path, monkeypatch, status=_status(states={"HfRunnerDaily": quiet}), hook=HOOK_OK)
        assert lw["problems"] == [], (quiet, lw["problems"])


def test_a_hung_look_shows_as_a_stale_status(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, status=_status(utc="2026-11-12T11:20:00Z"), hook=HOOK_OK)
    assert any("last finished look is 40 min old" in p for p in lw["problems"]), lw["problems"]
    lw = _plant(tmp_path, monkeypatch, status=_status(utc="2026-11-12T11:31:00Z"), hook=HOOK_OK)
    assert lw["problems"] == [], "29 min is inside the limit"


def test_the_whole_look_blind_or_stopped_or_errored(tmp_path, monkeypatch):
    for look, word in (("CANNOT-TELL-processes", "CANNOT TELL"), ("STOP", "paused"), ("ERROR", "error")):
        lw = _plant(tmp_path, monkeypatch, status=_status(look=look), hook=HOOK_OK)
        assert any(word in p for p in lw["problems"]), (look, lw["problems"])


def test_hash_mismatch_and_missing_file_from_the_hook(tmp_path, monkeypatch):
    h = "2026-11-12T11:57:30Z sha-mismatch sha256 " + "A" * 64 + ", pinned " + "B" * 64
    lw = _plant(tmp_path, monkeypatch, status=_status(), hook=h)
    assert any("sha-mismatch" in p and "reinstall after review" in p for p in lw["problems"]), lw["problems"]
    lw = _plant(tmp_path, monkeypatch, status=_status(), hook=r"2026-11-12T11:57:30Z missing F:\launcher_watchdog\launcher_watchdog.ps1")
    assert any("did not start the watchdog: missing" in p for p in lw["problems"]), lw["problems"]


def test_a_hook_that_stopped_running(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, status=_status(), hook="2026-11-12T10:00:00Z started ")
    assert any("guard hook last ran 120 min ago" in p for p in lw["problems"]), lw["problems"]


def test_one_file_without_the_other_is_a_problem_not_clean(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, status=_status())
    assert lw["installed"] and any("hook.status is missing" in p for p in lw["problems"])
    (tmp_path / "status.json").unlink()
    lw2 = _plant(tmp_path, monkeypatch, hook=HOOK_OK)
    assert lw2["installed"] and any("never finished a look" in p for p in lw2["problems"])


def test_unreadable_files_are_named(tmp_path, monkeypatch):
    lw = _plant(tmp_path, monkeypatch, raw_status="{not json", hook="garbage")
    assert any("status.json is unreadable" in p for p in lw["problems"])
    assert any("hook.status is unreadable" in p for p in lw["problems"])


def test_an_old_beat_says_so_first():
    lw = {"installed": True, "problems": ["IpGuard GAVE-UP since x"]}
    got = _digest(lw, utc="2026-11-12T06:00:00+00:00")
    assert "6 h old" in got[0] and "IpGuard GAVE-UP" in "".join(got)
    assert _digest(lw, utc=None)[0].startswith("  launcher watchdog: the workstation beat is undated")


def test_older_publisher_and_unreadable_beat_are_silent_here():
    assert SD.launcher_watchdog_lines({"utc": "2026-11-12T11:59:00+00:00"}, NOW) == []
    assert SD.launcher_watchdog_lines({"unreadable": "NoSuchKey"}, NOW) == []


def test_publish_carries_it(monkeypatch, tmp_path):
    put = {}
    (tmp_path / "status.json").write_text(json.dumps(_status(states={"IpGuard": "GAVE-UP"})), encoding="utf-8")
    (tmp_path / "hook.status").write_text(HOOK_OK, encoding="ascii")
    monkeypatch.setattr(GH, "LAUNCHER_WATCHDOG_DIR", str(tmp_path))
    monkeypatch.setattr(GH, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(GH, "_emptiness_verdict", lambda: {})
    monkeypatch.setattr(GH, "_lane_beat", lambda: None)
    monkeypatch.setattr(GH, "_sec_edgar_beat", lambda: None)
    monkeypatch.setattr(GH, "_notices_beat", lambda: {"run": None, "new": 0, "broken": 0, "items": []})
    # the beat is judged at publish time, against the real clock: make the planted files fresh for it
    real_now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    (tmp_path / "status.json").write_text(json.dumps(_status(utc=real_now, states={"IpGuard": "GAVE-UP"})), encoding="utf-8")
    (tmp_path / "hook.status").write_text(real_now + " started ", encoding="ascii")

    class C:
        def put_object(self, **kw):
            put.update(kw)
    monkeypatch.setattr(GH.r2_util, "client", lambda write=False: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH.publish() == 0
    lw = json.loads(put["Body"])["launcher_watchdog"]
    assert lw["installed"] and lw["problems"] == ["IpGuard GAVE-UP since 2026-11-12T09:00:00Z"], lw["problems"]


def test_check_prints_it_and_does_not_fail(monkeypatch, capsys):
    body = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "host": "h", "jobs_alive": [], "tracked": [],
            "table_ok": True, "launcher_watchdog": {"installed": True, "problems": ["IpGuard GAVE-UP since x"]}}

    class C:
        def get_object(self, **kw):
            return {"Body": io.BytesIO(json.dumps(body).encode("utf-8"))}
    monkeypatch.setattr(GH.r2_util, "client", lambda *a, **k: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH.check(45.0) == 0
    assert "NOTE: LAUNCHER WATCHDOG: IpGuard GAVE-UP since x" in capsys.readouterr().out
