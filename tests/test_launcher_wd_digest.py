"""The desktop launcher watchdog: the guard beat carries its summary (tools/guard_heartbeat.py `_launcher_wd_beat`)
and the daily digest prints it (updater/send_digest.py `launcher_wd_lines`). Every state is planted in a tmp folder,
never F:\\launcher_watchdog, and a watchdog that cannot be read must never read as a quiet one."""
import datetime as dt
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import guard_heartbeat as GH  # noqa: E402
from updater import send_digest as SD  # noqa: E402

NOW = dt.datetime(2026, 11, 12, 12, 0, tzinfo=dt.timezone.utc)
BEAT_UTC = "2026-11-12T11:58:00+00:00"
SUMMARY = "IpGuard=alive HfRunner=GAVE-UP HfRunnerDaily=alive HfDailyDispatch=cannot-tell EconOrigin=excluded"


def _stamp(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _plant(d, hb_lines, log_lines=None, rolled=None, encoding="ascii"):
    d.mkdir(parents=True, exist_ok=True)
    (d / "watchdog.heartbeat").write_bytes(("\r\n".join(hb_lines) + "\r\n").encode(encoding))
    if log_lines is not None:
        (d / "watchdog.log").write_bytes(("\r\n".join(log_lines) + "\r\n").encode(encoding))
    if rolled is not None:
        (d / "watchdog.log.1").write_bytes(("\r\n".join(rolled) + "\r\n").encode(encoding))


def _log(t, text, pid=56292):
    return f"{_stamp(t)} [{pid}] {text}"


def test_missing_heartbeat_file_is_none(tmp_path):
    assert GH._launcher_wd_beat(str(tmp_path / "nothing"), now=NOW) is None
    (tmp_path / "only_log").mkdir()
    (tmp_path / "only_log" / "watchdog.log").write_text(_log(NOW, "GAVE UP X: y") + "\n")
    assert GH._launcher_wd_beat(str(tmp_path / "only_log"), now=NOW) is None, "only the heartbeat file means present"


def test_alerts_are_last_24h_newest_ten_and_truncated(tmp_path):
    old = NOW - dt.timedelta(hours=25)
    lines = [_log(old, "GAVE UP Ancient: long ago"), _log(NOW - dt.timedelta(hours=1), "IpGuard alive")]
    lines += [_log(NOW - dt.timedelta(minutes=60 - i), f"GAVE UP HfRunner: try {i}") for i in range(12)]
    lines += [_log(NOW - dt.timedelta(minutes=5), "CANNOT TELL: the process query failed (" + "x" * 400 + ")")]
    _plant(tmp_path, [_stamp(NOW - dt.timedelta(minutes=3)), SUMMARY], lines)
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert b["summary"] == SUMMARY and b["hb_utc"] == _stamp(NOW - dt.timedelta(minutes=3))
    assert b["alerts_total"] == 13 and len(b["alerts"]) == 10
    assert not any("Ancient" in a for a in b["alerts"]) and not any("IpGuard alive" in a for a in b["alerts"])
    assert b["alerts"][-1].startswith(_stamp(NOW - dt.timedelta(minutes=5))) and len(b["alerts"][-1]) == 200
    assert "try 3" in b["alerts"][0], "the NEWEST ten are kept"
    json.dumps(b)


def test_a_just_rolled_log_still_shows_alerts_from_the_rolled_file(tmp_path):
    _plant(tmp_path, [_stamp(NOW), SUMMARY], [_log(NOW, "IpGuard alive")],
           rolled=[_log(NOW - dt.timedelta(hours=2), "GAVE UP HfRunner: three restarts failed")])
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert b["alerts_total"] == 1 and "three restarts" in b["alerts"][0]


def test_a_utf16_log_is_not_a_silent_zero(tmp_path):
    _plant(tmp_path, [_stamp(NOW), SUMMARY], [_log(NOW, "GAVE UP HfRunner: x")], encoding="utf-16")
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert b["summary"] == SUMMARY and b["alerts_total"] == 1


def test_a_tail_read_drops_the_partial_line_and_says_the_window_is_cut(tmp_path, monkeypatch):
    lines = [_log(NOW - dt.timedelta(minutes=50 - i), f"GAVE UP J{i}: z") for i in range(40)]
    _plant(tmp_path, [_stamp(NOW), SUMMARY], lines)
    monkeypatch.setattr(GH, "LAUNCHER_WD_TAIL_BYTES", 300)
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert b["window_cut"] is True and b["log_parsed"] >= 1
    assert all(re.match(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ \[", a) for a in b["alerts"]), "no partial first line"


def test_a_locked_log_keeps_the_summary_and_says_alerts_unknown(tmp_path, monkeypatch):
    _plant(tmp_path, [_stamp(NOW), SUMMARY], [])
    monkeypatch.setattr(GH, "_wd_tail", lambda *a, **k: (_ for _ in ()).throw(PermissionError("locked")))
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert b["summary"] == SUMMARY and "PermissionError" in b["log_unreadable"]
    got = SD.launcher_wd_lines({"utc": BEAT_UTC, "launcher_watchdog": b}, NOW)
    assert SUMMARY in got[0] and any("log is unreadable" in l and "alerts UNKNOWN" in l for l in got)
    pub = "\n".join(SD.launcher_wd_lines({"utc": BEAT_UTC, "launcher_watchdog": b}, NOW, public=True))
    assert "log is unreadable" in pub and "locked" not in pub


def test_any_other_fault_is_unreadable_not_quiet(tmp_path, monkeypatch):
    _plant(tmp_path, [_stamp(NOW), SUMMARY], [])
    monkeypatch.setattr(GH, "_wd_decode", lambda raw: (_ for _ in ()).throw(RuntimeError("bad bytes")))
    b = GH._launcher_wd_beat(str(tmp_path), now=NOW)
    assert "unreadable" in b and "RuntimeError" in b["unreadable"]
    got = SD.launcher_wd_lines({"utc": BEAT_UTC, "launcher_watchdog": b}, NOW)
    assert "unreadable" in got[0] and "bad bytes" in got[0]


def test_every_module_object_is_kept_off_the_real_folder():
    """Some tests import the module bare (`import guard_heartbeat`, tools/ on sys.path): a SECOND module object
    with its own constant. The autouse fixture must have redirected every copy loaded at collection time."""
    copies = [m for n, m in list(sys.modules.items())
              if m is not None and n.split(".")[-1] == "guard_heartbeat" and hasattr(m, "LAUNCHER_WD_DIR")]
    assert copies, "control: at least this file's own import is present"
    real = "F:" + chr(92) + "launcher_watchdog"
    assert all(m.LAUNCHER_WD_DIR != real for m in copies), [(m.__name__, m.LAUNCHER_WD_DIR) for m in copies]


def test_publish_carries_the_field_and_survives_a_reader_exception(tmp_path, monkeypatch):
    put = {}
    monkeypatch.setattr(GH, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(GH, "_emptiness_verdict", lambda: {})
    monkeypatch.setattr(GH, "_lane_beat", lambda: None)
    monkeypatch.setattr(GH, "_sec_edgar_beat", lambda: None)
    monkeypatch.setattr(GH, "_notices_beat", lambda: {"run": None, "new": 0, "broken": 0, "items": []})

    class C:
        def put_object(self, **kw):
            put.update(kw)
    monkeypatch.setattr(GH.r2_util, "client", lambda write=False: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)

    _plant(tmp_path, [_stamp(NOW), SUMMARY], [])
    monkeypatch.setattr(GH, "LAUNCHER_WD_DIR", str(tmp_path))
    assert GH.publish() == 0
    assert json.loads(put["Body"])["launcher_watchdog"]["summary"] == SUMMARY

    monkeypatch.setattr(GH, "_launcher_wd_beat", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert GH.publish() == 0
    assert "unreadable" in json.loads(put["Body"])["launcher_watchdog"]


def _w(**kw):
    w = {"hb_utc": _stamp(NOW - dt.timedelta(minutes=3)), "summary": SUMMARY, "alerts": [], "alerts_total": 0,
         "log_lines": 0, "log_parsed": 0, "window_cut": False}
    w.update(kw)
    return {"utc": BEAT_UTC, "launcher_watchdog": w}


def test_digest_lines_for_each_state():
    assert SD.launcher_wd_lines({"utc": BEAT_UTC}, NOW) == [], "a publisher older than the field: no section"
    assert "NOT REPORTING" in SD.launcher_wd_lines({"utc": BEAT_UTC, "launcher_watchdog": None}, NOW)[0]
    assert "could not read the workstation beat" in SD.launcher_wd_lines({"unreadable": "NoSuchKey"}, NOW)[0]
    fresh = SD.launcher_wd_lines(_w(), NOW)
    assert fresh[0] == f"  launcher watchdog: last look 3 min ago - {SUMMARY}"
    stale = SD.launcher_wd_lines(_w(hb_utc=_stamp(NOW - dt.timedelta(minutes=40))), NOW)[0]
    assert "last look 40 min ago - STALE (over 15 min)" in stale
    a = ["2026-11-12T11:00:00Z [56292] GAVE UP HfRunner: three restarts failed"]
    got = SD.launcher_wd_lines(_w(alerts=a, alerts_total=12), NOW)
    assert "     " + a[0] in got and any("and 11 older alert(s)" in l for l in got)
    assert any("alerts UNKNOWN" in l for l in SD.launcher_wd_lines(_w(log_lines=5, log_parsed=0), NOW))
    assert any("does not reach back 24 h" in l for l in SD.launcher_wd_lines(_w(window_cut=True), NOW))
    assert "UNDATED" in SD.launcher_wd_lines(_w(hb_utc="garbage"), NOW)[0]


def test_an_old_guard_beat_does_not_blame_a_watchdog_that_was_fresh():
    beat = _w(hb_utc="2026-11-12T08:57:00Z")
    beat["utc"] = "2026-11-12T09:00:00+00:00"
    got = SD.launcher_wd_lines(beat, NOW)
    assert "3 h old" in got[0] and "STALE" not in got[1]


def test_the_public_log_version_carries_no_names_pids_or_log_text():
    a = ["2026-11-12T11:00:00Z [56292] GAVE UP HfRunner: C:\\Users\\someone\\x failed"]
    got = "\n".join(SD.launcher_wd_lines(_w(alerts=a, alerts_total=1), NOW, public=True))
    assert "1 alert(s) in 24 h" in got
    for leak in ("HfRunner", "IpGuard", "56292", "C:\\", "GAVE UP"):
        assert leak not in got, leak
    unread = {"utc": BEAT_UTC, "launcher_watchdog": {"unreadable": "PermissionError: F:\\launcher_watchdog\\x"}}
    assert "F:\\" not in "\n".join(SD.launcher_wd_lines(unread, NOW, public=True))
    undated = "\n".join(SD.launcher_wd_lines(_w(hb_utc="C:\\Users\\someone"), NOW, public=True))
    assert "UNDATED" in undated and "C:\\" not in undated


def test_the_printed_body_swaps_the_section_and_never_leaks_it():
    a = ["2026-11-12T11:00:00Z [56292] GAVE UP HfRunner: restart failed"]
    beat = _w(alerts=a, alerts_total=1)
    full, pub = SD.launcher_wd_lines(beat, NOW), SD.launcher_wd_lines(beat, NOW, public=True)
    lines = ["head", "  publisher notices: 0 new", ""] + full + ["tail"]
    out = SD.redacted_log_body(lines, 3, full, pub)
    assert out.startswith("head\n") and out.endswith("tail") and "1 alert(s) in 24 h" in out
    assert "HfRunner" not in out and "56292" not in out
    moved = ["inserted later"] + lines                 # the recorded position is now wrong
    out2 = SD.redacted_log_body(moved, 3, full, pub)
    assert "HfRunner" not in out2 and "56292" not in out2 and "head" in out2 and "tail" in out2


def test_the_digest_reads_the_beat_once_and_prints_the_redacted_body():
    src = open(SD.__file__, encoding="utf-8").read()
    assert src.count("read_guard_beat()") == 1, "the beat is read once and shared by both sections"
    assert "{launcher_html}" in src and "launcher_html = (" in src, "the section must reach the HTML (real) email"
    assert 'print(f"[digest] {subject}\\n{log_body}\\n"' in src, "the public CI log must print the redacted body"
    assert "log_body = redacted_log_body(lines, _launcher_at, _launcher, _launcher_public)" in src


def test_the_public_route_copies_none_of_it():
    src = open(os.path.join(ROOT, "api", "worker", "src", "guardHeartbeat.ts"), encoding="utf-8").read()
    assert "launcher" not in src.lower(), "the public route serves whitelisted counts only"
