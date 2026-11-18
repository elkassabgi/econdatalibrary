"""The daily digest shows what gus_dbw has rebuilt but nobody has published (PR #291, R1937).

The crawler only RECORDS an owed publish in _publish_owed.jsonl and never publishes. Without a line
in the email, a passed rebuild would sit unserved with nothing saying so. The same line names the
areas the row-loss guard is holding back, because a refused area stays due and a persistent cause
refuses it on every pass.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import updater.send_digest as sd                   # noqa: E402
from jobs import gus_dbw_refresh as R              # noqa: E402

NOW = dt.datetime(2026, 11, 18, 12, 0, 0, tzinfo=dt.timezone.utc)


def _owe(d, area, md5, hours_ago):
    with open(os.path.join(d, R.OWED_FILE), "a", encoding="utf-8") as f:
        f.write(json.dumps({"area": area, "md5": md5,
                            "rebuilt_utc": (NOW - dt.timedelta(hours=hours_ago)).isoformat()}) + "\n")


def test_owed_count_and_oldest_age(tmp_path):
    d = str(tmp_path)
    _owe(d, "7", "aa", 30.0)        # published below: NOT owed
    _owe(d, "9", "bb", 5.0)
    _owe(d, "43", "cc", 2.0)
    with open(os.path.join(d, R.PUBLISHED_FILE), "w", encoding="utf-8") as f:
        f.write(json.dumps({"area": "7", "md5": "aa"}) + "\n")
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert lines == ["     gus_dbw publish: 2 area(s) rebuilt and OWED to the publisher, "
                     "oldest rebuilt 5.0h ago (area_9)"]


def test_nothing_owed_is_said(tmp_path):
    assert sd.gus_dbw_publish_lines(str(tmp_path), NOW) == ["     gus_dbw publish: 0 areas owed"]


def test_refused_areas_and_torn_lines_are_attention(tmp_path):
    d = str(tmp_path)
    os.makedirs(os.path.join(d, R.REFUSED_SUBDIR, "area_5", "20261118T110000Z"))
    with open(os.path.join(d, R.OWED_FILE), "w", encoding="utf-8") as f:
        f.write('{"area": "8", "md5": "ab')                       # torn last line
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert any("ATTENTION 1 unreadable line" in ln for ln in lines)
    assert any(ln.startswith("     gus_dbw guard: ATTENTION 1 area(s) refused and still due, newest "
                             "refusal 1.0h ago (area_5)") for ln in lines)


def test_an_unreadable_record_is_said_not_silent(tmp_path):
    d = str(tmp_path)
    os.makedirs(os.path.join(d, R.OWED_FILE))                     # a directory where the file should be
    lines = sd.gus_dbw_publish_lines(d, NOW)
    assert len(lines) == 1 and "could not be read" in lines[0] and "ATTENTION" in lines[0]


def test_the_line_reaches_both_bodies():
    src = open(sd.__file__, encoding="utf-8").read()
    cut = src.index("# ---- HTML (the real email) ----")
    assert "lines += _gpub" in src[:cut], "the text body stopped showing gus_dbw's owed publishes"
    assert "for l in _gpub" in src[cut:], "the html body stopped showing gus_dbw's owed publishes"


# ------------------------------------------------------------------ R1938: the digest is sent from CI
# data/ is gitignored and the CI job restores only catalog.db + state.db, so the local record is ABSENT
# where the mail is built. The workstation's guard beat carries publish_summary(); the digest renders it.
def _store_with_debt(d, now):
    _owe(d, "9", "bb", 5.0)
    os.makedirs(os.path.join(d, R.REFUSED_SUBDIR, "area_46",
                             (now - dt.timedelta(hours=1)).strftime("%Y%m%dT%H%M%SZ")))
    return R.publish_summary(d, now)


def test_the_beat_renders_exactly_what_the_local_store_renders(tmp_path):
    d = str(tmp_path)
    s = _store_with_debt(d, NOW)
    assert sd.gus_dbw_beat_lines({"gus_dbw": s}, NOW) == sd.gus_dbw_publish_lines(d, NOW)
    assert sd.gus_dbw_beat_lines({"gus_dbw": s}, NOW)[0].startswith("     gus_dbw publish: 1 area(s) rebuilt and OWED")


def test_a_missing_stale_or_unreadable_summary_is_attention_unknown(tmp_path):
    fresh = R.publish_summary(str(tmp_path), NOW)
    cases = {
        "unreadable beat": {"unreadable": "NoSuchKey"},
        "summary missing": {"utc": NOW.isoformat()},
        "summary stale": {"gus_dbw": dict(fresh, measured_utc=(NOW - dt.timedelta(hours=3)).isoformat())},
        "no measured_utc": {"gus_dbw": {k: v for k, v in fresh.items() if k != "measured_utc"}},
        "record unreadable": {"gus_dbw": {"measured_utc": NOW.isoformat(), "dir_present": True,
                                          "unreadable": "OSError: x"}},
        "no store dir": {"gus_dbw": R.publish_summary(str(tmp_path / "absent"), NOW)},
    }
    for name, beat in cases.items():
        lines = sd.gus_dbw_beat_lines(beat, NOW)
        assert len(lines) == 1 and "ATTENTION UNKNOWN" in lines[0], (name, lines)
        if name == "record unreadable":
            assert "could not read the record: OSError: x" in lines[0], lines
    # negative control: a fresh summary is not UNKNOWN
    assert "UNKNOWN" not in sd.gus_dbw_beat_lines({"gus_dbw": fresh}, NOW)[0]


def test_the_workstation_beat_carries_the_summary(tmp_path, monkeypatch):
    from tools import guard_heartbeat as GH
    d = str(tmp_path)
    _store_with_debt(d, dt.datetime.now(dt.timezone.utc))
    b = GH._gus_dbw_beat(d)
    assert b["dir_present"] and b["owed"] == 1 and b["refused"] == 1 and b["refused_areas"] == ["46"]
    os.makedirs(os.path.join(str(tmp_path / "bad"), R.OWED_FILE))          # unreadable record
    bad = GH._gus_dbw_beat(str(tmp_path / "bad"))
    assert "unreadable" in bad and "measured_utc" in bad
    # and publish() puts it into the body it sends
    put = {}
    monkeypatch.setattr(GH, "GUS_DBW_DIR", d)
    monkeypatch.setattr(GH, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(GH, "_emptiness_verdict", lambda: {})
    monkeypatch.setattr(GH, "_lane_beat", lambda: None)
    monkeypatch.setattr(GH, "_sec_edgar_beat", lambda: None)
    monkeypatch.setattr(GH, "_notices_beat", lambda: {"run": None, "new": 0, "broken": 0, "items": []})
    monkeypatch.setattr(GH, "_launcher_watchdog_beat", lambda: {"installed": False})

    class C:
        def put_object(self, **kw):
            put.update(kw)
    monkeypatch.setattr(GH.r2_util, "client", lambda write=False: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH.publish() == 0
    assert json.loads(put["Body"])["gus_dbw"]["owed"] == 1


def _ci_digest(tmp_path, monkeypatch, beat):
    """send_digest.main() in the CI shape: the first-pass store root is an EMPTY directory, so no gus_dbw
    dir exists, exactly as on the ubuntu runner."""
    from tests.test_launcher_watchdog_beat import _send_digest
    real = sd.firstpass_ages
    empty = tmp_path / "clean_full"
    empty.mkdir()
    monkeypatch.setattr(sd, "firstpass_ages", lambda _root, names, now: real(str(empty), names, now))
    return _send_digest(tmp_path, monkeypatch, beat)


def _beat_with(gus):
    b = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "host": "h",
         "notices": {"run": None, "new": 0, "broken": 0, "items": []},
         "launcher_watchdog": {"installed": False}}
    if gus is not None:
        b["gus_dbw"] = gus
    return b


def test_ci_digest_shows_owed_and_refused_from_the_beat_in_both_bodies(tmp_path, monkeypatch, capsys):
    import html as H
    store = tmp_path / "ws"
    store.mkdir()
    s = _store_with_debt(str(store), dt.datetime.now(dt.timezone.utc))
    sent = _ci_digest(tmp_path, monkeypatch, _beat_with(s))
    capsys.readouterr()
    assert not os.path.isdir(tmp_path / "clean_full" / "gus_dbw")
    for needle in ("gus_dbw publish: 1 area(s) rebuilt and OWED to the publisher",
                   "gus_dbw guard: ATTENTION 1 area(s) refused and still due"):
        assert needle in sent["body"], sent["body"][-3000:]
        assert H.escape(needle) in sent["html"], sent["html"][-3000:]


def test_ci_digest_says_unknown_when_the_summary_is_missing_or_stale(tmp_path, monkeypatch, capsys):
    import html as H
    stale = R.publish_summary(str(tmp_path), dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=5))
    for i, gus in enumerate((None, stale)):
        sub = tmp_path / f"r{i}"
        sub.mkdir()
        sent = _ci_digest(sub, monkeypatch, _beat_with(gus))
        capsys.readouterr()
        assert "gus_dbw publish: ATTENTION UNKNOWN" in sent["body"], sent["body"][-3000:]
        assert H.escape("gus_dbw publish: ATTENTION UNKNOWN") in sent["html"], sent["html"][-3000:]
