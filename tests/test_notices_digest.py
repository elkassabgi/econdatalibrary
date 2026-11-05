"""The publisher-notice summary: the guard beat carries it (tools/guard_heartbeat.py) and the daily digest prints it
(updater/send_digest.py). Each state is planted, and a beat that cannot be read must never read as "no notices"."""
import datetime as dt
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import guard_heartbeat as GH  # noqa: E402
from updater import send_digest as SD  # noqa: E402

NOW = dt.datetime(2026, 11, 6, 12, 0, tzinfo=dt.timezone.utc)
FRESH = "2026-11-06T11:55:00+00:00"


def _q(path, lines):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(d) + "\n" for d in lines), encoding="utf-8")


def test_beat_counts_new_items_best_first_and_skips_triaged(tmp_path, monkeypatch):
    q = tmp_path / "data" / "_maintenance" / "queue.jsonl"
    st = tmp_path / "logs" / "notices.last.json"
    _q(q, [{"id": "a", "kind": "publisher_notice", "status": "new", "score": 3, "publisher_id": "bls", "title": "x"},
           {"id": "b", "kind": "publisher_notice", "status": "new", "score": 9, "publisher_id": "fed_board",
            "title": "H15: Discontinuance", "link": "https://f/1"},
           {"id": "c", "kind": "feed_broken", "status": "new", "publisher_id": "ons", "title": "feed failed"},
           {"id": "d", "kind": "publisher_notice", "status": "new", "score": 5, "title": "triaged"},
           {"id": "d", "event": "status", "status": "irrelevant", "note": "n", "utc": "2026-11-06T00:00:00Z"}])
    st.parent.mkdir(parents=True)
    st.write_text('{"started":"2026-11-06T09:00:00Z","ended":"2026-11-06T09:01:00Z","rc":0,"pid":1,'
                  '"last_ok_started":"2026-11-06T09:00:00Z"}', encoding="ascii")
    monkeypatch.setattr(GH, "NOTICES_QUEUE_LOCAL", str(q))
    monkeypatch.setattr(GH, "NOTICES_STATUS_LOCAL", str(st))
    b = GH._notices_beat()
    assert b["new"] == 3 and b["broken"] == 1 and b["run"]["rc"] == 0
    assert [i["publisher_id"] for i in b["items"]][:2] == ["fed_board", "bls"]
    lines = SD.notices_lines({"utc": FRESH, "notices": b}, NOW)
    assert lines[0] == "  publisher notices: 3 new, 1 broken feed(s)"
    assert any("H15: Discontinuance" in l for l in lines) and not any("triaged" in l for l in lines)


def test_a_bad_queue_line_is_unreadable_not_empty(tmp_path, monkeypatch):
    q = tmp_path / "q.jsonl"
    q.write_text("not json\n", encoding="utf-8")
    monkeypatch.setattr(GH, "NOTICES_QUEUE_LOCAL", str(q))
    monkeypatch.setattr(GH, "NOTICES_STATUS_LOCAL", str(tmp_path / "absent.json"))
    b = GH._notices_beat()
    assert "unreadable" in b and b["run"] is None
    assert any("queue is unreadable" in l for l in SD.notices_lines({"utc": FRESH, "notices": b}, NOW))


def _lines(n, utc=FRESH):
    return SD.notices_lines({"utc": utc, "notices": n}, NOW)


def test_digest_lines_for_each_state():
    assert SD.notices_lines({"utc": FRESH}, NOW) == [], "a beat from before the watcher: no section"
    assert "could not read" in SD.notices_lines({"unreadable": "NoSuchKey"}, NOW)[0]
    assert _lines({"run": None, "new": 0, "broken": 0, "items": []}) == [], \
        "never run and nothing queued: no line every morning that says nothing (AR-436)"
    got = _lines({"run": None, "new": 2, "broken": 0, "items": [{"publisher_id": "p", "title": "t"}] * 2})
    assert got[0].startswith("  publisher notices: 2 new, 0 broken feed(s); the daily run has not run")
    late = {"run": {"rc": 0, "last_ok_started": "2026-11-04T09:00:00Z"}, "new": 0, "broken": 0, "items": []}
    assert "LATE" in _lines(late)[0]
    failed = {"run": {"rc": 96, "last_ok_started": "2026-11-06T09:00:00Z"}, "new": 0, "broken": 0, "items": []}
    assert "last run exited 96" in _lines(failed)[0]
    many = {"run": {"rc": 0, "last_ok_started": "2026-11-06T09:00:00Z"}, "new": 12, "broken": 0,
            "items": [{"publisher_id": "p", "title": "t", "link": ""}] * 10}
    assert any("and 2 more" in l for l in _lines(many))


def test_an_old_beat_blames_the_guard_loop_not_the_watcher():
    late = {"run": {"rc": 0, "last_ok_started": "2026-11-04T09:00:00Z"}, "new": 0, "broken": 0, "items": []}
    got = _lines(late, utc="2026-11-04T12:00:00+00:00")
    assert "48 h old" in got[0] and not any("LATE" in l for l in got)
    assert "undated" in _lines(late, utc=None)[0]


def test_odd_queue_lines_cannot_stop_the_beat(tmp_path, monkeypatch):
    """The two AR-436 probes: a string score beside an int one, and mixed-type found_utc at equal score."""
    q = tmp_path / "q.jsonl"
    _q(q, [{"id": "a", "status": "new", "score": "5", "found_utc": 7},
           {"id": "b", "status": "new", "score": 3, "found_utc": "2026-11-06T00:00:00Z"},
           {"id": "c", "status": "new", "score": 3, "found_utc": None, "title": ["not", "a", "string"]}])
    monkeypatch.setattr(GH, "NOTICES_QUEUE_LOCAL", str(q))
    monkeypatch.setattr(GH, "NOTICES_STATUS_LOCAL", str(tmp_path / "absent.json"))
    b = GH._notices_beat()
    assert b["new"] == 3 and [i["score"] for i in b["items"]] == [5.0, 3.0, 3.0] and "unreadable" not in b
    json.dumps(b)


def test_publish_survives_a_notices_fault(monkeypatch):
    put = {}
    monkeypatch.setattr(GH, "_notices_beat", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(GH, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(GH, "_emptiness_verdict", lambda: {})
    monkeypatch.setattr(GH, "_lane_beat", lambda: None)
    monkeypatch.setattr(GH, "_sec_edgar_beat", lambda: None)

    class C:
        def put_object(self, **kw):
            put.update(kw)
    monkeypatch.setattr(GH.r2_util, "client", lambda write=False: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH.publish() == 0
    assert "unreadable" in json.loads(put["Body"])["notices"]


def test_the_beat_read_is_bounded(monkeypatch):
    import time
    from core import r2_util

    class Slow:
        def get_object(self, **kw):
            time.sleep(5)
    monkeypatch.setattr(r2_util, "client", lambda *a, **k: Slow())
    t0 = time.monotonic()
    got = SD.read_guard_beat(timeout_s=0.5)
    assert time.monotonic() - t0 < 3 and "no answer from R2" in got["unreadable"]


def test_the_public_route_copies_none_of_it():
    src = open(os.path.join(ROOT, "api", "worker", "src", "guardHeartbeat.ts"), encoding="utf-8").read()
    assert "notices" not in src, "the public route serves whitelisted counts only; queue titles stay private"
