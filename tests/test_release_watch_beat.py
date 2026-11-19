"""The release watch in the guard beat (tools/guard_heartbeat.py _release_watch_beat, updater/release_watch.py
beat_value): release-watch design Revision 5, tests B8.10 (C4: isolation, size cap) and B8.12 (C7: the first scrub
pass, and the public route). No R2: publish() is called with a fake client and cutover forced off."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tools import guard_heartbeat as GH  # noqa: E402
from updater import release_watch as RW  # noqa: E402

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
BIO, CAT, PROC = (s for s, _u in RW.PHASE1_UNITS)


def _write_record(path, seq_by_sid):
    rec = None
    for sid, toks in seq_by_sid.items():
        for i, p in enumerate(toks):
            rec = RW.apply_outcome(rec, sid, "_all", {"p": p, "error": None if p else "probe: x"},
                                   NOW.replace(day=1 + i))
    RW.write_atomic(str(path), rec)
    return rec


def _status(path, **kw):
    s = {"started": "2026-11-20T09:00:00Z", "ended": "2026-11-20T09:01:00Z", "rc": 0,
         "last_ok_started": "2026-11-20T09:00:00Z", "pid": 1, "note": None}
    s.update(kw)
    path.write_text(json.dumps(s), encoding="ascii")


def _point(monkeypatch, tmp_path):
    rec, st = tmp_path / "data" / "_release_watch" / "record.json", tmp_path / "logs" / "release_watch.last.json"
    st.parent.mkdir(parents=True)
    monkeypatch.setattr(GH, "RELEASE_WATCH_RECORD_LOCAL", str(rec))
    monkeypatch.setattr(GH, "RELEASE_WATCH_STATUS_LOCAL", str(st))
    monkeypatch.setattr(RW, "_gated_from_denylist", lambda: set())
    return rec, st


def test_the_beat_carries_the_record_and_the_runner_status(tmp_path, monkeypatch):
    rec, st = _point(monkeypatch, tmp_path)
    _write_record(rec, {BIO: ["10003|a", "10004|b"], CAT: [None]})
    _status(st, rc=96, note="killed after 15 min (process tree)")
    b = GH._release_watch_beat()
    assert b["run"] == {"started_utc": "2026-11-20T09:00:00Z", "ended_utc": "2026-11-20T09:01:00Z", "rc": 96,
                        "last_ok_started_utc": "2026-11-20T09:00:00Z", "note": "killed after 15 min (process tree)"}
    u = b["units"][RW.unit_key(BIO, "_all")]
    assert u["last_p"] == "10004|b" and [e["to"] for e in u["events"]] == ["10003|a", "10004|b"]
    assert u["events"][0]["initial"] is True and "initial" not in u["events"][1]
    assert b["pending"][RW.unit_key(CAT, "_all")]["cnc_days"] == 1
    json.dumps(b)


def test_no_record_yet_still_carries_the_run(tmp_path):
    st = tmp_path / "release_watch.last.json"
    _status(st)
    b = RW.beat_value(str(tmp_path / "absent" / "record.json"), str(st), gated_reader=lambda: set())
    assert b["units"] == {} and b["pending"] == {} and b["run"]["last_ok_started_utc"] == "2026-11-20T09:00:00Z"
    b = RW.beat_value(str(tmp_path / "absent" / "record.json"), str(tmp_path / "absent" / "s.json"),
                      gated_reader=lambda: set())
    assert b == {"schema": RW.SCHEMA, "run": None, "units": {}, "pending": {}}


# ------------------------------------------------------------------ B8.10 isolation and the size cap (C4)
def test_b8_10_a_corrupt_record_is_unreadable(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    rec.parent.mkdir(parents=True)
    rec.write_text("{\"schema\": \"ekd-release-watch/1\", \"units\": [", encoding="utf-8")
    assert "unreadable" in GH._release_watch_beat()
    rec.write_text(json.dumps({"schema": "something/else", "units": {}}), encoding="utf-8")
    assert "schema" in GH._release_watch_beat()["unreadable"]


def test_b8_10_a_record_over_the_size_cap_is_unreadable_never_partial(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    long = {sid: [f"{i:03d}" + "x" * 200 for i in range(25)] for sid, _u in RW.PHASE1_UNITS}
    _write_record(rec, long)
    assert GH._release_watch_beat() == {"unreadable": "record over size cap"}
    _write_record(rec, {BIO: ["10003|a"]})
    small = GH._release_watch_beat()
    assert "units" in small and len(json.dumps(small, separators=(",", ":"))) <= RW.BEAT_MAX_BYTES


def test_b8_10_tokens_events_and_errors_are_capped(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    r = _write_record(rec, {BIO: ["10003|" + "y" * 300]})
    r["units"][RW.unit_key(BIO, "_all")]["last_error"] = "e" * 500
    RW.write_atomic(str(rec), r)
    u = GH._release_watch_beat()["units"][RW.unit_key(BIO, "_all")]
    assert len(u["last_p"]) == RW.TOKEN_MAX and len(u["events"][0]["to"]) == RW.TOKEN_MAX
    assert len(u["last_error"]) == RW.ERROR_MAX


def _publish(monkeypatch):
    put = {}
    monkeypatch.setattr(GH, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(GH, "_emptiness_verdict", lambda: {})
    monkeypatch.setattr(GH, "_lane_beat", lambda: None)
    monkeypatch.setattr(GH, "_sec_edgar_beat", lambda: None)
    monkeypatch.setattr(GH, "_notices_beat", lambda: {"run": None, "new": 0, "broken": 0, "items": []})
    monkeypatch.setattr(GH, "_launcher_watchdog_beat", lambda: {})
    monkeypatch.setattr(GH, "_gus_dbw_beat", lambda: {})

    class C:
        def put_object(self, **kw):
            put.update(kw)
    monkeypatch.setattr(GH.r2_util, "client", lambda write=False: C())
    from core import cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    assert GH.publish() == 0
    return json.loads(put["Body"])


def test_b8_10_a_reader_that_raises_keyboardinterrupt_cannot_stop_the_beat(monkeypatch):
    def boom(*a, **k):
        raise KeyboardInterrupt("reader interrupted")
    monkeypatch.setattr(RW, "beat_value", boom)
    body = _publish(monkeypatch)
    assert body["release_watch"] == {"unreadable": "KeyboardInterrupt: reader interrupted"}
    assert "notices" in body and "jobs_alive" in body and "utc" in body


def test_b8_10_a_corrupt_record_cannot_stop_the_beat(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    rec.parent.mkdir(parents=True)
    rec.write_bytes(b"\xff\xfe not json")
    body = _publish(monkeypatch)
    assert "unreadable" in body["release_watch"]
    assert "notices" in body and "jobs_alive" in body


# ------------------------------------------------------------------ B8.12 the first scrub pass (C7)
def test_b8_12_a_gated_id_in_the_record_is_dropped_from_the_beat(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    _write_record(rec, {BIO: ["10003|a"], CAT: ["10002|x"], PROC: [None]})
    monkeypatch.setattr(RW, "_gated_from_denylist", lambda: {BIO, PROC})
    b = GH._release_watch_beat()
    assert list(b["units"]) == [RW.unit_key(CAT, "_all")] and b["pending"] == {}
    assert BIO not in json.dumps(b) and PROC not in json.dumps(b)


def test_b8_12_an_unreadable_denylist_at_the_desktop_gives_counts_only(tmp_path, monkeypatch):
    rec, st = _point(monkeypatch, tmp_path)
    _write_record(rec, {BIO: ["10003|a"], CAT: ["10002|x"], PROC: [None]})
    _status(st)
    monkeypatch.setattr(RW, "_gated_from_denylist", lambda: None)
    b = GH._release_watch_beat()
    assert set(b) == {"schema", "run", "units_n", "names"} and b["units_n"] == 3
    assert b["names"] == "withheld: denylist unreadable"
    s = json.dumps(b)
    assert not any(x in s for x in (BIO, CAT, PROC, "10003", "10002"))


def test_b8_12_the_denylist_reader_is_the_digests_reader():
    """The beat reads core.gen_denylist.committed_gate, the reader send_digest._gated_ids uses."""
    from updater import send_digest as SD
    same = RW._gated_from_denylist() == SD._gated_ids()      # compared, never printed: the ids are gated
    assert same, "the beat and the digest read different denylists"


def test_b8_12_free_text_in_the_beat_is_scrubbed(tmp_path, monkeypatch):
    rec, _st = _point(monkeypatch, tmp_path)
    r = _write_record(rec, {BIO: ["10003|a", None]})
    r["units"][RW.unit_key(BIO, "_all")]["last_error"] = "probe: 404 for zzgatedsrc_table"
    RW.write_atomic(str(rec), r)
    monkeypatch.setattr(RW, "_gated_from_denylist", lambda: {"zzgatedsrc"})
    b = GH._release_watch_beat()
    assert "zzgatedsrc" not in json.dumps(b) and b["units"][RW.unit_key(BIO, "_all")]["last_error"] == "[withheld]"


def test_b8_12_the_public_route_copies_none_of_it():
    src = open(os.path.join(ROOT, "api", "worker", "src", "guardHeartbeat.ts"), encoding="utf-8").read()
    assert "release_watch" not in src and "release" not in src.lower()


def test_the_beat_reads_the_files_the_watch_writes():
    """Verification C6: the guard beat's two paths are the watch's own constants, so a rename cannot leave the beat
    reading a file nothing writes."""
    assert os.path.normcase(GH.RELEASE_WATCH_RECORD_LOCAL) == os.path.normcase(RW.RECORD_PATH)
    assert os.path.normcase(GH.RELEASE_WATCH_STATUS_LOCAL) == os.path.normcase(RW.STATUS_PATH)


def test_the_worst_case_record_token_re_allows_fits_under_the_cap(tmp_path, monkeypatch):
    """Verification C2: the largest record the code can produce - 3 units, a trimmed 20-event history of the longest
    tokens TOKEN_RE accepts (9-digit version, 7-digit fraction, Z), errors and a runner note past their caps - must
    still be carried, never 'record over size cap'. The measured size is pinned with headroom."""
    import re
    from datetime import timedelta
    worst = "999999999|2026-12-31T23:59:59.1234567Z"
    assert re.fullmatch(RW.TOKEN_RE, worst) and len(worst) == 38
    rec_p, st = _point(monkeypatch, tmp_path)
    rec = None
    for sid, uid in RW.PHASE1_UNITS:
        for i in range(RW.EVENTS_MAX + 10):
            p = f"{999999999 - i}|2026-12-31T23:59:59.1234567Z"
            rec = RW.apply_outcome(rec, sid, uid, {"p": p, "error": None}, NOW - timedelta(days=60 - i))
        rec = RW.apply_outcome(rec, sid, uid, {"p": None, "error": "e" * 500}, NOW)
    RW.write_atomic(str(rec_p), rec)
    _status(st, rc=96, note="n" * 500)
    b = GH._release_watch_beat()
    assert "unreadable" not in b, b
    assert all(len(u["events"]) == RW.EVENTS_MAX and u["trimmed"] for u in b["units"].values())
    size = len(json.dumps(b, separators=(",", ":")))
    assert size <= RW.BEAT_MAX_BYTES - 1024, f"{size} B leaves under 1 KB of headroom under {RW.BEAT_MAX_BYTES}"


def test_the_digest_rebuilds_from_out_of_a_real_beat(tmp_path, monkeypatch):
    """The beat leaves `from` out where it is the previous event's `to`; the digest must put it back, including the
    first kept move of a trimmed history (which keeps its own `from`)."""
    from datetime import timedelta
    rec_p, st = _point(monkeypatch, tmp_path)
    _status(st)
    _write_record(rec_p, {BIO: ["10003|a", "10004|b", "10005|c"]})
    b = {"utc": NOW.isoformat(), "release_watch": GH._release_watch_beat()}
    assert "from" not in b["release_watch"]["units"][RW.unit_key(BIO, "_all")]["events"][1]
    v = RW.digest_view(b, [(BIO, "_all", "10004|b")], None, set(), NOW, 2.0, 30.0)
    assert v["suffix"][(BIO, "_all")].startswith("release: PUBLISHER MOVED 10004|b -> 10005|c since "), v["suffix"]
    rec = None
    for i in range(30):
        rec = RW.apply_outcome(rec, BIO, "_all", {"p": f"1{i:04d}|t", "error": None}, NOW - timedelta(days=40 - i))
    RW.write_atomic(str(rec_p), rec)
    b = {"utc": NOW.isoformat(), "release_watch": GH._release_watch_beat()}
    ev = b["release_watch"]["units"][RW.unit_key(BIO, "_all")]["events"]
    assert ev[1]["from"] == "10010|t" and "from" not in ev[2]
    v = RW.digest_view(b, [], None, set(), NOW, 2.0, 30.0)
    assert v["suffix"][(BIO, "_all")].startswith("release: PUBLISHER MOVED 10010|t -> 10029|t since on or before ")
