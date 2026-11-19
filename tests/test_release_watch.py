"""The release watch (updater/release_watch.py): the record, the digest rules, the paths, the acks and both mail
bodies. Build spec: release-watch design Revision 5, tests B8.1-B8.3, B8.8, B8.9, B8.11, B8.12 (digest half), B8.13.

No network and no child process here: tests/test_release_watch_sandbox.py runs the probe children (stub probes),
tests/test_release_watch_beat.py the guard beat. tools/release_watch_mutants.py applies each named B8 mutant on disk,
one at a time, and fails if a mutant survives."""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import release_watch as RW  # noqa: E402

NOW = datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)
BIO, CAT, PROC = (s for s, _u in RW.PHASE1_UNITS)
K_BIO = RW.unit_key(BIO, "_all")
OLD_H, LATE_H = 2.0, 30.0                       # send_digest.GUARD_BEAT_OLD_H / NOTICES_RUN_LATE_H (pinned below)


def day(n, h=10):
    """A probe instant n days before NOW (at h:00 UTC)."""
    return (NOW - timedelta(days=n)).replace(hour=h, minute=0, second=0)


def record_of(seq, sid=BIO, uid="_all", rec=None):
    """Fold [(days_before_now, P or None)] through apply_outcome, oldest first."""
    for n, p in seq:
        rec = RW.apply_outcome(rec, sid, uid, {"p": p, "error": None if p else "probe: HTTPError 503"}, day(n))
    return rec


def beat_of(rec, run=None, age_h=0.1, gated=frozenset()):
    """A guard beat that carries beat_value() of `rec` (written to a file the way the driver writes it)."""
    rw = {"schema": RW.SCHEMA, "run": run, "units": {}, "pending": {}}
    if rec is not None:
        rw["units"] = json.loads(json.dumps(rec["units"]))
        rw["pending"] = json.loads(json.dumps(rec.get("pending") or {}))
    rw["units"] = {k: v for k, v in rw["units"].items() if k.split("/")[0] not in gated}
    return {"utc": (NOW - timedelta(hours=age_h)).isoformat(), "release_watch": rw}


def ok_run(hours_ago=3, rc=0, ok_hours_ago=None):
    s = (NOW - timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    ok = s if ok_hours_ago is None else (NOW - timedelta(hours=ok_hours_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"started_utc": s, "ended_utc": s, "rc": rc, "last_ok_started_utc": ok}


def view(beat, rows=(), acks=None, gated=frozenset(), now=NOW):
    return RW.digest_view(beat, list(rows), acks, None if gated is None else set(gated), now, OLD_H, LATE_H)


def text(v):
    return "\n".join(v["lines"]) + "\n" + "\n".join(v["suffix"].values())


def test_the_digest_uses_the_notices_clocks_unchanged():
    """C8: GUARD_BEAT_OLD_H and NOTICES_RUN_LATE_H are reused, not re-picked."""
    from updater import send_digest as SD
    assert (SD.GUARD_BEAT_OLD_H, SD.NOTICES_RUN_LATE_H) == (OLD_H, LATE_H) == (2.0, 30.0)


# ------------------------------------------------------------------ B8.1-B8.3: LATE, stale, rc (C1)
def test_b8_1_a_dead_runner_reads_late_and_no_line_says_a_bare_same():
    rec = record_of([(5, "10003|2026-07-15T11:40:26"), (4, "10003|2026-07-15T11:40:26")])
    for sid in (CAT, PROC):
        rec = record_of([(4, "10002|2026-05-27T15:49:10")], sid=sid, rec=rec)
    stamped = [(BIO, "_all", "10003|2026-07-15T11:40:26"), (CAT, "_all", "10002|2026-05-27T15:49:10"),
               (PROC, "_all", "10002|2026-05-27T15:49:10")]
    v = view(beat_of(rec, run=ok_run(hours_ago=72)), rows=stamped)
    late = [l for l in v["lines"] if "release watch LATE (last ok run " in l]
    assert late and (NOW - timedelta(hours=72)).strftime("%Y-%m-%d") in late[0]
    assert len(v["suffix"]) == 3
    for s in v["suffix"].values():
        assert re.fullmatch(r"release: same as of \d{4}-\d{2}-\d{2}", s), s
    for l in v["lines"]:
        if any(sid in l for sid in (BIO, CAT, PROC)) and re.search(r"\bsame\b", l):
            assert re.search(r"same as of \d{4}-\d{2}-\d{2}", l), l


def test_b8_1b_a_runner_that_never_ran_is_late_too():
    v = view(beat_of(None, run=None))
    assert any("release watch LATE (last ok run none)" in l for l in v["lines"])
    assert set(v["suffix"].values()) == {"release: no record (no real probe yet)"}


def test_b8_2_a_stale_beat_says_so_and_judges_neither_late_nor_same():
    rec = record_of([(4, "10003|2026-07-15T11:40:26")])
    v = view(beat_of(rec, run=ok_run(hours_ago=72), age_h=3.0))
    assert any("release watch: workstation beat is 3 h old - publisher releases unknown" in l for l in v["lines"])
    assert not any("LATE" in l for l in v["lines"])
    assert "same as of" not in text(v)
    assert v["suffix"][(BIO, "_all")] == "release: unknown (workstation beat stale)"


def test_b8_3_a_killed_run_beside_a_fresh_ok_run_is_not_late_but_says_its_rc():
    rec = record_of([(1, "10003|2026-07-15T11:40:26")])
    v = view(beat_of(rec, run=ok_run(hours_ago=1, rc=96, ok_hours_ago=20)))
    assert not any("LATE" in l for l in v["lines"])
    assert any("last run exited 96" in l for l in v["lines"])
    assert any("LATE" in l and "last run exited 1" in l for l in view(beat_of(rec, run=ok_run(rc=1, ok_hours_ago=40)))["lines"])


def test_unreadable_missing_and_absent_are_three_different_lines():
    assert "could not read the workstation beat (R2 timeout)" in text(view({"unreadable": "R2 timeout"}))
    assert "release watch: not running on the workstation" in text(view({"utc": NOW.isoformat()}))
    got = text(view({"utc": NOW.isoformat(), "release_watch": {"unreadable": "record over size cap"}}))
    assert "could not read the workstation record (record over size cap)" in got
    assert "could not read the workstation beat (no beat)" in text(view(None))


def test_an_unreadable_status_file_is_late_not_ok():
    rec = record_of([(1, "10003|2026-07-15T11:40:26")])
    v = view(beat_of(rec, run={"unreadable": "JSONDecodeError: x"}))
    assert any("LATE (the runner's status file is unreadable" in l for l in v["lines"])


# ------------------------------------------------------------------ B8.8 the record
def test_b8_8_first_real_p_creates_event_0():
    rec = record_of([(3, "10003|2026-07-15T11:40:26")])
    u = rec["units"][K_BIO]
    assert rec["schema"] == RW.SCHEMA
    assert u["events"] == [{"to": "10003|2026-07-15T11:40:26", "from": None, "since_utc": RW.iso(day(3)),
                            "initial": True}]
    assert u["last_p"] == "10003|2026-07-15T11:40:26" and u["cnc_days"] == 0 and u["last_probe_ok"] is True


def test_b8_8_a_failure_on_day_1_creates_no_record():
    assert RW.apply_outcome(None, BIO, "_all", {"p": None, "error": "timeout: x"}, day(1)) is None
    assert RW.apply_outcome(None, BIO, "_all", {"p": "", "error": None}, day(1)) is None


def test_b8_8_a_failure_between_two_tokens_gives_one_move_and_same_days_add_nothing():
    rec = record_of([(9, "10003|a"), (8, None), (7, "10004|b")])
    ev = rec["units"][K_BIO]["events"]
    assert [(e["from"], e["to"]) for e in ev] == [(None, "10003|a"), ("10003|a", "10004|b")]
    assert ev[1]["since_utc"] == RW.iso(day(7))
    rec = record_of([(n, "10004|b") for n in (6, 5, 4, 3, 2)], rec=rec)
    assert len(rec["units"][K_BIO]["events"]) == 2, "five more days of the same P add no event"
    assert rec["units"][K_BIO]["first_seen_utc"] == RW.iso(day(7))


def test_b8_8_a_failure_keeps_last_p_and_cnc_days_counts_days_not_runs():
    rec = record_of([(5, "10003|a")])
    for n, h in ((4, 9), (4, 11), (4, 13), (3, 9), (2, 9), (2, 15)):
        rec = RW.apply_outcome(rec, BIO, "_all", {"p": None, "error": "exit: 1"}, day(n, h))
    u = rec["units"][K_BIO]
    assert u["cnc_days"] == 3 and u["last_p"] == "10003|a" and u["last_probe_ok"] is False
    assert len(u["events"]) == 1 and u["last_ok_probe_utc"] == RW.iso(day(5))
    rec = record_of([(1, "10003|a")], rec=rec)
    assert rec["units"][K_BIO]["cnc_days"] == 0 and rec["units"][K_BIO]["last_error"] is None


def test_b8_8_a_never_seen_unit_is_pending_not_recorded():
    rec = record_of([(3, "10003|a")])
    rec = record_of([(3, None), (2, None)], sid=CAT, rec=rec)
    assert RW.unit_key(CAT, "_all") not in rec["units"]
    assert rec["pending"][RW.unit_key(CAT, "_all")]["cnc_days"] == 2


def test_b8_8_trimming_at_21_events_keeps_event_0_and_the_newest_19():
    rec = record_of([(40 - i, f"1{i:04d}|t") for i in range(21)])
    ev = rec["units"][K_BIO]["events"]
    assert len(ev) == 20 and ev[0]["initial"] and ev[0]["to"] == "10000|t"
    assert [e["to"] for e in ev[1:]] == [f"1{i:04d}|t" for i in range(2, 21)]
    assert rec["units"][K_BIO]["trimmed"] is True


def test_the_driver_writes_only_after_a_real_p_and_keeps_a_corrupt_record(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(RW, "_scrubber", lambda: (lambda s: s))
    path = str(tmp_path / "rw" / "record.json")
    fail = lambda sid, uid: {"p": None, "kind": "timeout", "error": "timeout: no answer"}       # noqa: E731
    assert RW.desktop_main(now_fn=lambda: NOW, probe=fail, record_path=path) == 3, "no real P is a failed run"
    assert not os.path.exists(path), "a failure never creates the record"
    good = lambda sid, uid: {"p": f"{sid}|1", "kind": "ok", "error": None}                       # noqa: E731
    assert RW.desktop_main(now_fn=lambda: NOW, probe=good, record_path=path) == 0
    rec = RW.load_record(path)
    assert sorted(rec["units"]) == sorted(RW.unit_key(*u) for u in RW.PHASE1_UNITS)
    assert rec["run"]["last_ok_started_utc"] == RW.iso(NOW) and "BOOTSTRAP" in capsys.readouterr().out
    open(path, "w").write("{not json")
    assert RW.desktop_main(now_fn=lambda: NOW, probe=good, record_path=path) == 2
    assert open(path).read() == "{not json", "an unreadable record is kept untouched"


def test_b8_10_an_atomic_write_killed_before_replace_leaves_the_old_file(tmp_path, monkeypatch):
    path = str(tmp_path / "record.json")
    RW.write_atomic(path, {"v": "old"})
    before = open(path, "rb").read()

    def killed(fd):
        raise KeyboardInterrupt("killed between write and replace")
    monkeypatch.setattr(RW.os, "fsync", killed)
    with pytest.raises(KeyboardInterrupt):
        RW.write_atomic(path, {"v": "new" * 1000})
    assert open(path, "rb").read() == before
    assert sorted(os.listdir(tmp_path)) == ["record.json"], "no temporary file is left behind"


# ------------------------------------------------------------------ B8.9 the digest's move rules (C6)
def _moves():
    """10003 (initial, day 10), 10004 (day 8), then 5 more days of 10004."""
    return record_of([(10, "10003|a"), (8, "10004|b"), (7, "10004|b"), (6, "10004|b"), (5, "10004|b"),
                      (4, "10004|b"), (3, "10004|b")])


def test_b8_9_a_sticky_move_keeps_its_original_since_date():
    v = view(beat_of(_moves(), run=ok_run()))
    want = f"PUBLISHER MOVED 10003|a -> 10004|b since {day(8).date()}"
    assert any(want in l for l in v["lines"]), v["lines"]
    assert v["suffix"][(BIO, "_all")] == f"release: {want}"
    assert "moved-open 1" in text(v)


def test_b8_9_t_equal_to_last_p_clears_it_with_nothing_written():
    rec = _moves()
    snapshot = json.dumps(rec, sort_keys=True)
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10004|b")])
    assert "MOVED" not in text(v) and v["suffix"][(BIO, "_all")].startswith("release: same as of ")
    assert json.dumps(rec, sort_keys=True) == snapshot


def test_b8_9_t_found_mid_history_picks_the_first_event_after_it():
    rec = record_of([(10, "10003|a"), (8, "10004|b"), (6, "10005|c"), (4, "10006|d")])
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10004|b")])
    assert v["suffix"][(BIO, "_all")].startswith(f"release: PUBLISHER MOVED 10004|b -> 10006|d since {day(6).date()}")
    assert "store stamped 10004|b" in v["suffix"][(BIO, "_all")]


def test_b8_9_t_absent_dates_the_move_from_the_first_move_ever():
    rec = record_of([(10, "10003|a"), (8, "10004|b"), (6, "10005|c")])
    for t in (None, "date-tail", "force", ""):
        v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", t)])
        assert v["suffix"][(BIO, "_all")] == f"release: PUBLISHER MOVED 10003|a -> 10005|c since {day(8).date()}", t


def test_b8_9_t_not_among_the_kept_events_of_a_trimmed_history_says_so():
    rec = record_of([(60 - i, f"1{i:04d}|t") for i in range(30)])
    kept = rec["units"][K_BIO]["events"]
    assert rec["units"][K_BIO]["trimmed"] and "10005|t" not in [e["to"] for e in kept]
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10005|t")])
    s = v["suffix"][(BIO, "_all")]
    assert s.startswith(f"release: PUBLISHER MOVED {kept[1]['from']} -> 10029|t since on or before "
                        f"{day(60 - 11).date()} "
                        f"(history trimmed)"), s


def test_b8_9_r5_note_history_trimmed_is_said_even_when_t_is_event_0():
    """T == event 0's token AND T also inside the trimmed stretch: the since-date computed from the kept events can
    be wrong, so the line must say the history was trimmed (R5 review note)."""
    seq = [(80, "A|0"), (79, "B|1"), (78, "A|0")] + [(77 - i, f"B|{i + 2}") for i in range(25)]
    rec = record_of(seq)
    ev = rec["units"][K_BIO]["events"]
    assert rec["units"][K_BIO]["trimmed"] and ev[0]["to"] == "A|0"
    assert [e["to"] for e in ev[1:]].count("A|0") == 0, "the return to T lies in the trimmed stretch"
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "A|0")])
    assert "(history trimmed)" in v["suffix"][(BIO, "_all")], v["suffix"]
    assert f"since on or before {day(77 - (25 - 19)).date()}" in v["suffix"][(BIO, "_all")], v["suffix"]
    untrimmed = record_of([(10, "A|0"), (8, "B|1"), (6, "A|0"), (4, "C|9")])
    assert "trimmed" not in view(beat_of(untrimmed, run=ok_run()), rows=[(BIO, "_all", "A|0")])["suffix"][(BIO, "_all")]


def test_rule_1_a_real_t_that_differs_from_p_is_a_new_release():
    rec = record_of([(3, "10004|b")])
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10003|a")])
    assert any("NEW RELEASE: publisher at 10004|b, store stamped 10003|a" in l for l in v["lines"])
    assert "new 1" in text(v)
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "date-tail")])
    assert "NEW" not in text(v), "a placeholder is never T"


def test_a_could_not_check_streak_gets_a_line_at_3_days_not_at_2():
    rec = record_of([(9, "10003|a"), (2, None), (1, None)])
    v = view(beat_of(rec, run=ok_run()))
    assert not any("COULD NOT CHECK" in l for l in v["lines"])
    assert v["suffix"][(BIO, "_all")] == f"release: could not check x2 d (last real probe {day(9).date()})"
    rec = record_of([(0, None)], rec=rec)
    v = view(beat_of(rec, run=ok_run()))
    assert any(f"release watch COULD NOT CHECK {BIO}/_all x3 d" in l for l in v["lines"])
    assert "(longest could-not-check streak 3 d)" in text(v)


def test_the_counter_is_the_steady_state():
    rec = record_of([(3, "10003|a")])
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10003|a")])
    assert v["lines"][1] == ("  release watch: units 3, same 1, publisher unchanged, store unmeasured 0, new 0, "
                             "moved-open 0, acknowledged 0, no record 2, could not check 0 (longest could-not-check "
                             "streak 0 d)")


def test_a_unit_with_no_store_token_is_never_counted_or_called_same():
    """R2.2 rule 2 / R1951 (verification C1): with no T (the trio today), the publisher being unchanged is not
    "same" - the store's holding is unmeasured. Its own bucket, its own wording, for every placeholder T."""
    rec = record_of([(3, "10003|a")])
    for sid in (CAT, PROC):
        rec = record_of([(3, "10002|x")], sid=sid, rec=rec)
    for rows in ([], [(BIO, "_all", None), (CAT, "_all", "date-tail"), (PROC, "_all", "force")]):
        v = view(beat_of(rec, run=ok_run()), rows=rows)
        t = text(v)
        assert "same 0, publisher unchanged, store unmeasured 3," in t, t
        assert not re.search(r"\bsame as of\b", t), t
        for s in v["suffix"].values():
            assert re.fullmatch(r"release: publisher unchanged as of \d{4}-\d{2}-\d{2}; store unmeasured "
                                r"\(no store token\)", s), s
    assert len(v["lines"]) == 3, "steady state is the heading and the counter only"


# ------------------------------------------------------------------ acks (R3.2)
def _ack(**kw):
    a = {"source_id": BIO, "unit_id": "_all", "to_token": "10004|b", "reason_quote": "spill walk restarted on 10004",
         "reason_url": "https://example.org/issue/1", "by": "operator", "acked_utc": "2026-11-15",
         "expires_utc": "2026-12-10"}
    a.update(kw)
    return {"acks": [a]}


def test_an_ack_moves_the_line_and_never_hides_it():
    v = view(beat_of(_moves(), run=ok_run()), acks=_ack())
    assert any("acknowledged (moved here, not hidden" in l for l in v["lines"])
    assert any("PUBLISHER MOVED 10003|a -> 10004|b" in l and "acknowledged (expires 2026-12-10)" in l
               for l in v["lines"])
    assert "moved-open 0, acknowledged 1" in text(v)


def test_an_ack_over_30_days_voids_the_whole_file_and_an_expired_one_returns_the_line():
    v = view(beat_of(_moves(), run=ok_run()), acks=_ack(expires_utc="2026-12-16"))
    assert any("release_acks.yaml IGNORED" in l for l in v["lines"]) and "moved-open 1" in text(v)
    v = view(beat_of(_moves(), run=ok_run()), acks=_ack(acked_utc="2026-10-01", expires_utc="2026-10-30"))
    assert any("ACK EXPIRED 2026-10-30" in l for l in v["lines"]) and "moved-open 1" in text(v)


def test_an_ack_for_another_token_does_not_apply_and_is_listed_stale():
    v = view(beat_of(_moves(), run=ok_run()), acks=_ack(to_token="10005|c"))
    assert "moved-open 1" in text(v) and any("stale release ack - remove it" in l for l in v["lines"])


def test_the_committed_acks_file_is_valid_and_empty():
    doc = RW.load_acks_doc()
    assert RW.validate_acks(doc, NOW.date()) == ([], None)


# ------------------------------------------------------------------ B8.11 paths (C5)
def test_b8_11_no_path_constant_resolves_under_state_dir_or_data_root():
    from updater import config

    def norm(p):
        return os.path.realpath(os.path.abspath(p)).replace("\\", "/").casefold().rstrip("/")
    bad = [norm(config.STATE_DIR), norm(config.DATA_ROOT)]
    assert len(RW.PATH_CONSTANTS) >= 5
    for p in RW.PATH_CONSTANTS:
        n = norm(p)
        for b in bad:
            assert n != b and not n.startswith(b + "/"), (p, b)
    for p, want in ((RW.RECORD_PATH, "data/_release_watch/record.json"), (RW.LOCK_PATH, "logs/release_watch.lock"),
                    (RW.STATUS_PATH, "logs/release_watch.last.json")):
        assert norm(p).endswith(want)


def test_b8_11_the_check_can_fail():
    """Negative control: a path inside STATE_DIR is caught by the same comparison."""
    from updater import config
    p = os.path.join(config.STATE_DIR, "release_watch.json")
    n = os.path.realpath(p).replace("\\", "/").casefold()
    assert n.startswith(os.path.realpath(config.STATE_DIR).replace("\\", "/").casefold() + "/")


# ------------------------------------------------------------------ B8.12 gated ids, digest half (C7)
def test_b8_12_a_gated_id_is_dropped_again_in_the_digest():
    rec = record_of([(3, "10003|a")])
    rec = record_of([(3, "10002|x")], sid=CAT, rec=rec)
    unscrubbed = beat_of(rec, run=ok_run())          # a beat that still names it (an older publisher)
    v = view(unscrubbed, gated={BIO})
    assert BIO not in text(v) and (BIO, "_all") not in v["suffix"]
    assert "units 2," in text(v), "a gated unit is neither named nor counted"


def test_b8_12_an_unreadable_denylist_at_the_digest_names_nothing():
    rec = record_of([(3, "10003|a")])
    v = view(beat_of(rec, run=ok_run(hours_ago=72)), gated=None)
    t = text(v)
    assert not any(s in t for s in (BIO, CAT, PROC, "10003")) and v["suffix"] == {}
    assert "names withheld: denylist unreadable" in t and "LATE" in t


def test_b8_12_a_beat_with_names_withheld_is_rendered_as_counts():
    beat = {"utc": NOW.isoformat(), "release_watch": {"run": ok_run(), "units_n": 3,
                                                      "names": "withheld: denylist unreadable"}}
    v = view(beat)
    assert "units 3; names withheld: denylist unreadable" in text(v) and v["suffix"] == {}


def test_free_text_is_scrubbed_in_the_digest():
    rec = record_of([(9, "10003|a"), (3, None), (2, None), (1, None)])
    rec["units"][K_BIO]["last_error"] = "probe: HTTPError for zzgatedsrc/report"
    v = view(beat_of(rec, run=ok_run()), gated={"zzgatedsrc"})
    assert "zzgatedsrc" not in text(v) and "[withheld]" in text(v)


# ------------------------------------------------------------------ B8.13 both bodies, end to end
def _state(path, runs, units):
    from updater import state as S
    con = sqlite3.connect(str(path))
    con.executescript(S.DDL)
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s, note) VALUES(?,?,?,?,0,1.0,?)",
                    runs)
    con.executemany("INSERT INTO unit_state(source_id, unit_id, upstream_vintage, status, last_attempt_utc, "
                    "last_success_utc, last_obs_date, last_error) VALUES(?,?,?,?,?,?,?,?)", units)
    con.commit()
    con.close()


def _digest(tmp_path, monkeypatch, beat, gated=frozenset()):
    from updater import registry
    from updater import send_digest as sd
    import core.status_mail as sm
    now = datetime.now(timezone.utc)
    note = f"{CAT}: 1/1 sub-unit(s) transient-failed; will retry [US.TradeFoodCatByProc: ReadTimeout]"
    runs = [((now - timedelta(days=d)).isoformat(timespec="seconds"), CAT, "_all", "transient_fail", note)
            for d in (4, 3, 2, 1)]
    _state(tmp_path / "state.db", runs, [(CAT, "_all", None, "transient_fail", now.isoformat(), None, None, note),
                                          ("eia", "_all", "x|1", "ok", now.isoformat(), now.isoformat(),
                                           "2026-11-01", None)])
    entries = [{"source_id": CAT, "live": True, "cadence": "weekly", "run_location": "local",
                "desktop_lane": "skip", "desktop_lane_exit": "a route that can finish a turn"},
               {"source_id": "eia", "live": True, "cadence": "daily", "run_location": "cloud"}]
    monkeypatch.setattr(sd, "STATE", str(tmp_path / "state.db"))
    monkeypatch.setattr(registry, "load", lambda: {"sources": entries})
    monkeypatch.setattr(sd, "read_guard_beat", lambda *a, **k: beat)
    monkeypatch.setattr(sd, "_gated_ids", lambda: None if gated is None else set(gated))
    sent = {}

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"
    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "success")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    with contextlib.redirect_stdout(io.StringIO()):
        sd.main()
    assert sent, "the mail was not built"
    return sent


def _live_beat(seq_by_sid):
    now = datetime.now(timezone.utc)
    rec = None
    for sid, seq in seq_by_sid.items():
        for n, p in seq:
            rec = RW.apply_outcome(rec, sid, "_all", {"p": p, "error": None if p else "probe: x"},
                                   now - timedelta(days=n, hours=1))
    run = {"started_utc": RW.iso(now - timedelta(hours=2)), "ended_utc": RW.iso(now - timedelta(hours=2)), "rc": 0,
           "last_ok_started_utc": RW.iso(now - timedelta(hours=2))}
    return {"utc": now.isoformat(), "release_watch": {"schema": RW.SCHEMA, "run": run, "units": rec["units"],
                                                      "pending": rec.get("pending") or {}}}


def test_b8_13_the_section_and_the_held_line_suffix_reach_both_bodies(tmp_path, monkeypatch):
    beat = _live_beat({CAT: [(9, "10002|x"), (5, "10003|y"), (0, "10003|y")], BIO: [(0, "10003|a")]})
    m = _digest(tmp_path, monkeypatch, beat)
    import html as H
    moved = "PUBLISHER MOVED 10002|x -> 10003|y since"
    for body, esc in ((m["body"], str), (m["html"], H.escape)):
        assert esc("release watch: units 3, same 0, publisher unchanged, store unmeasured 1, new 0, moved-open 1") in body
        assert esc(moved) in body
        held = [l for l in body.splitlines() if f"{CAT}/_all" in l and "UNIT" in l]
        assert held and esc(f"| release: {moved}") in held[0], held
    assert "Release watch</h3>" in m["html"]


def test_b8_13_an_unreadable_denylist_names_no_unit_in_either_body(tmp_path, monkeypatch):
    beat = _live_beat({BIO: [(0, "10003|a")], PROC: [(0, "10009|z")]})
    m = _digest(tmp_path, monkeypatch, beat, gated=None)
    for body in (m["body"], m["html"]):
        assert "names withheld: denylist unreadable" in body
        assert BIO not in body and PROC not in body and "10009|z" not in body and "| release:" not in body


def test_b8_13_a_gated_unit_is_in_neither_body(tmp_path, monkeypatch):
    beat = _live_beat({BIO: [(0, "10003|a")], PROC: [(0, "10009|z")]})
    m = _digest(tmp_path, monkeypatch, beat, gated={PROC})
    for body in (m["body"], m["html"]):
        assert "release watch: units 2, same 0, publisher unchanged, store unmeasured 1," in body and PROC not in body and "10009|z" not in body


# ------------------------------------------------------------------ review round 1 (AR on this PR)
def test_a_run_with_no_real_p_exits_3_and_a_bootstrap_day_keeps_early_failures(tmp_path, monkeypatch):
    monkeypatch.setattr(RW, "_scrubber", lambda: (lambda s: s))
    path = str(tmp_path / "record.json")
    calls = iter([{"p": None, "kind": "timeout", "error": "timeout: x"}, {"p": "10002|y", "kind": "ok", "error": None},
                  {"p": None, "kind": "exit", "error": "exit: 1"}])
    assert RW.desktop_main(now_fn=lambda: NOW, probe=lambda s, u: next(calls), record_path=path) == 0
    rec = RW.load_record(path)
    assert list(rec["units"]) == [RW.unit_key(CAT, "_all")]
    assert sorted(rec["pending"]) == sorted([K_BIO, RW.unit_key(PROC, "_all")]), "the failure probed first counts"
    fail = lambda s, u: {"p": None, "kind": "timeout", "error": "timeout: x"}                   # noqa: E731
    assert RW.desktop_main(now_fn=lambda: NOW + timedelta(days=1), probe=fail, record_path=path) == 3
    rec = RW.load_record(path)
    assert rec["run"]["rc"] == 3 and rec["run"]["last_ok_started_utc"] == RW.iso(NOW)


def test_a_replace_held_open_for_a_moment_is_retried(tmp_path, monkeypatch):
    path = str(tmp_path / "record.json")
    real, n = os.replace, {"left": 2}

    def flaky(a, b):
        if n["left"]:
            n["left"] -= 1
            raise PermissionError("held open by a reader")
        return real(a, b)
    monkeypatch.setattr(RW.os, "replace", flaky)
    RW.write_atomic(path, {"v": 1})
    assert json.load(open(path)) == {"v": 1} and n["left"] == 0


def test_record_errors_never_quote_a_unit_key():
    bad = {"schema": RW.SCHEMA, "units": {"zzgatedsrc/_all": {"last_p": ""}}}
    with pytest.raises(RW.RecordError) as e:
        RW.validate_record(bad)
    assert "zzgatedsrc" not in str(e.value)


def test_unreadable_reasons_are_scrubbed_in_the_digest():
    beat = {"utc": NOW.isoformat(), "release_watch": {"unreadable": "RecordError: zzgatedsrc/_all broke"}}
    assert "zzgatedsrc" not in text(view(beat, gated={"zzgatedsrc"}))
    assert "zzgatedsrc" not in text(view(beat, gated=None)) and "withheld" in text(view(beat, gated=None))
    assert "zzgatedsrc" not in text(view({"unreadable": "zzgatedsrc"}, gated=None))
    run = {"unreadable": "zzgatedsrc in the status"}
    assert "zzgatedsrc" not in text(view(beat_of(None, run=run), gated={"zzgatedsrc"}))


def test_a_withheld_beat_is_counted_with_the_digests_own_denylist():
    beat = {"utc": NOW.isoformat(), "release_watch": {"run": ok_run(), "units_n": 3,
                                                      "names": "withheld: denylist unreadable"}}
    assert "units 2; names withheld" in text(view(beat, gated={BIO}))


def test_moved_wins_over_new_and_says_when_the_store_token_predates_the_record():
    rec = record_of([(10, "10003|a"), (8, "10004|b")])
    v = view(beat_of(rec, run=ok_run()), rows=[(BIO, "_all", "10001|z")])
    s = v["suffix"][(BIO, "_all")]
    assert s.startswith("release: PUBLISHER MOVED 10003|a -> 10004|b") and "(store token predates the record)" in s
    assert "moved-open 1" in text(v) and "new 0" in text(v)


def test_tokens_longer_than_the_beat_cap_still_clear():
    long = "10004|" + "x" * 200
    rec = record_of([(10, "10003|a"), (8, long)])
    b = beat_of(rec, run=ok_run())
    for u in b["release_watch"]["units"].values():          # as beat_value cuts them
        u["last_p"] = u["last_p"][:RW.TOKEN_MAX]
        for e in u["events"]:
            e["to"] = e["to"][:RW.TOKEN_MAX]
            e["from"] = e["from"][:RW.TOKEN_MAX] if e["from"] else None
    v = view(b, rows=[(BIO, "_all", long)])
    assert "MOVED" not in text(v) and "NEW" not in text(v)
