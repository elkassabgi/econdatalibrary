"""The csv_retry_queue can no longer stall unseen (2026-11-17).

WHAT WAS WRONG (state copy of 2026-11-14): 848,541 queued ids in 20 sources, the oldest from 2026-08-23. health.py did
not read the queue, the daily mail did not show it, and the leftover drain (_drain_leftover_retries) gave the whole
pass budget to the oldest-enqueued source first. usda (68,983 ids) and wid (39,230) never empty, so they kept the
oldest stamp, went first on every pass and took the budget; imf_imts_direct (1,470 ids) and three unctad sources
were not reached in 13 passes.

WHAT THESE TESTS HOLD:
  (1) health: a source whose oldest queued row is MORE than 7 days old carries "CSV-RETRY BACKLOG: n ids, oldest d
      days - served CSVs lag the store" and is held at ATTENTION, never OK or ROTATING; at 7 days or less, nothing;
      an unstamped row counts (its age cannot be shown young); a row held by the backlog ALONE is not in
      gate_failures (yet), and a row that is ATTENTION for another reason still is;
  (2) the digest: a csv retry queue section with the fleet total, each source's count and oldest age, the BACKLOG
      mark by health's own rule, and the sentence that the last drain's unreached sources are not recorded - in
      BOTH bodies of send_digest.main();
  (3) the drain: each source has its own deadline min(pass deadline, start + AQUEDUCT_RETRY_DRAIN_PER_SOURCE_MIN,
      default 15), handed to _drain_csv_retry_queue AND _r2_mirror_gate, so a first source that never empties
      cannot starve the second;
  (4) the rotation: candidates go in ring order from the UTC day's pointer; the ring is the sources THIS pass could
      drain (review AR-285 F2), so a cloud-routed queue takes no ring day;
  (5) review AR-285: the drain is work-conserving (a lone never-emptying source uses the whole budget; a later
      round never re-tries an id of this pass; a source that refails everything fast gets no second round); the
      mirror check runs a chunk at a time, so its cost follows what the slice derives; a ROTATING source held by
      the backlog alone stays in the digest's calm list, while another hold beside it does not.
Replay parity (tools/measure_ci_lag.py against the real run_once) is in test_replay_parity_leftover_drain.py.
Nothing touches the network or a path outside tmp_path.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config, health, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402

SRC = "noaa"                  # a real, live registry id, so assess() emits its row
DEFERRAL = "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next tick"


def _iso(t):
    return t.isoformat(timespec="seconds")


def _healthy(st, now, status="ok", err=None):
    st.upsert_source(SRC, status="ok", last_success_utc=now.isoformat(), last_attempt_utc=now.isoformat())
    st.upsert_unit(SRC, "_all", status=status, last_obs_date=now.date().isoformat(),
                   last_attempt_utc=now.isoformat(), last_success_utc=now.isoformat(), last_error=err)


def _queue(st, n, when, source=SRC):
    st.enqueue_csv_retry(source, [f"{source}:S{i:04d}" for i in range(n)], "derive budget spent - deferred")
    st.db.execute("UPDATE csv_retry_queue SET enqueued_utc=? WHERE source_id=?", (when, source))
    st.db.commit()


def _row(st):
    return next(r for r in health.assess(store=st)["sources"] if r["source"] == SRC)


# ---- (1) health -------------------------------------------------------------------------------------

def test_the_rule_is_more_than_seven_days_not_seven():
    now = dt.datetime(2026, 11, 17, 12, 0, tzinfo=dt.timezone.utc)
    of = health.csv_retry_backlog_of
    assert health.CSV_RETRY_BACKLOG_DAYS == 7.0
    assert of((3, _iso(now - dt.timedelta(days=7)), 0), now) is None, "exactly 7 days is not a backlog"
    assert of((3, _iso(now - dt.timedelta(days=6, hours=23)), 0), now) is None
    b = of((3, _iso(now - dt.timedelta(days=7, seconds=1)), 0), now)
    assert b and b["ids"] == 3 and b["oldest_days"] == 7.0
    assert b["text"] == "CSV-RETRY BACKLOG: 3 ids, oldest 7 days - served CSVs lag the store"
    b = of((68_983, "2026-08-24T00:19:56+00:00", 0), now)
    assert b["text"] == "CSV-RETRY BACKLOG: 68,983 ids, oldest 85 days - served CSVs lag the store"
    assert of(None, now) is None and of((0, None, 0), now) is None


def test_an_unstamped_or_unreadable_row_is_not_called_young():
    now = dt.datetime(2026, 11, 17, tzinfo=dt.timezone.utc)
    of = health.csv_retry_backlog_of
    b = of((5, _iso(now), 2), now)                       # a new stamp beside 2 NULL stamps: MIN() hides them
    assert b and b["oldest_days"] is None and b["unstamped"] == 2
    assert b["text"].startswith("CSV-RETRY BACKLOG: 5 ids, oldest unknown (2 with no enqueued time)")
    b = of((1, "not a time", 0), now)
    assert b and "oldest unknown (enqueued time unreadable" in b["text"]


def test_assess_flags_past_seven_days_and_holds_attention():
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _healthy(st, now)
    assert _row(st)["health"] == "OK", "precondition: the fabricated source is OK on every other signal"
    _queue(st, 4, _iso(now - dt.timedelta(days=6, hours=23)))          # young: 6.96 days
    row = _row(st)
    assert row["health"] == "OK" and row["csv_retry_backlog"] is None
    assert not any("CSV-RETRY BACKLOG" in a for a in row["attention"])
    st.db.execute("UPDATE csv_retry_queue SET enqueued_utc=?", (_iso(now - dt.timedelta(days=7, hours=1)),))
    st.db.commit()
    row = _row(st)
    assert row["health"] == "ATTENTION"
    assert row["attention"][0] == "CSV-RETRY BACKLOG: 4 ids, oldest 7 days - served CSVs lag the store"
    assert row["csv_retry_backlog"] == {"ids": 4, "oldest_days": 7.0, "unstamped": 0}
    assert row["csv_retry_backlog_hold"] is True
    st.clear_csv_retries([f"{SRC}:S{i:04d}" for i in range(4)])
    assert _row(st)["health"] == "OK", "the queue paid: the hold lifts"


def test_a_rotating_source_is_held_at_attention_never_rotating_or_ok():
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _healthy(st, now, status="partial", err=DEFERRAL)
    assert _row(st)["health"] == "ROTATING", "precondition: a clean budget deferral reads ROTATING"
    _queue(st, 2, _iso(now - dt.timedelta(days=30)))
    row = _row(st)
    assert row["health"] == "ATTENTION" and row["health"] not in ("OK", "ROTATING")
    assert row["csv_retry_backlog_hold"] is True


def test_the_backlog_alone_does_not_fail_the_gate_but_another_hold_still_does(monkeypatch):
    monkeypatch.setattr(config, "BACKEND", "local", raising=False)     # a local invocation judges every source
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _healthy(st, now)
    _queue(st, 2, _iso(now - dt.timedelta(days=30)))
    rep = health.assess(store=st)
    row = next(r for r in rep["sources"] if r["source"] == SRC)
    assert row["live"] and row["health"] == "ATTENTION" and row["csv_retry_backlog_hold"]
    assert not any(f.split()[-1] == SRC for f in health.gate_failures(rep)), "the backlog is not a gate yet"
    # the same backlog on a source that is ATTENTION for its own reason: that reason still gates
    st.note_full_rederive_owed(SRC, vintage="v", note="n")
    rep = health.assess(store=st)
    row = next(r for r in rep["sources"] if r["source"] == SRC)
    assert row["health"] == "ATTENTION" and row["csv_retry_backlog_hold"] is False
    # the hold that SET the verdict keeps attention[0] (its remedy); the backlog note is appended (review F1)
    assert row["attention"][0].startswith("full re-derive OWED") and row["attention"][-1].startswith("CSV-RETRY")
    assert any(f.split()[-1] == SRC for f in health.gate_failures(rep))


def test_a_desktop_owed_hold_beside_the_backlog_still_gates(monkeypatch):
    monkeypatch.setattr(config, "BACKEND", "local", raising=False)
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _healthy(st, now)
    _queue(st, 2, _iso(now - dt.timedelta(days=30)))
    st.note_csv_desktop_owed(SRC, [(f"{SRC}:X1", 10, "too large for the cloud path")])
    rep = health.assess(store=st)
    row = next(r for r in rep["sources"] if r["source"] == SRC)
    assert row["health"] == "ATTENTION" and row["csv_retry_backlog_hold"] is False
    assert "OWED to the desktop derive" in row["attention"][0] and row["attention"][-1].startswith("CSV-RETRY")
    assert any(f.split()[-1] == SRC for f in health.gate_failures(rep))


def test_a_stamp_with_no_zone_reads_as_utc():
    now = dt.datetime(2026, 11, 17, 12, tzinfo=dt.timezone.utc)
    b = health.csv_retry_backlog_of((1, "2026-11-01T12:00:00", 0), now)
    assert b and b["oldest_days"] == 16.0
    assert health.csv_retry_backlog_of((1, "2026-11-16T12:00:00", 0), now) is None


def test_a_worse_verdict_stands():
    st = StateStore(path=":memory:")
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=4000)
    _healthy(st, old)
    base = _row(st)["health"]
    assert base.startswith("RED"), base
    _queue(st, 1, _iso(old))
    row = _row(st)
    assert row["health"] == base and row["csv_retry_backlog_hold"] is False
    assert any(a.startswith("CSV-RETRY BACKLOG") for a in row["attention"])


def test_assess_reads_the_queue_with_one_group_by():
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _queue(st, 3, _iso(now - dt.timedelta(days=10)))
    _queue(st, 2, _iso(now), source="usda")
    st.db.execute("UPDATE csv_retry_queue SET enqueued_utc=NULL WHERE series_id='usda:S0000'")
    st.db.commit()
    assert st.csv_retry_backlog() == {SRC: (3, _iso(now - dt.timedelta(days=10)), 0), "usda": (2, _iso(now), 1)}
    calls = []
    real = st.csv_retry_backlog
    st.csv_retry_backlog = lambda: calls.append(1) or real()
    health.assess(store=st)
    assert calls == [1]


# ---- (2) the digest ---------------------------------------------------------------------------------

def test_the_digest_section_lines():
    from updater import send_digest as sd
    now = dt.datetime(2026, 11, 17, tzinfo=dt.timezone.utc)
    groups = {"usda": (68_983, "2026-08-24T00:19:56+00:00", 0),
              "imf_imts_direct": (1_470, _iso(now - dt.timedelta(days=3)), 0), "gone": (0, None, 0)}
    lines = sd.csv_retry_lines(groups, now, health.csv_retry_backlog_of, health.CSV_RETRY_BACKLOG_DAYS)
    text = "\n".join(lines)
    assert "70,453 ids in 2 source(s), oldest current enqueue 85 days" in lines[0]
    assert "an id queued again restarts its time" in lines[0], "review AR-285 F5: say what the age measures"
    assert "1 past the health check's 7-day BACKLOG mark" in lines[0]
    usda = next(l for l in lines if "usda" in l)
    imts = next(l for l in lines if "imf_imts_direct" in l)
    assert "68,983 ids" in usda and "oldest enqueue" in usda and "85.0d" in usda and "BACKLOG (ATTENTION)" in usda
    assert "1,470 ids" in imts and "3.0d" in imts and "BACKLOG" not in imts
    assert lines.index(usda) < lines.index(imts), "oldest first"
    assert "gone" not in text
    assert sd.CSV_RETRY_DRAIN_NOT_RECORDED in text and "NOT RECORDED" in sd.CSV_RETRY_DRAIN_NOT_RECORDED
    assert sd.csv_retry_lines({}, now, health.csv_retry_backlog_of, 7.0)[0].startswith("  csv retry queue: empty")


def _digest(tmp_path, monkeypatch, capsys, queue_rows=None):
    """send_digest.main() end to end with a temp state file (the giant-lane harness, plus a csv_retry_queue)."""
    from updater import send_digest as sd
    import core.status_mail as sm
    db = tmp_path / "state.db"
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, "
                "last_success_utc TEXT, last_error TEXT, last_attempt_utc TEXT)")
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                ("cnb", "ok", "2026-11-01", "2026-11-01T00:00:00+00:00", None, "2026-11-01T00:00:00+00:00"))
    if queue_rows is not None:
        con.execute("CREATE TABLE csv_retry_queue(series_id TEXT PRIMARY KEY, source_id TEXT, enqueued_utc TEXT, "
                    "attempts INTEGER DEFAULT 0, last_error TEXT)")
        con.executemany("INSERT INTO csv_retry_queue VALUES (?,?,?,1,NULL)", queue_rows)
    con.commit()
    con.close()
    monkeypatch.setattr(sd, "STATE", str(db))
    sent = {}

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"
    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "success")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    sd.main()
    capsys.readouterr()
    assert sent, "the report was not handed to the mail module"
    return sent


def test_both_digest_bodies_carry_the_queue_section(tmp_path, monkeypatch, capsys):
    import html as H
    import re
    old = _iso(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=40))
    new = _iso(dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1))
    rows = [(f"usda:S{i}", "usda", old) for i in range(3)] + [("wid:S0", "wid", new)]
    sent = _digest(tmp_path, monkeypatch, capsys, rows)
    for name, body in (("TEXT", sent["body"]), ("HTML", H.unescape(sent["html"]))):
        assert "4 ids in 2 source(s), oldest current enqueue 40 days" in body, name
        assert re.search(r"usda\s+3 ids\s+oldest enqueue\s+40\.0d\s+BACKLOG \(ATTENTION\)", body), name
        assert re.search(r"wid\s+1 ids\s+oldest enqueue\s+1\.0d", body), name
        assert "NOT RECORDED" in body, name
    assert "<b>csv retry queue</b>" in sent["html"]


def test_an_unreadable_queue_is_said_not_shown_empty(tmp_path, monkeypatch, capsys):
    sent = _digest(tmp_path, monkeypatch, capsys, queue_rows=None)      # a state file with no queue table
    assert "csv retry queue: could not be read (LookupError)" in sent["body"]
    assert "csv retry queue: empty" not in sent["body"]
    assert "could not be read" in sent["html"]


CLOUD = "cnb"                 # a real, live registry id with no run_location: the cloud gate judges it


def _gate_verdict(tmp_path, st):
    import json
    from updater import send_digest as sd
    p = tmp_path / "health.json"
    p.write_text(json.dumps(health.assess(store=st)), encoding="utf-8")
    return sd.gate_rotating(str(p), dt.datetime.now(dt.timezone.utc))


def _rotating_cloud(st, now):
    st.upsert_source(CLOUD, status="ok", last_success_utc=now.isoformat(), last_attempt_utc=now.isoformat())
    st.upsert_unit(CLOUD, "_all", status="partial", last_obs_date=now.date().isoformat(),
                   last_attempt_utc=now.isoformat(), last_success_utc=now.isoformat(), last_error=DEFERRAL)


def test_a_backlog_keeps_a_rotating_source_in_the_digests_calm_list(tmp_path):
    """Review AR-285 F4 / the owner's order of 2026-10-11: health holds the source at ATTENTION (never ROTATING), but
    the email classes do not change - its rows stay "in progress (normal)", not under "Needs attention"."""
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _rotating_cloud(st, now)
    calm, _t, off = _gate_verdict(tmp_path, st)
    assert off is None and CLOUD in calm, "precondition: a clean deferral is in the calm list"
    _queue(st, 3, _iso(now - dt.timedelta(days=30)), source=CLOUD)
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == CLOUD)
    assert row["health"] == "ATTENTION" and row["rotating_before_backlog"] is True and row["csv_retry_backlog_hold"]
    calm, _t, off = _gate_verdict(tmp_path, st)
    assert off is None and CLOUD in calm, "the backlog must not move a ROTATING source under Needs attention"


def test_another_hold_beside_the_backlog_still_leaves_the_calm_list(tmp_path):
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    _rotating_cloud(st, now)
    _queue(st, 3, _iso(now - dt.timedelta(days=30)), source=CLOUD)
    st.note_full_rederive_owed(CLOUD, vintage="v", note="n")
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == CLOUD)
    assert row["csv_retry_backlog_hold"] is False and row["rotating_before_backlog"] is False
    calm, _t, _off = _gate_verdict(tmp_path, st)
    assert CLOUD not in calm, "an owed re-derive is real trouble: it stays under Needs attention"


def test_an_ok_source_with_a_backlog_is_not_called_rotating(tmp_path):
    st = StateStore(path=":memory:")
    now = dt.datetime.now(dt.timezone.utc)
    st.upsert_source(CLOUD, status="ok", last_success_utc=now.isoformat(), last_attempt_utc=now.isoformat())
    st.upsert_unit(CLOUD, "_all", status="ok", last_obs_date=now.date().isoformat(), last_attempt_utc=now.isoformat(),
                   last_success_utc=now.isoformat())
    _queue(st, 3, _iso(now - dt.timedelta(days=30)), source=CLOUD)
    row = next(r for r in health.assess(store=st)["sources"] if r["source"] == CLOUD)
    assert row["health"] == "ATTENTION" and row["rotating_before_backlog"] is False
    assert CLOUD not in _gate_verdict(tmp_path, st)[0]


# ---- (3) + (4) the drain ----------------------------------------------------------------------------

class _Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


class _Store:
    def __init__(self, queued):
        self.queued = queued

    def csv_retry_sources(self):
        return dict(self.queued)

    def claim_lease(self, key, owner, ttl_s=3600):
        return True

    def release_lease(self, key, owner=None):
        pass


class _U:
    def __init__(self, sid):
        self.source_id, self.key = sid, f"{sid}/_all"


@pytest.fixture
def drain(monkeypatch):
    """_drain_leftover_retries on a virtual clock. By default the derive never empties a queue: it uses all the time
    it is given (deadline_ts) - the usda / wid shape. `fast` sources instead return at once with every id refailed.
    Records (source, minutes given, deadline), the mirror gate's (source, deadline) and each call's skip set."""
    clock = _Clock(20_000 * 86400 + 3600)
    calls, gates, skips = [], [], []
    fast: set = set()
    monkeypatch.delenv("AQUEDUCT_RETRY_DRAIN_PER_SOURCE_MIN", raising=False)
    monkeypatch.setattr(orchestrate, "time", clock)
    monkeypatch.setattr(orchestrate, "_here", lambda: "local")
    monkeypatch.setattr(orchestrate, "_served_by_lane", lambda sid: False)
    monkeypatch.setattr(orchestrate, "_leftover_drain_min", lambda: 45.0)
    monkeypatch.setattr(orchestrate, "_drain_margin_min", lambda: 15.0)
    monkeypatch.setattr(orchestrate, "_r2_mirror_gate",
                        lambda sid, d=None, cache=None: gates.append((sid, d)) or None)

    def fake(unit, blob, store, deadline_ts=None, id_gate=None, skip=None, cap=None):
        calls.append((unit.source_id, round((deadline_ts - clock.t) / 60.0, 6), deadline_ts))
        skips.append((unit.source_id, set(skip or ())))
        if unit.source_id in fast:                    # 3 ids, all refailed at once: nothing left untried
            clock.t += 1.0
            return {"queued": 3, "attempted": 3, "cleared": 0, "still_queued": 3, "held_back": 0,
                    "tried": ["x1", "x2", "x3"], "untried": 0}
        clock.t = deadline_ts
        return {"queued": 1, "attempted": 1, "cleared": 0, "still_queued": 1, "held_back": 0,
                "tried": [f"{unit.source_id}:t{len(calls)}"], "untried": 1}
    monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", fake)

    def run(queued, day=0, route=None):
        monkeypatch.setattr(orchestrate, "_drain_rotation_day", lambda: day)
        calls.clear()
        gates.clear()
        skips.clear()
        t0 = clock.t
        cands = {s: [_U(s)] for s in queued if route is None or s in route}
        orchestrate._drain_leftover_retries(cands, None, _Store(queued), route=route)
        run.used = (clock.t - t0) / 60.0
        return list(calls), list(gates)
    run.fast, run.skips = fast, skips
    return run


QUEUE = {"usda": (68_983, "2026-08-24T00:19:56+00:00"), "imf_imts_direct": (1_470, "2026-11-05T02:26:12+00:00")}
WID = {"wid": (39_230, "2026-10-18T01:12:58+00:00")}


def test_a_single_never_emptying_source_uses_the_whole_budget(drain):
    """Review AR-285 F1 (probe_idle.py): main used 45 of 45 min for wid alone, the first cut 15. Work-conserving."""
    calls, _g = drain(WID)
    assert [(s, m) for s, m, _d in calls] == [("wid", 15.0)] * 3 and drain.used == 45.0
    calls, _g = drain(dict(WID, **QUEUE), day=1)            # ring imf_imts_direct, usda, wid: usda first
    assert drain.used == 45.0, "two never-emptying sources: still the whole budget"


def test_a_never_emptying_first_source_cannot_starve_the_second(drain):
    calls, gates = drain(QUEUE, day=1)                    # ring imf_imts_direct, usda: day 1 puts usda first
    assert [(s, m) for s, m, _d in calls] == [("usda", 15.0), ("imf_imts_direct", 15.0), ("usda", 15.0)]
    # the mirror check gets the SAME per-source deadline as the derive, turn by turn
    assert [(s, d) for s, d in gates] == [(s, d) for s, _m, d in calls]


def test_a_later_round_skips_what_this_pass_already_tried(drain):
    drain(WID)
    assert [sk for _s, sk in drain.skips] == [set(), {"wid:t1"}, {"wid:t1", "wid:t2"}]


def test_a_source_that_refails_everything_fast_gets_no_second_round(drain):
    drain.fast.add("imf_imts_direct")
    calls, _g = drain(QUEUE, day=0)                       # imts first: 3 ids, all refailed in 1 s
    assert [s for s, _m, _d in calls] == ["imf_imts_direct", "usda", "usda", "usda"]
    assert [m for s, m, _d in calls if s == "usda"] == [15.0, 15.0, pytest.approx(14.983333, abs=1e-5)]


def test_the_share_is_configurable_and_never_unbounded(drain, monkeypatch):
    monkeypatch.setenv("AQUEDUCT_RETRY_DRAIN_PER_SOURCE_MIN", "20")
    assert [(s, m) for s, m, _d in drain(QUEUE, day=1)[0]] == [("usda", 20.0), ("imf_imts_direct", 20.0),
                                                                ("usda", 5.0)]
    for bad in ("0", "-3", "x"):
        monkeypatch.setenv("AQUEDUCT_RETRY_DRAIN_PER_SOURCE_MIN", bad)
        assert orchestrate._drain_per_source_min() == 15.0, bad


def test_the_pass_deadline_still_binds(drain, monkeypatch):
    monkeypatch.setattr(orchestrate, "_leftover_drain_min", lambda: 20.0)
    calls, _g = drain(dict(QUEUE, **WID), day=1)
    assert [(s, m) for s, m, _d in calls] == [("usda", 15.0), ("wid", 5.0)], "imts not reached: 0 min left"


def test_the_rotation_moves_the_head_each_day(drain):
    q = dict(QUEUE, **WID)                                # ring: imf_imts_direct, usda, wid
    heads = [drain(q, day=day)[0][0][0] for day in (0, 1, 2, 3)]
    assert heads == ["imf_imts_direct", "usda", "wid", "imf_imts_direct"]


def test_the_ring_is_this_passs_route_not_the_whole_queue(drain):
    """Review AR-285 F2: 13 of the 20 queued sources were cloud-routed. They must not take ring days, or the local
    source after them alphabetically holds the head. Route = usda, wid: the head alternates day by day."""
    cloud = {f"imf_{c}_direct": (100, "2026-10-25") for c in "abcdefghijklm"}    # 13 sources before usda, wid
    q = dict(cloud, usda=QUEUE["usda"], **WID)
    heads = [drain(q, day=d, route={"usda", "wid"})[0][0][0] for d in range(6)]
    assert heads == ["usda", "wid"] * 3
    assert orchestrate.drain_ring(q, {"usda", "wid"}) == ["usda", "wid"]
    assert orchestrate.drain_ring(q, {"usda", "wid"}, lane=("wid",)) == ["usda"]
    # negative control: the whole-queue ring (the first cut) keeps usda first on the 13 cloud days and more
    whole = [orchestrate.rotate_drain_order(["usda", "wid"], d, orchestrate.drain_ring(q))[0] for d in range(15)]
    assert whole.count("usda") >= 13


def test_rotate_drain_order():
    r = orchestrate.rotate_drain_order
    assert r([], 5) == [] and r([], 5, ["a"]) == []
    assert r(["a", "b", "c"], 0) == ["a", "b", "c"]
    assert r(["c", "a", "b"], 1) == ["b", "c", "a"]                     # no ring: the sorted candidates
    assert r(["a", "b", "c"], 20_409) == ["a", "b", "c"]                 # 20409 % 3 == 0
    ring = ["a", "b", "c", "d", "e"]
    assert r(["a", "c", "e"], 1, ring) == ["c", "e", "a"]               # pointer b (not a candidate): c is next
    assert r(["a", "c", "e"], 3, ring) == ["e", "a", "c"]
    for n in range(1, 8):
        order = [f"s{i}" for i in range(n)]
        assert {r(order, d)[0] for d in range(100, 100 + n)} == set(order), n
        assert all(sorted(r(order, d, order)) == order for d in range(n)), "a rotation drops or adds nothing"
    assert orchestrate.drain_ring({"a": (3, "x"), "b": (0, None), "c": (1, None)}) == ["a", "c"]
    assert orchestrate._drain_rotation_day() == int(orchestrate.time.time() // 86400)


def test_the_whole_class_a_steady_source_is_first_within_the_ring_length_whatever_the_others_do():
    """R1260 rule 1 (review F3): the candidate set changes from pass to pass. A source that is a candidate every day
    is FIRST at least once in every len(ring) consecutive days, whatever the other sources' membership does. A
    rotation of the candidate list itself (the first cut) fails this with the seeded membership below."""
    import random
    rnd = random.Random(20261117)
    ring = [f"s{i:02d}" for i in range(20)]
    steady = "s07"

    def firsts(rotate):
        out = []
        for day in range(400):
            cands = sorted([s for s in ring if s != steady and rnd.random() < 0.8] + [steady])
            out.append(rotate(cands, day)[0] == steady)
        return out

    def worst_gap(hits):
        gap, worst = 0, 0
        for h in hits:
            gap = 0 if h else gap + 1
            worst = max(worst, gap)
        return worst
    assert worst_gap(firsts(lambda c, d: orchestrate.rotate_drain_order(c, d, ring))) < len(ring)

    def candidate_list_rotation(c, d):
        k = d % len(c)
        return c[k:] + c[:k]
    rnd.seed(20261117)
    assert worst_gap(firsts(candidate_list_rotation)) >= len(ring), "negative control: the old rule had no bound"


def test_the_start_line_names_the_rotation(drain, capsys):
    drain(QUEUE, day=1)
    out = capsys.readouterr().out
    assert "at most 15 min per source, in rotation order from usda (UTC day 1): usda, imf_imts_direct" in out
    assert "csv retry drain round 2: 15.0 min left go to 2 source(s) that still hold ids" in out


# ---- (5) the mirror check runs a chunk at a time (review AR-285 F3) ---------------------------------

@pytest.fixture
def chunks(monkeypatch):
    """_drain_gated_chunks with a scripted gate and a scripted derive + booking on a virtual clock."""
    clock = _Clock(20_000 * 86400)
    rec = {"gate": [], "derive": []}
    monkeypatch.setattr(orchestrate, "time", clock)
    monkeypatch.delenv("AQUEDUCT_RETRY_GATE_CHUNK", raising=False)

    def run(n, *, hold=(), stop_after=None, gate_s=0.0, per_id_s=0.0, budget_min=15.0):
        rec["gate"].clear()
        rec["derive"].clear()
        held = set(hold)

        def gate(ids):
            rec["gate"].append(len(ids))
            clock.t += gate_s
            return [s for s in ids if s not in held], {s: "unproven: f" for s in ids if s in held}

        def book(unit, blob, store, ids, budget):
            rec["derive"].append(len(ids))
            clock.t += per_id_s * len(ids)
            stop = stop_after is not None and len(rec["derive"]) >= stop_after
            return list(ids), set(), stop, list(ids)
        monkeypatch.setattr(orchestrate, "_drain_derive_and_book", book)
        ids = [f"wid:S{i:05d}" for i in range(n)]
        rows = [{"series_id": s} for s in ids]
        summ = {"queued": n, "attempted": 0, "cleared": 0, "still_queued": 0, "held_back": 0, "tried": [],
                "untried": 0}
        return orchestrate._drain_gated_chunks(_U("wid"), None, None, rows, ids, clock.t + budget_min * 60.0, gate,
                                               summ)
    return run, rec


def test_the_check_follows_what_the_slice_derives_not_the_cap(chunks):
    run, rec = chunks
    s = run(20_000, stop_after=1)                         # the derive's budget ended in the first chunk
    assert rec["gate"] == [500] and rec["derive"] == [500], "20,000 ids were checked for a 500-id slice"
    assert s["attempted"] == 500 and s["untried"] == 19_500 and len(s["tried"]) == 500


def test_every_chunk_is_checked_then_derived_until_the_end(chunks):
    run, rec = chunks
    s = run(1_200)
    assert rec["gate"] == [500, 500, 200] and rec["derive"] == [500, 500, 200]
    assert s["attempted"] == s["cleared"] == 1_200 and s["still_queued"] == 0 and s["untried"] == 0


def test_a_held_head_chunk_does_not_stop_the_slice(chunks):
    """The R1842 fixed-head class: a first chunk the check holds back whole must not end the turn."""
    run, rec = chunks
    s = run(1_000, hold={f"wid:S{i:05d}" for i in range(500)})
    assert rec["gate"] == [500, 500] and rec["derive"] == [500]
    assert s["held_back"] == 500 and s["attempted"] == 500 and s["untried"] == 0


def test_the_deadline_ends_the_chunks_and_counts_the_rest_untried(chunks):
    run, rec = chunks
    s = run(5_000, per_id_s=1.0, budget_min=15.0)         # 900 s: one chunk of 500 (500 s), then 400 s left
    assert rec["derive"] == [500, 500] and s["attempted"] == 1_000 and s["untried"] == 4_000


def test_the_chunk_size_is_configurable_and_never_unbounded(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_RETRY_GATE_CHUNK", "50")
    assert orchestrate._gate_chunk() == 50
    for bad in ("0", "-1", "x"):
        monkeypatch.setenv("AQUEDUCT_RETRY_GATE_CHUNK", bad)
        assert orchestrate._gate_chunk() == 500, bad


def test_the_drain_takes_the_chunk_path_only_with_a_gate_and_a_deadline():
    import inspect
    src = inspect.getsource(orchestrate._drain_csv_retry_queue)
    assert "if id_gate is not None and deadline_ts is not None:" in src
    assert "return _drain_gated_chunks(unit, blob, store, _retry_rows, _retry_ids, deadline_ts, id_gate, _summary)" in src
