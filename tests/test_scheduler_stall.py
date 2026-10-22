"""tools/scheduler_stall.py: the workstation mails the owner when GitHub stops creating the scheduled updater runs
(review AR-362 of PR #185 - the CI writer gate fails closed, so such a stall blocks every desktop pass).

Planted stalls and planted non-stalls, each with a control that differs in ONE property, plus a replay of the real
scheduled-run history (tests/fixtures/scheduler_stall_history.json) that the rule must sit through silently."""
import datetime as dt
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import ci_writer_gate as gate  # noqa: E402
import guard_heartbeat as gh  # noqa: E402
import scheduler_stall as ss  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
ACTIVE = {"updater-daily.yml": "active", "updater-heavy.yml": "active"}
HISTORY = os.path.join(ROOT, "tests", "fixtures", "scheduler_stall_history.json")


def run(t: dt.datetime, event="schedule"):
    return {"createdAt": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": event, "status": "completed"}


def sched(daily_age_h, heavy_age_h, now=NOW):
    return {"updater-daily.yml": [run(now - dt.timedelta(hours=daily_age_h))],
            "updater-heavy.yml": [run(now - dt.timedelta(hours=heavy_age_h))]}


def stalled_names(v):
    return sorted(r["workflow"] for r in v["stalled"])


# ------------------------------------------------------------------ the rule
def test_the_limit_is_36_h_one_skipped_cron_plus_the_gates_credit_window():
    assert ss.STALL_AFTER == dt.timedelta(hours=36)
    assert ss.STALL_AFTER == 2 * gate.CRON_SPACING + gate.PENDING_LIMIT
    # the largest lag the gate credits to a cron, after one skipped cron, stays under the limit
    assert dt.timedelta(hours=24) + dt.timedelta(minutes=710) < ss.STALL_AFTER


def test_a_writer_with_no_scheduled_run_for_37_h_is_stalled_and_one_at_35_h_is_not():
    v = ss.assess(sched(37, 3), ACTIVE, NOW)
    assert stalled_names(v) == ["updater-daily.yml"]
    assert [r["workflow"] for r in v["ok"]] == ["updater-heavy.yml"]
    control = ss.assess(sched(35, 3), ACTIVE, NOW)
    assert control["stalled"] == [] and len(control["ok"]) == 2


def test_the_boundary_is_exclusive_at_exactly_36_h():
    assert ss.assess(sched(36, 1), ACTIVE, NOW)["stalled"] == []
    assert stalled_names(ss.assess(sched(36.02, 1), ACTIVE, NOW)) == ["updater-daily.yml"]


def test_a_fresh_manual_dispatch_does_not_hide_a_stalled_schedule():
    """A workflow_dispatch writes the state like a cron does, but it is not the scheduler creating runs."""
    s = sched(40, 2)
    s["updater-daily.yml"].append(run(NOW - dt.timedelta(minutes=5), event="workflow_dispatch"))
    assert stalled_names(ss.assess(s, ACTIVE, NOW)) == ["updater-daily.yml"]
    # control: the same fresh run as a SCHEDULED one clears it
    s["updater-daily.yml"][-1]["event"] = "schedule"
    assert ss.assess(s, ACTIVE, NOW)["stalled"] == []


def test_a_disabled_workflow_is_not_judged_because_the_gate_does_not_wait_for_it():
    states = dict(ACTIVE, **{"updater-heavy.yml": "disabled_manually"})
    v = ss.assess(sched(2, 100), states, NOW)
    assert v["stalled"] == [] and any("disabled_manually" in n for n in v["notes"])
    # control: the same history, the workflow active -> stalled
    assert stalled_names(ss.assess(sched(2, 100), ACTIVE, NOW)) == ["updater-heavy.yml"]


def test_a_workflow_missing_from_githubs_list_is_still_judged_like_the_gate_still_waits_for_it():
    v = ss.assess(sched(2, 100), {"updater-daily.yml": "active"}, NOW)
    assert stalled_names(v) == ["updater-heavy.yml"]
    assert any("not in GitHub's workflow list" in n for n in v["notes"])


def test_no_scheduled_run_listed_at_all_is_a_stall_with_no_age():
    v = ss.assess({"updater-daily.yml": [], "updater-heavy.yml": [run(NOW)]}, ACTIVE, NOW)
    assert v["stalled"] == [{"workflow": "updater-daily.yml", "newest": None, "age_h": None}]
    subject, text = ss.compose("stall", v, {}, NOW)
    assert "lists no scheduled" in subject and "lists NO scheduled run" in text


def _history():
    with open(HISTORY, encoding="utf-8") as fh:
        h = json.load(fh)
    return {wf: [ss._t(c) for c in v] for wf, v in h["runs"].items()}, ss._t(h["snapshot_utc"])


def _replay(runs, start, end, step=dt.timedelta(minutes=30)):
    """Evaluate the rule every `step` with only the runs created by then. Returns the instants it reads stalled."""
    hits, t = [], start
    while t <= end:
        s = {wf: [run(c) for c in v if c <= t] for wf, v in runs.items()}
        if ss.assess(s, ACTIVE, t)["stalled"]:
            hits.append(t)
        t += step
    return hits


def test_the_real_history_never_reads_stalled():
    """No false alarm over the measured history: the 85 days in which both writers ran (from 2026-07-28), including the 2026-08-31
    skips (gap 26.1 h) and the 710-min lag of 2026-08-28."""
    runs, snap = _history()
    start = max(v[0] for v in runs.values()) + dt.timedelta(minutes=1)
    assert len(runs["updater-daily.yml"]) > 150 and len(runs["updater-heavy.yml"]) > 150
    assert _replay(runs, start, snap) == []


def test_the_real_history_with_its_newest_runs_removed_reads_stalled_within_the_limit():
    """The same replay, with the scheduler planted dead at a real instant: every run after it removed. The rule must
    fire, and no later than 36 h after the last run it saw (plus one evaluation step)."""
    runs, snap = _history()
    cut = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
    dead = {wf: [c for c in v if c <= cut] for wf, v in runs.items()}
    last = max(c for v in dead.values() for c in v)
    hits = _replay(dead, cut, cut + dt.timedelta(hours=48))
    assert hits, "a scheduler dead for 48 h was never reported"
    newest_each = min(max(v) for v in dead.values())
    assert hits[0] - newest_each <= ss.STALL_AFTER + dt.timedelta(minutes=30)
    assert hits[0] - newest_each > ss.STALL_AFTER
    assert last <= cut


# ------------------------------------------------------------------ the state machine
STALL = {"stalled": [{"workflow": "updater-daily.yml", "newest": "2026-10-20T20:00:00Z", "age_h": 40.0}],
         "ok": [{"workflow": "updater-heavy.yml", "newest": "2026-10-22T10:00:00Z", "age_h": 2.0}], "notes": []}
OK = {"stalled": [], "ok": STALL["ok"] + [{"workflow": "updater-daily.yml", "newest": "2026-10-22T11:00:00Z",
                                         "age_h": 1.0}], "notes": []}


def test_a_stall_mails_on_its_second_sighting_not_its_first():
    s1, k1 = ss.step({}, STALL, None, NOW)
    assert k1 is None and s1["episode_since_utc"] == "2026-10-22T12:00:00Z"
    s2, k2 = ss.step(s1, STALL, None, NOW + dt.timedelta(minutes=30))
    assert k2 == "stall" and s2["alert_active"] is True
    # control: a first sighting followed by a clean evaluation mails nothing and closes the episode
    s3, k3 = ss.step(s1, OK, None, NOW + dt.timedelta(minutes=30))
    assert k3 is None and s3["episode_since_utc"] is None and not s3["alert_active"]


def test_a_stall_reminds_every_24_h_and_not_before():
    s, _ = ss.step({}, STALL, None, NOW)
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=30))
    assert k == "stall"
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(hours=24))
    assert k is None
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(hours=24, minutes=31))
    assert k == "remind"


def test_recovery_mails_once_after_an_alert_and_never_without_one():
    s, _ = ss.step({}, STALL, None, NOW)
    s, _ = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=30))
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(hours=1))
    assert k == "recovered" and not s["alert_active"] and s["episode_since_utc"] is None
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(hours=2))
    assert k is None
    assert ss.step({}, OK, None, NOW)[1] is None


def test_github_unreadable_for_more_than_36_h_mails_blind_once():
    s, k = ss.step({}, None, "RuntimeError: gh auth", NOW)
    assert k is None and s["unknown_since_utc"] == "2026-10-22T12:00:00Z"
    s, k = ss.step(s, None, "RuntimeError: gh auth", NOW + dt.timedelta(hours=35))
    assert k is None
    s, k = ss.step(s, None, "RuntimeError: gh auth", NOW + dt.timedelta(hours=36, minutes=30))
    assert k == "blind"
    s, k = ss.step(s, None, "RuntimeError: gh auth", NOW + dt.timedelta(hours=40))
    assert k is None, "the blind mail is sent once per blind spell"
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(hours=41))
    assert s["unknown_since_utc"] is None and s["blind_alerted"] is False


def test_an_unreadable_spell_does_not_close_an_open_stall_episode():
    s, _ = ss.step({}, STALL, None, NOW)
    s, _ = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=30))
    s, k = ss.step(s, None, "TimeoutExpired", NOW + dt.timedelta(hours=1))
    assert k is None and s["alert_active"] is True


# ------------------------------------------------------------------ one tick, as the guard loop runs it
class Mailer:
    def __init__(self, result="cloudflare"):
        self.result, self.sent = result, []

    def __call__(self, subject, text):
        self.sent.append((subject, text))
        return self.result


def _tick(tmp_path, verdict_sched, now, mailer, states=ACTIVE, cut=False):
    return ss.tick(now=now, read=lambda: (verdict_sched, states), send=mailer,
                   path=str(tmp_path / "logs" / "scheduler_stall.json"), cut_over=lambda: cut)


def test_tick_mails_a_planted_stall_on_the_second_evaluation_and_records_it(tmp_path):
    m = Mailer()
    _tick(tmp_path, sched(40, 2), NOW, m)
    assert m.sent == []
    out = _tick(tmp_path, sched(40.5, 2.5), NOW + dt.timedelta(minutes=30), m)
    assert len(m.sent) == 1 and "stall mail sent (cloudflare)" in out
    subject, text = m.sent[0]
    assert "desktop pass is blocked" in subject and "updater-daily.yml" in text and "updater-heavy.yml" in text
    st = json.loads((tmp_path / "logs" / "scheduler_stall.json").read_text())
    assert st["alert_active"] is True and st["last_verdict"] == "stalled"
    # the next evaluation inside 24 h sends nothing
    _tick(tmp_path, sched(41.5, 3.5), NOW + dt.timedelta(hours=1, minutes=30), m)
    assert len(m.sent) == 1


def test_tick_sends_nothing_for_a_planted_non_stall(tmp_path):
    m = Mailer()
    for i in range(6):
        _tick(tmp_path, sched(30, 20), NOW + dt.timedelta(minutes=30 * i), m)
    assert m.sent == []


def test_a_mail_that_was_not_delivered_is_retried_at_the_next_evaluation(tmp_path):
    for failed in ("skipped", "failed"):
        d = tmp_path / failed
        m = Mailer(failed)
        _tick(d, sched(40, 2), NOW, m)
        out = _tick(d, sched(40, 2), NOW + dt.timedelta(minutes=30), m)
        assert len(m.sent) == 1 and "NOT sent" in out
        st = json.loads((d / "logs" / "scheduler_stall.json").read_text())
        assert not st.get("alert_active"), "an undelivered mail must not be recorded as sent"
        m.result = "resend"
        _tick(d, sched(40, 2), NOW + dt.timedelta(minutes=60), m)
        assert len(m.sent) == 2 and "STILL" not in m.sent[1][0], "the retry is the onset mail, not a reminder"
        assert json.loads((d / "logs" / "scheduler_stall.json").read_text())["alert_active"] is True


def test_tick_evaluates_at_most_every_30_min(tmp_path):
    calls = []

    def read():
        calls.append(1)
        return sched(1, 1), ACTIVE
    p = str(tmp_path / "s.json")
    ss.tick(now=NOW, read=read, send=Mailer(), path=p, cut_over=lambda: False)
    ss.tick(now=NOW + dt.timedelta(minutes=5), read=read, send=Mailer(), path=p, cut_over=lambda: False)
    ss.tick(now=NOW + dt.timedelta(minutes=29), read=read, send=Mailer(), path=p, cut_over=lambda: False)
    assert len(calls) == 1
    ss.tick(now=NOW + dt.timedelta(minutes=30), read=read, send=Mailer(), path=p, cut_over=lambda: False)
    assert len(calls) == 2
    # a clock that moved back does not silence it
    ss.tick(now=NOW - dt.timedelta(hours=2), read=read, send=Mailer(), path=p, cut_over=lambda: False)
    assert len(calls) == 3


def test_a_state_that_cannot_be_written_mails_nothing(tmp_path, monkeypatch):
    m = Mailer()
    p = str(tmp_path / "s.json")
    ss.tick(now=NOW, read=lambda: (sched(40, 2), ACTIVE), send=m, path=p, cut_over=lambda: False)

    def boom(*a, **k):
        raise PermissionError("denied")
    monkeypatch.setattr(ss, "save_state", boom)
    out = ss.tick(now=NOW + dt.timedelta(minutes=30), read=lambda: (sched(40, 2), ACTIVE), send=m, path=p,
                  cut_over=lambda: False)
    assert m.sent == [] and "nothing mailed" in out


def test_a_corrupt_state_file_starts_fresh_and_says_so(tmp_path):
    p = tmp_path / "s.json"
    p.write_text("{not json")
    out = ss.tick(now=NOW, read=lambda: (sched(1, 1), ACTIVE), send=Mailer(), path=str(p), cut_over=lambda: False)
    assert "unreadable" in out and json.loads(p.read_text())["last_verdict"] == "ok"


def test_github_unreadable_is_unknown_not_ok_and_not_stalled(tmp_path):
    def read():
        raise RuntimeError("gh run list failed: HTTP 401")
    p = str(tmp_path / "s.json")
    out = ss.tick(now=NOW, read=read, send=Mailer(), path=p, cut_over=lambda: False)
    assert "unknown" in out
    st = json.loads(open(p).read())
    assert st["last_verdict"] == "unknown" and "401" in st["last_error"]


def test_after_t0_nothing_is_read_and_nothing_is_mailed(tmp_path):
    m = Mailer()
    out = ss.tick(now=NOW, read=lambda: pytest.fail("GitHub read after T0"), send=m,
                  path=str(tmp_path / "s.json"), cut_over=lambda: True)
    assert "not applicable after T0" in out and m.sent == []


def test_tick_never_raises(tmp_path):
    out = ss.tick(now=NOW, read=lambda: (sched(1, 1), ACTIVE), send=Mailer(), path=str(tmp_path / "s.json"),
                  cut_over=lambda: (_ for _ in ()).throw(OSError("flag unreadable")))
    assert out.startswith("scheduler-stall: check failed")


def test_mail_goes_from_the_status_address_never_a_person(monkeypatch):
    seen = {}

    def fake(subject, text, html=None, *, sender, to, user_agent, log=print):
        seen.update(sender=sender, to=to)
        return "skipped"
    import core.status_mail as sm
    monkeypatch.setattr(sm, "send_status_mail", fake)
    monkeypatch.setattr("core.config.load_env", lambda *a, **k: {})
    monkeypatch.delenv("DIGEST_TO", raising=False)
    assert ss.send_mail("s", "t") == "skipped"
    assert seen["sender"] == "Econ Data Library <noreply@hfdatalibrary.com>"
    assert seen["to"] == "admin@hfdatalibrary.com"


def test_every_mail_kind_composes(monkeypatch):
    for kind, v in (("stall", STALL), ("remind", STALL), ("recovered", OK), ("blind", None)):
        subject, text = ss.compose(kind, v, {"episode_since_utc": "x", "unknown_since_utc": "y",
                                             "last_error": "e"}, NOW)
        assert subject.startswith("econ: ") and "tools/scheduler_stall.py" in text


# ------------------------------------------------------------------ the gate's fetchers carry the timeout
def test_the_gates_fetchers_pass_the_timeout_and_keep_60_s_by_default(monkeypatch):
    seen = []

    class R:
        returncode, stdout, stderr = 0, "[]", ""

    def fake_run(cmd, **kw):
        seen.append(kw["timeout"])
        return R()
    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    gate._gh_sched("updater-daily.yml")
    gate._gh_sched("updater-daily.yml", timeout=ss.GH_TIMEOUT_S)
    gate._gh_states(timeout=ss.GH_TIMEOUT_S)
    gate._gh_runs("updater-daily.yml")
    assert seen == [60, 20, 20, 60]


# ------------------------------------------------------------------ riding the guard loop
def _beat_rig(monkeypatch, order):
    class C:
        def put_object(self, **kw):
            order.append(("put", json.loads(kw["Body"].decode("utf-8"))))
    monkeypatch.setattr(gh, "_alive_jobs_detail", lambda: [])
    monkeypatch.setattr(gh, "_emptiness_verdict", lambda: {"ran": True, "fetch_without_write": 0})
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: C())
    monkeypatch.setattr(ss, "summary_for_beat", lambda path=ss.STATE_PATH: {"last_verdict": "ok"})


def test_publish_flag_runs_the_stall_check_after_the_beat_is_put(monkeypatch):
    order = []
    _beat_rig(monkeypatch, order)
    monkeypatch.setattr(ss, "tick", lambda *a, **k: order.append(("tick", None)) or "scheduler-stall: ok")
    monkeypatch.setattr(sys, "argv", ["guard_heartbeat.py", "--publish"])
    assert gh.main() == 0
    assert [o[0] for o in order] == ["put", "tick"]
    assert order[0][1]["scheduler_stall"] == {"last_verdict": "ok"}


def test_a_failing_stall_check_does_not_change_the_publish(monkeypatch, capsys):
    order = []
    _beat_rig(monkeypatch, order)

    def boom(*a, **k):
        raise RuntimeError("anything")
    monkeypatch.setattr(ss, "tick", boom)
    assert gh.publish(stall_check=True) == 0
    assert [o[0] for o in order] == ["put"]
    assert "could not run" in capsys.readouterr().out


def test_a_direct_publish_never_reaches_github(monkeypatch):
    order = []
    _beat_rig(monkeypatch, order)
    monkeypatch.setattr(ss, "tick", lambda *a, **k: pytest.fail("stall check ran without --publish"))
    assert gh.publish() == 0
    assert "scheduler_stall" not in order[0][1]


def test_the_cloud_check_prints_a_stalled_verdict_but_does_not_fail_on_it(monkeypatch, capsys):
    body = {"utc": dt.datetime.now(UTC).isoformat(), "host": "w", "jobs_alive": [], "tracked": [],
            "jobs_detail": [], "table_ok": True, "emptiness": {"ran": True, "fetch_without_write": 0},
            "scheduler_stall": {"last_verdict": "stalled", "stalled": ["updater-heavy.yml"],
                                "last_evaluated_utc": "2026-10-22T11:30:00Z", "alert_active": True}}

    class C:
        def get_object(self, **kw):
            import io
            return {"Body": io.BytesIO(json.dumps(body).encode())}
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: C())
    monkeypatch.setattr(gh, "_lane_problem", lambda *a, **k: None)
    assert gh.check(45) == 0
    out = capsys.readouterr().out
    assert "scheduler-stall check reads stalled" in out and "updater-heavy.yml" in out
