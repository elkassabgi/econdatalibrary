"""tools/scheduler_stall.py: the workstation mails the owner when GitHub stops creating the scheduled updater runs
(review AR-362 of PR #185 - the CI writer gate fails closed, so such a stall blocks every desktop pass).

Planted stalls and planted non-stalls, each with a control that differs in ONE property, plus a replay of the real
scheduled-run history (tests/fixtures/scheduler_stall_history.json) that the rule must sit through silently.
No test here can reach a mail service or GitHub: see the autouse fixture."""
import datetime as dt
import json
import os
import re
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import ci_writer_gate as gate  # noqa: E402
import guard_heartbeat as gh  # noqa: E402
import scheduler_stall as ss  # noqa: E402

_REAL_GH_JSON = gate._gh_json

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 22, 12, 0, tzinfo=UTC)
ACTIVE = {"updater-daily.yml": "active", "updater-heavy.yml": "active"}
HISTORY = os.path.join(ROOT, "tests", "fixtures", "scheduler_stall_history.json")
MAIL_OK = {"CLOUDFLARE_EMAIL_TOKEN": "t", "CLOUDFLARE_ACCOUNT_ID": "a", "RESEND_API_KEY": "", "DIGEST_TO": ""}


@pytest.fixture(autouse=True)
def _no_real_mail_and_no_real_env(monkeypatch, tmp_path):
    """The production checkout (tools/pretest.py) has a .env with real mail settings (R1210 class): point the
    module at a .env that does not exist, and make any real mail POST or gh call fail the test."""
    monkeypatch.setattr(ss, "ENV_PATH", str(tmp_path / "no-such" / ".env"))
    import core.status_mail as sm
    monkeypatch.setattr(sm, "_post", lambda *a, **k: pytest.fail("a test reached a real mail POST"))
    monkeypatch.setattr(gate, "_gh_json", lambda *a, **k: pytest.fail("a test reached gh"))


def run(t: dt.datetime, event="schedule", status="completed"):
    return {"createdAt": t.strftime("%Y-%m-%dT%H:%M:%SZ"), "event": event, "status": status}


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


def test_the_gates_writer_crons_match_the_workflow_files():
    """The rule's arithmetic (and the gate's) assumes these crons; a moved cron must fail here (review 12)."""
    for wf, hours in gate.WRITERS.items():
        with open(os.path.join(ROOT, ".github", "workflows", wf), encoding="utf-8") as fh:
            crons = re.findall(r"cron:\s*'([^']+)'", fh.read())
        got = sorted(int(c.split()[1]) for c in crons)
        assert got == sorted(hours), (wf, crons)
        assert all(c.split()[0] == "0" for c in crons)
        assert max(b - a for a, b in zip(got, got[1:] + [got[0] + 24])) * 3600 == gate.CRON_SPACING.total_seconds()


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


def test_the_newest_run_is_the_max_not_the_first_listed():
    s = {"updater-daily.yml": [run(NOW - dt.timedelta(hours=50)), run(NOW - dt.timedelta(hours=2))],
         "updater-heavy.yml": [run(NOW - dt.timedelta(hours=1))]}
    assert ss.assess(s, ACTIVE, NOW)["stalled"] == []


def test_an_unfinished_writer_run_older_than_the_limit_is_a_stall_although_crons_keep_firing():
    """Review finding 1: the gate blocks on every unfinished writer run, and GitHub keeps creating scheduled runs
    behind it as pending, so the newest createdAt stays fresh."""
    runs = {"updater-heavy.yml": [run(NOW - dt.timedelta(hours=40), status="in_progress"),
                                  run(NOW - dt.timedelta(hours=1), status="pending")]}
    v = ss.assess(sched(2, 1), ACTIVE, NOW, runs)
    assert stalled_names(v) == ["updater-heavy.yml"] and "still in_progress" in v["stalled"][0]["reason"]
    subject, _ = ss.compose("stall", v, {}, NOW)
    assert subject.endswith("updater-heavy.yml - a run unfinished for 40 h")
    # control: the same run 20 h old (heavy runs last up to 23.6 h) is not
    runs["updater-heavy.yml"][0] = run(NOW - dt.timedelta(hours=20), status="in_progress")
    assert ss.assess(sched(2, 1), ACTIVE, NOW, runs)["stalled"] == []


def test_completed_runs_older_than_the_limit_are_not_stuck():
    """Review finding 2: _gh_runs returns the last 50 runs, nearly all completed and older than 36 h. Only an
    UNFINISHED run is stuck; every completed one is the excluded population and must stay excluded."""
    runs = {wf: [run(NOW - dt.timedelta(hours=h)) for h in (40, 60, 100)]
            + [run(NOW - dt.timedelta(hours=50), event="workflow_dispatch")] for wf in gate.WRITERS}
    assert ss.assess(sched(2, 1), ACTIVE, NOW, runs)["stalled"] == []


def test_a_disabled_workflow_is_not_judged_because_the_gate_does_not_wait_for_it():
    states = dict(ACTIVE, **{"updater-heavy.yml": "disabled_manually"})
    v = ss.assess(sched(2, 100), states, NOW)
    assert v["stalled"] == [] and any("disabled_manually" in n for n in v["notes"])
    empty = {"updater-daily.yml": [run(NOW)], "updater-heavy.yml": []}
    assert ss.assess(empty, states, NOW)["stalled"] == [], "disabled + nothing listed is still not judged"
    # control: the same history, the workflow active -> stalled
    assert stalled_names(ss.assess(sched(2, 100), ACTIVE, NOW)) == ["updater-heavy.yml"]


def test_a_workflow_missing_from_githubs_list_is_still_judged_like_the_gate_still_waits_for_it():
    v = ss.assess(sched(2, 100), {"updater-daily.yml": "active"}, NOW)
    assert stalled_names(v) == ["updater-heavy.yml"]
    assert any("not in GitHub's workflow list" in n for n in v["notes"])


def test_no_scheduled_run_listed_at_all_is_a_stall_with_no_age():
    v = ss.assess({"updater-daily.yml": [], "updater-heavy.yml": [run(NOW)]}, ACTIVE, NOW)
    assert stalled_names(v) == ["updater-daily.yml"] and v["stalled"][0]["age_h"] is None
    subject, text = ss.compose("stall", v, {}, NOW)
    assert "no scheduled run listed" in subject and "lists NO scheduled run" in text


def test_a_newest_run_in_this_machines_future_is_clock_skew_not_fresh():
    with pytest.raises(ss.ClockSkew):
        ss.assess(sched(-1, 1), ACTIVE, NOW)
    assert ss.assess(sched(-0.1, 1), ACTIVE, NOW)["stalled"] == []    # 6 min ahead: within the slack


def _history():
    with open(HISTORY, encoding="utf-8") as fh:
        h = json.load(fh)
    return {wf: [ss._t(c) for c in v] for wf, v in h["runs"].items()}, ss._t(h["snapshot_utc"])


def _replay(runs, start, end, step=dt.timedelta(minutes=30)):
    """Evaluate the rule every `step` with only the runs created by then. Returns the instants it reads stalled."""
    hits, t = [], start
    while t <= end:
        s = {wf: [run(c) for c in v if c <= t] for wf, v in runs.items()}
        # the same runs as the unfiltered list too (all completed, as they are in GitHub's listing by now)
        if ss.assess(s, ACTIVE, t, {wf: v[-50:] for wf, v in s.items()})["stalled"]:
            hits.append(t)
        t += step
    return hits


def test_the_real_history_never_reads_stalled():
    """No false alarm over the measured history: the 85 days in which both writers ran (from 2026-07-28),
    including the 2026-08-31 skips (gap 26.1 h) and the 710-min lag of 2026-08-28."""
    runs, snap = _history()
    start = max(v[0] for v in runs.values()) + dt.timedelta(minutes=1)
    assert len(runs["updater-daily.yml"]) > 150 and len(runs["updater-heavy.yml"]) > 150
    assert _replay(runs, start, snap) == []


def test_the_real_history_with_its_newest_runs_removed_reads_stalled_within_the_limit():
    """The same replay, with the scheduler planted dead at a real instant: every run after it removed. The rule must
    fire, after 36 h and no later than one evaluation step past it."""
    runs, _snap = _history()
    cut = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
    dead = {wf: [c for c in v if c <= cut] for wf, v in runs.items()}
    hits = _replay(dead, cut, cut + dt.timedelta(hours=48))
    assert hits, "a scheduler dead for 48 h was never reported"
    oldest_newest = min(max(v) for v in dead.values())
    assert ss.STALL_AFTER < hits[0] - oldest_newest <= ss.STALL_AFTER + dt.timedelta(minutes=30)


# ------------------------------------------------------------------ the state machine
STALL = {"stalled": [{"workflow": "updater-daily.yml", "newest": "2026-10-20T20:00:00Z", "age_h": 40.0,
                      "reason": "newest scheduled run created 2026-10-20T20:00:00Z (40.0 h ago)"}],
         "ok": [{"workflow": "updater-heavy.yml", "newest": "2026-10-22T10:00:00Z", "age_h": 2.0,
                 "reason": "newest scheduled run created 2026-10-22T10:00:00Z (2.0 h ago)"}], "notes": []}
OK = {"stalled": [], "ok": STALL["ok"] + [{"workflow": "updater-daily.yml", "newest": "2026-10-22T11:00:00Z",
                                         "age_h": 1.0, "reason": "fresh"}], "notes": []}


def _deliver(s, kind, now):
    return ss.on_delivered(s, kind, now) if kind else s


def test_a_stall_comes_due_on_its_second_sighting_not_its_first():
    s1, k1 = ss.step({}, STALL, None, NOW)
    assert k1 is None and s1["episode_since_utc"] == "2026-10-22T12:00:00Z"
    s2, k2 = ss.step(s1, STALL, None, NOW + dt.timedelta(minutes=30))
    assert k2 == "stall"
    # control: a first sighting followed by a clean evaluation is due nothing and closes the episode
    s3, k3 = ss.step(s1, OK, None, NOW + dt.timedelta(minutes=30))
    assert k3 is None and s3["episode_since_utc"] is None and not s3.get("alert_active")


def test_an_undelivered_stall_keeps_coming_due():
    s, _ = ss.step({}, STALL, None, NOW)
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=30))
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=60))
    assert k == "stall", "not delivered, so still due"


def test_a_delivered_stall_reminds_every_24_h_and_not_before():
    s, _ = ss.step({}, STALL, None, NOW)
    t = NOW + dt.timedelta(minutes=30)
    s, k = ss.step(s, STALL, None, t)
    s = _deliver(s, k, t)
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(hours=24))
    assert k is None
    s, k = ss.step(s, STALL, None, t + dt.timedelta(hours=24))
    assert k == "remind"


def test_recovery_comes_due_once_after_a_delivered_alert_and_never_without_one():
    s, _ = ss.step({}, STALL, None, NOW)
    t = NOW + dt.timedelta(minutes=30)
    s, k = ss.step(s, STALL, None, t)
    s = _deliver(s, k, t)
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(hours=1))
    assert k == "recovered"
    s = _deliver(s, k, NOW + dt.timedelta(hours=1))
    assert not s["alert_active"] and s["episode_since_utc"] is None
    assert ss.step(s, OK, None, NOW + dt.timedelta(hours=2))[1] is None
    assert ss.step({}, OK, None, NOW)[1] is None


def test_github_unreadable_for_more_than_36_h_comes_due_blind_once():
    s, k = ss.step({}, None, "RuntimeError: gh auth", NOW)
    assert k is None and s["unknown_since_utc"] == "2026-10-22T12:00:00Z"
    s, k = ss.step(s, None, "e", NOW + dt.timedelta(hours=35))
    assert k is None
    t = NOW + dt.timedelta(hours=36, minutes=30)
    s, k = ss.step(s, None, "e", t)
    assert k == "blind"
    s = _deliver(s, k, t)
    s, k = ss.step(s, None, "e", NOW + dt.timedelta(hours=40))
    assert k is None, "the blind mail is sent once per blind spell"
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(hours=41))
    assert s["unknown_since_utc"] is None and s["blind_alerted"] is False


def test_future_stored_instants_are_discarded_not_waited_for():
    """Review finding 6: a corrected clock must not freeze the reminder or the blind clock."""
    s = {"alert_active": True, "episode_since_utc": "2026-10-20T00:00:00Z", "last_mail_utc": "2027-01-01T00:00:00Z"}
    assert ss.step(s, STALL, None, NOW)[1] == "remind"
    s, k = ss.step({"unknown_since_utc": "2027-01-01T00:00:00Z"}, None, "e", NOW)
    assert k is None and s["unknown_since_utc"] == "2026-10-22T12:00:00Z", "restarted at now"


def test_an_unreadable_spell_does_not_close_an_open_stall_episode():
    s, _ = ss.step({}, STALL, None, NOW)
    t = NOW + dt.timedelta(minutes=30)
    s, k = ss.step(s, STALL, None, t)
    s = _deliver(s, k, t)
    s, k = ss.step(s, None, "TimeoutExpired", NOW + dt.timedelta(hours=1))
    assert k is None and s["alert_active"] is True


# ------------------------------------------------------------------ one tick, as the guard loop runs it
class Mailer:
    def __init__(self, result="cloudflare"):
        self.result, self.sent = result, []

    def __call__(self, subject, text):
        self.sent.append((subject, text))
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


class Rig:
    def __init__(self, tmp_path, mailer=None, env=None):
        self.path = str(tmp_path / "logs" / "scheduler_stall.json")
        self.mailer = mailer or Mailer()
        self.env = MAIL_OK if env is None else env
        self.reads = 0

    def tick(self, now, s=None, states=ACTIVE, runs=None, started=None, cut=False, read=None):
        def default_read(budget_end):
            self.reads += 1
            return (s if s is not None else sched(40, 2, now)), (runs or {}), states
        return ss.tick(now=now, started=time.monotonic() if started is None else started, read=read or default_read,
                       send=self.mailer, path=self.path, cut_over=lambda: cut, env=self.env)

    def state(self):
        with open(self.path, encoding="utf-8") as fh:
            return json.load(fh)


def _confirmed_stall(rig, t=NOW):
    """Two evaluations 30 min apart that see the same stall: the second decides the mail and sends it."""
    rig.tick(t)
    assert rig.mailer.sent == [], "one sighting mails nothing"
    out = rig.tick(t + dt.timedelta(minutes=30))
    assert "stall mail due" in out
    return t + dt.timedelta(minutes=30)


def test_a_planted_stall_is_mailed_once_when_it_is_confirmed(tmp_path):
    rig = Rig(tmp_path)
    t = _confirmed_stall(rig)
    assert len(rig.mailer.sent) == 1
    subject, text = rig.mailer.sent[0]
    assert subject == "[econ] scheduler stall: updater-daily.yml - no scheduled run for 40 h"
    assert "updater-heavy.yml" in text
    st = rig.state()
    assert st["alert_active"] is True and "mail_due" not in st and st["last_send"]["result"] == "cloudflare"
    assert isinstance(st["last_step_elapsed_s"], float)
    for i in range(1, 12):         # the next five and a half hours: nothing more
        rig.tick(t + dt.timedelta(minutes=30 * i))
    assert len(rig.mailer.sent) == 1


def test_a_planted_non_stall_mails_nothing(tmp_path):
    rig = Rig(tmp_path)
    for i in range(12):
        t = NOW + dt.timedelta(minutes=30 * i)
        rig.tick(t, sched(30, 20, t))
    assert rig.mailer.sent == [] and rig.state()["last_verdict"] == "ok"


def test_an_undelivered_mail_stays_due_and_is_retried_every_30_min_not_every_tick(tmp_path):
    for failed in ("skipped", "failed"):
        rig = Rig(tmp_path / failed, Mailer(failed))
        t = _confirmed_stall(rig)
        assert len(rig.mailer.sent) == 1
        st = rig.state()
        assert not st.get("alert_active") and st["mail_due"]["kind"] == "stall"
        for m in (5, 10, 20, 25):
            rig.tick(t + dt.timedelta(minutes=m))
        assert len(rig.mailer.sent) == 1, "retried at most every 30 min, not every 5"
        rig.mailer.result = "resend"
        rig.tick(t + dt.timedelta(minutes=31))
        assert len(rig.mailer.sent) == 2 and "(reminder)" not in rig.mailer.sent[1][0]
        assert rig.state()["alert_active"] is True


def test_an_undelivered_mail_is_tried_at_most_4_times_a_day(tmp_path):
    """Review finding 6: a Cloudflare timeout after it accepted the mail reads as not delivered."""
    rig = Rig(tmp_path, Mailer("skipped"))
    t = _confirmed_stall(rig)
    for i in range(1, 30):
        rig.tick(t + dt.timedelta(minutes=31 * i))
    assert len(rig.mailer.sent) == 4
    rig.tick(t + dt.timedelta(hours=24, minutes=5))
    assert len(rig.mailer.sent) == 5, "the next day it tries again"


def test_a_kill_during_the_send_cannot_repeat_the_mail_every_tick_and_does_not_stop_the_evaluation(tmp_path):
    """Review finding 3: the loop kills the publish at 90 s. A kill after the mail is accepted and before the result
    is recorded leaves an attempt with no result; the next ticks hold it, and keep evaluating."""
    rig = Rig(tmp_path, Mailer(KeyboardInterrupt()))
    rig.tick(NOW)
    with pytest.raises(KeyboardInterrupt):
        rig.tick(NOW + dt.timedelta(minutes=30))
    t = NOW + dt.timedelta(minutes=30)
    assert len(rig.mailer.sent) == 1 and rig.state()["mail_attempt"]["kind"] == "stall"
    rig.mailer.result = "cloudflare"
    reads = rig.reads
    for m in (35, 60, 120, 300):
        out = rig.tick(t + dt.timedelta(minutes=m))
        assert "never reported back" in out
    assert len(rig.mailer.sent) == 1
    assert rig.reads == reads + 3, "the hold-off holds the send, not the evaluation"
    out = rig.tick(t + dt.timedelta(hours=6, minutes=10))
    assert "stall mail sent" in out and len(rig.mailer.sent) == 2, "after the hold-off: at most one duplicate"


def test_a_stall_that_ends_during_a_hold_off_sends_no_stale_stall_mail(tmp_path):
    rig = Rig(tmp_path, Mailer(KeyboardInterrupt()))
    rig.tick(NOW)
    with pytest.raises(KeyboardInterrupt):
        rig.tick(NOW + dt.timedelta(minutes=30))
    rig.mailer.result = "cloudflare"
    t = NOW + dt.timedelta(hours=2)
    out = rig.tick(t, sched(1, 1, t))               # the scheduler is back, inside the 6 h hold-off
    # the killed onset may have reached the owner, so he gets the "over" mail - never a stale "stall" one
    assert "recovered mail sent" in out and len(rig.mailer.sent) == 2
    assert rig.mailer.sent[1][0].startswith("[econ] scheduler stall over")
    st = rig.state()
    assert "mail_due" not in st and "mail_attempt" not in st
    for h in (3, 7, 12):
        rig.tick(NOW + dt.timedelta(hours=h), sched(1, 1, NOW + dt.timedelta(hours=h)))
    assert len(rig.mailer.sent) == 2


def test_a_blind_mail_does_not_replace_an_undelivered_stall_mail(tmp_path):
    """Review round 2, change 2."""
    rig = Rig(tmp_path, Mailer("failed"))
    t = _confirmed_stall(rig)

    def read(budget_end):
        raise TimeoutError("gh")
    for i in range(1, 80):                           # 40 h blind, retries failing
        rig.tick(t + dt.timedelta(minutes=30 * i), read=read)
    st = rig.state()
    assert st["unknown_since_utc"] and st["mail_due"]["kind"] == "stall"
    assert all("blind" not in s[0] for s in rig.mailer.sent)
    rig.mailer.result = "cloudflare"
    before = len(rig.mailer.sent)
    for i in range(1, 60):                           # the mail path is back; the daily cap paces the first retry
        rig.tick(t + dt.timedelta(hours=40, minutes=31 * i), read=read)
    delivered = [s[0] for s in rig.mailer.sent[before:]]
    assert len(delivered) == 2, delivered
    assert delivered[0].startswith("[econ] scheduler stall: "), "the stall mail goes first"
    assert "check blind" in delivered[1], "then the blind one"


def test_a_slow_tick_skips_the_evaluation_and_defers_the_send(tmp_path):
    rig = Rig(tmp_path)
    out = rig.tick(NOW, sched(1, 1), started=time.monotonic() - 60)       # 60 s of the publish gone
    assert "evaluation skipped" in out and rig.reads == 0
    rig2 = Rig(tmp_path / "b", Mailer("failed"))
    t = _confirmed_stall(rig2)
    rig2.mailer.result = "cloudflare"
    out = rig2.tick(t + dt.timedelta(minutes=31), started=time.monotonic() - 75)
    assert "deferred" in out and len(rig2.mailer.sent) == 1
    out = rig2.tick(t + dt.timedelta(minutes=36), started=time.monotonic() - 20)
    assert "stall mail sent" in out and len(rig2.mailer.sent) == 2


def test_the_send_budget_fits_the_measured_workstation_times():
    """Plan review 2026-10-22: the publish reached this step 24.5-29.3 s in under normal load (plus the PUT), and
    an evaluation's five gh calls took 17.2-17.6 s. Both must fit, with one POST after them, inside the 90 s kill."""
    reach, evaluation, one_post = 29.3 + 2.0, 17.6, 2.0
    assert reach <= ss.EVAL_START_MAX_S
    assert reach + evaluation <= ss.SEND_START_MAX_S
    assert ss.SEND_START_MAX_S + 15 <= 90 and ss.HARD_STOP_S < 90


def test_tick_evaluates_at_most_every_30_min(tmp_path):
    rig = Rig(tmp_path)
    for m in (0, 5, 29):
        rig.tick(NOW + dt.timedelta(minutes=m), sched(1, 1))
    assert rig.reads == 1
    rig.tick(NOW + dt.timedelta(minutes=30), sched(1, 1))
    assert rig.reads == 2
    rig.tick(NOW - dt.timedelta(hours=2), sched(1, 1))      # a clock that moved back does not silence it
    assert rig.reads == 3


def test_a_state_that_cannot_be_written_mails_nothing(tmp_path, monkeypatch):
    rig = Rig(tmp_path)
    rig.tick(NOW)

    def boom(*a, **k):
        raise PermissionError("denied")
    monkeypatch.setattr(ss, "save_state", boom)
    out = rig.tick(NOW + dt.timedelta(minutes=30))
    assert rig.mailer.sent == [] and "nothing mailed" in out


def test_a_second_loop_holding_the_lock_skips_this_tick(tmp_path):
    rig = Rig(tmp_path)
    held = ss._lock(rig.path)
    assert held is not None
    try:
        assert "holds the lock" in rig.tick(NOW, sched(1, 1)) and rig.reads == 0
    finally:
        ss._unlock(held)
    assert "ok" in rig.tick(NOW, sched(1, 1)) and rig.reads == 1
    again = ss._lock(rig.path)
    assert again is not None, "the step released its lock"
    ss._unlock(again)


def test_a_corrupt_state_file_starts_fresh_and_says_so(tmp_path):
    rig = Rig(tmp_path)
    os.makedirs(os.path.dirname(rig.path), exist_ok=True)
    with open(rig.path, "w") as fh:
        fh.write("{not json")
    assert ss.summary_for_beat(rig.path).get("unreadable")
    out = rig.tick(NOW, sched(1, 1))
    assert "unreadable" in out and rig.state()["last_verdict"] == "ok" and rig.state()["state_reset"]


def test_github_unreadable_or_clock_skew_is_unknown_not_ok_and_not_stalled(tmp_path):
    def read(budget_end):
        raise RuntimeError("gh run list failed: HTTP 401")
    rig = Rig(tmp_path)
    assert "unknown" in rig.tick(NOW, read=read)
    st = rig.state()
    assert st["last_verdict"] == "unknown" and "401" in st["last_error"]
    rig2 = Rig(tmp_path / "skew")
    assert "unknown" in rig2.tick(NOW, sched(-2, 1))
    assert "ClockSkew" in rig2.state()["last_error"]


def test_a_blind_evaluation_keeps_a_mail_already_due(tmp_path):
    rig = Rig(tmp_path, Mailer("failed"))
    t = _confirmed_stall(rig)
    called = []

    def read(budget_end):
        called.append(1)
        raise TimeoutError("gh")
    out = rig.tick(t + dt.timedelta(minutes=30), read=read)
    assert called and "unknown" in out and rig.state()["last_verdict"] == "unknown"
    assert rig.state()["mail_due"]["kind"] == "stall"
    assert len(rig.mailer.sent) == 2, "the kept mail is tried again"


def test_after_t0_nothing_is_read_and_nothing_is_mailed(tmp_path):
    rig = Rig(tmp_path)
    out = rig.tick(NOW, read=lambda b: pytest.fail("GitHub read after T0"), cut=True)
    assert "not applicable after T0" in out and rig.mailer.sent == []


def test_tick_never_raises_an_exception(tmp_path):
    out = ss.tick(now=NOW, read=lambda b: (sched(1, 1), {}, ACTIVE), send=Mailer(), path=str(tmp_path / "s.json"),
                  cut_over=lambda: (_ for _ in ()).throw(OSError("flag unreadable")))
    assert out.startswith("scheduler-stall: check failed")


def test_the_github_read_respects_the_time_left_in_the_tick(monkeypatch):
    seen = []
    monkeypatch.setattr(gate, "_gh_sched", lambda wf, timeout: seen.append(timeout) or [])
    monkeypatch.setattr(gate, "_gh_runs", lambda wf, timeout: seen.append(timeout) or [])
    monkeypatch.setattr(gate, "_gh_states", lambda timeout: seen.append(timeout) or {})
    ss._read_github(time.monotonic() + 300)
    assert seen == [20, 20, 20, 20, 20]
    seen.clear()
    ss._read_github(time.monotonic() + 9.5)
    assert seen and max(seen) <= 9
    with pytest.raises(TimeoutError):
        ss._read_github(time.monotonic() + 1)


# ------------------------------------------------------------------ mail settings and sender
def test_mail_settings_read_only_the_mail_keys_from_env_and_the_environment_wins(tmp_path, monkeypatch):
    p = tmp_path / ".env"
    p.write_text('R2_WRITE_SECRET_ACCESS_KEY=nope\nRESEND_API_KEY="r"\nDIGEST_TO=x@example.org\n'
                 "CLOUDFLARE_EMAIL_TOKEN=fromfile\n", encoding="utf-8")
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "fromenv")
    monkeypatch.delenv("R2_WRITE_SECRET_ACCESS_KEY", raising=False)
    env = ss.mail_env(str(p))
    assert env["RESEND_API_KEY"] == "r" and env["DIGEST_TO"] == "x@example.org"
    assert env["CLOUDFLARE_EMAIL_TOKEN"] == "fromenv"
    assert "R2_WRITE_SECRET_ACCESS_KEY" not in env and not os.environ.get("R2_WRITE_SECRET_ACCESS_KEY")
    assert ss.mail_setup(env) == {"mail_configured": True, "transport": "resend", "mail_to": "DIGEST_TO"}
    assert ss.mail_setup({k: "" for k in ss.MAIL_KEYS}) == {
        "mail_configured": False, "transport": "none", "mail_to": "default admin@hfdatalibrary.com"}


def test_mail_goes_from_the_status_address_never_a_person(monkeypatch):
    seen = {}

    def fake(subject, text, html=None, *, sender, to, user_agent, log=print):
        seen.update(sender=sender, to=to)
        return "skipped"
    import core.status_mail as sm
    monkeypatch.setattr(sm, "send_status_mail", fake)
    assert ss.send_mail("s", "t", {k: "" for k in ss.MAIL_KEYS}) == "skipped"
    assert seen == {"sender": "Econ Data Library <noreply@hfdatalibrary.com>", "to": "admin@hfdatalibrary.com"}


def test_every_mail_kind_composes_with_the_stable_prefix():
    for kind, v in (("stall", STALL), ("remind", STALL), ("recovered", OK), ("blind", None)):
        subject, text = ss.compose(kind, v, {"episode_since_utc": "x", "unknown_since_utc": "y",
                                             "last_error": "e"}, NOW)
        assert subject.startswith("[econ] scheduler stall") and "tools/scheduler_stall.py" in text


# ------------------------------------------------------------------ the gate's fetchers carry the timeout
def test_the_gates_fetchers_pass_the_timeout_and_keep_60_s_by_default(monkeypatch):
    seen = []

    class R:
        returncode, stdout, stderr = 0, "[]", ""

    def fake_run(cmd, **kw):
        seen.append(kw["timeout"])
        return R()
    monkeypatch.setattr(gate, "_gh_json", _REAL_GH_JSON)   # the real one, with subprocess faked below
    monkeypatch.setattr(gate.subprocess, "run", fake_run)
    gate._gh_sched("updater-daily.yml")
    gate._gh_sched("updater-daily.yml", timeout=ss.GH_TIMEOUT_S)
    gate._gh_states(timeout=7)
    gate._gh_runs("updater-daily.yml")
    gate._gh_runs("updater-daily.yml", timeout=5)
    assert seen == [60, 20, 7, 60, 5]


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
    monkeypatch.setattr(ss, "tick", lambda *a, **k: order.append(("tick", k.get("started"))) or "scheduler-stall: ok")
    monkeypatch.setattr(sys, "argv", ["guard_heartbeat.py", "--publish"])
    assert gh.main() == 0
    assert [o[0] for o in order] == ["put", "tick"]
    assert order[0][1]["scheduler_stall"] == {"last_verdict": "ok"}
    assert isinstance(order[1][1], float) and order[1][1] <= time.monotonic(), "the publish's start is passed"


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


def _check(monkeypatch, stall, present=True):
    body = {"utc": dt.datetime.now(UTC).isoformat(), "host": "w", "jobs_alive": [], "tracked": [],
            "jobs_detail": [], "table_ok": True, "emptiness": {"ran": True, "fetch_without_write": 0}}
    if present:
        body["scheduler_stall"] = stall

    class C:
        def get_object(self, **kw):
            import io
            return {"Body": io.BytesIO(json.dumps(body).encode())}
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: C())
    return gh.check(45)


def _armed(**kw):
    now = dt.datetime.now(UTC)
    s = {"last_evaluated_utc": ss._iso(now - dt.timedelta(minutes=20)), "last_verdict": "ok", "stalled": [],
         "alert_active": False, "unknown_since_utc": None, "mail_configured": True, "transport": "cloudflare",
         "mail_to": "DIGEST_TO", "last_send": None, "mail_due": None}
    s.update(kw)
    return s


def test_the_cloud_check_passes_an_armed_alarm_and_prints_a_stalled_verdict(monkeypatch, capsys):
    assert _check(monkeypatch, _armed()) == 0
    assert _check(monkeypatch, _armed(last_verdict="stalled", stalled=["updater-heavy.yml"], alert_active=True)) == 0
    out = capsys.readouterr().out
    assert "scheduler-stall check reads stalled" in out and "updater-heavy.yml" in out


def test_the_cloud_check_FAILS_when_the_alarm_is_not_armed(monkeypatch, capsys):
    """Review finding 5: the alarm can only be shown armed while the scheduler works - so this is where it fails."""
    now = dt.datetime.now(UTC)
    old = ss._iso(now - dt.timedelta(hours=7))
    for bad in (_armed(mail_configured=False), _armed(last_evaluated_utc=old), _armed(unknown_since_utc=old),
                _armed(mail_due={"kind": "stall", "since": old}), {"never_evaluated": True}, {"unreadable": "x"},
                "garbage"):
        assert _check(monkeypatch, bad) == 1, bad
        assert "SCHEDULER-STALL ALARM NOT ARMED" in capsys.readouterr().out
    # controls: a mail due for 1 h, GitHub unreadable for 1 h, after T0, and an older publisher - all pass
    recent = ss._iso(now - dt.timedelta(hours=1))
    for fine in (_armed(mail_due={"kind": "stall", "since": recent}), _armed(unknown_since_utc=recent),
                 {"not_applicable": "after T0"}):
        assert _check(monkeypatch, fine) == 0, fine
    assert _check(monkeypatch, None, present=False) == 0
    assert "predates the scheduler-stall check" in capsys.readouterr().out
