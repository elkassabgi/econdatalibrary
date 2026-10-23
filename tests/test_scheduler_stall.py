"""tools/scheduler_stall.py: the workstation warns the owner when GitHub stops creating the scheduled updater runs
(review AR-362 of PR #185 - the CI writer gate fails closed, so such a stall blocks every desktop pass).

Planted stalls and planted non-stalls, each with a control that differs in ONE property, plus a replay of the real
scheduled-run history (tests/fixtures/scheduler_stall_history.json) that the rule must sit through silently.
SHADOW MODE is the default (R1661): the shadow tests plant stalls and prove the send function is never called.
An empty scheduled-run listing is never a stall by itself (R1660): the empty-listing tests drive the gate's own
read_sched() through the module's real GitHub reader with faked gh calls.
No test here can reach a mail service or GitHub, or see the real send flag: see the autouse fixture."""
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
    module at a .env that does not exist, and make any real mail POST or gh call fail the test. The real send
    flag (logs/SCHEDULER_STALL_SEND on the checkout) may exist on the workstation, so the module's default flag
    path points at a file that does not exist either."""
    monkeypatch.setattr(ss, "ENV_PATH", str(tmp_path / "no-such" / ".env"))
    monkeypatch.setattr(ss, "SEND_FLAG", str(tmp_path / "no-such" / "SCHEDULER_STALL_SEND"))
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


def test_a_stuck_run_of_a_DISABLED_writer_is_a_stall_because_the_gate_still_blocks_on_it():
    """Review AR-366, required change 1: the gate's in-flight check ignores the workflow's state."""
    states = dict(ACTIVE, **{"updater-heavy.yml": "disabled_manually"})
    runs = {"updater-heavy.yml": [run(NOW - dt.timedelta(hours=48), event="workflow_dispatch", status="in_progress")]}
    gate_runs = {"updater-daily.yml": [run(NOW - dt.timedelta(hours=2))],
                 "updater-heavy.yml": runs["updater-heavy.yml"] + [run(NOW - dt.timedelta(hours=2))]}
    assert gate.assess(gate_runs, NOW, states, sched(2, 2))[0] is True, "the gate blocks on it"
    v = ss.assess(sched(2, 100), states, NOW, runs)
    assert stalled_names(v) == ["updater-heavy.yml"] and "disabled_manually" in v["stalled"][0]["reason"]
    # controls: the same disabled writer with that run 20 h old, or completed, is not stalled
    runs["updater-heavy.yml"][0] = run(NOW - dt.timedelta(hours=20), event="workflow_dispatch", status="in_progress")
    assert ss.assess(sched(2, 100), states, NOW, runs)["stalled"] == []
    runs["updater-heavy.yml"][0] = run(NOW - dt.timedelta(hours=48), event="workflow_dispatch")
    assert ss.assess(sched(2, 100), states, NOW, runs)["stalled"] == []


def test_a_workflow_missing_from_githubs_list_is_still_judged_like_the_gate_still_waits_for_it():
    v = ss.assess(sched(2, 100), {"updater-daily.yml": "active"}, NOW)
    assert stalled_names(v) == ["updater-heavy.yml"]
    assert any("not in GitHub's workflow list" in n for n in v["notes"])


def test_a_confirmed_empty_listing_is_UNDETERMINED_never_stalled_and_never_ok():
    """R1660 / R1663: what read_sched returns as [] (three empty reads, no scheduled row in the unfiltered
    listing) is a read fault. The writer is neither stalled nor ok - counting it as ok is what made AR-375's
    false 'recovered' (its mutant E5)."""
    s = {"updater-daily.yml": [], "updater-heavy.yml": [run(NOW)]}
    v = ss.assess(s, ACTIVE, NOW)
    assert v["stalled"] == [] and v["undetermined"] == ["updater-daily.yml"]
    assert [r["workflow"] for r in v["ok"]] == ["updater-heavy.yml"], "the empty writer is NOT in ok"
    assert any("UNDETERMINED" in n and "scheduled listing" in n for n in v["notes"])
    # an empty UNFILTERED listing (the stuck rule's input) is undetermined too, even with a fresh scheduled run
    v2 = ss.assess(sched(2, 1), ACTIVE, NOW, {"updater-daily.yml": [run(NOW)], "updater-heavy.yml": []})
    assert v2["undetermined"] == ["updater-heavy.yml"] and [r["workflow"] for r in v2["ok"]] == ["updater-daily.yml"]
    # but a stuck run in a non-empty unfiltered listing stalls the writer whatever its scheduled listing says
    stuck = {"updater-daily.yml": [run(NOW)], "updater-heavy.yml": [run(NOW - dt.timedelta(hours=40), status="queued")]}
    v3 = ss.assess({"updater-daily.yml": [run(NOW)], "updater-heavy.yml": []}, ACTIVE, NOW, stuck)
    assert stalled_names(v3) == ["updater-heavy.yml"] and v3["undetermined"] == []
    # and an age stall needs only the scheduled listing
    v4 = ss.assess(sched(2, 40), ACTIVE, NOW, {"updater-daily.yml": [run(NOW)], "updater-heavy.yml": []})
    assert stalled_names(v4) == ["updater-heavy.yml"]
    # controls: a disabled writer's empty scheduled listing is not judged at all; no unfiltered listing given
    # (runs=None) asks nothing of the stuck rule
    states = dict(ACTIVE, **{"updater-daily.yml": "disabled_manually"})
    off = ss.assess(s, states, NOW)
    assert off["stalled"] == [] and off["undetermined"] == []
    assert ss.assess(sched(2, 1), ACTIVE, NOW)["undetermined"] == []


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


HEAVY_STALL = {"stalled": [{"workflow": "updater-heavy.yml", "newest": None, "age_h": None,
                            "reason": "GitHub lists NO scheduled run"}],
               "ok": [{"workflow": "updater-daily.yml", "newest": "2026-10-22T11:00:00Z", "age_h": 1.0,
                       "reason": "fresh"}], "notes": []}


def test_two_different_writers_seen_once_each_confirm_nothing():
    """R1660's shape: daily read stalled at 14:57Z, heavy at 15:00Z. The confirmation is per writer."""
    s1, k1 = ss.step({}, STALL, None, NOW)
    s2, k2 = ss.step(s1, HEAVY_STALL, None, NOW + dt.timedelta(minutes=30))
    assert k1 is None and k2 is None
    s3, k3 = ss.step(s2, HEAVY_STALL, None, NOW + dt.timedelta(minutes=60))
    assert k3 == "stall", "the same writer on two evaluations in a row confirms"
    # a blind evaluation between two sightings of the same writer neither confirms nor resets
    b, _ = ss.step(s1, None, "TimeoutError", NOW + dt.timedelta(minutes=30))
    assert ss.step(b, STALL, None, NOW + dt.timedelta(minutes=60))[1] == "stall"


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
    assert k is None, "one clear evaluation does not confirm a recovery (R1663)"
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(minutes=90))
    assert k == "recovered"
    s = _deliver(s, k, NOW + dt.timedelta(minutes=90))
    assert not s["alert_active"] and s["episode_since_utc"] is None
    assert ss.step(s, OK, None, NOW + dt.timedelta(hours=2))[1] is None
    assert ss.step({}, OK, None, NOW)[1] is None


UND = {"stalled": [], "ok": [OK["ok"][0]], "undetermined": ["updater-daily.yml"], "notes": []}


def test_an_undetermined_evaluation_neither_recovers_nor_resets_and_breaks_the_clear_streak():
    s, _ = ss.step({}, STALL, None, NOW)
    s, k = ss.step(s, STALL, None, NOW + dt.timedelta(minutes=30))
    s = _deliver(s, k, NOW + dt.timedelta(minutes=30))
    s, k = ss.step(s, UND, None, NOW + dt.timedelta(minutes=60))
    assert k is None and s["alert_active"] and s["episode_since_utc"] and s["stalled"] == ["updater-daily.yml"]
    s, k = ss.step(s, OK, None, NOW + dt.timedelta(minutes=90))
    s, k2 = ss.step(s, UND, None, NOW + dt.timedelta(minutes=120))
    s, k3 = ss.step(s, OK, None, NOW + dt.timedelta(minutes=150))
    assert (k, k2, k3) == (None, None, None), "ok, undetermined, ok is not two clear evaluations in a row"
    assert ss.step(s, OK, None, NOW + dt.timedelta(minutes=180))[1] == "recovered"
    # before any alert: an undetermined writer that was stalled still confirms its stall on its next sighting
    s, _ = ss.step({}, STALL, None, NOW)
    s, k = ss.step(s, UND, None, NOW + dt.timedelta(minutes=30))
    assert k is None and s["episode_since_utc"] == "2026-10-22T12:00:00Z"
    assert ss.step(s, STALL, None, NOW + dt.timedelta(minutes=60))[1] == "stall"


def test_a_writer_undetermined_for_more_than_36_h_makes_one_blind_mail_due_never_a_stall():
    s, k = ss.step({}, UND, None, NOW)
    assert k is None and s["undetermined_since"] == {"updater-daily.yml": "2026-10-22T12:00:00Z"}
    t = NOW + dt.timedelta(hours=36, minutes=30)
    s, k = ss.step(s, UND, None, t)
    assert k == "blind"
    subject, text = ss.compose("blind", UND, s, t)
    assert "updater-daily.yml" in subject and "read empty" in subject and "read fault" in text
    s = _deliver(s, k, t)
    assert ss.step(s, UND, None, t + dt.timedelta(hours=1))[1] is None, "once per spell"
    s, _ = ss.step(s, OK, None, t + dt.timedelta(hours=2))
    assert s["undetermined_since"] == {} and s["blind_alerted"] is False


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
    """sending=True creates the send flag in tmp_path (the live path); sending=False leaves it absent (shadow)."""
    def __init__(self, tmp_path, mailer=None, env=None, sending=True):
        self.path = str(tmp_path / "logs" / "scheduler_stall.json")
        self.flag = str(tmp_path / "logs" / "SCHEDULER_STALL_SEND")
        os.makedirs(os.path.dirname(self.flag), exist_ok=True)
        if sending:
            with open(self.flag, "w", encoding="utf-8") as fh:
                fh.write("on\n")
        self.mailer = mailer or Mailer()
        self.env = MAIL_OK if env is None else env
        self.reads = 0

    def tick(self, now, s=None, states=ACTIVE, runs=None, started=None, cut=False, read=None):
        def default_read(budget_end):
            self.reads += 1
            return (s if s is not None else sched(40, 2, now)), runs, states
        return ss.tick(now=now, started=time.monotonic() if started is None else started, read=read or default_read,
                       send=self.mailer, path=self.path, cut_over=lambda: cut, env=self.env, flag=self.flag)

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
    assert "mail sent" not in out and len(rig.mailer.sent) == 1, "one clear evaluation confirms no recovery"
    t = NOW + dt.timedelta(hours=2, minutes=30)
    out = rig.tick(t, sched(1, 1, t))
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
    monkeypatch.setattr(gate, "_gh_sched", lambda wf, timeout: seen.append(timeout) or [run(NOW)])
    monkeypatch.setattr(gate, "_gh_runs", lambda wf, timeout: seen.append(timeout) or [run(NOW)])
    monkeypatch.setattr(gate, "_gh_states", lambda timeout: seen.append(timeout) or {})
    ss._read_github(time.monotonic() + 300, sleep=lambda s: None)
    assert seen == [20, 20, 20, 20, 20]
    seen.clear()
    ss._read_github(time.monotonic() + 9.5, sleep=lambda s: None)
    assert seen and max(seen) <= 9
    with pytest.raises(TimeoutError):
        ss._read_github(time.monotonic() + 1, sleep=lambda s: None)


def test_an_empty_listing_with_no_time_left_for_its_rereads_is_unknown_never_a_stall(monkeypatch, tmp_path):
    """The re-reads of an empty answer are bounded by the tick too: running out of time raises, which the tick
    books as UNKNOWN (blind), never as 'no scheduled run'."""
    monkeypatch.setattr(gate, "_gh_sched", lambda wf, timeout: [])
    monkeypatch.setattr(gate, "_gh_runs", lambda wf, timeout: [])
    monkeypatch.setattr(gate, "_gh_states", lambda timeout: dict(ACTIVE))
    rig = Rig(tmp_path, Mailer(), sending=False)
    # 3.5 s of budget: the first empty read fits, the real 2 s pause before the re-read leaves under 2 s
    out = rig.tick(NOW, read=lambda b: ss._read_github(time.monotonic() + 3.5))
    st = rig.state()
    assert "unknown" in out and st["last_verdict"] == "unknown" and "TimeoutError" in st["last_error"]
    assert st.get("undetermined") is None and st.get("stalled") is None


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


# ------------------------------------------------------------------ SHADOW MODE (R1661)
def _boom(*a, **k):
    pytest.fail("a send function was called in shadow mode")


def test_sending_is_on_only_when_the_flag_FILE_exists(tmp_path):
    flag = tmp_path / "SCHEDULER_STALL_SEND"
    assert ss.sending_enabled(str(flag)) is False, "absent = shadow"
    flag.mkdir()
    assert ss.sending_enabled(str(flag)) is False, "a directory is not the flag"
    flag.rmdir()
    flag.write_text("", encoding="utf-8")
    assert ss.sending_enabled(str(flag)) is True
    assert ss.SEND_FLAG.endswith(os.path.join("no-such", "SCHEDULER_STALL_SEND")), "tests never see the real flag"
    assert "logs/SCHEDULER_STALL_SEND" in ss.__doc__
    with open(os.path.join(ROOT, "tools", "scheduler_stall.py"), encoding="utf-8") as fh:
        src = fh.read()
    assert 'SEND_FLAG = os.path.join(ROOT, "logs", "SCHEDULER_STALL_SEND")' in src, "logs/ is gitignored: no pull sets it"
    with open(os.path.join(ROOT, ".gitignore"), encoding="utf-8") as fh:
        assert "logs/" in fh.read().split()


def test_shadow_mode_never_calls_a_send_function_even_with_a_planted_stall(tmp_path, monkeypatch):
    """The no-send proof: a confirmed planted stall, its daily reminders and its recovery, over three days of
    5-minute guard ticks, with the tick's DEFAULT send path, the module's send_mail and status_mail's sender all
    wired to fail the test. Every would-be mail is recorded instead, in order."""
    import core.status_mail as sm
    monkeypatch.setattr(sm, "send_status_mail", _boom)
    monkeypatch.setattr(ss, "send_mail", _boom)
    path, flag = str(tmp_path / "logs" / "scheduler_stall.json"), str(tmp_path / "logs" / "SCHEDULER_STALL_SEND")
    outs, t = [], NOW
    while t < NOW + dt.timedelta(hours=72):
        stalled_now = t < NOW + dt.timedelta(hours=60)
        s = sched(40, 2, t) if stalled_now else sched(1, 1, t)
        outs.append(ss.tick(now=t, started=time.monotonic(), read=lambda b, s=s: (s, None, ACTIVE), path=path,
                            cut_over=lambda: False, env=MAIL_OK, flag=flag))         # send= left at its default
        t += dt.timedelta(minutes=5)
    with open(path, encoding="utf-8") as fh:
        st = json.load(fh)
    assert not any("check failed" in o for o in outs), [o for o in outs if "check failed" in o][:2]
    assert st["mode"] == "shadow" and st["last_send"]["result"] == "shadow"
    kinds = [w["kind"] for w in st["would_mail"]]
    assert kinds == ["stall", "remind", "remind", "recovered"], kinds
    assert st["would_mail_count"] == 4 and "mail_due" not in st and st["alert_active"] is False
    assert st["would_mail"][0]["subject"] == "[econ] scheduler stall: updater-daily.yml - no scheduled run for 40 h"
    assert st["would_mail"][0]["at"] == "2026-10-22T12:30:00Z", "recorded at the confirming evaluation"
    assert sum("SHADOW: stall mail would be sent now, NOT sent" in o for o in outs) == 1
    # and an explicit send callable is not called either
    rig = Rig(tmp_path / "explicit", Mailer(), sending=False)
    rig.mailer = _boom
    _confirmed_stall_shadow(rig)
    assert rig.state()["would_mail"][0]["kind"] == "stall"


def _confirmed_stall_shadow(rig, t=NOW):
    rig.tick(t)
    out = rig.tick(t + dt.timedelta(minutes=30))
    assert "SHADOW: stall mail would be sent now" in out, out
    return t + dt.timedelta(minutes=30)


def test_the_flag_turns_sending_on_and_deleting_it_is_the_off_switch(tmp_path):
    rig = Rig(tmp_path, Mailer(), sending=True)
    t = _confirmed_stall(rig)
    assert len(rig.mailer.sent) == 1 and rig.state()["mode"] == "send"
    os.remove(rig.flag)                                         # the owner's off switch: no commit, no CI
    t2 = t + dt.timedelta(hours=24, minutes=1)
    out = rig.tick(t2)
    assert "SHADOW: remind mail would be sent now, NOT sent" in out and len(rig.mailer.sent) == 1
    assert rig.state()["mode"] == "shadow"


def test_a_shadow_would_be_mail_keeps_the_send_window(tmp_path):
    """The shadow log must say when the live alarm would REALLY have sent: a slow tick defers it there too."""
    class Clock:                     # the module's monotonic clock, moved by the slow read below
        v = 1000.0

        @classmethod
        def monotonic(cls):
            return cls.v

    import types
    rig = Rig(tmp_path, Mailer(), sending=False)
    ss_time = ss.time
    try:
        ss.time = types.SimpleNamespace(monotonic=Clock.monotonic, sleep=lambda s: None)
        rig.tick(NOW, started=Clock.v)

        def slow_read(budget_end):
            Clock.v += 25                                      # the evaluation's gh calls take 25 s
            return sched(40, 2, NOW), {}, ACTIVE
        out = rig.tick(NOW + dt.timedelta(minutes=30), started=Clock.v - 50, read=slow_read)
        assert "stall mail due" in out and "SHADOW: stall mail due, deferred (75 s" in out, out
        assert not rig.state().get("would_mail")
        out = rig.tick(NOW + dt.timedelta(minutes=35), started=Clock.v)
        assert "would be sent now" in out and rig.state()["would_mail"][0]["at"] == "2026-10-22T12:35:00Z"
    finally:
        ss.time = ss_time


# ------------------------------------------------------------------ the real reader: an empty listing (R1660)
class FakeGH:
    """gh, faked at the gate's fetchers, so the module's real _read_github -> gate.read_sched path runs.
    sched[wf]: a list of answers for the --event schedule call (the last one repeats); runs[wf]: the unfiltered
    listing - one answer (a list of run dicts, or a callable), or a list of answers whose first item is itself a
    list or a callable (the last one repeats). Counts every call."""
    def __init__(self, monkeypatch, sched, runs, states=None):
        self.sched, self.runs, self.states = sched, runs, states or dict(ACTIVE)
        self.calls = {"sched": {wf: 0 for wf in gate.WRITERS}, "runs": {wf: 0 for wf in gate.WRITERS}, "states": 0}
        monkeypatch.setattr(gate, "_gh_sched", self._sched)
        monkeypatch.setattr(gate, "_gh_runs", self._runs)
        monkeypatch.setattr(gate, "_gh_states", self._states)

    def _sched(self, wf, timeout=60):
        answers = self.sched[wf]
        i = self.calls["sched"][wf]
        self.calls["sched"][wf] += 1
        a = answers[min(i, len(answers) - 1)]
        return list(a() if callable(a) else a)

    def _runs(self, wf, timeout=60):
        r = self.runs[wf]
        i = self.calls["runs"][wf]
        self.calls["runs"][wf] += 1
        if isinstance(r, list) and r and (isinstance(r[0], list) or callable(r[0])):
            r = r[min(i, len(r) - 1)]
        return list(r() if callable(r) else r)

    def _states(self, timeout=60):
        self.calls["states"] += 1
        return dict(self.states)


def _real_read(budget_end):
    return ss._read_github(budget_end, sleep=lambda s: None)


def test_a_transient_EMPTY_listing_with_scheduled_runs_in_the_unfiltered_one_is_no_stall(tmp_path, monkeypatch):
    """R1660 exactly: heavy's filtered answer is empty on EVERY read, while the unfiltered listing shows its
    scheduled runs. Six evaluations: never stalled, nothing would be mailed, and the re-reads did happen."""
    fresh = [run(NOW - dt.timedelta(hours=3))]
    gh_ = FakeGH(monkeypatch, sched={"updater-daily.yml": [fresh], "updater-heavy.yml": [[]]},
                 runs={"updater-daily.yml": fresh, "updater-heavy.yml": [run(NOW - dt.timedelta(hours=7))]})
    rig = Rig(tmp_path, Mailer(), sending=False)
    for i in range(6):
        rig.tick(NOW + dt.timedelta(minutes=30 * i), read=_real_read)
    st = rig.state()
    assert st["last_verdict"] == "ok" and st["stalled"] == [] and st["undetermined"] == []
    assert not st.get("would_mail") and rig.mailer.sent == []
    assert gh_.calls["sched"]["updater-heavy.yml"] == 6 * (1 + gate.SCHED_EMPTY_RETRIES), "re-read every time"
    assert any("using the 1 scheduled run(s) in the unfiltered listing" in n for n in st["last_notes"])
    # and one empty read that the re-read answers is no stall either
    dispatch = [run(NOW - dt.timedelta(hours=1), event="workflow_dispatch")]
    gh2 = FakeGH(monkeypatch, sched={"updater-daily.yml": [fresh], "updater-heavy.yml": [[], fresh]},
                 runs={"updater-daily.yml": fresh, "updater-heavy.yml": dispatch})
    rig2 = Rig(tmp_path / "b", Mailer(), sending=False)
    rig2.tick(NOW, read=_real_read)
    assert rig2.state()["undetermined"] == [] and gh2.calls["sched"]["updater-heavy.yml"] == 2


def test_three_empty_reads_and_an_empty_unfiltered_listing_are_UNDETERMINED_never_a_stall(tmp_path, monkeypatch):
    """AR-375 required 2: a confirmed all-empty listing is a read fault (a real stall still lists the old
    scheduled runs). However long it lasts it never becomes a stall; past 36 h it is the one 'blind' mail."""
    fresh = [run(NOW - dt.timedelta(hours=3))]
    dispatch = [run(NOW - dt.timedelta(hours=1), event="workflow_dispatch")]   # no scheduled run among them
    clock = {"t": NOW}
    daily = lambda: [run(clock["t"] - dt.timedelta(hours=3))]          # noqa: E731 - daily keeps running
    gh_ = FakeGH(monkeypatch, sched={"updater-daily.yml": [daily], "updater-heavy.yml": [[]]},
                 runs={"updater-daily.yml": [daily], "updater-heavy.yml": dispatch})
    rig = Rig(tmp_path, Mailer(), sending=False)
    rig.tick(NOW, read=_real_read)
    st = rig.state()
    assert gh_.calls["sched"]["updater-heavy.yml"] == 3, "three reads"
    assert st["last_verdict"] == "undetermined" and st["undetermined"] == ["updater-heavy.yml"]
    assert st["stalled"] == []
    for i in range(1, 76):                           # 37.5 h, one evaluation every 30 min
        clock["t"] = NOW + dt.timedelta(minutes=30 * i)
        rig.tick(clock["t"], read=_real_read)
        assert rig.state()["stalled"] == [], i
    st = rig.state()
    assert [w["kind"] for w in st["would_mail"]] == ["blind"] and rig.mailer.sent == []
    assert "updater-heavy.yml" in st["would_mail"][0]["subject"] and "read empty" in st["would_mail"][0]["subject"]
    assert st["undetermined_since"] == {"updater-heavy.yml": "2026-10-22T12:00:00Z"}
    # control: the same listings with one scheduled run in them -> determined, ok
    gh2 = FakeGH(monkeypatch, sched={"updater-daily.yml": [fresh], "updater-heavy.yml": [[], [], fresh]},
                 runs={"updater-daily.yml": fresh, "updater-heavy.yml": dispatch})
    rig2 = Rig(tmp_path / "b", Mailer(), sending=False)
    rig2.tick(NOW, read=_real_read)
    assert rig2.state()["last_verdict"] == "ok" and rig2.state()["undetermined"] == []
    assert gh2.calls["sched"]["updater-heavy.yml"] == 3


def _stalled_rig(tmp_path, monkeypatch, heavy_sched, heavy_runs):
    """A shadow rig over FakeGH whose heavy answers are given as lists of per-evaluation answers."""
    fresh = lambda: [run(NOW)]                                          # noqa: E731
    gh_ = FakeGH(monkeypatch, sched={"updater-daily.yml": [fresh], "updater-heavy.yml": heavy_sched},
                 runs={"updater-daily.yml": [fresh], "updater-heavy.yml": heavy_runs})
    return Rig(tmp_path, Mailer(), sending=False), gh_


def test_P1_one_empty_scheduled_listing_inside_a_real_stall_is_no_recovery(tmp_path, monkeypatch):
    """AR-375 probe P1: a real 40 h heavy stall, one evaluation whose scheduled listing is confirmed empty. Before
    the fix the would-be mails were stall, recovered, stall."""
    plan = {"n": 0}
    old = [run(NOW - dt.timedelta(hours=40))]
    dispatch = [run(NOW - dt.timedelta(hours=1), event="workflow_dispatch")]
    rig, _ = _stalled_rig(tmp_path, monkeypatch, [lambda: [] if plan["n"] in (2, 3) else old], [dispatch])
    for i in range(8):                               # evaluations 2 and 3: the listing reads empty
        plan["n"] = i
        rig.tick(NOW + dt.timedelta(minutes=30 * i), read=_real_read)
        if i in (2, 3):
            assert rig.state()["last_verdict"] == "undetermined", i
    assert [w["kind"] for w in rig.state()["would_mail"]] == ["stall"], rig.state()["would_mail"]
    assert rig.state()["alert_active"] is True


def test_P9_one_empty_unfiltered_listing_during_a_stuck_run_alert_is_no_recovery(tmp_path, monkeypatch):
    """AR-375 probe P9 / live plant L4: the stuck-run rule's input read empty once. Now it is re-read, and still
    empty is UNDETERMINED - never clear."""
    plan = {"n": 0}
    stuck = [run(NOW - dt.timedelta(hours=40), status="in_progress"), run(NOW, status="pending")]
    rig, gh_ = _stalled_rig(tmp_path, monkeypatch, [lambda: [run(NOW)]],
                            [lambda: [] if plan["n"] == 2 else stuck])
    for i in range(6):
        plan["n"] = i
        rig.tick(NOW + dt.timedelta(minutes=30 * i), read=_real_read)
    st = rig.state()
    assert [w["kind"] for w in st["would_mail"]] == ["stall"] and st["alert_active"] is True
    assert gh_.calls["runs"]["updater-heavy.yml"] == 5 + (1 + gate.SCHED_EMPTY_RETRIES), "the empty one re-read"
    # control: an empty unfiltered answer that the re-read answers keeps the writer determined (stalled)
    seq = iter([[], stuck] + [stuck] * 50)
    rig2, gh2 = _stalled_rig(tmp_path / "b", monkeypatch, [lambda: [run(NOW)]], [lambda: next(seq)])
    rig2.tick(NOW, read=_real_read)
    assert rig2.state()["stalled"] == ["updater-heavy.yml"] and gh2.calls["runs"]["updater-heavy.yml"] == 2
    assert any("came back empty, then held 2 run(s)" in n for n in rig2.state()["last_notes"])


def test_recovery_needs_every_writer_clear_on_two_evaluations_in_a_row_through_the_tick(tmp_path, monkeypatch):
    plan = {"n": 0}
    old = [run(NOW - dt.timedelta(hours=40))]
    dispatch = [run(NOW - dt.timedelta(hours=1), event="workflow_dispatch")]
    # 0-1 stalled (onset at 1); 2 clear; 3 undetermined; 4 clear; 5 clear -> recovered at 5
    answers = {0: old, 1: old, 2: [run(NOW)], 3: [], 4: [run(NOW)], 5: [run(NOW)], 6: [run(NOW)]}
    rig, _ = _stalled_rig(tmp_path, monkeypatch, [lambda: answers[plan["n"]]], [dispatch])
    kinds = []
    for i in range(7):
        plan["n"] = i
        rig.tick(NOW + dt.timedelta(minutes=30 * i), read=_real_read)
        kinds.append([w["kind"] for w in rig.state().get("would_mail") or []])
    assert kinds[1] == ["stall"] and kinds[4] == ["stall"], kinds
    assert kinds[5] == ["stall", "recovered"] and kinds[6] == ["stall", "recovered"], kinds
    assert rig.state()["would_mail"][1]["at"] == ss._iso(NOW + dt.timedelta(minutes=150))


def test_the_R1660_shape_two_writers_empty_on_alternate_evaluations_never_stalls(tmp_path, monkeypatch):
    fresh = [run(NOW - dt.timedelta(hours=3))]
    turn = {"n": 0}

    def answer(wf):
        return lambda: [] if (turn["n"] % 2 == 0) == (wf == "updater-daily.yml") else fresh
    dispatch = [run(NOW - dt.timedelta(hours=1), event="workflow_dispatch")]
    FakeGH(monkeypatch, sched={wf: [answer(wf)] for wf in gate.WRITERS}, runs={wf: dispatch for wf in gate.WRITERS})
    rig = Rig(tmp_path, Mailer(), sending=False)
    for i in range(8):
        turn["n"] = i
        rig.tick(NOW + dt.timedelta(minutes=30 * i), read=_real_read)
        assert rig.state()["stalled"] == [] and len(rig.state()["undetermined"]) == 1, i
    assert not rig.state().get("would_mail")


def _tick_replay(tmp_path, monkeypatch, runs_by_wf, start, end, sending=False, mailer=None, blank=None):
    """The guard tick every 30 min over a span, reading GitHub through the real _read_github/read_sched with a
    fake gh that answers what existed at that instant (newest first, at the gate's limits). blank(wf, t): True
    makes EVERY listing of that writer read empty at t (both calls, every re-read) - the R1660 episode shape."""
    import bisect
    rig = Rig(tmp_path, mailer or Mailer(), sending=sending)
    clock = {"t": start}
    asc = {wf: sorted(v) for wf, v in runs_by_wf.items()}
    rows = {wf: [run(c) for c in v] for wf, v in asc.items()}

    def listed(wf, limit):
        if blank is not None and blank(wf, clock["t"]):
            return []
        n = bisect.bisect_right(asc[wf], clock["t"])
        return rows[wf][max(0, n - limit):n][::-1]
    FakeGH(monkeypatch, sched={wf: [lambda wf=wf: listed(wf, gate.SCHED_LIMIT)] for wf in gate.WRITERS},
           runs={wf: (lambda wf=wf: listed(wf, gate.RUN_LIMIT)) for wf in gate.WRITERS})
    t = start
    while t <= end:
        clock["t"] = t
        out = rig.tick(t, read=_real_read)
        assert "check failed" not in out, out
        t += ss.CHECK_EVERY
    return rig


def test_the_real_85_day_history_through_the_tick_gives_zero_would_be_mails(tmp_path, monkeypatch):
    runs, snap = _history()
    start = max(v[0] for v in runs.values()) + dt.timedelta(minutes=1)
    assert snap - start > dt.timedelta(days=84)
    rig = _tick_replay(tmp_path, monkeypatch, runs, start, snap)
    st = rig.state()
    assert st["mode"] == "shadow" and st.get("would_mail_count", 0) == 0 and not st.get("would_mail")
    assert st["last_verdict"] == "ok" and rig.mailer.sent == []


def test_a_planted_real_stall_in_the_history_gives_a_would_be_mail_and_sends_nothing(tmp_path, monkeypatch):
    runs, _snap = _history()
    cut = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
    dead = {wf: [c for c in v if c <= cut] for wf, v in runs.items()}
    rig = _tick_replay(tmp_path, monkeypatch, dead, cut, cut + dt.timedelta(hours=48))
    st = rig.state()
    assert st["would_mail"] and st["would_mail"][0]["kind"] == "stall" and rig.mailer.sent == []
    first = ss._t(st["would_mail"][0]["at"])
    oldest_newest = min(max(v) for v in dead.values())
    assert ss.STALL_AFTER < first - oldest_newest <= ss.STALL_AFTER + 2 * ss.CHECK_EVERY
    # control: the same plant with the owner's flag set sends exactly that mail
    rig2 = _tick_replay(tmp_path / "send", monkeypatch, dead, cut, cut + dt.timedelta(hours=48), sending=True)
    assert [s[0] for s in rig2.mailer.sent] == [st["would_mail"][0]["subject"]]


def _episodes(spans):
    """blank(wf, t) for a list of (writer, start, minutes) all-empty episodes."""
    def blank(wf, t):
        return any(w == wf and s <= t < s + dt.timedelta(minutes=m) for w, s, m in spans)
    return blank


def test_long_all_empty_episodes_in_the_real_history_give_zero_would_be_mails(tmp_path, monkeypatch):
    """AR-375 required 2, as a test: its replay gave 12 would-be mails with 65-min episodes and 76 with 95-min
    ones. Here: 12 episodes of 95 min to 5 h, both writers, every listing empty, over the 85-day history."""
    runs, snap = _history()
    start = max(v[0] for v in runs.values()) + dt.timedelta(minutes=1)
    spans = []
    for i in range(12):
        wf = sorted(gate.WRITERS)[i % 2]
        spans.append((wf, start + dt.timedelta(days=3 + 6.5 * i, hours=i), (95, 185, 300)[i % 3]))
    rig = _tick_replay(tmp_path, monkeypatch, runs, start, snap, blank=_episodes(spans))
    st = rig.state()
    assert st.get("would_mail_count", 0) == 0 and not st.get("would_mail"), st.get("would_mail")
    assert st["last_verdict"] == "ok"


def test_short_all_empty_episodes_inside_a_planted_stall_make_no_recovery_and_no_second_onset(tmp_path, monkeypatch):
    """AR-375's replay shape: a planted real stall from 2026-10-10T00Z with all-empty episodes inside it (10, 35,
    65 min, both writers). Before the fix: a false 'recovered' and a second onset. Now: one onset, reminders."""
    runs, _snap = _history()
    cut = dt.datetime(2026, 10, 10, 0, 0, tzinfo=UTC)
    dead = {wf: [c for c in v if c <= cut] for wf, v in runs.items()}
    spans = [(wf, cut + dt.timedelta(hours=h), m) for h, m in ((40, 10), (44, 35), (50, 65), (60, 35), (70, 10))
             for wf in gate.WRITERS]
    rig = _tick_replay(tmp_path, monkeypatch, dead, cut, cut + dt.timedelta(hours=96), blank=_episodes(spans))
    kinds = [w["kind"] for w in rig.state()["would_mail"]]
    assert kinds[0] == "stall" and set(kinds[1:]) <= {"remind"} and len(kinds) >= 2, kinds
    assert rig.mailer.sent == []


def test_turning_sending_on_during_an_open_shadow_episode_starts_it_again(tmp_path):
    """AR-375 recommended: the owner's first real mail is a confirmed onset, never a reminder."""
    rig = Rig(tmp_path, Mailer(), sending=False)
    _confirmed_stall_shadow(rig)
    assert rig.state()["alert_active"] is True
    with open(rig.flag, "w", encoding="utf-8") as fh:            # the owner turns sending on
        fh.write("on\n")
    out = rig.tick(NOW + dt.timedelta(hours=1))
    st = rig.state()
    assert "starts again" in out and st["mode"] == "send" and st["alert_active"] is False and rig.mailer.sent == []
    assert st["sending_since_utc"] == "2026-10-22T13:00:00Z"
    out = rig.tick(NOW + dt.timedelta(hours=1, minutes=30))
    assert len(rig.mailer.sent) == 1 and rig.mailer.sent[0][0].startswith("[econ] scheduler stall: "), out
    assert "(reminder)" not in rig.mailer.sent[0][0]
    # and a later tick in send mode does not start it again
    rig.tick(NOW + dt.timedelta(hours=2))
    assert rig.state()["alert_active"] is True and "sending_since_utc" in rig.state()


def test_the_beat_carries_the_mode_and_the_latest_would_be_mails(tmp_path, monkeypatch):
    p = str(tmp_path / "s.json")
    ss.save_state({"mode": "shadow", "last_evaluated_utc": "2026-10-22T12:00:00Z", "last_verdict": "stalled",
                   "would_mail_count": 7,
                   "would_mail": [{"kind": "remind", "subject": f"s{i}", "at": "x", "text": "no"} for i in range(7)]},
                  p)
    import core.cutover as cutover
    monkeypatch.setattr(cutover, "is_cut_over", lambda: False)
    b = ss.summary_for_beat(p)
    assert b["mode"] == "shadow" and b["would_mail_count"] == 7
    assert [w["subject"] for w in b["would_mail"]] == ["s2", "s3", "s4", "s5", "s6"]
    assert all("text" not in w for w in b["would_mail"])
    assert ss.summary_for_beat(str(tmp_path / "none.json")) == {"never_evaluated": True, "mode": "shadow"}


def test_the_armed_step_in_SHADOW_mode_fails_only_when_the_alarm_is_not_evaluating(monkeypatch, capsys):
    """Brief item 3: shadow mode must not fail on 'no mail' or 'no mail service'; no evaluation for 6 h is red."""
    now = dt.datetime.now(UTC)
    old = ss._iso(now - dt.timedelta(hours=7))
    would = [{"kind": "stall", "subject": "[econ] scheduler stall: updater-heavy.yml - x", "at": "2026-10-22T12:30:00Z"}]
    for fine in (_armed(mode="shadow", mail_configured=False, transport="none"),
                 _armed(mode="shadow", unknown_since_utc=old),
                 _armed(mode="shadow", mail_due={"kind": "stall", "since": old}),
                 _armed(mode="shadow", would_mail=would, would_mail_count=1, last_verdict="stalled",
                        stalled=["updater-heavy.yml"])):
        assert _armed_check(monkeypatch, fine) == 0, fine
        out = capsys.readouterr().out
        assert "SHADOW MODE (sends nothing)" in out and "NOT ARMED:" not in out.split("would read")[0]
    _armed_check(monkeypatch, _armed(mode="shadow", would_mail=would, would_mail_count=1))
    out = capsys.readouterr().out
    assert "would-be mail 2026-10-22T12:30:00Z [stall]" in out and "would-be mails so far 1" in out
    _armed_check(monkeypatch, _armed(mode="shadow", mail_configured=False))
    assert "with sending on, this would read NOT ARMED: no mail service" in capsys.readouterr().out
    for bad in (_armed(mode="shadow", last_evaluated_utc=old), {"never_evaluated": True, "mode": "shadow"}):
        assert _armed_check(monkeypatch, bad) == 1, bad
        assert "SCHEDULER-STALL ALARM NOT ARMED" in capsys.readouterr().out
    # control: the same no-mail-service beat in SEND mode, or with no mode at all (PR #191's beats), fails
    assert _armed_check(monkeypatch, _armed(mode="send", mail_configured=False)) == 1
    assert _armed_check(monkeypatch, _armed(mail_configured=False)) == 1


def test_with_sending_on_a_writer_undetermined_for_6_h_is_not_armed(monkeypatch, capsys):
    now = dt.datetime.now(UTC)
    old, recent = ss._iso(now - dt.timedelta(hours=7)), ss._iso(now - dt.timedelta(hours=1))
    assert _armed_check(monkeypatch, _armed(mode="send", undetermined_since={"updater-heavy.yml": old})) == 1
    assert "updater-heavy.yml as UNDETERMINED" in capsys.readouterr().out
    assert _armed_check(monkeypatch, _armed(mode="send", undetermined_since={"updater-heavy.yml": recent})) == 0
    assert _armed_check(monkeypatch, _armed(mode="shadow", undetermined_since={"updater-heavy.yml": old})) == 0
    assert "with sending on, this would read NOT ARMED" in capsys.readouterr().out


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


def _beat(monkeypatch, stall, present=True, utc=None):
    body = {"utc": (utc or dt.datetime.now(UTC)).isoformat(), "host": "w", "jobs_alive": [], "tracked": [],
            "jobs_detail": [], "table_ok": True, "emptiness": {"ran": True, "fetch_without_write": 0}}
    if present:
        body["scheduler_stall"] = stall

    class C:
        def get_object(self, **kw):
            import io
            return {"Body": io.BytesIO(json.dumps(body).encode())}
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: C())


def _check(monkeypatch, stall, present=True, utc=None):
    _beat(monkeypatch, stall, present, utc)
    return gh.check(45)


def _armed_check(monkeypatch, stall, present=True, utc=None):
    _beat(monkeypatch, stall, present, utc)
    return gh.check_stall_armed()


def _armed(**kw):
    now = dt.datetime.now(UTC)
    s = {"last_evaluated_utc": ss._iso(now - dt.timedelta(minutes=20)), "last_verdict": "ok", "stalled": [],
         "alert_active": False, "unknown_since_utc": None, "mail_configured": True, "transport": "cloudflare",
         "mail_to": "DIGEST_TO", "last_send": None, "mail_due": None}
    s.update(kw)
    return s


def test_the_watchdog_check_prints_a_stalled_verdict_and_never_fails_on_the_alarm(monkeypatch, capsys):
    """AR-366 round 2: the armed check has its OWN step, so the watchdog step never fails on it."""
    assert _check(monkeypatch, _armed()) == 0
    assert _check(monkeypatch, _armed(last_verdict="stalled", stalled=["updater-heavy.yml"], alert_active=True)) == 0
    out = capsys.readouterr().out
    assert "scheduler-stall check (send mode) reads stalled" in out and "updater-heavy.yml" in out
    assert _check(monkeypatch, _armed(mode="shadow", last_verdict="stalled", stalled=["updater-heavy.yml"])) == 0
    assert "(shadow mode) reads stalled" in capsys.readouterr().out
    assert _check(monkeypatch, _armed(mail_configured=False)) == 0, "an unarmed alarm is not the watchdog's failure"
    assert "NOT ARMED" not in capsys.readouterr().out


def test_an_unarmed_alarm_cannot_hide_a_dead_watchdog(monkeypatch, capsys):
    """The two claims fail in two steps: a stale beat still fails the watchdog step while the alarm is unarmed."""
    stale = dt.datetime.now(UTC) - dt.timedelta(hours=2)
    assert _check(monkeypatch, _armed(mail_configured=False), utc=stale) == 1
    assert "GUARD HEARTBEAT STALE" in capsys.readouterr().out
    assert _armed_check(monkeypatch, _armed(mail_configured=False), utc=stale) == 1
    assert "SCHEDULER-STALL ALARM NOT ARMED" in capsys.readouterr().out


def test_the_armed_step_FAILS_when_the_alarm_is_not_armed(monkeypatch, capsys):
    """Review finding 5: the alarm can only be shown armed while the scheduler works - so this is where it fails."""
    now = dt.datetime.now(UTC)
    old = ss._iso(now - dt.timedelta(hours=7))
    for bad in (_armed(mail_configured=False), _armed(last_evaluated_utc=old), _armed(unknown_since_utc=old),
                _armed(mail_due={"kind": "stall", "since": old}), {"never_evaluated": True}, {"unreadable": "x"},
                "garbage"):
        assert _armed_check(monkeypatch, bad) == 1, bad
        assert "SCHEDULER-STALL ALARM NOT ARMED" in capsys.readouterr().out
    # controls: a mail due for 1 h, GitHub unreadable for 1 h, after T0, and an older publisher - all pass
    recent = ss._iso(now - dt.timedelta(hours=1))
    for fine in (_armed(), _armed(mail_due={"kind": "stall", "since": recent}), _armed(unknown_since_utc=recent),
                 {"not_applicable": "after T0"}):
        assert _armed_check(monkeypatch, fine) == 0, fine
    assert _armed_check(monkeypatch, None, present=False) == 0
    assert "predates the scheduler-stall check" in capsys.readouterr().out


def test_the_armed_step_does_not_fail_on_a_missing_beat_that_the_watchdog_step_reports(monkeypatch, capsys):
    class C:
        def get_object(self, **kw):
            raise FileNotFoundError("no beat")
    monkeypatch.setattr(gh.r2_util, "client", lambda *a, **k: C())
    assert gh.check_stall_armed() == 0 and "cannot be judged" in capsys.readouterr().out
    assert gh.check(45) == 1


def test_the_daily_workflow_runs_the_armed_check_in_its_own_always_step_after_the_watchdog():
    with open(os.path.join(ROOT, ".github", "workflows", "updater-daily.yml"), encoding="utf-8") as fh:
        y = fh.read()
    w = y.index("run: python tools/guard_heartbeat.py --check\n")
    a = y.index("run: python tools/guard_heartbeat.py --check-stall-armed")
    assert w < a
    step = y[y.rindex("- name:", 0, a):a]
    assert "Scheduler-stall alarm armed" in step and "if: ${{ always() }}" in step
    assert "--check-stall-armed" not in y[:w], "not folded into the watchdog step"


def test_the_flag_reaches_the_armed_check(monkeypatch):
    called = []
    monkeypatch.setattr(gh, "check_stall_armed", lambda: called.append(1) or 7)
    monkeypatch.setattr(sys, "argv", ["guard_heartbeat.py", "--check-stall-armed"])
    assert gh.main() == 7 and called == [1]
