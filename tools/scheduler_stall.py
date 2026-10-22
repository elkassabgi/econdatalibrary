"""Mail the owner when GitHub stops creating the scheduled updater runs. Runs on the workstation.

WHY (review AR-362 of PR #185). tools/ci_writer_gate.py fails CLOSED: a cron of updater-daily.yml or
updater-heavy.yml blocks the desktop heavy pass from its time until GitHub creates its run, at most 12 h, and
then the next cron blocks. So if GitHub stops creating scheduled runs while the workflows stay "active", the
desktop pass is blocked for ever. Before this file the only warning was a line in a local log
(run_local_heavy.ps1, after 2 x MinHours without a pass), and that line fires routinely. Every alarm that
leaves the machine - the daily digest, health.route_silence, guard_heartbeat --check - runs INSIDE
updater-daily, which the same stall stops. The owner would notice only a digest that did not arrive.

WHAT EACH PLACE FOR THE CHECK CAN AND CANNOT SEE
  * A check in a GitHub workflow on another schedule (billing-guard every 30 min, selfhost-watch daily) sees a
    stall of ONE workflow's schedule. It cannot see a stall of GitHub's scheduler for the whole repository or
    the whole service, because that stops its own run too. So it cannot be the only detector.
  * This check runs on the workstation, in the guard loop that already ticks every 5 minutes
    (RELAUNCH_GUARD_LOOP.ps1 -> guard_heartbeat.py --publish), and mails through the same path as the digest
    (core/status_mail.py). It sees both shapes. It cannot see anything while the workstation, the guard loop
    or its network is down - but then no desktop pass is blocked by the gate either, and the cloud's
    guard_heartbeat --check reds on a stale beat while CI runs.

THE RULE. For each writer the gate waits for (active, or missing from GitHub's workflow list - the gate still
waits for those), age = now - createdAt of its newest SCHEDULED run. A writer is stalled when age > STALL_AFTER.
A disabled workflow is not judged: the gate does not wait for it, so it blocks nothing. After T0 the gate is
not asked (the cloud updater workflows are disabled), so this does nothing.

WHY 36 HOURS. Measured on a gh snapshot of both writers' scheduled runs at 2026-10-22T01:51Z, 90 days: the
largest gap between two consecutive scheduled-run creations of one writer is 1,569 min (26.2 h: the
2026-08-31 skip of the 03:00Z / 06:00Z crons, next run 20:25Z / 22:28Z), and no gap is over 28 h. Start lag
over the 60 days to 2026-10-20 (review AR-362): p99 661, max 710 min. The age this rule reads is at most
"one cron gap + the next run's lag" without a skip (12 h + 710 min = 23.8 h) and "one skipped cron + the
next run's lag" with one skip (24 h + 710 min = 35.8 h). 36 h = 2 x CRON_SPACING + PENDING_LIMIT: one
skipped cron plus the 12 h window in which the gate credits a run to its cron at all. A single skipped cron
cannot cross it at any lag the gate itself accepts; two consecutive skips of one workflow can, and none was
seen in the 90 days. The cost is speed: the mail comes 24-36 h after the first cron GitHub did not create.
Re-measure: `python tools/measure_ci_lag.py lag` and the gap between consecutive createdAt values.

MAIL. Once when a stall is first confirmed (seen on two evaluations in a row, so one odd listing cannot mail),
again every REMIND_EVERY while it lasts, and once when every writer is under the limit again. If GitHub cannot
be read for longer than STALL_AFTER, a different mail says the check is BLIND: the gate cannot read CI either,
answers UNKNOWN, and run_local_heavy.ps1 does not start in that state. A send that is skipped (no mail service
configured) or fails is not recorded as sent, so the next evaluation tries again. The sender is the status
address the digest uses (noreply@hfdatalibrary.com), never a person. Mail settings come from os.environ, then
the checkout's .env through core.config.load_env(), read only when a mail is about to go.

STATE. logs/scheduler_stall.json, written atomically. Evaluated at most every CHECK_EVERY, so the guard tick
asks GitHub three times per half hour, not three times per 5 minutes. If the state cannot be written, nothing is
mailed (a state that cannot be recorded would mail on every tick).

    python tools/scheduler_stall.py            # read-only: print what the rule sees now; no mail, no state
    python tools/scheduler_stall.py --tick     # what the guard loop does: evaluate, record, mail
    python tools/scheduler_stall.py --test-mail  # owner's check that the mail path works from this machine
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (ROOT, os.path.join(ROOT, "tools")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ci_writer_gate as gate                                # noqa: E402

STALL_AFTER = 2 * gate.CRON_SPACING + gate.PENDING_LIMIT     # 36 h - see the docstring for the measurement
REMIND_EVERY = dt.timedelta(hours=24)
CHECK_EVERY = dt.timedelta(minutes=30)
GH_TIMEOUT_S = 20            # each of the three gh calls; the guard loop kills the whole publish at 90 s
STATE_PATH = os.path.join(ROOT, "logs", "scheduler_stall.json")
SENDER = "Econ Data Library <noreply@hfdatalibrary.com>"
# What one evaluation records whether or not a mail it makes due is delivered (see tick).
_EVAL_KEYS = ("last_evaluated_utc", "last_verdict", "last_error", "unknown_since_utc", "stalled")
UA = "econdatalibrary-scheduler-stall/1.0"


def _iso(t: dt.datetime | None) -> str | None:
    return None if t is None else t.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _t(s) -> dt.datetime | None:
    if not s:
        return None
    try:
        v = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None
    return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)


def assess(sched: dict, states: dict, now: dt.datetime) -> dict:
    """sched: {workflow file: [{"createdAt", "event", ...}]} (scheduled runs, as gate._gh_sched returns them);
    states: {workflow file: "active" | "disabled_manually" | ...} (gate._gh_states). Pure.

    Returns {"stalled": [...], "ok": [...], "notes": [...]}, each writer entry
    {"workflow", "newest": iso | None, "age_h": float | None} (None: GitHub lists no scheduled run)."""
    out = {"stalled": [], "ok": [], "notes": []}
    for wf in gate.WRITERS:
        state = states.get(wf)
        if state is not None and state != "active":
            out["notes"].append(f"{wf} is {state}: the gate does not wait for it, so it is not judged")
            continue
        if state is None:
            out["notes"].append(f"{wf} is not in GitHub's workflow list; the gate still waits for it, so it is judged")
        created = [c for c in (_t(r.get("createdAt")) for r in sched.get(wf, []) if r.get("event") == "schedule")
                   if c is not None]
        newest = max(created) if created else None
        age_h = None if newest is None else max(0.0, (now - newest).total_seconds() / 3600)
        row = {"workflow": wf, "newest": _iso(newest), "age_h": None if age_h is None else round(age_h, 1)}
        stalled = newest is None or now - newest > STALL_AFTER
        out["stalled" if stalled else "ok"].append(row)
    return out


def _describe(rows: list) -> str:
    return "\n".join(
        f"  {r['workflow']}: " + (f"newest scheduled run created {r['newest']} ({r['age_h']:.1f} h ago)"
                                  if r["newest"] else "GitHub lists NO scheduled run (the listing may be wrong; "
                                                      "the gate blocks either way)")
        for r in rows)


def compose(kind: str, verdict: dict | None, state: dict, now: dt.datetime) -> tuple[str, str]:
    """(subject, text) for one mail. kind: stall | remind | recovered | blind."""
    limit_h = int(STALL_AFTER.total_seconds() // 3600)
    if kind in ("stall", "remind"):
        ages = [r["age_h"] for r in verdict["stalled"]]
        lead = "STILL " if kind == "remind" else ""
        what = (f"no scheduled updater run created for {max(ages):.0f} h" if None not in ages
                else "GitHub lists no scheduled updater run")
        subject = f"econ: {lead}{what} - the desktop pass is blocked"
        text = (f"GitHub has not created a scheduled run of a state-writing workflow for more than {limit_h} h:\n"
                f"{_describe(verdict['stalled'])}\n\n"
                "tools/ci_writer_gate.py blocks the desktop heavy pass from each cron's time until its run is "
                "created, so while this lasts NO desktop pass starts, and the run_location: local sources do not "
                "update. The daily digest is not coming either: it runs inside updater-daily.\n\n"
                "Check on GitHub: the Actions tab of elkassabgi/econdatalibrary, githubstatus.com, and\n"
                "  gh run list --workflow updater-daily.yml --event schedule --limit 5\n"
                "  gh workflow list --all\n"
                "Disabling and re-enabling a workflow restores its schedule. A pass can be forced with "
                "run_local_heavy.ps1 -SkipCiCheck, but only when no cloud run is in flight (both writers "
                "compare-and-swap on the state, and the loser's bookkeeping is lost).\n")
        if verdict["ok"]:
            text += f"\nCreating runs normally:\n{_describe(verdict['ok'])}\n"
        if kind == "remind":
            text += f"\nFirst seen {state.get('episode_since_utc')}; this reminder repeats every " \
                    f"{int(REMIND_EVERY.total_seconds() // 3600)} h while it lasts.\n"
    elif kind == "recovered":
        subject = "econ: scheduled updater runs are being created again"
        text = (f"Every state-writing workflow has a scheduled run younger than {limit_h} h:\n"
                f"{_describe(verdict['ok'])}\n\nThe stall first seen {state.get('episode_since_utc')} is over. "
                "The desktop pass starts again at its next CLEAR window. Nothing to do.\n")
    elif kind == "blind":
        subject = f"econ: the workstation cannot read GitHub Actions (for more than {limit_h} h)"
        text = (f"Since {state.get('unknown_since_utc')} every attempt to read the scheduled runs failed. "
                f"Last error: {state.get('last_error')}\n\n"
                "tools/ci_writer_gate.py cannot read them either, so it answers UNKNOWN and run_local_heavy.ps1 "
                "does not start a pass. This check is blind for the same reason, so it cannot say whether GitHub "
                "is creating runs. On the workstation: gh auth status, and the network.\n")
    else:
        raise ValueError(kind)
    text += f"\nSent by tools/scheduler_stall.py from the workstation guard loop at {_iso(now)}.\n"
    return subject, text


def step(state: dict, verdict: dict | None, error: str | None, now: dt.datetime) -> tuple[dict, str | None]:
    """The state machine. verdict None means GitHub could not be read (error says why). Pure.
    Returns (new state if the mail - when one is due - is delivered, the kind of mail due or None)."""
    s = dict(state)
    s["last_evaluated_utc"] = _iso(now)
    if verdict is None:
        s["last_verdict"] = "unknown"
        s["last_error"] = (error or "")[:300]
        since = _t(s.get("unknown_since_utc"))
        if since is None:
            s["unknown_since_utc"] = _iso(now)
            return s, None
        if now - since > STALL_AFTER and not s.get("blind_alerted"):
            s["blind_alerted"] = True
            return s, "blind"
        return s, None
    s["unknown_since_utc"], s["blind_alerted"], s["last_error"] = None, False, None
    s["stalled"] = [r["workflow"] for r in verdict["stalled"]]
    if verdict["stalled"]:
        s["last_verdict"] = "stalled"
        if s.get("episode_since_utc") is None:
            s["episode_since_utc"] = _iso(now)      # first sighting: confirm on the next evaluation
            return s, None
        if not s.get("alert_active"):
            s["alert_active"], s["last_mail_utc"] = True, _iso(now)
            return s, "stall"
        last = _t(s.get("last_mail_utc"))
        if last is None or now - last >= REMIND_EVERY:
            s["last_mail_utc"] = _iso(now)
            return s, "remind"
        return s, None
    s["last_verdict"] = "ok"
    was_active = s.get("alert_active")
    s_ok = dict(s, episode_since_utc=None, alert_active=False)
    if was_active:
        s_ok["last_mail_utc"] = _iso(now)
        return s_ok, "recovered"
    return s_ok, None


def load_state(path: str = STATE_PATH) -> dict:
    """{} when there is no state yet. A file that cannot be parsed is also {} - and is reported in the result,
    so a corrupt file reads as a fresh start, never as a hidden one."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {"_load_error": f"not an object: {type(data).__name__}"}
    except FileNotFoundError:
        return {}
    except Exception as e:                                   # noqa: BLE001
        return {"_load_error": f"{type(e).__name__}: {e}"[:200]}


def save_state(state: dict, path: str = STATE_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({k: v for k, v in state.items() if not k.startswith("_")}, fh, indent=2)
    os.replace(tmp, path)


def send_mail(subject: str, text: str) -> str:
    """Through core/status_mail.py, like the digest. Never raises; returns its path name."""
    try:
        from core import config as core_config               # noqa: PLC0415
        core_config.load_env()        # setdefault only: a value already in the environment wins
        from core.status_mail import send_status_mail         # noqa: PLC0415
        return send_status_mail(subject, text, sender=SENDER,
                                to=os.environ.get("DIGEST_TO") or "admin@hfdatalibrary.com",
                                user_agent=UA, log=print)
    except Exception as e:                                   # noqa: BLE001
        print(f"scheduler-stall mail: {type(e).__name__}")
        return "failed"


def _read_github():
    """(sched, states) from GitHub with the gate's own fetchers, each call bounded by GH_TIMEOUT_S."""
    sched = {wf: gate._gh_sched(wf, timeout=GH_TIMEOUT_S) for wf in gate.WRITERS}
    return sched, gate._gh_states(timeout=GH_TIMEOUT_S)


def tick(now: dt.datetime | None = None, read=_read_github, send=send_mail, path: str = STATE_PATH,
         cut_over=None) -> str:
    """One evaluation, as the guard loop runs it. Never raises. Returns a one-line outcome for the log."""
    try:
        now = now or dt.datetime.now(dt.timezone.utc)
        if cut_over is None:
            from core import cutover                         # noqa: PLC0415
            cut_over = cutover.is_cut_over
        if cut_over():
            return "scheduler-stall: not applicable after T0 (the CI writer gate is not asked)"
        state = load_state(path)
        load_error = state.pop("_load_error", None)
        last = _t(state.get("last_evaluated_utc"))
        # A last evaluation in the future (a clock that moved back) does not silence the check: evaluate.
        if last is not None and last <= now < last + CHECK_EVERY:
            return f"scheduler-stall: evaluated {state.get('last_evaluated_utc')}, next after {_iso(last + CHECK_EVERY)}"
        try:
            sched, states = read()
            verdict, error = assess(sched, states, now), None
        except Exception as e:                               # noqa: BLE001 - "cannot read" is its own verdict
            verdict, error = None, f"{type(e).__name__}: {e}"
        new, kind = step(state, verdict, error, now)
        # Record the evaluation BEFORE any mail, and only the evaluation: if the state cannot be written, a mail
        # would repeat on every tick; and if the mail then fails, the state still says it is due.
        pre = new if kind is None else dict(state, **{k: new.get(k) for k in _EVAL_KEYS})
        try:
            save_state(pre, path)
        except Exception as e:                               # noqa: BLE001
            return f"scheduler-stall: could not write {path} ({type(e).__name__}) - nothing mailed"
        summary = (f"scheduler-stall: {new.get('last_verdict')}"
                   + (f" ({', '.join(new.get('stalled') or [])})" if new.get("stalled") else "")
                   + (f"; state file was unreadable ({load_error}), started fresh" if load_error else ""))
        if kind is None:
            return summary
        subject, text = compose(kind, verdict, new, now)
        path_used = send(subject, text)
        if path_used in ("cloudflare", "resend"):
            save_state(new, path)
            return f"{summary}; {kind} mail sent ({path_used})"
        return f"{summary}; {kind} mail NOT sent ({path_used}) - retried at the next evaluation"
    except Exception as e:                                   # noqa: BLE001 - never break the guard tick
        return f"scheduler-stall: check failed ({type(e).__name__}: {str(e)[:160]})"


def summary_for_beat(path: str = STATE_PATH) -> dict | None:
    """What the beat carries: the last evaluation, read from the local state file (no network)."""
    s = load_state(path)
    if not s:
        return None
    if "_load_error" in s:
        return {"unreadable": s["_load_error"]}
    return {k: s.get(k) for k in ("last_evaluated_utc", "last_verdict", "stalled", "alert_active",
                                  "episode_since_utc", "unknown_since_utc")}


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if "--tick" in argv:
        print(tick())
        return 0
    if "--test-mail" in argv:
        path_used = send_mail("econ: scheduler-stall test mail",
                              "A test of the mail path of tools/scheduler_stall.py, sent by hand from the "
                              "workstation. Nothing is wrong.\n")
        print(f"test mail: {path_used}")
        return 0 if path_used in ("cloudflare", "resend") else 1
    now = dt.datetime.now(dt.timezone.utc)
    try:
        sched, states = _read_github()
    except Exception as e:                                   # noqa: BLE001
        print(f"UNKNOWN: could not read GitHub ({type(e).__name__}: {str(e)[:200]})")
        return 2
    v = assess(sched, states, now)
    for n in v["notes"]:
        print(f"NOTE: {n}")
    limit_h = int(STALL_AFTER.total_seconds() // 3600)
    if v["stalled"]:
        print(f"STALLED (no scheduled run for more than {limit_h} h):\n{_describe(v['stalled'])}")
    if v["ok"]:
        print(f"ok:\n{_describe(v['ok'])}")
    return 3 if v["stalled"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
