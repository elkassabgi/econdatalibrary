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
waits for those), a writer is STALLED when
  * its newest SCHEDULED run was created more than STALL_AFTER ago (a manual dispatch does not count: it
    writes the state, but it says nothing about the scheduler), or GitHub lists no scheduled run for it; or
  * one of its runs (any trigger) has not completed for more than STALL_AFTER. The gate blocks on every
    unfinished writer run however old, and GitHub keeps creating scheduled runs behind it as pending, so the
    first rule alone cannot see this shape (plan review finding 1). Normal: createdAt -> updatedAt at most
    11.5 h (daily) and 23.6 h (heavy) over the AR-362 snapshot.
A disabled workflow is not judged: the gate does not wait for it, so it blocks nothing. After T0 the gate is
not asked (the cloud updater workflows are disabled), so this does nothing. A newest createdAt more than 10 min
in the future means this machine's clock is behind: that evaluation is UNKNOWN, never "fresh".

WHY 36 HOURS. Measured on a gh snapshot of both writers' scheduled runs at 2026-10-22T01:51Z: over the 85 days
in which both writers ran (both two-cron schedules exist for about 70 of them), the largest gap between two
consecutive scheduled-run creations of one writer is 1,569 min (26.2 h: the 2026-08-31 skip of the 03:00Z /
06:00Z crons, next run 20:25Z / 22:28Z), and no gap is over 28 h. A second instrument (the plan review,
sampling the age on a 5-minute grid) gives 26.1 h. Start lag over the 60 days to 2026-10-20 (review AR-362):
p99 661, max 710 min. The age this rule reads is at most "one cron gap + the next run's lag" without a skip
(12 h + 710 min = 23.8 h) and "one skipped cron + the next run's lag" with one skip (24 h + 710 min = 35.8 h).
36 h = 2 x CRON_SPACING + PENDING_LIMIT: one skipped cron plus the 12 h window in which the gate credits a run
to its cron at all. Two consecutive skips of one workflow can cross it; none was seen. The cost is speed: the
mail comes 24-36 h after the first cron GitHub did not create, when the desktop has been blocked about a day
(the normal longest gap between desktop passes is about 73 h, AR-362).

MAIL. A stall mails once when it is seen on two evaluations in a row (one odd listing cannot mail), again every
REMIND_EVERY while it lasts, and once when every writer is clear again. GitHub unreadable for longer than
STALL_AFTER sends a "blind" mail: the gate cannot read CI either, answers UNKNOWN, and run_local_heavy.ps1 does
not start then. The sender is the status address the digest uses (noreply@hfdatalibrary.com), never a person.

TWO PHASES, BECAUSE THE GUARD LOOP KILLS THE PUBLISH AT 90 s. An evaluation (four gh calls, about 11-20 s) only
DECIDES a mail and records it as due. The send happens in a later tick, first thing in the step, and only when
little of the 90 s has gone (two mail POSTs can take 60 s). Before a send the attempt is recorded; a later tick
that finds an attempt with no result (the process was killed during the send) waits HOLD_OFF before it tries
again. So a kill can cause a duplicate mail, never one per tick. A mail that is skipped (no mail service
configured) or fails stays due and is tried again at the next tick.

STATE. logs/scheduler_stall.json, written atomically, under a lock file so that two guard loops cannot both
send. Evaluated at most every CHECK_EVERY. Stored instants more than 5 min in the future (a clock that was
corrected) are discarded. A state that cannot be written mails nothing. The beat carries a summary, and the
cloud's guard_heartbeat --check FAILS while CI runs if this check is not armed (no evaluation for 6 h, no mail
service configured, GitHub unreadable for 6 h, or a mail due and undelivered for 6 h): the only time the alarm
can be shown to be armed is while the scheduler still works.

MAIL SETTINGS come from os.environ, else from the checkout's .env - only the mail keys (MAIL_KEYS), never the
whole file, and only when a mail is about to go or the settings are reported.

    python tools/scheduler_stall.py             # read-only: what the rule sees now, the recipient and the transport
    python tools/scheduler_stall.py --now 2026-10-24T12:00:00Z   # the same, judged at another instant
    python tools/scheduler_stall.py --tick      # what the guard loop does: evaluate or send, and record
    python tools/scheduler_stall.py --test-mail # owner's check that the mail path works from this machine
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _p in (ROOT, os.path.join(ROOT, "tools")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ci_writer_gate as gate                                # noqa: E402

STALL_AFTER = 2 * gate.CRON_SPACING + gate.PENDING_LIMIT     # 36 h - see the docstring for the measurement
REMIND_EVERY = dt.timedelta(hours=24)
CHECK_EVERY = dt.timedelta(minutes=30)
HOLD_OFF = dt.timedelta(hours=6)          # after a send that never reported back (killed mid-send)
ARMED_MAX_AGE = dt.timedelta(hours=6)     # guard_heartbeat --check: how stale "armed" may be
FUTURE_SLACK = dt.timedelta(minutes=5)
CLOCK_SKEW = dt.timedelta(minutes=10)
# Seconds since the publish started (the guard loop kills it at 90 s). Mail POSTs: up to 2 x 30 s.
EVAL_START_MAX_S, SEND_START_MAX_S, HARD_STOP_S = 40, 25, 85
GH_TIMEOUT_S = 20
LOCK_STALE = dt.timedelta(minutes=10)
STATE_PATH = os.path.join(ROOT, "logs", "scheduler_stall.json")
ENV_PATH = os.path.join(ROOT, ".env")
MAIL_KEYS = ("CLOUDFLARE_EMAIL_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "RESEND_API_KEY", "DIGEST_TO")
DEFAULT_TO = "admin@hfdatalibrary.com"
SENDER = "Econ Data Library <noreply@hfdatalibrary.com>"
UA = "econdatalibrary-scheduler-stall/1.0"
LIMIT_H = int(STALL_AFTER.total_seconds() // 3600)


class ClockSkew(Exception):
    pass


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


def _past(s, now: dt.datetime) -> dt.datetime | None:
    """A stored instant, or None when it is missing, unreadable or in the future (a corrected clock would
    otherwise freeze every rule that waits for it)."""
    v = _t(s)
    return None if v is None or v > now + FUTURE_SLACK else v


# ------------------------------------------------------------------ the rule
def assess(sched: dict, states: dict, now: dt.datetime, runs: dict | None = None) -> dict:
    """sched: {workflow: [runs]} SCHEDULED runs only (gate._gh_sched); runs: {workflow: [runs]} of any trigger
    (gate._gh_runs), for the unfinished-run rule; states: {workflow: state} (gate._gh_states). Pure.

    Returns {"stalled": [...], "ok": [...], "notes": [...]}; each writer row is
    {"workflow", "newest": iso | None, "age_h": float | None, "reason": str}. Raises ClockSkew."""
    out = {"stalled": [], "ok": [], "notes": []}
    runs = runs or {}
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
        if newest is not None and newest > now + CLOCK_SKEW:
            raise ClockSkew(f"{wf}'s newest scheduled run is {_iso(newest)}, after this machine's clock {_iso(now)}")
        age_h = None if newest is None else round(max(0.0, (now - newest).total_seconds() / 3600), 1)
        row = {"workflow": wf, "newest": _iso(newest), "age_h": age_h}
        if newest is None:
            row["reason"] = "GitHub lists NO scheduled run (the listing may be wrong; the gate blocks either way)"
        elif now - newest > STALL_AFTER:
            row["reason"] = f"newest scheduled run created {row['newest']} ({age_h:.1f} h ago)"
        else:
            stuck = [r for r in runs.get(wf, []) if r.get("status") != "completed" and _t(r.get("createdAt"))
                     and now - _t(r["createdAt"]) > STALL_AFTER]
            if stuck:
                r0 = min(stuck, key=lambda r: r["createdAt"])
                row["reason"] = (f"a {r0.get('event')} run created {r0['createdAt']} is still {r0.get('status')} "
                                 f"after {(now - _t(r0['createdAt'])).total_seconds() / 3600:.1f} h (the gate blocks "
                                 f"on it; scheduled runs queue behind it)")
            else:
                row["reason"] = f"newest scheduled run created {row['newest']} ({age_h:.1f} h ago)"
                out["ok"].append(row)
                continue
        out["stalled"].append(row)
    return out


def _describe(rows: list) -> str:
    return "\n".join(f"  {r['workflow']}: {r['reason']}" for r in rows)


def compose(kind: str, verdict: dict | None, state: dict, now: dt.datetime) -> tuple[str, str]:
    """(subject, text) for one mail. kind: stall | remind | recovered | blind. A stable "[econ] scheduler stall"
    prefix, so the owner can filter it into a folder he reads, never lose it."""
    if kind in ("stall", "remind"):
        names = ", ".join(r["workflow"] for r in verdict["stalled"])
        ages = [r["age_h"] for r in verdict["stalled"]]
        what = (f"no scheduled run for {max(ages):.0f} h" if None not in ages else "no scheduled run listed")
        subject = f"[econ] scheduler stall{' (reminder)' if kind == 'remind' else ''}: {names} - {what}"
        text = (f"A state-writing workflow has been without a new scheduled run, or held by an unfinished run, for "
                f"more than {LIMIT_H} h:\n{_describe(verdict['stalled'])}\n\n"
                "tools/ci_writer_gate.py blocks the desktop heavy pass while a cron has no run and while a writer "
                "run is unfinished, so while this lasts NO desktop pass starts and the run_location: local "
                "sources do not update. If updater-daily is the one stalled, the daily digest is not coming either.\n\n"
                "Check on GitHub: the Actions tab of elkassabgi/econdatalibrary, githubstatus.com, and\n"
                "  gh run list --workflow updater-daily.yml --event schedule --limit 5\n"
                "  gh run list --workflow updater-heavy.yml --limit 5\n"
                "  gh workflow list --all\n"
                "Disabling and re-enabling a workflow (gh workflow disable / enable) restores its schedule; a stuck "
                "run can be cancelled (gh run cancel <id>). A pass can be forced with run_local_heavy.ps1 "
                "-SkipCiCheck, but only when no cloud run is in flight: both writers compare-and-swap on the state, "
                "and the loser's bookkeeping is lost.\n")
        if verdict["ok"]:
            text += f"\nCreating runs normally:\n{_describe(verdict['ok'])}\n"
        if kind == "remind":
            text += (f"\nFirst seen {state.get('episode_since_utc')}; this reminder repeats every "
                     f"{int(REMIND_EVERY.total_seconds() // 3600)} h while it lasts.\n")
    elif kind == "recovered":
        subject = "[econ] scheduler stall over: scheduled updater runs are being created again"
        text = (f"Every state-writing workflow has a scheduled run younger than {LIMIT_H} h and no run unfinished "
                f"for that long:\n{_describe(verdict['ok'])}\n\nThe stall first seen "
                f"{state.get('episode_since_utc')} is over. The desktop pass starts again at its next CLEAR "
                "window. Nothing to do.\n")
    elif kind == "blind":
        subject = f"[econ] scheduler stall check blind: GitHub Actions unreadable from the workstation for {LIMIT_H}+ h"
        text = (f"Since {state.get('unknown_since_utc')} every attempt to read the updater runs failed. "
                f"Last error: {state.get('last_error')}\n\n"
                "tools/ci_writer_gate.py cannot read them either, so it answers UNKNOWN and run_local_heavy.ps1 "
                "does not start a pass. This check is blind for the same reason, so it cannot say whether GitHub "
                "is creating runs. On the workstation: gh auth status, the network, and the clock.\n")
    else:
        raise ValueError(kind)
    text += f"\nSent by tools/scheduler_stall.py from the workstation guard loop; decided at {_iso(now)}.\n"
    return subject, text


def step(state: dict, verdict: dict | None, error: str | None, now: dt.datetime) -> tuple[dict, str | None]:
    """The state machine for one evaluation. verdict None = GitHub could not be read (error says why). Pure.
    Returns (state after the evaluation, kind of mail now due or None). What a DELIVERED mail changes is
    on_delivered(); until then the same kind keeps coming due, so an undelivered mail is never lost."""
    s = dict(state)
    s["last_evaluated_utc"] = _iso(now)
    if verdict is None:
        s["last_verdict"] = "unknown"
        s["last_error"] = (error or "")[:300]
        since = _past(s.get("unknown_since_utc"), now)
        if since is None:
            s["unknown_since_utc"] = _iso(now)
            return s, None
        if now - since > STALL_AFTER and not s.get("blind_alerted"):
            return s, "blind"
        return s, None
    s["unknown_since_utc"], s["blind_alerted"], s["last_error"] = None, False, None
    s["stalled"] = [r["workflow"] for r in verdict["stalled"]]
    if verdict["stalled"]:
        s["last_verdict"] = "stalled"
        if _past(s.get("episode_since_utc"), now) is None:
            s["episode_since_utc"] = _iso(now)      # first sighting: confirm on the next evaluation
            return s, None
        if not s.get("alert_active"):
            return s, "stall"
        last = _past(s.get("last_mail_utc"), now)
        return s, ("remind" if last is None or now - last >= REMIND_EVERY else None)
    s["last_verdict"] = "ok"
    if s.get("alert_active"):
        return s, "recovered"
    s["episode_since_utc"] = None
    return s, None


def on_delivered(state: dict, kind: str, now: dt.datetime) -> dict:
    s = dict(state, last_mail_utc=_iso(now), last_mail_kind=kind)
    if kind == "stall":
        s["alert_active"] = True
    elif kind == "recovered":
        s["alert_active"], s["episode_since_utc"] = False, None
    elif kind == "blind":
        s["blind_alerted"] = True
    return s


# ------------------------------------------------------------------ state, lock, mail settings
def load_state(path: str = STATE_PATH) -> dict:
    """{} when there is no state yet. A file that cannot be parsed is also a fresh start - and says so in
    `_load_error`, so a corrupt file never reads as a hidden one."""
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
    for attempt in range(3):          # Windows: os.replace refuses while another process has the target open
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if attempt == 2:
                raise
            time.sleep(0.2)


def _lock(path: str, now: dt.datetime) -> bool:
    """One evaluation-or-send at a time, even with two guard loops. A lock older than LOCK_STALE is a killed
    holder (the loop kills at 90 s) and is taken over."""
    lock = path + ".lock"
    os.makedirs(os.path.dirname(lock), exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, _iso(now).encode())
            os.close(fd)
            return True
        except FileExistsError:
            try:
                age = time.time() - os.path.getmtime(lock)
            except OSError:
                continue
            if age < LOCK_STALE.total_seconds():
                return False
            try:
                os.remove(lock)
            except OSError:
                return False
    return False


def _unlock(path: str) -> None:
    try:
        os.remove(path + ".lock")
    except OSError:
        pass


def mail_env(env_path: str | None = None) -> dict:
    """The mail settings: os.environ first, then ONLY the MAIL_KEYS lines of the checkout's .env. Values are
    never printed."""
    env_path = ENV_PATH if env_path is None else env_path
    out = {k: os.environ.get(k, "").strip() for k in MAIL_KEYS}
    try:
        with open(env_path, encoding="utf-8") as fh:
            for line in fh:
                k, sep, v = line.strip().partition("=")
                k = k.strip()
                if sep and k in MAIL_KEYS and not out[k]:
                    out[k] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return out


def mail_setup(env: dict) -> dict:
    """What a send would use, safe to record: the transport and where the recipient came from (the address itself
    is printed only on this machine - DIGEST_TO is a secret in CI and the beat's check runs in a public log)."""
    cf = bool(env.get("CLOUDFLARE_EMAIL_TOKEN") and env.get("CLOUDFLARE_ACCOUNT_ID"))
    rs = bool(env.get("RESEND_API_KEY"))
    return {"mail_configured": cf or rs,
            "transport": "cloudflare" + (", resend fallback" if rs else "") if cf else ("resend" if rs else "none"),
            "mail_to": "DIGEST_TO" if env.get("DIGEST_TO") else f"default {DEFAULT_TO}"}


def send_mail(subject: str, text: str, env: dict | None = None) -> str:
    """Through core/status_mail.py, like the digest. Never raises; returns its path name."""
    try:
        env = mail_env() if env is None else env
        for k in MAIL_KEYS:           # status_mail reads os.environ; set only the mail keys, never overwrite
            if env.get(k) and not os.environ.get(k):
                os.environ[k] = env[k]
        from core.status_mail import send_status_mail         # noqa: PLC0415
        return send_status_mail(subject, text, sender=SENDER, to=env.get("DIGEST_TO") or DEFAULT_TO,
                                user_agent=UA, log=print)
    except Exception as e:                                   # noqa: BLE001
        print(f"scheduler-stall mail: {type(e).__name__}")
        return "failed"


# ------------------------------------------------------------------ one tick
def _read_github(budget_end: float):
    """(sched, runs, states) with the gate's own fetchers; every call bounded by GH_TIMEOUT_S and by the
    time left before the guard loop's kill."""
    def t():
        left = budget_end - time.monotonic()
        if left < 2:
            raise TimeoutError("no time left in this guard tick")
        return int(min(GH_TIMEOUT_S, left))
    sched = {wf: gate._gh_sched(wf, timeout=t()) for wf in gate.WRITERS}
    runs = {wf: gate._gh_runs(wf, timeout=t()) for wf in gate.WRITERS}
    return sched, runs, gate._gh_states(timeout=t())


def tick(now: dt.datetime | None = None, started: float | None = None, read=_read_github, send=send_mail,
         path: str = STATE_PATH, cut_over=None, env=None) -> str:
    """One step, as the guard loop runs it after the beat is published. Never raises. Returns a one-line outcome.
    started: time.monotonic() when the publish began (the loop kills it at 90 s); None = now."""
    started = time.monotonic() if started is None else started
    try:
        now = now or dt.datetime.now(dt.timezone.utc)
        if cut_over is None:
            from core import cutover                         # noqa: PLC0415
            cut_over = cutover.is_cut_over
        if cut_over():
            return "scheduler-stall: not applicable after T0 (the CI writer gate is not asked)"
        if not _lock(path, now):
            return "scheduler-stall: another evaluation holds the lock - skipped"
        try:
            return _tick_locked(now, started, read, send, path, env)
        finally:
            _unlock(path)
    except Exception as e:                                   # noqa: BLE001 - never break the guard tick
        return f"scheduler-stall: check failed ({type(e).__name__}: {str(e)[:160]})"


def _tick_locked(now, started, read, send, path, env) -> str:
    state = load_state(path)
    load_error = state.pop("_load_error", None)
    if load_error:
        state["state_reset"] = f"{_iso(now)}: {load_error}"
    elapsed = time.monotonic() - started
    due = state.get("mail_due")

    # PHASE 1 - a mail decided earlier: send it first, while the most of the tick is left. A send that already
    # failed for this kind is retried at most every CHECK_EVERY, and in between the evaluation keeps running.
    prev = state.get("last_send") if isinstance(state.get("last_send"), dict) else {}
    prev_at = _past(prev.get("at"), now) if prev.get("kind") == (due or {}).get("kind") else None
    retry_wait = (prev_at is not None and prev.get("result") not in ("cloudflare", "resend")
                  and now < prev_at + CHECK_EVERY)
    if isinstance(due, dict) and due.get("kind") and not retry_wait:
        attempt = state.get("mail_attempt")
        at = _past(attempt.get("at"), now) if isinstance(attempt, dict) else None
        if at is not None and now - at < HOLD_OFF:
            return (f"scheduler-stall: {due['kind']} mail attempted {attempt.get('at')} never reported back (killed "
                    f"mid-send?) - held until {_iso(at + HOLD_OFF)} so a kill cannot repeat it every tick")
        if elapsed > SEND_START_MAX_S:
            return f"scheduler-stall: {due['kind']} mail due, deferred ({elapsed:.0f} s of the tick gone)"
        save_state(dict(state, mail_attempt={"kind": due["kind"], "at": _iso(now)}), path)
        result = send(due["subject"], due["text"])
        state.pop("mail_attempt", None)
        state["last_send"] = {"kind": due["kind"], "at": _iso(now), "result": result}
        if result in ("cloudflare", "resend"):
            state = on_delivered(state, due["kind"], now)
            state.pop("mail_due", None)
            save_state(state, path)
            return f"scheduler-stall: {due['kind']} mail sent ({result})"
        save_state(state, path)
        return f"scheduler-stall: {due['kind']} mail NOT sent ({result}) - tried again at the next tick"

    # PHASE 2 - evaluate, at most every CHECK_EVERY, and only with time left in the tick.
    last = _past(state.get("last_evaluated_utc"), now)
    if last is not None and now < last + CHECK_EVERY:
        if load_error:
            save_state(state, path)
        return f"scheduler-stall: evaluated {state.get('last_evaluated_utc')}, next after {_iso(last + CHECK_EVERY)}"
    if elapsed > EVAL_START_MAX_S:
        return f"scheduler-stall: evaluation skipped ({elapsed:.0f} s of the tick gone)"
    try:
        sched, runs, states = read(started + HARD_STOP_S)
        verdict, error = assess(sched, states, now, runs), None
    except Exception as e:                                   # noqa: BLE001 - "cannot read" is its own verdict
        verdict, error = None, f"{type(e).__name__}: {e}"
    new, kind = step(state, verdict, error, now)
    new.update(mail_setup(mail_env() if env is None else env))
    if kind is None:
        if verdict is not None:       # a blind evaluation says nothing about a mail already due: keep it
            new.pop("mail_due", None)
    else:
        subject, text = compose(kind, verdict, new, now)
        new["mail_due"] = {"kind": kind, "subject": subject, "text": text, "since":
                           (due or {}).get("since") if (due or {}).get("kind") == kind else _iso(now)}
    try:
        save_state(new, path)
    except Exception as e:                                   # noqa: BLE001
        return f"scheduler-stall: could not write {path} ({type(e).__name__}) - nothing mailed"
    return (f"scheduler-stall: {new.get('last_verdict')}"
            + (f" ({', '.join(new.get('stalled') or [])})" if new.get("stalled") else "")
            + (f"; {kind} mail due, sent at the next tick" if kind else "")
            + (f"; state file was unreadable ({load_error}), started fresh" if load_error else ""))


# ------------------------------------------------------------------ the beat and its cloud reader
def summary_for_beat(path: str = STATE_PATH) -> dict:
    """What the beat carries: the last evaluation, read from the local state file (no network). Never raises."""
    try:
        from core import cutover                             # noqa: PLC0415
        if cutover.is_cut_over():
            return {"not_applicable": "after T0"}
    except Exception:                                        # noqa: BLE001
        pass
    s = load_state(path)
    if not s:
        return {"never_evaluated": True}
    if "_load_error" in s:
        return {"unreadable": s["_load_error"]}
    due = s.get("mail_due") if isinstance(s.get("mail_due"), dict) else None
    return {**{k: s.get(k) for k in ("last_evaluated_utc", "last_verdict", "stalled", "alert_active",
                                     "episode_since_utc", "unknown_since_utc", "mail_configured", "transport",
                                     "mail_to", "last_send")},
            "mail_due": None if due is None else {"kind": due.get("kind"), "since": due.get("since")}}


def armed_problem(summary, now: dt.datetime) -> str | None:
    """Why the alarm is NOT armed, judged by guard_heartbeat --check while CI runs, or None. A summary
    absent from the beat (an older publisher) is not judged here."""
    if not isinstance(summary, dict):
        return "the beat's scheduler_stall field is not an object"
    if summary.get("not_applicable"):
        return None
    if summary.get("never_evaluated"):
        return "it has never completed an evaluation on the workstation"
    if "unreadable" in summary:
        return f"its state file is unreadable ({summary['unreadable']})"
    last = _t(summary.get("last_evaluated_utc"))
    if last is None or now - last > ARMED_MAX_AGE:
        return f"its last evaluation was {summary.get('last_evaluated_utc')} (more than {ARMED_MAX_AGE} ago)"
    if summary.get("mail_configured") is False:
        return "no mail service is configured on the workstation (CLOUDFLARE_EMAIL_TOKEN + ACCOUNT_ID or RESEND_API_KEY)"
    since = _t(summary.get("unknown_since_utc"))
    if since is not None and now - since > ARMED_MAX_AGE:
        return f"the workstation cannot read GitHub Actions since {summary.get('unknown_since_utc')}"
    due = summary.get("mail_due") or {}
    dsince = _t(due.get("since"))
    if dsince is not None and now - dsince > ARMED_MAX_AGE:
        last_send = summary.get("last_send") or {}
        return (f"a {due.get('kind')} mail has been due since {due.get('since')} and is not delivered "
                f"(last send: {last_send.get('result')})")
    return None


# ------------------------------------------------------------------ command line
def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if "--tick" in argv:
        print(tick())
        return 0
    env = mail_env()
    setup = mail_setup(env)
    if "--test-mail" in argv:
        path_used = send_mail("[econ] scheduler stall: test mail",
                              "A test of the mail path of tools/scheduler_stall.py, sent by hand from the "
                              "workstation. Nothing is wrong.\n", env)
        print(f"test mail to {env.get('DIGEST_TO') or DEFAULT_TO}: {path_used}")
        return 0 if path_used in ("cloudflare", "resend") else 1
    now = dt.datetime.now(dt.timezone.utc)
    if "--now" in argv:
        now = _t(argv[argv.index("--now") + 1])
        if now is None:
            print("--now needs an ISO UTC instant such as 2026-10-24T12:00:00Z")
            return 2
    print(f"judged at {_iso(now)}; limit {LIMIT_H} h; mail would go to {env.get('DIGEST_TO') or DEFAULT_TO} "
          f"({setup['mail_to']}) via {setup['transport']}")
    try:
        sched, runs, states = _read_github(time.monotonic() + 120)
        v = assess(sched, states, now, runs)
    except Exception as e:                                   # noqa: BLE001
        print(f"UNKNOWN: could not read GitHub ({type(e).__name__}: {str(e)[:200]})")
        return 2
    for n in v["notes"]:
        print(f"NOTE: {n}")
    if v["stalled"]:
        print(f"STALLED (more than {LIMIT_H} h):\n{_describe(v['stalled'])}")
    if v["ok"]:
        print(f"ok:\n{_describe(v['ok'])}")
    return 3 if v["stalled"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
