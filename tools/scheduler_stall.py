"""Warn the owner when GitHub stops creating the scheduled updater runs. Runs on the workstation.

SHADOW MODE IS THE DEFAULT (ledger R1661). The check evaluates, and records in its state file and in the beat
what it WOULD mail (kind, subject, time). It sends NOTHING until a person creates the flag file SEND_FLAG
(logs/SCHEDULER_STALL_SEND in the checkout the guard loop runs - E:). That file is the owner's step, taken only
after a week of shadow evaluations with 0 false would-be mails. logs/ is gitignored, so no pull, merge or
checkout can create or remove it. Deleting it is the OFF switch: it takes effect at the next guard tick (about
5 minutes), with no commit, no CI run and no relaunch. The first version of this check (PR #191) went live
sending and mailed a false stall and a false recovery within hours (R1660, R1661); its revert then waited 3 h
on a CI runner while the live copy kept sending.

WHY (review AR-362 of PR #185). tools/ci_writer_gate.py fails CLOSED: a cron of updater-daily.yml or
updater-heavy.yml blocks the desktop heavy pass from its time until GitHub creates its run, at most 12 h, and
then the next cron blocks. So if GitHub stops creating scheduled runs while the workflows stay "active", the
desktop pass is blocked for ever. Before this file the only warning was a line in a local log
(run_local_heavy.ps1, after 2 x MinHours without a pass), and that line fires routinely. The alarms that
leave the machine - the daily digest, health.route_silence, guard_heartbeat --check, selfhost-watch,
billing-guard - all run in GitHub workflows, which the same stall stops. The owner would notice only a digest
that did not arrive.

WHAT EACH PLACE FOR THE CHECK CAN AND CANNOT SEE
  * A check in a GitHub workflow on another schedule (billing-guard every 30 min, selfhost-watch daily) sees a
    stall of ONE workflow's schedule. It cannot see a stall of GitHub's scheduler for the whole repository or
    the whole service, because that stops its own run too. So it cannot be the only detector.
  * This check runs on the workstation, in the guard loop that already ticks every 5 minutes
    (RELAUNCH_GUARD_LOOP.ps1 -> guard_heartbeat.py --publish), and once sending is on it mails through the same
    path as the digest (core/status_mail.py). It sees both shapes. It cannot see anything while the
    workstation, the guard loop or its network is down - but then no desktop pass is blocked by the gate
    either, and the cloud's guard_heartbeat --check reds on a stale beat while CI runs.

THE RULE. For each writer the gate waits for (active, or missing from GitHub's workflow list - the gate still
waits for those), a writer is STALLED when
  * its newest SCHEDULED run was created more than STALL_AFTER ago (a manual dispatch does not count: it
    writes the state, but it says nothing about the scheduler); or
  * GitHub lists no scheduled run for it at all, CONFIRMED (see "AN EMPTY LISTING" below); or
  * one of its runs (any trigger) has not completed for more than STALL_AFTER. The gate blocks on every
    unfinished writer run however old, and GitHub keeps creating scheduled runs behind it as pending, so the
    first rule alone cannot see this shape (plan review finding 1). Normal: createdAt -> updatedAt at most
    11.5 h (daily) and 23.6 h (heavy) over the AR-362 snapshot.
For a disabled workflow only the third rule applies: the gate does not wait for its crons, but its in-flight
check still blocks on an unfinished run of it, whatever its state (review AR-366). After T0 the gate is
not asked (the cloud updater workflows are disabled), so this does nothing. A newest createdAt more than 10 min
in the future means this machine's clock is behind: that evaluation is UNKNOWN, never "fresh".

AN EMPTY LISTING IS NEVER A STALL BY ITSELF (ledger R1660). `gh run list --workflow <wf> --event schedule`
sometimes answers an empty list, exit 0, for a writer that has scheduled runs: on 2026-10-22 it did so for
updater-daily.yml at 14:57:42Z and for updater-heavy.yml at 15:00:51Z, and PR #191 read each as "GitHub lists
NO scheduled run" and mailed. So this check never calls the gate's raw `_gh_sched`. It reads through
`ci_writer_gate.read_sched()`, the gate's own reader since PR #193: an empty answer is read up to 2 more times,
2 s apart, and schedule-event rows of the unfiltered listing are added when the filtered answer lacks them. A
writer then reads "no scheduled run listed" only when all 3 reads were empty AND the unfiltered listing holds no
scheduled run either. Even that is not yet a stall: it must hold on 2 evaluations in a row (CHECK_EVERY apart)
for the SAME writer, and a stall must then be seen on one more evaluation for the same writer before a mail
comes due (the confirmation below). A blind evaluation in between keeps the count; a readable one that sees a
scheduled run resets it.

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

MAIL (the same in shadow mode, where "sent" means "recorded as would-be"). A stall comes due when the SAME
writer is seen stalled on two evaluations in a row (one odd listing cannot mail, and two different writers
seen once each cannot mail either - the R1660 shape), again every REMIND_EVERY while it lasts, and once when
every writer is clear again. GitHub unreadable for longer than STALL_AFTER makes ONE "blind" mail due per blind
spell (no reminder, no "over" mail): the gate cannot read CI either, answers UNKNOWN, and run_local_heavy.ps1
does not start then. The sender is the status address the digest uses (noreply@hfdatalibrary.com), never a
person. In shadow mode a would-be mail moves the state machine exactly as a delivered one does, so the shadow
log shows the mails the live alarm would have sent, in order. Turning sending on during an open episode
continues that episode: its next mail is a reminder or the "over" mail.

THE GUARD LOOP KILLS THE PUBLISH AT 90 s, and this step runs after the beat's PUT. A detached child process does
not escape that: measured 2026-10-22, a DETACHED_PROCESS child of the job died at the Stop-Job (91 lines written
at the stop, 91 fifteen seconds later; CREATE_BREAKAWAY_FROM_JOB is refused). So each step does, in this order:
  1. EVALUATE when CHECK_EVERY has passed, if at most EVAL_START_MAX_S of the publish has gone; every gh call is
     bounded by the time left to HARD_STOP_S (a call with no time left raises, and the evaluation is UNKNOWN,
     never a stall). A mail the verdict makes due is recorded as due.
  2. SEND (or, in shadow mode, RECORD) a due mail, if at most SEND_START_MAX_S has gone. The attempt is recorded
     first; an attempt that never reported back (killed during the send) holds that mail for HOLD_OFF, so a
     kill can cause one duplicate, never one per tick. An undelivered mail (no service configured, or a
     failure - which can also be a Cloudflare timeout after it accepted the mail) is retried at most every
     CHECK_EVERY and at most MAX_SENDS_PER_DAY times a day. A deferred or held send never stops step 1.
Measured on the workstation under normal load (plan review, 2026-10-22 ~02:00Z): the publish reaches this step
24.5-29.3 s after it starts, plus the PUT (about 31.3 s). Step 1 makes five gh calls and took 17.2-17.6 s
(review, 06:10Z), so the send decision comes at about 49 s; a send is one POST (a second only on a Cloudflare
failure). An empty filtered answer adds up to 2 x (2 s + one read) per writer; when that leaves no time, the
evaluation is UNKNOWN. Each step records `last_step_elapsed_s`, so the margin on the real machine is in the
state file and the beat. UNDER HEAVY LOAD THIS STEP IS STARVED, AND THE CLOUD CHECK MAY NOT SAY SO: on
2026-10-22, 02:40Z-05:55Z, the guard loop logged 18 "heartbeat publish EXCEEDED 90s" lines, the publish reached
this step at 100-130 s (after the kill), and the beats that did land had emptiness_ran=false (the audit at its
60 s timeout), so the step was skipped while the cloud's beat check could stay green. A spell like that is
shorter than ARMED_MAX_AGE (6 h), so nothing reds for it. What it costs is TIME, not the alarm: a stall that is
due during such a spell is mailed late by the length of the spell, at the first tick after it in which the
step is reached. It is a delay, not a loss - the state file and the rule do not depend on the spell, and a
spell longer than 6 h reds the cloud check ("no evaluation for 6 h") while CI runs.

STATE. logs/scheduler_stall.json, written atomically, under an OS file lock (released when the holder dies) so
that two guard loops cannot both send. Stored instants more than 5 min in the future (a clock that was
corrected) are discarded. A state that cannot be written mails nothing. The beat carries a summary, with the
mode and the latest would-be mails. The cloud's guard_heartbeat --check-stall-armed FAILS while CI runs when no
evaluation has completed for 6 h; with sending on it also fails on no mail service configured, GitHub
unreadable for 6 h, or a mail due and undelivered for 6 h. In shadow mode those three are printed, not failed:
nothing is meant to be sent. The only time the alarm can be shown to be evaluating is while the scheduler still
works.

MAIL SETTINGS come from os.environ, else from the checkout's .env - only the mail keys (MAIL_KEYS), never the
whole file, and only when a mail is about to go or the settings are reported.

    python tools/scheduler_stall.py             # read-only: what the rule sees now, the mode, the transport
    python tools/scheduler_stall.py --now 2026-10-24T12:00:00Z   # the same, judged at another instant
    python tools/scheduler_stall.py --tick      # what the guard loop does: evaluate, record (shadow) or send
    python tools/scheduler_stall.py --test-mail # OWNER ONLY, by hand: sends one real test mail, whatever the mode
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
ARMED_MAX_AGE = dt.timedelta(hours=6)     # guard_heartbeat --check-stall-armed: how stale an evaluation may be
FUTURE_SLACK = dt.timedelta(minutes=5)
CLOCK_SKEW = dt.timedelta(minutes=10)
EMPTY_EVALUATIONS = 2                     # evaluations in a row a confirmed-empty listing must hold (R1660)
# Seconds since the publish started (the guard loop kills it at 90 s); see the docstring for the measurement.
# A send started at 70 s has 20 s for one POST (normally 1-2 s); a slower one may be killed, which HOLD_OFF covers.
EVAL_START_MAX_S, SEND_START_MAX_S, HARD_STOP_S = 55, 70, 80
GH_TIMEOUT_S = 20
MAX_SENDS_PER_DAY = 4
WOULD_MAIL_KEEP = 100                     # would-be mails kept in the state file (the beat carries the last 5)
STATE_PATH = os.path.join(ROOT, "logs", "scheduler_stall.json")
# THE SEND SWITCH. Absent (the default) = shadow mode: evaluate and record, send nothing. A person creates it.
SEND_FLAG = os.path.join(ROOT, "logs", "SCHEDULER_STALL_SEND")
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


def sending_enabled(flag: str | None = None) -> bool:
    """True only when a person has created the send flag file. Anything else - absent, a directory, unreadable -
    is shadow mode. Never raises."""
    try:
        return os.path.isfile(SEND_FLAG if flag is None else flag)
    except Exception:                                        # noqa: BLE001
        return False


# ------------------------------------------------------------------ the rule
def _stuck(wf: str, runs: dict, now: dt.datetime) -> dict | None:
    """The oldest run of this writer (any trigger) unfinished for more than STALL_AFTER, or None."""
    stuck = [r for r in runs.get(wf, []) if r.get("status") != "completed" and _t(r.get("createdAt"))
             and now - _t(r["createdAt"]) > STALL_AFTER]
    return min(stuck, key=lambda r: r["createdAt"]) if stuck else None


def assess(sched: dict, states: dict, now: dt.datetime, runs: dict | None = None, empty_before=()) -> dict:
    """sched: {workflow: [runs]} SCHEDULED runs only, as gate.read_sched() returns them (so an empty list means
    three empty reads AND no scheduled run in the unfiltered listing); runs: {workflow: [runs]} of any trigger
    (gate._gh_runs), for the unfinished-run rule; states: {workflow: state} (gate._gh_states); empty_before: the
    writers whose listing was confirmed empty on the previous readable evaluation. Pure.

    The unfinished-run rule applies WHATEVER the workflow's state: the gate's in-flight check blocks on an
    unfinished run of a disabled workflow too (review AR-366). Only the scheduled-run rules are skipped for a
    workflow that is not active, because the gate does not wait for its crons.

    Returns {"stalled": [...], "ok": [...], "empty": [...], "notes": [...]}; each writer row is
    {"workflow", "newest": iso | None, "age_h": float | None, "reason": str}. "empty" lists the judged writers
    whose listing is confirmed empty in THIS evaluation (the caller keeps it for the next). Raises ClockSkew."""
    out = {"stalled": [], "ok": [], "empty": [], "notes": []}
    runs = runs or {}
    empty_before = set(empty_before or ())
    for wf in gate.WRITERS:
        state = states.get(wf)
        judged = state is None or state == "active"
        if not judged:
            out["notes"].append(f"{wf} is {state}: the gate does not wait for its crons, so only an unfinished "
                                f"run of it is judged")
        elif state is None:
            out["notes"].append(f"{wf} is not in GitHub's workflow list; the gate still waits for it, so it is judged")
        created = [c for c in (_t(r.get("createdAt")) for r in sched.get(wf, []) if r.get("event") == "schedule")
                   if c is not None]
        newest = max(created) if created else None
        if newest is not None and newest > now + CLOCK_SKEW:
            raise ClockSkew(f"{wf}'s newest scheduled run is {_iso(newest)}, after this machine's clock {_iso(now)}")
        age_h = None if newest is None else round(max(0.0, (now - newest).total_seconds() / 3600), 1)
        row = {"workflow": wf, "newest": _iso(newest), "age_h": age_h}
        r0 = _stuck(wf, runs, now)
        empty = judged and newest is None
        if empty:
            out["empty"].append(wf)
        if empty and wf in empty_before:
            row["reason"] = (f"GitHub lists NO scheduled run, on {EMPTY_EVALUATIONS} evaluations in a row (each: "
                             f"{1 + gate.SCHED_EMPTY_RETRIES} empty reads and no scheduled run in the unfiltered "
                             f"listing; the listing may still be wrong - the gate blocks either way)")
        elif judged and not empty and now - newest > STALL_AFTER:
            row["reason"] = f"newest scheduled run created {row['newest']} ({age_h:.1f} h ago)"
        elif r0 is not None:
            row["stuck_h"] = round((now - _t(r0["createdAt"])).total_seconds() / 3600, 1)
            row["reason"] = (f"a {r0.get('event')} run created {r0['createdAt']} is still {r0.get('status')} "
                             f"after {row['stuck_h']:.1f} h (the gate blocks on it"
                             + ("; scheduled runs queue behind it)" if judged else f", although {wf} is {state})"))
        elif empty:
            out["notes"].append(f"{wf}: GitHub lists no scheduled run on this evaluation (confirmed by "
                                f"{1 + gate.SCHED_EMPTY_RETRIES} reads and the unfiltered listing); not a stall "
                                f"until the next evaluation sees the same (R1660)")
            continue
        else:
            if judged:
                row["reason"] = f"newest scheduled run created {row['newest']} ({age_h:.1f} h ago)"
                out["ok"].append(row)
            continue
        out["stalled"].append(row)
    return out


def _short(row: dict) -> str:
    """The subject's few words for one stalled writer: which rule, and how long."""
    if row.get("stuck_h") is not None:
        return f"a run unfinished for {row['stuck_h']:.0f} h"
    if row.get("age_h") is None:
        return "no scheduled run listed"
    return f"no scheduled run for {row['age_h']:.0f} h"


def _describe(rows: list) -> str:
    return "\n".join(f"  {r['workflow']}: {r['reason']}" for r in rows)


def compose(kind: str, verdict: dict | None, state: dict, now: dt.datetime) -> tuple[str, str]:
    """(subject, text) for one mail. kind: stall | remind | recovered | blind. A stable "[econ] scheduler stall"
    prefix, so the owner can filter it into a folder he reads, never lose it."""
    if kind in ("stall", "remind"):
        names = ", ".join(r["workflow"] for r in verdict["stalled"])
        subject = (f"[econ] scheduler stall{' (reminder)' if kind == 'remind' else ''}: {names} - "
                   + "; ".join(_short(r) for r in verdict["stalled"]))
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
    text += (f"\nSent by tools/scheduler_stall.py from the workstation guard loop; decided at {_iso(now)}. "
             f"To stop these mails at once, delete {SEND_FLAG} on the workstation.\n")
    return subject, text


def step(state: dict, verdict: dict | None, error: str | None, now: dt.datetime) -> tuple[dict, str | None]:
    """The state machine for one evaluation. verdict None = GitHub could not be read (error says why). Pure.
    Returns (state after the evaluation, kind of mail now due or None). What a DELIVERED mail changes is
    on_delivered(); until then the same kind keeps coming due, so an undelivered mail is never lost.

    A stall comes due only when a writer stalled now was ALSO stalled on the previous readable evaluation: the
    confirmation is per writer, so two different writers seen once each (R1660's two empty listings, 3 min
    apart) confirm nothing."""
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
    before = set(s.get("stalled") or [])
    s["unknown_since_utc"], s["blind_alerted"], s["last_error"] = None, False, None
    s["stalled"] = [r["workflow"] for r in verdict["stalled"]]
    s["empty_listing"] = list(verdict.get("empty") or [])
    if verdict["stalled"]:
        s["last_verdict"] = "stalled"
        if _past(s.get("episode_since_utc"), now) is None:
            s["episode_since_utc"] = _iso(now)      # first sighting: confirm on the next evaluation
        if not s.get("alert_active"):
            # confirmed only when the SAME writer was stalled on the previous readable evaluation
            return s, ("stall" if before & set(s["stalled"]) else None)
        last = _past(s.get("last_mail_utc"), now)
        return s, ("remind" if last is None or now - last >= REMIND_EVERY else None)
    s["last_verdict"] = "ok"
    # An onset/reminder send that never reported back (killed mid-send) may have reached the owner: he gets the
    # "over" mail too, rather than a last word that says "stalled".
    attempt = s.get("mail_attempt") if isinstance(s.get("mail_attempt"), dict) else {}
    if s.get("alert_active") or attempt.get("kind") in ("stall", "remind"):
        return s, "recovered"
    s["episode_since_utc"] = None
    return s, None


def on_delivered(state: dict, kind: str, now: dt.datetime) -> dict:
    s = dict(state, last_mail_utc=_iso(now), last_mail_kind=kind)
    if kind == "stall":
        s["alert_active"] = True
    elif kind == "recovered":
        s["alert_active"], s["episode_since_utc"] = False, None
        s.pop("mail_attempt", None)
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


def _lock(path: str):
    """One step at a time, even with two guard loops: an OS lock on an open handle (msvcrt on Windows, flock
    elsewhere). The OS releases it when the holder dies - the loop kills at 90 s - so there is no stale lock to
    judge or take over, and no takeover race. Returns the open handle, or None when another process holds it."""
    lock = path + ".lock"
    os.makedirs(os.path.dirname(lock), exist_ok=True)
    fh = open(lock, "a+b")
    try:
        if os.name == "nt":
            import msvcrt                                    # noqa: PLC0415
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl                                     # noqa: PLC0415
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fh
    except OSError:
        fh.close()
        return None


def _unlock(fh) -> None:
    try:
        if os.name == "nt":
            import msvcrt                                    # noqa: PLC0415
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
    except OSError:
        pass
    fh.close()


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
def _read_github(budget_end: float, sleep=time.sleep):
    """(sched, runs, states, notes) with the gate's own fetchers; every call bounded by GH_TIMEOUT_S and by the
    time left before the guard loop's kill. The scheduled runs come through gate.read_sched() - NEVER the raw
    `_gh_sched` answer - so an empty answer is read again and cross-checked against the unfiltered listing,
    which is therefore read first (R1660)."""
    def t():
        left = budget_end - time.monotonic()
        if left < 2:
            raise TimeoutError("no time left in this guard tick")
        return int(min(GH_TIMEOUT_S, left))
    runs = {wf: gate._gh_runs(wf, timeout=t()) for wf in gate.WRITERS}
    sched, notes = {}, []
    for wf in gate.WRITERS:
        sched[wf], wf_notes = gate.read_sched(wf, lambda w: gate._gh_sched(w, timeout=t()), runs[wf], sleep=sleep)
        notes += wf_notes
    return sched, runs, gate._gh_states(timeout=t()), notes


def tick(now: dt.datetime | None = None, started: float | None = None, read=_read_github, send=send_mail,
         path: str = STATE_PATH, cut_over=None, env=None, flag: str | None = None) -> str:
    """One step, as the guard loop runs it after the beat is published. Never raises. Returns a one-line outcome.
    started: time.monotonic() when the publish began (the loop kills it at 90 s); None = now. flag: the send
    switch's path (None = SEND_FLAG); while it is absent, `send` is NEVER called."""
    started = time.monotonic() if started is None else started
    try:
        now = now or dt.datetime.now(dt.timezone.utc)
        if cut_over is None:
            from core import cutover                         # noqa: PLC0415
            cut_over = cutover.is_cut_over
        if cut_over():
            return "scheduler-stall: not applicable after T0 (the CI writer gate is not asked)"
        lock = _lock(path)
        if lock is None:
            return "scheduler-stall: another step holds the lock - skipped"
        try:
            return _tick_locked(now, started, read, send, path, env, sending_enabled(flag))
        finally:
            _unlock(lock)
    except Exception as e:                                   # noqa: BLE001 - never break the guard tick
        return f"scheduler-stall: check failed ({type(e).__name__}: {str(e)[:160]})"


def _evaluate(state, due, now, started, read, env, notes) -> dict:
    """Step 1. Returns the state after the evaluation, with `mail_due` set, kept or dropped."""
    try:
        got = read(started + HARD_STOP_S)
        sched, runs, states = got[:3]
        read_notes = list(got[3]) if len(got) > 3 else []
        verdict, error = assess(sched, states, now, runs, state.get("empty_listing") or ()), None
        verdict["notes"] = read_notes + verdict["notes"]
    except Exception as e:                                   # noqa: BLE001 - "cannot read" is its own verdict
        verdict, error = None, f"{type(e).__name__}: {e}"
    new, kind = step(state, verdict, error, now)
    new["last_notes"] = [] if verdict is None else verdict["notes"][-8:]
    new.update(mail_setup(mail_env() if env is None else env))
    if kind is None:
        if verdict is not None:       # a blind evaluation says nothing about a mail already due: keep it
            new.pop("mail_due", None)
    elif kind == "blind" and isinstance(due, dict) and due.get("kind") in ("stall", "remind", "recovered"):
        pass      # an undelivered stall-episode mail is not replaced; blind stays due until that one is delivered
    else:
        subject, text = compose(kind, verdict, new, now)
        same = isinstance(due, dict) and due.get("kind") == kind
        new["mail_due"] = {"kind": kind, "subject": subject, "text": text,
                           "since": due.get("since") if same else _iso(now),
                           "sends": due.get("sends", []) if same else []}
    notes.append(f"{new.get('last_verdict')}" + (f" ({', '.join(new.get('stalled') or [])})"
                                                  if new.get("stalled") else "")
                 + (f", {kind} mail due" if kind else ""))
    return new


def _record_would_mail(state, now, notes) -> dict:
    """Shadow mode's 'send': record what would go, and move the state machine as a delivery would. No mail."""
    due = state["mail_due"]
    kind = due["kind"]
    entry = {"kind": kind, "subject": due.get("subject"), "at": _iso(now)}
    would = [w for w in (state.get("would_mail") or []) if isinstance(w, dict)]
    state = dict(state, would_mail=(would + [entry])[-WOULD_MAIL_KEEP:],
                 would_mail_count=int(state.get("would_mail_count") or 0) + 1,
                 last_send={"kind": kind, "at": _iso(now), "result": "shadow"})
    state = on_delivered(state, kind, now)
    state.pop("mail_due", None)
    notes.append(f"SHADOW: {kind} mail would be sent now, NOT sent (sending is off): {entry['subject']}")
    return state


def _send(state, now, started, send, path, notes, sending) -> dict:
    """Step 2. Sends the due mail when nothing holds it - or, in shadow mode, records it. Returns the state."""
    due = state.get("mail_due")
    if not (isinstance(due, dict) and due.get("kind")):
        return state
    kind = due["kind"]
    elapsed = time.monotonic() - started
    if not sending:
        # The same send window as a real send, so the shadow log records the instant a mail would really go.
        if elapsed > SEND_START_MAX_S:
            notes.append(f"SHADOW: {kind} mail due, deferred ({elapsed:.0f} s of the tick gone)")
            return state
        state = _record_would_mail(state, now, notes)
        save_state(state, path)
        return state
    attempt = state.get("mail_attempt") if isinstance(state.get("mail_attempt"), dict) else None
    # The hold-off is for a repeat of the SAME mail; a different kind (the "over" mail after a killed onset) goes.
    at = _past((attempt or {}).get("at"), now) if (attempt or {}).get("kind") == kind else None
    if at is not None and now - at < HOLD_OFF:
        notes.append(f"{kind} mail attempted {attempt.get('at')} never reported back (killed mid-send?) - held "
                     f"until {_iso(at + HOLD_OFF)}")
        return state
    sends = [s for s in (_past(x, now) for x in due.get("sends", [])) if s is not None]
    if sends and now < max(sends) + CHECK_EVERY:
        notes.append(f"{kind} mail not delivered at {_iso(max(sends))} - next try after {_iso(max(sends) + CHECK_EVERY)}")
        return state
    if len([s for s in sends if now - s < dt.timedelta(hours=24)]) >= MAX_SENDS_PER_DAY:
        notes.append(f"{kind} mail tried {MAX_SENDS_PER_DAY} times in 24 h - paused (a Cloudflare timeout after "
                     "acceptance would otherwise repeat a delivered mail)")
        return state
    if elapsed > SEND_START_MAX_S:
        notes.append(f"{kind} mail due, deferred ({elapsed:.0f} s of the tick gone)")
        return state
    due = dict(due, sends=[_iso(s) for s in sends] + [_iso(now)])
    state = dict(state, mail_due=due, mail_attempt={"kind": kind, "at": _iso(now)})
    save_state(state, path)               # the attempt is on disk BEFORE the mail can leave
    result = send(due["subject"], due["text"])
    state.pop("mail_attempt", None)
    state["last_send"] = {"kind": kind, "at": _iso(now), "result": result}
    if result in ("cloudflare", "resend"):
        state = on_delivered(state, kind, now)
        state.pop("mail_due", None)
        notes.append(f"{kind} mail sent ({result})")
    else:
        notes.append(f"{kind} mail NOT sent ({result}) - tried again after {_iso(now + CHECK_EVERY)}")
    save_state(state, path)
    return state


def _tick_locked(now, started, read, send, path, env, sending) -> str:
    state = load_state(path)
    load_error = state.pop("_load_error", None)
    notes = []
    if load_error:
        state["state_reset"] = f"{_iso(now)}: {load_error}"
        notes.append(f"state file was unreadable ({load_error}), started fresh")
    state["mode"] = "send" if sending else "shadow"
    state["last_step_elapsed_s"] = round(time.monotonic() - started, 1)

    # 1. EVALUATE - first, so a held or deferred send never freezes the verdict, recovery or armed-ness.
    last = _past(state.get("last_evaluated_utc"), now)
    if last is not None and now < last + CHECK_EVERY:
        notes.append(f"evaluated {state.get('last_evaluated_utc')}, next after {_iso(last + CHECK_EVERY)}")
    elif time.monotonic() - started > EVAL_START_MAX_S:
        notes.append(f"evaluation skipped ({time.monotonic() - started:.0f} s of the tick gone)")
    else:
        state = _evaluate(state, state.get("mail_due"), now, started, read, env, notes)
    try:
        save_state(state, path)
    except Exception as e:                                   # noqa: BLE001
        return f"scheduler-stall: could not write {path} ({type(e).__name__}) - nothing mailed"

    # 2. SEND (or, in shadow mode, RECORD) a due mail.
    _send(state, now, started, send, path, notes, sending)
    return f"scheduler-stall [{state['mode']}]: " + "; ".join(notes)


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
        return {"never_evaluated": True, "mode": "send" if sending_enabled() else "shadow"}
    if "_load_error" in s:
        return {"unreadable": s["_load_error"]}
    due = s.get("mail_due") if isinstance(s.get("mail_due"), dict) else None
    would = [w for w in (s.get("would_mail") or []) if isinstance(w, dict)]
    return {**{k: s.get(k) for k in ("mode", "last_evaluated_utc", "last_verdict", "stalled", "empty_listing",
                                     "alert_active", "episode_since_utc", "unknown_since_utc", "mail_configured",
                                     "transport", "mail_to", "last_send", "last_step_elapsed_s")},
            "would_mail_count": int(s.get("would_mail_count") or 0),
            "would_mail": [{k: w.get(k) for k in ("kind", "subject", "at")} for w in would[-5:]],
            "mail_due": None if due is None else {"kind": due.get("kind"), "since": due.get("since")}}


def armed_problem(summary, now: dt.datetime) -> str | None:
    """Why the alarm is NOT armed, judged by guard_heartbeat --check-stall-armed while CI runs, or None. In shadow
    mode only "is it evaluating?" is judged - nothing is meant to be sent, so a missing mail service or an
    unsent mail is not a fault. A summary with no `mode` comes from a sending version (PR #191) and is judged
    as sending. A summary absent from the beat (an older publisher) is not judged here."""
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
    if summary.get("mode") == "shadow":
        return None
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


def shadow_report(summary, now: dt.datetime) -> list:
    """The lines the CI armed step prints about a SHADOW-mode alarm: the verdict and the would-be mails, plus the
    conditions that would fail the step once sending is on. Never raises."""
    if not isinstance(summary, dict):
        return []
    out = [f"SHADOW MODE (sends nothing): last evaluation {summary.get('last_evaluated_utc')}, verdict "
           f"{summary.get('last_verdict')}" + (f" {summary.get('stalled')}" if summary.get("stalled") else "")
           + f", would-be mails so far {summary.get('would_mail_count', 0)}, step at "
           f"{summary.get('last_step_elapsed_s')} s, transport {summary.get('transport')}"]
    for w in summary.get("would_mail") or []:
        if isinstance(w, dict):
            out.append(f"  would-be mail {w.get('at')} [{w.get('kind')}]: {w.get('subject')}")
    probe = dict(summary, mode="send")
    bad = armed_problem(probe, now)
    if bad:
        out.append(f"  NOTE: with sending on, this would read NOT ARMED: {bad}")
    return out


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
    mode = "SEND (the flag file exists)" if sending_enabled() else f"SHADOW (sends nothing; flag {SEND_FLAG} absent)"
    print(f"judged at {_iso(now)}; limit {LIMIT_H} h; mode {mode}; mail would go to "
          f"{env.get('DIGEST_TO') or DEFAULT_TO} ({setup['mail_to']}) via {setup['transport']}")
    state = load_state()
    try:
        sched, runs, states, notes = _read_github(time.monotonic() + 120)
        v = assess(sched, states, now, runs, state.get("empty_listing") or ())
    except Exception as e:                                   # noqa: BLE001
        print(f"UNKNOWN: could not read GitHub ({type(e).__name__}: {str(e)[:200]})")
        return 2
    for n in notes + v["notes"]:
        print(f"NOTE: {n}")
    if v["stalled"]:
        print(f"STALLED (more than {LIMIT_H} h):\n{_describe(v['stalled'])}")
    if v["ok"]:
        print(f"ok:\n{_describe(v['ok'])}")
    return 3 if v["stalled"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
