"""Desktop slot dispatch (PLAN V2, review AR-470): the slot titles, the ONE settled predicate, the gate's slot credit
and marker, the in-workflow guard (tools/slot_check.py), the dispatcher (tools/econ_dispatch.py), the runner's lock
order (tools/run_local_heavy.ps1) and the workflow wiring. Every guard here has a mutant that a test below kills
(the mutant list and their kills are recorded in the PR)."""
from __future__ import annotations

import ast
import datetime as dt
import os
import re
import subprocess
import sys
import time
import uuid

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import ci_writer_gate as g  # noqa: E402
import econ_dispatch as ed  # noqa: E402
import slot_check as sc  # noqa: E402

UTC = dt.timezone.utc


@pytest.fixture(autouse=True)
def _real_gh_unreachable(monkeypatch, tmp_path):
    """F7 (review AR-470 round 2, R1869): no test here, and no mutant of the code it tests, can reach GitHub. Every
    gh call these modules make goes through subprocess.run; a call whose program is gh is refused, the dispatcher's
    real gh and real root are pointed at nothing, and the repo name is bogus."""
    real_run = subprocess.run

    def guarded(argv, *a, **k):
        prog = os.path.basename(str(argv[0] if isinstance(argv, (list, tuple)) else argv)).lower()
        if prog in ("gh", "gh.exe") or (isinstance(argv, (list, tuple)) and "workflow" in argv and "run" in argv):
            raise RuntimeError(f"test isolation: refused to run {argv!r}")
        return real_run(argv, *a, **k)
    for mod in (g, sc, ed):
        monkeypatch.setattr(mod.subprocess, "run", guarded)
    monkeypatch.setattr(g, "REPO", "invalid-owner/none")
    monkeypatch.setattr(ed, "REAL_GH", str(tmp_path / "no-gh.exe"))
    monkeypatch.setattr(ed, "REAL_ROOT", str(tmp_path))
D, H = "updater-daily.yml", "updater-heavy.yml"


def T(s):
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def row(rid, created, event="schedule", title="", status="completed", conclusion="success"):
    return {"databaseId": rid, "createdAt": created, "event": event, "displayTitle": title, "status": status,
            "conclusion": conclusion}


PULLED = [{"name": "update", "steps": [{"name": "Pull state from R2", "conclusion": "success"}]}]
NOT_PULLED = [{"name": "update", "steps": [{"name": "Pull state from R2", "conclusion": "failure"}]}]
GUARD_ONLY = [{"name": "guard", "steps": [{"name": "Slot guard (before any state pull)", "conclusion": "success"}]}]


# ------------------------------------------------------------------ titles and slots
def test_slot_tag_round_trip_and_rejections():
    c = T("2026-11-12T03:00:00Z")
    assert g.slot_tag(H, c) == "heavy-03Z-2026-11-12"
    assert g.parse_slot_tag("heavy-03Z-2026-11-12", H) == c
    assert g.parse_slot_tag("daily-06Z-2026-11-12", D) == T("2026-11-12T06:00:00Z")
    for bad in ("heavy-06Z-2026-11-12", "daily-03Z-2026-11-12", "heavy-03Z-2026-13-01", "", " ",
                "heavy-03Z-2026-11-12x", "heavy-3Z-2026-11-12"):
        assert g.parse_slot_tag(bad, H if bad.startswith("heavy") else D) is None, bad
    assert g.parse_slot_tag("daily-06Z-2026-11-12", H) is None


def test_cron_title_maps_to_its_own_day_across_midnight():
    # a 12Z cron created 00:30Z next day belongs to the previous day's 12Z
    r = row(1, "2026-11-13T00:30:00Z", title="cron 0 12 * * *")
    assert g.run_slot(H, r) == (T("2026-11-12T12:00:00Z"), "cron")
    r = row(2, "2026-11-12T03:00:00Z", title="cron 0 3 * * *")
    assert g.run_slot(H, r) == (T("2026-11-12T03:00:00Z"), "cron")


def test_titles_count_only_on_their_own_event_and_legacy_has_none():
    assert g.run_slot(H, row(1, "2026-11-12T03:01:00Z", event="schedule", title="slot heavy-03Z-2026-11-12")) is None
    assert g.run_slot(H, row(1, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="cron 0 3 * * *")) is None
    assert g.run_slot(H, row(1, "2026-11-12T03:01:00Z", title="updater-heavy")) is None
    assert g.run_slot(H, row(1, "2026-11-12T03:01:00Z", title="")) is None
    assert g.run_slot(D, row(1, "2026-11-12T08:01:00Z", event="workflow_dispatch",
                             title="manual source=ecb force=true dry_run=false")) is None


# ------------------------------------------------------------------ the ONE settled predicate
@pytest.mark.parametrize("status,conclusion,jobs,want", [
    ("completed", "success", PULLED, True),
    ("completed", "failure", PULLED, True),        # the daily health gate reds the run after the push (AR-470 c)
    ("completed", "cancelled", PULLED, False),
    ("completed", "success", NOT_PULLED, False),
    ("completed", "success", GUARD_ONLY, False),   # a no-op: its guard ran, nothing pulled
    ("completed", "success", None, False),         # jobs unreadable
    ("completed", "success", [], False),
    ("in_progress", "", PULLED, False),
    ("in_progress", "success", PULLED, False),   # an unfinished run never settles, whatever it reports
    ("completed", None, PULLED, False),
])
def test_run_settles(status, conclusion, jobs, want):
    assert g.run_settles({"status": status, "conclusion": conclusion}, jobs) is want


def _calls(path, name):
    tree = ast.parse(open(path, encoding="utf-8").read())
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", None) == name]


def test_one_predicate_is_used_by_guard_gate_dispatcher_and_replay():
    """AR-470 H2: guard, gate, dispatcher and replay share ONE predicate, and no one else re-implements it."""
    assert _calls(os.path.join(ROOT, "tools", "slot_check.py"), "run_settles")
    gate_src = open(os.path.join(ROOT, "tools", "ci_writer_gate.py"), encoding="utf-8").read()
    assert gate_src.count("def run_settles(") == 1 and "run_settles(r, fetch_jobs(rid))" in gate_src
    assert _calls(os.path.join(ROOT, "tools", "econ_dispatch.py"), "settled_runs")
    for f in ("slot_check.py", "econ_dispatch.py", "scheduler_stall.py"):
        src = open(os.path.join(ROOT, "tools", f), encoding="utf-8").read()
        assert '"Pull state"' not in src and "'Pull state'" not in src, f"{f} re-implements the pull test"
    # the replay's `pulled` (the predicate's third leg) reads the gate's constant; its older "Pull state" literal
    # times the read offset only
    mcl = open(os.path.join(ROOT, "tools", "measure_ci_lag.py"), encoding="utf-8").read()
    assert "startswith(g.PULL_STEP_PREFIX)" in mcl


# ------------------------------------------------------------------ the gate: slot credit and marker
def _gate(runs_d, runs_h, now, settled=frozenset(), marker=None):
    runs = {D: runs_d, H: runs_h}
    # yesterday's titled cron runs (10, 11, 12) settled their slots
    return g.assess(runs, now, None, runs, (), frozenset(settled) | {10, 11, 12}, marker)


def test_titled_cron_run_credits_only_if_unfinished_or_settled():
    """review AR-470 round 2 F3: an evicted or failed-closed cron run does not read as its cron's start."""
    c = T("2026-11-12T03:00:00Z")
    run = row(30, "2026-11-12T09:00:00Z", title="cron 0 3 * * *")
    assert g.slot_credited(H, 3, c, [run], [run], frozenset({30})) is True
    assert g.slot_credited(H, 3, c, [run], [run], frozenset()) is False
    assert g.slot_credited(H, 3, c, [dict(run, conclusion="cancelled")], [], frozenset({30})) is False
    assert g.slot_credited(H, 3, c, [dict(run, status="in_progress", conclusion="")], [], frozenset()) is True


def _base_history(day="2026-11-12"):
    # yesterday's crons all have their own titled runs, so only today's crons can block
    prev = (T(day + "T00:00:00Z") - dt.timedelta(days=1)).date().isoformat()
    d = [row(10, prev + "T12:00:00Z", title="cron 0 6 * * *")]
    h = [row(11, prev + "T09:00:00Z", title="cron 0 3 * * *"), row(12, prev + "T18:00:00Z", title="cron 0 12 * * *")]
    return d, h


def test_tagged_dispatch_that_settled_credits_its_cron():
    d, h = _base_history()
    h = h + [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")]
    now = T("2026-11-12T05:30:00Z")
    assert _gate(d, h, now, settled=frozenset({20}))[0] is False
    blocked, why, _ = _gate(d, h, now, settled=frozenset())          # same run, not settled: still blocks
    assert blocked and "03:00Z cron has not started" in why


def test_tagged_dispatch_in_flight_blocks_and_cancelled_never_credits():
    d, h = _base_history()
    now = T("2026-11-12T05:30:00Z")
    h1 = h + [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12",
                  status="in_progress", conclusion="")]
    assert _gate(d, h1, now)[0] is True
    h2 = h + [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12",
                  conclusion="cancelled")]
    assert _gate(d, h2, now, settled=frozenset({20}))[0] is True       # cancelled: slot_credited needs settled AND
    assert g.slot_credited(H, 3, T("2026-11-12T03:00:00Z"), h2, h2, frozenset()) is False


def test_titled_run_never_credits_another_cron():
    """AR-470 C7 / AR-426: a 03Z cron run created late inside the 12Z window credits 03Z only."""
    d, h = _base_history()
    d = d + [row(24, "2026-11-12T06:01:00Z", event="workflow_dispatch", title="slot daily-06Z-2026-11-12")]
    h = h + [row(21, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12"),
             row(22, "2026-11-12T12:40:00Z", title="cron 0 3 * * *")]
    now = T("2026-11-12T13:00:00Z")
    blocked, why, _ = _gate(d, h, now, settled=frozenset({21, 24}))
    assert blocked and "updater-heavy.yml 12:00Z cron has not started" in why
    h2 = h + [row(23, "2026-11-12T12:01:00Z", event="workflow_dispatch", title="slot heavy-12Z-2026-11-12")]
    assert _gate(d, h2, now, settled=frozenset({21, 23, 24}))[0] is False


def test_legacy_untitled_history_keeps_the_window_rule():
    d = [row(1, "2026-11-12T11:00:00Z", title="updater-daily")]
    h = [row(2, "2026-11-12T08:00:00Z", title="updater-heavy"), row(3, "2026-11-11T19:00:00Z", title="")]
    now = T("2026-11-12T11:30:00Z")
    assert _gate(d, h, now)[0] is False          # both crons credited by window, as before titles


def test_marker_blocks_ten_minutes_and_future_marker_blocks():
    d, h = _base_history()
    h = h + [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")]
    now = T("2026-11-12T05:30:00Z")
    assert _gate(d, h, now, frozenset({20}), marker=now - dt.timedelta(minutes=9, seconds=59))[0] is True
    assert _gate(d, h, now, frozenset({20}), marker=now - dt.timedelta(minutes=10))[0] is False
    assert _gate(d, h, now, frozenset({20}), marker=now + dt.timedelta(minutes=30))[0] is True


def test_marker_file_never_reads_absent(tmp_path):
    p = tmp_path / "m"
    assert g.read_marker(str(p)) is None
    p.write_text("2026-11-12T05:25:00Z\nupdater-heavy.yml heavy-03Z\n", encoding="utf-8")
    assert g.read_marker(str(p)) == T("2026-11-12T05:25:00Z")
    p.write_text("garbage\n", encoding="utf-8")
    assert g.read_marker(str(p)) is not None


def test_settled_runs_fails_closed_on_unreadable_jobs():
    now = T("2026-11-12T05:30:00Z")
    rows = [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")]

    def boom(_rid):
        raise RuntimeError("gh down")
    s, notes = g.settled_runs({D: [], H: rows}, {D: [], H: []}, now, boom)
    assert s == frozenset() and notes
    s, _ = g.settled_runs({D: [], H: rows}, {D: [], H: []}, now, lambda _r: PULLED)
    assert s == frozenset({20})


def test_gate_main_reads_titles_and_marker(tmp_path):
    assert "displayTitle" in g.RUN_FIELDS and "databaseId" in g.RUN_FIELDS and "conclusion" in g.RUN_FIELDS
    d, h = _base_history("2026-11-12")
    h = h + [row(20, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")]
    lists = {D: d, H: h}
    m = tmp_path / "marker"
    rc = g.main(fetch=lambda wf: lists[wf], fetch_states=lambda: {}, now=T("2026-11-12T05:30:00Z"),
                fetch_sched=lambda wf: lists[wf], sleep=lambda _s: None, fetch_jobs=lambda _r: PULLED,
                marker_path=str(m))
    assert rc == g.EXIT_CLEAR
    m.write_text("2026-11-12T05:25:00Z\n", encoding="utf-8")
    rc = g.main(fetch=lambda wf: lists[wf], fetch_states=lambda: {}, now=T("2026-11-12T05:30:00Z"),
                fetch_sched=lambda wf: lists[wf], sleep=lambda _s: None, fetch_jobs=lambda _r: PULLED,
                marker_path=str(m))
    assert rc == g.EXIT_BLOCKED


# ------------------------------------------------------------------ the in-workflow guard
def _env(**kw):
    e = {"SLOT_WORKFLOW": H, "SLOT_EVENT": "schedule", "SLOT_SCHEDULE": "0 3 * * *", "SLOT_INPUT": "",
         "SLOT_SOURCE": "", "SLOT_FORCE": "false", "SLOT_DRY_RUN": "false", "GITHUB_RUN_ID": "100"}
    e.update(kw)
    e.setdefault("GITHUB_EVENT_NAME", e["SLOT_EVENT"])      # the runner's own variable; must agree (F2)
    return e


# an older run of the workflow: a complete listing always reaches back past the slot's cron (F1)
OLD = row(5, "2026-11-11T09:00:00Z", title="cron 0 3 * * *")


def _decide(env, rows, jobs=None, reads=None, old=True):
    seq = list(reads) if reads is not None else None

    def lister(_wf):
        if seq is not None:
            got = seq.pop(0) if seq else []
            if isinstance(got, Exception):
                raise got
            return got + ([OLD] if old and got else [])
        return list(rows) + ([OLD] if old else [])
    return sc.decide(env, lister, (lambda _r: jobs) if not callable(jobs) else jobs, sleep=lambda _s: None)


OWN_CRON = row(100, "2026-11-12T09:30:00Z", title="cron 0 3 * * *")
DISP = row(90, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")


def test_guard_late_cron_after_settled_dispatch_is_noop():
    real, why = _decide(_env(), [OWN_CRON, DISP], PULLED)
    assert real is False and "settled by run 90" in why


def test_guard_cron_real_when_earlier_did_not_settle():
    assert _decide(_env(), [OWN_CRON, dict(DISP, conclusion="cancelled")], PULLED)[0] is True
    assert _decide(_env(), [OWN_CRON, DISP], NOT_PULLED)[0] is True
    assert _decide(_env(), [OWN_CRON, DISP], GUARD_ONLY)[0] is True      # an earlier no-op does not settle
    assert _decide(_env(), [OWN_CRON], PULLED)[0] is True


def test_guard_other_slots_do_not_count():
    other = row(91, "2026-11-11T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-11")
    other12 = row(92, "2026-11-12T12:01:00Z", event="workflow_dispatch", title="slot heavy-12Z-2026-11-12")
    assert _decide(_env(), [OWN_CRON, other, other12], PULLED)[0] is True


def test_guard_orders_by_database_id_not_created_at():
    # same createdAt; only the LOWER id is earlier
    a = row(100, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")
    b = row(101, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")
    env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="heavy-03Z-2026-11-12", SLOT_SCHEDULE="")
    assert _decide(env, [a, b], PULLED)[0] is True                       # 100: 101 is later
    assert _decide(dict(env, GITHUB_RUN_ID="101"), [a, b], PULLED)[0] is False


@pytest.mark.parametrize("case", ["empty_listing", "own_absent", "earlier_unfinished", "jobs_error",
                                  "bad_tag", "slot_with_source", "slot_with_force", "slot_with_dry_run",
                                  "own_title_missing", "bad_schedule", "other_event", "short_listing",
                                  "own_only", "event_mismatch", "gh_errors"])
def test_guard_fails_closed(case):
    env, rows, jobs = _env(), [OWN_CRON, DISP], PULLED
    reads = None
    if case == "empty_listing":
        reads = [[], [], []]
    elif case == "own_absent":
        rows = [DISP]
    elif case == "earlier_unfinished":
        rows = [OWN_CRON, dict(DISP, status="in_progress", conclusion="")]
    elif case == "jobs_error":
        def jobs(_r):
            raise RuntimeError("gh api failed")
    elif case == "bad_tag":
        env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="heavy-04Z-2026-11-12")
    elif case == "slot_with_source":
        env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="heavy-03Z-2026-11-12", SLOT_SOURCE="un_wpp")
    elif case == "slot_with_force":
        env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="heavy-03Z-2026-11-12", SLOT_FORCE="true")
    elif case == "slot_with_dry_run":
        env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="heavy-03Z-2026-11-12", SLOT_DRY_RUN="true")
    elif case == "own_title_missing":
        rows = [dict(OWN_CRON, displayTitle="updater-heavy"), DISP]
    elif case == "bad_schedule":
        env = _env(SLOT_SCHEDULE="0 4 * * *")
    elif case == "other_event":
        env = _env(SLOT_EVENT="push")
    old = True
    if case == "short_listing":            # a partial answer that starts after the slot's cron, at any length
        rows, old = [OWN_CRON, DISP] + [row(200 + i, "2026-11-12T09:31:00Z", event="workflow_dispatch",
                                            title="manual source=x force=true dry_run=false") for i in range(3)], False
    elif case == "own_only":               # review AR-470 round 2 probe P1: [own run] alone is not complete
        rows, old = [OWN_CRON], False
    elif case == "event_mismatch":         # F2: a wiring slip may not turn a cron into a slotless dispatch
        env = _env(SLOT_EVENT="workflow_dispatch", GITHUB_EVENT_NAME="schedule")
    elif case == "gh_errors":
        reads = [RuntimeError("gh 502")] * 3
    with pytest.raises(sc.FailClosed):
        _decide(env, rows, jobs, reads, old=old)


def test_guard_retries_a_failed_read_inside_its_budget():
    real, _ = _decide(_env(), None, PULLED, reads=[RuntimeError("gh 502"), [OWN_CRON, DISP]])
    assert real is False


def test_guard_listing_limit_is_at_least_fifty():
    assert sc.LISTING_LIMIT >= 50 and sc.LISTING_READS >= 3


def test_guard_env_wiring_in_both_workflows():
    """F2: the env the guard reads is the right context in both workflows (a swapped name would decide wrongly)."""
    for wf, dry in ((D, "${{ inputs.dry_run }}"), (H, "'false'")):
        env = [s for s in _wf(wf)["jobs"]["guard"]["steps"] if s.get("id") == "slot"][0]["env"]
        assert env == {"GH_TOKEN": "${{ github.token }}", "ECON_GH_REPO": "${{ github.repository }}",
                       "SLOT_WORKFLOW": wf, "SLOT_EVENT": "${{ github.event_name }}",
                       "SLOT_SCHEDULE": "${{ github.event.schedule }}", "SLOT_INPUT": "${{ inputs.slot }}",
                       "SLOT_SOURCE": "${{ inputs.source }}", "SLOT_FORCE": "${{ inputs.force }}",
                       "SLOT_DRY_RUN": dry if wf == D else "false"}, wf


def test_guard_rereads_until_own_run_is_listed():
    real, _ = _decide(_env(), None, PULLED, reads=[[], [DISP], [OWN_CRON, DISP]])
    assert real is False


def test_guard_dispatch_without_slot_is_real_without_reading():
    env = _env(SLOT_EVENT="workflow_dispatch", SLOT_INPUT="", SLOT_SOURCE="un_wpp", SLOT_FORCE="true")

    def no_list(_wf):
        raise AssertionError("must not list")
    assert sc.decide(env, no_list, no_list)[0] is True


def test_guard_main_writes_output_and_exit_codes(tmp_path):
    out = tmp_path / "out"
    env = dict(_env(), GITHUB_OUTPUT=str(out))
    assert sc.main(env, lambda _wf: [OWN_CRON, DISP, OLD], lambda _r: PULLED, sleep=lambda _s: None) == 0
    assert out.read_text(encoding="utf-8").strip() == "real=false"
    out.unlink()
    assert sc.main(env, lambda _wf: [], lambda _r: PULLED, sleep=lambda _s: None) == 1
    assert not out.exists()                    # no `real` output: `update` never starts


# ------------------------------------------------------------------ workflows
def _wf(name):
    with open(os.path.join(ROOT, ".github", "workflows", name), encoding="utf-8") as f:
        return yaml.safe_load(f)


def _eval_run_name(expr: str, event: str, schedule: str = "", **inputs) -> str:
    """A small evaluator for the run-name expression (&&, ||, !, ==, format) - pins its semantics offline."""
    body = expr.strip()
    assert body.startswith("${{") and body.endswith("}}")
    body = body[3:-2]
    ctx = {"github_event_name": event, "github_event_schedule": schedule,
           "inputs": {"slot": "", "source": "", "force": False, "dry_run": False, **inputs}}
    py = (body.replace("github.event_name", "C['github_event_name']")
              .replace("github.event.schedule", "C['github_event_schedule']")
              .replace("&&", " and ").replace("||", " or "))
    py = re.sub(r"!(?!=)", " not ", py)
    py = re.sub(r"inputs\.([a-z_]+)", r"C['inputs']['\1']", py)
    py = py.replace("format(", "F(")

    def F(fmt, *args):
        return fmt.format(*[str(a).lower() if isinstance(a, bool) else a for a in args])
    return eval(py, {"C": ctx, "F": F})  # noqa: S307 - our own workflow text


@pytest.mark.parametrize("wf", [D, H])
def test_run_name_tags_only_a_plain_slot_dispatch(wf):
    expr = _wf(wf)["run-name"]
    tag = ("daily-06Z" if wf == D else "heavy-03Z") + "-2026-11-12"
    assert _eval_run_name(expr, "schedule", "0 6 * * *") == "cron 0 6 * * *"
    assert _eval_run_name(expr, "workflow_dispatch", slot=tag) == f"slot {tag}"
    for extra in ({"source": "ecb"}, {"force": True}) + (({"dry_run": True},) if wf == D else ()):
        assert not _eval_run_name(expr, "workflow_dispatch", slot=tag, **extra).startswith("slot "), extra
    assert _eval_run_name(expr, "workflow_dispatch").startswith("manual ")


@pytest.mark.parametrize("wf,first", [(D, "update"), (H, "setup")])
def test_guard_job_comes_first_and_gates_the_work(wf, first):
    w = _wf(wf)
    jobs = w["jobs"]
    assert list(jobs)[0] == "guard"
    guard = jobs["guard"]
    assert guard["permissions"].get("actions") == "read"
    assert guard["outputs"]["real"] == "${{ steps.slot.outputs.real }}"
    steps = guard["steps"]
    assert not any((s.get("name") or "").startswith("Pull state") for s in steps)
    slot = [s for s in steps if s.get("id") == "slot"][0]
    assert "${{" not in slot["run"] and slot["run"].strip() == "python3 tools/slot_check.py"     # AR-470 L3
    assert slot["env"]["SLOT_WORKFLOW"] == wf
    assert jobs[first]["needs"] == "guard"
    assert jobs[first]["if"] == "${{ needs.guard.outputs.real == 'true' }}"
    if wf == H:
        assert jobs["heavy"]["needs"] == "setup" and "needs.setup.result == 'success'" in jobs["heavy"]["if"]
    # AR-470 H4: concurrency stays at WORKFLOW level, and no job declares its own
    assert w["concurrency"]["group"] == "aqueduct-updater" and w["concurrency"]["cancel-in-progress"] is False
    assert not any("concurrency" in j for j in jobs.values())
    on = w.get("on", w.get(True))          # PyYAML reads the key `on` as True
    assert on["workflow_dispatch"]["inputs"]["slot"]["default"] == ""


def test_daily_noop_skips_digest_and_heartbeat_structurally():
    jobs = _wf(D)["jobs"]
    names = [s.get("name", "") for s in jobs["update"]["steps"]]
    assert any(n.startswith("Daily digest") for n in names) and any(n.startswith("Heartbeat commit") for n in names)
    assert not any(n.startswith(("Daily digest", "Heartbeat commit")) for n in
                   [s.get("name", "") for s in jobs["guard"]["steps"]])


def test_gate_writers_match_slot_kinds():
    assert set(g.SLOT_KIND) == set(g.WRITERS)


# ------------------------------------------------------------------ the runner's lock order (AR-470 H1)
def test_runner_takes_the_lock_atomically_then_rereads_the_gate():
    src = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8-sig").read()
    create = src.find("[System.IO.FileMode]::CreateNew")
    reread = src.find("ASK THE GATE AGAIN")
    reread_call = src.find("ci_writer_gate.py", reread)
    pull = src.find('Say "pull-state ..."')
    assert -1 not in (create, reread, reread_call, pull)
    assert create < reread < reread_call < pull
    assert "Set-Content -Path $lockFile" not in src           # the non-atomic write is gone
    seg = src[reread:pull]
    assert "Release-LocalHeavyLock" in seg and "exit 2" in seg and "$gateRc2 -ne 0" in seg


# ------------------------------------------------------------------ the dispatcher
class FakeGh:
    def __init__(self, daily=None, heavy=None, jobs=None, dispatch_ok=True):
        self.lists = {D: list(daily or []), H: list(heavy or [])}
        self.jobmap = jobs or {}
        self.dispatch_ok = dispatch_ok
        self.sent = []
        self.calls = []

    def runs(self, wf):
        self.calls.append(("runs", wf))
        return list(self.lists[wf])

    def sched(self, wf):
        self.calls.append(("sched", wf))
        return [r for r in self.lists[wf] if r.get("event") == "schedule"]

    def jobs(self, rid):
        v = self.jobmap.get(rid, PULLED)
        if isinstance(v, Exception):
            raise v
        return v

    def dispatch(self, wf, inputs):
        self.sent.append((wf, dict(inputs)))
        if self.dispatch_ok:
            self.lists[wf].insert(0, row(9000 + len(self.sent), iso(NOW), event="workflow_dispatch",
                                         title=f"slot {inputs['slot']}" if "slot" in inputs else "manual",
                                         status="queued", conclusion=""))
        return self.dispatch_ok, "ok"


NOW = T("2026-11-12T03:02:00Z")


def _hist():
    """Yesterday fully titled and done; today nothing yet: at 03:02Z the 03Z heavy slot is open."""
    d = [row(10, "2026-11-11T12:00:00Z", title="cron 0 6 * * *")]
    h = [row(11, "2026-11-11T09:00:00Z", title="cron 0 3 * * *"), row(12, "2026-11-11T18:00:00Z",
                                                                       title="cron 0 12 * * *")]
    return d, h


def _disp(tmp_path, gh, now=NOW, lock=False, running=False, dry=False):
    os.makedirs(tmp_path / "logs", exist_ok=True)
    return ed.Dispatcher(str(tmp_path), gh, now=lambda: now, sleep=lambda _s: None, dry_run=dry,
                         lock_present=lambda: lock, pass_running=(lambda: running) if not callable(running)
                         else running)


def test_dispatches_the_open_slot_with_slot_only_and_in_order(tmp_path, monkeypatch):
    monkeypatch.setattr(ed, "LIST_WAIT_S", 0)
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    assert d.look() == 0
    assert gh.sent == [(H, {"slot": "heavy-03Z-2026-11-12"})]
    assert d.trace == ["marker", "lock", "process", "listings", "marker", "dispatch"]     # AR-470 H1 order
    assert os.path.exists(d.paths["marker"])             # kept: the gate blocks 10 min from it


@pytest.mark.parametrize("status", ["queued", "pending", "in_progress", "waiting", "requested"])
@pytest.mark.parametrize("where", [D, H])
def test_m12_never_dispatches_while_a_run_is_unfinished(tmp_path, status, where):
    """AR-394 M12 / R81 / R291: dispatch only into an EMPTY group."""
    d0, h0 = _hist()
    extra = row(50, "2026-11-12T02:50:00Z", event="workflow_dispatch", title="manual", status=status, conclusion="")
    gh = FakeGh(d0 + ([extra] if where == D else []), h0 + ([extra] if where == H else []))
    assert _disp(tmp_path, gh).look() == 0
    assert gh.sent == []


def test_unfinished_run_appearing_after_the_marker_stops_the_dispatch(tmp_path):
    d0, h0 = _hist()
    gh = FakeGh(d0, h0)
    disp = _disp(tmp_path, gh)
    orig = disp.read_listings
    n = {"i": 0}

    def later():
        n["i"] += 1
        if n["i"] == 2:     # the re-read under the marker sees a manual run that was just created
            gh.lists[D].insert(0, row(60, "2026-11-12T03:01:30Z", event="workflow_dispatch",
                                      title="manual source=x force=true dry_run=false", status="queued",
                                      conclusion=""))
        return orig()
    disp.read_listings = later
    disp.look()
    assert gh.sent == [] and not os.path.exists(disp.paths["marker"])


def test_lock_present_or_local_pass_or_failed_check_means_no_dispatch(tmp_path):
    for kw in ({"lock": True}, {"running": True}):
        gh = FakeGh(*_hist())
        d = _disp(tmp_path, gh, **kw)
        d.look()
        assert gh.sent == [], kw
        assert not os.path.exists(d.paths["marker"])

    def boom():
        raise RuntimeError("cim failed")
    gh = FakeGh(*_hist())
    assert _disp(tmp_path, gh, running=boom).look() == 1 and gh.sent == []


def test_marker_is_written_before_the_lock_is_read(tmp_path):
    gh = FakeGh(*_hist())
    seen = {}

    def lock_probe():
        seen["marker_at_lock_read"] = os.path.exists(str(tmp_path / "logs" / "econ_dispatch.marker"))
        return True
    d = ed.Dispatcher(str(tmp_path), gh, now=lambda: NOW, sleep=lambda _s: None, lock_present=lock_probe,
                      pass_running=lambda: False)
    d.look()
    assert seen == {"marker_at_lock_read": True}


def test_empty_listing_is_never_idle(tmp_path):
    gh = FakeGh([], [])
    assert _disp(tmp_path, gh).look() == 1 and gh.sent == []


def test_settled_slot_is_not_dispatched_again(tmp_path):
    d0, h0 = _hist()
    done = row(70, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12")
    gh = FakeGh(d0, h0 + [done], jobs={70: PULLED})
    assert _disp(tmp_path, gh, now=T("2026-11-12T06:30:00Z")).look() == 0
    assert [s for s in gh.sent if s[1].get("slot", "").startswith("heavy-03Z")] == []
    assert gh.sent == [(D, {"slot": "daily-06Z-2026-11-12"})]       # the next open slot instead


def test_failed_or_human_cancelled_dispatch_is_left_to_the_cron(tmp_path):
    d0, h0 = _hist()
    for conc, jobs in (("failure", NOT_PULLED), ("cancelled", GUARD_ONLY), ("failure", []), ("success", []),
                       ("cancelled", RuntimeError("jobs unreadable"))):
        r = row(71, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12",
                conclusion=conc)
        gh = FakeGh(d0, h0 + [r], jobs={71: jobs})
        _disp(tmp_path, gh, now=T("2026-11-12T03:30:00Z")).look()
        assert gh.sent == [], (conc, jobs)


def test_evicted_dispatch_is_redispatched_once(tmp_path):
    d0, h0 = _hist()
    ev = row(72, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12",
             conclusion="cancelled")
    gh = FakeGh(d0, h0 + [ev], jobs={72: []})
    d = _disp(tmp_path, gh, now=T("2026-11-12T03:30:00Z"))
    d.save_state({"slots": {"heavy-03Z-2026-11-12": {"sent": 1}}, "last_dispatch_utc": "2026-11-12T03:00:30Z"})
    d.look()
    assert gh.sent == [(H, {"slot": "heavy-03Z-2026-11-12"})]
    # second eviction: the limit is reached
    ev2 = dict(gh.lists[H][0], status="completed", conclusion="cancelled", databaseId=73)
    gh2 = FakeGh(d0, h0 + [ev, ev2], jobs={72: [], 73: []})
    d2 = _disp(tmp_path, gh2, now=T("2026-11-12T04:30:00Z"))
    d2.look()
    assert gh2.sent == []


def test_unlisted_dispatch_is_never_sent_again(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh, now=T("2026-11-12T03:40:00Z"))
    d.save_state({"slots": {"heavy-03Z-2026-11-12": {"sent": 1}}, "last_dispatch_utc": "2026-11-12T03:01:00Z"})
    d.look()
    assert gh.sent == []


def test_no_dispatch_within_ten_minutes_of_the_last(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh, now=T("2026-11-12T06:05:00Z"))
    d.save_state({"last_dispatch_utc": "2026-11-12T05:58:00Z"})
    d.look()
    assert gh.sent == []


def test_slot_given_up_when_its_cron_window_ends(tmp_path):
    """AR-470 H1: the 03Z heavy slot is open only until 12:00Z (its window), not 20 h."""
    d0, h0 = _hist()
    d0 = d0 + [row(80, "2026-11-12T06:01:00Z", event="workflow_dispatch", title="slot daily-06Z-2026-11-12")]
    gh = FakeGh(d0, h0, jobs={80: PULLED})
    d = _disp(tmp_path, gh, now=T("2026-11-12T11:59:00Z"))
    assert [c for c, wf, _h in d.open_slots(*_listed(gh), T("2026-11-12T11:59:00Z"))] == [T("2026-11-12T03:00:00Z")]
    assert d.open_slots(*_listed(gh), T("2026-11-12T12:00:00Z")) == [(T("2026-11-12T12:00:00Z"), H, 12)]


def _listed(gh):
    return {wf: gh.runs(wf) for wf in g.WRITERS}, {wf: gh.sched(wf) for wf in g.WRITERS}


def test_two_dispatchers_one_critical_section(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    other = ed.FileLock(d.paths["cs"])
    assert other.acquire()
    try:
        d.look()
        assert gh.sent == []
    finally:
        other.release()


def test_one_loop_per_root(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    held = ed.FileLock(d.paths["loop"])
    assert held.acquire()
    try:
        assert d.loop(looks=1, sleep_s=0) == 0
        assert gh.calls == []                      # the second copy left without looking
    finally:
        held.release()


def test_stop_file_and_dry_run_send_nothing(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    open(os.path.join(d.logs, "STOP_DISPATCH"), "w").close()
    d.look()
    assert gh.sent == [] and gh.calls == []
    os.remove(os.path.join(d.logs, "STOP_DISPATCH"))
    gh2 = FakeGh(*_hist())
    d2 = _disp(tmp_path, gh2, dry=True)
    d2.look()
    assert gh2.sent == [] and not os.path.exists(d2.paths["state"])


def test_dispatch_call_failure_is_recorded_and_not_repeated(tmp_path, monkeypatch):
    monkeypatch.setattr(ed, "LIST_WAIT_S", 0)
    gh = FakeGh(*_hist(), dispatch_ok=False)
    d = _disp(tmp_path, gh)
    assert d.look() == 1
    assert d.load_state()["slots"]["heavy-03Z-2026-11-12"]["sent"] == 1
    gh.dispatch_ok = True
    d2 = _disp(tmp_path, gh, now=NOW + dt.timedelta(minutes=30))
    d2.look()
    assert len(gh.sent) == 1                   # never again: the cron serves the slot


def test_manual_waits_for_the_group_then_dispatches_without_slot(tmp_path, monkeypatch):
    monkeypatch.setattr(ed, "LIST_WAIT_S", 0)
    d0, h0 = _hist()
    busy = row(55, "2026-11-12T02:00:00Z", status="in_progress", conclusion="", title="cron 0 3 * * *")
    gh = FakeGh(d0, h0 + [busy])
    d = _disp(tmp_path, gh)
    n = {"i": 0}

    def sleep(_s):
        n["i"] += 1
        if n["i"] == 2:
            busy["status"], busy["conclusion"] = "completed", "success"
    d.sleep = sleep
    assert d.manual(D, {"source": "ecb", "force": "true"}, wait_min=60) == 0
    assert gh.sent == [(D, {"source": "ecb", "force": "true"})]


def test_manual_honours_a_young_marker(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    with open(d.paths["marker"], "w", encoding="utf-8") as f:
        f.write(iso(NOW - dt.timedelta(minutes=2)) + "\n")
    assert d.manual(D, {}, wait_min=0) == 3
    assert gh.sent == []


def _cli(argv, tmp_path):
    """Run the CLI in a CHILD with the real gh made unreachable (F7: review AR-470 round 2 ran a mutant of this very
    test against the real GitHub): REAL_GH -> a missing file, REAL_ROOT -> tmp, the repo -> a bogus name, PATH
    without gh, an empty gh config, and a hard timeout so a mutant that loops cannot hang the suite."""
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import ci_writer_gate as g; g.REPO = 'invalid-owner/none';"
            " import econ_dispatch as ed; ed.REAL_GH = sys.argv[2]; ed.REAL_ROOT = sys.argv[3];"
            " sys.exit(ed.main(sys.argv[4:]))")
    os.makedirs(tmp_path / "ghcfg", exist_ok=True)
    env = {"PATH": os.path.dirname(sys.executable), "GH_CONFIG_DIR": str(tmp_path / "ghcfg"), "GH_TOKEN": "x",
           "SYSTEMROOT": os.environ.get("SYSTEMROOT", ""), "ECON_GH_REPO": "invalid-owner/none"}
    return subprocess.run([sys.executable, "-c", code, os.path.join(ROOT, "tools"), str(tmp_path / "no-gh.exe"),
                           str(tmp_path), *argv], capture_output=True, text=True, timeout=60, env=env)


@pytest.mark.parametrize("argv", [["--now", "2026-11-12T03:00:00Z", "--once"], ["--looks", "1"],
                                  ["--sleep", "0", "--looks", "1"], ["--dry-run"], ["--root", "{tmp}", "--once"]])
def test_cli_refuses_test_parameters_without_gh(argv, tmp_path):
    argv = [a.replace("{tmp}", str(tmp_path)) for a in argv]
    r = _cli(argv, tmp_path)
    assert r.returncode == 2 and "Nothing was done" in r.stderr, (r.returncode, r.stderr[-300:])


@pytest.mark.parametrize("argv", [["--root", "Z:/no/such/dir", "--gh", "x.py", "--once"],
                                  ["--gh", 'a" -b', "--once"], ["--gh", "fake.py -Once", "--once"],
                                  ["--gh", "x.py", "--now", "nope", "--once"],
                                  ["--gh", "x.py", "--now", "2026-11-12T03:00:00Z"]])
def test_cli_refuses_bad_values(argv, tmp_path):
    r = _cli(argv, tmp_path)
    assert r.returncode == 2, (r.returncode, r.stderr[-300:])


@pytest.mark.skipif(os.name != "nt", reason="the process check is a Windows CIM query")
def test_process_check_sees_a_planted_process_and_a_clean_zero():
    """R1859: the checker's own command line contains the pattern; it must exclude itself, and see a real one."""
    pat = f"zz_{uuid.uuid4().hex[:10]}.ps1"
    m0, u0 = ed.local_pass_probe(pat)
    assert m0 == 0 and u0 >= 0                 # unreadable ones (elevated) are counted apart, never as a match
    p = subprocess.Popen(["powershell", "-NoProfile", "-Command", f"Start-Sleep 25 # {pat}"])
    try:
        time.sleep(3)
        assert ed.local_pass_probe(pat)[0] == 1
        assert ed.local_pass_running(pat) is True
    finally:
        p.kill()
        p.wait()


# ------------------------------------------------------------------ scheduler_stall shadow
def test_dispatch_shadow_flags_and_never_mails(tmp_path, monkeypatch):
    import scheduler_stall as ss
    now = T("2026-11-12T05:30:00Z")
    hb = tmp_path / "hb"
    sched = {D: [row(10, "2026-11-11T12:00:00Z", title="cron 0 6 * * *")],
             H: [row(11, "2026-11-11T09:00:00Z", title="cron 0 3 * * *")]}
    out = ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))
    assert out["dispatcher"].startswith("not installed")
    assert any("heavy-03Z-2026-11-12 has no run" in f for f in out["flags"])
    hb.write_text(iso(now - dt.timedelta(minutes=45)) + "\n", encoding="utf-8")
    out = ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))
    assert any("heartbeat" in f for f in out["flags"])
    hb.write_text(iso(now - dt.timedelta(minutes=5)) + "\n", encoding="utf-8")
    sched[H].append(row(30, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12"))
    assert ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))["flags"] == []
    assert ss.dispatch_shadow({D: [], H: []}, None, now, heartbeat=str(hb))["flags"] == []     # empty: no verdict


# ------------------------------------------------------------------ the V2 replay (tools/measure_ci_lag.py)
import measure_ci_lag as mcl  # noqa: E402


def _crun(wf, created, exec_min, event="schedule", pulled=True):
    c = T(created)
    return mcl.CloudRun(wf, int(c.timestamp()) % 10 ** 8 + (1 if wf == D else 2), event, c, "completed", "success",
                        float(exec_min), 0.5, float(exec_min), True, 1, c, c + dt.timedelta(minutes=exec_min), True,
                        pulled=pulled)


def _v2_replay(policy, runs, start="2026-11-12T00:00:00Z", end="2026-11-13T00:00:00Z"):
    book = mcl.UnitBook({}, {}, [], {}, {}, (), ())
    tmpl = {D: _crun(D, "2026-11-10T06:00:00Z", 60), H: _crun(H, "2026-11-10T03:00:00Z", 30)}
    cfg = mcl.ReplayConfig(policy=policy, min_hours=10 ** 9, sample_gate=True)
    return mcl.Replay(cfg, runs, book, T(start), T(end), {D: 8.0, H: 2.0}, tmpl, T("2026-11-01T00:00:00Z"),
                      None).run()


def _hist_day():
    return [_crun(H, "2026-11-11T09:00:00Z", 30), _crun(D, "2026-11-11T12:00:00Z", 60),
            _crun(H, "2026-11-11T18:00:00Z", 30),
            _crun(H, "2026-11-12T09:00:00Z", 40), _crun(D, "2026-11-12T12:30:00Z", 90),
            _crun(H, "2026-11-12T19:00:00Z", 20)]


def test_v2_replay_dispatches_at_the_slot_and_the_late_cron_is_a_noop():
    rp = _v2_replay("v2", _hist_day())
    disp = sorted((r.created, r.wf, r.exec_min) for r in rp.dispatched)
    assert disp == [(T("2026-11-12T03:01:00Z"), H, 40.0), (T("2026-11-12T06:01:00Z"), D, 90.0),
                    (T("2026-11-12T12:01:00Z"), H, 20.0)]      # each at its slot, with its own cron run's minutes
    noops = sorted((s, r.wf) for r, s in rp.v2_noops)
    assert noops == [(T("2026-11-12T09:00:00Z"), H), (T("2026-11-12T12:30:00Z"), D), (T("2026-11-12T19:00:00Z"), H)]
    m = mcl.replay_metrics(rp, 0.0, [])
    assert m["v2"]["by_dispatch"] == 3 and m["v2"]["unserved"] == 0 and m["collisions"] == 0
    assert m["gate_min_day"]["pending"] < 10            # BASE below: hours


def test_v2_replay_control_base_has_no_dispatch_and_real_late_crons():
    rp = _v2_replay("today", _hist_day())
    assert rp.dispatched == [] and rp.v2_noops == []
    assert mcl.replay_metrics(rp, 0.0, [])["gate_min_day"]["pending"] > 600


def test_v2_replay_m12_never_dispatches_into_a_busy_group():
    """A manual run in flight at 03:00Z (pending behind it: another) - the dispatcher waits for an EMPTY group."""
    runs = _hist_day() + [_crun(D, "2026-11-12T02:30:00Z", 60, event="workflow_dispatch"),
                          _crun(D, "2026-11-12T02:40:00Z", 30, event="workflow_dispatch")]
    rp = _v2_replay("v2", runs)
    first = min(r.created for r in rp.dispatched)
    assert first == T("2026-11-12T04:01:00Z")           # 02:30+60 = 03:30, then +30 = 04:00; dispatch the next minute
    for r in rp.dispatched:
        t = r.created - dt.timedelta(minutes=1)
        for _w, s, e, _d, x in rp.done:
            if x is not r:
                assert not (s <= t < e), "dispatched while a run held the group"


# ------------------------------------------------------------------ round-2 survivors (review AR-470 round 2)
def test_unfinished_run_only_in_the_scheduled_listing_blocks_the_dispatch(tmp_path):
    d0, h0 = _hist()
    gh = FakeGh(d0, h0)
    stuck = row(61, "2026-11-11T20:00:00Z", title="cron 0 12 * * *", status="queued", conclusion="")
    gh.sched = lambda wf: [r for r in gh.lists[wf] if r.get("event") == "schedule"] + ([stuck] if wf == H else [])
    _disp(tmp_path, gh).look()
    assert gh.sent == []


def test_a_listing_missing_a_run_seen_before_is_not_trusted(tmp_path):
    gh = FakeGh(*_hist())
    d = _disp(tmp_path, gh)
    d.save_state({"seen_max": {D: 10, H: 99999}})
    assert d.look() == 1 and gh.sent == []
    d.save_state({"seen_max": {D: 10, H: 12}})          # control: the known newest rows are listed
    d.look()
    assert gh.sent == [(H, {"slot": "heavy-03Z-2026-11-12"})]


def test_evicted_dispatch_with_unreadable_jobs_is_not_redispatched(tmp_path):
    d0, h0 = _hist()
    ev = row(72, "2026-11-12T03:01:00Z", event="workflow_dispatch", title="slot heavy-03Z-2026-11-12",
             conclusion="cancelled")
    gh = FakeGh(d0, h0 + [ev], jobs={72: RuntimeError("gh api down")})
    d = _disp(tmp_path, gh, now=T("2026-11-12T03:30:00Z"))
    d.save_state({"slots": {"heavy-03Z-2026-11-12": {"sent": 1}}})
    d.look()
    assert gh.sent == []


def test_one_marker_path_for_gate_and_dispatcher(tmp_path):
    assert ed.Dispatcher(g.ROOT).paths["marker"] == g.MARKER_PATH
    assert os.path.basename(ed.Dispatcher(str(tmp_path)).paths["marker"]) == os.path.basename(g.MARKER_PATH)


def test_process_check_text_pins_self_exclusion_null_rule_and_pattern():
    assert ed.LOCAL_PASS_SCRIPT == "run_local_heavy.ps1"
    assert ed.local_pass_running.__defaults__[0] == "run_local_heavy.ps1"
    assert ed.local_pass_probe.__defaults__[0] == "run_local_heavy.ps1"
    assert "$_.ProcessId -ne $PID" in ed._PS_CHECK                    # R1859
    assert "$null -eq $_.CommandLine }).Count" in ed._PS_CHECK        # an unreadable command line is COUNTED (L-c)
    assert "$null -ne $_.CommandLine -and" in ed._PS_CHECK            # ... separately from a matched one
    assert 'Write-Output \\"$m $u\\"' in ed._PS_CHECK or 'Write-Output "$m $u"' in ed._PS_CHECK
    assert "powershell.exe" in ed._PS_CHECK and "pwsh.exe" in ed._PS_CHECK


def _lock_region():
    src = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8-sig").read()
    a = src.index("# >>> LOCK-THEN-REREAD")
    b = src.index("# <<< LOCK-THEN-REREAD")
    assert a < b < src.index('Say "pull-state ..."')
    return src[a:b]


@pytest.mark.skipif(os.name != "nt", reason="runs the runner's own PowerShell lines")
@pytest.mark.parametrize("case", ["gate_blocks", "gate_clear", "live_lock", "stale_lock"])
def test_runner_lock_then_reread_behaves(case, tmp_path):
    """AR-470 H1 / round 2 F4: the runner's real lines, run: the lock is taken atomically, THEN the gate is read
    again, and a gate that does not say CLEAR releases the lock and stops before the pull."""
    tools = tmp_path / "repo" / "tools"
    tools.mkdir(parents=True)
    code = 3 if case == "gate_blocks" else 0
    (tools / "ci_writer_gate.py").write_text(f"import sys; print('FAKE GATE'); sys.exit({code})", encoding="utf-8")
    lock = tmp_path / "local_heavy.lock"
    lp = "$null"
    if case in ("live_lock", "stale_lock"):
        lock.write_text("4242,99", encoding="ascii")
    if case == "stale_lock":
        lp = "@('4242','99')"
    nl = chr(13) + chr(10)
    head = [ "function Say([string]$m) { Write-Output ('SAY: ' + $m) }", f"$lockFile = '{lock}'",
             "$script:lockFile = $lockFile", f"$lp = {lp}", "$selfHosted = $false", "$SkipCiCheck = $false",
             f"$pythonExe = '{sys.executable}'", f"$repo = '{tmp_path / 'repo'}'"]
    body = nl.join(head) + nl + _lock_region().replace(chr(10), nl) + nl + "Write-Output 'REACHED-PULL'" + nl + "exit 0" + nl
    h = tmp_path / "h.ps1"
    h.write_bytes(bytes([0xEF, 0xBB, 0xBF]) + body.encode("utf-8"))
    r = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(h)],
                       capture_output=True, text=True, timeout=120)
    out = r.stdout
    if case == "gate_blocks":
        assert r.returncode == 2 and "REACHED-PULL" not in out and "ABORT AFTER TAKING THE LOCK" in out, out
        assert "FAKE GATE" in out and not lock.exists()                  # read after the lock; lock released
    elif case == "gate_clear":
        assert r.returncode == 0 and "REACHED-PULL" in out and "re-read with the lock held: CLEAR" in out, out
        assert lock.exists() and lock.read_text(encoding="ascii").split(",")[0].strip().isdigit()
    elif case == "live_lock":
        assert r.returncode == 0 and "REACHED-PULL" not in out and "REFUSING TO START" in out, out
        assert lock.read_text(encoding="ascii").startswith("4242,99")     # never overwritten
        assert "FAKE GATE" not in out
    else:
        assert r.returncode == 0 and "REACHED-PULL" in out, out
        assert not lock.read_text(encoding="ascii").startswith("4242")


def test_v2_replay_gate_blocks_an_uncredited_cron():
    rp = _v2_replay("v2", _hist_day()[:3])           # no cron run of today in the history
    t = T("2026-11-12T03:00:30Z")                     # the dispatcher's run is created only at 03:01
    rec = rp.v2[(H, T("2026-11-12T03:00:00Z"))]
    assert rec["settled_at"] is None or rec["settled_at"] > t
    assert rp._v2_pending(t) is True
    assert rp._v2_pending(T("2026-11-12T02:59:00Z")) is False


# ------------------------------------------------------------------ install hardening (AR-470 round 2: L-a, L-b, L-c)
SLOT03 = "heavy-03Z-2026-11-12"


def _evicted(rid, created="2026-11-12T03:01:00Z"):
    return row(rid, created, event="workflow_dispatch", title=f"slot {SLOT03}", conclusion="cancelled")


def test_la_lost_state_cannot_reset_the_dispatch_limit(tmp_path):
    """L-a: no state file at all, but GitHub lists TWO evicted slot dispatches of the slot: the limit holds."""
    d0, h0 = _hist()
    gh = FakeGh(d0, h0 + [_evicted(72), _evicted(73, "2026-11-12T03:20:00Z")], jobs={72: [], 73: []})
    d = _disp(tmp_path, gh, now=T("2026-11-12T04:30:00Z"))
    assert not os.path.exists(d.paths["state"])
    d.echo = True
    d.look()
    assert gh.sent == []
    assert "listed 2" in open(d.paths["log"], encoding="utf-8").read()


def test_la_control_one_listed_eviction_and_no_state_redispatches_once_and_heals_the_count(tmp_path, monkeypatch):
    monkeypatch.setattr(ed, "LIST_WAIT_S", 0)
    d0, h0 = _hist()
    gh = FakeGh(d0, h0 + [_evicted(72)], jobs={72: []})
    d = _disp(tmp_path, gh, now=T("2026-11-12T03:30:00Z"))
    d.look()
    assert gh.sent == [(H, {"slot": SLOT03})]
    assert d.load_state()["slots"][SLOT03]["sent"] == 2           # max(state 0, listed 1) + 1


def test_la_reset_count_in_state_is_not_trusted_over_the_listing(tmp_path):
    d0, h0 = _hist()
    gh = FakeGh(d0, h0 + [_evicted(72), _evicted(73, "2026-11-12T03:20:00Z")], jobs={72: [], 73: []})
    d = _disp(tmp_path, gh, now=T("2026-11-12T04:30:00Z"))
    d.save_state({"slots": {SLOT03: {"sent": 0}}})
    d.look()
    assert gh.sent == []


def _plant_checkout(root, guard_root=None, runner=None, guard_text=None):
    """A fake checkout with the runner's real path lines and a guard script naming `guard_root`."""
    os.makedirs(os.path.join(root, "tools"), exist_ok=True)
    os.makedirs(os.path.join(root, "logs"), exist_ok=True)
    src = runner if runner is not None else open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"),
                                                 encoding="utf-8-sig").read()
    with open(os.path.join(root, "tools", "run_local_heavy.ps1"), "w", encoding="utf-8") as f:
        f.write(src)
    if guard_text is None:
        guard_text = (f'$root = "{guard_root if guard_root is not None else root}"\n'
                      "$heavy = Join-Path (Join-Path $root 'tools') 'run_local_heavy.ps1'\n")
    if guard_text is not False:
        with open(os.path.join(root, "RELAUNCH_GUARD.ps1"), "w", encoding="utf-8") as f:
            f.write(guard_text)
    return str(root)


def _probs(root, gate_root=None):
    gate_root = root if gate_root is None else gate_root
    return ed.root_problems(root, gate_root=gate_root,
                            gate_marker=os.path.join(gate_root, "logs", os.path.basename(g.MARKER_PATH)))


def test_lb_positive_control_the_desktop_checkout_passes(tmp_path):
    root = _plant_checkout(tmp_path / "prod")
    assert _probs(root) == []


@pytest.mark.parametrize("case", ["no_guard", "guard_elsewhere", "guard_no_root", "guard_no_runner", "no_runner",
                                  "runner_lock_moved", "runner_gate_moved", "other_gate_checkout"])
def test_lb_refuses_any_checkout_but_the_desktop_pass_one(tmp_path, case):
    real = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8-sig").read()
    root = str(tmp_path / "clone")
    gate_root = None
    if case == "no_guard":
        _plant_checkout(root, guard_text=False)
    elif case == "guard_elsewhere":
        _plant_checkout(root, guard_root=str(tmp_path / "prod"))
    elif case == "guard_no_root":
        _plant_checkout(root, guard_text="# no root line\n& run_local_heavy.ps1\n")
    elif case == "guard_no_runner":
        _plant_checkout(root, guard_text=f'$root = "{root}"\n')
    elif case == "no_runner":
        _plant_checkout(root)
        os.remove(os.path.join(root, "tools", "run_local_heavy.ps1"))
    elif case == "runner_lock_moved":
        _plant_checkout(root, runner=real.replace("Join-Path $logDir 'local_heavy.lock'",
                                                  "Join-Path 'C:\\other' 'local_heavy.lock'"))
    elif case == "runner_gate_moved":
        _plant_checkout(root, runner=real.replace("Join-Path $repo 'tools\\ci_writer_gate.py'",
                                                  "Join-Path 'C:\\other' 'tools\\ci_writer_gate.py'"))
    elif case == "other_gate_checkout":
        _plant_checkout(root)
        gate_root = _plant_checkout(tmp_path / "prod")
    probs = _probs(root, gate_root)
    want = {"no_guard": "RELAUNCH_GUARD.ps1 cannot be read", "guard_elsewhere": "starts the desktop pass from",
            "guard_no_root": "has no $root", "guard_no_runner": "does not start run_local_heavy.ps1",
            "no_runner": "has no desktop pass", "runner_lock_moved": "no longer derives its lock",
            "runner_gate_moved": "no longer derives its lock",
            "other_gate_checkout": "is not the checkout this program and its gate run from"}[case]
    assert any(want in p for p in probs), (case, probs)


def test_la_unreadable_sent_count_is_the_limit(tmp_path):
    d0, h0 = _hist()
    gh = FakeGh(d0, h0 + [_evicted(72)], jobs={72: []})
    d = _disp(tmp_path, gh, now=T("2026-11-12T03:30:00Z"))
    d.save_state({"slots": {SLOT03: {"sent": "garbage"}}})
    d.look()
    assert gh.sent == []


def test_lb_marker_must_be_the_gates_marker(tmp_path):
    root = _plant_checkout(tmp_path / "prod")
    probs = ed.root_problems(root, gate_root=root, gate_marker=os.path.join(root, "elsewhere", "econ_dispatch.marker"))
    assert any("marker" in p for p in probs)


def test_lb_dispatcher_uses_the_checked_paths(tmp_path):
    d = ed.Dispatcher(str(tmp_path))
    p = ed.root_paths(str(tmp_path))
    assert d.paths["marker"] == p["marker"] and d.lock_file == p["lock"]
    assert p["lock"] == os.path.join(str(tmp_path), "logs", "local_heavy.lock")
    assert os.path.basename(p["marker"]) == os.path.basename(g.MARKER_PATH)


def test_lb_runner_literals_are_in_the_real_runner():
    src = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8-sig").read()
    assert [lit for lit in ed.RUNNER_PATH_LITERALS if lit not in src] == []


@pytest.mark.parametrize("argv", [["--once"], ["manual", "--workflow", "updater-daily.yml", "--wait-min", "0"], []])
def test_lb_cli_refuses_to_start_outside_the_desktop_checkout(argv, tmp_path):
    """The real-gh CLI from a root that is not the desktop pass's checkout exits 2 before any gh call."""
    r = _cli(argv, tmp_path)
    assert r.returncode == 2 and "not the checkout the desktop pass runs from" in r.stderr, (r.returncode,
                                                                                            r.stderr[-400:])


def test_lb_cli_dry_run_only_warns(tmp_path):
    r = _cli(["--once", "--dry-run"], tmp_path)
    assert "WARNING (dry run, nothing is sent)" in r.stderr, r.stderr[-400:]
    assert "Nothing was done" not in r.stderr


def test_lb_guard_copy_and_launcher_draft_name_the_same_checkout():
    guard = open(os.path.join(ROOT, "tools", "machine", "RELAUNCH_GUARD.ps1.workstation-copy"),
                 encoding="utf-8-sig").read()
    m = ed._GUARD_ROOT_RE.search(guard)
    assert m and "run_local_heavy.ps1" in guard
    draft = open(os.path.join(ROOT, "tools", "machine", "EconSlotDispatch.cmd.draft"), encoding="ascii").read()
    prod = m.group(1)
    assert f"'{prod}\\tools\\econ_dispatch.py' loop" in draft
    assert f"Set-Location '{prod}'" in draft


def _probe_disp(tmp_path, gh, probe, now=NOW):
    os.makedirs(tmp_path / "logs", exist_ok=True)
    return ed.Dispatcher(str(tmp_path), gh, now=lambda: now, sleep=lambda _s: None, lock_present=lambda: False,
                         pass_probe=probe)


def _loud_lines(d):
    try:
        return [ln for ln in open(d.paths["log"], encoding="utf-8") if "LOUD:" in ln]
    except FileNotFoundError:
        return []


def test_lc_unreadable_command_line_blocks_loudly_and_rate_limited(tmp_path):
    gh = FakeGh(*_hist())
    d = _probe_disp(tmp_path, gh, lambda: (0, 2))
    assert d.look() == 0 and gh.sent == []                         # fail safe
    assert not os.path.exists(d.paths["marker"])
    assert len(_loud_lines(d)) == 1 and "V2 IS STALLED" in _loud_lines(d)[0]
    pc = d.load_state()["process_check"]
    assert pc["state"] == "unreadable" and pc["unreadable_procs"] == 2 and pc["blocked_looks"] == 1
    for minutes in (1, 30, 59):                                    # inside the hour: no second loud line
        d2 = _probe_disp(tmp_path, FakeGh(*_hist()), lambda: (0, 2), now=NOW + dt.timedelta(minutes=minutes))
        d2.look()
    assert len(_loud_lines(d)) == 1
    assert d.load_state()["process_check"]["blocked_looks"] == 4
    assert d.load_state()["process_check"]["unreadable_since"] == iso(NOW)
    d3 = _probe_disp(tmp_path, FakeGh(*_hist()), lambda: (0, 2), now=NOW + dt.timedelta(minutes=61))
    d3.look()
    assert len(_loud_lines(d)) == 2


def test_lc_heartbeat_carries_the_field_and_clears_when_readable(tmp_path, monkeypatch):
    monkeypatch.setattr(ed, "LIST_WAIT_S", 0)
    d = _probe_disp(tmp_path, FakeGh(*_hist()), lambda: (0, 1))
    d.look()
    d.heartbeat()
    lines = open(d.paths["heartbeat"], encoding="utf-8").read().splitlines()
    assert lines[0] == iso(NOW)
    import json as _json
    f = _json.loads(lines[1])
    assert f["process_check"] == "unreadable" and f["unreadable_since"] == iso(NOW) and f["unreadable_procs"] == 1
    gh = FakeGh(*_hist())
    d2 = _probe_disp(tmp_path, gh, lambda: (0, 0), now=NOW + dt.timedelta(minutes=5))
    d2.look()
    assert gh.sent == [(H, {"slot": SLOT03})]                      # control: a readable check dispatches
    assert "readable again" in open(d2.paths["log"], encoding="utf-8").read()
    d2.heartbeat()
    f2 = _json.loads(open(d2.paths["heartbeat"], encoding="utf-8").read().splitlines()[1])
    assert f2["process_check"] == "clear"


def test_lc_matched_process_is_not_reported_as_unreadable(tmp_path):
    d = _probe_disp(tmp_path, FakeGh(*_hist()), lambda: (1, 3))
    d.look()
    assert _loud_lines(d) == [] and "process_check" not in d.load_state()


def test_lc_slot_alarm_shadow_reads_the_field(tmp_path):
    import scheduler_stall as ss
    now = T("2026-11-12T05:30:00Z")
    hb = tmp_path / "hb"
    sched = {D: [row(10, "2026-11-11T12:00:00Z", title="cron 0 6 * * *")],
             H: [row(11, "2026-11-11T09:00:00Z", title="cron 0 3 * * *"),
                 row(30, "2026-11-12T03:01:00Z", event="workflow_dispatch", title=f"slot {SLOT03}")]}
    fresh = iso(now - dt.timedelta(minutes=1))
    hb.write_text(fresh + '\n{"process_check": "clear", "checked_utc": "' + fresh + '"}\n', encoding="utf-8")
    assert ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))["flags"] == []          # control
    hb.write_text(fresh + "\n", encoding="utf-8")                                        # pre-field dispatcher
    assert ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))["flags"] == []
    hb.write_text(fresh + '\n{"process_check": "unreadable", "unreadable_since": "2026-11-12T01:00:00Z", '
                  '"unreadable_procs": 2, "blocked_looks": 7, "last_seen_utc": "' + fresh + '"}\n', encoding="utf-8")
    out = ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))
    assert any("BLOCKED since 2026-11-12T01:00:00Z" in f for f in out["flags"]), out
    assert "UNREADABLE" in out["dispatcher"] and out["mode"].startswith("shadow")
    hb.write_text(fresh + "\nnot json\n", encoding="utf-8")
    assert any("line 2" in f for f in ss.dispatch_shadow(sched, None, now, heartbeat=str(hb))["flags"])


def test_lc_shadow_reads_what_the_dispatcher_writes(tmp_path):
    """End to end: the dispatcher's own heartbeat file, read by the shadow."""
    import scheduler_stall as ss
    d = _probe_disp(tmp_path, FakeGh(*_hist()), lambda: (0, 1))
    d.look()
    d.heartbeat()
    sched = {D: [row(10, "2026-11-11T12:00:00Z", title="cron 0 6 * * *")], H: _hist()[1]}
    out = ss.dispatch_shadow(sched, None, NOW + dt.timedelta(minutes=1), heartbeat=d.paths["heartbeat"])
    assert any("BLOCKED" in f for f in out["flags"]), out


@pytest.mark.skipif(os.name != "nt", reason="a Windows named mutex")
def test_mutex_allows_only_one_loop(tmp_path):
    name = f"Local\\EconSlotDispatchLoop_test_{uuid.uuid4().hex[:12]}"
    held = ed.NamedMutex(name)
    assert held.acquire()
    try:
        gh = FakeGh(*_hist())
        d = _disp(tmp_path, gh)
        d.mutex_name = name
        assert d.loop(looks=1, sleep_s=0) == 0
        assert gh.calls == []                                      # left without looking
        assert "holds the loop mutex" in open(d.paths["log"], encoding="utf-8").read()
    finally:
        held.release()
    gh2 = FakeGh(*_hist())                                         # control: released, the loop looks
    d2 = _disp(tmp_path, gh2)
    d2.mutex_name = name
    d2.loop(looks=1, sleep_s=0)
    assert gh2.calls != []
    again = ed.NamedMutex(name)                                    # and the loop released it on the way out
    assert again.acquire()
    again.release()


def test_mutex_name_is_one_per_root_written_one_way(tmp_path):
    a = ed.mutex_name_for(str(tmp_path))
    assert a == ed.mutex_name_for(str(tmp_path) + os.sep)
    assert a.startswith("Local\\EconSlotDispatchLoop_") and a != ed.mutex_name_for(str(tmp_path / "x"))
    assert ed.Dispatcher(str(tmp_path)).mutex_name == a


def test_launcher_draft_header_and_command():
    p = os.path.join(ROOT, "tools", "machine", "EconSlotDispatch.cmd.draft")
    txt = open(p, encoding="ascii").read()                        # ASCII only (R196)
    head = [ln for ln in txt.splitlines() if ln.startswith("rem")]
    body = [ln for ln in txt.splitlines() if ln.strip() and not ln.startswith("rem") and ln.strip() != "@echo off"]
    joined = "\n".join(head)
    for word in ("NOT INSTALLED", "Stop switch", "STOP_DISPATCH", "Heartbeat", "econ_dispatch.heartbeat", "Log ",
                 "econ_dispatch.log", "Remove", "mutex", "LOG-ON"):
        assert word in joined, word
    assert len(body) == 1 and body[0].startswith('start "" powershell -NoProfile -ExecutionPolicy Bypass '
                                                 '-WindowStyle Hidden -Command ')
    for bad in ("--gh", "--root", "--now", "--looks", "--sleep", "--dry-run", "--once", "manual"):
        assert bad not in body[0], bad
    assert body[0].count("econ_dispatch.py' loop") == 1
    assert not os.path.basename(p).endswith(".cmd")               # a draft cannot run from Startup by accident


# ------------------------------------------------------------------ AR-470 round 3 (R1875): the probe fails CLOSED
class _Out:
    def __init__(self, returncode, stdout, stderr=""):
        self.returncode, self.stdout, self.stderr = returncode, stdout, stderr


@pytest.mark.parametrize("out", [_Out(1, "0 0\n", "cim failed"), _Out(0, "5\n"), _Out(0, "x 0\n"), _Out(0, "")],
                         ids=["returncode_1", "one_field", "non_digit", "empty_stdout"])
def test_probe_raises_on_output_it_cannot_measure(monkeypatch, out):
    """An answer the probe cannot read must raise, never read as (0, 0) = "no pass running" (R503 class)."""
    monkeypatch.setattr(ed, "_IS_WINDOWS", True)
    calls = []

    def fake_run(argv, *a, **k):
        calls.append(argv)
        return out
    monkeypatch.setattr(ed.subprocess, "run", fake_run)
    with pytest.raises(RuntimeError, match="process check failed"):
        ed.local_pass_probe("run_local_heavy.ps1")
    with pytest.raises(RuntimeError):
        ed.local_pass_running("run_local_heavy.ps1")
    assert calls and calls[0][0] == "powershell"


def test_probe_control_reads_two_counts(monkeypatch):
    monkeypatch.setattr(ed, "_IS_WINDOWS", True)
    monkeypatch.setattr(ed.subprocess, "run", lambda *a, **k: _Out(0, "noise\n1 2\n"))
    assert ed.local_pass_probe() == (1, 2)


def test_probe_that_raises_means_no_dispatch_and_a_log_line(tmp_path, monkeypatch):
    """The real probe, fed unreadable output, inside a look: logged, return 1, and gh gets no `workflow run`."""
    monkeypatch.setattr(ed, "_IS_WINDOWS", True)
    real_run = ed.subprocess.run

    def fake_run(argv, *a, **k):
        if argv and argv[0] == "powershell":
            return _Out(0, "garbage\n")
        return real_run(argv, *a, **k)
    monkeypatch.setattr(ed.subprocess, "run", fake_run)
    gh = FakeGh(*_hist())
    os.makedirs(tmp_path / "logs", exist_ok=True)
    d = ed.Dispatcher(str(tmp_path), gh, now=lambda: NOW, sleep=lambda _s: None, lock_present=lambda: False)
    assert d._pass_probe is ed.local_pass_probe            # the REAL probe, not an injected one
    assert d.look() == 1
    assert gh.sent == []                                     # no `workflow run` reached the fake gh
    log = open(d.paths["log"], encoding="utf-8").read()
    assert "process check failed" in log and "not now" in log
    assert not os.path.exists(d.paths["marker"])            # the marker of an unsent dispatch is removed


def test_probe_off_windows_raises_never_reads_as_idle(monkeypatch):
    monkeypatch.setattr(ed, "_IS_WINDOWS", False)
    with pytest.raises(RuntimeError, match="needs Windows"):
        ed.local_pass_probe()
