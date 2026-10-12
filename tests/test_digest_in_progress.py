"""The daily report has two lists: "Needs attention" and "In progress (normal)" (owner's order, 2026-10-11).

WHY. On 2026-10-10 (run 38086608492) sources stood under "Needs attention" only because they are too large for
one run ("N sub-unit(s) attempted, none failed; M deferred by budget and taken next tick"), beside rows that
were real trouble. The report read as a failure every morning.

WHAT MUST NEVER HAPPEN. A row with real trouble in the calm list. So the calm list is the health gate's own
ROTATING class, read from the verdict the gate wrote in this run. Review AR-298 refused two earlier cuts: a
list of trouble words (R1538), and the gate's text rule alone (R1540) - the gate also asks the data clock, an
owed re-derive and CSVs owed to the desktop.

THE STATE AND THE VERDICT COME FROM THE WRITERS. The state file is written with the repository's StateStore,
every budget note by the production emitter (finalize()), and health.json by the gate's own main(). The
sources are real registry ids (the gate judges registry entries).
No row is dropped: the two lists together are the old list.
"""
from __future__ import annotations

import datetime as dt
import html as H
import json
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import updater.send_digest as sd  # noqa: E402
from updater import config, health, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import base  # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize  # noqa: E402

NOW = dt.datetime(2026, 10, 11, 12, 0, 0, tzinfo=dt.timezone.utc)
CLOCK = (base.CADENCE_DAYS, base.PARTIAL_RETRY_DAYS)


def _ago(hours, now=NOW):
    return (now - dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def emitted(attempted=15, deferred=130, names=("DS_TOUR_CAP",)):
    """The budget note, from the production emitter."""
    t = Tally()
    for _ in range(attempted):
        t.added_unit(10)
    for i in range(deferred):
        t.deferred_unit(names[i % len(names)] if names else None)
    res = finalize(t, total_rows=1000, last_obs="2026-08-01", source="x", series_cursors={"k": "2026-08-01"})
    assert res.status == "partial", res.status
    assert "deferred by budget and taken next tick" in res.error, res.error
    return res.error


NOTE = emitted()
CRASH_TAIL = "csv_derive crashed (3 of 9 changed series queued): ArrowInvalid('bad parquet')"
TOO_LARGE_TAIL = ("csv coverage note: 3 of 90 flow(s) too large for the cloud derive and 2 not reached inside the "
                  "derive budget — owed to the desktop (csv_desktop_owed), none failed")


# ---------------------------------------------------------------- the small rules
def _verdict(tmp_path, made, sources):
    p = tmp_path / "health.json"
    p.write_text(json.dumps({"generated_utc": made, "sources": sources}), encoding="utf-8")
    return str(p)


def test_the_verdict_of_this_run_gives_the_rotating_sources(tmp_path):
    p = _verdict(tmp_path, _ago(0.1), [{"source": "a", "health": "ROTATING"}, {"source": "b", "health": "RED-DATA"},
                                       {"source": "c", "health": "ATTENTION"}, {"source": "d", "health": "OK"}])
    assert sd.gate_rotating(p, NOW) == ({"a"}, None)


@pytest.mark.parametrize("made", [_ago(6.1), _ago(30), _ago(-0.5), "", None, "not a time"])
def test_a_verdict_that_is_not_of_this_run_is_not_used(tmp_path, made):
    rot, why = sd.gate_rotating(_verdict(tmp_path, made, [{"source": "a", "health": "ROTATING"}]), NOW)
    assert rot is None and why


def test_a_verdict_that_cannot_be_read_is_not_used(tmp_path):
    assert sd.gate_rotating(str(tmp_path / "absent.json"), NOW)[0] is None
    bad = tmp_path / "health.json"
    bad.write_text("{not json", encoding="utf-8")
    assert sd.gate_rotating(str(bad), NOW)[0] is None
    bad.write_text(json.dumps({"generated_utc": _ago(0.1)}), encoding="utf-8")       # no `sources`
    assert sd.gate_rotating(str(bad), NOW)[0] is None


def test_the_attempt_window_is_the_retry_clock_plus_the_margin():
    assert sd.retry_clock() == CLOCK
    assert sd.attempt_window_hours("daily", CLOCK) == pytest.approx(0.9 * 24 + 36)
    for cadence in ("weekly", "monthly", "irregular", "quarterly", "", "no such cadence"):
        assert sd.attempt_window_hours(cadence, CLOCK) == pytest.approx(6.3 * 24 + 36), cadence


@pytest.mark.parametrize("cadence", ["daily", "weekly", "monthly", "irregular"])
def test_a_healthy_rotation_is_calm_every_morning(cadence):
    """Review AR-298 C1, with the scheduler's own rule: one run a day for 30 days; the unit is attempted when
    its retry clock says it is due. With a fixed 36 hours a weekly or monthly rotator was calm 10 mornings of 30."""
    start = dt.datetime(2026, 9, 1, 21, 0, 0, tzinfo=dt.timezone.utc)
    last = start.isoformat()
    calm = 0
    for day in range(1, 31):
        run = start + dt.timedelta(days=day)
        if base._due_after(min(base.CADENCE_DAYS.get(cadence, 7), base.PARTIAL_RETRY_DAYS), last, run):
            last = (run + dt.timedelta(minutes=40)).isoformat()            # the attempt of this run
        report_time = run + dt.timedelta(hours=2, minutes=20)
        calm += sd.is_in_progress("s", "partial", "", last, cadence, {"s"}, CLOCK, report_time)
    assert calm == 30


def _calm(**kw):
    a = dict(source_id="s", status="partial", late="", last_attempt=_ago(2), cadence="daily", rotating={"s"},
             clock=CLOCK, now=NOW)
    a.update(kw)
    return sd.is_in_progress(**a)


def test_each_condition_alone_keeps_a_row_under_needs_attention():
    assert _calm() is True
    assert _calm(rotating=set()) is False                 # the gate does not call the source ROTATING
    assert _calm(rotating={"other"}) is False
    assert _calm(rotating=None) is False                  # no verdict of this run
    assert _calm(clock=None) is False                     # no retry clock
    for status in ("transient_fail", "error", "ok", "running"):
        assert _calm(status=status) is False, status
    assert _calm(late=" LATE") is False and _calm(late=" LATE(local)") is False
    assert _calm(last_attempt=None) is False and _calm(last_attempt="not a time") is False
    assert _calm(last_attempt=_ago(-3)) is False          # a time in the future


def test_the_edge_of_the_attempt_window():
    assert _calm(last_attempt=_ago(57.6)) is True and _calm(last_attempt=_ago(57.7)) is False
    assert _calm(cadence="monthly", last_attempt=_ago(187.2)) is True
    assert _calm(cadence="monthly", last_attempt=_ago(187.3)) is False


def test_the_retry_clock_loads_when_the_file_runs_as_a_script(tmp_path):
    """The workflow runs `python updater/send_digest.py`: only updater/ is on the path and a relative import
    raises. The clock must still load there, or the calm list is off in every real run."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        f"sys.path[:] = [{os.path.join(ROOT, 'updater')!r}] + [p for p in sys.path if p and p != {ROOT!r}]\n"
        "import send_digest as s\n"
        "assert not s.__package__, s.__package__\n"
        "c = s.retry_clock()\n"
        "print('CLOCK', c is not None and c[1])\n", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    out = subprocess.run([sys.executable, "-B", str(probe)], cwd=str(tmp_path), env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-800:]
    assert f"CLOCK {base.PARTIAL_RETRY_DAYS}" in out.stdout, out.stdout


def test_the_report_reads_the_file_the_gate_writes():
    """A drift test: the gate's main() writes health.json into the state folder, where the report looks."""
    import inspect
    src = inspect.getsource(health.main)
    assert 'os.path.join(config.STATE_DIR, "health.json")' in src and "json.dump(report, f" in src
    src = inspect.getsource(sd.main)
    assert 'gate_rotating(os.path.join(os.path.dirname(STATE), "health.json"), _now)' in src
    if not os.environ.get("AQUEDUCT_STATE_DIR"):
        assert os.path.normcase(os.path.dirname(sd.STATE)) == os.path.normcase(config.STATE_DIR)


# ---------------------------------------------------------------- the whole report on a real state and a real verdict
REG = {e["source_id"]: e for e in registry.load().get("sources", [])}

# (source, status, age of the newest observation in days, text, last attempt hours ago, what else the state holds)
CASES = [
    ("bls", "ok", 3, None, 1, None),
    ("abs", "partial", 10, emitted(3, 2, names=("<i>T1</i> & T2",)), 1, None),         # calm; a publisher's text
    ("unsdg", "partial", 10, NOTE, 24 * 5, None),                                       # calm: inside its retry clock
    ("ilostat", "partial", 400, NOTE, 1, None),                                         # the rotation brings no new data
    ("ipea", "partial", 10, NOTE + "; " + TOO_LARGE_TAIL, 1, "desktop"),                # CSVs owed to the desktop
    ("stat_slovenia", "partial", 10, NOTE, 1, "rederive"),                              # a full re-derive is owed
    ("stat_estonia", "partial", 10, NOTE, 1, "desktop"),                                # owed, the plain note
    ("ksh_stadat", "partial", 10, NOTE + "; " + CRASH_TAIL, 1, None),                   # the csv phase crashed
    ("insee_melodi", "partial", 10, NOTE, 24 * 9, None),                                # overdue for its retry clock
    ("cso", "partial", 10, "3/9 sub-unit(s) transient-failed; will retry", 1, None),
    ("dst", "transient_fail", 10, "detect:UNEXPECTED:HTTPError('400')", 1, None),
    ("noaa", "error", 10, "Traceback", 1, None),
]
CALM = ("abs", "unsdg")
TROUBLE = ("ilostat", "ipea", "stat_slovenia", "stat_estonia", "ksh_stadat", "insee_melodi", "cso", "dst", "noaa")
GATE_SAYS_NO = ("ilostat", "ipea", "stat_slovenia", "stat_estonia", "ksh_stadat")       # review AR-298, both passes


def _write_state(tmp_path, cases):
    """A state file from the repository's StateStore, and the gate's verdict of it from the gate's own main()."""
    now = dt.datetime.now(dt.timezone.utc)
    db = str(tmp_path / "state.db")
    store = StateStore(db)
    for sid, status, obs_age, text, hours, owed in cases:
        e = REG[sid]                                                     # a KeyError here: the id left the registry
        tried = _ago(hours, now)
        store.upsert_unit(sid, "_all", strategy=e["strategy"], status=status,
                          last_obs_date=(now - dt.timedelta(days=obs_age)).strftime("%Y-%m-%d"),
                          last_attempt_utc=tried, last_error=text,
                          **({"last_success_utc": tried} if status == "ok" else {}))
        store.upsert_source(sid, strategy=e["strategy"], cadence=e.get("cadence"), status=status,
                            last_attempt_utc=tried)
        if owed == "desktop":
            store.note_csv_desktop_owed(sid, [(f"{sid}:FLOW_{i}", 1000, "too large for the cloud derive")
                                              for i in range(5)])
        if owed == "rederive":
            store.note_full_rederive_owed(sid, vintage="v1", note="fetcher reported no series_cursors")
    store.close()
    return db


def _gate(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state.db"))
    monkeypatch.setattr(sys, "argv", ["updater.health", "--json"])
    health.main()
    capsys.readouterr()
    rep = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    return {r["source"]: r["health"] for r in rep["sources"]}


def _send(tmp_path, monkeypatch, capsys, run_status="failure"):
    monkeypatch.setattr(sd, "STATE", str(tmp_path / "state.db"))
    sent = {}
    import core.status_mail as sm

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"

    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", run_status)
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    sd.main()
    capsys.readouterr()
    assert sent, "the report was not handed to the mail module"
    return sent


@pytest.fixture
def run(tmp_path, monkeypatch, capsys):
    _write_state(tmp_path, CASES)
    gate = _gate(tmp_path, monkeypatch, capsys)
    return gate, _send(tmp_path, monkeypatch, capsys)


def _calm_ids(html):
    p = html.index("In progress (normal) (")
    part = html[p:html.index("Current sources (")]
    return {sid for sid, *_ in CASES if f"<b>{sid}</b>" in part}


def test_the_gate_judges_the_cases_as_the_review_measured(run):
    gate, _ = run
    for sid in GATE_SAYS_NO:
        assert gate[sid] != "ROTATING", (sid, gate[sid])
    assert gate["ilostat"] == "RED-DATA"
    for sid in CALM + ("insee_melodi",):
        assert gate[sid] == "ROTATING", (sid, gate[sid])


def test_no_source_is_calm_that_the_gate_does_not_call_rotating(run):
    """Review AR-298, second pass, B3: the calm list is inside the gate's ROTATING class - on the gate's word."""
    gate, sent = run
    rotating = {sid for sid, h in gate.items() if h == "ROTATING"}
    calm = _calm_ids(sent["html"])
    assert calm <= rotating, calm - rotating
    assert calm == set(CALM)
    assert rotating - calm == {"insee_melodi"}, "ROTATING by the gate, overdue for its retry clock: attention"


def test_the_subject_counts_the_two_lists(run):
    _, sent = run
    assert sent["subject"] == ("econdatalibrary daily: ATTENTION — run=failure, 1 failed, "
                               "8 need attention, 2 in progress")


def test_the_html_body_has_both_lists_and_no_row_twice_or_missing(run):
    _, sent = run
    html = sent["html"]
    assert "In progress (normal) (2)" in html and "Needs attention" in html
    a, p = html.index("Needs attention</h3>"), html.index("In progress (normal) (2)")
    end = html.index("Current sources (")
    assert a < p < end, "the real problems come first, then the calm list, then the current sources"
    attention, progress = html[a:p], html[p:end]
    for sid in TROUBLE:
        assert f"<b>{sid}</b>" in attention and f"<b>{sid}</b>" not in progress, sid
    for sid in CALM:
        assert f"<b>{sid}</b>" in progress and f"<b>{sid}</b>" not in attention, sid
    for sid in CALM + TROUBLE:
        assert html.count(f"<b>{sid}</b>") == 1, sid + " must stand in exactly one list"
    for label in (">in progress<", ">need attention<", ">failed<", ">current<"):
        assert label in html, label
    assert "The in-progress list is off" not in html and "The in-progress list is off" not in sent["body"]


def test_the_calm_list_escapes_the_text_of_a_row(run):
    """A sub-unit name is a publisher's text: it reaches the calm table's detail cell."""
    html = run[1]["html"]
    assert "<i>T1</i>" not in html
    assert "&lt;i&gt;T1&lt;/i&gt; &amp; T2" in html


def test_the_calm_list_says_only_what_the_gate_proves(run):
    for text in (run[1]["html"], run[1]["body"]):
        low = " ".join(text.split()).lower()
        assert "nothing failed, and the health check found nothing else wrong" in low
        assert "in its last run" in low
        assert "next run" not in low and "next one" not in low, "the list must not promise that the rotation moves"


def test_the_text_body_has_both_lists(run):
    body = run[1]["body"]
    assert "1 ok/no_change · 8 need attention · 2 in progress · 1 failed" in body
    head, _, calm = body.partition("in progress (normal)")
    assert calm, "the calm list is missing from the text body"
    calm = calm.split("bls")[0]
    for sid in TROUBLE:
        assert f"!! {sid} " in head and f" {sid} " not in calm, sid
    for sid in CALM:
        assert f".. {sid} " in calm and f"!! {sid} " not in head, sid


def test_a_source_with_one_row_out_has_all_its_rows_under_attention(tmp_path, monkeypatch, capsys):
    """Two rows of one source (a leftover unit): one inside the window, one not. Never one source in two lists."""
    _write_state(tmp_path, [c for c in CASES if c[0] in ("bls", "abs")])
    store = StateStore(str(tmp_path / "state.db"))
    store.upsert_unit("abs", "_old", strategy=REG["abs"]["strategy"], status="partial", last_obs_date="2026-01-01",
                      last_attempt_utc=_ago(24 * 20, dt.datetime.now(dt.timezone.utc)), last_error=NOTE)
    store.close()
    gate = _gate(tmp_path, monkeypatch, capsys)
    sent = _send(tmp_path, monkeypatch, capsys)
    if gate["abs"] == "ROTATING":
        assert "0 in progress" in sent["subject"] and sent["html"].count("<b>abs</b>") == 2
    assert "In progress (normal)" not in sent["html"]


def test_a_report_without_such_rows_has_no_calm_list(tmp_path, monkeypatch, capsys):
    _write_state(tmp_path, [c for c in CASES if c[0] == "bls"])
    _gate(tmp_path, monkeypatch, capsys)
    sent = _send(tmp_path, monkeypatch, capsys, "success")
    assert "In progress (normal)" not in sent["html"] and "in progress (normal)" not in sent["body"]
    assert sent["subject"] == "econdatalibrary daily: OK — 1 sources current"


def test_a_red_run_is_never_all_systems_normal(tmp_path, monkeypatch, capsys):
    """With every row current or calm, a run that ended red still says so."""
    _write_state(tmp_path, [c for c in CASES if c[0] in ("bls", "abs")])
    _gate(tmp_path, monkeypatch, capsys)
    sent = _send(tmp_path, monkeypatch, capsys, "failure")
    assert sent["subject"].startswith("econdatalibrary daily: ATTENTION — run=failure, 0 failed, 0 need attention, "
                                      "1 in progress")
    assert "All systems normal" not in sent["html"]


@pytest.mark.parametrize("how", ["absent", "old", "no clock"])
def test_both_bodies_say_so_when_the_verdict_cannot_be_used(tmp_path, monkeypatch, capsys, how):
    _write_state(tmp_path, [c for c in CASES if c[0] in ("bls", "abs")])
    _gate(tmp_path, monkeypatch, capsys)
    p = tmp_path / "health.json"
    if how == "absent":
        p.unlink()
    elif how == "old":
        rep = json.loads(p.read_text(encoding="utf-8"))
        rep["generated_utc"] = _ago(30, dt.datetime.now(dt.timezone.utc))
        p.write_text(json.dumps(rep), encoding="utf-8")
    else:
        monkeypatch.setattr(sd, "retry_clock", lambda: None)
    sent = _send(tmp_path, monkeypatch, capsys)
    assert "1 need attention, 0 in progress" in sent["subject"]
    said = "The in-progress list is off: the health check's verdict of this run could not be used"
    assert said in sent["body"] and H.escape(said) in sent["html"]
    assert "<b>abs</b>" in sent["html"] and "In progress (normal)" not in sent["html"]
    if how == "absent":
        assert "FileNotFoundError(" in sent["body"]
        assert "No such file or directory'" not in sent["html"], "the reason is escaped in the html body"
