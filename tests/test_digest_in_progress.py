"""The daily report has two lists: "Needs attention" and "In progress (normal)" (owner's order, 2026-10-11).

WHY. On 2026-10-10 (run 38086608492) sources stood under "Needs attention" only because they are too large for
one run ("N sub-unit(s) attempted, none failed; M deferred by budget and taken next tick"), beside rows that
were real trouble. The report read as a failure every morning.

WHAT MUST NEVER HAPPEN. A row with real trouble in the calm list. So the text rule is the health gate's own
(health._deferral_only, an anchored whitelist), the row must have been attempted in the last 36 hours, and it
must not be LATE. Review AR-298 (R1538) failed the first cut, a list of trouble words: it called
"csv_derive crashed (...)" and a zero-attempt note "in progress".

THE TEXTS COME FROM THE WRITER. Every budget note below is built by the production emitter (finalize()), never
typed from a mail: the mail cuts a text at 110 characters. The tails the orchestrator joins on are pinned to
its source text by test_the_crash_tails_are_the_orchestrators_own.
No row is dropped: the two lists together are the old list.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import updater.send_digest as sd  # noqa: E402
from updater import health  # noqa: E402
from updater.strategies.base import NON_FAILURE_NOTES, NOT_HOSTED_NOTE  # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize  # noqa: E402

NOW = dt.datetime(2026, 10, 11, 12, 0, 0, tzinfo=dt.timezone.utc)


def _ago(hours):
    return (NOW - dt.timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


FRESH = _ago(2)


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

# The tails the orchestrator joins on with "; " after a csv phase that crashed (updater/orchestrate.py). Each is
# (the text as the f-string writes it, the fragment that must stand in the orchestrator's source).
CRASH_TAILS = [
    ("csv_derive crashed (3 of 9 changed series queued): ArrowInvalid('bad parquet')",
     'changed series "\n'),
    ("csv_derive crashed (3 of 9 changed series queued): IncompleteRead(512 bytes read)", None),
    ("csv_derive crashed (3 of 9 changed series queued): Exception('boom')", None),
    ("csv_derive crashed (3 of 9 changed series booked as desktop debts): StopIteration()",
     'as desktop debts): "'),
    ("csv_derive crashed and its 40 changed key(s) could not be mapped - recorded as unmapped keys: ArrowInvalid('x')",
     "changed key(s) could not be \""),
    ("csv_derive crashed and its 40 changed key(s) mapped to 0 catalogue ids - recorded as unmapped keys: KeyError('k')",
     "changed key(s) mapped to 0 \""),
]


def test_the_crash_tails_are_the_orchestrators_own():
    """A drift test: the tails above start as the orchestrator's f-strings start, and it still joins with '; '."""
    src = open(os.path.join(ROOT, "updater", "orchestrate.py"), encoding="utf-8").read()
    for frag in ('f"csv_derive crashed ({len(_q)} of {len(changed)} changed series "',
                 'f"queued): " + repr(e)',
                 'f"csv_derive crashed ({len(_ids)} of {len(changed)} changed series booked "',
                 'f"as desktop debts): " + repr(e)',
                 'f"csv_derive crashed and its {len(changed)} changed key(s) could not be "',
                 'f"csv_derive crashed and its {len(changed)} changed key(s) mapped to 0 "'):
        assert src.count(frag) == 1, frag
    assert '"; ".join' in src


# ---------------------------------------------------------------- the rule
def test_the_emitters_own_note_is_in_progress():
    assert sd.is_in_progress("partial", NOTE, "", FRESH, NOW) is True
    assert sd.is_in_progress("partial", emitted(names=()), "", FRESH, NOW) is True
    assert sd.is_in_progress("partial", emitted(1, 1), "", FRESH, NOW) is True


@pytest.mark.parametrize("tail", [
    NON_FAILURE_NOTES[0] + " derive budget spent - 40 of 90 id(s) deferred to csv_retry_queue, none failed",
    NOT_HOSTED_NOTE + " 3 table(s) are listed by the publisher with no data",
])
def test_a_tail_the_gate_calls_a_non_failure_keeps_the_row_calm(tail):
    """ecb carries a csv coverage note on every pass, ksh_stadat a not-hosted note: the gate strips both."""
    assert sd.is_in_progress("partial", NOTE + "; " + tail, "", FRESH, NOW) is True


@pytest.mark.parametrize("tail", [t for t, _ in CRASH_TAILS], ids=[str(i) for i in range(len(CRASH_TAILS))])
def test_a_crashed_csv_phase_needs_attention(tail):
    """Review AR-298 B1. None of these holds the word 'error', 'fail' or 'timeout' by necessity."""
    assert sd.is_in_progress("partial", NOTE + "; " + tail, "", FRESH, NOW) is False


def test_a_run_that_attempted_nothing_needs_attention():
    """Review AR-298 B1: a zero-attempt note is a wedged rotation, not a rotation."""
    zero = NOTE.replace("15 sub-unit(s) attempted", "0 sub-unit(s) attempted", 1)
    assert zero.startswith("0 sub-unit(s) attempted, none failed; 130 deferred")
    assert sd.is_in_progress("partial", zero, "", FRESH, NOW) is False


@pytest.mark.parametrize("extra", [
    "; csv_derive failed 3/10 series [a, b, c]",
    "; csv coherence unmet: fetcher reported no series_cursors for 5 merged obs",
    "; 2/9 sub-unit(s) transient-failed; will retry",
    "; refusing shrink",
    " and one more word",
])
def test_anything_joined_onto_the_note_needs_attention(extra):
    assert sd.is_in_progress("partial", NOTE + extra, "", FRESH, NOW) is False


@pytest.mark.parametrize("status,text,late,attempt", [
    ("transient_fail", NOTE, "", FRESH),                 # not partial
    ("error", NOTE, "", FRESH),
    ("ok", NOTE, "", FRESH),
    ("partial", NOTE, " LATE", FRESH),                   # late for its cadence
    ("partial", NOTE, " LATE(local)", FRESH),
    ("partial", NOTE, "", _ago(37)),                     # review AR-298 B2: not attempted in the last 36 hours
    ("partial", NOTE, "", _ago(24 * 5)),
    ("partial", NOTE, "", None),                         # no attempt time
    ("partial", NOTE, "", "not a time"),
    ("partial", NOTE, "", _ago(-3)),                     # a time in the future
    ("partial", "3/9 sub-unit(s) transient-failed; will retry", "", FRESH),
    ("partial", "", "", FRESH),
    ("partial", None, "", FRESH),
], ids=[str(i) for i in range(13)])
def test_each_condition_alone_keeps_a_row_under_needs_attention(status, text, late, attempt):
    assert sd.is_in_progress(status, text, late, attempt, NOW) is False


def test_the_edge_of_the_attempt_window():
    assert sd.is_in_progress("partial", NOTE, "", _ago(36), NOW) is True
    assert sd.is_in_progress("partial", NOTE, "", _ago(36.01), NOW) is False


def test_the_report_and_the_gate_use_one_rule():
    """For a fresh, not-late partial row the calm list is exactly the gate's deferral-only class."""
    texts = [NOTE, emitted(names=()), NOTE + "; " + CRASH_TAILS[0][0], NOTE + "; " + NON_FAILURE_NOTES[0] + " x",
             NOTE.replace("15 sub", "0 sub", 1), "+5 new rows; csv coherence unmet: x", "", NOTE[:60]]
    for text in texts:
        gate = health._deferral_only([{"status": "partial", "last_error": text}])
        assert sd.is_in_progress("partial", text, "", FRESH, NOW) is gate, text
    assert sd._gate_rule() is health._deferral_only and sd.GATE_RULE_ERROR is None


def test_without_the_gates_rule_no_row_is_in_progress(monkeypatch):
    monkeypatch.setattr(sd, "_gate_rule", lambda: None)
    assert sd.is_in_progress("partial", NOTE, "", FRESH, NOW) is False


def test_the_gates_rule_loads_when_the_file_runs_as_a_script(tmp_path):
    """The workflow runs `python updater/send_digest.py`: only updater/ is on the path and the relative import
    raises. The rule must still load there, or the calm list is silently off in every real run."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys\n"
        f"sys.path[:] = [{os.path.join(ROOT, 'updater')!r}] + [p for p in sys.path if p and p != {ROOT!r}]\n"
        "import send_digest as s\n"
        "assert not s.__package__, s.__package__\n"
        "r = s._gate_rule()\n"
        "print('RULE', r is not None, s.GATE_RULE_ERROR)\n", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    out = subprocess.run([sys.executable, "-B", str(probe)], cwd=str(tmp_path), env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr[-800:]
    assert "RULE True None" in out.stdout, out.stdout


# ---------------------------------------------------------------- the whole report, both bodies
def _state(tmp_path, rows):
    p = tmp_path / "state.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, last_success_utc TEXT, "
                "last_error TEXT, last_attempt_utc TEXT)")
    con.executemany("INSERT INTO unit_state VALUES (?,?,?,?,?,?)", rows)
    con.commit()
    con.close()
    return str(p)


def _real(hours=0.0, days=0.0):
    t = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours, days=days)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _run(tmp_path, monkeypatch, capsys, rows, cadence, run_status="failure"):
    monkeypatch.setattr(sd, "STATE", _state(tmp_path, rows))
    from updater import registry
    monkeypatch.setattr(registry, "load", lambda: {"sources": [
        {"source_id": r[0], "cadence": cadence.get(r[0], "daily"), "live": True} for r in rows]})
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


CALM = ("src_big", "src_big2", "src_tags")
TROUBLE = ("src_big_late", "src_big_old", "src_big_never", "src_crash", "src_zero", "src_trouble", "src_down",
           "src_broken")


@pytest.fixture
def report(tmp_path, monkeypatch, capsys):
    now, old = _real(hours=1), _real(days=30)
    rows = [
        ("src_ok", "ok", "2026-10-09", now, None, now),
        ("src_big", "partial", "2026-12-31", now, NOTE, now),
        ("src_big2", "partial", "2026-12-31", now, emitted(names=()), now),
        ("src_tags", "partial", "2026-12-31", now, emitted(3, 2, names=("<i>T1</i> & T2",)), now),
        ("src_big_late", "partial", "2026-12-31", old, NOTE, old),             # daily, 30 days: LATE
        ("src_big_old", "partial", "2026-12-31", old, NOTE, _real(days=5)),    # monthly, 5 days: not LATE, not this run
        ("src_big_never", "partial", "2026-12-31", old, NOTE, _real(days=90)),  # irregular: never LATE
        ("src_crash", "partial", "2026-12-31", now, NOTE + "; " + CRASH_TAILS[0][0], now),
        ("src_zero", "partial", "2026-12-31", now, NOTE.replace("15 sub", "0 sub", 1), now),
        ("src_trouble", "partial", "2026-01-01", now, "3/9 sub-unit(s) transient-failed; will retry", now),
        ("src_down", "transient_fail", None, now, "detect:UNEXPECTED:HTTPError('400')", now),
        ("src_broken", "error", None, now, "Traceback", now),
    ]
    return _run(tmp_path, monkeypatch, capsys, rows, {"src_big_old": "monthly", "src_big_never": "irregular"})


def test_the_subject_counts_the_two_lists(report):
    assert report["subject"] == ("econdatalibrary daily: ATTENTION — run=failure, 1 failed, "
                                 "7 need attention, 3 in progress")


def test_the_html_body_has_both_lists_and_no_row_twice_or_missing(report):
    html = report["html"]
    assert "In progress (normal) (3)" in html and "Needs attention" in html
    a, p = html.index("Needs attention</h3>"), html.index("In progress (normal) (3)")
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
    assert "the health gate's rule could not be loaded" not in html


def test_the_calm_list_escapes_the_text_of_a_row(report):
    """A sub-unit name is a publisher's text: it reaches the calm table's detail cell."""
    html = report["html"]
    assert "<i>T1</i>" not in html
    assert "&lt;i&gt;T1&lt;/i&gt; &amp; T2" in html


def test_the_calm_list_says_only_what_the_rule_proves(report):
    for text in (report["html"], report["body"]):
        low = " ".join(text.split()).lower()
        assert "each ran in the last 36 hours, attempted a part of its work, and nothing failed" in low
        assert "next run" not in low and "next one" not in low, "the list must not promise that the rotation moves"


def test_the_text_body_has_both_lists(report):
    body = report["body"]
    assert "1 ok/no_change · 7 need attention · 3 in progress · 1 failed" in body
    head, _, calm = body.partition("in progress (normal)")
    assert calm, "the calm list is missing from the text body"
    calm = calm.split("src_ok")[0]
    for sid in TROUBLE:
        assert f"!! {sid} " in head and sid not in calm, sid
    for sid in CALM:
        assert f".. {sid} " in calm and f"!! {sid} " not in head, sid


def test_a_report_without_such_rows_has_no_calm_list(tmp_path, monkeypatch, capsys):
    sent = _run(tmp_path, monkeypatch, capsys, [("src_ok", "ok", "2026-10-09", None, None, None)], {}, "success")
    assert "In progress (normal)" not in sent["html"] and "in progress (normal)" not in sent["body"]
    assert sent["subject"] == "econdatalibrary daily: OK — 1 sources current"


def test_a_red_run_is_never_all_systems_normal(tmp_path, monkeypatch, capsys):
    """With every row current or calm, a run that ended red still says so."""
    now = _real(hours=1)
    rows = [("src_ok", "ok", "2026-10-09", now, None, now), ("src_big", "partial", "2026-12-31", now, NOTE, now)]
    sent = _run(tmp_path, monkeypatch, capsys, rows, {}, "failure")
    assert sent["subject"].startswith("econdatalibrary daily: ATTENTION — run=failure, 0 failed, 0 need attention, "
                                      "1 in progress")
    assert "All systems normal" not in sent["html"]


def test_both_bodies_say_so_when_the_gates_rule_did_not_load(tmp_path, monkeypatch, capsys):
    def no_rule():
        sd.GATE_RULE_ERROR = "ImportError('x <b>')"
        return None

    monkeypatch.setattr(sd, "_gate_rule", no_rule)
    monkeypatch.setattr(sd, "GATE_RULE_ERROR", None)
    now = _real(hours=1)
    rows = [("src_ok", "ok", "2026-10-09", now, None, now), ("src_big", "partial", "2026-12-31", now, NOTE, now)]
    sent = _run(tmp_path, monkeypatch, capsys, rows, {})
    assert "1 need attention, 0 in progress" in sent["subject"]
    import html as H
    said = "The in-progress list is off: the health gate's rule could not be loaded"
    assert said in sent["body"] and H.escape(said) in sent["html"]
    assert "ImportError('x <b>')" not in sent["html"] and "ImportError(&#x27;x &lt;b&gt;&#x27;)" in sent["html"]
    assert "<b>src_big</b>" in sent["html"] and "In progress (normal)" not in sent["html"]
