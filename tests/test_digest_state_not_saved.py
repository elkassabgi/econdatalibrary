"""A run whose record was not saved says so in the daily mail, and still gets a health verdict.

WHAT HAPPENED. The run keeps its record (state.db) in one stored file: pulled at the start, saved at the
end, and the save is refused when another writer saved the file in between (updater/run.py, push_state:
never a blind overwrite). On such a morning (run 38407606463, 2026-10-13) two things were missing:

  * the step "Health gate (fail past 2x SLA)" had no `if:`, so GitHub skipped it after the failed step:
    no verdict, and the mail's in-progress list was off;
  * the mail did not say that the record was not saved, that the published data stays, and what is
    lost.

WHAT THIS FILE HOLDS.
  1. The workflow: when each step runs, read from the workflow file and worked out for each kind of
     morning. The health gate runs after a failed step; the two steps that publish to the catalogue
     database still wait for a saved record.
  2. The writer: the real push_state() loses the compare-and-swap against a stand-in store, and what it
     hands to the workflow is what the mail's rule understands.
  3. The mail: both bodies, from a state file written with the repository's StateStore and a verdict
     written by the gate's own main() (the helpers of tests/test_digest_in_progress.py).
"""
from __future__ import annotations

import html as H
import inspect
import os
import re
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import updater.send_digest as sd  # noqa: E402
from updater import health  # noqa: E402
from updater import run as runmod  # noqa: E402

from test_digest_in_progress import CASES, _gate, _send, _write_state  # noqa: E402
from test_updater_phase1 import fake_r2  # noqa: E402, F401  (a fixture: the stand-in store)

REFUSED = ["The record of this run was not saved, because another updater or tool saved its record first.",
           "The data that this run published stays published.",
           "The times and results that this run recorded are lost. These sources are checked again when they "
           "next run. A source that works in parts goes on from its saved place."]


def _other(ended):
    return [f"The step that saves the record of this run {ended}, and this report does not know the cause.",
            "The data that this run published stays published.",
            "If the record was not saved, the times and results that this run recorded are lost, and these "
            "sources are checked again when they next run."]


# ---------------------------------------------------------------- 1. the workflow
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "updater-daily.yml")
with open(WORKFLOW, encoding="utf-8") as _fh:
    STEPS = yaml.safe_load(_fh)["jobs"]["update"]["steps"]

GATE = "Health gate (fail past 2x SLA)"
PUSH = "Push state to R2 (compare-and-swap + dated backup)"
FRESHNESS = "Sync freshness to D1"
CATALOGUE = "Sync new catalog series to D1"
MAIL = "Daily digest email (morning report, like hfdatalibrary's)"
STATUS_WORDS = ("always()", "success()", "failure()", "cancelled()")


def _step(name):
    found = [s for s in STEPS if s.get("name") == name]
    assert len(found) == 1, f"{len(found)} steps are named {name!r} in updater-daily.yml"
    return found[0]


def _index(name):
    return STEPS.index(_step(name))


def runs(name, *, failed_before, pull, dry_run, push="success", cancelled=False, catalog_sync="0"):
    """Does GitHub run this step? Its `if:` is read from the workflow file and worked out for one morning.

    Only the words these steps use are known. Any other word fails the test: a rule this helper cannot
    read must not pass because it was not read. GitHub's own rule is kept: an `if:` without a status
    word (and a step without an `if:`) runs only when every earlier step succeeded."""
    ok_so_far = not failed_before and not cancelled
    cond = _step(name).get("if")
    if cond is None:
        return ok_so_far
    text = " ".join(str(cond).split())
    assert text.startswith("${{") and text.endswith("}}"), text
    text = text[3:-2].strip()
    values = {"always()": True, "success()": ok_so_far, "failure()": failed_before and not cancelled,
              "cancelled()": cancelled, "steps.pull_state.outcome": pull, "steps.push_state.outcome": push,
              "inputs.dry_run": dry_run, "vars.CATALOG_SYNC_ENABLED": catalog_sync, "true": True, "false": False}
    words = re.findall(r"&&|\|\||==|!=|[()]|'[^']*'|[A-Za-z_][A-Za-z0-9_.]*(?:\(\))?", text)
    assert "".join(words) == text.replace(" ", ""), f"a part of this `if:` was not read: {text}"
    out = []
    for w in words:
        if w in ("==", "!=", "(", ")"):
            out.append(w)
        elif w == "&&":
            out.append("and")
        elif w == "||":
            out.append("or")
        elif w.startswith("'"):
            out.append(repr(w[1:-1]))
        else:
            assert w in values, f"this helper does not know the word {w!r} in: {text}"
            out.append(repr(values[w]))
    result = bool(eval(" ".join(out), {"__builtins__": {}}))            # noqa: S307 - words checked above
    if not any(w in STATUS_WORDS for w in words):
        result = ok_so_far and result
    return result


# name of the morning -> what GitHub knows by the time the later steps are reached
REFUSED_PUSH = dict(failed_before=True, pull="success", push="failure", dry_run=None)       # the cron run
UPDATER_FAILED = dict(failed_before=True, pull="success", push="success", dry_run=False)
ALL_WELL = dict(failed_before=False, pull="success", push="success", dry_run=None)
DRY_RUN = dict(failed_before=False, pull="success", push="skipped", dry_run=True)
DRY_RUN_FAILED = dict(failed_before=True, pull="success", push="skipped", dry_run=True)
PULL_FAILED = dict(failed_before=True, pull="failure", push="skipped", dry_run=None)
CANCELLED = dict(failed_before=False, cancelled=True, pull="success", push="cancelled", dry_run=None)


def test_the_helper_reads_the_rules_this_file_did_not_change():
    """The helper against rules whose answers are known: the push step, and a step with no `if:`."""
    assert runs(PUSH, **ALL_WELL) and runs(PUSH, **UPDATER_FAILED) and runs(PUSH, **CANCELLED)
    assert not runs(PUSH, **PULL_FAILED) and not runs(PUSH, **DRY_RUN)
    plain = "Pull state from R2"
    assert _step(plain).get("if") is None
    assert runs(plain, **ALL_WELL) and not runs(plain, **UPDATER_FAILED) and not runs(plain, **CANCELLED)


@pytest.mark.parametrize("morning", [REFUSED_PUSH, UPDATER_FAILED, CANCELLED],
                         ids=["the push was refused", "the updater ended non-zero", "the job was cancelled"])
def test_the_health_gate_runs_after_a_failed_step(morning):
    assert runs(GATE, **morning), "no verdict on the morning that most needs one"


def test_the_health_gate_runs_as_before_when_nothing_failed():
    assert runs(GATE, **ALL_WELL)
    assert runs(GATE, **DRY_RUN), "a dry run that went well had a verdict before this change"


def test_the_health_gate_does_not_run_without_a_pulled_state_or_in_a_failed_dry_run():
    assert not runs(GATE, **PULL_FAILED), "without the pulled state there is nothing of this run to judge"
    assert not runs(GATE, **DRY_RUN_FAILED)
    for pull in ("cancelled", "skipped"):
        assert not runs(GATE, failed_before=True, pull=pull, push="skipped", dry_run=None), pull


def test_the_gate_stands_after_the_push_and_before_the_mail_and_is_the_same_command():
    assert _index(PUSH) < _index(GATE) < _index(MAIL), "the mail reads the verdict; nothing pushes after the gate"
    assert _step(GATE)["run"].strip() == "python -m updater.health --fail-past-2x-sla"
    later = "\n".join(str(s.get("run") or "") for s in STEPS[_index(GATE):])
    assert "--push-state" not in later, "a state push after the gate would save what the gate's run changed"


def test_the_health_gate_can_make_the_run_red():
    """A gate that cannot fail the run is no gate (review AR-310, note N2): the step must not carry
    `continue-on-error`, and it must be the command that exits non-zero past the limit."""
    step = _step(GATE)
    assert "continue-on-error" not in step, "with continue-on-error a red verdict leaves the run green"
    assert "--fail-past-2x-sla" in step["run"]


def test_the_steps_that_publish_the_record_still_wait_for_a_saved_record():
    """They publish freshness from the state file. After a refused push that state was not saved."""
    for name in (FRESHNESS, CATALOGUE):
        for morning in (REFUSED_PUSH, PULL_FAILED, DRY_RUN, CANCELLED):
            assert not runs(name, catalog_sync="1", **morning), (name, morning)
    assert runs(FRESHNESS, **ALL_WELL) and runs(FRESHNESS, **UPDATER_FAILED)
    assert runs(CATALOGUE, catalog_sync="1", **ALL_WELL) and not runs(CATALOGUE, catalog_sync="0", **ALL_WELL)


def test_the_mail_is_sent_on_those_mornings_and_gets_the_three_values():
    for morning in (REFUSED_PUSH, UPDATER_FAILED, PULL_FAILED, ALL_WELL):
        assert runs(MAIL, **morning), morning
    env = _step(MAIL)["env"]
    assert env["PULL_STATE_OUTCOME"] == "${{ steps.pull_state.outcome }}"
    assert env["PUSH_STATE_OUTCOME"] == "${{ steps.push_state.outcome }}"
    assert env["PUSH_STATE_REFUSED"] == "${{ steps.push_state.outputs.%s }}" % runmod.REFUSED_OUTPUT
    by_id = {s["id"]: s for s in STEPS if s.get("id")}
    assert by_id["pull_state"]["run"].strip() == "python -m updater.run --pull-state"
    assert by_id["push_state"]["run"].strip() == "python -m updater.run --push-state"
    src = inspect.getsource(sd.main)
    for name in ("PULL_STATE_OUTCOME", "PUSH_STATE_OUTCOME", "PUSH_STATE_REFUSED"):
        assert f'os.environ.get("{name}")' in src, f"the mail does not read {name}"


def test_the_health_gate_holds_no_code_that_writes_to_a_store():
    """The gate may run after a failed step only because it publishes nothing. Its own file must hold no
    store client, no state push and no child process."""
    src = inspect.getsource(health)
    for word in ("blob", "r2_util", "d1_remote", "boto3", "put_object", "push_state", "subprocess"):
        assert word not in src, f"updater/health.py now holds {word!r}: read the gate's `if:` again"
    assert 'os.path.join(config.STATE_DIR, "health.json")' in inspect.getsource(health.main)


def test_no_test_holds_the_step_output_file_of_the_job_that_runs_the_suite():
    """tests/conftest.py removes GITHUB_OUTPUT: a refused push in a test must not write a step output of
    the pytest step. (On a GitHub runner the variable is set before pytest starts.)"""
    assert "GITHUB_OUTPUT" not in os.environ


# ---------------------------------------------------------------- 2. the writer
def _outputs(path):
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return dict(line.split("=", 1) for line in f.read().splitlines() if line)


def test_a_refused_save_names_itself_and_the_mail_understands_it(fake_r2, monkeypatch, tmp_path):  # noqa: F811
    out = str(tmp_path / "step_output.txt")
    monkeypatch.setenv("GITHUB_OUTPUT", out)
    assert runmod.push_state() == 0                       # stands for the pull: the ETag is recorded
    assert _outputs(out) == {}, "a save that succeeded names no refusal"
    fake_r2.s3.objs[runmod.STATE_KEY] = b"another writer's state"
    assert runmod.push_state() == 2, "the refusal itself is unchanged"
    assert fake_r2.s3.objs[runmod.STATE_KEY] == b"another writer's state"
    got = _outputs(out)
    assert got == {runmod.REFUSED_OUTPUT: runmod.REFUSED_ANOTHER_WRITER}
    assert runmod.REFUSED_ANOTHER_WRITER == sd.PUSH_REFUSED_ANOTHER_WRITER
    assert sd.state_not_saved_note("success", "failure", got[runmod.REFUSED_OUTPUT]) == REFUSED


def test_a_refusal_with_another_cause_is_not_named(fake_r2, monkeypatch, tmp_path):  # noqa: F811
    """Refused the same way (exit 2), but nobody is known to have saved after a pull of this machine."""
    out = str(tmp_path / "step_output.txt")
    monkeypatch.setenv("GITHUB_OUTPUT", out)
    fake_r2.s3.objs[runmod.STATE_KEY] = b"remote exists"            # this machine never pulled
    assert runmod.push_state() == 2 and _outputs(out) == {}
    del fake_r2.s3.objs[runmod.STATE_KEY]
    assert runmod.push_state() == 0                                  # seed, the ETag is recorded
    del fake_r2.s3.objs[runmod.STATE_KEY]                            # the stored object is gone
    assert runmod.push_state() == 2 and _outputs(out) == {}


def test_the_note_never_changes_how_the_save_step_ends(fake_r2, monkeypatch, tmp_path):  # noqa: F811
    assert runmod.push_state() == 0
    fake_r2.s3.objs[runmod.STATE_KEY] = b"another writer's state"
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)              # not in a workflow: nothing is written
    assert runmod.push_state() == 2
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path))              # a folder: the write fails
    assert runmod.push_state() == 2, "a failed note must not change the exit code"
    assert fake_r2.s3.objs[runmod.STATE_KEY] == b"another writer's state"


# ---------------------------------------------------------------- 3. the rule of the mail
@pytest.mark.parametrize("pull,push,refused", [
    (None, None, None), ("", "", ""),                                # a local run, an older workflow file
    ("success", None, None), ("success", "", "another_writer"),      # the save step's result is absent
    (None, "failure", "another_writer"), ("", "failure", ""),        # the pull's result is absent
    ("failure", "skipped", ""), ("cancelled", "failure", "another_writer"),   # the pull did not succeed
    ("success", "success", ""), ("success", "success", "another_writer"),     # the record was saved
    ("success", "neutral", ""), ("success", "failed", "another_writer"),      # not one of GitHub's words
])
def test_nothing_is_said_when_nothing_is_known_or_nothing_is_wrong(pull, push, refused):
    assert sd.state_not_saved_note(pull, push, refused) == []


def test_the_refused_save_is_three_plain_sentences():
    assert sd.state_not_saved_note("success", "failure", "another_writer") == REFUSED
    assert sd.state_not_saved_note(" Success ", "FAILURE", " another_writer ") == REFUSED


# THE WHOLE NOTE, WORD FOR WORD, WRITTEN OUT HERE AND NOT BUILT FROM THE NAMES ABOVE (review AR-310, F1).
# The first form of sentence 3 was "The next run does the work of this run again." It promised what the
# code does not do: a source that works in parts keeps its place in the store and goes on after this run's
# part, and the next run has its own order and budget. A second form said "A later run checks these sources
# again." That is not true of every source that a named dispatch ran outside the live tier: such a source
# may have no later scheduled run. So the words are "when they next run". A change of any word must be made here by hand, against
# the code, and not by a search-and-replace over one shared name.
WORD_FOR_WORD = {
    ("failure", "another_writer"): (
        "The record of this run was not saved, because another updater or tool saved its record first.",
        "The data that this run published stays published.",
        "The times and results that this run recorded are lost. These sources are checked again when they "
        "next run. A source that works in parts goes on from its saved place."),
    ("failure", ""): (
        "The step that saves the record of this run failed, and this report does not know the cause.",
        "The data that this run published stays published.",
        "If the record was not saved, the times and results that this run recorded are lost, and these "
        "sources are checked again when they next run."),
    ("cancelled", ""): (
        "The step that saves the record of this run was stopped before its end, and this report does not "
        "know the cause.",
        "The data that this run published stays published.",
        "If the record was not saved, the times and results that this run recorded are lost, and these "
        "sources are checked again when they next run."),
    ("skipped", ""): (
        "The step that saves the record of this run did not run, and this report does not know the cause.",
        "The data that this run published stays published.",
        "If the record was not saved, the times and results that this run recorded are lost, and these "
        "sources are checked again when they next run."),
}


@pytest.mark.parametrize("push,refused", sorted(WORD_FOR_WORD), ids=["cancelled", "failed", "refused", "skipped"])
def test_the_note_is_these_three_sentences_and_promises_no_repeat_of_the_work(push, refused):
    note = sd.state_not_saved_note("success", push, refused)
    assert tuple(note) == WORD_FOR_WORD[(push, refused)]
    assert len(note) == 3


@pytest.mark.parametrize("push,refused,ended", [
    ("failure", "", "failed"), ("failure", None, "failed"), ("failure", "some other word", "failed"),
    ("cancelled", "", "was stopped before its end"), ("skipped", "", "did not run"),
    ("cancelled", "another_writer", "was stopped before its end"),   # only a FAILED step ended in the refusal
])
def test_any_other_end_of_the_save_step_names_no_cause(push, refused, ended):
    note = sd.state_not_saved_note("success", push, refused)
    assert note == _other(ended)
    text = " ".join(note)
    assert "another" not in text and "was not saved, because" not in text
    assert "If the record was not saved" in text, "the record may be saved: the step can fail after its upload"


# ---------------------------------------------------------------- 4. the whole mail, both bodies
MAIL_CASES = [c for c in CASES if c[0] in ("bls", "abs", "cso")]     # one current, one in progress, one in trouble


def _mail(tmp_path, monkeypatch, capsys, pull, push, refused):
    _write_state(tmp_path, MAIL_CASES)
    _gate(tmp_path, monkeypatch, capsys)
    for name, value in (("PULL_STATE_OUTCOME", pull), ("PUSH_STATE_OUTCOME", push), ("PUSH_STATE_REFUSED", refused)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    return _send(tmp_path, monkeypatch, capsys)


@pytest.mark.parametrize("push,refused,note", [
    ("failure", "another_writer", REFUSED), ("failure", "", _other("failed")),
    ("cancelled", "", _other("was stopped before its end")),
], ids=["refused", "failed", "cancelled"])
def test_both_bodies_say_it_near_the_top(tmp_path, monkeypatch, capsys, push, refused, note):
    sent = _mail(tmp_path, monkeypatch, capsys, "success", push, refused)
    lines = sent["body"].splitlines()
    assert lines[0] == "Run 123: failure" and lines[2] == ""
    assert lines[3:6] == ["  " + s for s in note] and lines[6] == "", lines[:8]
    assert "!! cso" in sent["body"] and sent["body"].index(note[0]) < sent["body"].index("!! cso")
    html = sent["html"]
    block = "<br>".join(H.escape(s) for s in note)
    assert html.count(block) == 1, "the three sentences stand together, once, in the mail people read"
    assert html.index("run 123") < html.index(block) < html.index(">current<") < html.index("Needs attention</h3>")
    for s in note:
        assert sent["body"].count(s) == 1 and html.count(H.escape(s)) == 1, s
    # the two lists and the subject are as they were
    assert sent["subject"] == "econdatalibrary daily: ATTENTION — run=failure, 0 failed, 1 need attention, 1 in progress"
    assert "In progress (normal) (1)" in html and "The in-progress list is off" not in html + sent["body"]


@pytest.mark.parametrize("pull,push,refused", [
    (None, None, None), ("success", "success", ""), ("failure", "skipped", ""), ("success", None, "another_writer"),
], ids=["not known", "saved", "the pull failed", "no result of the save step"])
def test_both_bodies_say_nothing_of_it_otherwise(tmp_path, monkeypatch, capsys, pull, push, refused):
    sent = _mail(tmp_path, monkeypatch, capsys, pull, push, refused)
    for text in (sent["body"], sent["html"]):
        assert "record of this run" not in text and "stays published" not in text
        assert "record was not saved" not in text
    assert sent["body"].splitlines()[3].startswith("  !! cso"), "the first row follows the counts, as before"
