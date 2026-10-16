"""The budget-deferral note changed its tail; the gate must read the old rows and the new ones alike.

finalize() wrote "N sub-unit(s) attempted, none failed; M deferred by budget and taken next tick". That
promise is false for a unit that has never succeeded: base.Strategy.is_due makes it wait min(cadence, 7) x
0.9 days after a partial (ledger R1558, R1096). The note now ends "..., not reached in this pass", which
promises neither a run nor completion (AR-328/R1586: ilostat's deferred tail is never reached).

health._DEFERRAL_BASE is an anchored whitelist that turns such a partial into ROTATING (exempt from the CI
gate) instead of ATTENTION. Rows already stored in unit_state.last_error and runs.note carry the OLD tail
and keep it until the unit runs again, so the matcher must accept both tails - and nothing else: every
other part of the grammar (the [1-9] attempted floor, both anchors, the optional " [ids]" suffix) is
unchanged, and a note that differs in any other way stays ATTENTION.

The digest's first-match label (send_digest kind_of) and tools/cost/classify_attention.py key on the
shorter "deferred by budget", so both tails must still read BUDGET there.

The NEW note is always built by the production emitter (finalize), never typed. The OLD note is typed on
purpose: it is what stored rows hold, and no code writes it any more.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import updater.send_digest as sd  # noqa: E402
from updater import config, health, registry  # noqa: E402
from updater.health import _DEFERRAL_BASE, _deferral_only, gate_failures  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize  # noqa: E402

OLD_TAIL = "deferred by budget and taken next tick"
NEW_TAIL = "deferred by budget, not reached in this pass"

# Stored rows, as the old finalize() wrote them (with and without the _named id list).
OLD_ROWS = (
    f"15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL}",
    f"15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL} [DS_TOUR_CAP, DS_FLORES_A17]",
    f"1 sub-unit(s) attempted, none failed; 49 {OLD_TAIL} [x]",
)


def emitted(attempted=15, deferred=130, names=("DS_TOUR_CAP",)):
    """The budget note, from the production emitter."""
    t = Tally()
    for _ in range(attempted):
        t.added_unit(10)
    for i in range(deferred):
        t.deferred_unit(names[i % len(names)] if names else None)
    res = finalize(t, total_rows=1000, last_obs="2026-08-01", source="x", series_cursors={"k": "2026-08-01"})
    assert res.status == "partial", res.status
    return res.error


NEW = emitted()
NEW_UNNAMED = emitted(3, 2, names=())


def _unit(err, status="partial"):
    return {"unit_id": "_all", "status": status, "last_error": err}


# ---------------------------------------------------------------- the emitter
def test_finalize_writes_the_new_tail_and_never_the_old_one():
    for note in (NEW, NEW_UNNAMED, emitted(1, 1, names=("a", "b"))):
        assert NEW_TAIL in note and "next tick" not in note, note
    assert NEW.startswith(f"15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL} ["), NEW
    assert NEW_UNNAMED == f"3 sub-unit(s) attempted, none failed; 2 {NEW_TAIL}", NEW_UNNAMED


# ---------------------------------------------------------------- the matcher
@pytest.mark.parametrize("note", OLD_ROWS)
def test_an_old_stored_row_still_reads_rotating(note):
    assert _DEFERRAL_BASE.match(note), note
    assert _deferral_only([_unit(note)]) is True, note


@pytest.mark.parametrize("note", [NEW, NEW_UNNAMED, emitted(1, 1, names=("a",))])
def test_a_new_row_reads_rotating(note):
    assert _DEFERRAL_BASE.match(note), note
    assert _deferral_only([_unit(note)]) is True, note


@pytest.mark.parametrize("note", [OLD_ROWS[1], NEW])
def test_a_coverage_note_tail_is_still_stripped_for_both_tails(note):
    full = note + "; csv coverage note: derive budget spent — 40 of 90 id(s) deferred to csv_retry_queue, none failed"
    assert _deferral_only([_unit(full)]) is True, full


NEAR_MISSES = [
    # a different tail
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken by the next pass",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken on the next pass",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next run",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken by a later pass",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget, not reached in this passes",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget, not reached in this pass.",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget not reached in this pass",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget, not reached",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next tickX",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget",
    "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken",
    # both tails at once, or a tail with something after it
    f"15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL} next tick",
    f"15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL} {NEW_TAIL}",
    f"15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL} and more",
    f"15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL} [a] [b]",
    f"15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL}; rest next tick",
    f"15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL}; csv_derive failed 3/10 series [a, b, c]",
    f"15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL}; 2/9 sub-unit(s) transient-failed; will retry",
    # the head of the grammar, with either tail
    f"0 sub-unit(s) attempted, none failed; 130 {NEW_TAIL}",
    f"0 sub-unit(s) attempted, none failed; 130 {OLD_TAIL}",
    f"15 sub-unit(s) attempted, 1 failed; 130 {NEW_TAIL}",
    f"x 15 sub-unit(s) attempted, none failed; 130 {NEW_TAIL}",
    f"15 sub-unit(s) attempted, none failed; {NEW_TAIL}",
    # the count with no tail at all: an optional tail group matches this
    "15 sub-unit(s) attempted, none failed; 130 ",
    "15 sub-unit(s) attempted, none failed; 130",
    # a tail alone, or a whole note inside other text: an alternation that escapes its group matches these
    NEW_TAIL,
    OLD_TAIL,
    f"130 {OLD_TAIL}",
    f"boom; 15 sub-unit(s) attempted, none failed; 130 {OLD_TAIL}",
]


@pytest.mark.parametrize("note", NEAR_MISSES)
def test_a_note_that_differs_stays_attention(note):
    assert not _DEFERRAL_BASE.match(note), note
    assert _deferral_only([_unit(note)]) is False, note


@pytest.mark.parametrize("note", [OLD_ROWS[0], NEW])
def test_a_non_partial_status_is_never_rotating_whatever_the_tail(note):
    for status in ("transient_fail", "definitive_fail", "running"):
        assert _deferral_only([_unit(note, status)]) is False, status


# ---------------------------------------------------------------- the gate, end to end on a real state file
REG = {e["source_id"]: e for e in registry.load().get("sources", [])}
CASES = {"abs": OLD_ROWS[1], "unsdg": NEW, "insee_melodi": NEAR_MISSES[0]}
for _sid in CASES:
    assert REG[_sid].get("live") and (REG[_sid].get("run_location") or "cloud") == "cloud", \
        _sid + " left the cloud route: pick another id"


def _write_state(tmp_path, cases, hours_ago):
    now = dt.datetime.now(dt.timezone.utc)
    tried = (now - dt.timedelta(hours=hours_ago)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    store = StateStore(str(tmp_path / "state.db"))
    for sid, note in cases.items():
        e = REG[sid]
        store.upsert_unit(sid, "_all", strategy=e["strategy"], status="partial",
                          last_obs_date=(now - dt.timedelta(days=10)).strftime("%Y-%m-%d"),
                          last_attempt_utc=tried, last_error=note)
        store.upsert_source(sid, strategy=e["strategy"], cadence=e.get("cadence"), status="partial",
                            last_attempt_utc=tried)
    store.close()


def _gate(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state.db"))
    monkeypatch.setattr(sys, "argv", ["updater.health", "--json"])
    health.main()
    capsys.readouterr()
    return json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))


def test_the_gate_reads_old_and_new_rows_as_rotating_and_a_near_miss_as_attention(tmp_path, monkeypatch, capsys):
    _write_state(tmp_path, CASES, hours_ago=1)
    rep = _gate(tmp_path, monkeypatch, capsys)
    verdict = {r["source"]: r["health"] for r in rep["sources"]}
    assert verdict["abs"] == "ROTATING", ("old stored row", verdict["abs"])
    assert verdict["unsdg"] == "ROTATING", ("new row", verdict["unsdg"])
    assert verdict["insee_melodi"] == "ATTENTION", ("near miss", verdict["insee_melodi"])
    fails = " | ".join(gate_failures(rep))
    assert "abs" not in fails and "unsdg" not in fails, fails
    assert "insee_melodi" in fails, "the near miss must still fail the gate"


# ---------------------------------------------------------------- the digest label and the cost classifier
def test_the_digest_labels_both_tails_budget(tmp_path, monkeypatch, capsys):
    # attempted 9 days ago: outside every retry window, so the rows stand under "Needs attention", where
    # the label is printed (a calm row carries no label)
    _write_state(tmp_path, {"abs": OLD_ROWS[1], "unsdg": NEW}, hours_ago=24 * 9)
    _gate(tmp_path, monkeypatch, capsys)
    monkeypatch.setattr(sd, "STATE", str(tmp_path / "state.db"))
    sent = {}
    import core.status_mail as sm

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"

    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "success")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    sd.main()
    capsys.readouterr()
    body = sent["body"]
    for sid in ("abs", "unsdg"):
        line = next((ln for ln in body.splitlines() if ln.lstrip().startswith(f"!! {sid} ")), None)
        assert line is not None, (sid, body[:2000])
        assert line.split()[2] == "BUDGET", line


def test_the_cost_classifier_buckets_both_tails_budget():
    spec = importlib.util.spec_from_file_location(
        "classify_attention", os.path.join(ROOT, "tools", "cost", "classify_attention.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for note in (OLD_ROWS[1], NEW):
        label = next((name for name, pat in mod.BUCKETS if pat.search(note)), "OTHER")
        assert label.startswith("BUDGET"), (note, label)
