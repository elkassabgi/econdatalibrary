"""registry `desktop_lane` (review AR-467): a MITIGATION for the windowless UNCTAD giants on the desktop pass.

  skip - the SCHEDULED desktop pass (run_local_heavy.ps1 without -Only, which passes --scheduled) does not
         attempt the unit; it prints GIANT SKIP (ATTENTION) in the loop and the summary, health holds the
         source at ATTENTION or worse, and the digest lists it under its own label. An explicit request still
         attempts it.
  last - the unit is ordered after every other unit (orchestrate.order_units).

Every test calls the PRODUCTION function (run_once, order_units, registry.validate, health.assess,
send_digest.giant_lane_lines, the replay's UnitBook.run_pass), never a re-implementation (R684 (3)).
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from updater import orchestrate, registry, config  # noqa: E402
from updater.state import StateStore, now_utc  # noqa: E402

SKIP = {"unctad_tradefoodprocbycat", "unctad_tradefoodcatbyproc"}
LAST = {"unctad_biotrademerch"}


def _reg():
    return {e["source_id"]: e for e in registry.load().get("sources", [])}


# ------------------------------------------------------------------------------------ the flagged set

def test_the_flagged_set_is_the_measured_one():
    reg = _reg()
    assert {s for s, e in reg.items() if e.get("desktop_lane") == "skip"} == SKIP
    assert {s for s, e in reg.items() if e.get("desktop_lane") == "last"} == LAST
    assert "desktop_lane" not in reg["unctad_oceantrade"], "oceantrade finishes its turns: never flagged"
    for s in SKIP | LAST:
        assert reg[s]["desktop_lane_exit"].strip(), s
    assert registry.validate(registry.load(), expected_count=config.EXPECTED_SOURCE_COUNT) == []


# ------------------------------------------------------------------------------------ fail-closed validation

def _one(**kw):
    e = {"source_id": "x", "strategy": "bulk_snapshot_if_changed", "cadence": "weekly", "run_location": "local",
         "desktop_lane": "skip", "desktop_lane_exit": "Owner: the project owner. Remove when a turn finishes."}
    e.update(kw)
    return [p for p in registry.validate({"sources": [{k: v for k, v in e.items() if v is not None}]})]


def test_a_valid_entry_passes():
    assert _one() == []
    assert _one(desktop_lane="last") == []


def test_validation_is_fail_closed():
    for bad in ("giant", "Skip", "yes", True, 1, ""):
        assert any("desktop_lane must be one of" in p for p in _one(desktop_lane=bad)), bad
    assert any("desktop_lane_exit" in p for p in _one(desktop_lane_exit=None))
    assert any("desktop_lane_exit" in p for p in _one(desktop_lane_exit="   "))
    assert any("run_location: local" in p for p in _one(run_location=None))
    assert any("window_stop together" in p for p in _one(window_stop=True))
    assert any("without desktop_lane" in p for p in _one(desktop_lane=None))


def test_to_units_carries_only_a_valid_value():
    good = registry.to_units({"source_id": "x", "strategy": "bulk_snapshot_if_changed", "desktop_lane": "skip",
                              "desktop_lane_exit": "e"})[0]
    bad = registry.to_units({"source_id": "x", "strategy": "bulk_snapshot_if_changed", "desktop_lane": "True"})[0]
    assert good.config["desktop_lane"] == "skip" and good.config["desktop_lane_exit"] == "e"
    assert bad.config["desktop_lane"] is None


# ------------------------------------------------------------------------------------ run_once admission

def _run(tmp_path, monkeypatch, capsys, sources, scheduled):
    for v in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_RUN_LOCATION"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "0")          # no budget gate: only the flag can skip
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    monkeypatch.setattr(orchestrate, "_after_t0", lambda: False)
    store = StateStore(str(tmp_path / "state.db"))
    admitted = []
    # the first call after the flag: record and stop there, so nothing past admission runs
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: admitted.append(unit.source_id) or False)
    try:
        orchestrate.run_once(sources=sources, dry=True, store=store, scheduled=scheduled)
    except SystemExit:
        pass                                   # "no runnable adapter" for a live source the recorder admitted
    return admitted, capsys.readouterr().out


def test_a_skip_unit_is_never_admitted_in_a_scheduled_pass(tmp_path, monkeypatch, capsys):
    srcs = sorted(SKIP) + ["eia", "unctad_biotrademerch"]
    admitted, out = _run(tmp_path, monkeypatch, capsys, srcs, scheduled=True)
    assert not SKIP & set(admitted), admitted
    assert {"eia", "unctad_biotrademerch"} <= set(admitted), admitted   # 'last' is attempted, not skipped
    for s in SKIP:
        assert f"GIANT SKIP (ATTENTION) {s}/_all" in out, out[-3000:]
        assert re.search(rf"GIANT SKIP \(ATTENTION\) {s}/_all .*No current route can finish it.*Exit: Owner", out)
    assert re.search(r"GIANT SKIP \(ATTENTION\), not attempted in this scheduled pass: "
                     r"unctad_tradefoodcatbyproc, unctad_tradefoodprocbycat", out), out[-3000:]
    assert "WARNING GIANT SKIP" not in out


def test_no_sources_is_a_scheduled_pass(tmp_path, monkeypatch, capsys):
    admitted, out = _run(tmp_path, monkeypatch, capsys, None, scheduled=False)
    assert admitted, "the recorder saw no unit at all - the harness is broken"
    assert not SKIP & set(admitted)
    assert "GIANT SKIP (ATTENTION) unctad_tradefoodprocbycat/_all" in out


def test_an_explicit_request_admits_it(tmp_path, monkeypatch, capsys):
    admitted, out = _run(tmp_path, monkeypatch, capsys, sorted(SKIP), scheduled=False)
    assert sorted(admitted) == sorted(SKIP), admitted
    assert "GIANT SKIP" not in out


def test_after_t0_every_skip_warns(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(orchestrate, "_after_t0", lambda: True)
    for v in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_RUN_LOCATION"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "0")
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: False)
    try:
        orchestrate.run_once(sources=sorted(SKIP), dry=True, store=StateStore(str(tmp_path / "s.db")),
                             scheduled=True)
    except SystemExit:
        pass
    out = capsys.readouterr().out
    for s in SKIP:
        assert f"WARNING GIANT SKIP {s}/_all: this is AFTER T0" in out, out[-2000:]


def test_the_ps1_marks_only_the_scheduled_pass():
    body = open(os.path.join(ROOT, "tools", "run_local_heavy.ps1"), encoding="utf-8-sig").read()
    assert re.search(r"if \(-not \$Only\) \{ \$srcArgs \+= '--scheduled'", body)
    run_py = open(os.path.join(ROOT, "updater", "run.py"), encoding="utf-8").read()
    assert run_py.count("scheduled=a.scheduled") == 2


# ------------------------------------------------------------------------------------ ordering

def test_a_last_unit_sorts_after_every_band():
    units = {u.source_id: u for u in registry.all_units()}
    pick = [units[s] for s in ("unctad_biotrademerch", "eia", "oecd", "unctad_oceantrade")]
    costs = {"unctad_biotrademerch": 1.0, "eia": 5000.0, "oecd": 30000.0, "unctad_oceantrade": 3000.0}
    # biotrademerch is the cheapest AND the stalest, and still goes last
    stale = {"unctad_biotrademerch": 0, "eia": 1, "oecd": 2, "unctad_oceantrade": 3}
    order = orchestrate.order_units(pick, costs, lambda u: stale[u.source_id])
    assert [u.source_id for u in order][-1] == "unctad_biotrademerch"
    assert [u.source_id for u in order][0] == "unctad_oceantrade"     # the rest keep the cost-band rule


# ------------------------------------------------------------------------------------ health + digest

def test_health_never_counts_a_skip_unit_green(tmp_path):
    from updater import health
    store = StateStore(str(tmp_path / "state.db"))
    t = now_utc()
    for s in SKIP | LAST:
        store.upsert_unit(s, "_all", strategy="bulk_snapshot_if_changed", cadence="weekly", status="ok",
                          last_success_utc=t, last_attempt_utc=t)
        store.upsert_source(s, strategy="bulk_snapshot_if_changed", status="ok", last_success_utc=t)
    rows = {r["source"]: r for r in health.assess(store)["sources"]}
    for s in SKIP:
        assert rows[s]["health"] not in ("OK", "ROTATING"), rows[s]
        assert rows[s]["attention"][0].startswith("GIANT SKIP (registry desktop_lane: skip)"), rows[s]
    for s in LAST:
        assert not any("GIANT SKIP" in a for a in rows[s]["attention"]), rows[s]
    assert any(f"{s}" in g for s in SKIP for g in health.gate_failures(
        {"sources": [dict(rows[s], run_location="cloud") for s in SKIP]}))


def test_the_digest_labels_them():
    from updater import send_digest
    from updater.health import giant_skip_held
    now = dt.datetime(2026, 11, 10, tzinfo=dt.timezone.utc)
    rows = [("unctad_tradefoodprocbycat", "ok", None, "2026-11-01T00:00:00+00:00", None, None)]
    out = send_digest.giant_lane_lines(registry.load()["sources"], rows, now, giant_skip_held)
    text = "\n".join(out)
    assert re.search(r"GIANT SKIP \(ATTENTION\)\s+unctad_tradefoodprocbycat\s+last success\s+9\.0d ago", text)
    assert re.search(r"GIANT SKIP \(ATTENTION\)\s+unctad_tradefoodcatbyproc\s+never succeeded", text)
    assert re.search(r"ordered LAST\s+unctad_biotrademerch", text)
    assert send_digest.giant_lane_lines([], rows, now, giant_skip_held) == []


# ------------------------------------------------------------------------------------ the replay (C6, R249)

def test_the_replay_skips_through_the_production_function():
    import measure_ci_lag as m
    assert m.local_desktop_lane_from_registry() == {**{s: "skip" for s in SKIP}, **{s: "last" for s in LAST}}
    start = dt.datetime(2026, 11, 10, 0, 0, tzinfo=dt.timezone.utc)
    book = m.UnitBook({"p": "monthly", "q": "monthly"}, {}, [], {"p": 10.0, "q": 10.0}, {},
                      desktop_lane={"p": "skip"})
    res = book.run_pass(start, budget_min=600)
    assert res["giant_skipped"] == ["p"]
    assert book.attempts["p"] == 0 and book.attempts["q"] == 1
    plain = m.UnitBook({"p": "monthly", "q": "monthly"}, {}, [], {"p": 10.0, "q": 10.0}, {})
    plain.run_pass(start, budget_min=600)
    assert plain.attempts["p"] == 1, "control: without the flag the replay attempts p"


# ------------------------------------------------------------------------------------ coverage audit (N1)

def test_the_coverage_audit_counts_skip_units_as_on_paper_only():
    import audit_schedule_coverage as a
    sched, why = a.scheduled_sources()
    for s in SKIP:
        assert s not in sched and s not in why, why.get(s)
        assert "desktop_lane: skip" in a.scheduled_sources.stranded[s]
    assert "unctad_biotrademerch" in sched, "a 'last' unit is still attempted, so still scheduled"
    assert "unctad_oceantrade" in sched


# ------------------------------------------------------------------------------------ the digest's WIRING (B1)

def _digest(tmp_path, monkeypatch, capsys):
    """send_digest.main() end to end, with the REAL registry (the skip units in it) and a temp unit_state."""
    import sqlite3
    from updater import send_digest as sd
    import core.status_mail as sm
    db = tmp_path / "state.db"
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, "
                "last_success_utc TEXT, last_error TEXT, last_attempt_utc TEXT)")
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                ("unctad_tradefoodprocbycat", "ok", "2025-12-31", "2026-11-01T00:00:00+00:00", None,
                 "2026-11-01T00:00:00+00:00"))
    con.commit()
    con.close()
    monkeypatch.setattr(sd, "STATE", str(db))
    sent = {}

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"

    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "success")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    sd.main()
    capsys.readouterr()
    assert sent, "the report was not handed to the mail module"
    return sent


def test_both_digest_bodies_carry_the_giant_skip_lines(tmp_path, monkeypatch, capsys):
    import html as H
    sent = _digest(tmp_path, monkeypatch, capsys)
    body, page = sent["body"], H.unescape(sent["html"])
    for s in sorted(SKIP):
        assert re.search(rf"GIANT SKIP \(ATTENTION\)\s+{s}\s", body), "not in the TEXT body: " + s
    assert re.search(r"ordered LAST\s+unctad_biotrademerch", body)
    assert "<b>giant units</b>" in page, "the HTML notes carry no giant-units section"
    for s in sorted(SKIP):
        assert re.search(rf"GIANT SKIP \(ATTENTION\)\s+{s}\s", page), "not in the HTML body: " + s
    assert re.search(r"ordered LAST\s+unctad_biotrademerch", page)


# ------------------------------------------------------------------------------------ the leftover drain (B2)

def test_a_scheduled_pass_never_drains_a_skip_units_retries(tmp_path, monkeypatch, capsys):
    """A skipped unit did not run, and it is NOT a leftover-drain candidate either, whatever its retry queue
    holds (like PROTECTED). Control: a BUDGET SKIPPED unit in the same pass IS one."""
    for v in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_RUN_LOCATION"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "30")      # no cost on record -> 90-min reserve -> BUDGET SKIP
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    monkeypatch.setattr(orchestrate, "_after_t0", lambda: False)
    store = StateStore(str(tmp_path / "state.db"))
    for s in sorted(SKIP) + ["eia"]:
        store.enqueue_csv_retry(s, [f"{s}:X:1", f"{s}:X:2"], error="leftover")
    seen = {}
    real = orchestrate.drain_candidates

    def spy(drain_ok, drain_block):
        out = real(drain_ok, drain_block)
        seen["ok"], seen["cand"] = set(drain_ok), set(out)
        return out

    monkeypatch.setattr(orchestrate, "drain_candidates", spy)
    monkeypatch.setattr(orchestrate, "_drain_leftover_retries", lambda cand, blob, st: [])
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: True)
    try:
        orchestrate.run_once(sources=sorted(SKIP) + ["eia"], dry=False, store=store, scheduled=True)
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "ok" in seen, "the drain was never reached - the harness is broken:\n" + out[-2000:]
    assert "eia" in seen["ok"] and "eia" in seen["cand"], ("control: the BUDGET SKIPPED unit is a candidate", seen)
    assert not SKIP & seen["ok"], seen
    assert not SKIP & seen["cand"], seen
    for s in SKIP:
        assert f"GIANT SKIP (ATTENTION) {s}/_all" in out
