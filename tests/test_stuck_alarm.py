"""The stuck-run alarm (updater/health.py repeat_failures): the same failure run after run.

Every case builds an IN-MEMORY `runs` table with the real schema (updater/state.py DDL) and reads it back with
health.read_runs, the one query production uses. The texts are the real ones from the state copy of 2026-11-17
(usda's NASS 404, bea's NIPA/T20900, the unctad RCA 1/1 structural break, eurostat's capped passes).

tests/test_stuck_alarm_mutants.py re-runs THIS file with each named mutant of updater/health.py applied (the env
var below), and fails if the file still passes. A case here is only worth keeping if a mutant needs it.
"""
from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import updater.health as H  # noqa: E402
from updater import state as S  # noqa: E402

# ---- mutant hook: tests/test_stuck_alarm_mutants.py sets this and expects the file to FAIL
_MUTANT = os.environ.get("STUCK_ALARM_MUTANT")
if _MUTANT:
    _spec = importlib.util.spec_from_file_location(
        "_stuck_mutants", os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_stuck_alarm_mutants.py"))
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    _src = _mod.mutated_health_source(_MUTANT)
    exec(compile(_src, H.__file__, "exec"), H.__dict__)          # the SAME module object: every importer sees it

T0 = datetime(2026, 11, 17, 1, 0, 0, tzinfo=timezone.utc)
USDA_404 = ("usda: NASS datasets page unreachable: HTTPError('404 Client Error: Not Found for url: "
            "https://www-origin.nass.usda.gov/datasets')")
EIA_OK = ("+1204 new rows; csv coverage note: 3 changed keys have no catalog row for eia - derive budget spent, "
          "the rest is derived next pass")
BEA = ("1/420 sub-unit(s) transient-failed; will retry [NIPA/T20900.parquet: 0 rows for Year=ALL although the "
       "store holds data through 2024]")
RCA = ("unctad_x: 1/1 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body (schema/structural break); "
       "existing data kept [US.TradeFoodProcCat_Cat_RCA: unsupported layout]")
KILLED = ("hard-stopped by run_local_heavy wall-clock budget; recorded by tools/record_killed_unit.py so "
          "run_cost_estimate re-bands this source honestly")
EUROSTAT_CAP = ("selected>cap: fetched 400 flows; 148 changed or new and 439 retry flow(s) wait beyond the cap; the "
                "unit is due again at the next run (+8561316 rows) - retry note: 0 failed flow(s) wait out")
EUROSTAT_HARD = ("hard limit reached after 390 of 400 flows (flow 391 was not started) (UnitTimeout): the sweep "
                 "stopped there, 239 changed flow(s) go to the CSV step (+4744912 rows)")
EUROSTAT_STRUCT = ("eurostat: 2/173 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body "
                   "(schema/structural break); existing data kept [migr_asyumaccm: fetch_flow reported structural, "
                   "migr_asyappctzm: fetch_flow reported structural]")
GATED = "zzgatedsrc"        # a made-up denylisted id: it must never be named nor counted


def E(sid, **kw):
    e = {"source_id": sid, "live": True, "cadence": "daily", "run_location": "cloud"}
    e.update(kw)
    return e


ENTRIES = [E("usda"), E("eia"), E("bea"), E("idb"), E("unctad_x"), E("unctad_y"), E("ksrc"), E("es"),
           E("paused_src", paused=True), E("dead_src", live=False), E("disc_src"), E("nokey_src", live=None),
           E("giant_src", desktop_lane="skip", desktop_lane_exit="Remove when a turn can finish inside its route.")]
ENTRIES += [E(f"grp{i}") for i in range(5)]


def at(days_ago, hours=0.0):
    return (T0 - timedelta(days=days_ago, hours=hours)).isoformat(timespec="seconds")


def runs_of(rows):
    """[(ts, source, unit, status, note)] -> read_runs() of an in-memory db with the real DDL."""
    con = sqlite3.connect(":memory:")
    con.executescript(S.DDL)
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s, note) VALUES(?,?,?,?,0,1.0,?)",
                    rows)
    out = H.read_runs(con)
    con.close()
    return out


def rf(rows, entries=None, disc=frozenset({"disc_src"}), acks=None, gated=frozenset({GATED}), now=T0):
    return H.repeat_failures(runs_of(rows), ENTRIES if entries is None else entries, set(disc), acks, now,
                             gated=None if gated is None else set(gated))


def ids(res, key="stuck"):
    return [(l["source_id"], l["unit_id"]) for l in res[key]]


def usda_rows(n=6):
    rows = [(at(n * 2 + 1), "usda", "_all", "ok", "+12 new rows")]
    rows += [(at(2 * (n - 1 - i)), "usda", "_all", "transient_fail", USDA_404) for i in range(n)]
    return rows


# ------------------------------------------------------------------ planted positive and negative control
def test_usda_planted_positive_fires_as_unit_same():
    res = rf(usda_rows(6))
    assert ids(res) == [("usda", "_all")], res
    l = res["stuck"][0]
    assert (l["grain"], l["runs"], l["n_classes"]) == ("UNIT", 6, 1)
    assert l["since"] == at(10) and l["last_success"] == at(13)
    text = H.stuck_line_text(l, T0)
    assert "SAME" in text and "failing since 2026-11-07" in text and "6 runs" in text


def test_eia_negative_control_fires_nothing():
    res = rf([(at(i), "eia", "_all", "ok", EIA_OK) for i in range(10)])
    assert res["stuck"] == [] and res["acknowledged"] == [] and res["blind"] == {}
    lines = H.stuck_section_lines(res, T0)
    assert any("nothing stuck" in x for x in lines) and not any("eia" in x for x in lines)
    assert res["read"]["rows"] == 10 and res["read"]["newest_ts"] == at(0)


# ------------------------------------------------------------------ boundaries
def test_two_failures_do_not_fire_and_three_do():
    two = [(at(3), "usda", "_all", "transient_fail", USDA_404), (at(0), "usda", "_all", "transient_fail", USDA_404)]
    assert rf(two)["stuck"] == []
    three = two + [(at(1.5), "usda", "_all", "transient_fail", USDA_404)]
    assert ids(rf(three)) == [("usda", "_all")]


def test_three_failures_inside_two_hours_do_not_fire():
    rows = [(at(0, h), "usda", "_all", "transient_fail", USDA_404) for h in (2.0, 1.0, 0.0)]
    assert rf(rows)["stuck"] == []


def test_a_success_in_between_clears_the_alarm():
    rows = usda_rows(6) + [(at(0, -0.5), "usda", "_all", "ok", "+3 new rows")]
    assert rf(rows)["stuck"] == []
    rows = [(at(4), "usda", "_all", "transient_fail", USDA_404), (at(3), "usda", "_all", "transient_fail", USDA_404),
            (at(2), "usda", "_all", "ok", "+1 new rows"), (at(1), "usda", "_all", "transient_fail", USDA_404),
            (at(0), "usda", "_all", "transient_fail", USDA_404)]
    assert rf(rows)["stuck"] == []


def test_window_stopped_neither_breaks_nor_counts():
    rows = [(at(3), "usda", "_all", "transient_fail", USDA_404),
            (at(2), "usda", "_all", "window_stopped", "window stop, no durable progress"),
            (at(1), "usda", "_all", "transient_fail", USDA_404),
            (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows)
    assert ids(res) == [("usda", "_all")] and res["stuck"][0]["runs"] == 3


def test_unknown_status_breaks_and_is_counted():
    rows = [(at(3), "usda", "_all", "transient_fail", USDA_404), (at(2), "usda", "_all", "weird", "x"),
            (at(1), "usda", "_all", "transient_fail", USDA_404), (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows)
    assert res["stuck"] == [] and res["read"]["unknown_status"] == {"weird": 1}
    assert any("unknown status" in x for x in H.stuck_section_lines(res, T0))


def test_new_counts_only_a_streak_that_crossed_in_the_newest_run():
    rows = [(at(3), "usda", "_all", "transient_fail", USDA_404), (at(2), "usda", "_all", "transient_fail", USDA_404),
            (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows)
    assert res["new"] == 1 and H.stuck_subject_prefix(res) == "1 stuck (1 new) · "
    assert rf(usda_rows(6))["new"] == 0


def test_new_counts_a_streak_that_crossed_with_two_runs_since_the_last_digest():
    """A unit records 2-3 runs between digests: the crossing run need not be its last one."""
    rows = [(at(3), "usda", "_all", "transient_fail", USDA_404), (at(2), "usda", "_all", "transient_fail", USDA_404),
            (at(0, 12), "usda", "_all", "transient_fail", USDA_404), (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows)
    assert ids(res) == [("usda", "_all")] and res["new"] == 1 and res["stuck"][0]["new"] is True


# ------------------------------------------------------------------ eurostat: a capped pass that merges rows
def test_eurostat_capped_progress_is_neutral_not_stuck():
    """Replayed over 60 days the unit-only rule turned eurostat on and off 4 times: a capped or hard-limited pass
    records transient_fail while merging millions of rows."""
    seq = [EUROSTAT_CAP, EUROSTAT_HARD, EUROSTAT_CAP, EUROSTAT_STRUCT, EUROSTAT_CAP, EUROSTAT_CAP, EUROSTAT_HARD,
           EUROSTAT_CAP, EUROSTAT_STRUCT, EUROSTAT_HARD]
    rows = [(at(len(seq) - i), "es", "_all", "partial" if n is EUROSTAT_STRUCT else "transient_fail", n)
            for i, n in enumerate(seq)]
    assert rf(rows)["stuck"] == []
    # control: the same failure without merged rows is stuck
    nogain = [(at(3 - i), "es", "_all", "transient_fail", EUROSTAT_CAP.replace("(+8561316 rows)", "(+0 rows)"))
              for i in range(4)]
    assert ids(rf(nogain)) == [("es", "_all")]


# ------------------------------------------------------------------ sub-unit grain
def test_bea_subunit_fires_and_unit_does_not():
    rows = [(at(34 - i * 3.4), "bea", "_all", "partial", BEA) for i in range(11)]
    res = rf(rows)
    assert ids(res) == [("bea", "_all")]
    l = res["stuck"][0]
    assert l["grain"] == "SUBUNIT" and l["tokens"] == ["NIPA/T20900.parquet"] and l["runs"] == 11
    assert H._stuck_unit([(r[0], 0, r[3], r[4]) for r in rows]) is None     # 1/420 is not all-failed


def _tok_note(tok, extra=""):
    return f"2/40 sub-unit(s) transient-failed; will retry [{tok}: merge guard (< 97% of existing){extra}]"


def test_truncated_run_is_unknown_not_a_break():
    rows = [(at(12), "idb", "_all", "partial", _tok_note("T/1")), (at(10), "idb", "_all", "partial", _tok_note("T/1")),
            (at(8), "idb", "_all", "partial", _tok_note("T/1")),
            (at(6), "idb", "_all", "partial", _tok_note("T/9", ", T/8: x, +12 more")),
            (at(4), "idb", "_all", "partial", _tok_note("T/1")), (at(0), "idb", "_all", "partial", _tok_note("T/1"))]
    res = rf(rows)
    assert ids(res) == [("idb", "_all")], res
    assert res["stuck"][0]["unknown"] == 1 and res["stuck"][0]["runs"] == 5
    assert res["blind"] == {"idb": 1}
    assert any("sub-unit view partial for: idb (1 runs truncated)" in x for x in H.stuck_section_lines(res, T0))


def test_a_slice_that_did_not_attempt_the_token_is_unknown():
    """stat_slovenia: a rotating partial that says nothing of the token does not clear it."""
    slice_note = "+2957 new rows; csv coherence unmet: 1503 changed series_keys have no catalog mapping"
    rows = []
    for i, d in enumerate((20, 17, 14, 11, 8, 5, 0)):
        rows.append((at(d), "idb", "_all", "partial", _tok_note("1012308S") if i % 2 == 0 or d == 0 else slice_note))
    rows += [(at(19), "idb", "_all", "partial", _tok_note("1012308S")), (at(10), "idb", "_all", "partial",
                                                                          _tok_note("1012308S"))]
    res = rf(rows)
    assert ids(res) == [("idb", "_all")] and res["stuck"][0]["unknown"] >= 1


def test_ok_or_a_complete_deferral_without_the_token_breaks_the_subunit_streak():
    base = [(at(12 - 2 * i), "idb", "_all", "partial", _tok_note("T/1")) for i in range(6)]
    assert ids(rf(base)) == [("idb", "_all")]
    ok = base[:3] + [(at(5.5), "idb", "_all", "ok", "+5 new rows")] + base[3:]
    assert rf(ok)["stuck"] == []
    clean = ("3 sub-unit(s) attempted, none failed; 2 deferred by budget, not reached in this pass "
             "[T/7: budget spent, T/8: budget spent]")
    deferral = base[:3] + [(at(5.5), "idb", "_all", "partial", clean)] + base[3:]
    assert rf(deferral)["stuck"] == []
    named = clean.replace("T/8", "T/1")                       # the token was deferred: NOT attempted -> unknown
    assert ids(rf(base[:3] + [(at(5.5), "idb", "_all", "partial", named)] + base[3:])) == [("idb", "_all")]


def test_a_rotating_newest_slice_that_skips_the_token_does_not_clear_it():
    """ecb: the newest slice did not attempt the failing dataflow; the line must not turn off with the slice."""
    slice_note = ("53 sub-unit(s) attempted, none failed; 487 deferred by budget and taken next tick "
                  "[ECB__A.parquet: budget 35 min spent, deferred, +486 more]")
    rows = [(at(14 - 2 * i), "idb", "_all", "partial", _tok_note("T/1")) for i in range(6)]
    rows.append((at(0), "idb", "_all", "partial", slice_note))
    res = rf(rows)
    assert ids(res) == [("idb", "_all")] and res["stuck"][0]["unknown"] == 1


def test_dict_shaped_items_give_dataflow_tokens():
    note = ("1/35 sub-unit(s) transient-failed; will retry [{'agency': 'ECB', 'id': 'CSEC', 'version': '1.0', "
            "'dsd': 'NA_SEC', 'dsd_agency': 'ESTAT'}: HTTP 500, {'agency': 'ESTAT', 'id': 'QSA', 'dsd': 'NA'}: x]")
    toks, blind, _d = H._stuck_view(note)
    assert toks == {"ECB/CSEC", "ESTAT/QSA"} and not blind


def test_unparseable_shrink_is_blind_not_a_token():
    note = "BLOCKER (never-shrink guard): refusing shrink 3362599->195391 at D:\\x\\y.parquet (< 97% of existing)"
    toks, blind, _d = H._stuck_view(note)
    assert toks == set() and blind is True
    toks, blind, _d = H._stuck_view("merge refused [a.parquet: refusing shrink 10->1]")
    assert toks == {"shrink:a.parquet"} and blind is False


# ------------------------------------------------------------------ killed_external and all-failed partials
def test_killed_runs_fire_with_the_killed_label():
    rows = [(at(30), "ksrc", "_all", "killed_external", KILLED), (at(20), "ksrc", "_all", "killed_external", KILLED),
            (at(10), "ksrc", "_all", "killed_external", KILLED), (at(0), "ksrc", "_all", "partial", RCA)]
    res = rf(rows)
    assert ids(res) == [("ksrc", "_all")] and res["stuck"][0]["grain"] == "KILLED"
    assert res["stuck"][0]["runs"] == 4 and res["stuck"][0]["n_classes"] == 2


def test_all_failed_partial_fires_as_unit():
    rows = [(at(14 - 7 * i), "unctad_x", "_all", "partial", RCA) for i in range(3)]
    res = rf(rows)
    assert ids(res) == [("unctad_x", "_all")] and res["stuck"][0]["grain"] == "UNIT"


def test_the_all_failed_test_reads_the_full_note_not_200_chars():
    note = "x" * 240 + "; 1/1 sub-unit(s) transient-failed; will retry [only: boom]"
    rows = [(at(4 - 2 * i), "unctad_x", "_all", "partial", note) for i in range(3)]
    assert ids(rf(rows)) == [("unctad_x", "_all")]


def test_desktop_lane_skip_is_held_not_counted():
    rows = [(at(36 + 10 * i), "giant_src", "_all", "killed_external", KILLED) for i in range(3)]
    res = rf(rows)
    assert res["stuck"] == [] and ids(res, "held") == [("giant_src", "_all")]
    lines = H.stuck_section_lines(res, T0)
    assert lines[0].startswith("Stuck: the same failure run after run - 0 stuck")
    assert any("held by the registry" in x for x in lines)
    assert any("exit: Remove when a turn can finish" in x and "giant_src" in x for x in lines)


# ------------------------------------------------------------------ grouping
def test_one_outage_across_many_units_is_one_line():
    note = ("detect:UNEXPECTED:HTTPError('404 Client Error: Not Found for url: "
            "https://unctadstat-api.unctad.org/api/reportMetadata/US.{}/en')")
    rows = [(at(d), f"grp{i}", "_all", "transient_fail", note.format(f"T{i}")) for i in range(5) for d in (2, 1, 0)]
    res = rf(rows)
    assert len(res["stuck"]) == 1 and len(res["stuck"][0]["group"]) == 5
    assert H.stuck_subject_prefix(res).startswith("1 stuck (")
    assert set(H.stuck_marks(res)) == {f"grp{i}" for i in range(5)}
    assert "5 units, one error class" in H.stuck_line_text(res["stuck"][0], T0)


def test_grouping_needs_the_same_class_and_the_same_first_day():
    note = ("detect:UNEXPECTED:HTTPError('404 Client Error: Not Found for url: "
            "https://unctadstat-api.unctad.org/api/reportMetadata/US.{}/en')")
    rows = [(at(d), f"grp{i}", "_all", "transient_fail", note.format(f"T{i}")) for i in range(3) for d in (2, 1, 0)]
    # the same day, an unrelated failure: NOT part of the outage
    rows += [(at(d), "grp3", "_all", "transient_fail", USDA_404.replace("usda", "grp3")) for d in (2, 1, 0)]
    # the same class, but a streak that began days earlier: NOT part of today's outage either
    rows += [(at(d), "grp4", "_all", "transient_fail", note.format("T4")) for d in (6, 4, 2, 1, 0)]
    res = rf(rows)
    groups = [l for l in res["stuck"] if l.get("group")]
    assert len(groups) == 1 and groups[0]["group"] == ["grp0/_all", "grp1/_all", "grp2/_all"], res["stuck"]
    assert sorted(ids(res)) == [("grp0", "_all"), ("grp3", "_all"), ("grp4", "_all")]


def test_same_day_404s_from_different_hosts_are_not_one_outage():
    a = "detect:UNEXPECTED:HTTPError('404 Client Error: Not Found for url: https://api.alpha.example/x/{}')"
    b = "detect:UNEXPECTED:HTTPError('404 Client Error: Not Found for url: https://api.beta.example/x/{}')"
    rows = [(at(d), "grp0", "_all", "transient_fail", a.format(1)) for d in (2, 1, 0)]
    rows += [(at(d), "grp1", "_all", "transient_fail", a.format(2)) for d in (2, 1, 0)]
    rows += [(at(d), "grp2", "_all", "transient_fail", b.format(3)) for d in (2, 1, 0)]
    res = rf(rows)
    assert len(res["stuck"]) == 2
    assert [l.get("group") for l in res["stuck"] if l.get("group")] == [["grp0/_all", "grp1/_all"]]


# ------------------------------------------------------------------ exclusions
def test_exclusions_paused_not_live_discontinued_unregistered_gated():
    rows = []
    for sid in ("paused_src", "dead_src", "disc_src", "nokey_src", "unregistered_src", GATED):
        rows += [(at(d), sid, "_all", "transient_fail", USDA_404) for d in (4, 2, 0)]
    res = rf(rows)
    assert res["stuck"] == [] and res["held"] == []
    assert res["excluded"] == {"paused": 1, "not_live": 2, "discontinued": 1}
    flat = "\n".join(H.stuck_section_lines(res, T0))
    assert "paused_src" in flat and "dead_src" in flat and "disc_src" in flat
    assert GATED not in flat and "unregistered_src" not in flat
    assert "withheld" not in flat and "registry entry" not in flat     # silent: a count is a mention


def test_gated_id_is_scrubbed_from_free_text_and_fails_closed():
    note = f"1/1 sub-unit(s) transient-failed; will retry [{GATED}_table: boom]"
    rows = [(at(d), "unctad_x", "_all", "partial", note) for d in (4, 2, 0)]
    res = rf(rows)
    flat = "\n".join(H.stuck_section_lines(res, T0))
    assert ids(res) == [("unctad_x", "_all")] and GATED not in flat and H.STUCK_WITHHELD in flat
    blind = rf(rows, gated=None)
    flat = "\n".join(H.stuck_section_lines(blind, T0))
    assert GATED not in flat and H.STUCK_WITHHELD_NO_GATE in flat


def test_held_exit_text_is_scrubbed_by_whole_token():
    rows = [(at(36 + 10 * i), "giant_src", "_all", "killed_external", KILLED) for i in range(3)]
    named = [dict(e, desktop_lane_exit=f"Wait for {GATED}/tables to finish.") if e["source_id"] == "giant_src" else e
             for e in ENTRIES]
    res = rf(rows, entries=named)
    assert res["held"][0]["exit"] == H.STUCK_WITHHELD
    inside = [dict(e, desktop_lane_exit=f"the x{GATED}y release is spilled") if e["source_id"] == "giant_src"
              else e for e in ENTRIES]
    res = rf(rows, entries=inside)
    assert res["held"][0]["exit"] == f"the x{GATED}y release is spilled"      # a substring of a word is not a mention
    assert rf(rows, gated=None)["held"][0]["exit"] == H.STUCK_WITHHELD_NO_GATE


def test_blind_list_uses_the_same_eligibility():
    trunc = "2/40 sub-unit(s) transient-failed; will retry [a: x, +12 more]"
    rows = [(at(1), "dead_src", "_all", "partial", trunc), (at(1), GATED, "_all", "partial", trunc),
            (at(1), "idb", "_all", "partial", trunc)]
    assert rf(rows)["blind"] == {"idb": 1}


# ------------------------------------------------------------------ acknowledgements
def ack(**kw):
    a = {"source_id": "usda", "unit_id": "_all", "note_contains": "NASS datasets page unreachable",
         "reason_quote": "The datasets page moved to a new address.", "reason_url": "https://example.org/notice",
         "acked_utc": "2026-11-10", "expires_utc": "2026-12-01", "by": "test"}
    a.update(kw)
    return {"acks": [a]}


def test_an_unexpired_ack_moves_the_line_it_does_not_hide_it():
    res = rf(usda_rows(6), acks=ack())
    assert res["stuck"] == [] and ids(res, "acknowledged") == [("usda", "_all")]
    lines = H.stuck_section_lines(res, T0)
    acked = [x for x in lines if "usda/_all" in x]
    assert len(acked) == 1 and "acknowledged (expires 2026-12-01)" in acked[0]
    assert "failing since 2026-11-07" in acked[0]           # the ack does not restart the clock
    assert lines[0].endswith("0 stuck (0 new), +1 acknowledged")


def test_an_expired_ack_puts_the_line_back_marked():
    res = rf(usda_rows(6), acks=ack(acked_utc="2026-10-01", expires_utc="2026-11-15"))
    assert ids(res) == [("usda", "_all")] and res["acknowledged"] == []
    assert "ACK EXPIRED 2026-11-15" in H.stuck_line_text(res["stuck"][0], T0)
    assert res["ack_expired"] == [{"source_id": "usda", "unit_id": "_all", "expired": "2026-11-15"}]


@pytest.mark.parametrize("bad", [
    {"expires_utc": "2027-01-30"},                      # 81 days after acked_utc
    {"acked_utc": "2026-11-20", "expires_utc": "2026-12-01"},   # acked in the future
    {"reason_quote": ""}, {"reason_url": "example.org"}, {"reason_quote": "<verbatim text from the publisher>"},
    {"unit_id": "*"}, {"note_contains": "NASS 404"}, {"colour": "red"},
])
def test_an_invalid_file_is_ignored_whole_and_every_line_shows(bad):
    doc = ack(**bad)
    doc["acks"].append(ack(source_id="bea")["acks"][0])
    res = rf(usda_rows(6), acks=doc)
    assert res["acks_error"] and ids(res) == [("usda", "_all")] and res["acknowledged"] == []
    assert any("stuck_acks.yaml IGNORED" in x for x in H.stuck_section_lines(res, T0))


def test_expiry_more_than_60_days_from_today_is_rejected():
    _valid, err = H._validate_stuck_acks({"acks": [dict(ack()["acks"][0], acked_utc="2026-11-17",
                                                        expires_utc="2027-01-16")]}, T0.date())
    assert err is None
    _valid, err = H._validate_stuck_acks({"acks": [dict(ack()["acks"][0], acked_utc="2026-11-17",
                                                        expires_utc="2027-01-17")]}, T0.date())
    assert err and "60 days" in err


def test_a_token_mismatch_still_fires_and_the_ack_is_stale():
    rows = [(at(34 - i * 3.4), "bea", "_all", "partial", BEA) for i in range(11)]
    res = rf(rows, acks=ack(source_id="bea", token="NIPA/T99999.parquet", note_contains="0 rows for Year=ALL although"))
    assert ids(res) == [("bea", "_all")] and res["acknowledged"] == []
    assert [s["token"] for s in res["stale_acks"]] == ["NIPA/T99999.parquet"]
    assert any(x.startswith("  stale ack - remove it: bea/_all") for x in H.stuck_section_lines(res, T0))
    hit = rf(rows, acks=ack(source_id="bea", token="NIPA/T20900.parquet", note_contains="0 rows for Year=ALL although"))
    assert hit["stuck"] == [] and ids(hit, "acknowledged") == [("bea", "_all")] and hit["stale_acks"] == []


USDA_401 = ("usda: NASS datasets page unreachable: HTTPError('401 Client Error: Unauthorized for url: "
            "https://www-origin.nass.usda.gov/datasets')")


def test_a_changed_error_after_the_ack_returns_the_line_and_counts_in_k():
    rows = usda_rows(6) + [(at(0, -6), "usda", "_all", "transient_fail", USDA_401),
                           (at(0, -12), "usda", "_all", "transient_fail", USDA_401)]
    res = rf(rows, acks=ack(note_contains="404 Client Error: Not Found for url"), now=T0 + timedelta(hours=13))
    assert ids(res) == [("usda", "_all")] and res["acknowledged"] == []
    assert H.stuck_subject_prefix(res).startswith("1 stuck (")


def test_an_ack_needs_every_run_since_it_was_written_to_match():
    rows = [(at(9), "usda", "_all", "transient_fail", USDA_404), (at(8), "usda", "_all", "transient_fail", USDA_404),
            (at(5), "usda", "_all", "transient_fail", USDA_401), (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows, acks=ack(acked_utc="2026-11-10", note_contains="404 Client Error: Not Found for url"))
    assert ids(res) == [("usda", "_all")] and res["acknowledged"] == []          # the 401 of 11-12 is since the ack
    ok = rf(rows, acks=ack(acked_utc="2026-11-13", note_contains="404 Client Error: Not Found for url"))
    assert ok["stuck"] == [] and ids(ok, "acknowledged") == [("usda", "_all")]   # every run since 11-13 is the 404


def test_an_ack_must_match_the_newest_error():
    rows = [(at(6), "usda", "_all", "transient_fail", "usda: connection reset by peer while reading the index"),
            (at(4), "usda", "_all", "transient_fail", USDA_404), (at(2), "usda", "_all", "transient_fail", USDA_404),
            (at(0), "usda", "_all", "transient_fail", USDA_404)]
    res = rf(rows, acks=ack(note_contains="connection reset by peer while reading"))
    assert ids(res) == [("usda", "_all")] and len(res["stale_acks"]) == 1


def test_the_committed_acks_file_is_valid_and_empty():
    doc = H.load_stuck_acks_doc()
    assert not isinstance(doc, BaseException), doc
    valid, err = H._validate_stuck_acks(doc, T0.date())
    assert err is None and valid == []


# ------------------------------------------------------------------ unreadable input
def test_db_without_a_runs_table_raises_for_the_caller_to_say_so(tmp_path):
    p = tmp_path / "state.db"
    con = sqlite3.connect(str(p))
    con.execute("CREATE TABLE unit_state(source_id TEXT)")
    con.commit()
    con.close()
    with pytest.raises(sqlite3.OperationalError):
        H.read_runs_readonly(str(p))
    with pytest.raises(FileNotFoundError):
        H.read_runs_readonly(str(tmp_path / "absent.db"))
    lines = H.stuck_unreadable_lines(sqlite3.OperationalError("no such table: runs"))
    assert lines[1] == ("  stuck alarm: could not be read (OperationalError) - repeat failures are unknown "
                        "this morning")


def test_the_section_is_never_empty_and_names_its_gaps():
    lines = H.stuck_section_lines(rf([]), T0)
    assert lines[1] == "  stuck alarm: read 0 runs rows, newest None"
    assert any("nothing stuck" in x for x in lines) and H.STUCK_GAPS in lines


# ------------------------------------------------------------------ the CLI and health.json
def _state_db(path, runs, units=()):
    con = sqlite3.connect(str(path))
    con.executescript(S.DDL)
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, obs, dur_s, note) VALUES(?,?,?,?,0,1.0,?)",
                    runs)
    con.executemany("INSERT INTO unit_state(source_id, unit_id, status, last_error, last_attempt_utc, "
                    "last_success_utc, last_obs_date) VALUES(?,?,?,?,?,?,?)", units)
    con.commit()
    con.close()


def _rel_rows(sid, status, note, days):
    now = datetime.now(timezone.utc)
    return [((now - timedelta(days=d)).isoformat(timespec="seconds"), sid, "_all", status, note) for d in days]


def test_cli_stuck_reads_only_and_writes_nothing(tmp_path, monkeypatch, capsys):
    from updater import config, registry
    _state_db(tmp_path / "state.db", _rel_rows("usda", "transient_fail", USDA_404, (10, 8, 6, 4, 2, 0)))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state.db"))
    monkeypatch.setattr(registry, "load", lambda: {"sources": ENTRIES})
    monkeypatch.setattr(sys, "argv", ["updater.health", "--stuck"])
    before = sorted(os.listdir(tmp_path))
    H.main()
    out = capsys.readouterr().out
    assert "  stuck alarm: read 6 runs rows" in out and "usda/_all · UNIT · 6 runs" in out
    assert sorted(os.listdir(tmp_path)) == before            # no health.json, no -wal/-shm


def test_cli_stuck_on_a_missing_db_says_so(tmp_path, monkeypatch):
    from updater import config
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "absent.db"))
    lines = H.stuck_cli()
    assert "could not be read (FileNotFoundError)" in lines[1]


def test_assess_shows_the_line_and_keeps_the_verdict(tmp_path, monkeypatch):
    from updater import config, registry
    _state_db(tmp_path / "state.db", _rel_rows("usda", "transient_fail", USDA_404, (10, 8, 6, 4, 2, 0)))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state.db"))
    monkeypatch.setattr(registry, "load", lambda: {"sources": [E("usda"), E("eia")]})
    store = S.StateStore(str(tmp_path / "state.db"))
    rep = H.assess(store)
    row = {r["source"]: r for r in rep["sources"]}
    assert [x["grain"] for x in row["usda"]["repeat_failure"]] == ["UNIT"] and row["eia"]["repeat_failure"] == []
    assert rep["repeat_failures"]["stuck"] == 1 and rep["repeat_failures"]["error"] is None
    monkeypatch.setattr(H, "stuck_report", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x")))
    rep2 = H.assess(store)
    store.close()
    assert {r["source"]: r["health"] for r in rep2["sources"]} == {r["source"]: r["health"] for r in rep["sources"]}
    assert rep2["repeat_failures"]["error"].startswith("RuntimeError")
    assert {r["source"]: r["repeat_failure"] for r in rep2["sources"]}["usda"] is None


# ------------------------------------------------------------------ both mail bodies
def _digest(tmp_path, monkeypatch, entries=None, state=True, gated=frozenset({GATED})):
    from updater import registry
    from updater import send_digest as sd
    import core.status_mail as sm
    if state:
        runs = _rel_rows("usda", "transient_fail", USDA_404, (10, 8, 6, 4, 2, 0))
        runs += _rel_rows(GATED, "transient_fail", USDA_404, (4, 2, 0))
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _state_db(tmp_path / "state.db", runs, [("usda", "_all", "transient_fail", USDA_404, now, None, None),
                                                ("eia", "_all", "ok", None, now, now, "2026-11-01")])
    monkeypatch.setattr(sd, "STATE", str(tmp_path / "state.db"))
    if entries is Exception:
        def boom():
            raise RuntimeError("registry unreadable")
        monkeypatch.setattr(registry, "load", boom)
    else:
        monkeypatch.setattr(registry, "load", lambda: {"sources": entries or ENTRIES})
    monkeypatch.setattr(sd, "read_guard_beat", lambda *a, **k: {"unreadable": "test"})
    monkeypatch.setattr(sd, "_gated_ids", lambda: None if gated is None else set(gated))
    sent = {}

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"

    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "success")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    with contextlib.redirect_stdout(io.StringIO()):
        sd.main()
    assert sent, "the mail was not built"
    return sent


def test_digest_renders_the_section_first_in_both_bodies(tmp_path, monkeypatch):
    m = _digest(tmp_path, monkeypatch)
    assert m["subject"].startswith("1 stuck (0 new) · econdatalibrary daily:"), m["subject"]
    body = m["body"].splitlines()
    assert body[0] == "Run 123: success" and body[1].startswith("Stuck: the same failure run after run - 1 stuck")
    assert body[2].startswith("  stuck alarm: read 9 runs rows, newest ")
    assert any("usda/_all · UNIT · 6 runs" in x for x in body)
    bang = [x for x in body if x.lstrip().startswith("!! usda")]
    assert bang and "STUCKx6" in bang[0]
    html = m["html"]
    assert "Stuck: the same failure run after run - 1 stuck" in html and "usda/_all · UNIT · 6 runs" in html
    assert html.index("Stuck: the same failure run after run") < html.index("Needs attention")
    assert html.index("</tr></table>") < html.index("Stuck: the same failure run after run")
    assert "STUCKx6" in html
    for part in (m["subject"], m["body"], html):
        assert GATED not in part


def test_digest_without_a_runs_table_still_goes(tmp_path, monkeypatch):
    con = sqlite3.connect(str(tmp_path / "state.db"))
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, last_success_utc TEXT, "
                "last_error TEXT, last_attempt_utc TEXT)")
    con.commit()
    con.close()
    m = _digest(tmp_path, monkeypatch, state=False)
    want = "stuck alarm: could not be read (OperationalError) - repeat failures are unknown this morning"
    assert want in m["body"] and want in m["html"] and "stuck ·" not in m["subject"]


def test_digest_with_no_state_db_still_goes_and_says_so(tmp_path, monkeypatch):
    m = _digest(tmp_path, monkeypatch, state=False)
    assert "could not be read (FileNotFoundError)" in m["body"] and "could not be read (FileNotFoundError)" in m["html"]
    assert "ATTENTION" in m["subject"]                      # an unreadable record is never "OK"


def test_digest_with_no_registry_names_nothing(tmp_path, monkeypatch):
    m = _digest(tmp_path, monkeypatch, entries=Exception)
    assert "stuck alarm: could not be read (RuntimeError)" in m["body"]
    assert GATED not in m["body"] and GATED not in m["html"]


def test_digest_with_an_unreadable_denylist_withholds_free_text(tmp_path, monkeypatch):
    m = _digest(tmp_path, monkeypatch, gated=None)
    line = next(x for x in m["body"].splitlines() if "usda/_all · UNIT" in x)
    assert H.STUCK_WITHHELD_NO_GATE in line and "NASS" not in line
