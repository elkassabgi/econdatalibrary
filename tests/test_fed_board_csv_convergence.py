"""fed_board Fix B, PR-2 (DESIGN_v2 §2, §3, §4, §7; reviews AR-458 r2 and AR-461, both PROCEED-WITH-CHANGES).

Offline: a tmp catalogue, a tmp StateStore, fake derives (they record the ids they were handed and report
put / failed / deferred like updater.derive.derive_and_put), the cutover flag pointed into tmp_path, and every
GITHUB_* / AQUEDUCT_* variable removed (R293). Each test names the mutant it kills (the PR body has the table).

  T3   two-call rotation: owed ids go FIRST, so the second call never defers the first call's tail.
       M4 owed-first removed; M5 owed ids appended last.
  T7   budget-deferred ids -> csv_desktop_owed, none queued; a failed booking keeps ALL of them for the queue,
       none owed (all or none, also with a row failing INSIDE the batch). M11 the old _book_csv_desktop_owed call;
       M12 per-id booking.
  T8   fence trip after k PUTs (the REAL run_once handler): booked = mapped - put_ids, and B3 clears the put
       ids' old owed rows. M13 the put_ids subtraction removed.
  T9   crash after derive_and_put returned: booked AND (held) queued ids exclude put_ids. M14 no subtraction.
  T10  B3: owed a,b,c and put {a,b} -> a,b cleared, c kept; the drain never clears an owed row; a LocalBlob
       derive clears nothing. M15 clear every mapped id; M16 B3 in the drain; M16b any store counts as served.
  T11  payable filter before / after T0; the repay runs at the early exits too; an unreadable owed table skips
       it loudly; an owed id that fails again does not demote. M17 all owed before T0; M23 early exits skip the
       repay; M24 an owed-read error raises into the crash path; M25 repaid failures demote.
  T12  THE HOLD: a legacy row (or a NULL stamp) holds; an unreadable queue, a cache miss and a bad
       csv_misses_from hold; only declaring sources are held; run_once resets the decision and says HELD; the
       drain routes as today while held. M18 hold removed; M19 only rows after the date; M19b MIN-style NULL
       blindness; M19c fail open on a queue error; M19d fail open on a cache miss; M19e cache miss holds every
       source; M19f a date object accepted; M19g no reset at run_once start.
  T13  the queue cap still rotates with routing ON: queue empty after 2 drains, H15 cleared. M20 a Fix-A
       id_gate in front of the drain.
  T14  derive_csv_bulk: fed_board passes the csv_misses refusal only with BOTH flags; ilostat is still refused;
       the clear never removes an unmapped-keys row unpaid, and pays it on streamed evidence; the heal's
       scratch-root output is unchanged. M21 exemption keyed on the source name; M22 exemption without the
       flags; M26 keys check removed; M27 blanket never-clear; M28 keys check before the writer guards; M31 the
       old --clear-owed-only rule (health's step (5) refused).
  H    health: a fed_board ' keys=' row never gets the cap-hit remedy; owed rows name the EQUAL-proof clear.
       M30 keys branch removed.
  R    registry: the three new keys validate; a bare YAML date is refused. M32 the validator accepts a date.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import blob as blob_mod, config, derive, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Result  # noqa: E402

SRC = "fed_board"
Z = [f"{SRC}:Z1:K{i:02d}" for i in range(1, 11)]                  # 10 catalogued Z1 ids
H = [f"{SRC}:H15:H{i}" for i in range(1, 3)]                       # 2 catalogued H15 ids
UNIT = types.SimpleNamespace(key=f"{SRC}/_all", source_id=SRC, unit_id="_all")


# --------------------------------------------------------------------------------------------- fixtures
@pytest.fixture
def env(tmp_path, monkeypatch):
    for k in list(os.environ):
        if k.startswith("GITHUB_") or k.startswith("AQUEDUCT_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover_flag"))
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, SRC) for i in Z + H])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    st = StateStore(path=str(tmp_path / "state.db"))
    ns = types.SimpleNamespace(tmp=tmp_path, st=st, mp=monkeypatch, calls=[], blob=blob_mod.R2Blob())
    return ns


def _release(st):
    """The run's hold decision, as run_once makes it, from `st` (empty queue = released)."""
    orchestrate._CSV_MISSES_HOLD = orchestrate._csv_misses_holds(st)
    return orchestrate._CSV_MISSES_HOLD


def _fake(env, cap=10 ** 6, fail=(), reasons=None, crash_after=False):
    """derive_and_put: PUT the first `cap` ids in the order handed (fail those in `fail`), defer the rest -
    derive.py's own in-order budget deferral (derive.py:566-573)."""
    def f(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        env.calls.append(list(ids))
        head, rest = ids[:cap], ids[cap:]
        put = [s for s in head if s not in set(fail)]
        bad = [s for s in head if s in set(fail)]
        return {"put": len(put), "put_ids": put, "failed": bad + rest, "deferred": len(rest), "deferred_ids": rest,
                "failed_reasons": {s: (reasons or {}).get(s, "ResolveError: zero rows") for s in bad},
                "skipped_identical": 0, "deferred_large": {}}
    env.mp.setattr(derive, "derive_and_put", f)
    return f


def _res(keys):
    return Result(status="ok", obs=100, series_cursors={k.split(":", 1)[1]: "2026-09-30" for k in keys})


def _owed(st):
    return sorted(r["series_id"] for r in st.csv_desktop_owed(SRC))


def _queued(st):
    return sorted(r["series_id"] for r in st.csv_retries(SRC))


# --------------------------------------------------------------------------------------------- T3
def test_t3_two_call_rotation_never_defers_the_same_tail(env):
    _release(env.st)
    _fake(env, cap=6)
    f1, n1, d1, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert env.calls[0] == Z and f1 == [] and d1 == [], (f1, d1)
    assert _owed(env.st) == Z[6:], "call 1 books its budget-deferred tail K07..K10"
    f2, n2, d2, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    handed = env.calls[1]
    assert handed[:4] == Z[6:], f"owed ids must be derived FIRST: {handed[:4]}"
    put2 = set(handed[:6])
    assert set(Z[6:]) <= put2, "call 2 PUTs every id call 1 deferred"
    assert not (set(handed[6:]) & set(Z[6:])), "the ids deferred by call 2 are disjoint from call 1's"
    assert _owed(env.st) == sorted(handed[6:]), "B3 cleared K07..K10; call 2's own tail is owed now"


# --------------------------------------------------------------------------------------------- T7
def test_t7_budget_deferred_ids_are_owed_not_queued_and_health_attends(env):
    _release(env.st)
    _fake(env, cap=4)
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert failed == [] and deferred == [] and note.startswith("csv coverage note:"), (failed, deferred, note)
    assert _owed(env.st) == Z[4:] and _queued(env.st) == []
    from updater import health
    row = next(r for r in health.assess(env.st)["sources"] if r["source"] == SRC)
    assert row["health"] != "OK" and any("CSV(s) OWED" in a for a in row["attention"]), row


def test_t7_a_failed_booking_keeps_every_deferred_id_for_the_queue(env):
    _release(env.st)
    _fake(env, cap=4)

    def boom(*a, **k):
        raise sqlite3.OperationalError("disk I/O error")
    env.mp.setattr(env.st, "note_csv_desktop_owed", boom)
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert sorted(deferred) == Z[4:] and failed == [], (failed, deferred, note)
    assert note.startswith("csv coverage note:"), "a deferral is coverage, not a crash"
    assert _owed(env.st) == []


def test_t7_a_row_failing_inside_the_batch_books_none(env):
    _release(env.st)
    _fake(env, cap=4)
    env.st.db.execute(f"CREATE TRIGGER t BEFORE INSERT ON csv_desktop_owed WHEN NEW.series_id='{Z[7]}' "
                      "BEGIN SELECT RAISE(ABORT, 'planted'); END")
    env.st.db.commit()
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert _owed(env.st) == [] and sorted(deferred) == Z[4:], (deferred, _owed(env.st))


# --------------------------------------------------------------------------------------------- run_once rig
class _Strategy:
    def __init__(self, result):
        self.result = result

    def is_due(self, unit, us, **k):
        return True

    def detect_change(self, unit, us):
        self._cur = "v2"
        return "v2"

    def run(self, unit, since=None):
        return self.result


@pytest.fixture
def rig(env, monkeypatch):
    data = env.tmp / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)

    def run(result):
        monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Strategy(result))
        return orchestrate.run_once(sources=[SRC], store=env.st, blob=env.blob)
    env.run = run
    return env


def _trip_after(env, put):
    def f(ids, blob, **kw):
        env.calls.append(list(ids))
        t = orchestrate.UnitTimeout("fed_board/_all (csv phase) exceeded")
        t.derive_partial = {"put_ids": list(put), "failed": [], "failed_reasons": {}, "deferred_large": {}}
        raise t
    env.mp.setattr(derive, "derive_and_put", f)


def test_t8_a_fence_trip_books_mapped_minus_put_and_b3_pays_the_put(rig, capsys):
    rig.st.note_csv_desktop_owed(SRC, [(Z[0], None, "old debt"), (Z[1], None, "old debt")])
    _trip_after(rig, put=Z[:3])
    rig.run(_res(Z))
    assert "csv_misses routing ON" in capsys.readouterr().out
    assert _owed(rig.st) == Z[3:], "booked = mapped - put_ids, and the put ids' old rows are cleared (B3)"
    assert all("fence" in r["reason"] for r in rig.st.csv_desktop_owed(SRC))
    assert _queued(rig.st) == []


# --------------------------------------------------------------------------------------------- T9
def _crash_after_derive(env, put):
    def f(ids, blob, **kw):
        env.calls.append(list(ids))
        return {"put": len(put), "put_ids": list(put), "failed": [s for s in ids if s not in set(put)],
                "deferred": 0, "deferred_ids": [], "failed_reasons": {}, "skipped_identical": 0,
                "deferred_large": {}}
    env.mp.setattr(derive, "derive_and_put", f)

    def boom(ids):
        raise RuntimeError("catalog sync file unwritable")
    env.mp.setattr(orchestrate, "_record_for_catalog_sync", boom)


def test_t9_a_crash_after_the_derive_never_books_a_put_id(env):
    _release(env.st)
    _crash_after_derive(env, put=Z[:4])
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert "crashed" in note and failed == []
    assert _owed(env.st) == Z[4:], "booked ids exclude out['put_ids']"


def test_t9_while_held_a_crash_never_requeues_a_put_id_with_a_fresh_stamp(env):
    orchestrate._CSV_MISSES_HOLD = {SRC: "csv coverage note: csv_misses HELD for fed_board - planted"}
    _crash_after_derive(env, put=Z[:4])
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z), env.blob, env.st)
    assert sorted(failed) == Z[4:], "held: the queued ids exclude put_ids (AR-461 F2d)"
    assert _owed(env.st) == []


# --------------------------------------------------------------------------------------------- T10
def test_t10_b3_clears_exactly_the_put_ids(env, capsys):
    _release(env.st)
    a, b, c = Z[0], Z[1], Z[2]
    env.st.note_csv_desktop_owed(SRC, [(a, None, "x"), (b, None, "x"), (c, None, "x")])
    env.st.enqueue_csv_retry(SRC, [a], "legacy")
    env.st.db.execute("UPDATE csv_retry_queue SET enqueued_utc='2026-11-20T00:00:00+00:00'")
    env.st.db.commit()
    _release(env.st)
    _fake(env, fail=[c])          # b: in put_ids exactly as a skip-identical PUT is (derive.py:489-499)
    orchestrate._derive_changed_csvs(UNIT, _res([a, b, c]), env.blob, env.st)
    assert _owed(env.st) == [c], "a, b paid; c failed again and stays owed"
    assert _queued(env.st) == [], "B3 clears the queue row of a put id too"
    assert "B3 cleared 2 csv_desktop_owed and 1 csv_retry_queue row(s)" in capsys.readouterr().out, \
        "exactly the put ids: c is never cleared (and then re-booked)"


def test_t10_negative_control_the_drain_never_clears_an_owed_row(env):
    a = Z[0]
    env.st.note_csv_desktop_owed(SRC, [(a, None, "owed")])
    env.st.enqueue_csv_retry(SRC, [a], "queued")
    env.st.db.execute("UPDATE csv_retry_queue SET enqueued_utc='2026-11-20T00:00:00+00:00'")
    env.st.db.commit()
    _release(env.st)
    _fake(env)
    orchestrate._drain_csv_retry_queue(UNIT, env.blob, env.st)
    assert _queued(env.st) == [] and _owed(env.st) == [a], "the drain clears its queue row, never the debt"


def test_t10_a_derive_to_a_local_store_clears_no_debt(env, capsys):
    _release(env.st)
    env.st.note_csv_desktop_owed(SRC, [(Z[0], None, "owed")])
    _fake(env)
    orchestrate._derive_changed_csvs(UNIT, _res([Z[0]]), blob_mod.LocalBlob(root=str(env.tmp / "lb")), env.st)
    assert _owed(env.st) == [Z[0]], "a LocalBlob PUT proves nothing served (AR-461 F6)"
    assert "B3 NOT clearing" in capsys.readouterr().out


# --------------------------------------------------------------------------------------------- T11
def test_t11_payable_before_t0_only_for_a_republished_release(env):
    _release(env.st)
    env.st.note_csv_desktop_owed(SRC, [(H[0], None, "owed")])
    _fake(env)
    orchestrate._derive_changed_csvs(UNIT, _res(Z[:2]), env.blob, env.st)
    assert H[0] not in env.calls[0] and _owed(env.st) == [H[0]], env.calls


def test_t11_after_t0_every_owed_id_is_payable(env):
    from core import cutover
    flag = env.tmp / "CUTOVER"
    flag.write_text("x")
    env.mp.setattr(cutover, "FLAG_PATH", str(flag))
    _release(env.st)
    env.st.note_csv_desktop_owed(SRC, [(H[0], None, "owed")])
    _fake(env)
    sh = blob_mod.SelfhostBlob.__new__(blob_mod.SelfhostBlob)     # the served store after T0 (never touched)
    orchestrate._derive_changed_csvs(UNIT, _res(Z[:2]), sh, env.st)
    assert env.calls[0][0] == H[0] and _owed(env.st) == [], env.calls


def test_t11_the_repay_runs_at_an_early_exit_too(env):
    """AR-458 r2 (e): a zero-mapped pass (a republished release whose changed keys map to nothing) still repays
    that release's owed ids. Backend r2, as on the runner: there the derive-all fallback never widens the set."""
    env.mp.setattr(orchestrate.config, "BACKEND", "r2")
    _release(env.st)
    env.st.note_csv_desktop_owed(SRC, [(H[0], None, "owed")])
    _fake(env)
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res([f"{SRC}:H15:NOT_CATALOGUED"]),
                                                                 env.blob, env.st)
    assert env.calls == [[H[0]]] and _owed(env.st) == [], (env.calls, note)
    assert "owed-first repay" in note


def test_t11_an_unreadable_owed_table_skips_the_repay_loudly(env, capsys):
    _release(env.st)
    _fake(env)

    def boom(*a, **k):
        raise sqlite3.DatabaseError("malformed")
    env.mp.setattr(env.st, "csv_desktop_owed", boom)
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z[:3]), env.blob, env.st)
    assert env.calls == [Z[:3]] and failed == [] and note is None, (note, env.calls)
    assert "owed-first repay SKIPPED" in capsys.readouterr().out


def test_t11_an_owed_id_that_fails_again_re_books_and_never_demotes(env):
    """AR-461 F5: an owed id whose key the Fed dropped from the release fails every repay; it must not make
    fed_board partial on every pass (the R359 class)."""
    _release(env.st)
    gone = f"{SRC}:Z1:K10"
    env.st.note_csv_desktop_owed(SRC, [(gone, None, "budget-deferred id")])
    _fake(env, fail=[gone], reasons={gone: "ResolveError: zero rows for Z1:K10"})
    failed, note, deferred, _ = orchestrate._derive_changed_csvs(UNIT, _res(Z[:3]), env.blob, env.st)
    assert failed == [] and note.startswith("csv coverage note:"), (failed, note)
    row = env.st.csv_desktop_owed(SRC)[0]
    assert row["series_id"] == gone and "zero rows" in row["reason"], row


# --------------------------------------------------------------------------------------------- T12
def _legacy(st, ids, stamp="2026-09-23T22:57:12+00:00"):
    st.enqueue_csv_retry(SRC, ids, "no fed_board flow file for 'Z1'")
    st.db.execute("UPDATE csv_retry_queue SET enqueued_utc=? WHERE series_id IN (%s)" % ",".join("?" * len(ids)),
                  (stamp, *ids))
    st.db.commit()


def test_t12_a_legacy_row_holds_and_the_drain_routes_as_today(env, capsys):
    _legacy(env.st, [Z[0]])
    hold = _release(env.st)
    assert "HELD" in hold[SRC] and orchestrate._csv_misses(SRC) == ""
    _fake(env, fail=[Z[0]])
    orchestrate._drain_csv_retry_queue(UNIT, env.blob, env.st)
    q = env.st.csv_retries(SRC)
    assert [r["series_id"] for r in q] == [Z[0]] and q[0]["attempts"] == 2, q
    assert q[0]["enqueued_utc"] == "2026-09-23T22:57:12+00:00", "the drain keeps the stamp"
    assert _owed(env.st) == [] and "csv_misses HELD" in capsys.readouterr().out


def test_t12_the_hold_releases_when_no_row_predates_the_cut_off(env):
    _legacy(env.st, [Z[0]], stamp="2026-11-10T00:00:00+00:00")     # ON the cut-off instant: not legacy
    assert _release(env.st)[SRC] == "" and orchestrate._csv_misses(SRC) == "desktop_owed"
    _legacy(env.st, [Z[1]], stamp="2026-11-09T23:59:59+00:00")     # one second before it: legacy
    assert "HELD" in _release(env.st)[SRC]


def test_t12_a_null_stamp_counts_as_legacy(env):
    env.st.enqueue_csv_retry(SRC, [Z[0], Z[1]], "x")
    env.st.db.execute("UPDATE csv_retry_queue SET enqueued_utc=NULL WHERE series_id=?", (Z[0],))
    env.st.db.execute("UPDATE csv_retry_queue SET enqueued_utc='2026-11-20T00:00:00+00:00' WHERE series_id=?",
                      (Z[1],))
    env.st.db.commit()
    assert "HELD" in _release(env.st)[SRC], "MIN() ignores NULL; the count must not (AR-461 F2b)"


def test_t12_an_unreadable_queue_holds(env):
    def boom(*a, **k):
        raise sqlite3.OperationalError("database is locked")
    env.mp.setattr(env.st, "csv_retry_count_before", boom)
    assert "could not be read" in _release(env.st)[SRC] and orchestrate._csv_misses(SRC) == ""


def test_t12_a_cache_miss_holds_the_declaring_source_only(env):
    orchestrate._CSV_MISSES_HOLD = None                  # a call outside run_once
    assert orchestrate._csv_misses(SRC) == ""
    assert orchestrate._csv_misses("ilostat") == "desktop_owed", "ilostat declares no csv_misses_from (F2c)"


@pytest.mark.parametrize("bad", [dt.date(2026, 11, 10), "2026-11-1", "2026/11/10", "2026-13-01",
                                 "2026-11-10T00:00:00Z", 20261110, None])
def test_t12_a_bad_csv_misses_from_holds(env, bad):
    orchestrate._csv_misses(SRC)                         # loads _REG_ENTRIES
    ent = dict(orchestrate._REG_ENTRIES[SRC])
    ent["csv_misses_from"] = bad
    env.mp.setitem(orchestrate._REG_ENTRIES, SRC, ent)
    assert "not a quoted ISO date" in _release(env.st)[SRC]
    assert orchestrate._csv_misses(SRC) == ""


def test_t12_run_once_decides_afresh_and_says_held(rig, capsys):
    _legacy(rig.st, [Z[0]])
    _fake(rig)
    rig.run(_res(Z[:2]))
    out = capsys.readouterr().out
    assert "csv_misses HELD for fed_board" in out
    us = rig.st.get_unit(SRC, "_all")
    assert "csv_misses HELD" in (us.get("last_error") or ""), us
    assert us["status"] == "ok", "the hold note is a coverage note, it never demotes"
    assert orchestrate._CSV_MISSES_HOLD is None, "the decision ends with the run"
    rig.st.clear_csv_retries([Z[0]])
    orchestrate._CSV_MISSES_HOLD = {SRC: "csv coverage note: csv_misses HELD - stale from a previous run"}
    rig.run(_res(Z[:2]))
    assert "csv_misses routing ON" in capsys.readouterr().out, "run_once must not inherit a stale decision"


def test_t12_measured_value_holds_today():
    """The registry's csv_misses_from against the stamp measured on a copy of the workstation's state.db (2026-11-08T09:25Z):
    all 29,975 fed_board rows were enqueued 2026-09-23T22:57:12..13+00:00."""
    e = {x["source_id"]: x for x in registry.load()["sources"]}[SRC]
    frm = registry.csv_misses_from_utc(e["csv_misses_from"])
    assert isinstance(e["csv_misses_from"], str) and frm is not None
    assert "2026-09-23T22:57:13+00:00" < frm and frm > "2026-11-08T23:59:59+00:00", frm


# --------------------------------------------------------------------------------------------- T13
def test_t13_the_queue_cap_still_rotates_with_routing_on(env, monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(orchestrate, "_CSV_RETRY_CAP", 3)
    env.st.enqueue_csv_retry(SRC, Z[:5], "no fed_board flow file for 'Z1'")
    env.st.enqueue_csv_retry(SRC, [H[0]], "no fed_board flow file for 'H15'")
    env.st.db.execute("UPDATE csv_retry_queue SET attempts=5, enqueued_utc='2026-11-20T00:00:00+00:00'")
    env.st.db.commit()
    _release(env.st)

    def only_h15_present(ids, blob, **kw):
        env.calls.append(list(ids))
        ok = [s for s in ids if ":H15:" in s]
        bad = [s for s in ids if s not in ok]
        return {"put": len(ok), "put_ids": ok, "failed": bad, "deferred": 0, "deferred_ids": [],
                "failed_reasons": {s: "ResolveError: no fed_board flow file for 'Z1'" for s in bad},
                "skipped_identical": 0, "deferred_large": {}}
    monkeypatch.setattr(derive, "derive_and_put", only_h15_present)
    orchestrate._drain_csv_retry_queue(UNIT, env.blob, env.st)
    orchestrate._drain_csv_retry_queue(UNIT, env.blob, env.st)
    assert _queued(env.st) == [], "the queue is empty after 2 drains"
    assert H[0] not in _owed(env.st) and _owed(env.st) == Z[:5]


# --------------------------------------------------------------------------------------------- T14
def _bulk(monkeypatch, tmp_path, *args, reg=None):
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import derive_csv_bulk as B
    if reg is not None:
        p = tmp_path / "registry.yaml"
        p.write_text(reg, encoding="utf-8")
        monkeypatch.setattr(config, "REGISTRY", str(p))
    monkeypatch.setattr(B, "ROOT", str(tmp_path / "empty_root"))
    monkeypatch.setattr(sys, "argv", ["derive_csv_bulk.py", *args])
    cleared = []
    monkeypatch.setattr(B, "_durable_clear", lambda src, **kw: cleared.append((src, kw)) or True)
    return B.main(), cleared


def test_t14_fed_board_passes_the_refusal_only_with_both_flags(env, capsys):
    rc, _ = _bulk(env.mp, env.tmp, "--source", SRC, "--qualify-with-shard", "--only-catalogued")
    out = capsys.readouterr().out
    assert rc == 2 and "REFUSING" not in out and "no parquet for fed_board" in out, out
    for args in ((), ("--qualify-with-shard",), ("--only-catalogued",)):
        rc, _ = _bulk(env.mp, env.tmp, "--source", SRC, *args)
        out = capsys.readouterr().out
        assert rc == 2 and "BOTH --qualify-with-shard and --only-catalogued" in out, (args, out)
    rc, _ = _bulk(env.mp, env.tmp, "--source", "ilostat", "--qualify-with-shard", "--only-catalogued")
    assert rc == 2 and "declares csv_misses: desktop_owed" in capsys.readouterr().out


def test_t14_the_exemption_is_the_registry_key_not_the_name(env, capsys):
    reg = ("sources:\n- source_id: xsrc\n  csv_misses: desktop_owed\n  csv_bulk: qualify_with_shard\n"
           "- source_id: fed_board\n  csv_misses: desktop_owed\n")
    rc, _ = _bulk(env.mp, env.tmp, "--source", "xsrc", "--qualify-with-shard", "--only-catalogued", reg=reg)
    assert "no parquet for xsrc" in capsys.readouterr().out
    rc, _ = _bulk(env.mp, env.tmp, "--source", SRC, "--qualify-with-shard", "--only-catalogued", reg=reg)
    assert rc == 2 and "declares csv_misses: desktop_owed" in capsys.readouterr().out


def test_t14_healths_step_5_clear_passes_and_refuses_an_unmapped_row(env, capsys):
    """AR-461 F3: health's fed_board remedy ends with `--clear-owed-only`; the tool must take it, and pass the
    unmapped-keys refusal to the clear."""
    rc, cleared = _bulk(env.mp, env.tmp, "--source", SRC, "--clear-owed-only")
    assert rc == 0 and cleared == [(SRC, {"refuse_unmapped": True})], cleared
    rc, cleared = _bulk(env.mp, env.tmp, "--source", SRC, "--clear-owed-only", "--after-desktop-derive")
    assert rc == 0 and cleared == [(SRC, {"refuse_unmapped": False})], cleared
    rc, cleared = _bulk(env.mp, env.tmp, "--source", "ilostat", "--clear-owed-only")
    assert rc == 2 and cleared == [], "ilostat keeps its rule"
    rc, cleared = _bulk(env.mp, env.tmp, "--source", "abs", "--clear-owed-only")
    assert cleared == [("abs", {})], "another source's call is exactly what it was"


@pytest.fixture
def dclear(env, monkeypatch):
    """tools/derive_csv_bulk._durable_clear on a tmp state store, the writer guards open, pull/push recorded."""
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import derive_csv_bulk as B
    data = env.tmp / "root"
    monkeypatch.setattr(B, "ROOT", str(data))
    monkeypatch.setattr(config, "STATE_DIR", str(env.tmp / "st"))
    monkeypatch.setattr(config, "STATE_DB", str(env.tmp / "st" / "state.db"))
    runs = []

    class _P:
        def __init__(self, args):
            runs.append(args[-1])
            self.returncode, self.stdout = 0, ""
    monkeypatch.setattr(B, "_writers_quiet", lambda manual, window_min=30: True)
    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda args, **kw: _P(args))
    st = StateStore()
    st.note_full_rederive_owed(SRC, note='0 changed keys mapped to catalogue ids; 2 changed STORE key(s) not '
                                         'mapped to catalogue ids (re-derive them on the desktop) '
                                         'keys=["Z1:K01", "Z1:K02"]')
    st.db.close()

    def owed():
        s = StateStore()
        try:
            return [r["source_id"] for r in s.full_rederives_owed()]
        finally:
            s.db.close()
    return types.SimpleNamespace(B=B, runs=runs, owed=owed)


def test_t14_the_clear_never_removes_an_unmapped_row_unpaid(dclear, capsys):
    assert dclear.B._durable_clear(SRC, refuse_unmapped=True, streamed_keys={"Z1:K01"}) is False
    assert dclear.owed() == [SRC] and dclear.runs == ["--pull-state"], dclear.runs
    assert "NOT clearing the fed_board row" in capsys.readouterr().out


def test_t14_a_campaign_that_streamed_every_listed_key_pays_the_row(dclear):
    assert dclear.B._durable_clear(SRC, refuse_unmapped=True, streamed_keys={"Z1:K01", "Z1:K02", "H15:H1"})
    assert dclear.owed() == [] and dclear.runs == ["--pull-state", "--push-state"]


def test_t14_the_stated_desktop_derive_clears_the_row(dclear):
    assert dclear.B._durable_clear(SRC, refuse_unmapped=False) and dclear.owed() == []


def test_t14_the_heals_scratch_root_output_is_unchanged(env, monkeypatch, capsys):
    """AR-461 F7: run_heal.ps1 runs the campaign with ECONDL_ROOT = a scratch folder. There the clear must refuse
    at the writer guard exactly as before: one 'NOT clearing: ...' line of the two counted forms, no line naming
    the state lifecycle, and no StateStore built (it would create scratch/data/_aqueduct/state.db)."""
    import re
    import subprocess
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import derive_csv_bulk as B
    monkeypatch.setattr(B, "ROOT", str(env.tmp / "scratch"))

    class _Gate:
        returncode, stdout = 0, "0\n"
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: _Gate())
    import updater.state as S

    def no_store(*a, **k):
        raise AssertionError("a StateStore before the writer guards returned")
    monkeypatch.setattr(S, "StateStore", no_store)
    assert B._durable_clear(SRC, refuse_unmapped=True, streamed_keys=set()) is False
    lines = capsys.readouterr().out.splitlines()
    nc = [ln for ln in lines if ln.startswith("NOT clearing: the CI writer gate does not show")
          or ln.startswith("NOT clearing: a desktop heavy pass is due within")]
    assert len(nc) == 1, lines
    assert not [ln for ln in lines if re.search("pull-state|push-state|full_rederive_owed cleared|nothing to clear",
                                                ln)], lines


# --------------------------------------------------------------------------------------------- health
def test_h_an_unmapped_keys_row_never_gets_the_cap_hit_remedy(env):
    from updater import health
    env.st.note_full_rederive_owed(SRC, note='0 changed keys mapped to catalogue ids; 1 changed STORE key(s) '
                                             'not mapped keys=["Z1:K01"]')
    row = next(r for r in health.assess(env.st)["sources"] if r["source"] == SRC)
    text = row["attention"][0]
    assert "--after-desktop-derive" in text and "every listed key was streamed" in text and "Z1:K01" in text, text
    assert "derive_ilostat_indicators" not in text and "(5) then, from the checkout" not in text, text


def test_h_a_cap_hit_row_keeps_its_remedy_with_the_hold_on(env):
    """AR-458 r2 (b)3: health reads the raw registry key; run with the hold ON."""
    from updater import health
    _legacy(env.st, [Z[0]])
    assert "HELD" in _release(env.st)[SRC]
    env.st.note_full_rederive_owed(SRC, note="cursor cap hit — changed-set evidence truncated (120,000 keys)")
    row = next(r for r in health.assess(env.st)["sources"] if r["source"] == SRC)
    assert "--qualify-with-shard" in row["attention"][0] and "derive_ilostat" not in row["attention"][0]


def test_h_owed_rows_name_the_equal_proof_clear(env):
    from updater import health
    env.st.note_csv_desktop_owed(SRC, [(Z[0], None, "budget-deferred id")])
    row = next(r for r in health.assess(env.st)["sources"] if r["source"] == SRC)
    text = " ".join(row["attention"])
    assert "clear_fed_board_z1_retries.py --table csv_desktop_owed" in text, text


# --------------------------------------------------------------------------------------------- registry
def test_r_the_registry_validates_with_the_new_keys():
    reg = registry.load()
    assert registry.validate(reg, expected_count=config.EXPECTED_SOURCE_COUNT) == []
    e = {x["source_id"]: x for x in reg["sources"]}[SRC]
    assert (e["csv_misses"], e["csv_owed_repay"]) == ("desktop_owed", "fresh")


@pytest.mark.parametrize("val, ok", [("2026-11-10", True), ("2026-11-10T06:00:00+00:00", True),
                                     (dt.date(2026, 11, 10), False), ("2026-11-10T06:00:00Z", False),
                                     ("10/11/2026", False), (20261110, False)])
def test_r_csv_misses_from_must_be_a_quoted_iso_value(val, ok):
    reg = registry.load()
    for e in reg["sources"]:
        if e["source_id"] == SRC:
            e["csv_misses_from"] = val
    probs = [p for p in registry.validate(reg) if "csv_misses_from" in p]
    assert (probs == []) is ok, probs


def test_r_csv_owed_repay_needs_its_two_partners():
    reg = registry.load()
    for e in reg["sources"]:
        if e["source_id"] == SRC:
            e.pop("csv_bulk")
    assert any("csv_owed_repay needs" in p for p in registry.validate(reg))


# --------------------------------------------------------------------------------------------- the EQUAL-proof clear
#  C   tools/clear_fed_board_z1_retries.py, extended (AR-458 r2 (b)4, AR-461 F7): the default is today's (Z1, the
#      queue, the Z1 ETag guard); --copy <REL>.parquet moves the prefix AND the guard to <REL>; --table
#      csv_desktop_owed clears owed rows and touches no queue row. M28c --table ignored; M29 the guard stays on Z1.
class _S3:
    def __init__(self):
        self.obj, self.calls = {}, []

    def head_object(self, Bucket, Key):
        self.calls.append(Key)
        if Key not in self.obj:
            class _E(Exception):
                response = {"ResponseMetadata": {"HTTPStatusCode": 404}, "Error": {"Code": "404"}}
            raise _E()
        return dict(self.obj[Key])


@pytest.fixture
def ctool(tmp_path, monkeypatch):
    import hashlib
    import pyarrow as pa
    import pyarrow.parquet as pq
    from tools import clear_fed_board_z1_retries as T
    from tools.derive_csv_bulk import csv_key
    from core.r2_util import gzip_bytes
    for k in ("GITHUB_ACTIONS", "GITHUB_RUN_ID", "AQUEDUCT_BACKEND", "AQUEDUCT_RUN_LOCATION", "ECONDL_DATA"):
        monkeypatch.delenv(k, raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover"))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state" / "state.db"))
    monkeypatch.setattr(T, "guards_ok", lambda window_min=None: True)
    s3 = _S3()
    copies = {}
    for rel in ("Z1", "H15"):
        p = tmp_path / "root" / "fed_board" / f"{rel}.parquet"
        os.makedirs(p.parent, exist_ok=True)
        pq.write_table(pa.table({
            "dataset": [rel] * 4, "series_key": ["A.Q", "A.Q", "B.Q", "B.Q"],
            "obs_date": pa.array([dt.date(2025, 12, 31), dt.date(2026, 6, 30)] * 2, type=pa.date32()),
            "value": [1.0, 2.0, 3.0, 4.0], "obs_status": pa.array([None] * 4, type=pa.string())}), str(p))
        raw = p.read_bytes()
        s3.obj[T.key_for(rel)] = {"ETag": '"%s"' % hashlib.md5(raw).hexdigest(), "ContentLength": len(raw),
                                  "LastModified": dt.datetime(2026, 9, 23, tzinfo=dt.timezone.utc)}
        copies[rel] = str(p)
        ids = [T.prefix_for(rel) + k for k in ("A.Q", "B.Q")]
        bodies = T.derive_local(str(p), ids)
        for s in ids:                                    # every served object EQUAL to the copy's CSV
            s3.obj[csv_key("series", "fed_board", s.split(":", 1)[1])] = {
                "ETag": '"%s"' % hashlib.md5(gzip_bytes(bodies[s])).hexdigest(), "Metadata": {},
                "ContentEncoding": "gzip"}
    st = StateStore()
    st.enqueue_csv_retry("fed_board", ["fed_board:Z1:A.Q", "fed_board:H15:A.Q"], "queued")
    st.note_csv_desktop_owed("fed_board", [("fed_board:Z1:A.Q", None, "owed"), ("fed_board:H15:B.Q", None, "owed")])
    st.db.close()
    runs = []

    def go(rel, ids, *extra, out="ev.tsv"):
        f = tmp_path / f"ids_{out}.txt"
        f.write_text("\n".join(ids) + "\n", encoding="utf-8")
        return T.main(["--copy", copies[rel], "--ids", str(f), "--out", str(tmp_path / out), "--spot", "2", *extra],
                      s3=s3, state_runner=lambda *a: runs.append(a[0]) or 0)

    def state():
        s = StateStore()
        try:
            return sorted(r["series_id"] for r in s.csv_retries()), sorted(r["series_id"] for r in s.csv_desktop_owed())
        finally:
            s.db.close()
    return types.SimpleNamespace(T=T, s3=s3, go=go, state=state, runs=runs, copies=copies)


def test_c_the_default_is_todays(ctool):
    T = ctool.T
    assert (T.PREFIX, T.Z1_KEY) == ("fed_board:Z1:", "clean_full/fed_board/Z1.parquet")
    assert (T.prefix_for("Z1"), T.key_for("Z1"), T.release_of(ctool.copies["Z1"])) == (T.PREFIX, T.Z1_KEY, "Z1")
    assert ctool.go("Z1", ["fed_board:Z1:A.Q"], "--apply") == T.EXIT_OK
    q, o = ctool.state()
    assert q == ["fed_board:H15:A.Q"] and o == ["fed_board:H15:B.Q", "fed_board:Z1:A.Q"], (q, o)
    # the copy proof (_compare_one), the ETag read, and the re-HEAD after the pull: all on Z1's key
    assert ctool.s3.calls.count(T.Z1_KEY) == 3 and T.key_for("H15") not in ctool.s3.calls


def test_c_another_release_moves_the_prefix_and_the_guard(ctool):
    T = ctool.T
    assert ctool.go("H15", ["fed_board:Z1:A.Q"], out="a.tsv") == T.EXIT_REFUSED, "Z1 ids under an H15 copy"
    assert ctool.go("H15", ["fed_board:H15:A.Q"], "--apply", out="b.tsv") == T.EXIT_OK
    assert ctool.state()[0] == ["fed_board:Z1:A.Q"]
    assert ctool.s3.calls.count(T.key_for("H15")) == 3 and T.Z1_KEY not in ctool.s3.calls, ctool.s3.calls


def test_c_another_releases_moved_etag_clears_nothing(ctool):
    T = ctool.T
    before = ctool.state()

    def runner(*a):
        ctool.runs.append(a[0])
        ctool.s3.obj[T.key_for("H15")]["ETag"] = '"' + "9" * 32 + '"'
        return 0
    f = ctool.copies["H15"]
    idf = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(f))), "ids_m.txt")
    with open(idf, "w", encoding="utf-8") as fh:
        fh.write("fed_board:H15:A.Q\n")
    rc = T.main(["--copy", f, "--ids", idf, "--out", os.path.join(os.path.dirname(idf), "m.tsv"), "--spot", "2",
                 "--apply"], s3=ctool.s3, state_runner=runner)
    assert rc == T.EXIT_MOVED and ctool.state() == before


def test_c_table_csv_desktop_owed_clears_owed_rows_only(ctool):
    T = ctool.T
    assert ctool.go("Z1", ["fed_board:Z1:A.Q"], "--apply", "--table", "csv_desktop_owed") == T.EXIT_OK
    q, o = ctool.state()
    assert o == ["fed_board:H15:B.Q"] and q == ["fed_board:H15:A.Q", "fed_board:Z1:A.Q"], (q, o)


def test_c_a_series_sidecar_is_not_a_release(ctool):
    T = ctool.T
    assert T.release_of(os.path.join("r", "fed_board", "Z1__series.parquet")) is None
    assert T.release_of(os.path.join("r", "other", "Z1.parquet")) is None
