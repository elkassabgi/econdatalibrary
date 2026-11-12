"""AR-477 round 6.

M (MEDIUM, regression of round 5): send_digest._kinds() imported UNHOMED_NOTE from updater.orchestrate, which
pulls pyarrow. The digest is an `if: always()` step, so on a morning when the dependency install failed it
crashed and no mail went out. The marker now lives in the stdlib-only updater/notes.py. The first test is the
reviewer's probe (tests/test_zz_digest_import_probe.py in the review clone): the REAL send_digest.main() with
updater.orchestrate made unimportable.

Z4: run_once's dry run `continue`s before strat.run, so the receipt rule's dry guard was unreachable through
run_once. The rule is now orchestrate._record_build_receipt, and a dry pass carrying a receipt is tested on it
directly. N4: _note_unmapped_debt's store-is-None branch is tested directly."""
from __future__ import annotations

import contextlib
import io
import sqlite3
import sys
import types

from updater.notes import UNHOMED_NOTE
from updater.strategies.base import Result


def test_r6_digest_mails_when_orchestrate_cannot_be_imported(tmp_path, monkeypatch):
    from updater import registry as R
    from updater import send_digest as D
    db = tmp_path / "state.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, "
                "last_success_utc TEXT, last_error TEXT, last_attempt_utc TEXT)")
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                ("yyy_managed_made_up", "failed", None, None, "fetch failed: boom", "2026-11-12T06:00:00Z"))
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                ("zzz_unhomed_made_up", "partial", None, None,
                 f"{UNHOMED_NOTE}: a changed id could not be booked; csv coverage note: none failed",
                 "2026-11-12T06:00:00Z"))
    con.commit(); con.close()
    monkeypatch.setattr(D, "STATE", str(db))
    monkeypatch.setattr(R, "load", lambda: {"sources": [
        {"source_id": "yyy_managed_made_up", "live": True, "cadence": "daily", "run_location": "cloud"},
        {"source_id": "zzz_unhomed_made_up", "live": True, "cadence": "daily", "run_location": "cloud"}]})
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    monkeypatch.setenv("RUN_STATUS", "failure")
    monkeypatch.setitem(sys.modules, "updater.orchestrate", None)       # the install failed: unimportable
    if hasattr(D, "_KINDS_CACHE"):
        monkeypatch.setattr(D, "_KINDS_CACHE", [])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        D.main()
    out = buf.getvalue()
    assert "yyy_managed_made_up" in out, out[-1500:]
    # the UNHOMED kind still works without orchestrate: the row is filed as a real failure, not SUBSET
    line = next(ln for ln in out.splitlines() if "zzz_unhomed_made_up" in ln and "!!" in ln)
    assert "UNHOMED" in line and "SUBSET" not in line, line


def test_r6_send_digest_source_never_imports_orchestrate_in_the_kind_path():
    """Static guard: the classifier reads the marker from updater.notes only."""
    import inspect
    from updater import send_digest as D
    src = inspect.getsource(D._kinds) + inspect.getsource(D.kind_of)
    assert "updater.notes" in src
    assert "import" not in src.replace("from updater.notes import UNHOMED_NOTE", "")


def test_r6_notes_module_is_stdlib_only():
    import ast
    import updater.notes as N
    tree = ast.parse(open(N.__file__, encoding="utf-8").read())
    assert not [n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))], \
        "updater/notes.py must import nothing"
    from updater import orchestrate
    assert orchestrate.UNHOMED_NOTE is N.UNHOMED_NOTE, "orchestrate must use the one shared constant"


# ------------------------------------------------------------------ Z4: the receipt rule, each case alone
class _Store:
    def __init__(self):
        self.rows = []

    def note_build_receipt(self, source_id, unit_id, receipt):
        self.rows.append((source_id, unit_id, receipt))


_UNIT = types.SimpleNamespace(key="fhfa/_all", source_id="fhfa", unit_id="_all")


def _res(receipt="B1"):
    return Result(status="ok", build_receipt=receipt)


def test_r6_receipt_rule_control_an_ok_pass_records():
    from updater import orchestrate as O
    st = _Store()
    assert O._record_build_receipt(st, _UNIT, _res(), "ok", dry=False, unhomed=False)
    assert O._record_build_receipt(st, _UNIT, _res("B2"), "partial", dry=False, unhomed=False)
    assert st.rows == [("fhfa", "_all", "B1"), ("fhfa", "_all", "B2")]


def test_r6_a_dry_pass_carrying_a_receipt_records_none():
    from updater import orchestrate as O
    st = _Store()
    assert not O._record_build_receipt(st, _UNIT, _res(), "ok", dry=True, unhomed=False)
    assert not O._record_build_receipt(st, _UNIT, _res(), "partial", dry=True, unhomed=False)
    assert st.rows == []


def test_r6_receipt_rule_status_unhomed_and_empty_receipt_record_none():
    from updater import orchestrate as O
    st = _Store()
    for status in ("transient_fail", "no_change", "definitive_fail"):
        assert not O._record_build_receipt(st, _UNIT, _res(), status, dry=False, unhomed=False), status
    assert not O._record_build_receipt(st, _UNIT, _res(), "ok", dry=False, unhomed=True)
    assert not O._record_build_receipt(st, _UNIT, _res(None), "ok", dry=False, unhomed=False)
    assert st.rows == []


# ------------------------------------------------------------------ N4: no store at all
def test_r6_unmapped_debt_with_no_store_signals_unhomed(monkeypatch):
    from updater import orchestrate as O
    monkeypatch.setattr(O, "_UNHOMED_SOURCES", set())
    assert O._note_unmapped_debt(None, "fhfa", ["k1"], "probe") is False
    assert "fhfa" in O._UNHOMED_SOURCES


def test_r6_unmapped_debt_recorded_does_not_signal(tmp_path, monkeypatch):
    """Control: a working store records the keys and leaves no signal."""
    from updater import orchestrate as O
    from updater.state import StateStore
    monkeypatch.setattr(O, "_UNHOMED_SOURCES", set())
    st = StateStore(path=str(tmp_path / "s.db"))
    assert O._note_unmapped_debt(st, "fhfa", ["k1"], "probe") is True
    assert "fhfa" not in O._UNHOMED_SOURCES
    assert [r["source_id"] for r in st.full_rederives_owed()] == ["fhfa"]
    st.db.close()
