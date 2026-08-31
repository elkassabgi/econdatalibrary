"""The WU-2a/WU-6 bundle migration's refusals must actually fire.

R503: a guard's except branch IS the guard, and a guard nobody drove is a guard that fails
open. Every refusal below is exercised against the SHIPPED loader, not a re-implementation.
None of these touch the state store — load_receipts is pure.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.purge_state_cursors_bundle import load_receipts  # noqa: E402


def _write(tmp_path, receipts):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({"receipts": receipts}), encoding="utf-8")
    return str(p)


def _ok(**kw):
    base = {"source_id": "bfs", "predicate": "series_key NOT LIKE 'BFS:%'",
            "pre_count": 1168, "match_count": 582, "post_count": 586, "decision": "GO"}
    base.update(kw)
    return base


def test_a_balanced_go_receipt_loads(tmp_path):
    got = load_receipts(_write(tmp_path, [_ok()]))
    assert len(got) == 1 and got[0]["source_id"] == "bfs"


def test_non_go_decisions_are_skipped_not_run(tmp_path):
    """The verifier's verdict is binding: NO-GO / GO-WITH-CHANGE never execute."""
    with pytest.raises(SystemExit):          # only non-GO present -> nothing authorised
        load_receipts(_write(tmp_path, [_ok(decision="NO-GO"),
                                        _ok(source_id="whr", decision="GO-WITH-CHANGE")]))
    got = load_receipts(_write(tmp_path, [_ok(), _ok(source_id="whr", decision="NO-GO")]))
    assert [r["source_id"] for r in got] == ["bfs"]


def test_unbalanced_arithmetic_is_refused(tmp_path):
    """pre - match must equal post IN THE RECEIPT ITSELF — a plan that cannot be right
    is rejected before the store is even pulled."""
    with pytest.raises(SystemExit, match="does not balance"):
        load_receipts(_write(tmp_path, [_ok(post_count=999)]))


def test_zero_match_is_refused(tmp_path):
    """A predicate matching nothing means it MISSED, not that the work is done."""
    with pytest.raises(SystemExit, match="matches 0 rows"):
        load_receipts(_write(tmp_path, [_ok(match_count=0, post_count=1168)]))


@pytest.mark.parametrize("clause", [
    "series_key LIKE 'X%'; DROP TABLE series_cursor",     # statement break
    "1=1 OR source_id != 'bfs'",                          # scope escape
    "series_key LIKE 'X%' -- comment",                    # comment terminator
    "series_key IN (SELECT k FROM t UNION SELECT 1)",     # union smuggling
])
def test_scope_escaping_predicates_are_refused(tmp_path, clause):
    """The clause is a WHERE fragment for ONE source. Anything that could reach another
    source, another table, or a second statement is refused before any pull."""
    with pytest.raises(SystemExit, match="forbidden token"):
        load_receipts(_write(tmp_path, [_ok(predicate=clause)]))


def test_missing_fields_are_refused(tmp_path):
    with pytest.raises(SystemExit, match="missing"):
        load_receipts(_write(tmp_path, [{"source_id": "bfs", "decision": "GO"}]))


def test_empty_plan_is_refused(tmp_path):
    """An empty receipts file is 'I could not measure', never 'nothing to purge'."""
    with pytest.raises(SystemExit, match="no receipts"):
        load_receipts(_write(tmp_path, []))


def test_the_in_flight_check_is_imported_not_retyped():
    """R191/R192: the single-writer check already exists; the bundle must use THAT one."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "tools", "purge_state_cursors_bundle.py"),
               encoding="utf-8").read()
    assert "from tools.prune_series_cursors import runs_in_flight" in src
    assert "def runs_in_flight" not in src, "the bundle re-implemented the in-flight check"


def test_only_the_invariant_count_gates_execution():
    """The verifiers' required change: match_count is the ONLY number pinned against the
    live store. It is the delete set itself, and it cannot drift — every predicate selects
    a legacy shape current code cannot emit, and put_series_cursors is upsert-only. pre/post
    DO drift (fed_board alone could add +44,967 on republish), so pinning them would abort a
    correct all-or-nothing session over a legitimate change. post is checked by the
    arithmetic identity instead."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "tools", "purge_state_cursors_bundle.py"),
               encoding="utf-8").read()
    assert 'ok = match == r["match_count"]' in src, "pre-flight no longer pins match_count"
    assert 'ok = pre == r["pre_count"]' not in src, "pre_count is being pinned again"
    assert "ok = post == pre - match" in src, "post is no longer checked by the identity"
    assert 'post == r["post_count"]' not in src, "post_count literal is being pinned again"


def test_delete_is_always_source_scoped():
    """Structural containment: every DELETE wraps the clause with an explicit
    source_id bind, so a receipt cannot reach another source's rows."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "tools", "purge_state_cursors_bundle.py"),
               encoding="utf-8").read()
    assert 'DELETE FROM series_cursor WHERE source_id=? AND ({r[\'predicate\']})' in src
    # Count only EXECUTABLE deletes (the f-string form). Plain "DELETE FROM" also occurs
    # in the module docstring, where it documents this very invariant.
    assert src.count('f"DELETE FROM') == 1, "more than one executable DELETE in the tool"


def test_pre_count_is_a_monotone_up_band_and_the_mapper_gate_exists():
    """The synthesis's two remaining gaps, both now closed.

    (1) pre_count may only GROW. Cursors are upsert-only (state.py:141) and the only
    DELETEs live in the purge tools, so a FALL means another writer deleted rows — a
    different store, not drift. A jump larger than CURSOR_CAP is likewise unexplained.

    (2) The property that actually authorises the deletes is not a count at all: every
    doomed key must map to NOTHING in the catalogue. It must run under FORCED r2
    semantics, because the local backend's derive-all fallback returns every id of a
    small-catalogue source with unmapped=[], so provably dead keys read as 100% mapped —
    the artefact that fooled two measuring agents on this very migration. A gate that
    fails open is not a gate (R503)."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "tools", "purge_state_cursors_bundle.py"),
               encoding="utf-8").read()
    assert "if drift < 0:" in src, "negative pre_count drift is no longer refused"
    assert "elif drift > _CURSOR_CAP:" in src, "an unexplained pre_count jump is not refused"
    assert "pre_count FELL" in src and "pre_count JUMPED" in src
    i = src.find("mapper gate")
    assert i != -1, "the mapper gate is gone"
    block = src[i - 1500:i + 1500]
    assert '_cfg.BACKEND = "r2"' in block, "the mapper gate no longer FORCES r2 semantics"
    assert "_orc._catalog_ids_for(" in block, "the gate no longer drives the shipped mapper"
    assert "if ids:" in block, "the gate no longer refuses when a doomed key maps"
    assert "_cfg.BACKEND = _saved" in block, "the gate leaks its backend override"
