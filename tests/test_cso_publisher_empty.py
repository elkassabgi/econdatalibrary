"""cso: a never-stored matrix with nothing to store is skipped until CSO changes it, not retried as a
transient failure on every run (2026-09-23).

Twelve matrices were booked "unparsed" transient on every run, keeping cso partial. Probed live:
HRD51 (108 of 108 values null) and DOTA09 (450,846 of 450,846 null) are published EMPTY; ROA41..ROA52
index time by rolling five-year windows ('2019-2023'), which the period grammar declines on purpose
(R288). None of the twelve is stored. The real update(), cursor and held logic run; CSO is faked.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import cso  # noqa: E402

_spec = importlib.util.spec_from_file_location("_cso_ingest_t", os.path.join(ROOT, "jobs", "ingest_cso_ireland.py"))
ING = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ING)


def _body(time_codes, values, role_time=True):
    return {"id": ["STATISTIC", "TLIST(A1)"], "size": [1, len(time_codes)],
            "role": {"time": ["TLIST(A1)"]} if role_time else {},
            "dimension": {"STATISTIC": {"category": {"index": {"S1": 0}, "label": {"S1": "s"}}},
                          "TLIST(A1)": {"category": {"index": {c: i for i, c in enumerate(time_codes)},
                                                     "label": {c: c for c in time_codes}}}},
            "value": values}


def test_the_ingester_names_why_a_body_parsed_to_nothing():
    assert ING._why_unparsed(_body(["2025"], [None])) == "all_null"
    assert ING._why_unparsed(_body(["2019-2023", "2020-2024"], [1.0, None])) == "span_time"
    assert ING._why_unparsed(_body(["2019-2023", "2024"], [1.0, 2.0])) == "unparsed", "mixed: ours"
    assert ING._why_unparsed(_body(["2019-2023"], [1.0], role_time=False)) == "span_time", "TLIST is time"
    assert ING._why_unparsed(_body(["x1", "x2"], [1.0, 2.0])) == "unparsed"


OUTCOMES = {"HRD51": "all_null", "ROA41": "span_time", "GAP01": "unparsed", "HELD1": "all_null"}


def _run(tmp_path, monkeypatch, held=("HELD1", "OTHER"), matrices=tuple(OUTCOMES)):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(cso.config, "source_dir", lambda s: str(tmp_path))
    upd = {m: "2026-09-02T11:00:00Z" for m in matrices}
    monkeypatch.setattr(cso, "_collection_updates", lambda epoch, timeout=300: (dict(upd), None))
    monkeypatch.setattr(cso, "search_vintages", lambda *a, **k: {})
    monkeypatch.setattr(cso, "_matrix_subject_map", lambda force=False: {m: "1_Sub" for m in matrices})
    (tmp_path / "_held.json").write_text(json.dumps(sorted(held)))
    asked = []

    def _fetch(mtr):
        asked.append(mtr)
        return [], OUTCOMES[mtr]
    monkeypatch.setattr(cso, "_ingester", lambda: types.SimpleNamespace(fetch_table_detailed=_fetch))
    res = cso.update(types.SimpleNamespace(config={}, key="cso/_all"), None)
    cursor = json.loads((tmp_path / "_collupd.json").read_text())
    held_after = set(json.loads((tmp_path / "_held.json").read_text()))
    return res, cursor, held_after, asked


def test_a_never_stored_empty_or_undated_matrix_is_skipped_and_its_cursor_advances(tmp_path, monkeypatch):
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch)
    err = res.error or ""
    assert "HRD51" not in err and "ROA41" not in err, err
    assert "HRD51" in cursor and "ROA41" in cursor, "fetched again only when CSO changes them"
    assert "HRD51" not in held_after and "ROA41" not in held_after, "never claimed as held"


def test_a_stored_matrix_that_comes_back_all_null_is_still_transient(tmp_path, monkeypatch):
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch)
    assert res.status == "partial" and "HELD1: all_null" in (res.error or ""), res.error
    assert "HELD1" not in cursor, "the publisher blanked a table we serve: retried"


def test_a_real_parser_gap_is_still_transient(tmp_path, monkeypatch):
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch)
    assert "GAP01: unparsed" in (res.error or "") and "GAP01" not in cursor


def test_a_stored_matrix_missing_from_held_is_still_transient(tmp_path, monkeypatch):
    """Review R1113: _held.json is not the stored set (67 stored matrices missing from it on R2).
    Whether a matrix is stored is read from its subject parquet."""
    pq.write_table(pa.table({"series_key": ["CSO:HRD51:STATISTIC=S1"],
                             "obs_date": pa.array([__import__("datetime").date(2024, 12, 31)]),
                             "value": [1.0]}), str(tmp_path / "1_Sub.parquet"))
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch)
    assert "HRD51: all_null" in (res.error or "") and "HRD51" not in cursor, res.error
    assert "ROA41" in cursor, "a matrix the subject file does not hold is still skipped"


def test_an_unreadable_subject_file_is_treated_as_stored(tmp_path, monkeypatch):
    (tmp_path / "1_Sub.parquet").write_bytes(b"not a parquet")
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch)
    assert "HRD51: all_null" in (res.error or "") and "HRD51" not in cursor, res.error


def test_with_no_held_set_the_store_still_decides(tmp_path, monkeypatch):
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, held=())
    assert "HRD51" in cursor and "HRD51" not in (res.error or ""), res.error


def test_the_next_run_does_not_ask_again_until_cso_changes_it(tmp_path, monkeypatch):
    _run(tmp_path, monkeypatch, matrices=("HRD51", "ROA41", "HELD1"))
    res, cursor, held_after, asked = _run(tmp_path, monkeypatch, matrices=("HRD51", "ROA41", "HELD1"))
    assert asked == ["HELD1"], asked


def test_a_batch_of_only_skipped_matrices_is_not_a_wholesale_outage(tmp_path, monkeypatch):
    names = tuple(f"ROA{i:02d}" for i in range(12))
    for n in names:
        OUTCOMES[n] = "span_time"
    try:
        res, cursor, held_after, asked = _run(tmp_path, monkeypatch, matrices=names)
    finally:
        for n in names:
            OUTCOMES.pop(n)
    assert res.status != "partial" and len(asked) == 12, (res.status, res.error)
