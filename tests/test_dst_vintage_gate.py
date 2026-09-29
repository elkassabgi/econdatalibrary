"""dst: a clean pass stores the catalogue token, so an unchanged StatBank catalogue is skipped.

finalize() stamps new_vintage="date-tail" on every Result, and overwrite_if_changed fills in the probed token
only when a fetcher returns None - so dst's unit stored "date-tail", the probe never matched, and every daily
tick ran update() (919-1,020 s to report no_change on 2026-09-05/06). Same defect as unctad (#83).
Hermetic: the StatBank HTTP layer is faked; the store is a tmp dir under the LOCAL backend and the merge is
the real merge.merge_and_write.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import dst as D  # noqa: E402
from updater.strategies.overwrite_if_changed import OverwriteIfChanged  # noqa: E402

CAT = [{"id": "AUS07", "updated": "2026-09-01T08:00:00"}, {"id": "FOLK1A", "updated": "2026-08-11T08:00:00"}]


def _rows(tid):
    return [(f"DST:{tid}:X=1", dt.date(2026, 1, 1), 1.0), (f"DST:{tid}:X=1", dt.date(2026, 2, 1), 2.0)], False


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(D.config, "source_dir", lambda s: str(tmp_path / s))
    cat = {"tables": [dict(t) for t in CAT]}
    monkeypatch.setattr(D, "_fetch_catalog", lambda *a, **k: [dict(t) for t in cat["tables"]])
    monkeypatch.setattr(D, "_fetch_table_rows", _rows)
    monkeypatch.setattr(D, "RATE", 0)
    return cat


def test_a_clean_pass_stores_the_token_the_probe_reads(store):
    first = D.update(None, None)
    assert first.status == "ok", (first.status, first.error)
    assert first.new_vintage == D.current_vintage(None) == D._catalog_token(CAT)
    again = D.update(None, None)                          # nothing due now: no_change, same token
    assert again.status == "no_change" and again.new_vintage == first.new_vintage


def test_a_pass_that_ends_partial_does_not_claim_the_catalogue(store, monkeypatch):
    """A transient table stays due; stamping the token would let the gate skip it until DST moves again."""
    def flaky(tid):
        return ([], True) if tid == "FOLK1A" else _rows(tid)
    monkeypatch.setattr(D, "_fetch_table_rows", flaky)
    res = D.update(None, None)
    assert res.status == "partial" and res.new_vintage != D._catalog_token(CAT), (res.status, res.new_vintage)


def test_a_budget_spent_pass_does_not_claim_the_catalogue(store, monkeypatch):
    monkeypatch.setattr(D, "Deadline", lambda minutes: types.SimpleNamespace(spent=lambda: True,
                                                                             elapsed_min=lambda: 0.0))
    res = D.update(None, None)
    assert res.status == "partial" and "budget spent" in (res.error or "")
    assert res.new_vintage != D._catalog_token(CAT)


def test_a_table_republished_during_the_pass_reads_as_changed_next_tick(store, monkeypatch):
    """The stamped token is the catalogue update() read at its START: a table republished WHILE the pass runs
    (after its rows were read) is not claimed, so the next tick's probe differs and re-pulls it."""
    def republish_mid_pass(tid):
        store["tables"][0]["updated"] = "2026-09-29T08:00:00"      # DST moves while we fetch
        return _rows(tid)
    monkeypatch.setattr(D, "_fetch_table_rows", republish_mid_pass)
    res = D.update(None, None)
    assert res.status == "ok" and res.new_vintage == D._catalog_token(CAT), res.new_vintage
    assert D.current_vintage(None) != res.new_vintage


def test_a_refused_subject_merge_does_not_claim_the_catalogue(store, monkeypatch):
    """merge_and_write refuses FOLK1's subject (never-shrink): the table stays owed, so no token (review
    R1283, mutant M1: the refusal no longer tallied transient)."""
    from updater.errors import DefinitiveError
    real = D.merge.merge_and_write

    def refuse(path, tbl, **kw):
        if os.path.basename(path).startswith("FOLK"):
            raise DefinitiveError("never-shrink refusal")
        return real(path, tbl, **kw)
    monkeypatch.setattr(D.merge, "merge_and_write", refuse)
    res = D.update(None, None)
    assert res.status == "partial" and res.new_vintage != D._catalog_token(CAT), (res.status, res.new_vintage)


def test_the_not_due_return_stamps_the_catalogue_it_compared(store, monkeypatch):
    """A republish that lands while the not-due pass reads the store frontier (~15 min under r2) must not
    be claimed (review R1283, mutant M2: a late catalogue read on the not-due return)."""
    first = D.update(None, None)
    assert first.status == "ok"
    real_gmd = D._global_max_date

    def moving():
        store["tables"][1]["updated"] = "2026-09-29T08:00:00"
        return real_gmd()
    monkeypatch.setattr(D, "_global_max_date", moving)
    again = D.update(None, None)
    assert again.status == "no_change" and again.new_vintage == D._catalog_token(CAT), again.new_vintage


def test_a_lost_subject_is_re_pulled_when_update_runs(store):
    """The gate's safety rests on this clause of `due`: a subject missing on disk is re-pulled on any run
    that is not skipped (review R1283, mutant M4)."""
    first = D.update(None, None)
    assert first.status == "ok"
    os.remove(D._subj_path(D._subj("FOLK1A")))
    D.update(None, None)
    assert os.path.exists(D._subj_path(D._subj("FOLK1A"))), "a missing subject was not re-pulled"


def test_a_catalogue_without_updated_stamps_never_seals_the_unit(store):
    """If DST dropped or nulled 'updated', the token would hash only the ids - a constant that matches for
    ever. The gate token is then None: the probe fetches and the pass stamps nothing (as unctad, R1154)."""
    for t in store["tables"]:
        t["updated"] = None
    assert D.current_vintage(None) is None
    res = D.update(None, None)
    assert res.new_vintage == "date-tail", res.new_vintage


def test_the_strategy_skips_an_unchanged_catalogue_and_fetches_the_placeholder(monkeypatch):
    token = D._catalog_token(CAT)
    fetcher = types.SimpleNamespace(current_vintage=lambda unit: token)
    monkeypatch.setattr("updater.strategies.overwrite_if_changed.get_fetcher", lambda sid: fetcher)
    unit = types.SimpleNamespace(source_id="dst")
    s = OverwriteIfChanged()
    assert s.detect_change(unit, {"upstream_vintage": token}) is None
    assert s.detect_change(unit, {"upstream_vintage": "date-tail"}) == token   # every dst unit today: fetch once
