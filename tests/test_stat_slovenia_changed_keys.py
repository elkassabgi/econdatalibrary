"""stat_slovenia reports a MERGE-MEASURED changed set at catalogue grain ('SI:<table_id>').

Before this, the orchestrator took the changed set from the cursors, which are keyed by the bare
table id ('0214809S') while the catalogue id is 'stat_slovenia:SI:0214809S'. None ever mapped (0 of
4,439 stored cursors), so every pass booked "csv coherence unmet: N changed series_keys have no
catalog mapping", which kept a pure budget-deferral pass out of ROTATING and demoted every
cycle-completing pass to partial. The real update() runs over a real store; SURS is faked, and the
mapping and the health verdict are asked of the PRODUCTION functions.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import stat_slovenia as S  # noqa: E402
from updater import orchestrate as O  # noqa: E402
from updater import health as H  # noqa: E402

GROUPS = ("AAA", "BBB")


class _Deadline:
    def __init__(self, allow):
        self.allow, self.asked = allow, 0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 40.0


def _meta(years):
    return {"title": "t", "variables": [{"code": "X", "values": ["0"]},
                                        {"code": "LETO", "values": list(years), "time": True}]}


def _body(year, value):
    return {"class": "dataset", "id": ["X", "LETO"], "size": [1, 1],
            "dimension": {"X": {"category": {"index": {"0": 0}}},
                          "LETO": {"category": {"index": {year: 0}}}},
            "role": {"time": ["LETO"]}, "value": [value]}


def _wire(monkeypatch, tmp_path, stored, years, post, allow=99):
    """stored: {group: [(series_key, date, value)]}; post: {table_id: json-stat2 body}."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(S, "_out_dir", lambda: str(tmp_path))
    monkeypatch.setattr(S, "RATE", 0)
    (tmp_path / "_catalog.json").write_text(json.dumps([{"id": f"{g}01S.px"} for g in GROUPS]))
    for g, rows in stored.items():
        p = tmp_path / f"{g}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": [r[0] for r in rows],
                                     "obs_date": pa.array([r[1] for r in rows], pa.date32()),
                                     "value": pa.array([r[2] for r in rows], pa.float64())}), p)
    monkeypatch.setattr(S, "Deadline", lambda minutes=None: _Deadline(allow))
    monkeypatch.setattr(S, "_get_meta", lambda sess, tid: _meta(years))
    monkeypatch.setattr(S, "_post_query", lambda sess, tid, body: post.get(tid.replace(".px", "")))
    return types.SimpleNamespace(config={}, key="stat_slovenia/_all")


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    p = tmp_path / "cat" / "catalog.db"
    p.parent.mkdir()
    c = sqlite3.connect(p)
    c.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    c.executemany("INSERT INTO series VALUES (?, 'stat_slovenia')",
                  [("stat_slovenia:SI:AAA01S",), ("stat_slovenia:SI:BBB01S",)])
    c.commit()
    c.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    return p


def _stored(year_aaa=2024, year_bbb=2025):
    return {"AAA": [("SI:AAA01S:X=0", dt.date(year_aaa, 12, 31), 1.0)],
            "BBB": [("SI:BBB01S:X=0", dt.date(year_bbb, 12, 31), 2.0)]}


def test_a_one_table_merge_reports_its_table_and_maps_exactly(monkeypatch, tmp_path, catalog):
    (tmp_path / "store").mkdir()
    unit = _wire(monkeypatch, tmp_path / "store", _stored(), ["2024", "2025"],
                 {"AAA01S": _body("2025", 7.0)})
    res = S.update(unit, None)
    assert res.status == "ok", (res.status, res.error)
    assert {k: str(v)[:10] for k, v in res.changed_keys.items()} == {"SI:AAA01S": "2025-12-31"}
    ids, unmapped = O._map_changed("stat_slovenia", res.changed_keys, merge_measured=True)
    assert ids == ["stat_slovenia:SI:AAA01S"] and unmapped == [], (ids, unmapped)


def test_an_identical_refetch_reports_nothing_changed(monkeypatch, tmp_path):
    # A far-future stored max (the year-6000 sentinel class) makes the fetcher re-pull every
    # parseable code, so the 2024 value already stored comes back IDENTICAL through the real merge.
    (tmp_path / "store").mkdir()
    stored = {"AAA": [("SI:AAA01S:X=0", dt.date(2024, 12, 31), 1.0),
                      ("SI:AAA01S:X=9", dt.date(6000, 12, 31), 3.0)],
              "BBB": [("SI:BBB01S:X=0", dt.date(2025, 12, 31), 2.0)]}
    posted = []
    body = _body("2024", 1.0)

    def _post(sess, tid, b):
        posted.append(tid)
        return body if tid.startswith("AAA") else None
    unit = _wire(monkeypatch, tmp_path / "store", stored, ["2024"], {})
    monkeypatch.setattr(S, "_post_query", _post)
    res = S.update(unit, None)
    assert posted and posted[0].startswith("AAA"), "control: the re-fetch must really POST and merge"
    assert res.changed_keys == {}, res.changed_keys


def test_a_revised_value_in_a_refetch_is_reported(monkeypatch, tmp_path):
    """Positive control for the test above: the same re-fetch with a DIFFERENT value is a change."""
    (tmp_path / "store").mkdir()
    stored = {"AAA": [("SI:AAA01S:X=0", dt.date(2024, 12, 31), 1.0),
                      ("SI:AAA01S:X=9", dt.date(6000, 12, 31), 3.0)],
              "BBB": [("SI:BBB01S:X=0", dt.date(2025, 12, 31), 2.0)]}
    unit = _wire(monkeypatch, tmp_path / "store", stored, ["2024"], {"AAA01S": _body("2024", 1.5)})
    res = S.update(unit, None)
    assert set(res.changed_keys) == {"SI:AAA01S"}, res.changed_keys


def test_a_deferral_pass_reads_rotating_in_production_health(monkeypatch, tmp_path, catalog):
    (tmp_path / "store").mkdir()
    unit = _wire(monkeypatch, tmp_path / "store", _stored(), ["2024", "2025"],
                 {"AAA01S": _body("2025", 7.0)}, allow=1)           # AAA worked, BBB deferred
    res = S.update(unit, None)
    assert res.status == "partial" and "1 deferred by budget" in (res.error or ""), res.error
    ids, unmapped = O._map_changed("stat_slovenia", res.changed_keys, merge_measured=True)
    assert ids and not unmapped, (ids, unmapped)      # all mapped: the orchestrator adds no coherence note
    assert H._deferral_only([{"status": "partial", "last_error": res.error}]) is True, res.error
    # NEGATIVE CONTROL: the note the old cursor channel produced (0 mapped) keeps it at ATTENTION.
    bare_ids, bare_un = O._map_changed("stat_slovenia", sorted(res.series_cursors), merge_measured=True)
    assert bare_ids == [] and bare_un, (bare_ids, bare_un)
    note, demote = O._classify_zero_mapped("stat_slovenia", "full", 2, None, 0, len(bare_un))
    assert demote and note.startswith("csv coherence unmet")
    assert H._deferral_only([{"status": "partial", "last_error": f"{res.error}; {note}"}]) is False


def test_fabricated_and_foreign_keys_do_not_map(catalog):
    ids, unmapped = O._map_changed("stat_slovenia", {"SI:ZZZ01S": None}, merge_measured=True)
    assert ids == [] and unmapped == ["SI:ZZZ01S"]
    assert S._table_key("SI:AAA01S:X=0:Y=1") == "SI:AAA01S"
    assert S._table_key("SI:0723405S.PX:X=0") == "SI:0723405S.PX"   # the catalogue keeps '.PX' too
    assert S._table_key("weird") == "weird"                           # returned WHOLE, so it shows unmapped


def test_a_table_reports_the_latest_changed_date_of_its_series(monkeypatch, tmp_path):
    """Two series of one table change at different dates: the table carries the LATER one."""
    (tmp_path / "store").mkdir()
    body = {"class": "dataset", "id": ["X", "LETO"], "size": [2, 2],
            "dimension": {"X": {"category": {"index": {"0": 0, "1": 1}}},
                          "LETO": {"category": {"index": {"2025": 0, "2026": 1}}}},
            "role": {"time": ["LETO"]}, "value": [5.0, None, None, 6.0]}
    unit = _wire(monkeypatch, tmp_path / "store", _stored(), ["2024", "2025", "2026"],
                 {"AAA01S": body})
    monkeypatch.setattr(S, "_get_meta", lambda sess, tid: {"title": "t", "variables": [
        {"code": "X", "values": ["0", "1"]},
        {"code": "LETO", "values": ["2024", "2025", "2026"], "time": True}]})
    res = S.update(unit, None)
    assert {k: str(v)[:10] for k, v in res.changed_keys.items()} == {"SI:AAA01S": "2026-12-31"}
