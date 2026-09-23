"""stat_slovenia: a per-table declared time axis for the two tables SURS mis-flags (2026-09-23).

SURS sets `time: true` on a CATEGORY axis of 1517309S and 1012308S. The shared resolver refuses a
mis-flagged axis and never substitutes another (R331), so both read "time axis parses to no dates"
on every run and stat_slovenia could never read ok. The recorded decision allows a per-table
override, never a resolver rule. The variables below are SURS's live metadata of 2026-09-23.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import stat_slovenia as S  # noqa: E402

QUARTERS = ["2025Q1", "2025Q2", "2025Q3", "2025Q4", "2026Q1", "2026Q2", "2026Q3", "2026Q4", "2027Q1", "2027Q2"]
PIGS = [{"code": "ŠTEVILO PRAŠIČEV", "text": "NUMBER OF PIGS", "values": ["TOT"],
         "valueTexts": ["Number of pigs - TOTAL"], "time": True},
        {"code": "ČETRTLETJE", "text": "QUARTER", "values": QUARTERS, "valueTexts": QUARTERS}]
CULTURE = [{"code": "ORGANIZACIJSKA OBLIKA", "text": "ORGANISATIONAL FORM", "values": list("0123456"),
            "valueTexts": ["TOTAL", "Public institute", "Private institute", "Society", "Institution",
                           "Company", "Other"], "time": True},
           {"code": "LETO", "text": "YEAR", "values": ["2012"], "valueTexts": ["2012"]}]
PIG_KEY = "SI:1517309S:ŠTEVILO PRAŠIČEV=TOT"


def test_the_declared_axis_is_used_for_the_two_tables():
    assert S._time_var(PIGS, "1517309S") == ("ČETRTLETJE", QUARTERS)
    assert S._time_var(CULTURE, "1012308S.px") == ("LETO", ["2012"])


def test_every_other_table_keeps_the_resolver_rule():
    """The same shape under any other id keeps the flagged axis (whose codes parse to no date, so
    update() books it structural): the override is per table, not a rule."""
    assert S._time_var(PIGS, "1517310S") == ("ŠTEVILO PRAŠIČEV", ["TOT"])
    assert S._time_var(PIGS) == ("ŠTEVILO PRAŠIČEV", ["TOT"])


def test_the_entry_goes_inert_when_the_publisher_fixes_its_flag():
    fixed = [dict(PIGS[0], time=None), dict(PIGS[1], time=True)]
    assert S._meta_time_code(fixed, "1517309S") == "ČETRTLETJE"      # the flag itself, now right
    dated = [dict(PIGS[0], values=["2024"], valueTexts=["2024"]), PIGS[1]]
    assert S._meta_time_code(dated, "1517309S") == "ŠTEVILO PRAŠIČEV", \
        "a flagged axis that parses to dates is the publisher's call, never overridden"
    positional = [dict(PIGS[0], values=["0", "1"], valueTexts=["2024", "2025"]), PIGS[1]]
    assert S._meta_time_code(positional, "1517309S") == "ŠTEVILO PRAŠIČEV", \
        "positional codes with year LABELS are a readable time axis too (review AR-125)"


def test_a_declared_axis_the_table_no_longer_has_is_ignored():
    gone = [PIGS[0], dict(PIGS[1], code="KVARTAL")]
    assert S._meta_time_code(gone, "1517309S") == "ŠTEVILO PRAŠIČEV"


def test_the_query_tails_the_declared_axis_only():
    q = S._build_query(PIGS, ["2027Q3"], "1517309S")
    assert {v["code"]: v["selection"]["values"] for v in q} == {"ŠTEVILO PRAŠIČEV": ["TOT"],
                                                                 "ČETRTLETJE": ["2027Q3"]}


def _jsonstat(quarters, values):
    return {"class": "dataset", "id": ["ŠTEVILO PRAŠIČEV", "ČETRTLETJE"], "size": [1, len(quarters)],
            "role": {"time": ["ŠTEVILO PRAŠIČEV"]},                    # SURS repeats its mis-flag here
            "dimension": {"ŠTEVILO PRAŠIČEV": {"category": {"index": {"TOT": 0},
                                                            "label": {"TOT": "Number of pigs - TOTAL"}}},
                          "ČETRTLETJE": {"category": {"index": {q: i for i, q in enumerate(quarters)},
                                                      "label": {q: q for q in quarters}}}},
            "value": values}


def _store(tmp_path, monkeypatch, meta, posted):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(S, "_out_dir", lambda: str(tmp_path))
    monkeypatch.setattr(S, "RATE", 0)
    (tmp_path / "_catalog.json").write_text(json.dumps([{"id": "1517309S.px"}]), encoding="utf-8")
    dates = [S._parse_date(q) for q in QUARTERS]
    pq.write_table(pa.table({"series_key": [PIG_KEY] * len(dates), "obs_date": pa.array(dates, pa.date32()),
                             "value": [float(i) for i in range(len(dates))]}), str(tmp_path / "151.parquet"))
    monkeypatch.setattr(S, "_get_meta", lambda sess, tid: {"title": "pigs", "variables": meta})

    def _post(sess, tid, body):
        posted.append(body)
        tq = next(v for v in body["query"] if v["code"] == "ČETRTLETJE")["selection"]["values"]
        return _jsonstat(tq, [123.0] * len(tq))
    monkeypatch.setattr(S, "_post_query", _post)
    return types.SimpleNamespace(config={}, key="stat_slovenia/_all")


def test_a_current_table_is_no_longer_a_structural_break(tmp_path, monkeypatch):
    posted = []
    res = S.update(_store(tmp_path, monkeypatch, PIGS, posted), None)
    assert "1517309S" not in (res.error or ""), res.error
    assert res.status != "partial" and posted == [], (res.status, posted)


def test_a_new_quarter_is_fetched_and_lands_on_the_stored_key(tmp_path, monkeypatch):
    posted = []
    meta = [PIGS[0], dict(PIGS[1], values=QUARTERS + ["2027Q3"], valueTexts=QUARTERS + ["2027Q3"])]
    res = S.update(_store(tmp_path, monkeypatch, meta, posted), None)
    assert res.status != "partial", res.error
    assert [{v["code"]: v["selection"]["values"] for v in b["query"]} for b in posted] == \
        [{"ŠTEVILO PRAŠIČEV": ["TOT"], "ČETRTLETJE": ["2027Q3"]}], posted
    t = pq.read_table(str(tmp_path / "151.parquet")).to_pylist()
    assert {r["series_key"] for r in t} == {PIG_KEY}, "a new key would orphan the stored series"
    assert max(r["obs_date"] for r in t) == S._parse_date("2027Q3")


def test_negative_control_without_the_entry_the_table_is_structural(tmp_path, monkeypatch):
    """What every run did before: a structural break (the orchestrator books it partial)."""
    monkeypatch.setattr(S, "TIME_AXIS_OVERRIDE", {})
    with pytest.raises(S.DefinitiveError, match="1517309S: time axis parses to no dates"):
        S.update(_store(tmp_path, monkeypatch, PIGS, []), None)
