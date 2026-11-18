"""Forward-dated series get ONE neutral qualifier on the page and in the meta description (AR_PREDEPLOY C5).

A page reading "582 series, 1801-2150" invites the reader to take 2150 as observed. The coverage bounds and
schema.org temporalCoverage stay as they are (= /v1/sources data_through); the qualifier is added only when the
source holds series whose end_date lies beyond the period that contains the build date, at the series' own
frequency. The build date is pinned in every test (TODAY is monkeypatched), so nothing here depends on the clock.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
BUILD = "2026-11-18"


@pytest.fixture()
def G(monkeypatch):
    spec = importlib.util.spec_from_file_location("gen_site_forward_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    monkeypatch.setattr(mod, "TODAY", BUILD)
    return mod


def _db(rows):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, frequency TEXT, start_date TEXT, "
                "end_date TEXT, geography TEXT, last_updated TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?, ?, ?, ?, NULL, '2026-11-01')",
                    [(sid, sid.split(":")[0], f, s, e) for sid, f, s, e in rows])
    return con


def _roll(G, con):
    con.row_factory = sqlite3.Row
    return {r["source_id"]: dict(r) for r in con.execute(G.ROLLUP_SQL, G.forward_cutoffs(G.TODAY))}


def _rec(G, sid, roll):
    src = {"name": "Test Statistical Office (TSO)", "license_id": "cc-by-4.0"}
    lic = {"cc-by-4.0": {"reservable": 1, "commercial_ok": 1, "attribution_required": 1}}
    return G.build_record(sid, src, lic, roll, {})


def test_cutoffs_are_the_end_of_the_period_containing_the_build_date(G):
    assert G.forward_cutoffs(BUILD) == {"fwd_a": "2026-12-31", "fwd_s": "2026-12-31", "fwd_q": "2026-12-31",
                                        "fwd_m": "2026-11-30", "fwd_w": "2026-11-22", "fwd_d": "2026-11-18"}
    assert G.forward_cutoffs("2027-02-10")["fwd_q"] == "2027-03-31"
    assert G.forward_cutoffs("2027-02-10")["fwd_s"] == "2027-06-30"
    assert G.forward_cutoffs("2027-02-10")["fwd_m"] == "2027-02-28"


def test_current_period_stamps_are_not_forward(G):
    # an annual (and an unknown-frequency) value stamped 12-31 of the build year is the CURRENT period
    con = _db([("cur:a", "A", "2000-12-31", "2026-12-31"), ("cur:n", None, "2000-12-31", "2026-12-31"),
               ("cur:m", "M", "2000-01-31", "2026-11-30"), ("cur:q", "Q", "2000-03-31", "2026-12-31")])
    roll = _roll(G, con)["cur"]
    assert roll["n_forward"] == 0 and roll["max_forward"] is None
    rec = _rec(G, "cur", roll)
    assert rec["forward_year"] is None and G.forward_sentence(rec) is None


def test_a_monthly_series_one_month_ahead_is_forward(G):
    con = _db([("mon:x", "M", "2000-01-31", "2026-12-31"), ("mon:y", "M", "2000-01-31", "2026-10-31")])
    roll = _roll(G, con)["mon"]
    assert (roll["n_forward"], roll["max_forward"]) == (1, "2026-12-31")
    rec = _rec(G, "mon", roll)
    assert G.forward_sentence(rec) == "Some series extend to 2026 (forward-dated values)."


def test_a_future_source_gets_the_neutral_wording_on_the_page_and_in_the_meta(G):
    con = _db([("fut:life", None, "1876-12-31", "2150-12-31"), ("fut:pop", "A", "1801-12-31", "2025-12-31")])
    rec = _rec(G, "fut", _roll(G, con)["fut"])
    assert rec["cov_end"] == "2150-12-31", "the bound is unchanged (= the API's data_through)"
    sentence = "Some series extend to 2150 (forward-dated values)."
    assert G.forward_sentence(rec) == sentence
    page = G.render_dataset_page(rec)
    assert sentence in page and "projection" not in page.lower()
    assert '"temporalCoverage": "1801-12-31/2150-12-31"' in page, "schema.org keeps the full range"
    meta = G.build_meta_desc(rec)
    assert "1801–2150 (incl. forward-dated values)" in meta and "projection" not in meta.lower()


def test_projections_only_for_a_curated_source(G, monkeypatch):
    con = _db([("prj:a", None, "1950-12-31", "2100-12-31")])
    rec = _rec(G, "prj", _roll(G, con)["prj"])
    assert G.SOURCE_FORWARD_NOTES == {}, "this PR adds no curated entry"
    assert "projections" not in G.forward_sentence(rec)
    monkeypatch.setitem(G.SOURCE_FORWARD_NOTES, "prj", "projections")
    assert G.forward_sentence(rec) == "Some series extend to 2100 (projections)."
    assert "(incl. projections)" in G.build_meta_desc(rec)


def test_a_past_only_source_renders_exactly_as_before(G, monkeypatch):
    con = _db([("old:a", "A", "1990-12-31", "2025-12-31"), ("old:b", "M", "1990-01-31", "2026-10-31")])
    rec = _rec(G, "old", _roll(G, con)["old"])
    assert rec["forward_year"] is None and G.forward_sentence(rec) is None
    page, meta = G.render_dataset_page(rec), G.build_meta_desc(rec)
    assert "Forward-dated" not in page and "forward-dated" not in meta
    # byte-identical to a render with the feature switched off entirely
    monkeypatch.setattr(G, "forward_sentence", lambda r: None)
    assert G.render_dataset_page(rec) == page and G.build_meta_desc(rec) == meta


def test_known_parse_damage_gets_no_qualifier(G):
    con = _db([("ksh_stadat:x", None, "2100-12-31", "2100-12-31")])
    rec = _rec(G, "ksh_stadat", _roll(G, con)["ksh_stadat"])
    assert rec["forward_year"] == 2100 and G.forward_sentence(rec) is None


@pytest.mark.parametrize("sid", ["ksh_stadat", "statfin", "imf_er_direct"])
def test_every_listed_defect_source_gets_no_qualifier_on_page_or_meta(G, sid):
    assert sid in G.FORWARD_DATE_DEFECTS and G.FORWARD_DATE_DEFECTS[sid], "listed, with its evidence"
    con = _db([(f"{sid}:x", "M", "1957-01-31", "2027-03-31"), (f"{sid}:y", None, "1950-12-31", "2020-12-31")])
    rec = _rec(G, sid, _roll(G, con)[sid])
    assert rec["forward_year"] == 2027, "the data IS forward; only the wording is withheld"
    assert G.forward_sentence(rec) is None
    assert "Forward-dated" not in G.render_dataset_page(rec) and "forward-dated" not in G.build_meta_desc(rec)


def test_sentinels_are_not_forward_years(G):
    con = _db([("sen:a", None, "1950-12-31", "9999-12-31"), ("sen:b", None, "1950-12-31", "2020-12-31")])
    roll = _roll(G, con)["sen"]
    assert roll["n_forward"] == 0 and G.forward_sentence(_rec(G, "sen", roll)) is None


def test_withheld_series_leave_the_forward_count(G):
    con = _db([("wh:held", None, "1950-12-31", "2090-12-31"), ("wh:kept", None, "1950-12-31", "2040-12-31"),
               ("wh:past", None, "1950-12-31", "2020-12-31")])
    roll = _roll(G, con)
    assert roll["wh"]["n_forward"] == 2
    con.row_factory = None
    G.subtract_withheld(con, roll, {"wh": ["wh:held"]})
    assert (roll["wh"]["n_forward"], roll["wh"]["max_forward"]) == (1, "2040-12-31")


def test_a_forward_sidecar_bound_cannot_bypass_the_qualifier(G):
    con = _db([("sc:a", None, "1950-12-31", "2020-12-31")])
    src = {"name": "Test Statistical Office (TSO)", "license_id": "cc-by-4.0"}
    lic = {"cc-by-4.0": {"reservable": 1}}
    rec = G.build_record("sc", src, lic, _roll(G, con)["sc"], {"last_obs": "2030-06-30"})
    assert rec["cov_end"] == "2030-06-30" and rec["forward_year"] == 2030
    rec2 = G.build_record("sc", src, lic, _roll(G, con)["sc"], {"last_obs": "2026-11-01"})
    assert rec2["forward_year"] is None
