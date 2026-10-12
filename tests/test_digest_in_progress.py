"""The daily report has two lists: "Needs attention" and "In progress (normal)" (owner's order, 2026-10-11).

WHY. On 2026-10-10 (run 38086608492) ten sources stood under "Needs attention" only because they are too
large for one run ("N sub-unit(s) attempted, none failed; M deferred by budget and taken next tick"), beside
fifteen rows that were real trouble. The report read as a failure every morning.

WHAT MUST NEVER HAPPEN. A row with real trouble in the calm list. The rule is a pattern over error text, so it
is narrow: partial, not late, the budget sentence with "none failed", and no other word of trouble. Each
condition is pinned alone below, and the texts are the ones of that run (as the report printed them).
No row is dropped: the two lists together are the old list.
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import updater.send_digest as sd  # noqa: E402

# (source, status, text as printed on 2026-10-10, in progress?)
REAL_ROWS = [
    ("abs", "partial", "213 sub-unit(s) attempted, none failed; 817 deferred by budget and taken next tick [C21_G01_SA2 deferred (budg", True),
    ("ecb", "partial", "312 sub-unit(s) attempted, none failed; 228 deferred by budget and taken next tick [ECB__CSEC__M__I8__2025.par", True),
    ("ilostat", "partial", "607 sub-unit(s) attempted, none failed; 1019 deferred by budget and taken next tick [EMP_NIFL_SEX_OCU_EST_NB_A", True),
    ("ksh_stadat", "partial", "103 sub-unit(s) attempted, none failed; 387 deferred by budget and taken next tick [gdp0050 (per-run cap 400),", True),
    ("stat_slovenia", "partial", "64 sub-unit(s) attempted, none failed; 75 deferred by budget and taken next tick [264: budget 40 min spent, gr", True),
    ("unsdg", "partial", "100 sub-unit(s) attempted, none failed; 603 deferred by budget and taken next tick", True),
    ("cso", "partial", "44/48 sub-unit(s) transient-failed; will retry [EAACC21: unparsed, EAACC22: unparsed, CPNI20: unparsed, CPNI21", False),
    ("dst", "partial", "2/25 sub-unit(s) transient-failed; will retry [VNASFK, VNKSFK]; csv coherence unmet: fetcher reported no serie", False),
    ("ssb", "partial", "198/512 sub-unit(s) transient-failed; will retry [InnGodsTr: metadata — ssb meta InnGodsTr: HTTP 429, InnLandG", False),
    ("unctad_tradefoodproccatcatrca", "partial", "unctad_tradefoodproccatcatrca: 1/1 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body (schema/", False),
    ("unctad_trademerchgr", "transient_fail", "detect:UNEXPECTED:HTTPError('400 Client Error: Bad Request for url: https://unctadstat-api.unctad.org/api/repo", False),
    ("eurostat", "partial", "selected>cap: fetched 400 of a larger changed set; remainder re-runs next tick (+16941179 rows); csv_derive fa", False),
    ("hagstofa", "transient_fail", "hagstofa/_all exceeded its 45-minute hard limit and was interrupted; existing data untouched, re-queued for th", False),
    ("noaa", "partial", "+552194304 new rows; csv coherence unmet: fetcher reported no series_cursors; store holds 552194304 rows, merg", False),
    ("unctad_criticalmineralstradebypart", "partial", "+109956508 new rows; csv coherence unmet: 8589597 changed series_keys have no catalog mapping for unctad_criti", False),
    ("unctad_tradefoodprocbycat", "partial", "1/1 sub-unit(s) transient-failed; will retry", False),
]
SENTENCE = "12 sub-unit(s) attempted, none failed; 30 deferred by budget and taken next tick"


@pytest.mark.parametrize("source,status,text,want", REAL_ROWS, ids=[r[0] for r in REAL_ROWS])
def test_the_rows_of_the_real_run_are_sorted_as_a_person_would_sort_them(source, status, text, want):
    assert sd.is_in_progress(status, text, "") is want


@pytest.mark.parametrize("status,text,late", [
    ("transient_fail", SENTENCE, ""),                                   # not partial
    ("error", SENTENCE, ""),
    ("ok", SENTENCE, ""),
    ("partial", SENTENCE, " LATE"),                                     # late for its cadence
    ("partial", SENTENCE, " LATE(local)"),
    ("partial", "12 sub-unit(s) attempted, 2 failed; 30 deferred by budget and taken next tick", ""),   # something failed
    ("partial", "30 deferred by budget and taken next tick", ""),       # no "none failed"
    ("partial", SENTENCE + "; csv coherence unmet: fetcher reported no series_cursors", ""),
    ("partial", SENTENCE + " [A, B]; refusing shrink", ""),
    ("partial", SENTENCE + " [A: HTTP 429]; then UnitTimeout", ""),
    ("partial", "detect:UNEXPECTED:ValueError; " + SENTENCE, ""),
    ("partial", SENTENCE + " [A (x[1]) b]; migration has not completed", ""),   # trouble after a bracket inside the list
    ("partial", "", ""),
    ("partial", None, ""),
])
def test_each_condition_alone_keeps_a_row_under_needs_attention(status, text, late):
    assert sd.is_in_progress(status, text, late) is False


@pytest.mark.parametrize("text", [
    SENTENCE,
    SENTENCE + " [T1 deferred (budget), T2 deferred (budget)]",
    SENTENCE + " [T1 (per-run cap 400), T2: budget 40 min spent, gr",     # the list is cut by the report's own limit
    "1,106 sub-unit(s) attempted, none failed; 1 deferred by budget and taken next tick [rahvaloendus: 64 table(s) a",
])
def test_the_plain_budget_sentence_is_in_progress(text):
    assert sd.is_in_progress("partial", text, "") is True


# ---------------------------------------------------------------- the whole report, both bodies
def _state(tmp_path, rows):
    p = tmp_path / "state.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, last_success_utc TEXT, "
                "last_error TEXT, last_attempt_utc TEXT)")
    con.executemany("INSERT INTO unit_state VALUES (?,?,?,?,?,?)", rows)
    con.commit()
    con.close()
    return str(p)


@pytest.fixture
def report(tmp_path, monkeypatch, capsys):
    import datetime as dt
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = [
        ("src_ok", "ok", "2026-10-09", now, None, now),
        ("src_big", "partial", "2026-12-31", now, SENTENCE + " [T1 deferred (budget)]", now),
        ("src_big2", "partial", "2026-12-31", now, SENTENCE, now),
        ("src_big_late", "partial", "2026-12-31", old, SENTENCE, old),            # the sentence, and late: attention
        ("src_trouble", "partial", "2026-01-01", now, "3/9 sub-unit(s) transient-failed; will retry", now),
        ("src_down", "transient_fail", None, now, "detect:UNEXPECTED:HTTPError('400')", now),
        ("src_broken", "error", None, now, "Traceback", now),
    ]
    monkeypatch.setattr(sd, "STATE", _state(tmp_path, rows))
    from updater import registry
    monkeypatch.setattr(registry, "load", lambda: {"sources": [
        {"source_id": r[0], "cadence": "daily", "live": True} for r in rows]})
    sent = {}
    import core.status_mail as sm

    def fake_send(subject, body, html_doc, **kw):
        sent.update(subject=subject, body=body, html=html_doc)
        return "sent"

    monkeypatch.setattr(sm, "send_status_mail", fake_send)
    monkeypatch.setenv("RUN_STATUS", "failure")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    sd.main()
    capsys.readouterr()
    assert sent, "the report was not handed to the mail module"
    return sent


def test_the_subject_counts_the_two_lists(report):
    assert report["subject"] == ("econdatalibrary daily: ATTENTION — run=failure, 1 failed, "
                                 "3 need attention, 2 in progress")


def test_the_html_body_has_both_lists_and_no_row_twice_or_missing(report):
    html = report["html"]
    assert "In progress (normal) (2)" in html and "Needs attention" in html
    a, p = html.index("Needs attention</h3>"), html.index("In progress (normal) (2)")
    end = html.index("Current sources (")
    assert a < p < end, "the real problems come first, then the calm list, then the current sources"
    attention, progress = html[a:p], html[p:end]
    for sid in ("src_trouble", "src_down", "src_broken", "src_big_late"):
        assert f"<b>{sid}</b>" in attention and f"<b>{sid}</b>" not in progress, sid
    for sid in ("src_big", "src_big2"):
        assert f"<b>{sid}</b>" in progress and f"<b>{sid}</b>" not in attention, sid
    for sid in ("src_big", "src_big2", "src_big_late", "src_trouble", "src_down", "src_broken"):
        assert html.count(f"<b>{sid}</b>") == 1, sid + " must stand in exactly one list"
    for label in (">in progress<", ">need attention<", ">failed<", ">current<"):
        assert label in html, label


def test_the_text_body_has_both_lists(report):
    body = report["body"]
    assert "1 ok/no_change · 3 need attention · 2 in progress · 1 failed" in body
    head, _, calm = body.partition("in progress (normal)")
    assert calm, "the calm list is missing from the text body"
    calm = calm.split("src_ok")[0]
    for sid in ("src_trouble", "src_down", "src_broken", "src_big_late"):
        assert f"!! {sid} " in head and sid not in calm, sid
    for sid in ("src_big ", "src_big2 "):
        assert f".. {sid}" in calm and f"!! {sid}" not in head, sid


def test_a_report_without_such_rows_has_no_calm_list(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sd, "STATE", _state(tmp_path, [("src_ok", "ok", "2026-10-09", None, None, None)]))
    from updater import registry
    monkeypatch.setattr(registry, "load", lambda: {"sources": [{"source_id": "src_ok", "cadence": "daily", "live": True}]})
    sent = {}
    import core.status_mail as sm
    monkeypatch.setattr(sm, "send_status_mail", lambda subject, body, html_doc, **kw: sent.update(s=subject, b=body, h=html_doc) or "sent")
    monkeypatch.setenv("RUN_STATUS", "success")
    sd.main()
    capsys.readouterr()
    assert "In progress (normal)" not in sent["h"] and "in progress (normal)" not in sent["b"]
    assert sent["s"] == "econdatalibrary daily: OK — 1 sources current"
