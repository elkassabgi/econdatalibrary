"""A publisher-discontinued source's page says it is archival - and nothing that promises a refresh (2026-10-20).

WHY. Every decision in updater/discontinued.yaml cites the owner's rule of 2026-08-09: archival sources stay listed
"labelled so nobody expects fresh data". catalog/gen_site.py never read that file, so the label did not exist:
catalog/site/unctad_mttgra.html said "Automated refresh: not yet wired - the data here is the verified initial
load" (a fetcher the file says cannot exist), and unctad_trademerchgr, once its registry entry was removed, would
have read "weekly (not automated at the moment) ... last fully completed automated run 2026-10-01". refresh_rows
now checks discontinued.yaml FIRST. These tests can fail: the negative control shows the old untrue sentence comes
back without the file, and the committed page of the regenerated source is pinned.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
sys.path.insert(0, ROOT)

ARCHIVAL = "<strong>archival</strong>"
PROMISES = ("not yet wired", "<strong>live</strong>", "not automated at the moment", "update run",
            "update pass", "paused", "update lane")


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_discontinued_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _text(rows):
    return " ".join(f"{k}: {v}" for k, v in rows)


def test_every_discontinued_source_reads_archival(G):
    """The REAL file, every entry, with the worst-case inputs: a cadence in the catalogue, a recorded success
    in state, and (for trademerchgr) the same shape the page builder would see after the registry entry went."""
    disc = G.load_discontinued()
    assert len(disc) >= 53 and "unctad_trademerchgr" in disc and "unctad_mttgra" in disc
    state = {"last_success": {s: "2026-10-01T22:27:34+00:00" for s in disc}, "attempted": set(disc)}
    for sid in disc:
        t = _text(G.refresh_rows(sid, "weekly", wiring={sid: False}, meta={}, state=state))
        assert ARCHIVAL in t and "has discontinued this dataset" in t and "not updated" in t, (sid, t)
        assert not any(p in t for p in PROMISES), (sid, t)
        assert "weekly" not in t, f"{sid}: the catalogue's cadence must not be shown for a frozen source"


def test_the_label_wins_over_paused_and_wired(G):
    """Precedence: even a stale registry claim (paused, or still wired) cannot put a refresh promise back."""
    disc = {"x_src": {"source_id": "x_src", "publisher": "UNCTAD"}}
    for wiring, meta in (({"x_src": True}, {}), ({"x_src": False}, {"x_src": {"paused": "on hold"}})):
        t = _text(G.refresh_rows("x_src", "weekly", wiring=wiring, meta=meta, state=None, disc=disc))
        assert ARCHIVAL in t and "UNCTAD has discontinued" in t, t
        assert not any(p in t for p in PROMISES), t


def test_without_the_file_the_untrue_sentences_come_back(G):
    """Negative control: with no discontinued.yaml entry the SAME inputs print the sentences this change removes,
    so the test above passes because of the rule and not because the inputs were harmless."""
    t_ran = _text(G.refresh_rows("x_src", "weekly", wiring={"x_src": False}, meta={},
                                 state={"last_success": {"x_src": "2026-10-01"}, "attempted": {"x_src"}}, disc={}))
    assert "not automated at the moment" in t_ran and ARCHIVAL not in t_ran
    t_never = _text(G.refresh_rows("x_src", "annual", wiring={"x_src": False}, meta={},
                                   state={"last_success": {}, "attempted": set()}, disc={}))
    assert "not yet wired" in t_never and ARCHIVAL not in t_never


def test_an_absent_file_labels_nothing_and_a_broken_one_stops_the_build(G, tmp_path):
    assert G.load_discontinued(str(tmp_path / "none.yaml")) == {}
    bad = tmp_path / "discontinued.yaml"
    bad.write_text("sources:\n  - source_id: x\n   bad: [\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="unreadable"):
        G.load_discontinued(str(bad))
    bad.write_text("sources: []\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="unreadable"):
        G.load_discontinued(str(bad))


def test_the_publisher_name_is_escaped(G):
    t = _text(G.discontinued_rows({"publisher": "A <b>&</b> B"}))
    assert "<b>" not in t and "A &lt;b&gt;&amp;&lt;/b&gt; B has discontinued" in t


def test_the_regenerated_page_carries_the_label():
    """catalog/site/unctad_trademerchgr.html was regenerated with `gen_site.py --only unctad_trademerchgr` in the
    change that froze it. Pin what a visitor reads there."""
    page = open(os.path.join(ROOT, "catalog", "site", "unctad_trademerchgr.html"), encoding="utf-8").read()
    assert "UNCTAD has discontinued this dataset. This is the final copy, and it is not updated." in page
    assert "discontinued by the publisher" in page
    low = page.lower()
    assert "not yet wired" not in low and "not automated at the moment" not in low
    assert "on the daily update run" not in low
