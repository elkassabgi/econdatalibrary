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


FINAL = "This is the final copy, and it is not updated."

# The entries whose own finding shows "<publisher> has discontinued this dataset." overstates, and the sentence
# that replaces it (2026-10-20). unesco_film: 1 of 76 codes still published. unesco_clte: 21 of 408 survive under
# new CE.RA.* keys. fao_ae / fao_af: the live AE / AF datasets were re-dimensioned, the old composite ids have no
# 1:1 successor. unctad_tabbapotta: the same indicator is published as a recomputed modern edition under M49 keys.
# unesco_cltt: 0 of its 34 codes published while the CULTURE theme is alive (the file's own warning: the theme being
# alive is not our indicators being alive), so only the series-level claim is measured. fao_ew: the probe says
# AQUASTAT "left FAOSTAT's bulk distribution", which was measured inside FAOSTAT only.
LABELS = {
    "unesco_film": "UNESCO Institute for Statistics no longer publishes these series.",
    "unesco_clte": "UNESCO Institute for Statistics no longer publishes these series.",
    "unesco_cltt": "UNESCO Institute for Statistics no longer publishes these series.",
    "fao_ae": "FAO (FAOSTAT) no longer publishes these series.",
    "fao_af": "FAO (FAOSTAT) no longer publishes these series.",
    "fao_ew": "FAO (FAOSTAT) no longer publishes these series.",
    "unctad_tabbapotta": "UNCTAD no longer publishes these series.",
}
DEFAULT_CADENCE = "none &mdash; <em>discontinued by the publisher</em>"
LABEL_CADENCE = "none &mdash; <em>archival copy</em>"


def _first_sentence(entry):
    """What the page must say first, from the entry alone (html-escaped as the page carries it)."""
    import html
    if entry.get("label"):
        return html.escape(entry["label"].strip())
    return html.escape(str(entry["publisher"]).strip()) + " has discontinued this dataset."


def test_every_discontinued_source_reads_archival(G):
    """The REAL file, every entry, with the worst-case inputs: a cadence in the catalogue, a recorded success
    in state, and (for trademerchgr) the same shape the page builder would see after the registry entry went."""
    disc = G.load_discontinued()
    assert len(disc) >= 53 and "unctad_trademerchgr" in disc and "unctad_mttgra" in disc
    state = {"last_success": {s: "2026-10-01T22:27:34+00:00" for s in disc}, "attempted": set(disc)}
    for sid in disc:
        t = _text(G.refresh_rows(sid, "weekly", wiring={sid: False}, meta={}, state=state))
        assert ARCHIVAL in t and _first_sentence(disc[sid]) in t and FINAL in t, (sid, t)
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


def test_the_wording_overrides_are_exactly_these_and_the_default_holds_for_the_rest(G):
    """The REAL file: exactly the LABELS entries carry a `label`, with exactly that text, and every other entry
    still prints the default sentence. Adding, dropping or rewording a label fails here."""
    disc = G.load_discontinued()
    got = {sid: e["label"] for sid, e in disc.items() if "label" in e}
    assert got == LABELS
    for sid, e in disc.items():
        t = _text(G.discontinued_rows(e))
        if sid in LABELS:
            assert f"&mdash; {LABELS[sid]} {FINAL}" in t, (sid, t)
            assert f"Update cadence: {LABEL_CADENCE}" in t, (sid, t)
            assert "has discontinued this dataset" not in t and "discontinued by the publisher" not in t, (sid, t)
        else:
            assert f"&mdash; {e['publisher']} has discontinued this dataset. {FINAL}" in t, (sid, t)
            assert f"Update cadence: {DEFAULT_CADENCE}" in t, (sid, t)


def test_a_label_replaces_only_the_first_sentence(G):
    """The override cannot drop the archival claim, is escaped, and an entry without one keeps the default
    (negative control: the same publisher prints the default sentence when `label` is absent)."""
    t = _text(G.discontinued_rows({"publisher": "UNCTAD", "label": "UNCTAD no <i>longer</i> publishes these series."}))
    assert ARCHIVAL in t and t.endswith(FINAL) and "<i>" not in t
    assert "UNCTAD no &lt;i&gt;longer&lt;/i&gt; publishes these series. " + FINAL in t
    assert "has discontinued" not in t and "discontinued by the publisher" not in t and LABEL_CADENCE in t
    t0 = _text(G.discontinued_rows({"publisher": "UNCTAD"}))
    assert "UNCTAD has discontinued this dataset. " + FINAL in t0 and "no longer publishes" not in t0
    assert DEFAULT_CADENCE in t0 and LABEL_CADENCE not in t0


@pytest.mark.parametrize("bad", ["", "   ", "null", "3", "[a, b]", "{x: 1}", "P no longer publishes these series"])
def test_a_label_that_is_not_a_sentence_stops_the_build(G, tmp_path, bad):
    f = tmp_path / "discontinued.yaml"
    f.write_text(f"sources:\n  - source_id: x\n    publisher: P\n    label: {bad}\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="label"):
        G.load_discontinued(str(f))
    f.write_text("sources:\n  - source_id: x\n    publisher: P\n    label: P no longer publishes these series.\n",
                 encoding="utf-8")
    assert G.load_discontinued(str(f))["x"]["label"] == "P no longer publishes these series."


def test_the_status_board_shows_discontinued_as_archival_not_attention():
    """AR-355. core/sync_state_d1.py publishes a discontinued source's recorded unit row with status 'discontinued';
    status.html gave every status it did not know the red 'bad' class ("failed ... until fixed"). It must be a
    neutral archival dot, decided before the catch-all, counted on its own, and explained in the legend."""
    s = open(os.path.join(ROOT, "catalog", "site", "status.html"), encoding="utf-8").read()
    rule = "else if(st==='discontinued'){cls='archival'}"
    assert ".archival{background:" in s and '<span class="dot archival"></span> archival' in s, "style and legend"
    assert rule in s and s.index(rule) < s.index("else{cls='bad'}"), "decided before the red catch-all"
    assert "else if(cls==='archival')archival++;else bad++;" in s, "counted on its own, never as attention"
    assert "archival+' archival · '" in s, "the summary line names the archival count"


def test_the_regenerated_page_carries_the_label():
    """catalog/site/unctad_trademerchgr.html was regenerated with `gen_site.py --only unctad_trademerchgr` in the
    change that froze it. Pin what a visitor reads there."""
    page = open(os.path.join(ROOT, "catalog", "site", "unctad_trademerchgr.html"), encoding="utf-8").read()
    assert "UNCTAD has discontinued this dataset. This is the final copy, and it is not updated." in page
    assert "discontinued by the publisher" in page
    low = page.lower()
    assert "not yet wired" not in low and "not automated at the moment" not in low
    assert "on the daily update run" not in low


def test_every_discontinued_page_is_regenerated_with_its_sentence(G):
    """The other 52 pages were regenerated (`gen_site.py --only <id>`, one run each) after the label landed (R1639:
    they still said "not yet wired - the data here is the verified initial load"). Every page of every entry must
    carry its own sentence and none of the old promises; this fails if a page is skipped or left stale."""
    disc = G.load_discontinued()
    for sid, e in disc.items():
        page = open(os.path.join(ROOT, "catalog", "site", f"{sid}.html"), encoding="utf-8").read()
        assert f"{_first_sentence(e)} {FINAL}" in page, sid
        low = page.lower()
        assert not any(p in low for p in ("not yet wired", "not automated at the moment",
                                          "on the daily update run", "verified initial load")), sid
        if sid in LABELS:
            assert f"<dt>Update cadence</dt><dd>{LABEL_CADENCE}</dd>" in page, sid
            assert "has discontinued this dataset" not in page and "discontinued by the publisher" not in page, sid
        else:
            assert f"<dt>Update cadence</dt><dd>{DEFAULT_CADENCE}</dd>" in page, sid
