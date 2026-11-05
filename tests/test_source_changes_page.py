"""catalog/source_changes.yaml -> source_changes.html: the public record of publisher changes.

Every rule of the file is planted with a breaking entry, so a rule that stops checking fails here."""
import os
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "catalog"))
sys.path.insert(0, ROOT)
import gen_site as G  # noqa: E402

GOOD = {"date": "2026-11-02", "sources": ["fed_board"], "status": "announced", "until": "2027-03-31",
        "change": "The Federal Reserve Board said it will stop publishing several H.15 rates.",
        "effect": "Those series stop updating here; their history stays downloadable.",
        "link": "https://www.federalreserve.gov/feeds/DataDownload.html#1"}


def _load(tmp_path, entries, denylisted=frozenset()):
    p = tmp_path / "sc.yaml"
    p.write_text(yaml.safe_dump({"version": 1, "entries": entries}), encoding="utf-8")
    return G.load_source_changes(str(p), source_ids={"fed_board", "bls"}, denylisted=set(denylisted))


def test_the_committed_file_loads():
    G.load_source_changes()                       # raises on a bad committed entry


def test_a_good_entry_renders_with_its_link_and_sources(tmp_path):
    got = _load(tmp_path, [GOOD, dict(GOOD, date="2026-01-05", sources=["bls"], status="handled", until=None)])
    assert [e["date"] for e in got] == ["2026-11-02", "2026-01-05"], "newest first"
    html = G.render_source_changes(got)
    assert "takes effect 2027-03-31" in html and 'href="fed_board.html"' in html
    assert GOOD["link"] in html and "Publisher&rsquo;s notice" in html


def test_empty_says_so_and_is_noindex_until_it_has_an_entry():
    html = G.render_source_changes([])
    assert "No publisher change is listed yet" in html
    assert G._NOINDEX_RE.search(html), "a thin page stays out of the index and the sitemap (AR-437 A1)"
    assert not G._NOINDEX_RE.search(G.render_source_changes([GOOD]))


@pytest.mark.parametrize("quoted", [
    "'" + " ".join(["w"] * 16) + "'",
    "‘" + " ".join(["w"] * 16) + "’",
    "«" + " ".join(["w"] * 16) + "»",
    "„" + " ".join(["w"] * 16) + "“",
    '"' + " ".join(["a"] * 10) + '" and "' + " ".join(["b"] * 10) + '"',
    # AR-437 r2: an apostrophe inside the quotation, or no closing mark, hid each of these
    "'" + " ".join(["w"] * 7) + " O'Brien " + " ".join(["w"] * 8) + "'",
    "'" + " ".join(["w"] * 7) + " publisher's " + " ".join(["w"] * 8) + "'",
    "‘" + " ".join(["w"] * 7) + " publisher’s " + " ".join(["w"] * 8) + "’",
    '"' + " ".join(["w"] * 16),
    "“" + " ".join(["w"] * 16),
])
def test_every_quote_style_and_the_total_count(tmp_path, quoted):
    """AR-437 A2: each of these was accepted with more than 15 quoted words."""
    with pytest.raises(SystemExit, match="quotes"):
        _load(tmp_path, [dict(GOOD, change=f"The publisher wrote {quoted} today.")])


def test_apostrophes_are_not_quotes(tmp_path):
    text = "The publisher's API and the bank's feeds moved; users' scripts and the agency's old URL fail."
    _load(tmp_path, [dict(GOOD, change=text)])
    _load(tmp_path, [dict(GOOD, change="O'Brien's team said the 'old endpoint' closes and the publisher’s "
                                       "‘new API’ replaces it.")])


@pytest.mark.parametrize("patch,why", [
    ({"link": "http://x.org/n"}, "https"),
    ({"sources": ["nope"]}, "not registry sources"),
    ({"status": "maybe"}, "status must be"),
    ({"date": "2 Nov 2026"}, "YYYY-MM-DD"),
    ({"until": "soon"}, "YYYY-MM-DD"),
    ({"effect": ""}, "`effect` is required"),
    ({"change": "x" * 401}, "over 400"),
    ({"change": 'They wrote "one two three four five six seven eight nine ten eleven twelve thirteen fourteen '
                'fifteen sixteen" today.'}, "quotes 16 words"),
])
def test_each_rule_stops_the_build(tmp_path, patch, why):
    with pytest.raises(SystemExit, match=why):
        _load(tmp_path, [dict(GOOD, **patch)])


def test_a_source_the_worker_refuses_is_not_publishable_and_not_named(tmp_path):
    with pytest.raises(SystemExit) as e:
        _load(tmp_path, [GOOD], denylisted={"fed_board"})
    assert "fed_board" not in str(e.value), "the refusal does not name the gated source"


def test_the_page_is_in_the_sitemap_list_and_linked_from_docs():
    assert ("source_changes.html", "weekly") in G.SITEMAP_EXTRA
    assert 'href="source_changes.html"' in G.render_docs()
