"""The two share cards (catalog/site/assets/og-image.png, portal/assets/og-image.png) and their SVG sources.

A card is a still picture. The cards of 2026-08-21 showed headline totals and a source count; the sites no
longer state those values, and a picture cannot carry the "being recalculated" notice that the econ home page
shows beside its totals. So: the card sources hold NO figure, and this test fails when one is put back.

What a test of a picture can and cannot do: it reads the SVG sources (the words) and the PNG headers (the
size), and it PINS the bytes of each PNG beside the text of its SVG. It does not read the pixels. So a changed
SVG without a new render fails, and a changed PNG without a new pin fails; that the pinned PNG is the true
rendering of the pinned SVG was checked by looking at both pictures (review AR-307). Whoever changes a pin
renders, looks at the picture, and then pins.
"""
import hashlib
import os
import re
import struct
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARDS_DIR = os.path.join(ROOT, "tools", "social_cards")
sys.path.insert(0, CARDS_DIR)

import make_cards  # noqa: E402

TEXT = re.compile(r"<text\b[^>]*>(.*?)</text>", re.S)

# name of the source -> (sha256 of its text with LF line ends, the served PNG, sha256 of the PNG's bytes)
PINS = {
    "econ.svg": ("b2195ab05b534c1001f33b862fcfc4d7ce29cf6460772e01dee483939f05eb0f",
                 "catalog/site/assets/og-image.png",
                 "05a2fb04913b8a7b7c0dc648a2d0088317ecd738ff97a4dc8daa42e401f5653e"),
    "portal.svg": ("b77396ada7a32aeb04d26efee336f91b208418b6bd58e26f1a0852ca22fbaca5",
                   "portal/assets/og-image.png",
                   "7bc9ae0f525711ea86a557aa176b847dc2569d03e2ee3427319c609a5547232f"),
}
MAGNITUDE = re.compile(r"\b(hundreds?|thousands?|millions?|billions?|trillions?|dozens?)\b", re.I)


def _words(svg):
    """The visible words of an SVG: the content of its <text> elements, tags and entities removed."""
    out = []
    for inner in TEXT.findall(svg):
        t = re.sub(r"<[^>]+>", "", inner)
        out.append(t.replace("&amp;", "&").replace("&#183;", "·").strip())
    return out


def _figures(svg):
    """The words of a card that hold a figure: a digit (the one allowed is the 1 of "1-MINUTE"), a "+", or a
    magnitude in words."""
    hits = []
    for t in _words(svg):
        rest = t.replace("1-MINUTE", "")
        if re.search(r"\d", rest) or "+" in rest or MAGNITUDE.search(rest):
            hits.append(t)
    return hits


def test_the_committed_sources_are_what_the_script_writes():
    for name, svg in make_cards.CARDS.items():
        with open(os.path.join(CARDS_DIR, name), encoding="utf-8", newline="") as fh:
            # a Windows checkout can hold CRLF; the words are what is compared
            assert fh.read().replace("\r\n", "\n") == svg, name + " differs from make_cards.py: run the script, render, commit"


def test_each_png_is_pinned_beside_its_source():
    assert set(PINS) == set(make_cards.CARDS)
    for name, (svg_sha, png_rel, png_sha) in PINS.items():
        assert hashlib.sha256(make_cards.CARDS[name].encode("utf-8")).hexdigest() == svg_sha, (
            name + " changed: render it again, LOOK at the picture, then pin the source and the PNG")
        with open(os.path.join(ROOT, *png_rel.split("/")), "rb") as fh:
            assert hashlib.sha256(fh.read()).hexdigest() == png_sha, (
                png_rel + " is not the picture that was looked at for " + name)
    assert len({p[2] for p in PINS.values()}) == len(PINS), "two cards with one picture"


def test_the_cards_say_these_words_and_no_others():
    assert _words(make_cards.CARDS["econ.svg"]) == [
        "Econ Data Library", "Free economic & financial time series data", "LICENSE", "PROVENANCE", "CITATION",
        "ON EVERY SERIES", "econdatalibrary.com · Ahmed Elkassabgi · University of Central Arkansas"]
    assert _words(make_cards.CARDS["portal.svg"]) == [
        "Elkassabgi Data", "One account. Every library.", "HF", "ECON", "IP",
        "1-MINUTE U.S. EQUITIES", "ECONOMIC & FINANCIAL DATA", "PATENTS & INNOVATION",
        "elkassabgidata.com · Ahmed Elkassabgi · University of Central Arkansas"]


def test_no_card_holds_a_figure():
    for name, svg in make_cards.CARDS.items():
        assert _figures(svg) == [], name


def test_the_figure_check_refuses_the_old_cards_and_a_figure_in_words():
    """The instrument can fail: cards built with the old tiles go through the same reader."""
    byline = "x &#183; y"
    old_econ = make_cards.card("Econ Data", "Library", "tagline", ("79.7B+", "7.7B+", "309"),
                               ("OBSERVATIONS", "SERIES", "SOURCES"), None, byline)
    assert _figures(old_econ) == ["79.7B+", "7.7B+", "309"]
    old_portal = make_cards.card("Elkassabgi", "Data", "tagline", ("79.7B+", "7.7B+", "3"),
                                 ("OBSERVATIONS", "SERIES", "DATA LIBRARIES"), None, byline)
    assert _figures(old_portal) == ["79.7B+", "7.7B+", "3"]
    in_words = make_cards.card("a", "b", "c", ("LICENSE", "PROVENANCE", "CITATION"), None, "ON BILLIONS OF SERIES", byline)
    assert _figures(in_words) == ["ON BILLIONS OF SERIES"]
    in_label = make_cards.card("a", "b", "c", ("HF", "ECON", "IP"), ("1-MINUTE 304M U.S. EQUITIES", "x", "y"), None, byline)
    assert _figures(in_label) == ["1-MINUTE 304M U.S. EQUITIES"]


def test_the_econ_card_uses_the_home_pages_own_sentence():
    home = open(os.path.join(ROOT, "catalog", "site", "index.html"), encoding="utf-8").read()
    assert "Every series carries its license, provenance, and producer-first citation." in " ".join(home.split())


def test_the_portal_card_names_exactly_the_libraries_the_portal_lists():
    page = open(os.path.join(ROOT, "portal", "index.html"), encoding="utf-8").read()
    listed = re.findall(r'<div class="lib">.*?<h3>(.*?)</h3>', page, re.S)
    assert listed == ["HF Data Library", "Econ Data Library", "IP Data Library"], listed
    # a card with a second class, with single quotes or without a heading is still a card
    assert len(re.findall(r"""class=["']lib\b""", page)) == 3
    tiles = _words(make_cards.CARDS["portal.svg"])[2:5]
    assert tiles == [n.split(" ")[0].upper() for n in listed]


def test_both_pngs_are_1200_by_630():
    for rel in ("catalog/site/assets/og-image.png", "portal/assets/og-image.png"):
        with open(os.path.join(ROOT, *rel.split("/")), "rb") as fh:
            head = fh.read(24)
        assert head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR", rel
        assert struct.unpack(">II", head[16:24]) == (1200, 630), rel
