"""The two share cards (catalog/site/assets/og-image.png, portal/assets/og-image.png) and their SVG sources.

A card is a still picture. The cards of 2026-08-21 showed headline totals and a source count; the sites no
longer state those values, and a picture cannot carry the "being recalculated" notice that the pages show
beside the totals. So: the card sources hold NO figure, and this test fails when one is put back.

What a test of a picture can and cannot do: it reads the SVG sources (the words) and the PNG headers (the
size). It does not read the pixels: that the PNG is the rendering of the SVG is checked by a person who looks.
"""
import os
import re
import struct
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARDS_DIR = os.path.join(ROOT, "tools", "social_cards")
sys.path.insert(0, CARDS_DIR)

import make_cards  # noqa: E402

TEXT = re.compile(r"<text\b[^>]*>(.*?)</text>", re.S)


def _words(svg):
    """The visible words of an SVG: the content of its <text> elements, tags and entities removed."""
    out = []
    for inner in TEXT.findall(svg):
        t = re.sub(r"<[^>]+>", "", inner)
        out.append(t.replace("&amp;", "&").replace("&#183;", "·").strip())
    return out


def test_the_committed_sources_are_what_the_script_writes():
    for name, svg in make_cards.CARDS.items():
        with open(os.path.join(CARDS_DIR, name), encoding="utf-8", newline="") as fh:
            # a Windows checkout can hold CRLF; the words are what is compared
            assert fh.read().replace("\r\n", "\n") == svg, name + " differs from make_cards.py: run the script, render, commit"


def test_the_cards_say_these_words_and_no_others():
    assert _words(make_cards.CARDS["econ.svg"]) == [
        "Econ Data Library", "Free economic & financial time series data", "LICENSE", "PROVENANCE", "CITATION",
        "ON EVERY SERIES", "econdatalibrary.com · Ahmed Elkassabgi · University of Central Arkansas"]
    assert _words(make_cards.CARDS["portal.svg"]) == [
        "Elkassabgi Data", "One account. Every library.", "HF", "ECON", "IP",
        "1-MINUTE U.S. EQUITIES", "ECONOMIC & FINANCIAL DATA", "PATENTS & INNOVATION",
        "elkassabgidata.com · Ahmed Elkassabgi · University of Central Arkansas"]


def test_no_card_holds_a_figure():
    """No count, no total, no "+": the one digit allowed is the 1 of "1-MINUTE"."""
    figure = re.compile(r"\d")
    for name, svg in make_cards.CARDS.items():
        for t in _words(svg):
            rest = t.replace("1-MINUTE", "")
            assert not figure.search(rest) and "+" not in rest, (name, t)
    # the instrument can fail: the words of the old cards are caught
    for old in ("79.7B+", "7.7B+", "309", "3"):
        assert figure.search(old)


def test_the_econ_card_uses_the_home_pages_own_sentence():
    home = open(os.path.join(ROOT, "catalog", "site", "index.html"), encoding="utf-8").read()
    assert "Every series carries its license, provenance, and producer-first citation." in " ".join(home.split())


def test_the_portal_card_names_the_libraries_the_portal_names():
    page = open(os.path.join(ROOT, "portal", "index.html"), encoding="utf-8").read()
    for name in ("HF Data Library", "Econ Data Library", "IP Data Library"):
        assert name in page, name


def test_both_pngs_are_1200_by_630():
    for rel in ("catalog/site/assets/og-image.png", "portal/assets/og-image.png"):
        with open(os.path.join(ROOT, *rel.split("/")), "rb") as fh:
            head = fh.read(24)
        assert head[:8] == b"\x89PNG\r\n\x1a\n" and head[12:16] == b"IHDR", rel
        assert struct.unpack(">II", head[16:24]) == (1200, 630), rel
