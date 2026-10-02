"""The stats page's "Institutions Represented" list is the FAMILY block, not a list of our own.

WHY THIS EXISTS. Until 2026-10-02 `catalog/gen_site.py::render_stats` carried its own ranking table
(INST_PRESTIGE), its own icon table (INST_DOMAINS) and its own sort over the institutions feed. hf
carried a second copy with different entries, and the feed itself was cleaned by a third list. The
owner found the three sites showing three different lists, with junk answers near the top.

The fix is one decision point - the hf API's registry (hfdatalibrary `api/src/institutions.js`) decides
which answers are shown, their names, order, featured cut and icon - and one page block,
`catalog/institutions_block.js`, that renders the feed as received. hf, econ and ip carry the SAME
BYTES of that block; each repository holds it to the same hash. These tests fail if:
  * the block here drifts from the family hash,
  * the generator or the served page stops carrying it verbatim,
  * a private list comes back,
  * the served page is no longer what the generator produces.
"""
from __future__ import annotations

import hashlib
import importlib.util
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG = os.path.join(ROOT, "catalog")
BLOCK = os.path.join(CATALOG, "institutions_block.js")
PAGE = os.path.join(CATALOG, "site", "stats.html")
GEN = os.path.join(CATALOG, "gen_site.py")

# The SAME constant stands in the hf and ip repositories. Change the block in all three together.
BLOCK_SHA256 = "9bc3f37c05122ec1af59585e5ebbc08e0857c019f13e15ff976e59b2de07f404"

PRIVATE_LISTS = ("INST_PRESTIGE", "INST_DOMAINS", "INST_ALIASES", "INST_JUNK", "instIcon(", "toggleInst(")


def _text(path: str) -> str:
    """File text with CRLF folded to LF: a Windows checkout must hash like the Linux runner."""
    with open(path, encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


def test_block_is_the_family_block():
    block = _text(BLOCK)
    assert hashlib.sha256(block.encode("utf-8")).hexdigest() == BLOCK_SHA256
    assert "\\" not in block, "a backslash would be eaten by the Python string the block is embedded in"
    assert all(ord(c) < 127 for c in block), "non-ASCII character in the block"
    assert '"""' not in block


def test_served_page_carries_the_block_verbatim_and_calls_it():
    page = _text(PAGE)
    assert _text(BLOCK) in page, "catalog/site/stats.html does not carry the family block byte for byte"
    assert "ekdLoadInstitutions('institution-list')" in page
    assert 'id="institution-list"' in page
    assert "__INSTITUTIONS_BLOCK__" not in page, "the placeholder was never substituted"


def test_no_private_institution_list_in_generator_or_page():
    page, gen = _text(PAGE), _text(GEN)
    for name in PRIVATE_LISTS:
        assert name not in page, f"{name} is back in catalog/site/stats.html"
        assert name not in gen, f"{name} is back in catalog/gen_site.py"
    # the old code sorted the feed; the page must render it as received
    institutions_js = page[page.index("EKD_FAMILY_STATS"):]
    assert ".sort(" not in institutions_js[:institutions_js.index("async function ekdLoadInstitutions")]


def test_served_page_is_what_the_generator_produces():
    """A hand edit to the served page, or a generator change that was never rendered, both fail here."""
    spec = importlib.util.spec_from_file_location("gen_site_for_stats_test", GEN)
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    body = g.render_stats().replace("</body>", g.FAMILY_BAND + g.FOOTER + "</body>", 1)
    page = _text(PAGE)
    stamps = set(re.findall(r"fdate\('(\d{4}-\d{2}-\d{2})'\)", page))
    assert len(stamps) == 1, stamps
    assert body.replace("\r\n", "\n").replace("__SITE_UPDATED__", stamps.pop()) == page


def test_controls_the_checks_can_fail():
    """R900: each predicate above is shown to reject what it exists to reject."""
    block = _text(BLOCK)
    assert hashlib.sha256((block + " ").encode("utf-8")).hexdigest() != BLOCK_SHA256
    assert block.replace("featured", "feetured") not in _text(PAGE)
    assert any(name in "var INST_PRESTIGE={'Stanford University':10};" for name in PRIVATE_LISTS)
