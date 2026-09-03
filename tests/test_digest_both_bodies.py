"""Every digest section must render in BOTH bodies, because only one of them is read.

`send_digest` posts `{"text": body, "html": html_doc}`, and the code's own comment at the HTML
block says "# ---- HTML (the real email) ----". Every HTML-capable client renders html_doc, so a
section that exists only in `lines` has not shipped.

On 2026-09-03 three did exactly that and would never have been seen:

  * the 11 unmanaged leftover state rows (no registry entry, so they never re-run)
  * the 7 sources outside the live tier (AQUEDUCT_LIVE_ONLY — the run does not execute them)
  * the first-pass crawlers (gus_dbw, cbs_nl — no unit_state row, invisible to the health gate)

Each was measured, tested, committed and rendered correctly in the text body, and none of it
reached the reader. The row-level work the same day DID reach HTML — `tried=` has its own cell and
the LATE marker is appended to it — which is what makes this failure mode easy to miss: half the
day's changes were fine.

This is a SOURCE-SHAPE guard, in the style of tests/test_gzip_bytes_deterministic.py, which scans
for bare `gzip.compress` calls on R2 write paths. It cannot prove the rendering is correct; it
proves neither body was forgotten, which is the mistake that actually happened.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import updater.send_digest as sd  # noqa: E402

SRC = open(sd.__file__, encoding="utf-8").read()

# (identifier, what it reports) — each must appear in the text body AND the html fragments.
SECTIONS = [
    ("orphans", "unmanaged leftover state rows with no registry entry"),
    ("dormant", "sources outside the live tier that the daily run never executes"),
    ("fp", "first-pass crawlers, which have no unit_state row"),
]


def _text_block() -> str:
    """The region that builds the plain-text body."""
    return SRC[:SRC.index("# ---- HTML (the real email) ----")]


def _html_block() -> str:
    return SRC[SRC.index("# ---- HTML (the real email) ----"):]


@pytest.mark.parametrize("name,what", SECTIONS, ids=[s[0] for s in SECTIONS])
def test_each_section_reaches_the_text_body(name, what):
    assert f"if {name}:" in _text_block(), f"the text body stopped reporting {what}"


@pytest.mark.parametrize("name,what", SECTIONS, ids=[s[0] for s in SECTIONS])
def test_each_section_reaches_the_html_body(name, what):
    """The one that actually gets read."""
    html = _html_block()
    assert f"if {name}:" in html, (
        f"the HTML body does not report {what} — it would render in the text fallback only, "
        f"which is what happened on 2026-09-03")


def test_the_notes_block_is_actually_placed_in_the_document():
    """Building the fragment and never interpolating it is the same bug one step later."""
    assert "notes_html = " in SRC, "notes_html is no longer built"
    assert "{notes_html}" in SRC, (
        "notes_html is built but never interpolated into html_doc — the sections would be "
        "computed and thrown away")


def test_the_html_preview_hook_still_exists():
    """Without it there is no way to LOOK at the real email, which is how this went unnoticed."""
    assert "AQUEDUCT_DIGEST_HTML_OUT" in SRC, (
        "the preview hook is gone; the only way left to inspect the HTML would be to send it")
