"""One rule for a catalogue title's line breaks (Ahmed, 2026-10-01: "do whats best for us").

A publisher's table title sometimes carries a line break from its own layout - DST's NABP10 is
"1-2.1.1 Production\\nand  generation of income (10a3-grouping)", three EIA titles start with "\\r\\n". On the
site, in search results and in a CSV header such a title shows on two lines or with an empty first line, and it
was the reason D1 held 25 titles altered by the SQL emitters' newline translation (R1315, fixed in #105).

clean_title() turns every run of whitespace that CONTAINS a CR or LF into one space, and strips the ends. Nothing
else changes: a double space elsewhere, punctuation and case stay exactly as the publisher wrote them. Every
writer of `series.title` that can produce such a title calls it (tests/test_clean_title.py names them).
"""
from __future__ import annotations

import re

_BREAK = re.compile(r"[^\S\r\n]*[\r\n][\s]*")


def clean_title(title):
    """The title with each line break (and the whitespace around it) as one space, stripped. None stays None."""
    if title is None:
        return None
    return _BREAK.sub(" ", str(title)).strip()
