"""One rule for a catalogue title's line breaks (Ahmed, 2026-10-01: "do whats best for us").

A publisher's table title sometimes carries a line break from its own layout - DST's NABP10 is
"1-2.1.1 Production\\nand  generation of income (10a3-grouping)", three EIA titles start with "\\r\\n". On the
site, in search results and in a CSV header such a title shows on two lines or with an empty first line, and it
was the reason D1 held 25 titles altered by the SQL emitters' newline translation (R1315, fixed in #105).

ONE PREDICATE (review R1324): a title is changed if and only if it holds a CR or LF - has_line_break(). Then every
run of whitespace that contains a CR or LF becomes one space and the ends are stripped. A title with NO line break
is returned unchanged, byte for byte (leading/trailing spaces, NBSP and all), so the D1 sync, the local cleaner, its
--check detector and the self-host copy all act on exactly the same set of rows.
"""
from __future__ import annotations

import re

_BREAK = re.compile(r"[^\S\r\n]*[\r\n][\s]*")


def has_line_break(title) -> bool:
    return isinstance(title, str) and ("\n" in title or "\r" in title)


def clean_title(title):
    """A title WITH a line break: each break (and the whitespace around it) as one space, ends stripped. Any other
    value - None, or a title without a CR/LF - is returned unchanged."""
    if not has_line_break(title):
        return title
    return _BREAK.sub(" ", title).strip()
