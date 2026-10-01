"""Catalogue titles carry no line breaks (Ahmed, 2026-10-01: "do whats best for us").

clean_title() turns each run of whitespace containing a CR or LF into one space and strips the ends; everything else
stays as published. The examples are real titles from the catalogue on 2026-09-30 (dst, eia, scb).

WHAT ENFORCES THE RULE (review R1323): ~45 modules write series.title, so no list of writers can be the guarantee.
The guarantee is (1) core/sync_catalog_d1.py cleaning every title on its way to D1 and (2)
tools/clean_catalogue_titles.py, which cleans the local copy and whose --check fails while any title holds a break
(tests/test_clean_catalogue_titles.py). The list below is defence in depth: the writers that produced such titles,
the INSERT OR REPLACE re-cataloguers, and the two tools that write D1 directly - each must keep calling clean_title.
"""
from __future__ import annotations

import ast
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core.titles import clean_title  # noqa: E402


@pytest.mark.parametrize("raw, want", [
    ("1-2.1.1 Production\nand  generation of income (10a3-grouping)",            # dst:DST:NABP10
     "1-2.1.1 Production and  generation of income (10a3-grouping)"),
    ("\r\nU.S. Net Imports from Curacao of Liquified Petroleum Gases",           # eia: a leading CRLF
     "U.S. Net Imports from Curacao of Liquified Petroleum Gases"),
    ("Price of \r\nSweetgrass", "Price of Sweetgrass"),                          # eia: CRLF after a space
    ("Quarter\r  2015K2", "Quarter 2015K2"),                                      # scb: a lone CR
    ("a \t\n\t b", "a b"),                                                        # whitespace around the break
    ("a\n\n\nb", "a b"),                                                          # several breaks in a row
    ("no  break here", "no  break here"),                                         # untouched: double space stays
    ("  padded  ", "  padded  "),                                                 # no break -> unchanged (R1324)
    (" nbsp ", " nbsp "),                                     # no break -> unchanged
    ("  a\nb  ", "a b"),                                                          # a break -> cleaned AND stripped
    ("", ""),
])
def test_clean_title(raw, want):
    assert clean_title(raw) == want


def test_none_stays_none():
    assert clean_title(None) is None


def test_the_result_never_holds_a_line_break():
    for s in ("x\ry", "x\r\ny", "\n", "\r\r\n\n", "a b"):
        out = clean_title(s)
        assert "\r" not in out and "\n" not in out, repr(s)


# Every writer of series.title that produced or can re-produce a title with a line break (the 2026-09-30 sources and
# the INSERT OR REPLACE re-cataloguers). A new title writer belongs here.
WRITERS = [
    "tools/catalog_pxweb_flowgrain.py", "tools/catalog_statcan_tables.py", "core/broaden_catalog.py",
    "tools/catalog_eia_tables.py", "tools/title_eia_eba_all.py", "tools/title_eia_nuclear_status.py",
    "tools/title_eia_outlook_scenarios.py", "tools/apply_series_names.py", "core/apply_title_wave.py",
    "core/catalog.py", "tools/refresh_sec_edgar.py", "core/sync_catalog_d1.py",
    "tools/enrich_sec_edgar_tickers.py", "tools/sync_titles_to_d1.py", "core/export_d1_new_series.py",
]   # tools/selfhost/origin_copies.py hands clean_title to SQLite (create_function) - its behaviour test covers it


def _calls_clean_title(path):
    tree = ast.parse(open(os.path.join(ROOT, path), encoding="utf-8").read())
    return any(isinstance(n, ast.Call) and getattr(n.func, "id", None) == "clean_title" for n in ast.walk(tree))


@pytest.mark.parametrize("path", WRITERS)
def test_every_named_title_writer_calls_clean_title(path):
    assert _calls_clean_title(path), f"{path} writes series.title without clean_title()"


def test_the_ratchet_can_fail(tmp_path):
    p = tmp_path / "w.py"
    p.write_text("def f(t):\n    return t.strip()\n", encoding="utf-8")
    assert not _calls_clean_title(str(p))
