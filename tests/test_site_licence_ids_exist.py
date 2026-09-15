"""Every licence identifier a generated page DISPLAYS must exist in the catalogue.

WHY THIS EXISTS. On 2026-09-08 a hand edit to a generated, deployed dataset page rewrote the
licence identifier it displays to a string that is not one of the rows in `data/catalog.db`'s
`license` table — a user-visible licence claim invented by a text substitution. The generator
never does that: it renders `license.license_id` (or its human label) straight from the
registry. Only an edit to the OUTPUT can produce an id the catalogue has never heard of, and a
page is what a visitor reads.

`catalog/site/*.html` is what `.github/workflows/deploy-site.yml` publishes
(`wrangler pages deploy catalog/site`), so this is a check on the served surface, not on a
draft.

The catalogue is gitignored (11.9 GB), so on a runner without it this test SKIPS — loudly, with
the reason — and the extractor control below still runs, so a broken extractor can never make
the skip look like a pass (R900: an inability to check is not a pass).
"""
from __future__ import annotations

import glob
import os
import re
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = os.path.join(ROOT, "catalog", "site")

def _db_path() -> str:
    """The catalogue is gitignored and lives outside a worktree, so allow an explicit
    override. Without it, the in-tree path; without that, the test skips."""
    return os.environ.get("ECON_CATALOG_DB") or os.path.join(ROOT, "data", "catalog.db")


DB = _db_path()

# The generator renders either a human LABEL (LICENSE_LABEL) or, when it has none, the raw
# licence id. This is gen_site.py's own definition of "this string is a raw id, not a label".
RAW_LICENSE_TOKEN = re.compile(r"^[a-z0-9][a-z0-9._+-]*$")

BADGE = re.compile(r'<span class="badge lic">([^<]+)</span>')
KV = re.compile(r"<dt>License</dt><dd>([^<]+)</dd>")

# Displayed strings that are neither a label nor a licence id: the catalog/status pages build
# badges client-side from a JS array, so their literal template text shows up here.
NON_ID = {"'+esc(r.license)+'", "'+esc(s.source)+'"}


def _displayed_ids(html: str) -> set[str]:
    out = set()
    for m in list(BADGE.finditer(html)) + list(KV.finditer(html)):
        s = m.group(1).strip()
        if not s or s in NON_ID:
            continue
        if RAW_LICENSE_TOKEN.match(s):
            out.add(s)
    return out


def test_the_extractor_is_not_vacuous():
    """The control. An extractor that finds nothing would make the assertion below pass on
    every page, invented licence ids included."""
    planted = (
        '<div class="badges"><span class="badge lic">not-a-real-licence-id</span></div>'
        "<dl class=\"kv\"><dt>License</dt><dd>cc-by-4.0</dd>"
    )
    got = _displayed_ids(planted)
    assert got == {"not-a-real-licence-id", "cc-by-4.0"}, got
    # a human label must NOT be mistaken for an id
    assert _displayed_ids('<span class="badge lic">CC BY 4.0 (attribution required)</span>') == set()

    pages = sorted(glob.glob(os.path.join(SITE, "*.html")))
    assert len(pages) > 100, f"only {len(pages)} generated pages found; the site path is wrong"
    with_ids = [p for p in pages if _displayed_ids(open(p, encoding="utf-8").read())]
    # Measured 2026-09-09: 38 of 333 pages display a RAW licence id; the other 295 display a
    # human label from LICENSE_LABEL, which is correct and must not be counted as an id.
    assert len(with_ids) >= 30, (
        f"only {len(with_ids)} of {len(pages)} pages yielded a displayed licence id (38 when "
        f"this floor was measured). The extractor is broken, so the assertion below would be "
        f"vacuous.")


def test_every_displayed_licence_id_exists_in_the_catalogue():
    if not os.path.exists(DB):
        pytest.skip(
            f"{DB} is not present (gitignored, 11.9 GB). Run this on a machine that holds the "
            f"catalogue; the extractor control above still ran.")
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    try:
        known = {r[0] for r in con.execute("SELECT license_id FROM license")}
    finally:
        con.close()
    assert len(known) > 20, f"only {len(known)} rows in the license table; wrong database?"

    bad: dict[str, set[str]] = {}
    for p in sorted(glob.glob(os.path.join(SITE, "*.html"))):
        with open(p, encoding="utf-8") as fh:
            shown = _displayed_ids(fh.read())
        unknown = {s for s in shown if s not in known}
        if unknown:
            bad[os.path.basename(p)] = unknown
    assert not bad, (
        "these generated pages display a licence identifier that is not a row in the "
        "catalogue's `license` table — a licence claim the registry never made:\n  "
        + "\n  ".join(f"{k}: {sorted(v)}" for k, v in sorted(bad.items()))
        + "\nFix the catalogue row and regenerate. Never rewrite a licence id on a page."
    )
