"""A dataset page must not offer a download the API answers with 451.

`catalog/gen_site.py::load_resolvable` reads `SUPPORTED_SOURCES` from
`api/worker/src/util.ts` — the RESOLVER list, meaning "the worker knows how to serve this". The
451 gate lives in a DIFFERENT file, `api/worker/src/denylist.ts`, and nothing in the generator
read it. So a source could be in the resolver, get a full page with seven "Free download"
buttons, and be refused by the API.

Measured live on 2026-08-25, found by an adversarial review of a Pages deploy:

    GET /v1/catalog?source=unsdg   ->  451 {"error":"non_redistributable", ...}
    econdatalibrary.com/unsdg      ->  200, "Redistributable.", "Free download" x7

That is exactly the shape `load_resolvable`'s own docstring exists to prevent — "a page is a
promise", written after cepii_gravity shipped a Download button over a 404 — and it missed it
because it checked the resolver rather than the gate.

This does not decide the licence question. `unsdg`'s canonical verdict in
`DATABASE_LICENSES_VERBATIM.md` CLEARS it, so the denylist may well be the stale side; that call
belongs to Ahmed. But whichever side is wrong, the page must describe what the data plane will
actually do.

The empty-set contract is load-bearing and is tested separately: an unreadable or unparsable
denylist must subtract NOTHING, because silently stripping the download offer from 322 working
pages is a far worse failure than the one being fixed.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
DENY = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")


def _gen_src() -> str:
    with open(GEN, encoding="utf-8") as fh:
        return fh.read()


def _parse_denylist(text: str) -> set:
    """The same expression gen_site uses, kept in one place so the tests cannot drift from it."""
    m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new\s+Set\s*\(\s*\[(.*?)\]\s*\)", text, re.S)
    if not m:
        m = re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*\[(.*?)\]\s*;", text, re.S)
    if not m:
        return set()
    body = re.sub(r"//.*", "", m.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    return set(re.findall(r'"([^"]+)"', body))


def test_the_denylist_gates_the_field_that_decides_the_download_offer():
    """It must gate `reservable`, which is what the download block reads.

    The first version of this test asserted `RESOLVABLE = load_resolvable() - DENYLISTED`, and
    PASSED on a fix that did nothing: RESOLVABLE has one unrelated consumer (gen_site.py:2269),
    while the download offer is decided by `rec["reservable"]` at :622, :1655, :1719 and :2160.
    The site was rebuilt and unsdg still carried seven "Free download" offers. A test that pins
    a code shape instead of the deciding field passes on the wrong fix.
    """
    s = _gen_src()
    lines = [ln for ln in s.splitlines()
             if ln.strip().startswith("reservable = bool(lrow.get(")]
    assert len(lines) == 1, (
        f"expected exactly one `reservable = bool(lrow.get(...)` assignment, found "
        f"{len(lines)} — re-locate the gate before trusting this test")
    assert "DENYLISTED" in lines[0], (
        "`reservable` is set without consulting the worker denylist, so a denylisted source "
        "will render a download offer the API refuses with 451")


def test_the_denylist_loader_exists_and_reads_the_workers_own_file():
    s = _gen_src()
    assert "def load_denylisted()" in s
    assert '"denylist.ts"' in s, (
        "the denylist must be read from the worker's own denylist.ts, not re-derived from "
        "licence rows — denylist.ts is the file the 451 gate actually consults")


def test_the_real_denylist_parses_and_is_not_empty():
    """A parser that silently returns an empty set would make this whole change a no-op."""
    with open(DENY, encoding="utf-8") as fh:
        ids = _parse_denylist(fh.read())
    assert len(ids) >= 10, f"parsed only {len(ids)} denylisted ids — the expression has drifted"
    assert "unsdg" in ids, (
        "unsdg is the source that exposed this defect; if it has left the denylist, re-check "
        "whether the page and the API now agree before assuming this test is stale")


def test_an_unreadable_denylist_subtracts_nothing():
    """Empty set => unknown => downgrade nothing. Silence beats stripping 322 download offers."""
    assert _parse_denylist("") == set()
    assert _parse_denylist("export const SOMETHING_ELSE = new Set([\"a\"]);") == set()


def test_comments_are_stripped_before_ids_are_harvested():
    """R137/R142 shape: prose inside a comment must not be harvested as a source id."""
    text = ('export const NON_REDISTRIBUTABLE = new Set([\n'
            '  // "not_an_id" appears in this comment\n'
            '  "real_id",\n'
            '  /* "also_not_an_id" */\n'
            '  "second_id",\n'
            ']);')
    assert _parse_denylist(text) == {"real_id", "second_id"}


def test_negative_control_a_source_only_in_the_resolver_would_have_passed_before():
    """R346/R414: prove the old logic could NOT see this.

    Reproduce the pre-fix rule — resolver membership alone — and assert it admits a denylisted
    source. If this ever fails, the resolver and the denylist have converged and the subtraction
    may no longer be doing anything.
    """
    with open(DENY, encoding="utf-8") as fh:
        deny = _parse_denylist(fh.read())
    util = os.path.join(ROOT, "api", "worker", "src", "util.ts")
    with open(util, encoding="utf-8") as fh:
        src = fh.read()
    m = re.search(r"SUPPORTED_SOURCES\s*:\s*readonly\s+string\[\]\s*=\s*\[(.*?)\]\s*;", src, re.S)
    body = re.sub(r"//.*", "", m.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    resolver = set(re.findall(r'"([^"]+)"', body))
    overlap = resolver & deny
    assert overlap, (
        "no source is in BOTH the resolver and the denylist, so the subtraction changes "
        "nothing — verify against the live API before deleting this guard")
    assert not (resolver - deny) & overlap
