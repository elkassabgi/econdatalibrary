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
    """A parser that silently returns an empty set would make this whole change a no-op.

    No id is named here on purpose (the gated ids are not repeated anywhere in this repository).
    The canary is structural instead: every parsed entry must look like a bare source id, so a
    parser that started harvesting prose from a comment would fail here.
    """
    with open(DENY, encoding="utf-8") as fh:
        ids = _parse_denylist(fh.read())
    assert ids, "parsed zero denylisted ids — the expression has drifted"
    assert all(re.fullmatch(r"[a-z0-9_]+", i) for i in ids), (
        "a parsed denylist entry is not a bare source id — the parser is harvesting prose")


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


def _resolver_ids() -> set:
    util = os.path.join(ROOT, "api", "worker", "src", "util.ts")
    with open(util, encoding="utf-8") as fh:
        src = fh.read()
    m = re.search(r"SUPPORTED_SOURCES\s*:\s*readonly\s+string\[\]\s*=\s*\[(.*?)\]\s*;", src, re.S)
    assert m, "SUPPORTED_SOURCES not found in util.ts"
    body = re.sub(r"//.*", "", m.group(1))
    body = re.sub(r"/\*.*?\*/", "", body, flags=re.S)
    return set(re.findall(r'"([^"]+)"', body))


def test_the_resolver_never_offers_a_denylisted_source():
    """Since 2026-09-08 the two lists are disjoint by construction: a resolver entry for a gated
    source is an offer the API answers with 451, so such entries were removed from
    SUPPORTED_SOURCES. This pins that in the direction the R8/R29 rule cares about — an id may be
    gated, or served, never both — and it is why the negative control below has to be synthetic.
    """
    with open(DENY, encoding="utf-8") as fh:
        deny = _parse_denylist(fh.read())
    resolver = _resolver_ids()
    assert len(resolver) > 250, f"parsed only {len(resolver)} resolver ids — the parser is broken"
    assert not (resolver & deny), (
        "these ids are in BOTH SUPPORTED_SOURCES and NON_REDISTRIBUTABLE — a gated source is "
        "being offered by the resolver: " + ", ".join(sorted(resolver & deny)))


def test_negative_control_a_source_only_in_the_resolver_would_have_passed_before():
    """R346/R414: prove the old logic could NOT see this.

    Reproduce the pre-fix rule — resolver membership alone — on a synthetic pair and assert it
    admits a denylisted source, while the subtraction the generator now applies withholds it.
    The pair is synthetic because the real lists are disjoint (see the test above), so a control
    drawn from them would be vacuous.
    """
    deny = _parse_denylist('export const NON_REDISTRIBUTABLE = new Set(["made_up_gated"]);')
    assert deny == {"made_up_gated"}, "the synthetic denylist did not parse — control is void"
    resolver = {"made_up_served", "made_up_gated"}
    assert "made_up_gated" in resolver                    # pre-fix rule: in the resolver -> offered
    assert "made_up_gated" not in (resolver - deny)       # the fix: subtract the gate -> withheld
    assert "made_up_served" in (resolver - deny)          # and a served id is untouched
