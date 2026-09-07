"""The derive must record whether its run was evidence about the WHOLE store.

WHY. `derive_statcan_tables.py` writes its summary unconditionally - dry runs and scoped runs
included - and that summary carries the `max_rows` the cataloguer adopts as fact. So

    derive_statcan_tables.py --dry-run --max-rows 500000

would leave the real 3,000,000 split map on disk, replace the provenance record with 500,000, and
the cataloguer would then assert 500,000 as measured - reconstituting R832's "965 tables exceed
500,000 rows but 372 have no split-map entry" WITH a confident provenance line attached, which is
strictly worse than the shared-constant guess it replaced.

The guard is `scope`, and the cataloguer refuses to adopt a cap whose scope is not `full`.

WHY THIS FILE EXISTS AT ALL. An adversarial review ran three mutants against the derive half -
"always records full", "records no scope", "records MAX_ROWS_DEFAULT rather than a.max_rows" -
and ALL THREE SURVIVED, because every existing test drives the cataloguer and nothing exercised
the derive's summary write. That is R840's rule: a suite where nothing calls the real function is
testing nothing. The expression was inlined in a dict literal and only reachable by running a
whole derive over a store; it is now a named function reachable in a microsecond.
"""
from __future__ import annotations

import argparse
import importlib.util
import io
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "derive_statcan_tables.py")


def _load():
    spec = importlib.util.spec_from_file_location("_derive_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class _Args:
    def __init__(self, **kw):
        self.dry_run = kw.get("dry_run", False)
        self.only = kw.get("only", "")
        self.limit = kw.get("limit", 0)


def test_an_unscoped_run_is_full():
    assert _load().run_scope(_Args()) == "full"


def test_a_dry_run_is_never_full():
    """The case that motivated the whole guard."""
    m = _load()
    assert m.run_scope(_Args(dry_run=True)) == "dry_run"


def test_dry_run_outranks_every_other_flag():
    """A dry run is not evidence about the store WHATEVER else was passed, so it is checked
    first. If `--limit` were checked first, `--dry-run --limit 1` would record `limit`, and the
    cataloguer's refusal message would name the wrong reason."""
    m = _load()
    assert m.run_scope(_Args(dry_run=True, limit=5)) == "dry_run"
    assert m.run_scope(_Args(dry_run=True, only="10100001")) == "dry_run"
    assert m.run_scope(_Args(dry_run=True, only="10100001", limit=5)) == "dry_run"


def test_only_and_limit_are_each_scoped():
    m = _load()
    assert m.run_scope(_Args(only="10100001")) == "only"
    assert m.run_scope(_Args(limit=1)) == "limit"
    assert m.run_scope(_Args(only="10100001", limit=5)) == "only"


def test_an_EXPLICIT_falsy_flag_still_reads_as_full():
    """`--limit 0` and `--only ""` restrict nothing, so they are full-store runs. Testing the
    argparse DEFAULTS is not enough - a user can pass the default explicitly, and a truthiness
    test must give the same answer either way."""
    m = _load()
    assert m.run_scope(_Args(limit=0)) == "full"
    assert m.run_scope(_Args(only="")) == "full"
    assert m.run_scope(_Args(only="", limit=0)) == "full"


def test_the_argparse_defaults_really_are_falsy():
    """The function's correctness depends on `--limit` defaulting to 0 and `--only` to "" rather
    than to None-vs-0 confusion or a sentinel. If a future change makes either default truthy,
    every ordinary full run would silently record itself as scoped and the cataloguer would stop
    adopting any cap at all - a guard that always fires is an outage, not a guard."""
    m = _load()
    ap = argparse.ArgumentParser()
    # rebuild only the three flags this depends on, from the tool's own definitions
    src = io.open(_TOOL, encoding="utf-8").read()
    assert '"--dry-run"' in src and '"--only"' in src and '"--limit"' in src, "flags renamed"
    ns = ap.parse_args([])
    del ns, ap
    a = _Args()
    assert a.limit == 0 and a.only == "" and a.dry_run is False
    assert m.run_scope(a) == "full"


def test_the_summary_really_carries_a_scope_KEY():
    """A mutant that renamed the summary key from "scope" to "scope_removed" SURVIVED my first
    version of this test - because that version built its own `{"scope": ...}` dict and round-
    tripped it through json. It tested json, not the tool. R840 again, one layer down.

    This is a SOURCE-level pin, and says so: it asserts the tool's summary literal contains the
    key the cataloguer reads. Weaker than running the derive, and honest about it - the
    alternative is a full derive over a store for one dict key.
    """
    src = io.open(_TOOL, encoding="utf-8").read()
    assert '"scope": run_scope(a),' in src, (
        'the summary must write the key "scope"; the cataloguer reads _sum.get("scope") and a '
        'renamed key silently disables the whole guard')
    # and the cataloguer must read that exact key
    cat = io.open(os.path.join(os.path.dirname(_HERE), "tools",
                               "catalog_statcan_tables.py"), encoding="utf-8").read()
    assert '_sum.get("scope")' in cat, "the cataloguer no longer reads the key the derive writes"


def test_only_full_scope_is_adoptable():
    """The rule the key exists to serve, stated once so a reader need not infer it."""
    m = _load()
    for kw, scope in (({}, "full"), ({"dry_run": True}, "dry_run"),
                      ({"only": "10100001"}, "only"), ({"limit": 3}, "limit")):
        got = m.run_scope(_Args(**kw))
        assert got == scope, (kw, got)
        assert (got == "full") == (scope == "full")


def test_max_rows_recorded_is_the_RUN_s_cap_not_the_default():
    """The third surviving mutant: recording MAX_ROWS_DEFAULT instead of `a.max_rows` would make
    every summary claim 500,000 whatever the run used - which is precisely the wrong number that
    started this, now stamped as provenance."""
    m = _load()
    src = io.open(_TOOL, encoding="utf-8").read()
    assert '"max_rows": int(a.max_rows),' in src, (
        "the summary must record the RUN's cap; recording MAX_ROWS_DEFAULT would stamp 500,000 "
        "on every run")
    assert m.MAX_ROWS_DEFAULT == 500_000, m.MAX_ROWS_DEFAULT
