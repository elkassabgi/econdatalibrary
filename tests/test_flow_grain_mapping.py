"""The coherence mapper and the client resolver must agree on the flow boundary.

If they disagree the catalogue advertises ids the mapper cannot resolve: the source merges
its rows, §5.7 finds unmapped keys, and it demotes to `partial` on EVERY run no matter how
complete its catalogue is — which is how unsdg sat permanently unhealthy while holding a
correct 396-row catalogue (measured 2026-08-07: 37,822 of 227,955 keys, 16.6%, were
undimensioned and so exact-missed AND flow-missed under the PxWeb '='-stripping rule).
"""
from __future__ import annotations

import pytest

from updater.orchestrate import _FIRST_SEGMENT_FLOW, _flow_of


class TestUnsdgFirstSegmentFlow:
    """unsdg's flow is the first ':'-segment, for BOTH key shapes it stores."""

    @pytest.mark.parametrize("key", [
        "AG_LND_DGRD:AFG",                       # undimensioned — the 16.6% that used to miss
        "AG_LND_DGRD:AFG|Sex=FEMALE",            # one dimension
        "SH_LGR_ACSRHEC9:100|Age=15+|Sex=BOTHSEX",  # several
        "AG_FPA_CFPI:100",
    ])
    def test_maps_to_the_series_code(self, key):
        assert _flow_of(key, "unsdg") == key.split(":", 1)[0]

    def test_undimensioned_key_is_not_its_own_flow(self):
        """The regression itself: without the rule the key maps to itself, which never
        matches a catalog id, which is what demoted the source every run."""
        k = "AG_LND_DGRD:AFG"
        assert _flow_of(k) == k                    # old behaviour, still there for PxWeb
        assert _flow_of(k, "unsdg") != k           # fixed for unsdg

    def test_trailing_colon_is_load_bearing(self):
        """A code must not swallow a longer code that starts with it."""
        assert _flow_of("AG_LND_DGRD2:AFG", "unsdg") == "AG_LND_DGRD2"


class TestPxWebUnchanged:
    """The PxWeb family predates this and must be byte-identical, with or without the id."""

    @pytest.mark.parametrize("key,expect", [
        ("LV:OSP_OD:ARA30.px:ContentsCode=X:Apmacibas=0", "LV:OSP_OD:ARA30.px"),
        ("SSB:A1Skog:Region=03:Tid=2020", "SSB:A1Skog"),
        # hagstofa's colon-bearing dimension VALUE (the 658-key bug) stays truncated at .px
        ("px:THJ11002.px:Atvinnugrein=K: 65", "px:THJ11002.px"),
    ])
    def test_pxweb_rule_holds(self, key, expect):
        assert _flow_of(key) == expect
        assert _flow_of(key, "stat_latvia") == expect
        assert _flow_of(key, "hagstofa") == expect


def test_resolver_and_mapper_agree_on_membership():
    """clients/python/econdl/_resolve.py serves flow-grain sources by prefix; every source
    the mapper treats as first-segment MUST be in that set, or a served id and a mapped id
    name different row sets."""
    import clients.python.econdl._resolve as r
    missing = _FIRST_SEGMENT_FLOW - r._FLOW_GRAIN
    assert not missing, (
        f"{sorted(missing)} use the first-segment flow rule in updater/orchestrate.py but are "
        f"absent from _FLOW_GRAIN in the client resolver — the catalogue would advertise ids "
        f"whose CSV and parquet download disagree.")
