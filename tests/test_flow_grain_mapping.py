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


class TestCsoFlowGrain:
    """cso (CSO Ireland, PxStat) is prefix-resolved like the other PxWeb sources.

    It was SERVED with 7,896 catalogue rows while absent from _FLOW_GRAIN, so the generic
    resolver's exact match found nothing: measured 2026-08-07, every id resolved to 0 rows
    and its derive failed 22/22. After adding it, 7,606 of 7,896 resolve against the real
    store (the remaining 290 are catalogue rows with no store data — a separate defect).
    """

    def test_cso_is_flow_grain(self):
        import clients.python.econdl._resolve as r
        assert "cso" in r._FLOW_GRAIN

    def test_store_key_starts_with_the_catalogue_native(self):
        native = "CSO:EIIEEA29"
        key = "CSO:EIIEEA29:STATISTIC=EIIEEA29C01:C01841V02268=1"
        assert key.startswith(native + ":")
        # and the trailing colon keeps a longer matrix code from being swallowed
        assert not "CSO:EIIEEA290:STATISTIC=X".startswith(native + ":")


class TestFileGrainResolver:
    """ons_uk / insee_melodi put the dataset identity in the FILENAME, not the key."""

    def _store(self, tmp_path, src, name, cols):
        import pyarrow as pa, pyarrow.parquet as pq
        d = tmp_path / src
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(cols), d / f"{name}.parquet")
        return str(tmp_path)

    def test_resolves_the_named_file_and_selects_its_rows(self, tmp_path):
        import clients.python.econdl._resolve as r
        import pyarrow.dataset as ds
        root = self._store(tmp_path, "ons_uk", "ageing-population-estimates", {
            "series_key": ["administrative-geography=E06000001:sex=male",
                           "administrative-geography=E06000002:sex=all"],
            "obs_date": ["2020-12-31", "2020-12-31"], "value": [1.0, 2.0]})
        res = r._resolve_file_grain("ons_uk:ageing-population-estimates", root)
        assert ds.dataset(res.parquet_path).filter(res.predicate).count_rows() == 2

    def test_flow_column_is_asserted_when_present(self, tmp_path):
        """insee_melodi carries a `flow` column; a mis-named file must select NOTHING
        rather than silently serve another dataset's rows under this id."""
        import clients.python.econdl._resolve as r
        import pyarrow.dataset as ds
        root = self._store(tmp_path, "insee_melodi", "DD_CNA_AGREGATS", {
            "flow": ["SOMETHING_ELSE", "SOMETHING_ELSE"],
            "series_key": ["A=1", "A=2"],
            "obs_date": ["2020-12-31", "2020-12-31"], "value": [1.0, 2.0]})
        res = r._resolve_file_grain("insee_melodi:DD_CNA_AGREGATS", root)
        assert ds.dataset(res.parquet_path).filter(res.predicate).count_rows() == 0

    def test_missing_file_raises_instead_of_serving_empty(self, tmp_path):
        """A catalogued id with no store file must fail loudly — an empty download is
        indistinguishable from a series that genuinely has no observations."""
        import clients.python.econdl._resolve as r
        (tmp_path / "ons_uk").mkdir(parents=True)
        with pytest.raises(r.ResolveError):
            r._resolve_file_grain("ons_uk:does-not-exist", str(tmp_path))
