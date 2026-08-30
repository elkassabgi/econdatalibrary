"""worldbank: the 8 income-group aggregates must actually refresh (AR-022 / R509 / R512).

WHY. `worldbank` re-projects `worldbank_wdi` by matching keys exactly. Eight of its 692
published ids end in a 2-char income-group code (XD/XM/XN/XT) while wdi carries the same
economies under the publisher's 3-char form (HIC/LIC/LMC/UMC), so the match missed and the
fetcher reported them `missing` on every run — never refreshed. The file's own docstring
promised "whoever adds the mapping will see the count go to zero".

THE TEST DESIGN IS ITSELF A FIX. My first version of this file re-implemented `update()`'s
selection loop in a local `_project()` helper and asserted against that. A reviewer mutated
the SHIPPED code — deleting the alias branch from `update()` outright — and all six tests
stayed green. The pin sat beside the code path instead of on it, which is R511's rule 4 in
a new dress. So the regression test now drives the real `wb.update()` with a fake upstream
table and a stubbed catalogue, and asserts on the rows it actually writes.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import worldbank as wb            # noqa: E402


def test_map_is_the_measured_four():
    """Pinned to the publisher's reference list. Changing this map is a data claim and must
    be re-measured against /v2/country, not edited from memory (R504)."""
    assert wb.WDI_GEO_TO_LEGACY == {"HIC": "XD", "LIC": "XM", "LMC": "XN", "UMC": "XT"}


def test_alias_rewrites_only_the_geo_segment():
    assert wb._legacy_alias("NY.GDP.MKTP.CD:HIC") == "NY.GDP.MKTP.CD:XD"
    assert wb._legacy_alias("SL.UEM.TOTL.ZS:UMC") == "SL.UEM.TOTL.ZS:XT"
    # MORE THAN ONE COLON is the case that actually distinguishes rpartition from split —
    # a reviewer showed my original "indicator codes contain dots" rationale was false, and
    # that `split(":")[0]` passed every test I had written.
    assert wb._legacy_alias("A:B:LIC") == "A:B:XM"
    assert wb._legacy_alias("A.B.C.D:LIC") == "A.B.C.D:XM"


def test_alias_is_none_for_everything_else():
    """The control. If this returned a value for ordinary economies the map would be a
    rewriter rather than a repair, and it would corrupt 684 working ids to fix 8."""
    for k in ("NY.GDP.MKTP.CD:USA", "NY.GDP.MKTP.CD:ZWE", "NY.GDP.MKTP.CD:LMY",
              "NY.GDP.MKTP.CD:XD", "no-colon-here", ""):
        assert wb._legacy_alias(k) is None, k


# ── the regression, driven through the shipped update() ─────────────────────

INDS = ["NY.GDP.MKTP.CD", "SL.UEM.TOTL.ZS"]
PUBLISHED = {f"{i}:{g}" for i in INDS for g in ("XD", "XM", "XN", "XT")} | \
            {f"{i}:USA" for i in INDS}


def _fake_upstream() -> pa.Table:
    """Shaped like the store `update()` actually opens: clean_full/worldbank_wdi with a
    `series_key` column of `WDI:<IND>:<GEO>`. (My first version measured the GROUPED tier,
    which has a `country` column and which update() never reads — right answer, wrong
    instrument.)"""
    keys, dates, vals = [], [], []
    for ind in INDS:
        for geo in ("HIC", "LIC", "LMC", "UMC", "USA"):
            keys.append(f"WDI:{ind}:{geo}")
            dates.append(dt.date(2025, 12, 31))
            vals.append(1.0)
    return pa.table({"series_key": pa.array(keys, pa.string()),
                     "obs_date": pa.array(dates, pa.date32()),
                     "value": pa.array(vals, pa.float64())})


@pytest.fixture
def driven(monkeypatch, tmp_path):
    """Run the REAL update() against a fake catalogue and a fake upstream, capturing the
    table it writes."""
    written = {}

    monkeypatch.setattr(wb, "_published_keys", lambda: set(PUBLISHED))
    monkeypatch.setattr(wb, "_migrate_legacy", lambda _p: None)
    monkeypatch.setattr(wb.config, "source_dir", lambda _s: str(tmp_path))
    monkeypatch.setattr(wb.blob, "exists", lambda _p: True)
    monkeypatch.setattr(wb.blob, "read_table", lambda _p, columns=None: _fake_upstream())
    monkeypatch.setattr(wb.blob, "row_count", lambda _p: 0)

    def fake_merge(path, tbl, mode=None, dedup_keys=None):
        written["tbl"] = tbl
        return tbl.num_rows, dt.date(2025, 12, 31)

    monkeypatch.setattr(wb.merge, "merge_and_write", fake_merge)
    return written


def test_update_projects_all_eight_aggregates(driven, capsys):
    """THE REGRESSION. Deleting the alias branch from update() must fail HERE — the previous
    version of this test re-implemented the loop and survived exactly that mutation."""
    wb.update("_all", None)
    tbl = driven["tbl"]
    got = set(tbl.column("series_key").to_pylist())
    assert PUBLISHED - got == set(), f"update() did not project: {sorted(PUBLISHED - got)}"
    assert got <= PUBLISHED, f"update() invented ids: {sorted(got - PUBLISHED)}"
    # The docstring's own promise, asserted on the fetcher's own output.
    out = capsys.readouterr().out
    assert "have no worldbank_wdi counterpart" not in out, (
        f"the `missing` line still fired, so the count did not go to zero:\n{out}")


def test_update_does_not_mint_ids_the_catalogue_lacks(driven):
    """An upstream 3-char geo whose 2-char form is NOT published must stay skipped, or the
    fetcher invents series nobody published (R487's shape)."""
    # Publish only ONE of the eight; the other seven aliases must be discarded.
    only = {"NY.GDP.MKTP.CD:XD"}
    import unittest.mock as m
    with m.patch.object(wb, "_published_keys", lambda: set(only)):
        wb.update("_all", None)
    got = set(driven["tbl"].column("series_key").to_pylist())
    assert got == only, f"expected exactly {only}, got {sorted(got)}"


def test_a_published_id_wins_over_an_alias_for_the_same_key(driven, monkeypatch):
    """If wdi ever publishes BOTH spellings they collapse onto the published id. Renamed from
    the misleading original: which VALUE survives is decided by upstream row order inside
    merge's dedup, not by this function — so this asserts key collapse only."""
    def both(_p, columns=None):
        return pa.table({
            "series_key": pa.array(["WDI:NY.GDP.MKTP.CD:XD", "WDI:NY.GDP.MKTP.CD:HIC"]),
            "obs_date": pa.array([dt.date(2025, 12, 31)] * 2, pa.date32()),
            "value": pa.array([111.0, 999.0], pa.float64())})
    monkeypatch.setattr(wb.blob, "read_table", both)
    import unittest.mock as m
    with m.patch.object(wb, "_published_keys", lambda: {"NY.GDP.MKTP.CD:XD"}):
        wb.update("_all", None)
    keys = driven["tbl"].column("series_key").to_pylist()
    assert set(keys) == {"NY.GDP.MKTP.CD:XD"}, keys
    assert len(keys) == 2, "both rows are kept and handed to merge's dedup, not dropped here"


def test_the_alias_is_tried_only_AFTER_a_direct_match(driven, monkeypatch):
    """ORDER IS LOAD-BEARING, and nothing pinned it: applying the alias first passed every
    other test in this file.

    If the catalogue ever publishes the 3-char spelling directly, an alias-first rule would
    rewrite that row to the 2-char id — claiming a key it was not asked for and silently
    dropping the one it was. Today no `<IND>:HIC` is published, so this is latent; a latent
    hazard with no test is how it stops being latent.
    """
    def upstream(_p, columns=None):
        return pa.table({
            "series_key": pa.array(["WDI:NY.GDP.MKTP.CD:HIC"]),
            "obs_date": pa.array([dt.date(2025, 12, 31)], pa.date32()),
            "value": pa.array([1.0], pa.float64())})
    monkeypatch.setattr(wb.blob, "read_table", upstream)
    import unittest.mock as m
    # BOTH spellings published. The direct match must win.
    with m.patch.object(wb, "_published_keys",
                        lambda: {"NY.GDP.MKTP.CD:HIC", "NY.GDP.MKTP.CD:XD"}):
        wb.update("_all", None)
    keys = driven["tbl"].column("series_key").to_pylist()
    assert keys == ["NY.GDP.MKTP.CD:HIC"], (
        f"expected the DIRECTLY published id, got {keys} — the alias ran before the direct "
        f"match and hijacked a row the catalogue publishes under its own name")
