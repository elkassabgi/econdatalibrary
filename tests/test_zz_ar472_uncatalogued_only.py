"""AR-472 probe: a stat_slovenia pass whose MERGE-MEASURED changed set holds only tables that
have no catalogue row (the store has 274 such tables, 175 with data to 2024+, and three whole
groups: 015, 080, 307) goes through the orchestrator's ZERO-MAPPED branch, which demotes with
'csv coherence unmet' even though AR-380 says an unmapped merge-measured key is coverage."""
import os
import sys
import types

_REPO = os.environ.get("AR472_REPO") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO)
sys.path.insert(0, os.path.join(_REPO, "tests"))

import test_stat_slovenia_changed_keys as T  # noqa: E402
from updater.strategies.fetchers import stat_slovenia as S  # noqa: E402
from updater import orchestrate as O  # noqa: E402
from updater import health as H  # noqa: E402

catalog = T.catalog  # fixture re-export


def test_uncatalogued_only_pass(monkeypatch, tmp_path, catalog):
    import sqlite3
    c = sqlite3.connect(catalog)
    c.execute("DELETE FROM series WHERE series_id='stat_slovenia:SI:BBB01S'")   # BBB is uncatalogued
    c.commit()
    c.close()
    (tmp_path / "store").mkdir()
    unit = T._wire(monkeypatch, tmp_path / "store", T._stored(year_aaa=2025, year_bbb=2024),
                   ["2024", "2025"], {"BBB01S": T._body("2025", 9.0)})
    res = S.update(unit, None)
    print("status", res.status, "err", res.error, "changed", res.changed_keys)
    assert set(res.changed_keys) == {"SI:BBB01S"}
    # production path: _derive_changed_csvs with no store
    unit2 = types.SimpleNamespace(source_id="stat_slovenia", key="stat_slovenia/_all", config={})
    out = O._derive_changed_csvs(unit2, res, None, None)
    print("derive_changed_csvs ->", out)
    note = out[1] or ""
    err = "; ".join(x for x in (res.error, note) if x)
    print("health deferral-only:", H._deferral_only([{"status": "partial", "last_error": err}]))
    assert not note.startswith("csv coherence unmet"), note
