"""ember: merge-measured changed keys in '<file stem>:<series_key>' form, mapped back to the 60 served ids,
and `catalog_scope: subset` (2026-09-23).

ember reported every merged key as a cursor (90,422-167,041 per pass). Its 60 catalogue ids are
ember:<A|M>:<metric>:<GEO>, which econdl._resolve._resolve_ember serves from ONE native key in ONE of two
files, so no cursor could map and every pass read "csv coherence unmet ... none of the CHANGED keys
matched" and demoted to partial (daily run 35783253243, never a success). The real update(), merge and
orchestrator mapping run; Ember's bucket is faked at the ingester functions.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import ember as E  # noqa: E402

YEARLY = "yearly_full_release_long_format"
MONTHLY = "monthly_full_release_long_format"
WORLD_GEN = "World|Electricity generation|Total|Total Generation|TWh"


def _wire(monkeypatch, tmp_path, files, version="v1"):
    """files: {dataset id: [(series_key, date, value)]} - one bucket object per dataset."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(E.config, "source_dir", lambda s: str(tmp_path))
    objs = [{"name": f"public-downloads/{ds}.csv", "updated": version, "size": str(len(rows))}
            for ds, rows in files.items()]
    monkeypatch.setattr(E.ig, "enumerate_catalog", lambda sess: objs)
    monkeypatch.setattr(E.ig, "current_csvs", lambda items: items)
    monkeypatch.setattr(E.ig, "dataset_id", lambda name: name.split("/")[-1][:-len(".csv")])
    monkeypatch.setattr(E.ig, "download_bytes", lambda sess, key: key)
    monkeypatch.setattr(E.ig, "read_csv_bytes", lambda raw: raw)
    monkeypatch.setattr(E.ig, "route", lambda ds, df: (
        "long", [{"series_key": k, "obs_date": d, "value": v} for k, d, v in files[ds]]))


D1, D2 = dt.date(2024, 12, 31), dt.date(2025, 12, 31)


def test_changed_keys_are_merge_measured_and_carry_their_file(monkeypatch, tmp_path):
    files = {YEARLY: [(WORLD_GEN, D1, 1.0)], "some_graphic": [(WORLD_GEN, D1, 9.0)]}
    _wire(monkeypatch, tmp_path, files)
    res = E.update(None, None)
    assert res.changed_keys == {f"{YEARLY}:{WORLD_GEN}": "2024-12-31",
                                f"some_graphic:{WORLD_GEN}": "2024-12-31"}, res.changed_keys
    assert res.cursor_cap_hit is False


def test_an_identical_refetch_reports_nothing_changed_and_a_revision_reports_its_key(monkeypatch, tmp_path):
    files = {YEARLY: [(WORLD_GEN, D1, 1.0), ("World|x|y|z|u", D1, 2.0)]}
    _wire(monkeypatch, tmp_path, files)
    E.update(None, None)
    _wire(monkeypatch, tmp_path, files, version="v2")         # republished, same numbers
    assert E.update(None, None).changed_keys == {}
    files = {YEARLY: [(WORLD_GEN, D1, 1.5), ("World|x|y|z|u", D1, 2.0)]}
    _wire(monkeypatch, tmp_path, files, version="v3")         # one revised value
    assert E.update(None, None).changed_keys == {f"{YEARLY}:{WORLD_GEN}": "2024-12-31"}


def test_a_pass_over_the_changed_keys_cap_reports_unknown_and_flags_it(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(E.merge, "CHANGED_KEYS_CAP", 1)
    _wire(monkeypatch, tmp_path, {YEARLY: [(WORLD_GEN, D1, 1.0), ("World|x|y|z|u", D1, 2.0)]})
    res = E.update(None, None)
    assert res.changed_keys is None and res.cursor_cap_hit is True
    assert "UNKNOWN (cursor_cap_hit)" in capsys.readouterr().out


def test_negative_control_at_the_cap_the_set_is_complete(monkeypatch, tmp_path):
    monkeypatch.setattr(E.merge, "CHANGED_KEYS_CAP", 2)
    _wire(monkeypatch, tmp_path, {YEARLY: [(WORLD_GEN, D1, 1.0), ("World|x|y|z|u", D1, 2.0)]})
    res = E.update(None, None)
    assert len(res.changed_keys) == 2 and res.cursor_cap_hit is False


# ---- the orchestrator's map back -----------------------------------------------------------------
def _catalog(tmp_path, monkeypatch, ids):
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")     # ember runs in the cloud
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "ember") for i in ids])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))


IDS = ["ember:A:gen_total_twh:WORLD", "ember:M:gen_total_twh:WORLD", "ember:A:gen_total_twh:USA"]


def test_a_served_key_maps_to_its_id_only_from_its_own_file(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, IDS)
    ids, unmapped = orchestrate._catalog_ids_for("ember", [
        f"{YEARLY}:{WORLD_GEN}", f"{MONTHLY}:{WORLD_GEN}", f"some_graphic:{WORLD_GEN}", f"{YEARLY}:World|x|y|z|u"])
    assert sorted(ids) == ["ember:A:gen_total_twh:WORLD", "ember:M:gen_total_twh:WORLD"], ids
    assert sorted(unmapped) == sorted([f"some_graphic:{WORLD_GEN}", f"{YEARLY}:World|x|y|z|u"]), unmapped


def test_negative_control_a_bare_cursor_key_never_maps(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, IDS)
    ids, unmapped = orchestrate._catalog_ids_for("ember", [WORLD_GEN])
    assert ids == [] and list(unmapped) == [WORLD_GEN]


def test_an_uncatalogued_id_is_not_claimed(tmp_path, monkeypatch):
    """The index is built from the CATALOGUE, so a resolvable-but-uncatalogued id is never derived."""
    _catalog(tmp_path, monkeypatch, ["ember:A:gen_total_twh:USA"])
    ids, unmapped = orchestrate._catalog_ids_for("ember", [f"{YEARLY}:{WORLD_GEN}"])
    assert ids == [] and list(unmapped) == [f"{YEARLY}:{WORLD_GEN}"]


def test_the_index_is_the_resolvers_own_inverse(tmp_path, monkeypatch):
    """Every id the index claims resolves (through the real resolver) to exactly the key it came from."""
    _catalog(tmp_path, monkeypatch, IDS)
    import core.derive_csv  # noqa: F401
    from econdl import _resolve as R
    con = sqlite3.connect(os.environ["ECONDL_CATALOG"])
    idx = orchestrate._ember_index(con)
    assert len(idx) == 3
    for key, cids in idx.items():
        stem, native = key.split(":", 1)
        for cid in cids:
            _, f, metric, geo = cid.split(":")
            cat, sub, var, unit = R._EMBER_METRICS[f][metric]
            assert R._EMBER_FILE[f] == stem + ".parquet"
            assert native == f"{R._EMBER_GEO[geo]}|{cat}|{sub}|{var}|{unit}"


def test_the_registry_declares_ember_a_catalogue_subset():
    orchestrate._REG_ENTRIES = None
    assert orchestrate._catalog_scope("ember") == "subset"


def test_a_pass_moving_only_uncatalogued_series_is_coverage_not_a_demotion(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, IDS)
    orchestrate._REG_ENTRIES = None
    unit = types.SimpleNamespace(key="ember/_all", source_id="ember", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors={}, new_vintage="v", status="ok",
                                changed_keys={f"some_graphic:k{i}": "2026-08-01" for i in range(60000)})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, None, store=None)
    assert failed == [] and note.startswith("csv coverage note:"), note


def test_a_served_series_changing_is_derived(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, IDS)
    orchestrate._REG_ENTRIES = None
    asked = []
    from updater import derive
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="ember/_all", source_id="ember", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors={}, new_vintage="v", status="ok",
                                changed_keys={f"{YEARLY}:{WORLD_GEN}": "2026-08-01",
                                              **{f"some_graphic:k{i}": "2026-08-01" for i in range(20)}})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert asked == ["ember:A:gen_total_twh:WORLD"] and failed == [], (asked, failed)
    assert note is None or note.startswith("csv coverage note:"), note


def test_a_truncated_pass_never_reads_as_a_green_coverage_note(tmp_path, monkeypatch):
    """The over-cap fallback's bare cursor keys cannot map; a sample of them must not 'prove' nothing
    served changed (AR-132, the abs twin)."""
    _catalog(tmp_path, monkeypatch, IDS)
    orchestrate._REG_ENTRIES = None
    unit = types.SimpleNamespace(key="ember/_all", source_id="ember", unit_id="_all")
    res = types.SimpleNamespace(obs=10, new_vintage="v", status="ok", changed_keys=None, cursor_cap_hit=True,
                                series_cursors={WORLD_GEN: "2026-08-01", "x|y": "2026-08-01"})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, None, store=None)
    assert note.startswith("csv coherence unmet:") and "cap-saturated" in note, note
