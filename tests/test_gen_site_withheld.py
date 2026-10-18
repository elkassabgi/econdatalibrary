"""The site does not advertise WITHHELD tables (api/worker/src/withheld.ts; AR-341 finding 4).

insee_melodi.html said "Series catalogued 139" and "139 series ... Free download" while the worker withheld 44 of
them (404 "withheld", in no listing). catalog/gen_site.py now subtracts the withheld series the catalogue holds,
read from api/worker/withheld.json, and `--only SOURCE` regenerates that one page through the same code.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sqlite3

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_withheld_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _db(ids):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, start_date TEXT, end_date TEXT, "
                "geography TEXT, last_updated TEXT)")
    rows = []
    for i in ids:
        held = i.endswith(":A")                                      # the held one has the widest coverage
        rows.append((i, i.split(":")[0], "1901-12-31" if held else "2000-12-31", "2026-12-31" if held else "2024-12-31",
                     "G-held" if held else "FR", "2026-10-18" if held else "2026-09-01"))
    con.executemany("INSERT INTO series VALUES (?, ?, ?, ?, ?, ?)", rows)
    return con


def test_the_withheld_series_the_catalogue_holds_are_subtracted(G, capsys):
    con = _db(["insee_melodi:A", "insee_melodi:B", "insee_melodi:C", "ecb:X"])
    roll = {"insee_melodi": {"n_series": 3}, "ecb": {"n_series": 1}}
    G.subtract_withheld(con, roll, {"insee_melodi": ["insee_melodi:A", "insee_melodi:NOT_CATALOGUED"],
                                    "wid": ["wid:Z"]})
    assert roll["insee_melodi"]["n_series"] == 2 and roll["ecb"] == {"n_series": 1}, "only a held id the catalogue holds"
    # AR-341 re-review N4: the coverage facets are re-measured without the held series
    assert {k: roll["insee_melodi"][k] for k in ("min_start", "max_end", "n_geo", "last_updated")} == \
        {"min_start": "2000-12-31", "max_end": "2024-12-31", "n_geo": 1, "last_updated": "2026-09-01"}
    assert "withheld: insee_melodi n_series -1" in capsys.readouterr().out
    control = {"insee_melodi": {"n_series": 3}}
    G.subtract_withheld(con, control, {})
    assert control == {"insee_melodi": {"n_series": 3}}, "no list, no change"


def test_the_generator_reads_the_committed_list(G):
    committed = json.load(open(os.path.join(ROOT, "api", "worker", "withheld.json"), encoding="utf-8"))["ids"]
    got = sorted(i for ids in G.WITHHELD.values() for i in ids)
    assert got == sorted(committed) and got
    assert set(G.WITHHELD) == {i.split(":")[0] for i in committed}


def test_load_registry_subtracts_after_the_carve_outs(G):
    src = open(GEN, encoding="utf-8").read()
    body = src[src.index("def load_registry():"):src.index("def load_sidecar():")]
    assert "subtract_withheld(con, series_roll)" in body
    assert body.index("for _src, _inds in SERIES_CARVEOUTS.items():") < body.index("subtract_withheld(con, series_roll)")


def test_only_writes_that_one_page_and_nothing_else(G, tmp_path, monkeypatch):
    out = tmp_path / "site"
    out.mkdir()
    (out / "keep.html").write_text("untouched", encoding="utf-8")
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    monkeypatch.setattr(G, "load_registry", lambda: ({}, {"aaa": {}, "insee_melodi": {}},
                                                     {"aaa": {"n_series": 1}, "insee_melodi": {"n_series": 94}}, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: ({}, "2026-10-17"))
    monkeypatch.setattr(G, "build_record", lambda sid, *a: {"id": sid, "reservable": 1, "license_id": "x"})
    monkeypatch.setattr(G, "build_page_seo", lambda records: {})
    monkeypatch.setattr(G, "jsonld_license", lambda r: {"x": 1})
    monkeypatch.setattr(G, "render_dataset_page", lambda rec: f"<html><body>{rec['id']}</body></html>")
    monkeypatch.setattr(G, "_load_lastmod_state", lambda: {"x": {}})
    G.main(["--only", "insee_melodi"])
    assert sorted(p.name for p in out.iterdir()) == ["insee_melodi.html", "keep.html"]
    monkeypatch.setattr(G, "render_catalog", lambda records, generated: "<html><body>hub " +
                        ",".join(r["id"] for r in records) + "</body></html>")
    G.main(["--only", "insee_melodi", "--with-catalog"])
    assert sorted(p.name for p in out.iterdir()) == ["catalog.html", "insee_melodi.html", "keep.html"]
    assert "hub aaa,insee_melodi" in (out / "catalog.html").read_text(encoding="utf-8"), "rendered from ALL records"
    with pytest.raises(SystemExit):
        G.main(["--with-catalog"])
    assert (out / "keep.html").read_text(encoding="utf-8") == "untouched"
    with pytest.raises(SystemExit):
        G.main(["--only", "not_a_source"])
