"""`gen_site.py --hub-rows SRC[,SRC...]` rewrites ONLY those rows of the committed catalog.html.

Why: `--only SRC --with-catalog` re-renders every hub row from the local catalogue, and that catalogue drifts from
what is served while the D1 catalogue sync is frozen. On 2026-10-20 a full re-render moved statcan to 467,611
series while the API served 466,341. So the hub kept insee_melodi 139 and dst 2264 after their pages (and the API)
said 95 and 2229. The splice takes the named rows from the same records as the pages and keeps every other byte.

The last test pins the property the bug broke, on the committed files: every hub row carries the same series
count and "Registry last updated" date as that source's own page.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
SITE = os.path.join(ROOT, "catalog", "site")


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_hub_rows_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _row(sid, n, updated=""):
    return {"id": sid, "name": sid.upper(), "desc": "d </x>", "license": "CC BY 4.0", "reservable": True,
            "cats": ["Macroeconomics"], "n_series": n, "page": f"{sid}.html", "pillar": "macro",
            "region": "Europe", "updated": updated}


def _hub(G, rows, eol="\n"):
    return eol.join(["<html><body><script>", "const IDX=" + G._idx_json(rows) + ";",
                     'const API="https://example";', "</script>", "Website updated: 2026-09-17", "</body></html>", ""])


def _idx(html):
    m = re.search(r"^const IDX=(.*);\r?$", html, re.M)
    return json.loads(m.group(1).replace("<\\/", "</"))


@pytest.mark.parametrize("eol", ["\n", "\r\n"])
def test_only_the_named_rows_change_and_every_other_byte_is_kept(G, eol):
    old = [_row("aaa", 10), _row("dst", 2264), _row("insee_melodi", 139, "2026-07-28"), _row("statcan", 466341)]
    html = _hub(G, old, eol)
    new, changes = G.splice_hub_rows(html, {"insee_melodi": _row("insee_melodi", 95, "2026-07-23"),
                                            "dst": _row("dst", 2229),
                                            "statcan": _row("statcan", 467611)},       # offered, NOT named
                                     ["insee_melodi", "dst"])
    assert changes == [("insee_melodi", {"n_series": (139, 95), "updated": ("2026-07-28", "2026-07-23")}),
                       ("dst", {"n_series": (2264, 2229)})]
    got = {r["id"]: r for r in _idx(new)}
    assert got["insee_melodi"]["n_series"] == 95 and got["dst"]["n_series"] == 2229
    assert got["statcan"]["n_series"] == 466341, "a row that was not named keeps its committed value"
    assert [r["id"] for r in _idx(new)] == ["aaa", "dst", "insee_melodi", "statcan"], "order kept"
    a, b = html.splitlines(keepends=True), new.splitlines(keepends=True)
    assert len(a) == len(b) and [k for k in range(len(a)) if a[k] != b[k]] == [1], "only the IDX line moves"
    assert b[1].endswith(eol) and "<\\/x>" in b[1], "line ending and the </ escape survive"
    again, ch2 = G.splice_hub_rows(new, {"insee_melodi": _row("insee_melodi", 95, "2026-07-23"),
                                         "dst": _row("dst", 2229)}, ["insee_melodi", "dst"])
    assert again == new and ch2 == [("insee_melodi", {}), ("dst", {})], "idempotent"


def test_it_refuses_rather_than_guess(G):
    html = _hub(G, [_row("aaa", 1), _row("dst", 2)])
    with pytest.raises(SystemExit, match="1 time|0 time"):
        G.splice_hub_rows(html, {"zzz": _row("zzz", 1)}, ["zzz"])               # not in the hub
    with pytest.raises(SystemExit, match="no published record"):
        G.splice_hub_rows(html, {}, ["dst"])                                    # no fresh row: would drop a source
    bad = dict(_row("dst", 3), extra=1)
    with pytest.raises(SystemExit, match="fields changed"):
        G.splice_hub_rows(html, {"dst": bad}, ["dst"])
    with pytest.raises(SystemExit, match="exactly 1"):
        G.splice_hub_rows(html.replace("const IDX=", "const XDI="), {"dst": _row("dst", 3)}, ["dst"])
    with pytest.raises(SystemExit, match="re-encode"):                          # not the generator's encoding
        G.splice_hub_rows(html.replace('"id": "aaa"', '"id":"aaa"'), {"dst": _row("dst", 3)}, ["dst"])
    dup = _hub(G, [_row("dst", 1), _row("dst", 2)])
    with pytest.raises(SystemExit, match="2 time"):
        G.splice_hub_rows(dup, {"dst": _row("dst", 3)}, ["dst"])


def test_the_committed_hub_round_trips_byte_for_byte(G):
    """Splicing every committed row with itself must give the committed bytes back: the shared encoding is the
    one the hub was written with, so a splice cannot rewrite a row it was not asked to touch."""
    with open(os.path.join(SITE, "catalog.html"), encoding="utf-8", newline="") as f:
        html = f.read()
    rows = _idx(html)
    assert len(rows) > 300
    out, changes = G.splice_hub_rows(html, {r["id"]: dict(r) for r in rows}, [r["id"] for r in rows])
    assert out == html and all(d == {} for _s, d in changes) and len(changes) == len(rows)


def test_main_hub_rows_writes_only_catalog_html(G, tmp_path, monkeypatch):
    out = tmp_path / "site"
    out.mkdir()
    (out / "keep.html").write_text("untouched", encoding="utf-8")
    hub = _hub(G, [_row("aaa", 1), _row("insee_melodi", 139, "2026-07-28")], "\r\n")
    (out / "catalog.html").write_bytes(hub.encode("utf-8"))
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    monkeypatch.setattr(G, "load_registry", lambda: ({}, {"aaa": {}, "insee_melodi": {}},
                                                     {"aaa": {"n_series": 7}, "insee_melodi": {"n_series": 95}}, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: ({}, "2026-10-20"))
    monkeypatch.setattr(G, "build_record", lambda sid, _s, _l, roll, *a: {"id": sid, "reservable": 1, "license_id": "x"})
    monkeypatch.setattr(G, "build_page_seo", lambda records: {})
    monkeypatch.setattr(G, "jsonld_license", lambda r: {"x": 1})
    monkeypatch.setattr(G, "_load_lastmod_state", lambda: {"x": {}})
    fresh = {"aaa": _row("aaa", 7), "insee_melodi": _row("insee_melodi", 95, "2026-07-23")}
    monkeypatch.setattr(G, "_catalog_idx", lambda records: [fresh[r["id"]] for r in records])
    monkeypatch.setattr(G, "render_dataset_page", lambda rec: pytest.fail("--hub-rows must not write a page"))
    monkeypatch.setattr(G, "render_catalog", lambda *a: pytest.fail("--hub-rows must not re-render the hub"))
    G.main(["--hub-rows", "insee_melodi"])
    assert sorted(p.name for p in out.iterdir()) == ["catalog.html", "keep.html"]
    new = (out / "catalog.html").read_bytes().decode("utf-8")
    got = {r["id"]: r for r in _idx(new)}
    assert got["insee_melodi"]["n_series"] == 95 and got["insee_melodi"]["updated"] == "2026-07-23"
    assert got["aaa"]["n_series"] == 1, "aaa's fresh row (7) was NOT named, so the committed 1 stays"
    assert new.count("\r\n") == hub.count("\r\n") and "Website updated: 2026-09-17" in new
    for bad in (["--hub-rows", "insee_melodi", "--only", "insee_melodi"],
                ["--hub-rows", "insee_melodi", "--with-catalog"], ["--hub-rows", ","]):
        with pytest.raises(SystemExit):
            G.main(bad)
    assert (out / "keep.html").read_text(encoding="utf-8") == "untouched"


def _page_facts(name):
    html = open(os.path.join(SITE, name), encoding="utf-8").read()
    n = re.findall(r'Series catalogued</div><div class="mval">([0-9,]+)<', html)
    u = re.findall(r'Registry last updated</div><div class="mval">([0-9-]+)<', html)
    return (int(n[0].replace(",", "")) if len(n) == 1 else None), (u[0] if u else "")


def test_every_hub_row_agrees_with_its_own_page():
    """The committed hub and the committed source pages are built from the same records. A page regenerated alone
    (`--only`) must bring its hub row with it (`--hub-rows`), or the hub advertises a different count."""
    with open(os.path.join(SITE, "catalog.html"), encoding="utf-8") as f:
        rows = _idx(f.read())
    assert len(rows) > 300
    seen, wrong = 0, []
    for r in rows:
        n, u = _page_facts(r["page"])
        assert n is not None, f"{r['page']}: no single 'Series catalogued' metric - the check would be blind"
        seen += 1
        if (r["n_series"], r["updated"]) != (n, u):
            wrong.append((r["id"], (r["n_series"], r["updated"]), (n, u)))
    assert seen == len(rows) and not wrong, f"hub row vs page (series, updated): {wrong}"
