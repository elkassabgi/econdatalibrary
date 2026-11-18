"""No public page carries internal engineering text (2026-11-18).

Every dataset page printed the gitignored sidecar's `description` (an ingester docstring for 91 of
its 102 sources: repo paths, checkpoint file names, source line numbers) as its lead and as "Full
description", plus "Update strategy" and "Storage layout" rows with internal store paths.
gen_site.build_record no longer reads those fields, and catalog/public_text_guard.py makes the
build fail if any such text reaches visible text, JSON-LD or meta/og content again.

Each test below can fail: the planted positives MUST raise, the controls MUST pass.
"""
from __future__ import annotations

import importlib.util
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")
SITE = os.path.join(ROOT, "catalog", "site")

# The shapes seen on the live pages (istat, bls, eia), condensed. None of this may reach a page.
INTERNAL_NOTE = ("Complete ISTAT dataflows the normal pull left INCOMPLETE. The retired SDMX ingester "
                 "(its parsers now live in jobs/_sdmx_common.py) writes data/clean_full/istat/ and records "
                 "_sliced_done.json; see lines 697-700. Run with --resume.")
INTERNAL_LAYOUT = "overwrite-multi-file: one zstd parquet per dataflow at data/clean_full/eia/<DATASET>.parquet"
SID = "zzpub"


@pytest.fixture()
def G():
    spec = importlib.util.spec_from_file_location("gen_site_public_text_under_test", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_one(G, tmp_path, monkeypatch, subtitle="Monthly consumer prices and industrial production indices"):
    out = tmp_path / "site"
    out.mkdir()
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    lic = {"cc-by-4.0": {"reservable": 1, "commercial_ok": 1, "attribution_required": 1, "no_modify": 0,
                         "url": "https://creativecommons.org/licenses/by/4.0/"}}
    src = {SID: {"name": "Test Statistics Office — Economic Indicators", "homepage": "https://example.org/",
                 "license_id": "cc-by-4.0", "attribution": "Source: Test Statistics Office.",
                 "terms_url": "https://example.org/terms"}}
    roll = {SID: {"n_series": 1234, "min_start": "1990-01-31", "max_end": "2026-06-30",
                  "frequencies": ["M"], "categories": [], "auto_subtitle": subtitle}}
    side = {SID: {"id": SID, "description": INTERNAL_NOTE, "storage_layout": INTERNAL_LAYOUT,
                  "strategy": "overwrite_if_changed", "scripts": ["jobs/ingest_zzpub.py"],
                  "cadence": "monthly", "measured_obs": 99999}}
    monkeypatch.setattr(G, "load_registry", lambda: (lic, src, roll, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: (side, "2026-09-10"))
    monkeypatch.setitem(G.SOURCE_SUBTITLES, SID, subtitle)
    G.main(["--only", SID])
    return (out / f"{SID}.html").read_text(encoding="utf-8")


def test_sidecar_internal_text_never_reaches_the_page(G, tmp_path, monkeypatch):
    html = _build_one(G, tmp_path, monkeypatch)
    for token in ("jobs/", "_sdmx_common", "data/clean_full", "_sliced_done", "lines 697", "--resume",
                  "<DATASET>", "Storage layout", "Update strategy", "Full description", "overwrite"):
        assert token not in html, f"internal text {token!r} reached the page"
    # the public lead is the subtitle, and the page still says what the database is
    assert '<p class="lead">Monthly consumer prices and industrial production indices</p>' in html
    assert G.public_text_guard.scan_html(html) == []


def test_planted_internal_text_in_a_public_field_fails_the_build(G, tmp_path, monkeypatch):
    with pytest.raises(G.public_text_guard.InternalTextLeak) as e:
        _build_one(G, tmp_path, monkeypatch, subtitle="Daily series written by jobs/ingest_zzpub.py at lines 10-20")
    assert "REFUSED" in str(e.value) and "zzpub.html" in str(e.value)
    assert not (tmp_path / "site" / f"{SID}.html").exists(), "a refused page must not be written"


def test_planted_text_in_json_ld_only_fails(G, tmp_path, monkeypatch):
    """A leak that reaches ONLY the JSON-LD (not visible text) must still fail."""
    real = G.dataset_jsonld
    monkeypatch.setattr(G, "dataset_jsonld",
                        lambda rec, d=None: {**real(rec, d), "keywords": ["see data/clean_full/x/<FLOW>.parquet"]})
    with pytest.raises(G.public_text_guard.InternalTextLeak):
        _build_one(G, tmp_path, monkeypatch)


def test_short_dataset_description_fails(G, tmp_path, monkeypatch):
    monkeypatch.setattr(G, "build_page_seo", lambda records: {r["id"]: ("T — x", "Too short.") for r in records})
    with pytest.raises(SystemExit, match="under 50 characters"):
        _build_one(G, tmp_path, monkeypatch)


@pytest.mark.parametrize("text", [
    "Untouched public text with no internal vocabulary at all, about consumer prices.",
    "Parquet, CSV, JSON and SDMX downloads through the API.",
    "fao_ge", "wid:WID:thwealj992:p81p82:992:j:LB", "imf_bop_direct:BOP:USA.CA.A",
    "GET /v1/series/{id}.metadata.json", "Source: Food and Agriculture Organization (FAO).",
    "1990–2024", "Data through line 5 of the table",
    # real publisher wording that a loose pattern would refuse - a false positive here blocks EVERY build
    "Fixed telephone lines 2020", "International poverty line 2.15 USD", "Spare parts/components",
    "Exports of tools/equipment", "core/periphery spreads", "Border checkpoint crossings",
    "SÃO PAULO", "Population: Total; true labour force participation",
    "Monthly; Annual; Quarterly", "Index (2010=100); Weighted index",
])
def test_guard_negative_controls(text):
    spec = importlib.util.spec_from_file_location("ptg", os.path.join(ROOT, "catalog", "public_text_guard.py"))
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    assert g.scan_text("t", text) == [], text


@pytest.mark.parametrize("text,label", [
    ("parsers live in jobs/_sdmx_common.py", "repo file path"),
    ("GROUPED storage (mirrors jobs/ingest_eurostat.py)", "repo file path"),
    ("see updater/strategies/fetchers/istat.py", "repo file path"),
    ("written to data/clean_full/istat/", "store path data/clean_full"),
    ("state in data/raw/bea/_done_groups.json", "store path data/clean_full"),
    ("state under data/_aqueduct", "state dir _aqueduct"),
    ("see orchestrate.py:1782", "source line reference"),
    ("lines 697-700", "line-number range"),
    ("run it with --resume", "CLI flag"),
    ("marks _sliced_done.json", "internal sidecar _x.json"),
    ("<prefix>.parquet + <prefix>.done (json stats)", "checkpoint marker .done"),
    ("_parts/ (transient checkpoint part files + .ckpt)", "checkpoint file"),
    ("merged from parts/area_{id}/v{var}_s{sec}.parquet", "parts/ directory"),
    ("concatenated and parts/ckpt removed", "parts/ directory"),
    ("a <DATASET> file", "<PLACEHOLDER>"),
    ("true - IRIIP", "boolean title segment"),
    ("All Items - true; Clothing and footwear", "boolean title segment"),
    ("broken � dash", "U+FFFD replacement char"),
    ("Statistik Ã–sterreich", "mojibake A-tilde"),
    ("CafÃ© prices", "mojibake A-tilde"),
    ("â€” em dash", "mojibake a-circumflex euro"),
])
def test_guard_planted_positives(text, label):
    spec = importlib.util.spec_from_file_location("ptg", os.path.join(ROOT, "catalog", "public_text_guard.py"))
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    assert label in {h[0] for h in g.scan_text("t", text)}, (text, label)


def test_code_pre_and_scripts_are_not_visible_text():
    spec = importlib.util.spec_from_file_location("ptg", os.path.join(ROOT, "catalog", "public_text_guard.py"))
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    page = ("<html><head><meta name=description content='A public description of a dataset.'>"
            "<script>fetch('/v1/auth?x=1'); var f='--flag'; // auth.js core/x.py</script></head><body>"
            "<p>pip install econdl</p><pre>python -m econdl --source x</pre><code>core/x.py</code>"
            '<div class="pid">fao_ge</div></body></html>')
    assert g.scan_html(page) == []
    assert g.scan_html(page.replace("pip install econdl", "see core/x.py")), "visible text IS checked"
    assert g.scan_html(page.replace("A public description", "data/clean_full/x")), "meta content IS checked"


def test_committed_hand_and_info_pages_pass_the_guard():
    """Negative control on real bytes: the pages gen_site does not take from the sidecar (docs, api,
    cite, download, account...) carry auth JS, API paths and code samples, and must all pass."""
    spec = importlib.util.spec_from_file_location("ptg", os.path.join(ROOT, "catalog", "public_text_guard.py"))
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    names = ["index.html", "docs.html", "api.html", "cite.html", "contact.html", "stats.html",
             "download.html", "account.html", "mcp.html", "status.html"]
    checked = 0
    for n in names:
        p = os.path.join(SITE, n)
        if os.path.isfile(p):
            assert g.scan_html(open(p, encoding="utf-8").read()) == [], n
            checked += 1
    assert checked >= 6, f"only {checked} control pages found - the control did not run"


# --------------------------------------------------------------------------------------------- #
#  The two non-page outputs, and the _provider.json writer (review of PR #294, C2)
# --------------------------------------------------------------------------------------------- #
def _build_full(G, tmp_path, monkeypatch, subtitle):
    out = tmp_path / "site"
    out.mkdir(exist_ok=True)
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    lic = {"cc-by-4.0": {"reservable": 1, "commercial_ok": 1, "attribution_required": 1, "no_modify": 0,
                         "url": "https://creativecommons.org/licenses/by/4.0/"}}
    src = {SID: {"name": "Test Statistics Office — Economic Indicators", "license_id": "cc-by-4.0"}}
    roll = {SID: {"n_series": 1234, "min_start": "1990-01-31", "max_end": "2026-06-30",
                  "frequencies": ["M"], "categories": [], "auto_subtitle": subtitle}}
    monkeypatch.setattr(G, "load_registry", lambda: (lic, src, roll, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: ({}, "2026-09-10"))
    G.main([])


def test_sources_meta_guard_refuses_internal_text(G, tmp_path, monkeypatch):
    """sources_meta.json is checked on its own: with the PAGE guard switched off, an internal subtitle
    must still stop a full build at sources_meta.json, before index/sitemap/state are written."""
    monkeypatch.setattr(G, "SITEMAP_STATE", str(tmp_path / "sitemap_state.json"))
    monkeypatch.setattr(G.public_text_guard, "assert_public", lambda name, html: None)
    with pytest.raises(G.public_text_guard.InternalTextLeak, match="sources_meta.json"):
        _build_full(G, tmp_path, monkeypatch, subtitle="Rows written by jobs/ingest_zzpub.py nightly")
    assert not (tmp_path / "site" / "sources_meta.json").exists()
    assert not (tmp_path / "sitemap_state.json").exists(), "the build went past the refusal"


def _hub(G, rows):
    return "\n".join(["<html><body><script>", "const IDX=" + G._idx_json(rows) + ";",
                      'const API="https://example";', "</script>", "</body></html>", ""])


def _hub_row(desc):
    return {"id": SID, "name": "ZZ", "desc": desc, "license": "CC BY 4.0", "reservable": True, "cats": [],
            "n_series": 1, "page": f"{SID}.html", "pillar": "macro", "region": "Europe", "updated": ""}


@pytest.mark.parametrize("desc,refused", [
    ("Monthly consumer prices and industrial production", False),       # control
    ("see data/clean_full/zzpub/<FLOW>.parquet", True),
])
def test_hub_rows_guard(G, tmp_path, monkeypatch, desc, refused):
    out = tmp_path / "site"
    out.mkdir()
    hub = _hub(G, [_hub_row("old blurb")])
    (out / "catalog.html").write_text(hub, encoding="utf-8")
    monkeypatch.setattr(G, "OUT_DIR", str(out))
    monkeypatch.setattr(G, "load_registry", lambda: ({"x": {"reservable": 1}}, {SID: {"license_id": "x"}},
                                                     {SID: {"n_series": 1}}, {}))
    monkeypatch.setattr(G, "load_sidecar", lambda: ({}, "2026-09-10"))
    monkeypatch.setattr(G, "build_record", lambda sid, *a: {"id": sid, "reservable": 1, "license_id": "x"})
    monkeypatch.setattr(G, "build_page_seo", lambda records: {})
    monkeypatch.setattr(G, "jsonld_license", lambda r: {"x": 1})
    monkeypatch.setattr(G, "_load_lastmod_state", lambda: {"x": {}})
    monkeypatch.setattr(G, "_catalog_idx", lambda records: [_hub_row(desc) for _r in records])
    if refused:
        with pytest.raises(G.public_text_guard.InternalTextLeak, match="hub-rows"):
            G.main(["--hub-rows", SID])
        assert (out / "catalog.html").read_text(encoding="utf-8") == hub, "a refused splice wrote the hub"
    else:
        G.main(["--hub-rows", SID])
        assert desc in (out / "catalog.html").read_text(encoding="utf-8")


def test_build_registry_does_not_copy_the_sidecar_description(tmp_path, monkeypatch):
    import json as _json
    import sys as _sys
    cat_json = tmp_path / "catalog.json"
    cat_json.write_text(_json.dumps({"sources": [{"id": SID, "name": "ZZ", "description": INTERNAL_NOTE,
                                                  "access": "api"}]}), encoding="utf-8")
    monkeypatch.syspath_prepend(ROOT)
    monkeypatch.syspath_prepend(os.path.join(ROOT, "core"))
    saved = _sys.modules.pop("catalog", None)
    try:
        spec = importlib.util.spec_from_file_location("build_registry_under_test",
                                                      os.path.join(ROOT, "core", "build_registry.py"))
        br = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(br)
    finally:
        _sys.modules.pop("catalog", None)
        if saved is not None:
            _sys.modules["catalog"] = saved
    monkeypatch.setattr(br, "CATJSON", str(cat_json))
    monkeypatch.setattr(br, "CLEAN", str(tmp_path / "no_store"))
    got = br.discover({"sources": {}})
    assert got[SID]["name"] == "ZZ" and got[SID]["catalogued"] is True, "control: the sidecar WAS read"
    assert got[SID]["description"] is None


# --------------------------------------------------------------------------------------------- #
#  Auto-derived subtitles that mislead or say nothing (review of PR #294, C3)
# --------------------------------------------------------------------------------------------- #
_BOOL_SEG = re.compile(r"(?:^|;\s*|\s-\s)(?:true|false)(?:\s-\s|;|$)")


def _subs(G, rows):
    import sqlite3
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE series (source_id TEXT, title TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?)", rows)
    return G._derive_subtitles(con)[0]


def test_boolean_and_placeholder_segments_never_lead_a_subtitle(G):
    s = _subs(G, [("zzb", "true - IRIIP - International investment position"),
                  ("zzb", "true - IRIIP - Reserve assets"),
                  ("zzc", "All Items - true"), ("zzc", "Clothing and footwear - Consumer price index (CPI)"),
                  ("zzd", "Not Aggregated - Proportion of Women in Ministerial Level Positions")])
    for sid, text in s.items():
        assert not _BOOL_SEG.search(text), (sid, text)
        assert "Not Aggregated" not in text, (sid, text)
    assert "Proportion of Women in Ministerial Level Positions" in s["zzd"]
    assert "Clothing and footwear" in s["zzc"]


def test_suppressed_source_publishes_no_derived_subtitle_anywhere(G):
    """imf_gsatf_direct's shape: 'Not Aggregated - <country> - Annual - <indicator>' with more than 30
    countries and indicators, so no column is kept and the fallback names the countries with most series
    ('Benin; Myanmar; Pakistan' for a worldwide survey). Such sources are listed in SUBTITLE_SUPPRESS:
    no derived subtitle, and the lead, meta description, Dataset, Croissant and catalog card fall back to
    the registry name. A geography register that catches this class without a list is a follow-up."""
    countries = ["Benin", "Myanmar", "Pakistan"] + [f"Country{i:02d}" for i in range(38)]
    indicators = [f"Indicator number {j} of financial access" for j in range(35)]
    def rows(sid):
        return [(sid, f"Not Aggregated - {c} - Annual - {indicators[(2 * k + j) % 35]}")
                for k, c in enumerate(countries) for j in range(2 if k > 2 else 5)]
    sup = "imf_gsatf_direct"
    assert sup in G.SUBTITLE_SUPPRESS and "imf_gslgrghts_direct" in G.SUBTITLE_SUPPRESS
    s = _subs(G, rows(sup) + rows("zzh_control"))
    assert s[sup] == "", s[sup]
    # control: the SAME rows under a source that is not listed still publish - the rule is the list
    assert s["zzh_control"].startswith("Benin"), s["zzh_control"]
    name = "IMF -- Gender Statistics: Access to Finance"
    rec = G.build_record(sup, {"name": name, "license_id": "x"}, {}, {"n_series": 3, "auto_subtitle": s[sup]}, None)
    assert rec["desc_short"] == name and not rec["subtitle"], rec
    outs = {"page": G.render_dataset_page(rec), "dataset": str(G.dataset_jsonld(rec)),
            "croissant": str(G.croissant_jsonld(rec)), "meta": G.build_meta_desc(rec),
            "catalog": G.render_catalog([rec], "2026-01-01")}
    for where, text in outs.items():
        for country in ("Benin", "Myanmar", "Pakistan"):
            assert country not in text, f"{country} leaked into the {where} output"
    ctl = G.build_record("zzh_control", {"name": name, "license_id": "x"}, {},
                         {"n_series": 3, "auto_subtitle": s["zzh_control"]}, None)
    assert "Benin" in G.render_dataset_page(ctl), "control: the page check above would be blind"


def test_blank_placeholder_does_not_empty_the_fallback(G):
    """imf_gsatf_direct-like titles that lead with 'Not Aggregated': the fallback must read the first
    NON-blank segment, else every row stems to '' and the source loses its subtitle."""
    rows = [("zzb", f"Not Aggregated - Account measure {j:02d} of adults") for j in range(45)]
    rows += [("zzb", "Not Aggregated - Account measure 00 of adults")] * 4
    assert _subs(G, rows)["zzb"].startswith("Account measure 00 of adults"), _subs(G, rows)["zzb"]


def test_single_segment_titles_keep_their_top_stems(G):
    """FAOSTAT / Energy Institute shape: one segment per title, far more than 30 distinct. The first (and
    only) segment IS the measure, so the fallback must still publish the most common ones."""
    rows = [("zzf", f"Emissions measure {j:02d}, All Crops") for j in range(45)]
    rows += [("zzf", "Emissions measure 00, All Crops")] * 4
    assert _subs(G, rows)["zzf"].startswith("Emissions measure 00, All Crops"), _subs(G, rows)["zzf"]


def test_placeholder_blanks_keep_columns_aligned(G):
    """imf_gssdo_direct's shape: the age column is 'Not Aggregated' on the totals. Removing that segment
    moved 'Annual' into the age column on those rows only, and the subtitle read 'Annual - Female; ...'."""
    bands = ["0-4 yrs", "5-9 yrs", "10-14 yrs", "15-19 yrs", "20-24 yrs", "25-29 yrs", "30-34 yrs",
             "35-39 yrs", "40-44 yrs", "45-49 yrs"]
    rows = [("zzs", f"Not Aggregated - Annual - {sex} - Population")
            for sex in ("Female", "Male") for _ in range(13)]
    rows += [("zzs", f"{b} - Annual - {sex} - Population") for b in bands for sex in ("Female", "Male")]
    s = _subs(G, rows)["zzs"]
    assert s and not any(stem.startswith("Annual") for stem in s.split("; ")), s


def test_year_only_or_tiny_subtitle_is_not_published_and_the_lead_is_the_name(G):
    s = _subs(G, [("zzy", "2022"), ("zzy", "2024"), ("zzy", "2021"), ("zzt", "Prices"),
                  ("zzk", "Consumer price index, all items")])
    assert s["zzy"] == "" and s["zzt"] == "", s
    assert s["zzk"] == "Consumer price index, all items", "control: a real subtitle survives"
    rec = G.build_record("zzy", {"name": "Test Statistics Office — Population Tables", "license_id": "x"},
                         {}, {"n_series": 3, "auto_subtitle": s["zzy"]}, None)
    assert rec["desc_short"] == "Test Statistics Office — Population Tables"
