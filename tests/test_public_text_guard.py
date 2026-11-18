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
])
def test_guard_negative_controls(text):
    spec = importlib.util.spec_from_file_location("ptg", os.path.join(ROOT, "catalog", "public_text_guard.py"))
    g = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(g)
    assert g.scan_text("t", text) == [], text


@pytest.mark.parametrize("text,label", [
    ("parsers live in jobs/_sdmx_common.py", "repo path jobs/"),
    ("written to data/clean_full/istat/", "store path data/clean_full"),
    ("state under data/_aqueduct", "state dir _aqueduct"),
    ("see orchestrate.py:1782", "source line reference"),
    ("lines 697-700", "line-number reference"),
    ("run it with --resume", "CLI flag"),
    ("marks _sliced_done.json", "internal sidecar _x.json"),
    ("a <DATASET> file", "<PLACEHOLDER>"),
    ("broken � dash", "U+FFFD replacement char"),
    ("Statistik Ã–sterreich", "mojibake Ã"),
    ("â€” em dash", "mojibake â€"),
    ("resumes from checkpoint", "checkpoint"),
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
