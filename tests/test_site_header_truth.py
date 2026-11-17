"""Every page of the econ site carries a TRUE header: a real "Website updated" date, no stale
"Data updated" claim, no unsubstituted placeholder, and no "Under Construction" band.

WHY THIS EXISTS. On 2026-11-17 the live site showed three header defects at once:
  * /download, /status and /mcp printed the literal "Website updated: __SITE_UPDATED__". The
    status strip had been copied into the hand-maintained pages with its placeholder, and only
    gen_site.py's _write() substitutes it, for generated pages.
  * every page printed "Data updated: August 23, 2026" while sources refresh daily. The value was
    /v1/stats `as_of`, the date of the series count, not of any data change.
  * the homepage carried an "Under Construction" band added in July.
The same copy also carried fdate's date regex with doubled backslashes, so on those pages a
date-only stamp was parsed as UTC midnight and shown one day early in the Americas.

`catalog/site/` is what `wrangler pages deploy catalog/site` publishes, so these checks run on
the served bytes, not on a draft. Each predicate has a control below that shows it rejects a
planted positive (R900: a check that cannot fail is not a check).
"""
from __future__ import annotations

import glob
import importlib.util
import os
import re
from datetime import date

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = os.path.join(ROOT, "catalog", "site")
GEN = os.path.join(ROOT, "catalog", "gen_site.py")

# An unsubstituted build placeholder: __SITE_UPDATED__, __GEN__, __INSTITUTIONS_BLOCK__ ...
# Upper case only, so JavaScript's __proto__ and Python dunders in code samples do not match;
# and standing alone, so a series-key separator such as WS_CBS_PUB__Q__US (bis.html) does not.
PLACEHOLDER = re.compile(r"(?<![A-Za-z0-9])__[A-Z][A-Z0-9_]*__(?![A-Za-z0-9])")
BAND = re.compile(r"under\s+construction", re.I)
STALE_FIELD = ("Data updated", 'id="sb-data"', "tago(")
ISO = re.compile(r"^\d{4}-\d{2}-\d{2}$")
STRIP_START = '  var API="https://econdl-api.elkassabgi.workers.dev";'
STRIP_END = "})();"
BAR_START = '<div class="status-bar" id="status-bar">'
BAR_END = "</div></div>"


@pytest.fixture(scope="module")
def G():
    spec = importlib.util.spec_from_file_location("gen_site_header_truth", GEN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read(path: str) -> str:
    with open(path, encoding="utf-8", newline="") as fh:
        return fh.read().replace("\r\n", "\n")


def _pages() -> dict:
    return {os.path.basename(p): _read(p) for p in sorted(glob.glob(os.path.join(SITE, "*.html")))}


def _segment(text: str, start: str, end: str) -> str:
    i = text.index(start)
    return text[i:text.index(end, i) + len(end)]


def _canonical(G) -> tuple:
    """The strip as the generator writes it (HEAD is a str.format template: braces are doubled)."""
    head = G.HEAD.replace("{{", "{").replace("}}", "}")
    return _segment(head, BAR_START, BAR_END), _segment(head, STRIP_START, STRIP_END)


def header_defects(name: str, html: str, canonical: tuple, G) -> list:
    """Every reason this page's header is not true. Empty list = clean."""
    out = []
    for m in PLACEHOLDER.finditer(html):
        out.append(f"{name}: unsubstituted placeholder {m.group(0)}")
    if BAND.search(html):
        out.append(f"{name}: 'Under Construction' band")
    for s in STALE_FIELD:
        if s in html:
            out.append(f"{name}: stale status field {s!r}")
    stamps = [m.group(2) for m in G._SITE_STAMP_RE.finditer(html)]
    if len(stamps) != 1:
        out.append(f"{name}: {len(stamps)} 'Website updated' stamps, expected 1")
    elif not ISO.match(stamps[0]) or not (date(2026, 1, 1) <= date.fromisoformat(stamps[0]) <= date.today()):
        out.append(f"{name}: 'Website updated' stamp {stamps[0]!r} is not a real past date")
    try:
        bar = _segment(html, BAR_START, BAR_END)
        strip = G._SITE_STAMP_RE.sub(lambda m: m.group(1) + "__SITE_UPDATED__" + m.group(3),
                                     _segment(html, STRIP_START, STRIP_END))
        if (bar, strip) != canonical:
            out.append(f"{name}: status strip differs from the generator's")
    except ValueError:
        out.append(f"{name}: status strip not found")
    return out


def test_every_served_page_has_a_true_header(G):
    pages = _pages()
    assert len(pages) >= 300, f"only {len(pages)} pages found under catalog/site: the check did not run"
    canonical = _canonical(G)
    checked, bad = 0, []
    for name, html in pages.items():
        bad += header_defects(name, html, canonical, G)
        checked += 1
    assert checked == len(glob.glob(os.path.join(SITE, "*.html")))
    assert not bad, "\n".join(bad[:40])


def test_homepage_keeps_its_census_date(G):
    """as_of is still the right value for the homepage's 'counts as of' note: only the strip lost it."""
    index = _pages()["index.html"]
    assert 'document.getElementById("live-asof").textContent = d.as_of' in index


def test_generator_template_is_clean(G):
    gen = _read(GEN)
    assert not BAND.search(G.HEAD) and "<strong>Under Construction</strong>" not in gen
    for s in STALE_FIELD:
        assert s not in G.HEAD, s
    assert "fdate('__SITE_UPDATED__')" in G.HEAD
    assert "Math.floor(Date.now()/3600000)" in G.HEAD, "the hourly cache bucket (R709) must stay"


def test_full_build_stamps_the_hand_pages(G):
    """main() is not run here (it needs the local catalogue), so pin that the full build calls the pass."""
    import inspect
    src = inspect.getsource(G.main)
    assert src.count("stamp_hand_pages()") == 1
    assert src.index("stamp_hand_pages()") < src.index("for name, changefreq in SITEMAP_EXTRA")


def test_set_site_stamp_needs_exactly_one_stamp(G):
    one = "x.textContent='Website updated: '+fdate('__SITE_UPDATED__');"
    assert G.set_site_stamp(one, "2026-11-17") == one.replace("__SITE_UPDATED__", "2026-11-17")
    with pytest.raises(SystemExit):
        G.set_site_stamp("no strip here", "2026-11-17")
    with pytest.raises(SystemExit):
        G.set_site_stamp(one + one, "2026-11-17")


def test_stamp_hand_pages_is_local_idempotent_and_keeps_line_endings(G, tmp_path, monkeypatch):
    monkeypatch.setattr(G, "_LASTMOD", {})
    monkeypatch.setattr(G, "_LASTMOD_PREV", {})
    page = "<p>a</p>\r\n  x.textContent='Website updated: '+fdate('__SITE_UPDATED__');\r\n<p>b</p>\r\n"
    for name in ("download.html", "status.html"):
        (tmp_path / name).write_bytes(page.encode("utf-8"))
    assert G.stamp_hand_pages(str(tmp_path), "2026-11-17") == ["download.html", "status.html"]
    got = (tmp_path / "download.html").read_bytes()
    assert got == page.replace("__SITE_UPDATED__", "2026-11-17").encode("utf-8")
    digest = G._LASTMOD["download.html"]["sha256"]
    # a later build on another day rewrites only the date and leaves the fingerprint alone
    assert G.stamp_hand_pages(str(tmp_path), "2026-11-18") == ["download.html", "status.html"]
    assert G._LASTMOD["download.html"]["sha256"] == digest
    assert G.stamp_hand_pages(str(tmp_path), "2026-11-18") == []
    (tmp_path / "status.html").write_bytes(b"<p>strip removed by a hand edit</p>")
    with pytest.raises(SystemExit):
        G.stamp_hand_pages(str(tmp_path), "2026-11-18")


def test_controls_each_predicate_rejects_a_planted_defect(G):
    canonical = _canonical(G)
    clean = _pages()["download.html"]
    assert header_defects("download.html", clean, canonical, G) == []
    planted = {
        "placeholder": clean.replace("fdate('" + G._SITE_STAMP_RE.search(clean).group(2) + "')",
                                     "fdate('__SITE_UPDATED__')"),
        "other placeholder": clean.replace("</body>", "__GEN__</body>"),
        "band": clean.replace("</body>", "<div>Under Construction</div></body>"),
        "data updated": clean.replace("</body>", "<span id=\"sb-data\">Data updated: x</span></body>"),
        "future stamp": G._SITE_STAMP_RE.sub(lambda m: m.group(1) + "2999-01-01" + m.group(3), clean),
        "double-backslash regex": clean.replace(r"var m=/^(\d{4})", r"var m=/^(\\d{4})"),
        "strip missing": clean.replace(STRIP_START, "  var API2=1;"),
    }
    for label, html in planted.items():
        assert html != clean, label
        assert header_defects("download.html", html, canonical, G), f"control '{label}' was not rejected"
    assert not PLACEHOLDER.search("obj.__proto__ and __init__ and WS_CBS_PUB__Q__US.parquet")
    assert PLACEHOLDER.search("fdate('__SITE_UPDATED__')") and PLACEHOLDER.search("<b>__GEN__</b>")
