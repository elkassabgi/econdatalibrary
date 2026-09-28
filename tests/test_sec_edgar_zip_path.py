"""SEC serves new dataset releases from a second directory; the fetcher must follow the page's own link.

Measured 2026-09-16 against www.sec.gov with the User-Agent the source requires:

    insider 2026q2_form345.zip   /files/structureddata/data/...          -> HTTP 404
    insider 2026q2_form345.zip   /files/datastandardsinnovation/data/... -> HTTP 200
    insider 2026q1_form345.zip   /files/structureddata/data/...          -> HTTP 200
    insider 2026q1_form345.zip   /files/datastandardsinnovation/data/... -> HTTP 404

The insider page links 2026q2 under the new directory and its 81 older keys under the old one; the 13F
page links its newest window (01jun2026-31aug2026) under the new directory and its 53 older ones under
the old. The fetcher composed the old path for every key, so each new release returned 404 on every run
- recorded as a transient failure, which keeps the source `partial` and fails the cloud health check.

Composing a path is therefore never allowed here: a key whose link cannot be parsed means the page
layout moved again, and guessing produces exactly the silent 404 loop this change removes. The keys and
their URLs come from one parse, and a published key with no parseable sec.gov link fails the whole page.
"""
import re

import pytest

from updater.strategies.fetchers import sec_edgar_13f as S

NEW = "/files/datastandardsinnovation/data"
OLD = "/files/structureddata/data"
HOST = "https://www.sec.gov"

INSIDER_PAGE = (
    '<td><a href="' + NEW + '/insider-transactions-data-sets/2026q2_form345.zip" download>2026 Q2 345</a></td>'
    '<td><a href="' + OLD + '/insider-transactions-data-sets/2026q1_form345.zip" download>2026 Q1 345</a></td>'
    '<td><a href="' + OLD + '/insider-transactions-data-sets/2025q4_form345.zip" download>2025 Q4 345</a></td>'
)
THIRTEENF_PAGE = (
    '<a href="' + NEW + '/form-13f-data-sets/01jun2026-31aug2026_form13f.zip">newest</a>'
    '<a href="' + OLD + '/form-13f-data-sets/01mar2026-31may2026_form13f.zip">older</a>'
)


def test_each_key_maps_to_the_directory_the_page_links():
    urls = S._zip_urls_on_page(INSIDER_PAGE, S.PRODUCTS["edgar_insider"])
    assert urls["2026q2"] == HOST + NEW + "/insider-transactions-data-sets/2026q2_form345.zip"
    assert urls["2026q1"] == HOST + OLD + "/insider-transactions-data-sets/2026q1_form345.zip"
    urls13 = S._zip_urls_on_page(THIRTEENF_PAGE, S.PRODUCTS["edgar_13f"])
    assert urls13["01jun2026-31aug2026"] == HOST + NEW + "/form-13f-data-sets/01jun2026-31aug2026_form13f.zip"
    assert urls13["01mar2026-31may2026"] == HOST + OLD + "/form-13f-data-sets/01mar2026-31may2026_form13f.zip"


def test_the_composed_template_is_the_url_that_404s_so_this_is_not_a_no_op():
    """Negative control: page and template disagree for a new release, agree for an old one."""
    cfg = S.PRODUCTS["edgar_insider"]
    urls = S._zip_urls_on_page(INSIDER_PAGE, cfg)
    assert cfg["zip_url"].format(key="2026q2") != urls["2026q2"]
    assert cfg["zip_url"].format(key="2026q1") == urls["2026q1"]


def test_a_link_off_sec_gov_is_never_followed():
    page = '<a href="https://evil.example.com/insider-transactions-data-sets/2026q3_form345.zip">x</a>'
    assert "2026q3" not in S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"])


def test_a_host_that_only_starts_with_sec_gov_is_not_sec_gov():
    page = ('<a href="https://www.sec.gov.evil.com/insider-transactions-data-sets/2026q3_form345.zip">a</a>'
            '<a href="https://www.sec.gov@evil.com/insider-transactions-data-sets/2025q3_form345.zip">b</a>')
    assert S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"]) == {}


def test_the_host_check_tolerates_case_and_port_and_the_scheme_is_normalised_to_https():
    page = ('<a href="HTTPS://WWW.SEC.GOV' + NEW + '/insider-transactions-data-sets/2026q4_form345.zip">a</a>'
            '<a href="https://www.sec.gov:443' + OLD + '/insider-transactions-data-sets/2025q3_form345.zip">b</a>'
            '<a href="http://www.sec.gov' + OLD + '/insider-transactions-data-sets/2025q2_form345.zip">c</a>')
    urls = S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"])
    assert urls["2026q4"] == HOST + NEW + "/insider-transactions-data-sets/2026q4_form345.zip"
    assert urls["2025q3"] == HOST + OLD + "/insider-transactions-data-sets/2025q3_form345.zip"
    assert urls["2025q2"] == HOST + OLD + "/insider-transactions-data-sets/2025q2_form345.zip"


def test_a_protocol_relative_link_resolves_and_a_document_relative_one_is_refused():
    page = ('<a href="//www.sec.gov' + NEW + '/insider-transactions-data-sets/2026q4_form345.zip">a</a>'
            '<a href="../data/insider-transactions-data-sets/2025q1_form345.zip">b</a>')
    urls = S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"])
    assert urls["2026q4"] == HOST + NEW + "/insider-transactions-data-sets/2026q4_form345.zip"
    assert "2025q1" not in urls


def test_the_first_link_for_a_key_wins_when_a_key_is_listed_twice():
    """Page order is newest-first; a migration that lists both directories must not pick the second."""
    page = ('<a href="' + NEW + '/insider-transactions-data-sets/2026q2_form345.zip">new</a>'
            '<a href="' + OLD + '/insider-transactions-data-sets/2026q2_form345.zip">old</a>')
    urls = S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"])
    assert urls["2026q2"] == HOST + NEW + "/insider-transactions-data-sets/2026q2_form345.zip"


def test_a_name_that_is_not_a_dataset_key_is_rejected_by_the_key_rule_not_by_the_pattern():
    cfg = S.PRODUCTS["edgar_insider"]
    page = '<a href="' + NEW + '/insider-transactions-data-sets/latest_form345.zip">not a key</a>'
    assert re.search(cfg["zip_re"], page), "the pattern must match it, or this test proves nothing"
    assert S._valid_key("latest") is False
    assert S._zip_urls_on_page(page, cfg) == {}


def test_published_keys_returns_the_keys_oldest_first_and_their_links(monkeypatch):
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: INSIDER_PAGE.encode())
    keys, urls = S._published_keys(S.PRODUCTS["edgar_insider"], None)
    assert keys == ["2025q4", "2026q1", "2026q2"]
    assert set(urls) == {"2025q4", "2026q1", "2026q2"}
    assert urls["2026q2"].startswith(HOST + NEW)


def test_a_published_key_with_no_parseable_link_fails_the_page_instead_of_guessing(monkeypatch):
    """The review's MAJOR: an unparseable href must not fall back to the path measured to 404."""
    page = (INSIDER_PAGE
            + '<a href=' + NEW + '/insider-transactions-data-sets/2026q3_form345.zip>unquoted</a>')
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: page.encode())
    with pytest.raises(S.TransientError) as e:
        S._published_keys(S.PRODUCTS["edgar_insider"], None)
    msg = str(e.value)
    assert "2026q3" in msg and "no parseable sec.gov link" in msg and "4 dataset key(s)" in msg


def test_current_vintage_still_hashes_the_published_keys(monkeypatch):
    """The other caller of _published_keys takes only the keys; the new return must not break it."""
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: (INSIDER_PAGE + THIRTEENF_PAGE).encode())
    signature = S.current_vintage(None)
    assert isinstance(signature, str) and signature.startswith("cat:")


def test_the_download_requires_a_link_and_uses_the_one_it_is_given(monkeypatch):
    seen = []

    def fake_get(url, **kw):
        seen.append(url)
        raise S.TransientError("stop here: the URL is what this test measures")

    monkeypatch.setattr(S, "_http_get", fake_get)
    cfg = S.PRODUCTS["edgar_insider"]
    page_url = HOST + NEW + "/insider-transactions-data-sets/2026q2_form345.zip"
    try:
        S._fetch_key(cfg, "2026q2", None, None, url=page_url)
    except S.TransientError:
        pass
    assert seen[-1] == page_url
    with pytest.raises(TypeError):
        S._fetch_key(cfg, "2026q2", None, None)          # no composed-path fallback exists


def test_each_product_pattern_has_exactly_one_capture_group():
    """_zip_urls_on_page wraps zip_re in one more group and unpacks (href, key); a second group inside
    zip_re would turn that unpack into a ValueError at run time."""
    for pid, cfg in S.PRODUCTS.items():
        assert re.compile(cfg["zip_re"]).groups == 1, pid


def test_a_malformed_link_does_not_crash_and_is_reported_as_unlinked(monkeypatch):
    """urlsplit raises ValueError on a malformed netloc; remote text must not produce a bare crash."""
    page = INSIDER_PAGE + '<a href="//[/insider-transactions-data-sets/2026q3_form345.zip">broken</a>'
    assert S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"]).get("2026q3") is None
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: page.encode())
    with pytest.raises(S.TransientError) as e:
        S._published_keys(S.PRODUCTS["edgar_insider"], None)
    assert "2026q3" in str(e.value)


def test_update_hands_each_download_the_link_from_that_product_page(monkeypatch):
    """Wires the whole path: without this, dropping url= in update() would pass every helper test."""
    per_product = {
        "edgar_13f": (["01jun2026-31aug2026"],
                      {"01jun2026-31aug2026": HOST + NEW + "/form-13f-data-sets/01jun2026-31aug2026_form13f.zip"}),
        "edgar_insider": (["2026q2"], {"2026q2": HOST + NEW + "/insider-transactions-data-sets/2026q2_form345.zip"}),
    }
    seen = {}

    def fake_fetch(prod_cfg, key, session, tally, cursors=None, *, url):
        seen[key] = url
        return 0

    monkeypatch.setattr(S, "_load_state", lambda: {})
    monkeypatch.setattr(S, "_save_state", lambda st: None)
    monkeypatch.setattr(S, "_keys_on_disk", lambda cfg: set())
    monkeypatch.setattr(S, "_published_keys", lambda cfg, sess: per_product[cfg["out_dir"]])
    monkeypatch.setattr(S, "_fetch_key", fake_fetch)

    S.update(None, None)

    assert seen == {k: v for _, (_, m) in per_product.items() for k, v in m.items()}
