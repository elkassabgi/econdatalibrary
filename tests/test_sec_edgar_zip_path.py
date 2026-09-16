"""SEC serves new dataset releases from a second directory; the fetcher must follow the page's own link.

Measured 2026-09-16 against www.sec.gov with the User-Agent the source requires:

    insider 2026q2_form345.zip   /files/structureddata/data/...          -> HTTP 404
    insider 2026q2_form345.zip   /files/datastandardsinnovation/data/... -> HTTP 200
    insider 2026q1_form345.zip   /files/structureddata/data/...          -> HTTP 200
    insider 2026q1_form345.zip   /files/datastandardsinnovation/data/... -> HTTP 404

The insider page links 2026q2 under the new directory and every older key under the old one; the 13F
page does the same (1 of 54 links new, its newest window). The fetcher composed the old path for every
key, so each new release returned 404 on every run - recorded as a transient failure, which keeps the
source `partial`, which fails the cloud health gate. The page already carries the right URL.
"""
from updater.strategies.fetchers import sec_edgar as S

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


def test_protocol_relative_and_absolute_sec_links_both_resolve():
    page = ('<a href="//www.sec.gov' + NEW + '/insider-transactions-data-sets/2026q4_form345.zip">a</a>'
            '<a href="' + HOST + OLD + '/insider-transactions-data-sets/2025q3_form345.zip">b</a>')
    urls = S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"])
    assert urls["2026q4"] == HOST + NEW + "/insider-transactions-data-sets/2026q4_form345.zip"
    assert urls["2025q3"] == HOST + OLD + "/insider-transactions-data-sets/2025q3_form345.zip"


def test_a_key_that_is_not_a_real_dataset_key_is_ignored():
    page = '<a href="' + NEW + '/insider-transactions-data-sets/latest_form345.zip">not a key</a>'
    assert S._zip_urls_on_page(page, S.PRODUCTS["edgar_insider"]) == {}


def test_published_keys_returns_the_keys_oldest_first_and_their_links(monkeypatch):
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: INSIDER_PAGE.encode())
    keys, urls = S._published_keys(S.PRODUCTS["edgar_insider"], None)
    assert keys == ["2025q4", "2026q1", "2026q2"]
    assert set(urls) == {"2025q4", "2026q1", "2026q2"}
    assert urls["2026q2"].startswith(HOST + NEW)


def test_current_vintage_still_hashes_the_published_keys(monkeypatch):
    """The other caller of _published_keys takes only the keys; the new return must not break it."""
    monkeypatch.setattr(S, "_http_get", lambda *a, **k: (INSIDER_PAGE + THIRTEENF_PAGE).encode())
    signature = S.current_vintage(None)
    assert isinstance(signature, str) and signature.startswith("cat:")


def test_the_download_uses_the_link_it_is_given_and_the_template_only_as_a_fallback(monkeypatch):
    seen = []

    def fake_get(url, **kw):
        seen.append(url)
        raise S.TransientError("stop here: the URL is what this test measures")

    monkeypatch.setattr(S, "_http_get", fake_get)
    cfg = S.PRODUCTS["edgar_insider"]
    page_url = HOST + NEW + "/insider-transactions-data-sets/2026q2_form345.zip"

    for given, expected in ((page_url, page_url), (None, cfg["zip_url"].format(key="2026q2"))):
        try:
            S._fetch_key(cfg, "2026q2", None, None, url=given)
        except S.TransientError:
            pass
        assert seen[-1] == expected
