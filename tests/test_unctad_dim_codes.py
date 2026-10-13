"""dim_codes reads a dimension's code list from the CREDENTIALED user API, by one path only.

Until 2026-10-13 dim_codes read the keyless
`unctadstat-api.unctad.org/datamart-api/{DS}/{version}/{DimTable}`. That address began to answer
403 "Blocked", while the same table answers 200 on the host the Facts pull already uses. Every
chunked pull asks for a dimension before its first chunk, so a blocked dim_codes stops every
over-cap UNCTAD dataset.

What these tests pin (two reviews of 2026-10-13 ran mutants against them):
  * the request goes to the Facts host, version-pinned, with both credential headers, the
    User-Agent, `$orderby=Order`, the culture and a timeout;
  * ONE path: a refusal, retries that run out and a failed connection never go to the keyless
    address (a fallback hides which address answered);
  * the credentials are never in the URL, however they would get there (a failed request's text
    is rendered into the run note) - the stand-in refuses any argument it does not model;
  * the publisher's order is kept, and only rows with no code are dropped (0 is a code);
  * the gateway's transient statuses are retried with the stated waits, four requests at most;
    every other error status is final at once.
"""
import os
import sys
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import jobs.ingest_unctad_ds as j  # noqa: E402

ID, SECRET = "the-client-id", "the-client-secret"


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} for the dimension table", response=self)

    def json(self):
        return self._body


class _Api:
    """Stands in for requests.get: answers from a queue (the last answer repeats). An answer that
    is an exception is raised. It REFUSES any argument it does not model: a stand-in that swallows
    `params=` would let a credential reach the URL unseen."""

    def __init__(self, *answers):
        self.answers, self.seen, self.timeouts = list(answers), [], []

    def __call__(self, url, headers=None, timeout=None, **kw):
        assert not kw, f"requests.get was given arguments this test does not model: {sorted(kw)}"
        self.seen.append((url, dict(headers or {})))
        self.timeouts.append(timeout)
        a = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(a, Exception):
            raise a
        return a


OK = _Resp(200, {"value": [{"Code": 2005}, {"Code": None}, {"Code": 0},
                           {"Label": "a row with no code"}, {"Code": 1990}]})


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(j, "creds", lambda: (ID, SECRET))
    slept = []
    monkeypatch.setattr(j.time, "sleep", slept.append)

    def install(*answers):
        a = _Api(*answers)
        a.slept = slept
        monkeypatch.setattr(j.requests, "get", a)
        return a
    return install


def test_it_asks_the_facts_host_for_the_pinned_version_with_both_credentials(api):
    a = api(OK)
    j.dim_codes("US.TradeServCatByPartner", 10005, "Years")
    (url, headers), = a.seen
    parts = urlsplit(url)
    assert f"{parts.scheme}://{parts.netloc}" == j.FACTS
    assert parts.path == "/US.TradeServCatByPartner/10005/Years"
    q = parse_qs(parts.query)
    assert q["$orderby"] == ["Order"] and q["culture"] == ["en"]
    assert headers["ClientId"] == ID and headers["ClientSecret"] == SECRET
    assert a.timeouts == [120], "a request with no timeout can hold a unit's whole turn"


def test_the_user_agent_is_sent(api):
    a = api(OK)
    j.dim_codes("US.X", 1, "Years")
    assert a.seen[0][1]["User-Agent"] == j.UA["User-Agent"]


def test_the_credentials_are_never_in_the_url(api):
    # The stand-in also refuses params= and every other argument, so no spelling gets past it.
    a = api(OK)
    j.dim_codes("US.X", 1, "Years")
    assert all(ID not in url and SECRET not in url for url, _ in a.seen)


# Written out, NOT read from the module: the statuses that are final at once.
@pytest.mark.parametrize("status", [400, 403, 404])
def test_a_refusal_is_one_request_and_never_the_keyless_address(api, status):
    # 403 is the publisher's block. No retry, no second address, and the error is raised: an
    # empty list would make the caller re-raise the size cap, which reads as a layout problem.
    a = api(_Resp(status, "Blocked"))
    with pytest.raises(requests.HTTPError):
        j.dim_codes("US.X", 1, "Years")
    assert len(a.seen) == 1 and a.slept == []
    assert all("datamart-api" not in url for url, _ in a.seen)


def test_a_failed_connection_is_raised_as_it_is_and_never_sent_to_the_keyless_address(api):
    a = api(requests.ConnectionError("the connection was reset"))
    with pytest.raises(requests.ConnectionError):
        j.dim_codes("US.X", 1, "Years")
    assert len(a.seen) == 1
    assert all("datamart-api" not in url for url, _ in a.seen)


# Written out, NOT read from j.DIM_RETRY: a test that iterates the constant under test still passes
# when a status is removed from it. These are the statuses facts_csv retries on this gateway.
@pytest.mark.parametrize("status", [401, 429, 500, 502, 503, 504])
def test_a_transient_status_is_asked_again_at_the_same_address_after_a_wait(api, status):
    a = api(_Resp(status), _Resp(status), OK)
    assert j.dim_codes("US.X", 1, "Years") == [2005, 0, 1990]
    assert len(a.seen) == 3
    assert len({url for url, _ in a.seen}) == 1
    assert a.slept == [20, 40], "the waits are part of the contract with the publisher"


def test_a_status_that_stays_transient_is_raised_after_four_requests(api):
    a = api(_Resp(503))
    with pytest.raises(requests.HTTPError):
        j.dim_codes("US.X", 1, "Years")
    assert len(a.seen) == 4
    assert a.slept == [20, 40, 60], "three waits, and none after the last request"
    assert all("datamart-api" not in url for url, _ in a.seen)


def test_the_publisher_order_is_kept_and_only_codeless_rows_are_dropped(api):
    # NOT sorted. 0 is a code: only None (and a row with no Code at all) is dropped.
    api(OK)
    assert j.dim_codes("US.X", 1, "Years") == [2005, 0, 1990]


def test_a_bare_list_body_is_read_the_same_way(api):
    api(_Resp(200, [{"Code": "B"}, {"Code": "A"}]))
    assert j.dim_codes("US.X", 1, "Economies") == ["B", "A"]


def test_dim_rows_gives_every_row_whole_in_the_publisher_order_by_the_same_one_request(api):
    # The title tool reads labels from these rows; dim_codes keeps their codes. One request path.
    a = api(OK)
    rows = j.dim_rows("US.X", 3, "Economies")
    assert rows == [{"Code": 2005}, {"Code": None}, {"Code": 0},
                    {"Label": "a row with no code"}, {"Code": 1990}]
    (url, headers), = a.seen
    assert url.startswith(j.FACTS + "/US.X/3/Economies?")
    assert headers["ClientId"] == ID and headers["ClientSecret"] == SECRET


def test_dim_codes_is_dim_rows_and_nothing_else(api, monkeypatch):
    # dim_codes must not have a request of its own: a second path could differ from the first.
    a = api(OK)
    monkeypatch.setattr(j, "dim_rows", lambda ds, v, t: [{"Code": "K"}, {"Code": None}, {"Code": 7}])
    assert j.dim_codes("US.X", 1, "Years") == ["K", 7]
    assert a.seen == []


def test_a_table_the_service_cut_short_is_refused(api):
    # An answer with @odata.nextLink is only a first page. A short code list would mean chunks that
    # are never asked for, and a short label table would leave series untitled, both in silence.
    api(_Resp(200, {"value": [{"Code": 1}, {"Code": 2}],
                    "@odata.nextLink": "https://example.invalid/next"}))
    with pytest.raises(RuntimeError, match="only a first page"):
        j.dim_rows("US.X", 1, "Products")
    with pytest.raises(RuntimeError, match="only a first page"):
        j.dim_codes("US.X", 1, "Products")


def test_a_whole_table_with_other_odata_fields_is_returned(api):
    api(_Resp(200, {"@odata.context": "https://example.invalid/$metadata#Years", "@odata.count": 2,
                    "value": [{"Code": 1}, {"Code": 2}]}))
    assert j.dim_codes("US.X", 1, "Years") == [1, 2]
