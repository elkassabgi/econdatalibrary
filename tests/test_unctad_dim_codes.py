"""dim_codes reads a dimension's code list from the CREDENTIALED user API, by one path only.

Until 2026-10-13 dim_codes read the keyless
`unctadstat-api.unctad.org/datamart-api/{DS}/{version}/{DimTable}`. That address began to answer
403 "Blocked", while the same table answers 200 on the host the Facts pull already uses. Every
chunked pull asks for a dimension before its first chunk, so a blocked dim_codes stops every
over-cap UNCTAD dataset.

What these tests pin, each with the mutant it stops (review of 2026-10-13):
  * the request goes to the Facts host, version-pinned, with both credential headers;
  * a refusal is ONE request - no second try at the keyless address (a fallback hides which
    address answered);
  * the credentials are never in the URL (a failed request's text is rendered into the run note);
  * the User-Agent header is sent;
  * the publisher's order is kept, and only rows with no code are dropped (0 is a code);
  * the gateway's transient statuses are retried, and 403 / 404 are final at once.
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
    """Stands in for requests.get: answers from a queue (the last answer repeats)."""

    def __init__(self, *answers):
        self.answers, self.seen = list(answers), []

    def __call__(self, url, headers=None, timeout=None, **kw):
        self.seen.append((url, dict(headers or {})))
        return self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]


OK = _Resp(200, {"value": [{"Code": 2005}, {"Code": None}, {"Code": 0},
                           {"Label": "a row with no code"}, {"Code": 1990}]})


@pytest.fixture
def api(monkeypatch):
    monkeypatch.setattr(j, "creds", lambda: (ID, SECRET))
    monkeypatch.setattr(j.time, "sleep", lambda s: None)

    def install(*answers):
        a = _Api(*answers)
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
    assert parse_qs(parts.query)["$orderby"] == ["Order"]
    assert headers["ClientId"] == ID and headers["ClientSecret"] == SECRET


def test_the_user_agent_is_sent(api):
    a = api(OK)
    j.dim_codes("US.X", 1, "Years")
    assert a.seen[0][1]["User-Agent"] == j.UA["User-Agent"]


def test_the_credentials_are_never_in_the_url(api):
    a = api(OK)
    j.dim_codes("US.X", 1, "Years")
    assert all(ID not in url and SECRET not in url for url, _ in a.seen)


def test_a_refusal_is_one_request_and_never_the_keyless_address(api):
    # 403 is the publisher's block. No retry, no second address, and the error is raised: an
    # empty list would make the caller re-raise the size cap, which reads as a layout problem.
    a = api(_Resp(403, "Blocked"))
    with pytest.raises(requests.HTTPError):
        j.dim_codes("US.X", 1, "Years")
    assert len(a.seen) == 1
    assert all("datamart-api" not in url for url, _ in a.seen)


def test_a_missing_table_is_final_at_once(api):
    a = api(_Resp(404))
    with pytest.raises(requests.HTTPError):
        j.dim_codes("US.X", 1, "NoSuchTable")
    assert len(a.seen) == 1


# Written out, NOT read from j.DIM_RETRY: a test that iterates the constant under test still passes
# when a status is removed from it. These are the statuses facts_csv retries on this gateway.
@pytest.mark.parametrize("status", [401, 429, 500, 502, 503, 504])
def test_a_transient_status_is_asked_again_at_the_same_address(api, status):
    a = api(_Resp(status), _Resp(status), OK)
    assert j.dim_codes("US.X", 1, "Years") == [2005, 0, 1990]
    assert len(a.seen) == 3
    assert len({url for url, _ in a.seen}) == 1


def test_a_status_that_stays_transient_is_raised_after_four_requests(api):
    a = api(_Resp(503))
    with pytest.raises(requests.HTTPError):
        j.dim_codes("US.X", 1, "Years")
    assert len(a.seen) == 4


def test_the_publisher_order_is_kept_and_only_codeless_rows_are_dropped(api):
    # NOT sorted. 0 is a code: only None (and a row with no Code at all) is dropped.
    api(OK)
    assert j.dim_codes("US.X", 1, "Years") == [2005, 0, 1990]


def test_a_bare_list_body_is_read_the_same_way(api):
    api(_Resp(200, [{"Code": "B"}, {"Code": "A"}]))
    assert j.dim_codes("US.X", 1, "Economies") == ["B", "A"]
