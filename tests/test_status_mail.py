"""core/status_mail.py: the econ jobs' status mail goes through Cloudflare Email Service when its token is
set, and through Resend exactly as before otherwise. No network: urllib.request.urlopen is replaced.
Each test names the wrong result it guards against."""
from __future__ import annotations

import io
import json
import os
import sys
import urllib.error
import urllib.request

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import status_mail as sm  # noqa: E402

TO = "admin@hfdatalibrary.com"
CF = "api.cloudflare.com"
RESEND = "api.resend.com"


class Resp:
    def __init__(self, status, body):
        self.status, self._raw = status, (body if isinstance(body, bytes) else json.dumps(body).encode())

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def net(monkeypatch):
    """calls: list of (url, payload, headers, timeout); replies: host -> Resp | exception."""
    calls, replies = [], {}

    def urlopen(req, timeout=None):
        url = req.full_url
        calls.append((url, json.loads(req.data), dict(req.header_items()), timeout))
        for host, reply in replies.items():
            if host in url:
                if isinstance(reply, BaseException):
                    raise reply
                if reply.status >= 400:
                    raise urllib.error.HTTPError(url, reply.status, "err", {}, io.BytesIO(reply.read()))
                return reply
        return Resp(200, {"id": "resend-1"})
    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    monkeypatch.setenv("RESEND_API_KEY", "re_test")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "acct")
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "")
    return calls, replies


def hosts(calls):
    return [CF if CF in c[0] else RESEND if RESEND in c[0] else c[0] for c in calls]


def send(html=None, log=None):
    return sm.send_status_mail("subj", "text body", html, sender="Econ Data Library <noreply@hfdatalibrary.com>",
                               to=TO, user_agent="ua-test/1.0", log=log or (lambda m: None))


def delivered():
    return Resp(200, {"success": True, "errors": [], "result": {"delivered": [TO], "queued": [], "permanent_bounces": []}})


def test_no_cloudflare_token_uses_resend_as_before(net):
    """The path production uses until the token exists: the request must be exactly the old one."""
    calls, _ = net
    assert send(html="<p>h</p>") == "resend"
    assert hosts(calls) == [RESEND], "with no token the mail must still go out through Resend"
    url, payload, headers, timeout = calls[0]
    assert payload == {"from": "Econ Data Library <noreply@hfdatalibrary.com>", "to": [TO], "subject": "subj",
                       "text": "text body", "html": "<p>h</p>"}, payload
    assert headers["Authorization"] == "Bearer re_test", "the Resend key was not sent"
    assert headers["User-agent"] == "ua-test/1.0", "urllib's default UA is 1010-blocked by Cloudflare"
    assert headers["Content-type"] == "application/json"
    assert timeout and timeout <= 60


def test_stray_whitespace_in_a_secret_is_not_sent(net, monkeypatch):
    """A pasted secret often carries a newline; sent as is, the header is malformed or refused."""
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", " cf_test\n")
    replies[CF] = delivered()
    send()
    assert calls[0][2]["Authorization"] == "Bearer cf_test"
    calls.clear()
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "")
    monkeypatch.setenv("RESEND_API_KEY", "re_test\r\n")
    send()
    assert calls[0][2]["Authorization"] == "Bearer re_test"


def test_a_resend_redirect_is_not_a_sent_mail(net):
    """Only a 2xx means Resend took the mail; a 3xx answer must read as failed."""
    calls, replies = net
    replies[RESEND] = Resp(302, {"location": "elsewhere"})
    assert send() == "failed"


def test_the_recipient_match_ignores_case_and_spaces(net, monkeypatch):
    """Control: an exact match sent every mail twice when Cloudflare echoed the address in another case."""
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    replies[CF] = Resp(200, {"success": True, "result": {"delivered": [" Admin@HFDataLibrary.com "], "queued": []}})
    assert send() == "cloudflare" and hosts(calls) == [CF]


def test_nothing_configured_is_skipped_not_failed(net, monkeypatch):
    calls, _ = net
    monkeypatch.setenv("RESEND_API_KEY", "")
    assert send() == "skipped" and calls == []


def test_an_accepted_cloudflare_mail_is_sent_once(net, monkeypatch):
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    replies[CF] = delivered()
    assert send(html="<p>h</p>") == "cloudflare"
    assert hosts(calls) == [CF], "an accepted mail was sent through Resend as well"
    url, payload, headers, timeout = calls[0]
    assert url.endswith("/accounts/acct/email/sending/send")
    assert headers["Authorization"] == "Bearer cf_test" and headers["User-agent"] == "ua-test/1.0"
    assert payload["from"] == "noreply@mail.hfdatalibrary.com" and payload["to"] == [TO]
    assert payload["subject"] == "subj" and headers["Content-type"] == "application/json"
    assert payload["reply_to"] == "noreply@hfdatalibrary.com"
    assert payload["text"] == "text body" and payload["html"] == "<p>h</p>"
    assert timeout and timeout <= 60, "a hung call would hold the job"


def test_a_queued_cloudflare_mail_counts_as_accepted(net, monkeypatch):
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    replies[CF] = Resp(200, {"success": True, "result": {"delivered": [], "queued": [TO]}})
    assert send() == "cloudflare" and hosts(calls) == [CF]


@pytest.mark.parametrize("reply", [
    Resp(400, {"success": False, "errors": [{"code": 10001, "message": "invalid"}], "result": None}),
    Resp(200, {"success": False, "errors": [], "result": None}),
    Resp(200, {"success": True, "result": {"delivered": [], "queued": [], "permanent_bounces": [TO]}}),
    Resp(200, {"success": True, "result": {"delivered": ["someone@else.example"], "queued": []}}),
    Resp(500, {"success": True, "result": {"delivered": [TO]}}),
    Resp(200, {"success": "yes", "result": {"delivered": [TO]}}),
    Resp(200, b"not json"),
    Resp(200, b""),
    Resp(200, []),
    Resp(200, "text"),
    Resp(200, {"success": True, "result": "x"}),
    Resp(200, {"success": True, "result": {"delivered": TO}}),
    Resp(200, {"success": False, "errors": 5, "result": None}),
    urllib.error.URLError("down"),
    TimeoutError("slow"),
], ids=["http-400", "success-false", "bounced", "other-recipient", "http-500-says-delivered", "success-not-true",
        "not-json", "empty", "body-list", "body-string", "result-string", "delivered-string", "errors-int",
        "network", "timeout"])
def test_anything_short_of_acceptance_falls_back_to_resend(net, monkeypatch, reply):
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    replies[CF] = reply
    assert send() == "resend"
    assert hosts(calls) == [CF, RESEND], "a mail Cloudflare did not accept was dropped"


def test_a_token_without_the_account_id_does_not_call_cloudflare(net, monkeypatch):
    calls, _ = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", "")
    assert send() == "resend" and hosts(calls) == [RESEND]


def test_resend_refusals_and_faults_are_reported_never_raised(net):
    calls, replies = net
    replies[RESEND] = Resp(403, {"message": "domain not verified"})
    seen = []
    assert send(log=seen.append) == "failed"
    assert any("403" in m and "domain not verified" in m for m in seen), "the refusal reason was not logged"
    replies[RESEND] = urllib.error.URLError("down")
    assert send() == "failed"


def test_the_tokens_are_never_logged(net, monkeypatch):
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_secret_value")
    monkeypatch.setenv("RESEND_API_KEY", "re_secret_value")
    seen = []
    replies[CF] = Resp(400, {"success": False, "errors": [{"code": 10000, "message": "Authentication error"}]})
    replies[RESEND] = Resp(401, {"message": "bad key"})
    send(log=seen.append)
    replies[CF] = urllib.error.URLError("down")
    send(log=seen.append)
    text = "\n".join(seen)
    assert "cf_secret_value" not in text and "re_secret_value" not in text


ALERTS = [("tools.billing_guard", "econdatalibrary-billing-guard/1.0"),
          ("tools.selfhost.watch_edge", "econdatalibrary-selfhost-watch/1.0")]


@pytest.mark.parametrize("module,ua", ALERTS, ids=[a[0] for a in ALERTS])
@pytest.mark.parametrize("token", ["cf_test", ""], ids=["cloudflare", "resend"])
def test_both_alert_senders_go_through_the_shared_path(net, monkeypatch, module, ua, token):
    """A sender that kept its own Resend call would never reach Cloudflare; both paths keep the
    sender's recipient, its User-Agent, and a text-only body."""
    import importlib
    calls, replies = net
    monkeypatch.setenv("DIGEST_TO", "")
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", token)
    replies[CF] = delivered()
    importlib.import_module(module).send_alert("alert-subj", "alert-body")
    assert hosts(calls) == ([CF] if token else [RESEND])
    url, payload, headers, _ = calls[0]
    assert payload["to"] == [TO] and payload["subject"] == "alert-subj" and payload["text"] == "alert-body"
    assert "html" not in payload, "an alert is text only"
    assert headers["User-agent"] == ua
    if not token:
        assert payload["from"] == "Econ Data Library <noreply@hfdatalibrary.com>"


def test_no_job_calls_resend_directly_any_more():
    """Source-shape guard: the three jobs send only through core/status_mail.py, and the digest
    passes its HTML (the body that is actually read), its sender and its recipient."""
    for rel in ("updater/send_digest.py", "tools/billing_guard.py", "tools/selfhost/watch_edge.py"):
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        assert "api.resend.com" not in src, f"{rel} still calls Resend itself"
        assert "send_status_mail" in src, f"{rel} does not use the shared mail path"
    digest = open(os.path.join(ROOT, "updater", "send_digest.py"), encoding="utf-8").read()
    assert "send_status_mail(subject, body, html_doc, sender=FROM, to=TO," in digest
    assert 'user_agent="econdatalibrary-digest/1.0"' in digest


def test_a_missing_mail_module_does_not_raise_from_an_alert(monkeypatch):
    """Control: an ImportError inside send_alert would turn a green guard run red."""
    import builtins
    import importlib
    real = builtins.__import__

    def refuse(name, *a, **k):
        if name == "core.status_mail":
            raise ImportError("planted")
        return real(name, *a, **k)
    for module, _ua in ALERTS:
        m = importlib.import_module(module)
        monkeypatch.setattr(builtins, "__import__", refuse)
        m.send_alert("s", "b")                       # must return, not raise
        monkeypatch.setattr(builtins, "__import__", real)
