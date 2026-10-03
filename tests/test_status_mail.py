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
    calls, _ = net
    assert send() == "resend"
    assert hosts(calls) == [RESEND], "with no token the mail must still go out through Resend"
    assert calls[0][1]["from"] == "Econ Data Library <noreply@hfdatalibrary.com>"


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


@pytest.mark.parametrize("module,call", [
    ("tools.billing_guard", lambda m: m.send_alert("s", "b")),
    ("tools.selfhost.watch_edge", lambda m: m.send_alert("s", "b")),
])
def test_both_alert_senders_go_through_the_shared_path(net, monkeypatch, module, call):
    """A sender that kept its own Resend call would never reach Cloudflare."""
    import importlib
    calls, replies = net
    monkeypatch.setenv("CLOUDFLARE_EMAIL_TOKEN", "cf_test")
    replies[CF] = delivered()
    call(importlib.import_module(module))
    assert hosts(calls) == [CF]


def test_no_job_calls_resend_directly_any_more():
    """Source-shape guard: the three jobs send only through core/status_mail.py."""
    for rel in ("updater/send_digest.py", "tools/billing_guard.py", "tools/selfhost/watch_edge.py"):
        src = open(os.path.join(ROOT, rel), encoding="utf-8").read()
        assert "api.resend.com" not in src, f"{rel} still calls Resend itself"
        assert "send_status_mail" in src, f"{rel} does not use the shared mail path"
