"""Status mail for the econ jobs (daily digest, billing guard, self-host watch).

One path for all three, so they cannot drift apart:

  1. Cloudflare Email Service, REST API, when CLOUDFLARE_EMAIL_TOKEN (permission "Email Sending:
     Edit") and CLOUDFLARE_ACCOUNT_ID are both set. Sent from noreply@mail.hfdatalibrary.com, the
     subdomain the hf site's account mail already uses; replies go to the old sender address.
  2. Resend, exactly as before, when Cloudflare is not configured or does not report the mail
     delivered or queued for the recipient.

Settings come from the environment only. A test or a desktop shell that does not export the token
can therefore never send through Cloudflare by accident.

`send_status_mail` never raises: in all three jobs the red workflow is the second delivery path,
and a mail fault must not change a run's result. It returns which path carried the mail -
"cloudflare", "resend", "skipped" (nothing configured) or "failed". A read that times out after
Cloudflare accepted a mail makes Resend send it again: a duplicate status mail, never a lost one.
The token is never printed.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

CF_FROM = "noreply@mail.hfdatalibrary.com"
CF_REPLY_TO = "noreply@hfdatalibrary.com"
TIMEOUT_S = 30


def _post(url: str, payload: dict, token: str, user_agent: str):
    """(HTTP status, parsed JSON body or None). Raises only on a network fault."""
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json",
                 # Both APIs sit behind Cloudflare bot protection, which 1010-blocks urllib's
                 # default signature - identify honestly.
                 "User-Agent": user_agent},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            raw, status = resp.read(), resp.status
    except urllib.error.HTTPError as e:
        try:
            raw = e.read()
        except Exception:  # noqa: BLE001
            raw = b""
        status = e.code
    try:
        body = json.loads(raw) if raw else None
    except ValueError:
        body = None
    return status, body


def _cloudflare(subject: str, text: str, html: str | None, to: str, user_agent: str, log) -> bool:
    token = os.environ.get("CLOUDFLARE_EMAIL_TOKEN", "").strip()
    account = os.environ.get("CLOUDFLARE_ACCOUNT_ID", "").strip()
    if not token or not account:
        return False
    payload = {"from": CF_FROM, "reply_to": CF_REPLY_TO, "to": [to], "subject": subject, "text": text}
    if html:
        payload["html"] = html
    try:
        status, body = _post(f"https://api.cloudflare.com/client/v4/accounts/{account}/email/sending/send",
                             payload, token, user_agent)
    except Exception as e:  # noqa: BLE001 - network fault: fall back
        log(f"Cloudflare mail: {type(e).__name__} - falling back to Resend")
        return False
    body = body if isinstance(body, dict) else {}
    result = body.get("result") if isinstance(body.get("result"), dict) else {}
    accepted = [a for k in ("delivered", "queued") if isinstance(result.get(k), list) for a in result[k]]
    if 200 <= status < 300 and body.get("success") is True and to in accepted:
        log(f"mail sent through Cloudflare: HTTP {status}")
        return True
    errs = body.get("errors")
    codes = [e.get("code") for e in (errs if isinstance(errs, list) else []) if isinstance(e, dict)]
    log(f"Cloudflare mail refused: HTTP {status} errors={codes} - falling back to Resend")
    return False


def send_status_mail(subject: str, text: str, html: str | None = None, *, sender: str, to: str,
                     user_agent: str, log=print) -> str:
    """Send one status mail. `sender` is the Resend sender ("Name <address>"). Never raises."""
    try:
        if _cloudflare(subject, text, html, to, user_agent, log):
            return "cloudflare"
    except Exception as e:  # noqa: BLE001
        log(f"Cloudflare mail path: {type(e).__name__} - falling back to Resend")
    key = os.environ.get("RESEND_API_KEY", "").strip()
    if not key:
        log("no mail service configured (CLOUDFLARE_EMAIL_TOKEN and RESEND_API_KEY unset) - email skipped")
        return "skipped"
    payload = {"from": sender, "to": [to], "subject": subject, "text": text}
    if html:
        payload["html"] = html
    try:
        status, body = _post("https://api.resend.com/emails", payload, key, user_agent)
    except Exception as e:  # noqa: BLE001
        log(f"Resend mail failed: {type(e).__name__}")
        return "failed"
    if 200 <= status < 300:
        log(f"mail sent through Resend: HTTP {status}")
        return "resend"
    # Resend's error JSON carries no secrets; its message makes a bad key or domain diagnosable.
    detail = json.dumps(body)[:300] if body is not None else "(no body)"
    log(f"Resend mail failed: HTTP {status} - {detail}")
    return "failed"
