"""The billing guard's two-part gate, and the diagnostic that named the wrong half.

`account_analytics()` refuses on `not tok or not acct`. `_account_id()` used to read only
`R2_WRITE_ENDPOINT` and fall back to the repo `.env`, which is gitignored and therefore absent
on a CI runner - and billing-guard.yml passed no endpoint. So the guard was blind in CI for a
reason that had nothing to do with the token, while its message said the token was missing.

Measured: on 2026-09-02, minutes after CF_ANALYTICS_TOKEN was added to the repo secrets
exactly as instructed, run 33669058808 printed "BILLING GUARD BLIND ... CF_ANALYTICS_TOKEN is
not set" - naming the one thing that WAS now set, and sending the person who had just fixed it
back to check it again.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import billing_guard as bg  # noqa: E402

TAG = "ce51d5c7fe3859098751b89bbebeab7a"          # shape only; any 32-hex value
KEYS = ("CLOUDFLARE_ACCOUNT_ID", "R2_WRITE_ENDPOINT")


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    for k in KEYS:
        monkeypatch.delenv(k, raising=False)
    # point the .env fallback at a directory that has none, so a developer's real .env
    # cannot make these pass locally while CI fails (the whole defect, in miniature)
    monkeypatch.setattr(bg.os.path, "abspath",
                        lambda p: str(tmp_path / "tools" / "billing_guard.py"))
    return monkeypatch


def test_the_account_id_comes_from_CLOUDFLARE_ACCOUNT_ID(clean_env):
    """The name CI already exports. Without this the guard is blind on every runner."""
    clean_env.setenv("CLOUDFLARE_ACCOUNT_ID", TAG)
    assert bg._account_id() == TAG


def test_the_R2_endpoint_still_works_as_a_fallback(clean_env):
    """An R2 endpoint is https://<account_id>.r2.cloudflarestorage.com — same value."""
    clean_env.setenv("R2_WRITE_ENDPOINT", f"https://{TAG}.r2.cloudflarestorage.com")
    assert bg._account_id() == TAG


def test_a_quoted_value_is_accepted(clean_env):
    """dotenv files quote values routinely; a quoted tag is the same tag."""
    clean_env.setenv("CLOUDFLARE_ACCOUNT_ID", f'"{TAG}"')
    assert bg._account_id() == TAG


@pytest.mark.parametrize("bad", ["", "not-an-account", TAG[:-1], TAG + "0", TAG.upper(),
                                 "https://.r2.cloudflarestorage.com"])
def test_a_wrong_shaped_value_is_REFUSED_not_passed_through(clean_env, bad):
    """A truncated or wrong tag builds a valid-looking GraphQL query against the wrong
    account and returns ZEROS, which this guard prices as a quiet month - the exact failure
    it exists to prevent (R507). Refusing is loud; passing it through is silent."""
    clean_env.setenv("CLOUDFLARE_ACCOUNT_ID", bad)
    assert bg._account_id() == ""


def test_a_good_endpoint_wins_over_a_junk_account_id(clean_env):
    """Order is a preference, not a commitment: if the canonical name holds something that is
    not an account tag, the other source must still be tried rather than the run going blind."""
    clean_env.setenv("CLOUDFLARE_ACCOUNT_ID", "this-is-not-a-tag")
    clean_env.setenv("R2_WRITE_ENDPOINT", f"https://{TAG}.r2.cloudflarestorage.com")
    assert bg._account_id() == TAG


def test_the_blind_message_names_the_MISSING_half_not_the_token_by_default(clean_env,
                                                                          monkeypatch):
    """The defect this file is named for. With the token present and the account id absent,
    the sentence must not say the token is missing."""
    monkeypatch.setenv("CF_ANALYTICS_TOKEN", "cfut_" + "x" * 48)
    bg._MEASURED["unmetered"] = False
    bg._MEASURED.pop("unmetered_missing", None)
    msg = bg.account_analytics()
    assert bg._MEASURED.get("unmetered") is True, msg
    assert "CLOUDFLARE_ACCOUNT_ID" in msg, msg
    assert "CF_ANALYTICS_TOKEN" not in msg, \
        "the guard blames the token while the token is set:\n" + msg


def test_the_blind_message_names_the_token_when_the_token_is_the_gap(clean_env, monkeypatch):
    """And the other way round, so the fix above is not a blanket rewording."""
    monkeypatch.delenv("CF_ANALYTICS_TOKEN", raising=False)
    monkeypatch.setenv("CLOUDFLARE_ACCOUNT_ID", TAG)
    monkeypatch.setattr(bg, "_load_env_token", lambda: "")
    bg._MEASURED["unmetered"] = False
    bg._MEASURED.pop("unmetered_missing", None)
    msg = bg.account_analytics()
    assert "CF_ANALYTICS_TOKEN" in msg, msg
    assert "CLOUDFLARE_ACCOUNT_ID" not in msg, msg


def test_the_workflow_passes_BOTH_names_to_the_job():
    """A secret that no env line passes is invisible to the job. CLOUDFLARE_ACCOUNT_ID was
    already there and R2_WRITE_ENDPOINT was not, which is why _account_id()'s only source was
    unreachable in CI."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    wf = open(os.path.join(root, ".github", "workflows", "billing-guard.yml"),
              encoding="utf-8").read()
    for name in ("CF_ANALYTICS_TOKEN", "CLOUDFLARE_ACCOUNT_ID", "R2_WRITE_ENDPOINT"):
        assert f"{name}: ${{{{ secrets.{name} }}}}" in wf, f"{name} is not passed to the job"
