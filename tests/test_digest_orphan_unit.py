"""The digest lists an unmanaged unit_state row as an ORPHAN, never as an attention row.

`unit_state` keeps a row for every source that has ever run, including sources since removed
from the registry. Those rows keep their last status forever, so without a filter the digest
reported de-registered sources as failing every single morning — and counted them in the
subject line. `send_digest.main()` scopes the verdict to the registry's sources and names the
leftovers on a separate "unmanaged leftover state row(s)" line.

`tests/test_digest_orphan_filter.py` proves the filter runs under the workflow's own
invocation, but it needs the real local state store and is skipped in CI. This test needs
neither: it builds a two-row unit_state in a temporary database — one managed source and one
MADE-UP orphan id no registry has ever carried — and drives main() exactly as the workflow
does (no RESEND_API_KEY, so it prints and skips sending).

Verified by reversion before commit: with the two filter lines removed from main()
(`orphans = ...` / `rows = [r for r in rows if r[0] in managed]`) the made-up orphan appears
as `!! zzz_orphan_made_up` in the attention list, the subject line says "1 failed", and this
test fails.
"""
from __future__ import annotations

import contextlib
import io
import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

ORPHAN = "zzz_orphan_made_up"      # never a registry id, by construction
MANAGED = "yyy_managed_made_up"    # the registry fed to main() below contains exactly this id


def _state_db(path: str) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE unit_state (source_id TEXT, status TEXT, last_obs_date TEXT, "
                "last_success_utc TEXT, last_error TEXT, last_attempt_utc TEXT)")
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                (MANAGED, "ok", "2026-09-01", "2026-09-07T06:00:00Z", None, "2026-09-07T06:00:00Z"))
    con.execute("INSERT INTO unit_state VALUES (?,?,?,?,?,?)",
                (ORPHAN, "failed", None, None, "boom", "2026-06-01T00:00:00Z"))
    con.commit()
    con.close()


@pytest.fixture
def digest_output(tmp_path, monkeypatch) -> str:
    from updater import registry as R
    from updater import send_digest as D
    db = tmp_path / "state.db"
    _state_db(str(db))
    monkeypatch.setattr(D, "STATE", str(db))
    monkeypatch.setattr(R, "load", lambda: {"sources": [
        {"source_id": MANAGED, "live": True, "cadence": "daily", "run_location": "cloud"},
    ]})
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    monkeypatch.delenv("AQUEDUCT_DIGEST_HTML_OUT", raising=False)
    monkeypatch.setenv("RUN_STATUS", "success")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        D.main()
    return buf.getvalue()


def test_the_fixture_reaches_the_digest(digest_output):
    """Control: the managed row is rendered, so an absent orphan line would mean a real gap."""
    assert "[digest]" in digest_output, digest_output[-800:]
    assert MANAGED in digest_output


def test_the_orphan_is_named_on_the_leftover_line(digest_output):
    line = next((l for l in digest_output.splitlines() if "unmanaged leftover state row(s)" in l), "")
    assert line, "no orphan line — the filter did not run:\n" + digest_output[-1200:]
    assert ORPHAN in line, line


def test_the_orphan_is_not_an_attention_row_and_not_counted(digest_output):
    attention = [l for l in digest_output.splitlines() if l.lstrip().startswith("!!")]
    assert not any(ORPHAN in l for l in attention), attention
    # a 'failed' orphan must not turn a green run red
    assert "1 failed" not in digest_output, digest_output[:400]
    assert "OK — 1 sources current" in digest_output, digest_output[:400]
