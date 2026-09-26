# -*- coding: utf-8 -*-
"""`account_id` and `workers_dev` must be TOP-LEVEL keys in the worker's wrangler.toml.

WHY THIS EXISTS. Both keys sat under the `[limits]` header. A TOML table header owns every
key after it until the next header, so they parsed as `limits.account_id` and
`limits.workers_dev` - and wrangler reads neither of those. Nothing broke, which is exactly
why it survived: with one account on the OAuth token wrangler auto-selects it, and
`workers_dev` falls back to `routes.length === 0`, which is true for this config. Both
defaults happened to match what the keys asked for.

The failure is latent and lands on whoever runs the deploy: add a second account to that
token and `wrangler deploy` stops on an interactive account picker, or fails outright where
there is no terminal to answer it.

Misplacement of this kind is invisible to a grep - the keys ARE in the file, on their own
lines, spelled correctly. Only a parse sees it, which is what this does.
"""
from __future__ import annotations

import os

import pytest

try:
    import tomllib
except ModuleNotFoundError:                      # py < 3.11
    tomllib = None

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOML = os.path.join(ROOT, "api", "worker", "wrangler.toml")


def _parsed() -> dict:
    with open(TOML, "rb") as fh:
        return tomllib.load(fh)


pytestmark = pytest.mark.skipif(tomllib is None, reason="tomllib needs Python 3.11+")


def test_the_file_parses_and_is_the_worker_we_think_it_is():
    """Positive control: a parse that returned {} would make every assertion below vacuous."""
    d = _parsed()
    assert d, "wrangler.toml parsed to nothing"
    assert d.get("name") == "econdl-api", f"this is not the worker config: name={d.get('name')!r}"
    assert d.get("main") == "src/index.ts"


def test_account_id_is_top_level_and_not_swallowed_by_a_table():
    d = _parsed()
    assert "account_id" in d, (
        "account_id is not a top-level key. It is almost certainly below a [table] header - "
        f"found instead under: {[k for k, v in d.items() if isinstance(v, dict) and 'account_id' in v]}. "
        "wrangler only reads it at the top level; anywhere else it is silently ignored and the "
        "deploy falls back to picking an account interactively."
    )
    assert isinstance(d["account_id"], str) and d["account_id"].strip()


def test_workers_dev_is_top_level_and_not_swallowed_by_a_table():
    d = _parsed()
    assert "workers_dev" in d, (
        "workers_dev is not a top-level key - found instead under: "
        f"{[k for k, v in d.items() if isinstance(v, dict) and 'workers_dev' in v]}. "
        "The live worker is reachable at econdl-api.elkassabgi.workers.dev, so this must be true "
        "explicitly rather than by falling back to `routes.length === 0`."
    )
    assert d["workers_dev"] is True


def test_limits_still_holds_only_what_belongs_to_it():
    """The other half of the move: the CPU ceiling must not have been carried out with them."""
    d = _parsed()
    limits = d.get("limits")
    assert isinstance(limits, dict), "the [limits] table is gone"
    assert limits.get("cpu_ms") == 300000, f"cpu_ms changed: {limits.get('cpu_ms')!r}"
    stray = sorted(set(limits) - {"cpu_ms"})
    assert not stray, f"[limits] holds keys that are not limits: {stray}"
