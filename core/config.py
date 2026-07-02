"""Tiny .env loader (no external dependency) -- loads server-side API keys.

Keys live in .env (gitignored). Connectors call load_env() then read os.environ.
In production (GitHub Actions) the same names come from Secrets, so connector code
is identical locally and in CI.
"""
import os

_DEFAULT = os.path.join(os.path.dirname(__file__), "..", ".env")


def load_env(path: str | None = None) -> dict:
    path = os.path.abspath(path or _DEFAULT)
    loaded = {}
    if not os.path.exists(path):
        return loaded
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            os.environ.setdefault(k, v)
            loaded[k] = v
    return loaded


def require(name: str) -> str:
    load_env()
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"missing required key {name!r} -- add it to .env")
    return val
