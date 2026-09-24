"""The self-hosted origin's wrangler config must stay undeployable (docs/ECON_SELF_HOSTING_PLAN.md).

The origin runs the worker in LOCAL mode, where download auth and the download log are skipped because
the edge worker did them. Deployed by mistake, it would be a public worker with no download auth.
Reviews R1164 (6), R1165 (7) and R1166 (7) asked for a parse of the TOP-LEVEL keys: the edge's
wrangler.toml once put workers_dev inside [limits], where wrangler never reads it, so a copy that set
`workers_dev = false` in the same place would have had no effect.
"""
import os
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORIGIN = os.path.join(ROOT, "api", "worker", "wrangler.origin.toml")


def _origin():
    with open(ORIGIN, "rb") as fh:
        return tomllib.load(fh)


def test_the_origin_config_cannot_replace_or_become_a_public_worker():
    c = _origin()
    assert c["name"] != "econdl-api", "the origin must never share the edge worker's name"
    assert c.get("workers_dev") is False, "TOP-LEVEL workers_dev = false"
    assert c.get("preview_urls") is False, "TOP-LEVEL preview_urls = false (defaults to true)"
    for key in ("route", "routes", "triggers", "account_id", "env"):
        # "env": an [env.x] block would be used by `wrangler deploy --env x` with its own workers_dev and
        # routes, bypassing every top-level key checked here (AR-150)
        assert key not in c, f"{key} must not appear in the origin config"
    assert "workers_dev" not in (c.get("limits") or {}), "workers_dev inside [limits] has no effect"


def test_the_origin_runs_local_mode_with_no_committed_secret():
    c = _origin()
    assert (c.get("vars") or {}).get("LOCAL") == "1"
    assert "ORIGIN_SECRET" not in (c.get("vars") or {}), "the secret lives only in .dev.vars"


def test_the_origin_binds_only_local_placeholders():
    c = _origin()
    dbs = {d["binding"]: d for d in c.get("d1_databases") or []}
    assert set(dbs) == {"CATALOG", "CATALOG_CLIMATE"}, "no USERS: the edge owns the identity DB"
    for d in dbs.values():
        assert d["database_id"].startswith("00000000-0000-0000-0000-"), d
    buckets = c.get("r2_buckets") or []
    assert [b["bucket_name"] for b in buckets] == ["econ-origin-local-placeholder"]


def test_negative_control_the_production_config_is_not_local():
    with open(os.path.join(ROOT, "api", "worker", "wrangler.toml"), "rb") as fh:
        prod = tomllib.load(fh)
    assert prod["name"] == "econdl-api"
    assert "LOCAL" not in (prod.get("vars") or {}), "local mode must never reach the edge worker"
