"""The soak worker's config (api/worker/wrangler.soak.toml; self-hosting plan step 4).

The soak worker is a second PUBLIC address bound to the production users database. These tests pin what
keeps it harmless there, and what keeps its switch away from the production worker:
  - it is another worker (its own name), with the production worker's account, runtime settings and limits;
  - no scheduled work: `crons = []` (an absent [triggers] block would leave a deployed cron in place);
  - FORWARD = "on" and SOAK = "1"; the USERS binding and nothing else;
  - no secret and no origin address in the file;
  - SOAK appears in NO other wrangler config: on the production worker it would turn the page-view routes
    into 404s with no error anywhere.
Parsed with tomllib, top-level keys read as top-level (a key inside a table has no effect for wrangler).
"""
import os
import tomllib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER = os.path.join(ROOT, "api", "worker")


def _load(name):
    with open(os.path.join(WORKER, name), "rb") as fh:
        return tomllib.load(fh)


def test_the_soak_worker_is_another_worker_with_the_production_runtime():
    soak, prod = _load("wrangler.soak.toml"), _load("wrangler.toml")
    assert soak["name"] == "econdl-api-soak" and prod["name"] == "econdl-api"
    for key in ("main", "compatibility_date", "compatibility_flags", "account_id", "limits", "observability"):
        assert soak[key] == prod[key], f"{key} must equal the production worker's"
    assert soak["workers_dev"] is True, "its only address is the workers.dev name"
    assert soak["preview_urls"] is False
    for key in ("route", "routes", "env", "services", "kv_namespaces", "durable_objects", "queues"):
        assert key not in soak, f"{key} must not appear in the soak config"
    assert "workers_dev" not in soak["limits"] and "account_id" not in soak["limits"]


def test_the_soak_worker_has_no_scheduled_work():
    soak = _load("wrangler.soak.toml")
    assert soak["triggers"] == {"crons": []}, "an empty list REMOVES deployed crons; an absent block keeps them"


def test_the_soak_worker_forwards_and_is_marked_as_soak():
    assert _load("wrangler.soak.toml")["vars"] == {"FORWARD": "on", "SOAK": "1"}


def test_the_soak_worker_binds_the_users_database_and_nothing_else():
    soak, prod = _load("wrangler.soak.toml"), _load("wrangler.toml")
    users = [d for d in prod["d1_databases"] if d["binding"] == "USERS"]
    assert soak["d1_databases"] == users and len(users) == 1
    assert "r2_buckets" not in soak, "with FORWARD on no route reads the bucket"


def test_no_secret_and_no_origin_address_is_committed():
    text = open(os.path.join(WORKER, "wrangler.soak.toml"), encoding="utf-8").read()
    data = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    for word in ("ORIGIN_URL", "ORIGIN_SECRET", "ORIGIN_ACCESS", "https://", "http://"):
        assert word not in data, f"{word} must not be set in the soak config"


def test_the_soak_switch_is_in_no_other_config():
    """Every wrangler config under api/worker but the soak one: no SOAK variable, in any table."""
    others = sorted(n for n in os.listdir(WORKER) if n.startswith("wrangler") and n.endswith(".toml")
                    and n != "wrangler.soak.toml")
    assert "wrangler.toml" in others and "wrangler.origin.toml" in others
    for name in others:
        text = open(os.path.join(WORKER, name), encoding="utf-8").read()
        data = "\n".join(ln.split("#", 1)[0] for ln in text.splitlines())
        assert "SOAK" not in data, f"{name}: SOAK belongs to wrangler.soak.toml only"
    assert "FORWARD" not in (_load("wrangler.toml").get("vars") or {}), \
        "the production worker does not forward before the cutover (this pin ends at plan step 6)"


def test_control_the_reader_sees_a_misplaced_or_changed_line(tmp_path):
    """The pins above compare parsed values: show that a cron, a second binding and a key swallowed by a
    table each give another parse."""
    text = open(os.path.join(WORKER, "wrangler.soak.toml"), encoding="utf-8").read()
    good = "".join(ln + "\n" for ln in text.splitlines() if not ln.lstrip().startswith("#"))   # the comments name the same words
    base = tomllib.loads(good)
    assert base == _load("wrangler.soak.toml")
    assert tomllib.loads(good.replace("crons = []", 'crons = ["*/30 * * * *"]'))["triggers"] != base["triggers"]
    assert tomllib.loads(good.replace('SOAK = "1"', 'SOAK = "0"'))["vars"] != base["vars"]
    moved = good.replace("workers_dev = true\n", "").replace("cpu_ms = 300000", "cpu_ms = 300000\nworkers_dev = true")
    assert "workers_dev" not in tomllib.loads(moved), "a key under [limits] is not a top-level key"
    assert good.count("crons = []") == 1 and good.count('SOAK = "1"') == 1 and good.count("workers_dev = true\n") == 1
