"""THE D1 chokepoint of the econ self-hosting move (docs/ECON_SELF_HOSTING_PLAN.md, section 3, changes 4c
and 5).

After T0 the two econ D1 catalogue databases are a frozen copy. They are still READ (the step-6b
reconcile pages both), but nothing may write to them. Every remote D1 access is meant to go through
query() here; tests/test_d1_remote.py fails when a NEW file calls D1 remotely any other way (the files that
do so today are listed there and move in step 1 of the plan).

After the machine-wide CUTOVER flag (core/cutover.py) exists:
  * the call needs D1_READ_TOKEN, a Cloudflare token with D1 READ only, so the SERVER refuses any write
    even if a check here were wrong; CLOUDFLARE_API_TOKEN (which can write) is never used;
  * the SQL must be ONE read statement: SELECT or WITH ... SELECT, no semicolon, no comment, and none of
    the write or schema keywords (INSERT, UPDATE, DELETE, REPLACE as a statement, CREATE, DROP, ALTER,
    ATTACH, DETACH, PRAGMA, VACUUM, REINDEX, RETURNING, TRIGGER, ANALYZE, BEGIN, COMMIT, SAVEPOINT...).
    String literals and quoted names are removed before the scan, so `WHERE title LIKE '%delete%'` is a
    read; anything the scan cannot classify is refused (fail closed).
Before the flag it behaves as today's callers do: any statement, with CLOUDFLARE_API_TOKEN.

The cost rule of this project still applies to every caller (CLAUDE.md "COST"): run ONE statement as a
SELECT and read meta.rows_read before any batch.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request

from core.cutover import CutoverRefused, is_cut_over

ACCOUNT_ID = "ce51d5c7fe3859098751b89bbebeab7a"
DATABASES = {
    "econ-catalog": "1a6d0755-ecef-46d0-a478-46cad1cf064c",
    "econ-catalog-climate": "e34114f2-c0be-43d9-bcb5-798a3952414c",
}

_WRITE_WORDS = frozenset("""
    INSERT UPDATE DELETE REPLACE UPSERT CREATE DROP ALTER ATTACH DETACH PRAGMA VACUUM REINDEX RETURNING
    TRIGGER ANALYZE BEGIN COMMIT ROLLBACK SAVEPOINT RELEASE TRANSACTION INTO""".split())
# END is not listed: it closes every CASE expression, and a bare END (= COMMIT) cannot start a statement here.
# BEGIN and COMMIT stay: neither appears inside a read.
_LITERALS = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|`[^`]*`|\[[^\]]*\]")
_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


class NotAReadStatement(CutoverRefused):
    """After T0, a statement that is not one plain read."""


def read_statement_problem(sql: str) -> str | None:
    """None when `sql` is ONE plain read statement; otherwise why it is not. Fail closed."""
    if not isinstance(sql, str) or not sql.strip():
        return "empty statement"
    bare = _LITERALS.sub("''", sql)
    if "'" in _LITERALS.sub("", bare) or '"' in _LITERALS.sub("", bare):
        return "an unterminated quote"
    if ";" in bare:
        return "a semicolon (more than one statement, or a trailing one)"
    if "--" in bare or "/*" in bare:
        return "a comment"
    words = [(m.group(0).upper(), m.end()) for m in _WORD.finditer(bare)]
    if not words or words[0][0] not in ("SELECT", "WITH"):
        return "it does not start with SELECT or WITH"
    for w, end in words:
        if w in _WRITE_WORDS:
            if w == "REPLACE" and bare[end:].lstrip().startswith("("):
                continue                                    # the replace() string function
            return f"the keyword {w}"
    if words[0][0] == "WITH" and not any(w == "SELECT" for w, _ in words):
        return "a WITH that has no SELECT"
    return None


def execute_wrangler(database: str, statements: list[str], *, cwd: str | None = None) -> bool:
    """Run statements on a remote econ D1 database through `wrangler d1 execute --remote` - the road the
    licence tools write D1 by today (wrangler's OAuth login; the desktop .env holds no API token). Stops at
    the first failure and prints why. AFTER T0 it refuses outright: the OAuth login can write, and D1 is
    the frozen copy (reads after T0 go through query() with the read-only token)."""
    import subprocess                                                           # noqa: PLC0415
    import sys                                                                  # noqa: PLC0415
    if database not in DATABASES:
        raise ValueError(f"unknown D1 database {database!r}; known: {sorted(DATABASES)}")
    if is_cut_over():
        raise CutoverRefused(f"refused: wrangler d1 execute --remote on {database} after T0 (D1 is frozen)")
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for stmt in statements:
        r = subprocess.run(["npx", "wrangler", "d1", "execute", database, "--remote", "--command", stmt],
                           cwd=cwd or os.path.join(root, "api", "worker"), capture_output=True, text=True,
                           encoding="utf-8", errors="replace", env=env, shell=(os.name == "nt"))
        ok = r.returncode == 0
        print(f"  D1: {stmt.split(' WHERE')[0]} -> {'ok' if ok else 'FAILED'}")
        if not ok:
            # captured as utf-8, printed to a console that may be cp1252: never let the error report crash
            detail = (r.stderr or r.stdout or "")[-600:]
            enc = sys.stdout.encoding or "utf-8"
            sys.stdout.write(detail.encode(enc, "replace").decode(enc, "replace") + "\n")
            return False
    return True


# ---- the wrangler roads the step-1 callers move onto ------------------------------------------------------
# The desktop reaches D1 through wrangler's OAuth login (its .env holds no API token), and CI through
# CLOUDFLARE_API_TOKEN in wrangler's environment. Before T0 these run wrangler exactly as the callers did.
# After T0: run_json() takes one plain read and goes over REST with D1_READ_TOKEN (query()'s rules);
# execute_file() refuses, as execute_wrangler() does.
WORKER_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "api", "worker")
WRANGLER_JS = os.path.join(WORKER_DIR, "node_modules", "wrangler", "bin", "wrangler.js")
_AUTH_TRANSIENT = "code: 10000"          # wrangler's intermittent OAuth error; retried twice (R733)


class D1Unreachable(RuntimeError):
    """D1 could not be asked at all: no wrangler or node, a timeout, a network failure. A RuntimeError, never
    SystemExit, so every caller that tells "could not look" from a finding keeps doing so (R1183: a timeout,
    a missing node and a network error used to escape as other exception types)."""


def _wrangler(args: list[str], *, timeout: int, retries: int = 2):
    """Run wrangler; retry ONLY its intermittent auth error (code 10000, on stdout or stderr), `retries`
    times. Every way of not reaching D1 raises D1Unreachable."""
    import subprocess                                                           # noqa: PLC0415
    import time                                                                 # noqa: PLC0415
    if not os.path.isfile(WRANGLER_JS):
        raise D1Unreachable(f"no wrangler at {WRANGLER_JS} (run npm ci in api/worker); cannot reach D1")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    for attempt in range(retries + 1):
        try:
            r = subprocess.run(["node", WRANGLER_JS, *args], cwd=WORKER_DIR, capture_output=True, text=True,
                               encoding="utf-8", errors="replace", env=env, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise D1Unreachable(f"wrangler timed out after {timeout} s") from None
        except OSError as e:                                    # node missing, cannot start
            raise D1Unreachable(f"wrangler could not run: {type(e).__name__}: {e}") from None
        if r.returncode == 0 or _AUTH_TRANSIENT not in (r.stderr or "") + (r.stdout or "") or attempt == retries:
            return r
        time.sleep(10)
    raise AssertionError("unreachable")


def statement_results(stdout: str) -> list[dict]:
    """The [{results, meta, success}, ...] array in wrangler's --json output. wrangler may print other JSON
    first (bindings), and a greedy match spans from the first '[' to the last ']' - so every top-level
    array is decoded and the first whose items all carry `results` is the answer."""
    dec = json.JSONDecoder()
    i = stdout.find("[")
    while i != -1:
        try:
            value, end = dec.raw_decode(stdout, i)
        except ValueError:
            i = stdout.find("[", i + 1)
            continue
        if isinstance(value, list) and value and all(isinstance(v, dict) and "results" in v for v in value):
            return value
        i = stdout.find("[", end)
    raise RuntimeError(f"no query result in wrangler's output: {stdout[-300:]!r}")


def run_json(database: str, sql: str, *, timeout: int = 900) -> list[dict]:
    """ONE statement on a remote econ D1 database; returns [{results, meta, ...}] - wrangler's --json shape,
    which is also the REST API's `result`. Before T0 through wrangler, any statement (today's behaviour);
    after T0 only one plain read, through query() with the read-only token."""
    if database not in DATABASES:
        raise ValueError(f"unknown D1 database {database!r}; known: {sorted(DATABASES)}")
    if is_cut_over():
        import urllib.error                                                     # noqa: PLC0415
        try:
            return [query(database, sql, timeout=timeout)]
        except (urllib.error.URLError, OSError, ValueError) as e:      # the network, or a garbled answer
            raise D1Unreachable(f"D1 {database}: {type(e).__name__}: {e}") from None
    r = _wrangler(["d1", "execute", database, "--remote", "--json", "--command", sql], timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"D1 {database}: wrangler exit {r.returncode}: stderr={(r.stderr or '')[-400:]!r} "
                           f"stdout={(r.stdout or '')[-200:]!r}")
    return statement_results(r.stdout)


def rows(database: str, sql: str, *, timeout: int = 900) -> tuple[list[dict], int]:
    """run_json flattened: (every result row, total meta.rows_read) - the cost is reported, not guessed."""
    out = run_json(database, sql, timeout=timeout)
    return ([row for e in out for row in (e.get("results") or [])],
            sum(int((e.get("meta") or {}).get("rows_read") or 0) for e in out))


def execute_file(database: str, path: str, *, timeout: int = 3600, tries: int = 1, on_retry=None) -> str:
    """`wrangler d1 execute <db> --remote --file <path> --yes` - the bulk loaders' road. Refuses after T0.

    By default it runs ONCE: an import that failed after it started may have applied part of the file, and
    not every file is idempotent (series_fts inserts are not), so a retry is the CALLER's decision (R1183).
    tries > 1 retries any failure or timeout with 5 s, 10 s, ... between attempts - the daily sync's policy
    (core/sync_state_d1.py). on_retry(attempt, why) is told about each retry. Returns wrangler's stdout;
    the final failure raises RuntimeError (D1Unreachable when D1 was never reached)."""
    import time                                                                 # noqa: PLC0415
    if database not in DATABASES:
        raise ValueError(f"unknown D1 database {database!r}; known: {sorted(DATABASES)}")
    if is_cut_over():
        raise CutoverRefused(f"refused: wrangler d1 execute --remote --file on {database} after T0 (D1 is frozen)")
    why, unreachable = "", False
    for attempt in range(max(1, tries)):
        try:
            r = _wrangler(["d1", "execute", database, "--remote", "--yes", f"--file={os.path.abspath(path)}"],
                          timeout=timeout, retries=0)
        except D1Unreachable as e:
            why, unreachable = str(e), True
        else:
            if r.returncode == 0:
                return r.stdout or ""
            why, unreachable = (f"exit {r.returncode}: stdout={(r.stdout or '')[-600:]!r} "
                                f"stderr={(r.stderr or '')[-600:]!r}"), False
        if attempt < tries - 1:
            if on_retry:
                on_retry(attempt + 1, why)
            time.sleep(5 * (attempt + 1))
    err = D1Unreachable if unreachable else RuntimeError
    raise err(f"D1 {database}: --file {path} failed after {max(1, tries)} attempt(s): {why}")


def query(database: str, sql: str, params: list | None = None, *, timeout: int = 120) -> dict:
    """Run ONE statement on a remote econ D1 database; returns the REST result ({results, meta, ...})."""
    if database not in DATABASES:
        raise ValueError(f"unknown D1 database {database!r}; known: {sorted(DATABASES)}")
    if is_cut_over():
        why = read_statement_problem(sql)
        if why:
            raise NotAReadStatement(f"refused: D1 {database} after T0 takes only one plain read; this has {why}")
        token = os.environ.get("D1_READ_TOKEN", "").strip()
        if not token:
            raise CutoverRefused("refused: after T0 remote D1 needs D1_READ_TOKEN (a D1-read-only token); "
                                 "CLOUDFLARE_API_TOKEN can write and is not used")
    else:
        token = os.environ.get("CLOUDFLARE_API_TOKEN", "").strip()
        if not token:
            raise RuntimeError("CLOUDFLARE_API_TOKEN is not set")
    url = (f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/d1/database/"
           f"{DATABASES[database]}/query")
    body = json.dumps({"sql": sql, "params": params or []}).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = json.load(r)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"D1 {database}: HTTP {e.code} {e.read()[:300]!r}") from None
    if not out.get("success"):
        raise RuntimeError(f"D1 {database}: {json.dumps(out.get('errors'))[:300]}")
    return out["result"][0]
