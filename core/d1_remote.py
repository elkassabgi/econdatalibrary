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
