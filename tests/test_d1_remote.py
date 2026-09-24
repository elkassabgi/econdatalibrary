"""core/d1_remote.py - the D1 chokepoint (plan 4c, 5). No network: urlopen is replaced."""
import io
import json
import os
import re

import pytest

from core import cutover, d1_remote

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

READS = [
    "SELECT COUNT(*) FROM series",
    "select series_id from series where series_id = ?",
    "WITH s AS (SELECT source_id FROM source) SELECT * FROM s",
    "SELECT title FROM series WHERE title LIKE '%delete%' OR title = 'x; drop table y'",
    'SELECT "insert" FROM t',
    "SELECT replace(title, 'a', 'b') FROM series",
    "SELECT CASE WHEN n > 1 THEN 'many' ELSE 'one' END FROM counts",   # AR-151: END closes a CASE
    "  SELECT 1  ",
]
NOT_READS = {
    "INSERT INTO series VALUES (1)": "SELECT or WITH",
    "SELECT 1; DELETE FROM series": "semicolon",
    "SELECT 1;": "semicolon",
    "SELECT 1 -- DELETE": "comment",
    "SELECT /* x */ 1": "comment",
    "WITH d AS (DELETE FROM series RETURNING *) SELECT * FROM d": "DELETE",
    "SELECT * FROM series RETURNING *": "RETURNING",
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x": "INSERT",
    "PRAGMA table_info(series)": "SELECT or WITH",
    "SELECT 1 FROM pragma_table_info('series') WHERE 0 UNION SELECT 1": None,   # read; pragma_ fn allowed
    "ATTACH DATABASE 'x' AS y": "SELECT or WITH",
    "SELECT * FROM t WHERE a = 'unterminated": "quote",
    "REPLACE INTO series VALUES (1)": "SELECT or WITH",
    "SELECT 1 UNION SELECT 2 INTO t": "INTO",
    "WITH x AS (VALUES (1))": "no SELECT",
    "": "empty",
}


@pytest.mark.parametrize("sql", READS)
def test_plain_reads_pass(sql):
    assert d1_remote.read_statement_problem(sql) is None, sql


@pytest.mark.parametrize("sql,why", NOT_READS.items())
def test_everything_else_is_refused_with_a_reason(sql, why):
    got = d1_remote.read_statement_problem(sql)
    if why is None:
        assert got is None, (sql, got)
    else:
        assert got is not None and why.lower() in got.lower(), (sql, got)


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def sent(monkeypatch):
    calls = []

    def urlopen(req, timeout=0):
        calls.append({"url": req.full_url, "auth": req.headers.get("Authorization"),
                      "body": json.loads(req.data)})
        return _Resp(json.dumps({"success": True, "result": [{"results": [], "meta": {"rows_read": 1}}]}).encode())
    monkeypatch.setattr(d1_remote.urllib.request, "urlopen", urlopen)
    return calls


@pytest.fixture
def cut_over(tmp_path, monkeypatch):
    (tmp_path / "CUTOVER").write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "CUTOVER"))


@pytest.fixture
def before_t0(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "absent" / "CUTOVER"))


def test_before_t0_anything_goes_with_the_normal_token(sent, before_t0, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "write-token")
    d1_remote.query("econ-catalog", "DELETE FROM pageview WHERE day < ?", ["2026-01-01"])
    assert sent[0]["auth"] == "Bearer write-token"
    assert sent[0]["url"].endswith("/d1/database/1a6d0755-ecef-46d0-a478-46cad1cf064c/query")
    assert sent[0]["body"] == {"sql": "DELETE FROM pageview WHERE day < ?", "params": ["2026-01-01"]}


def test_after_t0_a_write_is_refused_before_any_request(sent, cut_over, monkeypatch):
    monkeypatch.setenv("D1_READ_TOKEN", "read-token")
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "write-token")
    with pytest.raises(d1_remote.NotAReadStatement):
        d1_remote.query("econ-catalog", "DELETE FROM series")
    with pytest.raises(cutover.CutoverRefused):
        d1_remote.query("econ-catalog-climate", "SELECT 1; DROP TABLE series")
    assert sent == []


def test_after_t0_a_read_uses_only_the_read_token(sent, cut_over, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "write-token")
    monkeypatch.delenv("D1_READ_TOKEN", raising=False)
    with pytest.raises(cutover.CutoverRefused, match="D1_READ_TOKEN"):
        d1_remote.query("econ-catalog", "SELECT 1")
    assert sent == [], "the write-capable token is never used after T0"
    monkeypatch.setenv("D1_READ_TOKEN", "read-token")
    out = d1_remote.query("econ-catalog-climate", "SELECT COUNT(*) FROM series")
    assert sent[0]["auth"] == "Bearer read-token" and out["meta"]["rows_read"] == 1


def test_only_the_two_econ_databases(sent, before_t0, monkeypatch):
    monkeypatch.setenv("CLOUDFLARE_API_TOKEN", "t")
    with pytest.raises(ValueError):
        d1_remote.query("hfdatalibrary-db", "SELECT 1")
    assert d1_remote.DATABASES == {"econ-catalog": "1a6d0755-ecef-46d0-a478-46cad1cf064c",
                                   "econ-catalog-climate": "e34114f2-c0be-43d9-bcb5-798a3952414c"}


# The files that reach D1 remotely by another road TODAY (2026-09-24). They move into d1_remote in plan
# step 1; this list may only SHRINK. A new file on it is a new unguarded write path after T0.
LEGACY_REMOTE_D1 = {
    ".github/workflows/sec-edgar-daily.yml", ".github/workflows/updater-daily.yml",   # disabled at T0 (6a)
    "core/catalog.py", "core/export_d1.py", "core/export_d1_i18n_delta.py", "core/export_d1_sources.py",
    "core/load_d1_chunked.py", "core/load_d1_rest.py", "core/sync_catalog_d1.py", "core/sync_state_d1.py",
    "tools/audit_d1_source_counts.py", "tools/audit_d1_vs_catalog.py", "tools/audit_licence_disclosure.py",
    "tools/audit_site.py", "tools/billing_guard.py", "tools/delist_source_rows.py", "tools/delist_timeless_tables.py",
    "tools/enrich_sec_edgar_tickers.py", "tools/migrate_noaa_shard.py", "tools/rebuild_series_fts.py",
    "tools/refresh_flowgrain_dates.py", "tools/refresh_sec_edgar.py", "tools/retire_source.py",
    "tools/stamp_source_data_through.py", "tools/sync_source_rows_d1_to_local.py", "tools/sync_titles_to_d1.py",
    "tools/verify_source_served.py",
}
REMOTE = re.compile(r"--remote\b|/d1/database/")


SKIP_DIRS = {".git", "node_modules", "data", "dist", "tests", "docs", "scratchpad", ".wrangler", "__pycache__",
             ".claude", "logs", "state"}
CODE = (".py", ".ps1", ".sh", ".yml", ".yaml", ".mjs", ".js", ".cjs", ".bat", ".cmd")


def _code_files():
    """Every script and workflow in the repo (a fixed folder list missed files before - the plan itself
    first named one that lives under tools/)."""
    for dirpath, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            if f.endswith(CODE):
                p = os.path.join(dirpath, f)
                yield os.path.relpath(p, ROOT).replace(os.sep, "/"), p


def test_no_new_file_calls_d1_remotely_outside_the_chokepoint():
    found = set()
    for rel, p in _code_files():
        with open(p, encoding="utf-8", errors="replace") as fh:
            if REMOTE.search(fh.read()):
                found.add(rel)
    found.discard("core/d1_remote.py")
    found.discard("tools/selfhost/cutover_hook.py")   # names the roads in order to REFUSE them (plan change 5)
    assert found - LEGACY_REMOTE_D1 == set(), "new remote-D1 callers: route them through core/d1_remote.query"
    gone = LEGACY_REMOTE_D1 - found
    assert not gone, f"these no longer call D1 remotely - remove them from LEGACY_REMOTE_D1: {sorted(gone)}"


def test_the_ratchet_can_fail(tmp_path):
    """Planted positive: the pattern really matches both roads."""
    assert REMOTE.search('subprocess.run(["npx", "wrangler", "d1", "execute", "econ-catalog", "--remote"])')
    assert REMOTE.search('url = f"{api}/accounts/{a}/d1/database/{db}/query"')
    assert not REMOTE.search("wrangler d1 execute econ-catalog --local")
