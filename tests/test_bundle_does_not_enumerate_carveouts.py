"""`/v1/bundle?source=` must not ENUMERATE the series it is not allowed to serve.

`bundle.ts` says it in its own comment: "a series from a denylisted source must never appear in
a bundle manifest, even as a stable URL ... the manifest itself must not advertise it". The
per-id loop honoured that for the CSV path and not for the id. The ids came from
`SERIES_IDS_FOR_SOURCE`, the ONE member of its family with no `carveoutExcl` — `browseSourceSql`,
`browseSourceVisibleCountSql` and both scoped search forms all apply it — so a carved id was read
out of the result set and still reached the manifest, echoed in `econdl:series_requested` and
named in `econdl:unresolved` with a reason identifying it as gated.

Verified live and anonymously on 2026-09-17 against a PUBLISHED carve-out, so the probe named
nothing protected:

    GET /v1/bundle?ids=worldbank:FP.CPI.TOTL.ZG:AGO,worldbank:NY.GDP.MKTP.CD:AGO
    -> "econdl:series_requested":["worldbank:FP.CPI.TOTL.ZG:AGO", ...]
       "econdl:unresolved":[{"id":"worldbank:FP.CPI.TOTL.ZG:AGO",
                             "reason":"not_redistributable: ... gated"}]

Read from the shipped TypeScript, like `test_catalog_source_name_query.py` and for the same
reason: `sql.ts` imports `./denylist` extensionlessly, so `node --test` cannot import it, and the
route itself needs a D1 binding. Comments are stripped first — this file's own docstring names
the symbols, and a bare substring match would find the prose instead of the code.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_TS = os.path.join(ROOT, "api", "worker", "src", "sql.ts")
BUNDLE_TS = os.path.join(ROOT, "api", "worker", "src", "bundle.ts")


def _code(path: str) -> str:
    src = open(path, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//.*", "", src)


def _fn_body(code: str, name: str) -> str:
    i = code.index(f"function {name}(")
    rest = code[i:]
    j = rest.find("\n}")
    return rest[: j if j > 0 else len(rest)]


def test_the_enumeration_query_excludes_carveouts():
    body = _fn_body(_code(SQL_TS), "seriesIdsForSourceSql")
    assert "carveoutExcl(" in body, (
        "the bundle's source enumeration no longer excludes carve-outs, so /v1/bundle?source= "
        "would list the very ids the gate exists to withhold")
    assert "FROM series WHERE source_id" in body, body[:200]
    assert body.index("carveoutExcl(") < body.index("ORDER BY"), (
        "the exclusion must sit in the WHERE clause, before ORDER BY")


def test_the_bundle_route_calls_the_filtered_builder():
    """Fixing the builder achieves nothing if the route still binds the unfiltered const."""
    code = _code(BUNDLE_TS)
    assert "seriesIdsForSourceSql(source)" in code, (
        "bundle.ts no longer uses the carve-out-excluding builder for its source enumeration")
    assert "prepare(SERIES_IDS_FOR_SOURCE)" not in code, (
        "bundle.ts still prepares the UNFILTERED query; the exclusion is bypassed")


def test_every_sibling_source_scoped_query_also_excludes():
    """The family property, and the reason this one stood out. If a NEW source-scoped query is
    added without the exclusion, this is the test that should notice."""
    code = _code(SQL_TS)
    for fn in ("browseSourceSql", "browseSourceVisibleCountSql", "seriesIdsForSourceSql"):
        body = _fn_body(code, fn)
        assert "carveoutExcl(" in body, f"{fn} does not apply carveoutExcl"


def test_the_helpers_parse_at_all():
    """Guards the three tests above: if `_fn_body` stopped matching they would pass vacuously
    on an empty string, or raise on a rename — which is the honest failure."""
    code = _code(SQL_TS)
    for fn in ("browseSourceSql", "seriesIdsForSourceSql"):
        body = _fn_body(code, fn)
        assert len(body) > 60, f"{fn} parsed as {len(body)} chars — too short to be the function"
        assert "SELECT" in body.upper(), f"{fn} body does not look like a query builder"
