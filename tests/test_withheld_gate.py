"""THE WITHHELD LIST (api/worker/src/withheld.ts; owner decision 2026-10-17, AR-339 Part 2) reaches every surface.

Pinned here, on the source text, so a new statement or route that forgets it fails CI:
  1. every statement exported by sql.ts is either WITHHELD-AWARE (it applies withheldExcl) or EXEMPT for a
     stated reason (finding 3: "a ratchet test should list every statement builder");
  2. catalog.ts and bundle.ts pass the withheld ids to every aware builder they call;
  3. index.ts gates .metadata.json and .csv with isWithheld BEFORE handleMetadata / auth, in route() AND in
     forwardRoute() (finding 1), and keys the /v1/catalog and /v1/stats caches with the list's tag (finding 6);
  4. the compiled-in floor is what api/worker/withheld.json gives (finding 5).
"""
from __future__ import annotations

import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "api", "worker", "src")

AWARE = {
    "searchFtsSql", "searchFtsCountSql", "searchLikeSql", "searchLikeCountSql", "browseAllSql",
    "searchFtsSourceSql", "searchFtsSourceCountSql", "searchLikeSourceSql", "searchLikeSourceCountSql",
    "browseSourceSql", "browseSourceVisibleCountSql", "seriesIdsForSourceSql",
}
EXEMPT = {
    "SELECT_SERIES": "one row by exact id: the routes gate the id before or after it (metadata/csv/bundle)",
    "SELECT_SOURCE": "a source row, names no series",
    "SELECT_LICENSE": "a licence row",
    "SELECT_SOURCES": "the source list; carries no series id and no per-series count",
    "SELECT_SOURCE_JOINED": "one source row",
    "BROWSE_SOURCE": "kept for callers/tests wanting the un-scoped shape; no handler prepares it",
    "BROWSE_SOURCE_COUNT_CACHED": "source_counts; the handler subtracts withheldPresentCountSql's count",
    "BROWSE_SOURCE_COUNT": "the live fallback count; the handler subtracts withheldPresentCountSql's count",
    "BROWSE_ALL_COUNT": "whole-table COUNT kept in its measured form; the handler subtracts (finding 3)",
    "SERIES_IDS_FOR_SOURCE": "kept for callers/tests; no handler prepares it",
    "LAST_UPDATES": "unit grain (state tables), no series id",
    "UNIT_STATE_FOR_SOURCE": "unit grain",
    "carveoutExcl": "a fragment builder",
    "hasCarveouts": "a predicate",
    "withheldExcl": "THE fragment builder itself",
    "withheldPresentCountSql": "the count the handlers subtract (PK lookups of the withheld ids)",
}


def _code(path: str) -> str:
    src = open(path, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//.*", "", src)


def _fn_body(code: str, name: str) -> str:
    i = code.index(f"function {name}(")
    rest = code[i:]
    j = rest.find("\n}")
    return rest[: j if j > 0 else len(rest)]


def test_every_sql_export_is_classified():
    code = _code(os.path.join(SRC, "sql.ts"))
    exported = set(re.findall(r"^export (?:const|function) (\w+)", code, flags=re.M))
    unknown = exported - AWARE - set(EXEMPT)
    assert not unknown, (f"sql.ts exports {sorted(unknown)} that are neither withheld-aware nor exempt: add "
                         f"withheldExcl to the statement, or an EXEMPT entry saying why it cannot name a series")
    gone = (AWARE | set(EXEMPT)) - exported
    assert not gone, f"classified but no longer exported (update this guard): {sorted(gone)}"
    for name in AWARE:
        body = _fn_body(code, name)
        assert "withheldExcl(" in body, f"{name} is listed as aware but does not apply withheldExcl"


def test_handlers_pass_the_withheld_ids_to_every_aware_builder():
    for f in ("catalog.ts", "bundle.ts"):
        code = _code(os.path.join(SRC, f))
        for name in AWARE:
            for m in re.finditer(rf"\b{name}\(([^()]*(?:\([^()]*\)[^()]*)*)\)", code):
                args = m.group(1)
                if name in ("searchFtsSql", "searchFtsCountSql", "searchLikeSql", "searchLikeCountSql", "browseAllSql"):
                    assert args.strip(), f"{f}: {name}() called with NO withheld ids"
                else:
                    assert "," in args, f"{f}: {name}({args}) called without the withheld ids"
    cat = _code(os.path.join(SRC, "catalog.ts"))
    assert "isWithheld(withheld" in cat, "catalog.ts dropped its belt-and-suspenders filter"
    for const in ("SEARCH_FTS", "SEARCH_LIKE", "BROWSE_ALL)"):
        assert f"prepare({const}" not in cat, f"catalog.ts prepares the removed unaware constant {const}"


def _between(code: str, start: str, end: str) -> str:
    i = code.index(start)
    return code[i: code.index(end, i)]


def test_index_gates_series_routes_before_metadata_and_auth_in_both_modes():
    idx = _code(os.path.join(SRC, "index.ts"))
    route = _between(idx, "async function route(", "async function forwardRoute(")
    meta = _between(route, 'tail.endsWith(".metadata.json")', 'tail.endsWith(".csv")')
    assert meta.index("isWithheld(") < meta.index("handleMetadata("), "metadata is described before the gate"
    csv = _between(route, 'if (tail.endsWith(".csv")) {', "return json(\n")
    assert csv.index("isWithheld(") < csv.index("requireDownloadAuth("), "the csv gate must run BEFORE auth"
    assert csv.index("isWithheld(") < csv.index("if (local) return await handleSeriesCsv("), "the origin too"
    fwd = idx[idx.index("async function forwardRoute("):]
    fwd = fwd[: fwd.index("\n}")]
    assert "isWithheld(await originWithheld(" in fwd, "the forwarding edge must read the ORIGIN's list"
    assert fwd.index("isWithheld(") < fwd.index("requireDownloadAuth("), "edge: withheld before auth (finding 1)"
    assert "keyWithWithheld(cacheKey(request)" in fwd, "edge cache key for catalog/stats lacks the list's tag"
    assert "keyWithWithheld(new Request(url.toString()" in route, "cloud cache keys lack the list's tag"
    assert route.count("keyWithWithheld(") == 2, "one for /v1/catalog, one for /v1/stats"


def test_the_floor_is_the_committed_list():
    r = subprocess.run([sys.executable, os.path.join(ROOT, "tools", "gen_withheld_floor.py"), "--check"],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr


def test_the_floor_check_can_fail(tmp_path, monkeypatch):
    """Planted positive: a floor that is NOT what the JSON gives must be reported stale."""
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import gen_withheld_floor as G
    ts = tmp_path / "withheldFloor.ts"
    ts.write_text(G.render(["insee_melodi:STALE"]), encoding="utf-8")
    monkeypatch.setattr(G, "TS_PATH", str(ts))
    assert G.main(["--check"]) == 1


def test_the_list_validates_with_the_workers_own_rule():
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import json
    import gen_withheld_floor as G
    ids = G.validate(json.load(open(G.JSON_PATH, encoding="utf-8")))
    assert ids and all(G.ID_RE.fullmatch(i) for i in ids)
    for bad in ({"version": 2, "ids": {}}, {"version": 1, "ids": {"x'y:z": {"until": "complete"}}},
                {"version": 1, "ids": {"insee_melodi:A": {"until": "later"}}}):
        try:
            G.validate(bad)
        except SystemExit:
            continue
        raise AssertionError(f"accepted {bad}")


def test_the_router_lets_the_edges_own_list_read_through():
    """The edge fetches /v1/withheld from the origin through the workstation's router; a router that refused it
    would leave the edge on its floor for ever after T0 (finding 1)."""
    sys.path.insert(0, os.path.join(ROOT, "tools", "selfhost"))
    import router
    assert router.forwardable("/v1/withheld")
    assert not router.forwardable("/v1/withheld/x") and not router.forwardable("/v1/withheldx")
