"""catalog/gen_site.py: the public page shows catalogue `sec_edgar` (XBRL) as wired because sec-edgar-daily.yml
refreshes it - not because the 13F registry entry used to share its id (renamed sec_edgar_13f, R275/R1197).
Without the rule, load_wiring() gives sec_edgar False and the page says "not yet wired" (review R1197).

gen_site.py is heavy at import (it reads the catalogue), so only load_wiring and the constant it needs are
executed, straight from the file."""
import ast
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEN = os.path.join(ROOT, "catalog", "gen_site.py")


def _load_wiring():
    tree = ast.parse(open(GEN, encoding="utf-8").read())
    keep = [n for n in tree.body
            if (isinstance(n, ast.FunctionDef) and n.name == "load_wiring")
            or (isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "_FETCHER_BACKED" for t in n.targets))]
    assert len(keep) == 2, "load_wiring or _FETCHER_BACKED moved - re-point this test"
    ns = {"os": os, "re": re, "json": json, "HERE": os.path.dirname(GEN)}
    exec(compile(ast.Module(body=keep, type_ignores=[]), GEN, "exec"), ns)   # noqa: S102
    return ns["load_wiring"]()


def test_the_xbrl_catalogue_id_reads_as_wired():
    w = _load_wiring()
    assert w.get("sec_edgar") is True, w.get("sec_edgar")
    assert "sec_edgar_13f" in w, "the 13F entry is a registry id of its own"
