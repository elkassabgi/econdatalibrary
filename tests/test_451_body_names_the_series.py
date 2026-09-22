"""Both series routes' 451 bodies carry `series_id` - the id the caller asked for (2026-09-17).

A client refused on several ids (a notebook looping over a list, the bundle route's per-id fetches) otherwise gets
identical `{"error":"not_redistributable","detail":...}` bodies and cannot tell which request each one answers. The
field echoes only what the caller sent, so it discloses nothing (api/CONTRACT.md makes the same argument for
`econdl:unresolved`).

Read from the shipped source with comments stripped, the same approach as test_metadata_route_is_gated.py. The
negative control proves the extractor finds the 451 call at all, so a missing branch cannot pass as "no field".
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_TS = os.path.join(ROOT, "api", "worker", "src", "index.ts")


def _code() -> str:
    src = open(INDEX_TS, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//.*", "", src)


def _gate_response(suffix: str) -> str:
    code = _code()
    i = code.index(f'tail.endsWith("{suffix}")')
    rest = code[i + 1:]
    j = rest.find("tail.endsWith(")
    branch = rest[:j] if j > 0 else rest
    m = re.search(r"if\s*\(\s*isGated\(id\)\s*\)\s*\{\s*(return\s+json\(.*?,\s*451\s*\)\s*;)", branch, re.S)
    assert m, f"no isGated(id) block in the {suffix} branch"
    return m.group(1)


def test_the_extractor_finds_both_451_calls():
    for suffix in (".metadata.json", ".csv"):
        body = _gate_response(suffix)
        assert "451" in body and "not_redistributable" in body, suffix


def test_metadata_451_names_the_series():
    assert re.search(r"series_id\s*:\s*id\b", _gate_response(".metadata.json"))


def test_csv_451_names_the_series():
    assert re.search(r"series_id\s*:\s*id\b", _gate_response(".csv"))
