"""gen_runbook's served-set must equal the coverage audit's — one structure, ONE parser.

The two tools each parsed SUPPORTED_SOURCES out of util.ts with their own regex (the R333
two-parsers drift class). gen_runbook's matched only one-id-per-line entries, so every source
on a packed line ("imf_bop", "imf_cdis", "imf_cpis", "imf_dot",) rendered a runbook page
claiming NOT SERVED while the live API listed it — the instrument behind the per-source
5-reads was lying. These tests pin the property that matters:

  1. on the REAL util.ts, the two harvests are identical (drift alarm — fails the moment
     either parser is edited alone);
  2. on a fixture exercising the known failure shapes (packed lines, trailing comments,
     quoted words inside prose comments), the harvest returns exactly the real ids.
"""
from __future__ import annotations

import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _load(modname, relpath):
    spec = importlib.util.spec_from_file_location(modname, os.path.join(ROOT, relpath))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_runbook_and_coverage_audit_harvest_identically_from_real_utilts():
    gen = _load("gen_runbook_t", os.path.join("tools", "gen_runbook.py"))
    aud = _load("audit_cov_t", os.path.join("tools", "audit_schedule_coverage.py"))
    a, b = gen.load_served(), aud.supported_sources()
    assert a == b, (f"served-set drift: runbook-only={sorted(a - b)[:5]} "
                    f"audit-only={sorted(b - a)[:5]}")
    # the original defect's fingerprint: packed-line ids must be present
    # Canary refreshed 2026-08-07: the original four (imf_bop/cdis/cpis/dot) were RETIRED
    # in the Class A wave; these four live on packed lines in today's util.ts.
    assert {"idb", "ilostat", "imf_fsire", "imf_gender_budgeting"} <= a, (
        "packed-line ids missing — the one-id-per-line regex is back")


def test_harvest_handles_packed_lines_and_comment_prose(tmp_path, monkeypatch):
    gen = _load("gen_runbook_t2", os.path.join("tools", "gen_runbook.py"))
    fixture = tmp_path / "util.ts"
    fixture.write_text(
        'export const SUPPORTED_SOURCES: readonly string[] = [\n'
        '  "alpha",\n'
        '  // a comment mentioning "fake_id" in prose must NOT be harvested\n'
        '  "bravo", "charlie", "delta",   // packed line + trailing comment\n'
        '  /* block comment with "another_fake" */\n'
        '  "echo",\n'
        '] ;\n', encoding="utf-8")
    monkeypatch.setattr(gen, "UTIL_TS", str(fixture))
    got = gen.load_served()
    assert got == {"alpha", "bravo", "charlie", "delta", "echo"}, got
