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


def _runs_where(page):
    return next(ln for ln in page.splitlines() if ln.startswith("| runs where |"))


def test_an_archival_source_page_does_not_say_it_runs_in_the_cloud():
    """AR-355. A source in updater/discontinued.yaml has no registry entry, so `run_location` fell to its default
    and the page said "runs where: cloud (GitHub Actions)". It runs nowhere. Negative control: an unregistered id
    that is NOT in the file still gets the old default, so the archival line comes from the file, not from the
    missing registry entry. Also pins the committed unctad_trademerchgr page (regenerated with this change)."""
    gen = _load("gen_runbook_t3", os.path.join("tools", "gen_runbook.py"))
    disc = gen.discontinued_ids()
    assert "unctad_trademerchgr" in disc and len(disc) >= 53
    args = ({}, {}, {}, {}, set())
    for sid in ("unctad_trademerchgr", "unesco_clte", "imf_ifs"):
        row = _runs_where(gen.render(sid, *args))
        assert "nowhere" in row and "archival copy" in row and "cloud" not in row, (sid, row)
    assert "cloud (GitHub Actions)" in _runs_where(gen.render("zz_not_listed_anywhere", *args))
    page = open(os.path.join(ROOT, "docs", "runbook", "unctad_trademerchgr.md"), encoding="utf-8").read()
    row = _runs_where(page)
    assert "nowhere" in row and "cloud" not in row, row
