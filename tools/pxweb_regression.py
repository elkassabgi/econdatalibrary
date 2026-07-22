"""tools/pxweb_regression.py — safety net for core/pxweb.py (the shared PxWeb
time-axis resolver). Fully local, no network.

  CHECK 1 — SUPERSET: core.pxweb.TIME_CODES must contain every time-dimension name
    any per-source parser recognises. It re-derives the names straight from the
    source files, so if a future edit drops one (exactly what froze hagstofa
    MAN00000 — missing Icelandic "ar"), this fails and names the missing token.

  CHECK 2 — RESOLVER UNIT: resolve_time_dim must pick the right axis on synthetic
    cubes mirroring the real failure modes — month-index BEFORE year, historical
    1703, projection 2085, authoritative flag, and degenerate index-only (None).

Run:  python tools/pxweb_regression.py      # exit 0 = all pass, 1 = any fail

The heavier LIVE replay (old parser vs new resolver over every cached table,
asserting byte-identical axis on working tables) is a separate pass gated on this.
"""
from __future__ import annotations
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from core import pxweb  # noqa: E402

# Every file carrying a PxWeb time-name list (module TIME_CODES or an inline
# is_time_dim `code ... in (...)`). Grepped 2026-07-21.
SOURCE_FILES = [
    "jobs/ingest_hagstofa.py", "jobs/ingest_pxweb.py", "jobs/ingest_scb.py",
    "jobs/ingest_ssb.py", "jobs/ingest_statfin.py", "jobs/ingest_stat_estonia.py",
    "jobs/ingest_stat_slovenia.py", "jobs/ingest_dst.py", "jobs/ingest_bfs.py",
    "updater/strategies/fetchers/ssb.py", "updater/strategies/fetchers/statfin.py",
    "updater/strategies/fetchers/stat_slovenia.py",
]

_DEF_RE = re.compile(r"TIME_CODES\s*=|(?:code\w*|\.lower\(\))\s*in\s*\(")
_TOK_RE = re.compile(r'"([^"\\]+)"')
# aggregate / total selection codes that share the `... in (...)` shape but are NOT
# time-dimension names (bfs/statfin/etc. collapse non-time dims to these).
_AGG = {"t", "tot", "total", "sss", "all", "alle", "tous", "heild", "kokku",
        "kaikki", "yhteensa", "yhteensä", "hele", "na", "item", "*"}


def _extract(rel: str) -> set[str]:
    """Pull the quoted tokens from each time-name definition block in one file."""
    toks: set[str] = set()
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        lines = f.readlines()
    i = 0
    while i < len(lines):
        if _DEF_RE.search(lines[i]):
            blob = lines[i]
            depth = blob.count("(") - blob.count(")")
            j = i + 1
            while depth > 0 and j < len(lines):
                blob += lines[j]
                depth += lines[j].count("(") - lines[j].count(")")
                j += 1
            for t in _TOK_RE.findall(blob):
                tl = t.strip().lower()
                # plausible time-name token: short, single word, not a regex/format
                if (tl and len(tl) <= 18 and " " not in tl
                        and not re.search(r"[\^\\{}\[\]%]", tl)
                        and not tl.isdigit() and tl not in _AGG):
                    toks.add(tl)
            i = max(j, i + 1)
        else:
            i += 1
    return toks


def check_superset() -> list[str]:
    problems = []
    covered = pxweb.TIME_CODES
    for rel in SOURCE_FILES:
        src = _extract(rel)
        missing = sorted(t for t in src if t not in covered)
        tag = "OK " if not missing else "MISS"
        print(f"  [{tag}] {rel:<48} {len(src)} tokens" + (f"  MISSING: {missing}" if missing else ""))
        if missing:
            problems.append(f"{rel}: {missing}")
    return problems


def _cube(dim_ids, dim_codes, meta_time_code=None, role_time=None):
    return pxweb.resolve_time_dim(dim_ids, dim_codes, meta_time_code=meta_time_code,
                                  role_time=role_time)


def check_resolver() -> list[str]:
    """Each case: (name, dim_ids, dim_codes, kwargs, expected_index)."""
    yrs = [str(y) for y in range(2000, 2021)]
    months = [str(m) for m in range(0, 12)]          # bare month indices -> parse to None
    hist = [str(y) for y in range(1703, 2027)]       # Statistics Iceland MAN00000
    proj = [str(y) for y in range(2024, 2086)]       # projection to 2085
    cases = [
        # month axis FIRST, year second (the hagstofa/statfin freeze): must pick YEAR (idx 1)
        ("month-before-year", ["Manudur", "Ar"], [months, yrs], {}, 1),
        # same but with Icelandic accented ids and NO flag (MAN00000 shape): pick year
        ("iceland-hist-noflag", ["Manudur", "Ar"], [months, hist], {}, 1),
        # authoritative flag wins even if it points at the "wrong-looking" axis
        ("flag-authority", ["Manudur", "Ar"], [months, yrs], {"meta_time_code": "Manudur"}, 0),
        # role.time authority
        ("role-time", ["Ar", "Kon"], [yrs, ["M", "K"]], {"role_time": ["Kon"]}, 1),
        # projection years to 2085 still recognised as the date axis
        ("projection-2085", ["Region", "Tid"], [["0114", "1280", "2584"], proj], {}, 1),
        # single clean year axis
        ("single-year", ["Tid"], [yrs], {}, 0),
        # degenerate: only index/category codes, no dates -> None
        ("degenerate-none", ["A", "B"], [["0", "1", "2"], ["x", "y"]], {}, None),
        # year + quarter both parse; quarter is finer but year is the named ISO axis;
        # both are date-parseable so tie-break prefers the named one (either is defensible,
        # but must NOT be None and must be a real date axis)
        ("year-plus-quarter", ["Ar", "Kvartal"], [yrs, ["2019Q1", "2019Q2", "2019Q3", "2019Q4"]], {}, "notnone"),
    ]
    problems = []
    for name, ids, codes, kw, exp in cases:
        got = _cube(ids, codes, **kw)
        if exp == "notnone":
            ok = got is not None
        else:
            ok = got == exp
        print(f"  [{'OK ' if ok else 'FAIL'}] {name:<22} -> idx {got}  (expected {exp})")
        if not ok:
            problems.append(f"{name}: got {got}, expected {exp}")
    return problems


def main() -> int:
    print("CHECK 1 — TIME_CODES superset (re-derived from source):")
    p1 = check_superset()
    print("\nCHECK 2 — resolve_time_dim unit cases:")
    p2 = check_resolver()
    print()
    if p1 or p2:
        print(f"FAIL: {len(p1)} superset gap(s), {len(p2)} resolver failure(s)")
        return 1
    print("ALL PASS — resolver superset + failure-mode selection verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
