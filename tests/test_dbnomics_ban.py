"""DBnomics is BANNED. This test makes the ban mechanical instead of remembered.

HISTORY. The ban is in CLAUDE.md §0 and ledger R251. It was enforced by memory, and memory
failed repeatedly across sessions: relay clients survived in connectors/ after a "repo-wide"
sweep said they were gone (R311/#49), and sessions kept re-proposing DBnomics as a data path.
Tasks #48/#49/#68 finally removed every live fetch path on 2026-08-0x. This test is the layer
that does not depend on anyone remembering: any push that reintroduces the domain fails CI.

WHAT IS BANNED: reaching db.nomics.world in RUNTIME code — fetching, probing, relaying,
mirroring. Mentioning the ban in a comment is obviously fine, so matches on lines that are
clearly comments about the ban are allowed via a narrow, per-line allowlist test rather than a
blanket exclusion.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Runtime surfaces. docs/, the ledger, and tests are deliberately excluded — describing the ban
# is not violating it.
RUNTIME_DIRS = ["updater", "core", "jobs", "tools", "connectors",
                os.path.join("api", "worker", "src"), os.path.join("clients", "python")]

DOMAIN = re.compile(r"db\.nomics\.world", re.IGNORECASE)

# A line may mention the domain ONLY while talking about the ban itself.
ALLOWED_LINE = re.compile(r"(banned|ban\b|never|forbidden|R251|do not|don't)", re.IGNORECASE)

# A DEFUSED module is exempt wholesale: the codebase's established treatment for retired
# relay-era code is a raise at the top, BEFORE any imports, naming the ban — so everything
# below it is unreachable. connectors/dbnomics/connector.py and fetchers/_dbnomics.py set the
# pattern; the six jobs/ scripts and two tools/ probes got the same treatment on 2026-08-04.
_DEFUSED = re.compile(
    r"raise (SystemExit|ImportError)\((?:[^)]|\n)*?(R251|BANNED|banned)", re.MULTILINE)


def _is_defused(text: str) -> bool:
    head = "\n".join(text.splitlines()[:90])
    return bool(_DEFUSED.search(head))


def _scan(root: str):
    hits = []
    for d in RUNTIME_DIRS:
        base = os.path.join(root, d)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [x for x in dirnames if x not in
                           ("node_modules", "__pycache__", ".git")]
            for fn in filenames:
                if not fn.endswith((".py", ".ts", ".js", ".yaml", ".yml", ".toml", ".json")):
                    continue
                p = os.path.join(dirpath, fn)
                try:
                    text = open(p, encoding="utf-8", errors="replace").read()
                except OSError:
                    hits.append((p, 0, "<unreadable — counts as a hit, not a skip>"))
                    continue
                if fn.endswith(".py") and _is_defused(text):
                    continue
                for i, line in enumerate(text.splitlines(), 1):
                    if DOMAIN.search(line) and not ALLOWED_LINE.search(line):
                        hits.append((os.path.relpath(p, root), i, line.strip()[:100]))
    return hits


def test_no_dbnomics_in_runtime_code():
    hits = _scan(ROOT)
    assert not hits, (
        "db.nomics.world found in runtime code — the ban (CLAUDE.md §0, R251) is mechanical "
        "now, not advisory. Remove the reference or, if the line is genuinely ABOUT the ban, "
        "make that explicit on the same line:\n  "
        + "\n  ".join(f"{p}:{ln}: {t}" for p, ln, t in hits[:20]))


def test_the_scanner_actually_detects_the_domain(tmp_path):
    """A guard that cannot fail is not a guard (R346): plant a violation, prove it is found."""
    d = tmp_path / "updater"
    d.mkdir()
    (d / "planted.py").write_text('URL = "https://api.db.nomics.world/v22/series"\n',
                                  encoding="utf-8")
    hits = _scan(str(tmp_path))
    assert len(hits) == 1 and hits[0][1] == 1, "scanner missed a planted violation"

    # and the allowlist works: a line ABOUT the ban is not a hit
    (d / "planted.py").write_text('# db.nomics.world is BANNED (R251) — never fetch it\n',
                                  encoding="utf-8")
    assert _scan(str(tmp_path)) == [], "allowlist failed — comments about the ban must pass"

    # and the defused-module exemption works — but ONLY when the raise names the ban
    (d / "planted.py").write_text(
        'raise SystemExit(\n    "RETIRED: BANNED (R251)")\n'
        'API = "https://api.db.nomics.world/v22"\n', encoding="utf-8")
    assert _scan(str(tmp_path)) == [], "defused-module exemption failed"
    (d / "planted.py").write_text(
        'raise SystemExit("unrelated error")\n'
        'API = "https://api.db.nomics.world/v22"\n', encoding="utf-8")
    assert len(_scan(str(tmp_path))) == 1, (
        "a raise that does NOT name the ban must not exempt the file — otherwise any "
        "error-raising module becomes a free pass")
