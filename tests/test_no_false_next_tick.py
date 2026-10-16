"""No updater text may promise that the NEXT tick or run does work the scheduler does not do.

base.Strategy.is_due decides when a unit runs again. A unit that ended `partial` and has never
succeeded waits min(cadence, 7) x 0.9 days after that attempt, so a text such as "deferred to the
next tick" or "re-runs next tick" on a partial path is false for it (ledger R1558, R1096; the
eurostat capped path of PRs #152/#153; review AR-322 listed the same words on other paths). Those
texts were reworded to "a later pass ...". This test keeps the false forms out of every string
literal under updater/: messages, notes, exception texts and docstrings. Comments are not read.

WHY AST AND NOT tokenize: from Python 3.12 an f-string is FSTRING_START/MIDDLE/END tokens, not
STRING, so a token scan sees no f-string on the desktop (3.14) while CI (3.11) sees them all; and a
per-token or per-line check misses a phrase split over two implicitly joined literals
("deferred to the next " "tick"). ast joins those, and a JoinedStr is read with each {...} as "{}".

ONE EXEMPTION, the exact sentence "deferred by budget and taken next tick". _common.finalize writes
it, health._DEFERRAL_BASE matches it exactly to tell ROTATING from ATTENTION, and stored state rows
carry it. It is false for a never-succeeded unit too, but changing it changes the matcher - a logic
change, left for its own fix. Only that exact sentence is exempt, never the words "next tick".
"""
from __future__ import annotations

import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UPDATER = ROOT / "updater"

CONTRACT = "deferred by budget and taken next tick"

# "next tick" in any form, and the "next run" forms that were reworded. A "next run" that IS true
# stays allowed: "due (again) at the next run" on a transient_fail path, "when they next run".
FALSE = re.compile(
    r"next\s+tick"
    r"|deferred\s+to\s+(?:the\s+)?next\s+run"
    r"|left\s+for\s+the\s+next\s+run"
    r"|drains?\s+next\s+run"
    r"|decided\s+next\s+run"
    r"|covered\s+by\s+the\s+next\s+run"
    r"|re-derives\s+next\s+run"
    r"|retr(?:y|ied)\s+next\s+run"
    r"|resurfaces\s+next\s+run"
    r"|re-queued\s+for\s+the\s+next"
    r"|the\s+next\s+run\s+resumes\s+after"
    r"|the\s+next\s+run's\s+guard"
    # "pass" is one orchestrator run here, so an unqualified "next pass" is the same promise in new
    # words (review AR-327). "the source's next pass" / "its next pass" name WHERE, not WHEN.
    r"|(?<!source's )(?<!its )next\s+pass"
    r"|following\s+pass",
    re.I)


def _texts(source: str):
    """Every string literal in `source`, implicit concatenations joined, f-strings with {}."""
    out = []
    nodes = list(ast.walk(ast.parse(source)))
    # an f-string's literal pieces are Constant nodes too; read them once, as the joined whole
    parts = {id(v) for n in nodes if isinstance(n, ast.JoinedStr) for v in n.values}
    for node in nodes:
        if isinstance(node, ast.JoinedStr):
            out.append("".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in parts:
            out.append(node.value)
    return out


def _false_promises(source: str):
    hits = []
    for text in _texts(source):
        flat = " ".join(text.split())
        for m in FALSE.finditer(flat.replace(CONTRACT, "")):
            hits.append(m.group(0))
    return hits


# ---- controls: the scanner must be able to fail, through every shape the false texts took ----

@pytest.mark.parametrize("planted", [
    'x = "the unit re-runs next tick"',                                  # plain literal
    'x = f"{n} deferred to the next tick (resuming after {k!r})"',      # f-string
    'x = (f"{n} deferred to the next "\n     f"tick")',                 # split over two literals
    'def f():\n    """Raises TransientError (the key re-runs next tick)."""',   # docstring
    'x = f"they drain next run"',
    'x = "transient_unit() timeout (retry next run)"',
    'x = f"{n} country file(s) DEFERRED to the next run"',
    'x = f"the next run RESUMES AFTER {k}"',
    'x = f"resuming after {t!r} on the next pass (bookmark held)"',
    'x = "the remainder drains on the following pass"',
])
def test_scanner_finds_a_planted_false_promise(planted):
    assert _false_promises(planted), planted


@pytest.mark.parametrize("allowed", [
    'x = "3 sub-unit(s) attempted, none failed; 2 deferred by budget and taken next tick"',
    'def f():\n    """the note (\'M deferred by budget and\n    taken next tick\', finalize)"""',
    'x = f"cap; the unit is due again at the next run (+{n} rows)"',
    'x = "sources are checked again when they next run."',
    'x = f"the source\'s next pass RESUMES AFTER {k}"',
    'x = " - escalating on its next pass"',
    'x = f"{n} deferred to a later pass (resuming after {k!r})"',
    '# a comment saying next tick is not read\nx = 1',
])
def test_scanner_passes_a_true_or_exempt_text(allowed):
    assert not _false_promises(allowed), allowed


def test_the_exemption_is_the_exact_sentence_only():
    # The contract sentence beside a second, real false promise: the second must still be found.
    assert _false_promises('x = "2 deferred by budget and taken next tick; rest next tick"') == ["next tick"]


# ---- the real check ----

def test_no_updater_text_promises_the_next_tick_or_run():
    files = sorted(UPDATER.rglob("*.py"))
    n_texts = 0
    found = []
    for p in files:
        src = p.read_text(encoding="utf-8")
        n_texts += len(_texts(src))
        found += [f"{p.relative_to(ROOT).as_posix()}: {h!r}" for h in _false_promises(src)]
    # the scan must have read the tree, or an empty result means nothing
    assert len(files) > 150 and n_texts > 10_000, (len(files), n_texts)
    assert not found, ("a text promises work at the next tick/run that base.Strategy.is_due does not "
                       "schedule; say 'a later pass ...' (or the real wait):\n  " + "\n  ".join(found))
