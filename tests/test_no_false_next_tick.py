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

ONE EXEMPTION, AND IT IS ONE AST NODE, NOT A SENTENCE. _common.finalize used to write "... deferred by
budget and taken next tick"; it now writes "..., not reached in this pass". Stored state rows still carry
the old words, so health._DEFERRAL_BASE accepts both tails, and the old tail lives in exactly one place:
the string assigned to the module-level name _DEFERRAL_OLD in updater/health.py. That one Constant node
is skipped, and the real check asserts it was skipped exactly once - so the old sentence anywhere else
(finalize, a message, a docstring, a second constant) is a finding, and so is any other value given to
that name.
"""
from __future__ import annotations

import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UPDATER = ROOT / "updater"

# The one exempt node: `_DEFERRAL_OLD = "<exactly this>"` at module level in updater/health.py.
EXEMPT_FILE = "updater/health.py"
EXEMPT_NAME = "_DEFERRAL_OLD"
EXEMPT_VALUE = "deferred by budget and taken next tick"

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


def _exempt_nodes(tree, rel: str):
    """ids of the Constant nodes that are exempt in this file: only `_DEFERRAL_OLD = EXEMPT_VALUE`,
    a plain module-level assignment to that one name, in EXEMPT_FILE. Nothing else, anywhere."""
    if rel != EXEMPT_FILE:
        return set()
    return {id(n.value) for n in tree.body
            if isinstance(n, ast.Assign) and len(n.targets) == 1
            and isinstance(n.targets[0], ast.Name) and n.targets[0].id == EXEMPT_NAME
            and isinstance(n.value, ast.Constant) and n.value.value == EXEMPT_VALUE}


def _scan(source: str, rel: str = ""):
    """(every string literal in `source` that is not exempt, number of exempt literals skipped).
    Implicit concatenations are joined, f-strings read with {}."""
    out = []
    tree = ast.parse(source)
    exempt = _exempt_nodes(tree, rel)
    nodes = list(ast.walk(tree))
    # an f-string's literal pieces are Constant nodes too; read them once, as the joined whole
    parts = {id(v) for n in nodes if isinstance(n, ast.JoinedStr) for v in n.values}
    skipped = 0
    for node in nodes:
        if isinstance(node, ast.JoinedStr):
            out.append("".join(v.value if isinstance(v, ast.Constant) else "{}" for v in node.values))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in parts:
            if id(node) in exempt:
                skipped += 1
                continue
            out.append(node.value)
    return out, skipped


def _texts(source: str, rel: str = ""):
    return _scan(source, rel)[0]


def _false_promises(source: str, rel: str = ""):
    hits = []
    for text in _texts(source, rel):
        flat = " ".join(text.split())
        for m in FALSE.finditer(flat):
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
    # the old finalize sentence is no longer exempt anywhere but the one health constant
    'x = "3 sub-unit(s) attempted, none failed; 2 deferred by budget and taken next tick"',
    'x = (f"{a} sub-unit(s) attempted, none failed; "\n     f"{d} deferred by budget and taken next tick")',
    'def f():\n    """the note (\'M deferred by budget and\n    taken next tick\', finalize)"""',
])
def test_scanner_finds_a_planted_false_promise(planted):
    assert _false_promises(planted), planted
    assert _false_promises(planted, EXEMPT_FILE), "outside the one exempt node, health.py is scanned like any file"


@pytest.mark.parametrize("planted, rel", [
    (f'{EXEMPT_NAME} = "{EXEMPT_VALUE}"', "updater/strategies/fetchers/_common.py"),   # the name, another file
    (f'_DEFERRAL_NOW = "{EXEMPT_VALUE}"', EXEMPT_FILE),                                 # another name
    (f'{EXEMPT_NAME} = "{EXEMPT_VALUE}; rest next tick"', EXEMPT_FILE),                 # another value
    (f'{EXEMPT_NAME} = "{EXEMPT_VALUE} [x]"', EXEMPT_FILE),
    (f'{EXEMPT_NAME} = "{EXEMPT_VALUE}"\nY = "{EXEMPT_VALUE}"', EXEMPT_FILE),            # the node, plus a copy
    (f'{EXEMPT_NAME} = Z = "{EXEMPT_VALUE}"', EXEMPT_FILE),                             # two targets
    (f'def f():\n    {EXEMPT_NAME} = "{EXEMPT_VALUE}"', EXEMPT_FILE),                    # not module level
    (f'{EXEMPT_NAME} = f"{{a}} {EXEMPT_VALUE}"', EXEMPT_FILE),                           # an f-string
])
def test_the_exemption_is_one_node_and_nothing_wider(planted, rel):
    assert _false_promises(planted, rel), (planted, rel)


def test_the_exempt_node_itself_is_skipped():
    src = f'{EXEMPT_NAME} = "{EXEMPT_VALUE}"   # the stored form'
    assert _scan(src, EXEMPT_FILE) == ([], 1)
    assert _false_promises(src) == ["next tick"], "the same line in any other file is a finding"


@pytest.mark.parametrize("allowed", [
    'x = "3 sub-unit(s) attempted, none failed; 2 deferred by budget, not reached in this pass"',
    'x = f"cap; the unit is due again at the next run (+{n} rows)"',
    'x = "sources are checked again when they next run."',
    'x = f"the source\'s next pass RESUMES AFTER {k}"',
    'x = " - escalating on its next pass"',
    'x = f"{n} deferred to a later pass (resuming after {k!r})"',
    '# a comment saying next tick is not read\nx = 1',
])
def test_scanner_passes_a_true_or_exempt_text(allowed):
    assert not _false_promises(allowed), allowed


# ---- the real check ----

def test_no_updater_text_promises_the_next_tick_or_run():
    files = sorted(UPDATER.rglob("*.py"))
    n_texts = 0
    exempt_used = {}
    found = []
    for p in files:
        rel = p.relative_to(ROOT).as_posix()
        src = p.read_text(encoding="utf-8")
        texts, skipped = _scan(src, rel)
        n_texts += len(texts)
        if skipped:
            exempt_used[rel] = skipped
        found += [f"{rel}: {h!r}" for h in _false_promises(src, rel)]
    # the scan must have read the tree, or an empty result means nothing
    assert len(files) > 150 and n_texts > 10_000, (len(files), n_texts)
    assert not found, ("a text promises work at the next tick/run that base.Strategy.is_due does not "
                       "schedule; say 'a later pass ...' (or the real wait):\n  " + "\n  ".join(found))
    # the exemption must be in use exactly once: the stored-form constant health still reads. If it is
    # gone (the matcher stopped reading old rows) or doubled, this test is the place that says so.
    assert exempt_used == {EXEMPT_FILE: 1}, exempt_used


def test_the_exempt_constant_is_what_the_matcher_reads():
    """The exemption is only honest while the constant is the matcher's old-tail alternative."""
    import sys
    sys.path.insert(0, str(ROOT))
    from updater import health
    assert health._DEFERRAL_OLD == EXEMPT_VALUE
    old = f"3 sub-unit(s) attempted, none failed; 2 {EXEMPT_VALUE}"
    assert health._DEFERRAL_BASE.match(old), old

# ---- the exempt constant's NAME: read in one place only ----
# The exemption above skips one Constant node. It does not stop the name being imported and used to
# emit the old words: `from updater.health import _DEFERRAL_OLD as _O; note = f"{n} {_O}"` reads as
# "{} {}" to the literal scan and survived every test (review AR-328, mutant M10, R1586). So the name
# itself is pinned: it may be loaded only inside the value of the module-level `_DEFERRAL_BASE = ...`
# in updater/health.py, exactly once. Any import, attribute read, getattr string or other load is a
# finding.
MATCHER_NAME = "_DEFERRAL_BASE"


def _matcher_value_ids(tree, rel: str):
    """ids of every node inside the value of the module-level `_DEFERRAL_BASE = ...` in EXEMPT_FILE."""
    if rel != EXEMPT_FILE:
        return set()
    out = set()
    for n in tree.body:
        if (isinstance(n, ast.Assign) and len(n.targets) == 1 and isinstance(n.targets[0], ast.Name)
                and n.targets[0].id == MATCHER_NAME):
            out |= {id(x) for x in ast.walk(n.value)}
    return out


def _name_uses(source: str, rel: str = ""):
    """(uses of EXEMPT_NAME outside the allowed spot, number of allowed loads). The definition
    `_DEFERRAL_OLD = ...` (a Store) is not a use."""
    tree = ast.parse(source)
    allowed = _matcher_value_ids(tree, rel)
    bad, ok = [], 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == EXEMPT_NAME:
            if isinstance(node.ctx, ast.Store):
                continue
            if id(node) in allowed:
                ok += 1
            else:
                bad.append(f"line {node.lineno}: name {type(node.ctx).__name__}")
        elif isinstance(node, ast.Attribute) and node.attr == EXEMPT_NAME:
            bad.append(f"line {node.lineno}: attribute .{EXEMPT_NAME}")
        elif isinstance(node, (ast.ImportFrom, ast.Import)):
            for a in node.names:
                if a.name == EXEMPT_NAME or a.asname == EXEMPT_NAME:
                    bad.append(f"line {node.lineno}: import {a.name}")
        elif isinstance(node, ast.Constant) and node.value == EXEMPT_NAME:
            bad.append(f"line {node.lineno}: the name as a string (getattr/dict key)")
    return bad, ok


_REAL_MATCHER = (f'{EXEMPT_NAME} = "{EXEMPT_VALUE}"\n'
                 f'{MATCHER_NAME} = re.compile(r"^x (?:" + re.escape(N) + "|" + re.escape({EXEMPT_NAME}) + r")$")')


@pytest.mark.parametrize("planted, rel", [
    # M10: the name imported under an alias and emitted through an f-string
    (f'from updater.health import {EXEMPT_NAME} as _O\nnote = f"2 {{_O}}"', "updater/strategies/fetchers/boc.py"),
    (f'from updater.health import {EXEMPT_NAME}\nnote = "2 " + {EXEMPT_NAME}', "updater/strategies/fetchers/boc.py"),
    (f'from updater import health\nnote = f"2 {{health.{EXEMPT_NAME}}}"', "updater/orchestrate.py"),
    (f'import updater.health as h\nnote = getattr(h, "{EXEMPT_NAME}")', "updater/orchestrate.py"),
    # inside health.py itself, anywhere but the matcher's value
    (_REAL_MATCHER + f'\nNOTE = f"2 {{{EXEMPT_NAME}}}"', EXEMPT_FILE),
    (_REAL_MATCHER + f'\ndef f():\n    return {EXEMPT_NAME}', EXEMPT_FILE),
    (_REAL_MATCHER + f'\nOTHER = re.compile(re.escape({EXEMPT_NAME}))', EXEMPT_FILE),
    # a second `_DEFERRAL_BASE = ...` that is not the module-level one
    (_REAL_MATCHER + f'\ndef f():\n    {MATCHER_NAME} = {EXEMPT_NAME}\n    return {MATCHER_NAME}', EXEMPT_FILE),
    (_REAL_MATCHER + f'\nclass K:\n    {MATCHER_NAME} = f"2 {{{EXEMPT_NAME}}}"', EXEMPT_FILE),
    # the matcher's shape in another file is not the matcher
    (_REAL_MATCHER, "updater/send_digest.py"),
])
def test_the_old_tail_name_cannot_be_reused(planted, rel):
    bad, _ = _name_uses(planted, rel)
    assert bad, (planted, rel)


def test_the_matcher_load_itself_is_allowed():
    assert _name_uses(_REAL_MATCHER, EXEMPT_FILE) == ([], 1)


def test_the_old_tail_name_is_read_only_by_the_matcher():
    files = sorted(UPDATER.rglob("*.py"))
    found, allowed = [], {}
    for p in files:
        rel = p.relative_to(ROOT).as_posix()
        bad, ok = _name_uses(p.read_text(encoding="utf-8"), rel)
        found += [f"{rel}: {b}" for b in bad]
        if ok:
            allowed[rel] = ok
    assert len(files) > 150, len(files)
    assert not found, ("the stored-form constant is read outside health._DEFERRAL_BASE; that re-emits "
                       "the old promise through the name:\n  " + "\n  ".join(found))
    assert allowed == {EXEMPT_FILE: 1}, allowed
