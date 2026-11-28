"""REVIEWER PROBES (round 4) for tools/selfhost/statcan_lost_cubes.py refused()/main() at PR #343 head 15a041285.
Written by the code reviewer (review_code/test_review_b3_census.py); copied into the branch unchanged, as regression
tests, at the coordinator's order after round 4 (F1). Windows only (they test Windows path semantics). Two kinds:

  A. The REAL live roots (E:\\research\\econfindatalibrary\\data, E:\\econ_live) with many spellings. EVERY function that
     could reach the disk is replaced by a spy that RAISES for a live-looking argument BEFORE delegating (so no live
     path ever reaches the OS), plus a process audit hook that raises on open/listdir/scandir/sqlite3.connect of a
     live-looking path while armed. A probe passes only if refused() refuses and NO spy saw a call.
  B. A FAKE live root in this test's tmp folder on F: (refused(path, roots=[fake])) to hunt spellings whose TEXT
     passes but that the OS resolves into the root - there the spies record (and pass through) and the probe
     reports whether the guard opened anything under the fake root.
"""
from __future__ import annotations

import builtins
import importlib.util
import ntpath
import os
import re
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows path semantics")

_spec = importlib.util.spec_from_file_location("_census_under_review",
                                               os.path.join(ROOT, "tools", "selfhost", "statcan_lost_cubes.py"))
C = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(C)

_LIVE_RE = re.compile(r"(^|[\\/?.:])e:|e\$|econ_live|econfindatalibrary", re.I)


def _live(arg) -> bool:
    try:
        s = os.fspath(arg)
    except TypeError:
        s = str(arg)
    if isinstance(s, bytes):
        s = s.decode("utf-8", "replace")
    return bool(_LIVE_RE.search(str(s)))


_ARMED = {"on": False, "seen": []}


def _audit(event, args):
    if not _ARMED["on"]:
        return
    if event in ("open", "os.listdir", "os.scandir", "sqlite3.connect", "os.chdir"):
        if args and _live(args[0]):
            _ARMED["seen"].append((event, str(args[0])))
            raise PermissionError(f"REVIEW AUDIT HOOK BLOCKED {event} on {args[0]!r}")


sys.addaudithook(_audit)


class Blocked(Exception):
    pass


@pytest.fixture
def spies(monkeypatch):
    """Replace every disk-reaching function the tool can call. A live-looking argument RAISES before delegating."""
    calls = []

    def wrap(name, real):
        def spy(*a, **k):
            arg = a[0] if a else None
            calls.append((name, str(arg)))
            if arg is not None and _live(arg):
                raise Blocked(f"BLOCKED {name}({arg!r}) - a live-looking path reached a disk call")
            return real(*a, **k)
        return spy
    for mod, names in ((ntpath, ("realpath", "isfile", "isdir", "exists", "lexists", "islink", "samefile",
                                 "getsize", "getmtime", "isjunction", "isreserved")),
                       (os, ("stat", "lstat", "listdir", "scandir", "open", "readlink"))):
        for n in names:
            if hasattr(mod, n):
                monkeypatch.setattr(mod, n, wrap(f"{mod.__name__}.{n}", getattr(mod, n)))
    for n in ("_getfinalpathname", "_getfinalpathname_nonstrict"):
        if hasattr(ntpath, n):
            monkeypatch.setattr(ntpath, n, wrap(f"ntpath.{n}", getattr(ntpath, n)))
    monkeypatch.setattr(builtins, "open", wrap("builtins.open", builtins.open))
    monkeypatch.setattr(sqlite3, "connect", wrap("sqlite3.connect", sqlite3.connect))
    monkeypatch.setattr(C.sqlite3, "connect", wrap("C.sqlite3.connect", sqlite3.connect))
    _ARMED.update(on=True, seen=[])
    yield calls
    _ARMED["on"] = False


LIVE_SPELLINGS = [
    r"E:\research\econfindatalibrary\data\catalog.db",
    r"e:\RESEARCH\EconFinDataLibrary\DATA\catalog.db",
    "E:/research/econfindatalibrary/data/clean_full/statcan/_lane_state.json",
    r"E:\research\econfindatalibrary\data\..\data\catalog.db",
    r"E:\research\x\..\econfindatalibrary\data\catalog.db",
    r"E:\research\econfindatalibrary\.\data\.\catalog.db",
    r"E:\\research\\\econfindatalibrary\\data\\catalog.db",
    r"\\?\E:\research\econfindatalibrary\data\catalog.db",
    r"\\.\E:\research\econfindatalibrary\data\catalog.db",
    r"\??\E:\econ_live\state\writer.lock",
    "//?/E:/econ_live/state/writer.lock",
    r"\\?\UNC\localhost\E$\research\econfindatalibrary\data\catalog.db",
    r"\\localhost\E$\research\econfindatalibrary\data\catalog.db",
    r"\\127.0.0.1\e$\econ_live\state",
    r"\\?\GLOBALROOT\Device\HarddiskVolume3\research\econfindatalibrary\data\catalog.db",
    r"\\?\Volume{00000000-0000-0000-0000-000000000000}\research\econfindatalibrary\data\catalog.db",
    r"E:\econ_live:stream",
    r"E:\econ_live::$DATA",
    r"E:\research\econfindatalibrary\data:x",
    r"E:\research\econfindatalibrary\data\catalog.db:Zone.Identifier",
    r"E:\RESEAR~1\ECONFI~1\data\catalog.db",
    r"E:\ECON_L~1\state\writer.lock",
    r"E:\research\econfindatalibrary\data.\catalog.db",
    r"E:\econ_live.\state",
    r"E:\econ_live",
    r"E:\research\econfindatalibrary\data",
    r"E:\research\econfindatalibrary\data\\",
]


@pytest.mark.parametrize("spelling", LIVE_SPELLINGS)
def test_A_every_live_spelling_is_refused_on_text_and_nothing_reaches_the_disk(spies, spelling):
    why = C.refused(spelling)
    print(f"A {spelling!r} -> {why!r}; disk calls {spies}")
    assert why and why.startswith("REFUSED"), why
    assert spies == [], f"a disk-reaching function was called: {spies}"
    assert _ARMED["seen"] == []


@pytest.mark.parametrize("spelling", [r"E:\research\econfindatalibrary\data\catalog.db",
                                      r"\\localhost\E$\research\econfindatalibrary\data\catalog.db",
                                      r"\??\E:\econ_live\state\state.db"])
@pytest.mark.parametrize("arg", ["--state", "--catalogue", "--store-dir", "--store-listing"])
def test_A_main_returns_2_for_a_live_or_unc_input_and_touches_nothing(spies, tmp_path, spelling, arg, capsys):
    st = tmp_path / "s.json"
    cat = tmp_path / "c.db"
    lst = tmp_path / "l.txt"
    args = {"--state": str(st), "--catalogue": str(cat), "--store-listing": str(lst)}
    if arg == "--store-dir":
        args.pop("--store-listing")
    args[arg] = spelling
    argv = [x for k, v in args.items() for x in (k, v)]
    rc = C.main(argv)
    out = capsys.readouterr().out
    live_calls = [c for c in spies if _live(c[1])]
    print(f"A main {arg}={spelling!r}: rc={rc} out={out.strip()[:160]!r} live calls={live_calls}")
    assert rc == 2 and "REFUSED" in out
    assert live_calls == [], live_calls


@pytest.mark.parametrize("spelling", [
    r"E:\research \econfindatalibrary\data\catalog.db",
    r"E:\research\econfindatalibrary \data\catalog.db",
    r"E:\research\econfindatalibrary\data \catalog.db",
    r"E:\research\econfindatalibrary::$INDEX_ALLOCATION\data\catalog.db",
    r"E:\research::$INDEX_ALLOCATION\econfindatalibrary\data\catalog.db",
    r"E:\::$INDEX_ALLOCATION\econ_live\state\writer.lock",
])
def test_A2_the_round4_hole_spellings_on_the_REAL_roots_with_blocking_spies(spies, spelling):
    """B found these classes on a fake root. Here on the real roots: the spies RAISE before any live call, so a
    Blocked exception means refused() passed the text test and was about to resolve (open) the live path."""
    try:
        why = C.refused(spelling)
    except Blocked as e:
        print(f"A2 {spelling!r}: TEXT PASSED - the resolve was stopped by the spy: {e}")
        pytest.fail(f"text test passed; refused() tried a disk call on a live path: {e}")
    print(f"A2 {spelling!r} -> {why!r}; calls {spies}")
    assert why and spies == []


# --------------------------------------------------------------------------- #
# B. hunting spellings whose TEXT passes but the OS resolves into a (fake) live root
# --------------------------------------------------------------------------- #
@pytest.fixture
def fake(tmp_path):
    live = tmp_path / "liveroot"
    data = live / "data"
    data.mkdir(parents=True)
    (data / "catalog.db").write_bytes(b"live")
    return tmp_path, live, data


def _touched_under(calls, root) -> list:
    r = os.path.normcase(str(root))
    out = []
    for name, a in calls:
        s = a
        for pre in ("\\\\?\\", "\\??\\", "\\\\.\\"):
            if s.startswith(pre):
                s = s[len(pre):]
        s = os.path.normcase(s.replace("/", "\\"))
        if s.startswith(r) and name in ("ntpath._getfinalpathname", "nt.stat", "nt.lstat", "os.stat", "os.lstat",
                                        "builtins.open", "os.open", "ntpath.isfile", "ntpath.isdir",
                                        "ntpath.exists"):
            out.append((name, a))
    return out


def test_B_os_facts_on_this_machine(fake):
    """The OS behaviour the hunt depends on, measured on F: (no live path)."""
    tmp, live, data = fake
    facts = {}
    for label, p in (
            ("trailing space in a component", str(live) + " \\data\\catalog.db"),
            ("trailing dot in a component", str(live) + ".\\data\\catalog.db"),
            ("INDEX_ALLOCATION stream on a parent", str(live) + "::$INDEX_ALLOCATION\\data\\catalog.db"),
            ("\\\\?\\ + .. after the file", "\\\\?\\" + str(data / "catalog.db") + "\\..\\..\\x"),
    ):
        try:
            os.stat(p)
            facts[label] = "OPENS"
        except OSError as e:
            facts[label] = f"{type(e).__name__} {getattr(e, 'winerror', None)}"
        facts[label + " / abspath"] = ntpath.abspath(p)
    for k, v in facts.items():
        print(f"B FACT {k}: {v}")


@pytest.mark.parametrize("label,make", [
    ("trailing space on the root's own component", lambda live, data: str(live) + "\\data \\catalog.db"),
    ("trailing space on a parent of the root", lambda live, data: str(live) + " \\data\\catalog.db"),
    ("INDEX_ALLOCATION stream on a parent", lambda live, data: str(live) + "::$INDEX_ALLOCATION\\data\\catalog.db"),
    ("\\\\?\\ + .. after the live file", lambda live, data: "\\\\?\\" + str(data / "catalog.db") + "\\..\\..\\x"),
    ("\\\\?\\ with forward slashes", lambda live, data: "\\\\?\\" + str(data / "catalog.db").replace("\\", "/")),
])
def test_B_a_spelling_the_text_passes_must_not_open_the_live_root(fake, spies, label, make):
    tmp, live, data = fake
    p = make(live, data)
    why = C.refused(p, roots=[str(data)])
    touched = _touched_under(spies, data)
    print(f"B {label}: {p!r} -> {why!r}; touched under the fake root: {touched}")
    assert not touched, f"the guard opened/stat-ed a path under the (fake) live root: {touched} (result {why!r})"
