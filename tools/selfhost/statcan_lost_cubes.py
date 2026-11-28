"""statcan's LOST-CUBE census, on COPIES only and read-only (T0 item B3, code review round 2, G3; S13 prep).

WHY. The post-T0 lane (jobs/statcan_selfhost_lane.py, lost_cube_check) BLOCKS - merges nothing, serves nothing -
while its lane state books a cube NOT HELD (`not_held`) that has no parquet in the store and at least one catalogued
id: a catalogued cube is never new, so that is a lost store file whose release was booked settled (code review F3).
Bookings the PRE-T0 lane made count too (a cube absent from the store with no local .done, e.g. during the
2026-09-05 partial restore, R753). If the live lane state already holds one at T0, the new lane is BLOCKED from its
first post-T0 iteration. This tool counts them BEFORE the S13 guard swap, so the number is a receipt, not an alarm.

INPUTS - ALL COPIES. R1940: never E:'s data/ catalogue or state database, not even read-only, and the coordinator's
brief: never E:'s store. This tool REFUSES any input under E:\\research\\econfindatalibrary\\data or E:\\econ_live.
It decides on the path's TEXT first (any case, either slash, `..`, a device prefix, a stream of a root, a stream or
a trailing space or dot in any folder name), and also refuses there any network or device path, any name with a ':'
or a trailing space or dot on the live drive, and an 8.3 short name there: those inputs are refused before
anything touches them. A path that passes is then resolved once, so a junction or link into a live place is still
refused, but that resolve touches it (refused() says what is and is not caught).
  --state          a copy of the lane state (data/clean_full/statcan/_lane_state.json)
  --catalogue      a copy of the catalogue - opened mode=ro&immutable=1 (no lock, no journal, no side file)
  --store-dir      a folder holding a copy of the statcan parquets: only their NAMES are listed, no file is opened
  --store-listing  or a text file with one store file name per line (a `dir /b` of a store copy)

THE RULES are the lane's own: a cube the state books `not_held`, no `<pid>.parquet` in the store, and at least one
series id in [statcan:<pid>, statcan:<pid>$); and, beside it, the lane's other first-iteration block on the same
inputs - a cube the state records as HELD (merged or served, not not_held) with no parquet (design review C6a).
tests/test_statcan_selfhost_lane.py pins this tool against jobs/statcan_selfhost_lane.py (not_held_without_parquet,
held_without_parquet, lost_cube_check) and the id scheme against tools/derive_statcan_tables.py unit_id.

EXIT 0: no such cube. 1: some (each one listed - restore its parquet before S13). 2: an input is missing,
unreadable or refused. It writes nothing anywhere.
"""
from __future__ import annotations

import argparse
import json
import ntpath
import os
import pathlib
import posixpath
import re
import sqlite3
import sys

# The LIVE places this tool must never read (R1940). Pinned against core.catalog_path (LIVE_STORE_ROOT\data and the
# folder of LOCK_PATH's folder) by tests/test_statcan_selfhost_lane.py; not imported from it, so this tool opens
# nothing through the resolver.
REFUSED_ROOTS = (r"E:\research\econfindatalibrary\data", r"E:\econ_live")
SOURCE = "statcan"


def unit_id(pid: str) -> str:
    """The cube's own series id - tools/derive_statcan_tables.py unit_id(pid), pinned equal by the tests."""
    return f"{SOURCE}:{pid}"


_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")
_SHORT_NAME = re.compile(r"~\d")                            # an 8.3 component, e.g. ECON_L~1


_DEVICE_PREFIXES = ("\\\\?\\", "\\\\.\\", "\\??\\")      # \\?\  \\.\  and the NT-namespace \??\


def _fold_prefix(s: str) -> str:
    r"""Drop a \\?\, \\.\ or \??\ device prefix (either slash, mixed too) in front of a drive path. \??\ matters:
    GetFullPathNameW reads it as a rooted path on the working drive, but CreateFileW opens it as the NT path."""
    if s[:4].replace("/", "\\") in _DEVICE_PREFIXES and _DRIVE.match(s[4:]):
        return s[4:]
    return s


def _is_network_or_device(s: str) -> bool:
    r"""A UNC path (\\host\share, \\localhost\E$), \\?\UNC\..., \\?\Volume{...}, \??\... or any other device path:
    on Windows every path that starts with two slashes or with \??\ once a drive prefix is dropped; on another OS
    one spelled so with backslashes. Decided on the text: such a path can name a live place in a way no text test
    can rule out."""
    s = _fold_prefix(s)
    b = s.replace("/", "\\") if os.name == "nt" else s
    return b.startswith("\\\\") or b.startswith("\\??\\")


def _text_form(path) -> str:
    r"""`path` as normalised TEXT: never stat-ed, opened or resolved (code re-review round 3, the G3 note). A Windows
    path (on Windows every path; on another OS one spelled with a drive letter) is folded with ntpath: either slash,
    any case, `..` collapsed, a device prefix dropped - the same on CI's Linux runner as on the desktop for these
    spellings. On Windows only, abspath (GetFullPathNameW: text only, it reads no file) also joins the working
    folder and drops trailing dots from the components (a trailing space inside the path is kept: another name)."""
    s = _fold_prefix(os.fspath(path))
    if os.name == "nt":
        return ntpath.normcase(ntpath.normpath(ntpath.abspath(s)))
    if _DRIVE.match(s):
        return ntpath.normcase(ntpath.normpath(s))
    return posixpath.normpath(posixpath.abspath(s))


def _is_windows_form(root_form: str) -> bool:
    return os.name == "nt" or bool(_DRIVE.match(root_form))


def _under(form: str, root_form: str) -> bool:
    """`form` is the root, under it, or a stream of the root itself (root:stream, Windows forms)."""
    win = _is_windows_form(root_form)
    r = root_form.rstrip("\\/")
    return form == r or form.startswith(r + ("\\" if win else "/")) or (win and form.startswith(r + ":"))


def _on_drive(form: str, root_form: str) -> bool:
    """`form` is a Windows form on the drive of a Windows-form root."""
    if not _is_windows_form(root_form):
        return False
    d = ntpath.splitdrive(form)[0]
    return bool(d) and d == ntpath.splitdrive(root_form)[0]


def _short_name_on(form: str, root_form: str) -> bool:
    """`form` has an 8.3 component on the root's drive: what it names is known only by asking the disk."""
    return _on_drive(form, root_form) and any(_SHORT_NAME.search(c) for c in form.split("\\"))


def _names(form: str) -> list:
    """The folder and file names of a Windows form, after its drive."""
    return [c for c in ntpath.splitdrive(form)[1].split("\\") if c]


def _plain_names(form: str) -> str:
    """`form` with each name after the drive read as NTFS may open it (code review round 4, F1): cut at its first ':'
    (NTFS opens `<folder>::$INDEX_ALLOCATION` as the folder itself, also in the MIDDLE of a path) and stripped of
    trailing spaces and dots (realpath's walk-up trims them where a name becomes the last one)."""
    drive, rest = ntpath.splitdrive(form)
    names = [c if c in (".", "..") else c.split(":", 1)[0].rstrip(" .") for c in rest.split("\\")]
    return ntpath.normpath(drive + "\\".join(names))


def _odd_name(form: str) -> bool:
    """A name after the drive with a ':' (a stream) or a trailing space or dot."""
    return any(":" in c or (c not in (".", "..") and c != c.rstrip(" .")) for c in _names(form))


def refused(path: str, roots=None) -> "str | None":
    """A refusal when `path` is (under) one of the live places. Decided on the TEXT first, with nothing resolved,
    stat-ed or opened (os.path.realpath opens a handle on Windows): a path under a root, a stream of a root, any
    network or device path, a path under a root once each name is read as NTFS reads it (a stream or a trailing space
    or dot dropped, also in a middle name), any name with a ':' or a trailing space or dot on a root's drive, and an
    8.3 short name on a root's drive are refused there. Only a path that passes is
    resolved (os.path.realpath, which touches it), so a junction, link or subst drive into a live place is still
    refused, and so is one that resolves to a network path (a mapped drive). The roots are compared as text and
    never resolved: if a root were itself a junction, a path spelled by its target would not be refused. A hard
    link elsewhere to a live file is not detected at all."""
    roots = REFUSED_ROOTS if roots is None else roots
    forms = [(r, _text_form(r)) for r in roots]
    s = os.fspath(path)
    if _is_network_or_device(s):
        return f"REFUSED: {path} is a network or device path - give a COPY on a local drive (R1940)"
    p = _text_form(s)
    for r, rf in forms:
        if _under(p, rf):
            return f"REFUSED: {path} is under {r}, a LIVE place (R1940) - give a COPY"
    plain = _plain_names(p)
    for r, rf in forms:
        if _is_windows_form(rf) and _under(plain, rf):
            return (f"REFUSED: {path} is under {r}, a LIVE place, once each name is read as NTFS reads it (a stream "
                    f"or a trailing space or dot dropped) - give a plain path to a COPY")
    for r, rf in forms:
        if _on_drive(p, rf) and _odd_name(p):
            return (f"REFUSED: {path} has a name with a ':' or a trailing space or dot on the drive of {r}, a LIVE "
                    f"place - give a plain path")
    for r, rf in forms:
        if _short_name_on(p, rf):
            return (f"REFUSED: {path} has an 8.3-style component (~<digit>) on the drive of {r}, a LIVE place - "
                    f"give the long path")
    real = os.path.realpath(path)            # passed the text test: resolved only now
    if _is_network_or_device(real):
        return f"REFUSED: {path} resolves to {real}, a network or device path - give a COPY on a local drive"
    rp = _text_form(real)
    for r, rf in forms:
        if _under(rp, rf):
            return f"REFUSED: {path} resolves to {real}, under {r}, a LIVE place (R1940) - give a COPY"
    return None


def store_names(store_dir: "str | None" = None, listing: "str | None" = None) -> set:
    """The parquet file names of the store copy: listed from the folder, or read from the listing file."""
    if store_dir is not None:
        return {f for f in os.listdir(store_dir) if f.endswith(".parquet")}
    with open(listing, encoding="utf-8-sig") as fh:
        return {os.path.basename(ln.strip()) for ln in fh if ln.strip().endswith(".parquet")}


def held_without_parquet(st: dict, names: set) -> list:
    """The cubes the state records as HELD (merged or served, not not_held) with no `<pid>.parquet` - the lane's
    other first-iteration block (design review C6a: restore them from a backup; the lane cannot re-pull them)."""
    cubes = st.get("cubes") if isinstance(st, dict) and isinstance(st.get("cubes"), dict) else {}
    return sorted(str(p) for p, c in cubes.items()
                  if isinstance(c, dict) and (c.get("merged") or c.get("served")) and not c.get("not_held")
                  and f"{p}.parquet" not in names)


def not_held_without_parquet(st: dict, names: set) -> dict:
    """pid -> booked release, for every cube the state books not_held with no `<pid>.parquet` (the lane's rule)."""
    cubes = st.get("cubes") if isinstance(st, dict) and isinstance(st.get("cubes"), dict) else {}
    return {str(p): c.get("release") for p, c in cubes.items()
            if isinstance(c, dict) and c.get("not_held") and f"{p}.parquet" not in names}


def catalogued_counts(catalogue: str, pids) -> dict:
    """pid -> number of catalogued ids, for the pids that have at least one: one primary-key range read each."""
    uri = pathlib.Path(catalogue).resolve().as_uri() + "?mode=ro&immutable=1"
    con = sqlite3.connect(uri, uri=True)    # plain-open: a catalogue COPY, mode=ro&immutable=1 - no lock, no journal
    try:
        out = {}
        for p in sorted(pids):
            lo = unit_id(p)
            n = con.execute("SELECT count(*) FROM series WHERE series_id >= ? AND series_id < ?",
                            (lo, lo + "$")).fetchone()[0]
            if n:
                out[p] = n
        return out
    finally:
        con.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--state", required=True, help="a COPY of the lane state (_lane_state.json)")
    ap.add_argument("--catalogue", required=True, help="a COPY of the catalogue (opened mode=ro&immutable=1)")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--store-dir", help="a folder with a COPY of the statcan parquets (names listed only)")
    src.add_argument("--store-listing", help="a text file, one store file name per line")
    a = ap.parse_args(argv)
    store = a.store_dir if a.store_dir is not None else a.store_listing
    for p in (a.state, a.catalogue, store):
        why = refused(p)
        if why:
            print(why)
            return 2
    for p, is_dir in ((a.state, False), (a.catalogue, False), (store, a.store_dir is not None)):
        if not (os.path.isdir(p) if is_dir else os.path.isfile(p)):
            print(f"MISSING: {p}")
            return 2
    try:
        with open(a.state, encoding="utf-8") as fh:
            st = json.load(fh)
    except (OSError, ValueError) as e:
        print(f"UNREADABLE: the state copy {a.state} ({e!r})")
        return 2
    cubes = st.get("cubes") if isinstance(st, dict) and isinstance(st.get("cubes"), dict) else {}
    if not cubes:
        print(f"UNREADABLE: the state copy {a.state} books no cubes - not the lane state?")
        return 2
    try:
        names = store_names(a.store_dir, a.store_listing)
    except OSError as e:
        print(f"UNREADABLE: the store copy {store} ({e!r})")
        return 2
    if not names:
        print(f"UNREADABLE: the store copy {store} lists 0 parquets - not a store copy? (the lane BLOCKS on 0 too)")
        return 2
    not_held = sum(1 for c in cubes.values() if isinstance(c, dict) and c.get("not_held"))
    cand = not_held_without_parquet(st, names)
    print(f"state copy: {a.state} - {len(cubes)} cube(s), {not_held} booked not held")
    print(f"store copy: {store} - {len(names)} parquet name(s)")
    print(f"catalogue copy: {a.catalogue} (mode=ro&immutable=1)")
    print(f"booked not held with no parquet: {len(cand)}")
    try:
        counts = catalogued_counts(a.catalogue, cand)
    except sqlite3.Error as e:
        print(f"UNREADABLE: the catalogue copy {a.catalogue} ({type(e).__name__}: {e})")
        return 2
    lost = sorted(counts)
    for p in lost:
        print(f"  LOST {p}: booked not held at release {cand[p]}, {counts[p]} catalogued id(s), no parquet")
    # the lane's OTHER first-iteration block on the same two inputs (design review C6a), counted beside it
    held = held_without_parquet(st, names)
    print(f"recorded HELD with no parquet: {len(held)}")
    for p in held:
        c = cubes.get(p) or {}
        print(f"  HELD-LOST {p}: merged {c.get('merged')}, served {c.get('served')}, no parquet")
    if lost or held:
        print(f"RESULT: {len(lost)} LOST not-held cube(s) and {len(held)} held cube(s) with no parquet - the post-T0 "
              f"lane would be BLOCKED from its first iteration. Restore each parquet from a backup before S13 (a "
              f"lost not-held cube's booked release stays unmerged until StatCan releases the cube again).")
        return 1
    print("RESULT: 0 lost cubes - the post-T0 lane's store checks pass on these copies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
