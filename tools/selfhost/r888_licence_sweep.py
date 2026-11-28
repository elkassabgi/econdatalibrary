"""B32 (T0 plan rev 6.7, ruling v; reviews rev 6.4 H2/H3/M3/M4/L1/L2/L7, rev 6.5 R1, rev 6.6 M2/M3/L1-L4, B35 H1):
the R888 licence-orphan delete, the VACUUM that removes the file's residue, and the whole-catalogue receipt sweep.

R888 (ledger rules 1-4): the owner ordered every mention of the deny-listed sources wiped. This tool never prints,
writes or returns a deny-listed id, a licence id or name, a row, or any count of member-carrying items. Its output is
PASS/FAIL, YES/NO per table and class, counts of tables and of rows scanned (never of the licence table), and wall
times. Exceptions are printed by TYPE only (L7). Hashes of id sets go only into a sealed, write-once state file.

MODES (all need --db; every mode refuses a path the rules under PATHS do not accept):

  --plan     read-only. YES/NO lines and PASS/FAIL only (no class count: a difference of two counts would be a count
             of member-carrying rows). Writes the sealed --state file: the selection hash, the keep-set hash, the
             held-class (v6) hash, the file token and this tool's sha. Prints the --confirm token (file identity +
             tool sha; not a function of any id set).
  --apply    the delete (needs --state, --confirm, --backup; --tmpdir unless --no-vacuum). SELECTION (plan rev 6.7):
             a licence row that NO row of any other table references through a column named license_id (the
             allow-list view: the worker reads a licence row only through a source's or a series' license_id,
             sql.ts:62-94) and whose id NO tracked file at HEAD names as a token (code, docs and configs; quoted
             and unquoted - configs/sources.yaml names licence ids as unquoted YAML keys). That includes rows that
             carry no member (review rev 6.6 M3). Such no-member rows are first written to the sealed --backup file
             (verified, values and file text, to carry no member of today's gate NOR of any committed version of
             denylist.ts - code review H1, R2257). Member rows are deleted with no backup. Unreferenced rows a tracked
             file names are KEPT (a member-carrying one is reported for B34's code PR). One BEGIN IMMEDIATE
             transaction: the token re-checked, ONE `SELECT DISTINCT license_id, typeof(license_id)` per referencing
             column (L2), the selection re-measured and compared in process with the plan's sealed hash, the keep
             set's hash too (R908 rule 5), PRAGMA secure_delete=ON, journal_mode=DELETE, locking_mode=NORMAL, each
             row deleted by its primary key, COMMIT. Receipts: the deleted rows' exact record images (table record and
             primary-key index record, built in memory from the rows' values) are each SEEN in the file before the
             delete (the control) and found in NO live cell after it; then, unless --no-vacuum, the VACUUM below and
             its receipt, and the record images found NOWHERE in the file.
  --vacuum   the standalone VACUUM (S4d runs --apply --no-vacuum, then B35's renames, then --vacuum - B35 review H1,
             R2274). Preconditions: free space on the database's drive >= 2x the file size, and no other handle on
             the file (Windows: an open with share mode 0 fails with ERROR_SHARING_VIOLATION while any handle is
             open; an unknown answer refuses). Then BEGIN EXCLUSIVE/COMMIT, VACUUM with temp_store=MEMORY and
             TMP/TEMP at --tmpdir. Receipt: freelist_count = 0, no -journal/-wal/-shm, --tmpdir empty, no dangling
             licence reference, quick_check (or --check integrity) ok, and no deny-list member in free pages. It
             cannot re-check the deleted rows' record images (they are member bytes and are never stored): that
             check is --apply's live-cell receipt, and VACUUM rebuilds every page from the live rows only.
  --sweep    read-only receipt sweep against today's deny list, core.gen_denylist.committed_gate() (bound in
             memory, never typed, never printed). Every table in sqlite_master and sqlite_master itself (or, with
             --licence-only, license + source + sqlite_master). Big rowid tables are sharded over --workers
             processes; per shard rows read == COUNT(*); the file's identity and change counter must not move.
               K1A FAIL  an unreferenced licence row that no tracked file names (after --apply there is none)
               K1N HELD  an unreferenced row a tracked file names that carries a member (B34's code PR; plan (v4))
               K2  FAIL  rows of a deny-listed source: a source_id equal to a member (also re-spelled), or a series
                         key prefix that is a member and not a committed SERIES_CARVEOUTS code
               K2C HELD  series keys under a member that ARE committed carve-outs (served by design)
               K3  HELD  v6: referenced licence rows that carry a member; with --compare-state its key hash is
                         compared with the proof copy's and a difference is REPORTED, never a FAIL (owner item 40)
               K4  HELD  v7: mentions inside other tables' rows, per table and column - YES/NO only
               K5  FAIL  PRAGMA freelist_count > 0 (INFO with --allow-free-pages: S10b, after S5 Road B writes)
               K6  FAIL  a license_id reference that names no licence row (dangling)
             The positive control never writes: a member chosen in process is planted into an IN-MEMORY copy of a
             real row of each table and sent through the same row classifier; a row of '~' cells must not fire. A
             table with no text cell, or with no rows, is UNMEASURED (never NO).
  --residue  read-only raw scan of the file's pages (outside SQLite: this Python's SQLite has no sqlite_dbpage),
             under a SHARED transaction. FREE (freelist trunk and leaf pages), UNALLOC (the gap after a b-tree page's
             cell-pointer array, and its freeblocks; stale copies of LIVE cells also sit there), sidecar files.
             YES/NO per region, never a size or a page count. The lock-byte page (offset 1 GiB) is skipped.

MATCHING (L1, exact). base(s): an all-ASCII string is lower-cased; any other string is NFKC-normalised, the
zero-width characters U+200B U+200C U+200D U+2060 U+FEFF U+00AD are removed, the Unicode dashes U+2010-U+2015,
U+2212, U+FE58, U+FE63, U+FF0D become '-', every Unicode space separator (category Zs) becomes ' ', and the result
is case-folded. norm(s) = base(s) with each of '-', '_', '.', ' ' replaced by '_' (the one separator character).
A cell is also read URL-unquoted when it holds a '%'. Senses of a member g (case-folded):
  EXACT  base(cell) == g            KEY    base(cell) starts with g + ':' or g + '/'
  TOKEN  g with no [a-z0-9_] neighbour        LOOSE  g with no [a-z0-9] neighbour ('_' is a separator)
  SUB5   g of length >= 5 as a plain substring        SUBALL  g (or norm(g)) as a plain substring, any length
  SEPN   (g has a separator) norm(g) inside norm(cell) with no [a-z0-9] neighbour - the R2253 '-' vs '_' class
  CONCAT (g has a separator and is >= 5 long without it) g with its separators removed, no [a-z0-9] neighbour
A cell CARRIES a member when any sense fires (the broad matcher: STRICT, SEP5B and SUBALL, plan rev 6.7).

PATHS. A path that starts with two slashes (UNC, \\\\?\\, \\\\.\\) is refused; the real path must be an existing file
with link count 1 (the shared 11-16 copy has 5) and no -journal/-wal/-shm beside it; the header must say rollback
journal and UTF-8. The inputs this tool reads from the checkout (denylist.ts, util.ts, the licence verdict file, the
two audit parsers, gen_denylist.py and this file) must equal HEAD (git status clean for them - code review M3).
WITHOUT --live: the real path must be under PROOF_ROOTS, must not be the build (os.path.samefile) or under the live
checkout or E:\\econ_live, and the CUTOVER flag must be absent. WITH --live (the named step S4d, before S5 and S7c,
so BEFORE the flag; also S10b): the path must be the build itself, this tool must run from the production checkout,
the writers must be held (logs\\STOP_DISPATCH and logs\\local_heavy.DONE present, logs\\local_heavy.lock and
logs\\statcan_writer.lock absent), and --apply/--vacuum take core.catalog_path.writer_lock() themselves (it works
before the flag; write_session() takes no lock before T0).

Run from the repo root:  python tools/selfhost/r888_licence_sweep.py --plan --db <path> --state <new file>
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import multiprocessing
import os
import re
import struct
import subprocess
import sys
import time
import unicodedata
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from core import catalog_path  # noqa: E402
from core import cutover  # noqa: E402
from core import gen_denylist  # noqa: E402

# Before T0 a proof copy, the state, the backup and the VACUUM temp folder are accepted only under these roots (an
# allow-list: a deny-list of spellings is the R1214/R1223 shape). Tests monkeypatch it.
PROOF_ROOTS = (r"F:\work\econ_finish\b32_r888_licence",)
LIVE_AREAS = (r"E:\econ_live",)                 # plus catalog_path.LIVE_STORE_ROOT
OLDER_COPY = r"F:\work\owed_pay\20261117\catalog_copy.db"   # read mode=ro&immutable=1 only (BUILDER_RULES)
GATE_FILE = "api/worker/src/denylist.ts"
TOOL_REL = "tools/selfhost/r888_licence_sweep.py"
HEAD_INPUTS = (GATE_FILE, "api/worker/src/util.ts", "DATABASE_LICENSES_VERBATIM.md", "core/gen_denylist.py",
               "tools/audit_license_coverage.py", "tools/audit_schedule_coverage.py", "core/catalog_path.py",
               "core/cutover.py", TOOL_REL)
RAW_CHUNK_PAGES = 1024                          # pages per raw read (tests make it 1 to put every edge at a page)
SIDE = ("-journal", "-wal", "-shm")
SHARD_ROWS = 1_000_000                          # a rowid table with more rows than this is sharded
CONTROL_SAMPLE = 20_000                         # rows per task on which the hosted-id control is counted
OK, FAIL, ERR = 0, 1, 2


class Refused(RuntimeError):
    """A precondition failed. The message never carries a cell value, an id or a SQL parameter."""


def say(line: str = "") -> None:
    print(line, flush=True)


def yn(b) -> str:
    return "YES" if b else "NO"


# ---------------------------------------------------------------------------------------------------------- matching
_ZW = dict.fromkeys(map(ord, "\u200b\u200c\u200d\u2060\ufeff\u00ad"), None)
_DASHES = frozenset("\u2010\u2011\u2012\u2013\u2014\u2015\u2212\ufe58\ufe63\uff0d")
_SEPS_RX = re.compile(r"[-_. ]")
_WORD = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")
_ALNUM = frozenset("abcdefghijklmnopqrstuvwxyz0123456789")
SENSES = ("EXACT", "KEY", "TOKEN", "LOOSE", "SUB5", "SEPN", "CONCAT", "SUBALL")


def base(s: str) -> str:
    if s.isascii():
        return s.lower()
    s = unicodedata.normalize("NFKC", s).translate(_ZW)
    s = "".join("-" if c in _DASHES else (" " if unicodedata.category(c) == "Zs" else c) for c in s)
    return s.casefold()


def norm(s: str) -> str:
    """base() text with every separator spelled '_' (the caller passes base() output)."""
    return _SEPS_RX.sub("_", s)


def _first_part(ng: str) -> str:
    return next((x for x in ng.split("_") if x), "")


def _bounded(v: str, g: str, word) -> bool:
    n, i = len(g), v.find(g)
    while i >= 0:
        a = v[i - 1] if i > 0 else ""
        b = v[i + n] if i + n < len(v) else ""
        if a not in word and b not in word:
            return True
        i = v.find(g, i + 1)
    return False


class Gate:
    """Compiled matcher for one member set. Holds the members in memory only; nothing here prints them."""

    def __init__(self, members):
        ms = {base(str(m)).strip() for m in members if str(m).strip()}
        ms.discard("")
        self.ms = sorted(ms)
        self.set = frozenset(ms)
        self.normset = frozenset(norm(g) for g in ms)
        self.multi = {g: (norm(g), _SEPS_RX.sub("", g)) for g in ms if _SEPS_RX.search(g)}
        pre = set()
        for g in ms:
            pre.add(_first_part(norm(g)) if g in self.multi else g)   # a leading separator never empties it (L6)
        pre.discard("")
        # a prefilter string that contains another one is implied by it (minimal set: fewer finds per cell)
        self.pre = sorted(p for p in pre if not any(q != p and q in p for q in pre))
        self.maxlen = max((len(g) for g in ms), default=0)

    def __bool__(self) -> bool:
        return bool(self.ms)

    def maybe(self, v: str) -> bool:
        for p in self.pre:
            if p in v:
                return True
        return False

    def senses_base(self, v: str) -> set:
        """Senses fired on ONE already-base() string."""
        hit = set()
        if not self.maybe(v):
            return hit
        nv = None
        for g in self.ms:
            if g in v:
                hit.add("SUBALL")
                if v == g:
                    hit.add("EXACT")
                if v.startswith(g + ":") or v.startswith(g + "/"):
                    hit.add("KEY")
                if _bounded(v, g, _WORD):
                    hit.add("TOKEN")
                if _bounded(v, g, _ALNUM):
                    hit.add("LOOSE")
                if len(g) >= 5:
                    hit.add("SUB5")
            sep = self.multi.get(g)
            if sep is not None:
                ng, cg = sep
                first = _first_part(ng)
                if first and first in v:
                    if nv is None:
                        nv = norm(v)
                    if _bounded(nv, ng, _ALNUM):
                        hit.add("SEPN")
                    if len(cg) >= 5 and _bounded(v, cg, _ALNUM):
                        hit.add("CONCAT")
                    if ng in nv:
                        hit.add("SUBALL")                   # the normalised spelling as a plain substring
        return hit

    def cell(self, value) -> set:
        """Senses fired on a cell value (str). Non-text cells fire nothing."""
        if not isinstance(value, str) or not value:
            return set()
        v = base(value)
        hit = self.senses_base(v)
        if "%" in value:
            try:
                u = base(urllib.parse.unquote(value))
            except Exception:  # noqa: BLE001 - an undecodable escape is read as the raw text only
                u = v
            if u != v:
                hit |= self.senses_base(u)
        return hit


def key_member(gate: Gate, value):
    """The member that is the key prefix of a series_id-like cell, or None."""
    if not isinstance(value, str):
        return None
    v = base(value)
    for sep in (":", "/"):
        if sep in v:
            head = v.split(sep, 1)[0]
            if head in gate.set or norm(head) in gate.normset:
                return head
    return None


def plant_member(gate: Gate) -> str:
    """The member used for every planted control: a multi-part one of length >= 5 if there is one (so SEPN and
    CONCAT are exercised), else the longest. Chosen in process, never printed."""
    multi = sorted((g for g in gate.multi if len(g) >= 5), key=lambda g: (-len(g), g))
    if multi:
        return multi[0]
    return sorted(gate.ms, key=lambda g: (-len(g), g))[0]


def matcher_self_test(gate: Gate) -> bool:
    g = plant_member(gate)
    plants = {"EXACT": g, "KEY": g + ":abc", "TOKEN": "zz " + g + " zz", "LOOSE": "zz_" + g + "_zz",
              "SUBALL": "zz" + g + "zz"}
    if len(g) >= 5:
        plants["SUB5"] = "zz" + g + "zz"
    if g in gate.multi:
        plants["SEPN"] = "zz " + _SEPS_RX.sub("-", g) + " zz"
        if len(_SEPS_RX.sub("", g)) >= 5:
            plants["CONCAT"] = "zz " + _SEPS_RX.sub("", g) + " zz"
    ok = True
    for sense, s in plants.items():
        good = sense in gate.cell(s)
        ok &= good
        say(f"matcher self-test, planted {sense:6}: {'PASS' if good else 'FAIL'}")
    good = "TOKEN" not in gate.cell("zz_" + g + "_zz")
    ok &= good
    say(f"matcher self-test, '<x>_<member>_<x>' is not TOKEN: {'PASS' if good else 'FAIL'}")
    good = not gate.cell("~~~~ #### ~~~~")
    ok &= good
    say(f"matcher self-test, negative control silent: {'PASS' if good else 'FAIL'}")
    escaped = "".join("%%%02X" % ord(ch) for ch in _SEPS_RX.sub("-", g))   # no member left in the raw text
    good = not gate.senses_base(base(escaped)) and bool(gate.cell("zz%20" + escaped + "%20zz"))
    ok &= good
    say(f"matcher self-test, URL-escaped member fires: {'PASS' if good else 'FAIL'}")
    return ok


# --------------------------------------------------------------------------------------------------------------- git
def _git(args, data: bytes | None = None) -> bytes:
    """A read-only git command in ROOT (--no-optional-locks). No id is ever put on its command line."""
    try:
        r = subprocess.run(["git", "--no-optional-locks", "-C", ROOT] + list(args), input=data,
                           capture_output=True, check=True)
    except (OSError, subprocess.CalledProcessError) as e:
        raise Refused(f"git could not be read ({type(e).__name__})") from None
    return r.stdout


def head_sha() -> str:
    return _git(["rev-parse", "HEAD"]).decode().strip()


def tree_clean(paths=HEAD_INPUTS) -> bool:
    """True when every input this tool reads from the checkout is tracked and equal to HEAD (code review M3)."""
    out = _git(["status", "--porcelain", "--untracked-files=all", "--"] + list(paths))
    tracked = _git(["ls-files", "--"] + list(paths)).decode().split()
    return not out.strip() and len(set(tracked)) == len(set(paths))


def _cat_blobs(shas) -> list:
    if not shas:
        return []
    data = _git(["cat-file", "--batch"], b"\n".join(shas) + b"\n")
    out, pos = [], 0
    while pos < len(data):
        nl = data.index(b"\n", pos)
        hdr = data[pos:nl].split()
        if len(hdr) != 3 or hdr[1] != b"blob":
            raise Refused("unexpected git cat-file output")
        size = int(hdr[2])
        out.append(data[nl + 1:nl + 1 + size])
        pos = nl + 1 + size + 1
    if len(out) != len(shas):
        raise Refused("git cat-file returned a different number of blobs than were asked for")
    return out


_IDCH = frozenset(b"abcdefghijklmnopqrstuvwxyz0123456789_.-")


def names_token(blob: bytes, k: bytes) -> bool:
    """`k` (lower-case bytes) occurs in `blob` (lower-cased) as a TOKEN: not preceded by an id character
    [a-z0-9_.-], and not followed by one, except that a '.' followed by a non-id character (a sentence's full stop)
    ends the token. That matches quoted literals ('id', "id"), unquoted YAML keys (`id:`), list items and prose,
    and NOT a longer id that merely starts with it."""
    n, i = len(k), blob.find(k)
    while i >= 0:
        a = blob[i - 1] if i > 0 else None
        j = i + n
        b = blob[j] if j < len(blob) else None
        if b == 0x2E and (j + 1 >= len(blob) or blob[j + 1] not in _IDCH):
            b = None
        if (a is None or a not in _IDCH) and (b is None or b not in _IDCH):
            return True
        i = blob.find(k, i + 1)
    return False


def code_named(ids) -> tuple:
    """(the ids that a tracked file at HEAD names as a token, HEAD sha). Every tracked blob - code, docs and configs
    - is read in process through `git cat-file --batch`; case-insensitive (plan rev 6.7, review rev 6.6 L3)."""
    want = {i.lower().encode("utf-8"): i for i in ids}
    head = head_sha()
    if not want:
        return set(), head
    tree = _git(["ls-tree", "-r", "-z", "--full-tree", "HEAD"])
    shas = [e.split(b"\t", 1)[0].split()[2] for e in tree.split(b"\x00") if e and b" blob " in e.split(b"\t")[0]]
    if not shas:
        raise Refused("git lists no tracked file")
    out = set()
    for blob in _cat_blobs(shas):
        low = blob.lower()
        for k, i in want.items():
            if i not in out and k in low and names_token(low, k):
                out.add(i)
    return out, head


def history_members() -> set:
    """Every id ANY committed version of denylist.ts gated (code review H1, R2257: a row of a source wiped earlier
    that has left the gate must still never be written to a file). A version committed_gate() cannot parse
    contributes every quoted lower-case token it holds (over-inclusive - the safe direction here: such a row is only
    deleted without a backup). In memory only."""
    if _git(["rev-parse", "--is-shallow-repository"]).decode().strip() != "false":
        raise Refused("a shallow clone cannot show the gate's whole history (code review r3 L4)")
    revs = [r for r in _git(["log", "--format=%H", "--", GATE_FILE]).decode().split() if r]
    if not revs:
        raise Refused("the gate file has no git history here")
    shas = []
    for r in revs:
        line = _git(["ls-tree", r, "--", GATE_FILE]).split()
        if len(line) >= 3:
            shas.append(line[2])
    out = set()
    for blob in _cat_blobs(sorted(set(shas))):
        src = blob.decode("utf-8", "replace")
        try:
            out |= {base(x) for x in gen_denylist.committed_gate(src)}
        except Exception:  # noqa: BLE001
            out |= {base(x) for x in re.findall(r"""["']([a-z0-9][a-z0-9_.-]+)["']""", src)}
    return out


# ------------------------------------------------------------------------------------------------- gate + keep set
def load_gate() -> Gate:
    try:
        ids = gen_denylist.committed_gate()
    except Exception as e:  # noqa: BLE001
        raise Refused(f"the committed gate could not be read ({type(e).__name__}) - an unreadable gate is not "
                      "an empty gate (R900)") from None
    g = Gate(ids)
    if not g:
        raise Refused("the committed gate read EMPTY (no api/worker/src/denylist.ts?) - refusing (R900)")
    return g


def load_carveouts() -> dict:
    try:
        return {base(k): {base(c) for c in v} for k, v in gen_denylist.committed_carveouts().items()}
    except Exception as e:  # noqa: BLE001
        raise Refused(f"the committed carve-outs could not be read ({type(e).__name__})") from None


def _load_tool(name: str):
    """Load tools/<name>.py by path (a side-effect-free module: functions and constants only)."""
    import importlib.util                                                    # noqa: PLC0415
    spec = importlib.util.spec_from_file_location(f"_b32_{name}", os.path.join(ROOT, "tools", f"{name}.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


UNVERDICTED = "(info) a member has no verdict entry, so CLEARED/REVIEW cannot be read for it"


def selected_members(gate: Gate, hosted_ids) -> tuple:
    """(Gate of the SELECTED members, {keep reason: bool}, keep set) - R908 rule 2, review rev 6.6 L2. The keep set
    is derived by code from tracked files and the catalogue (nothing typed here):
      CLEARED / REVIEW  the member's verdict in DATABASE_LICENSES_VERBATIM.md, as tools/audit_license_coverage.py
                        assessed() parses it, contains 'CLEARED' or 'REVIEW' (a substring test: it keeps MORE);
      served            the member is in SUPPORTED_SOURCES of util.ts (tools/audit_schedule_coverage.py);
      hosted            the member has a source row in the catalogue;
      carve-out anchor  the member keys a committed SERIES_CARVEOUTS block;
      written grant     the member is in core.gen_denylist.GRANTED_EXCEPTIONS.
    Since plan rev 6.7 the keep set does not choose what is deleted (the allow-list view does); its hash is bound
    into the plan and the apply (R908 rule 5). An unreadable record is a Refused (R900)."""
    try:
        verdicts = {base(k): str(v) for k, v in _load_tool("audit_license_coverage").assessed().items()}
        supported = {base(s) for s in _load_tool("audit_schedule_coverage").supported_sources()}
        carve = {base(k) for k in gen_denylist.committed_carveouts()}
        grants = {base(s) for s in getattr(gen_denylist, "GRANTED_EXCEPTIONS", set())}
    except BaseException as e:  # noqa: BLE001 - SystemExit from a parser included
        raise Refused(f"the keep set could not be derived ({type(e).__name__}) - refusing (R908 rule 2, R900)") \
            from None
    if not verdicts or not supported:
        raise Refused("the keep set could not be derived (an empty verdict index or SUPPORTED_SOURCES)")
    hosted = {base(h) for h in hosted_ids}
    keep, why = set(), {"CLEARED": False, "REVIEW": False, "served": False, "hosted": False,
                        "carve-out anchor": False, "written grant": False}
    why[UNVERDICTED] = any(m not in verdicts for m in gate.ms)
    for m in gate.ms:
        v = verdicts.get(m, "").upper()
        tests = {"CLEARED": "CLEARED" in v, "REVIEW": "REVIEW" in v, "served": m in supported,
                 "hosted": m in hosted, "carve-out anchor": m in carve, "written grant": m in grants}
        if any(tests.values()):
            keep.add(m)
            for k, b in tests.items():
                why[k] |= b
    return Gate(set(gate.ms) - keep), why, keep


# ------------------------------------------------------------------------------------------------------------ paths
def _norm_path(p: str) -> str:
    return os.path.normcase(os.path.realpath(p))


def _under(p: str, root: str) -> bool:
    r = _norm_path(root).rstrip("\\/")
    return p == r or p.startswith(r + os.sep)


def _samefile(a: str, b: str) -> bool:
    try:
        return os.path.samefile(a, b)
    except (OSError, ValueError, TypeError):
        return False


def _flag_present() -> bool:
    try:
        os.stat(cutover.FLAG_PATH)
        return True
    except OSError:
        return False


def held_writers_problems() -> list:
    logs = os.path.join(catalog_path.LIVE_STORE_ROOT, "logs")
    probs = []
    for name in ("STOP_DISPATCH", "local_heavy.DONE"):
        if not os.path.exists(os.path.join(logs, name)):
            probs.append(f"logs\\{name} is absent (the writers are not held, plan S1/S4)")
    for name in ("local_heavy.lock", "statcan_writer.lock"):
        if os.path.lexists(os.path.join(logs, name)):
            probs.append(f"logs\\{name} is present (a writer is running)")
    return probs


def header(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read(100)


def resolve_target(db: str, live: bool) -> str:
    """The real path of the database to open, or Refused. The rules are in the module docstring."""
    if not db:
        raise Refused("--db is required and must name a file")
    if db.startswith(("\\\\", "//")):
        raise Refused("a path that starts with two slashes (UNC, \\\\?\\ or \\\\.\\) is refused")
    p = os.path.realpath(db)
    if p.startswith(("\\\\", "//")):
        raise Refused("the real path starts with two slashes (UNC or \\\\?\\) - refused")
    if not os.path.isfile(p):
        raise Refused("--db names no existing file")
    if os.stat(p).st_nlink != 1:
        raise Refused("the file has more than one hard link - a shared copy; make a NEW full byte copy")
    for s in SIDE:
        if os.path.lexists(p + s):
            raise Refused(f"a {s} file lies beside the database - refused (a leftover journal can hold rows, "
                          "and a hot one means a writer died mid-transaction)")
    h = header(p)
    if len(h) < 100 or not h.startswith(b"SQLite format 3\x00"):
        raise Refused("not an SQLite 3 database file")
    if h[18] != 1 or h[19] != 1:
        raise Refused("the header says WAL mode - refused (this tool works on rollback-journal files only)")
    if struct.unpack(">I", h[56:60])[0] not in (0, 1):
        raise Refused("the text encoding is not UTF-8")
    n = os.path.normcase(p)
    is_build = _samefile(p, catalog_path.BUILD_PATH) or n == _norm_path(catalog_path.BUILD_PATH)
    live_area = _under(n, catalog_path.LIVE_STORE_ROOT) or any(_under(n, a) for a in LIVE_AREAS)
    if live:
        if not is_build or n != _norm_path(catalog_path.BUILD_PATH):
            raise Refused("--live names only the build itself (core.catalog_path.BUILD_PATH)")
        if not _samefile(ROOT, catalog_path.LIVE_STORE_ROOT):
            raise Refused("--live runs only from the production checkout (so the gate is ITS denylist.ts)")
        probs = held_writers_problems()
        if probs:
            raise Refused("--live needs the writers held: " + "; ".join(probs))
        return p
    if is_build or live_area:
        raise Refused("the live build / live checkout / E:\\econ_live is refused without --live (R1940)")
    if not any(_under(n, r) for r in PROOF_ROOTS):
        raise Refused("before T0 a proof copy must lie under a PROOF_ROOTS folder")
    if _flag_present() or cutover.is_cut_over():
        raise Refused("the CUTOVER flag is set (or unreadable): after T0 only --live on the build is accepted")
    return p


def file_state(p: str) -> tuple:
    st = os.stat(p)
    h = header(p)
    return (os.path.normcase(os.path.realpath(p)), st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns,
            h[24:28].hex(), h[28:32].hex())


def tool_sha() -> str:
    """The git blob id of this file at HEAD (code review r3 C3: a CRLF checkout hashes differently on disk, so the
    reviewed identity is the committed blob; _check_inputs() refuses when the working file differs from HEAD)."""
    return _git(["rev-parse", f"HEAD:{TOOL_REL}"]).decode().strip()


def confirm_token(p: str, mode: str) -> str:
    s = "|".join(map(str, file_state(p))) + "|" + tool_sha() + "|" + mode
    return "B32-" + hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def connect_ro(p: str):
    con = catalog_path.connect_path(p, write=False)
    con.execute("PRAGMA temp_store=MEMORY")       # a sort spill would be a temp file holding row text
    con.execute("PRAGMA query_only=1")
    return con


def _lower_priority() -> None:
    if os.name != "nt":
        with contextlib.suppress(Exception):
            os.nice(5)
        return
    try:
        import ctypes                                                         # noqa: PLC0415
        k = ctypes.windll.kernel32
        k.GetCurrentProcess.restype = ctypes.c_void_p
        k.SetPriorityClass.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        k.SetPriorityClass(k.GetCurrentProcess(), 0x00004000)               # BELOW_NORMAL_PRIORITY_CLASS
    except Exception:  # noqa: BLE001
        pass


# ---- the sealed state file and backup (plan -> apply; proof -> live) -------------------------------------------
def set_hash(ids) -> str:
    """sha256 of a sorted id set. It is a deterministic function of a set that may hold member-carrying ids, so it
    is written ONLY into the sealed state file and compared in process - never printed or logged (review L5)."""
    return hashlib.sha256("\n".join(sorted(ids)).encode("utf-8")).hexdigest()


def _work_path(path: str, must_exist: bool, what: str) -> str:
    if not path or path.startswith(("\\\\", "//")):
        raise Refused(f"{what} must name a file in the work folder")
    p = os.path.realpath(path)
    if not any(_under(os.path.normcase(p), r) for r in PROOF_ROOTS):
        raise Refused(f"{what} must lie under a PROOF_ROOTS folder (the work folder), never beside the build")
    if must_exist and not os.path.isfile(p):
        raise Refused(f"{what} names no file - run --plan first")
    if not must_exist and os.path.lexists(p):
        raise Refused(f"{what} already exists - it is written once; name a new one")
    return p


def _seal(body: dict) -> str:
    """A checksum of the content (detects an edit; it is not a secret-keyed signature - review L7)."""
    return hashlib.sha256(repr(sorted(body.items())).encode("utf-8")).hexdigest()


def write_state(path: str, body: dict) -> None:
    import json                                                               # noqa: PLC0415
    p = _work_path(path, must_exist=False, what="--state")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "x", encoding="utf-8") as f:
        json.dump({"body": body, "seal": _seal(body)}, f, sort_keys=True)


def read_state(path: str) -> dict:
    import json                                                               # noqa: PLC0415
    p = _work_path(path, must_exist=True, what="--state")
    try:
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        body = d["body"]
        good = d["seal"] == _seal(body)
    except Exception:  # noqa: BLE001
        raise Refused("the state file cannot be read") from None
    if not good:
        raise Refused("the state file's seal does not match its content - it was edited")
    return body


def write_backup(path: str, gate: Gate, cols, rows) -> None:
    """The sealed backup of the deleted rows that carry NO member. Refused - nothing written - if any value, or the
    serialised file text, carries a member (of `gate`: today's gate plus its git history) in any sense."""
    import json                                                               # noqa: PLC0415
    p = _work_path(path, must_exist=False, what="--backup")
    body = {"kind": "b32-backup-no-member-rows", "cols": list(cols), "rows": [list(r) for r in rows],
            "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    text = json.dumps({"body": body, "seal": _seal({"rows": repr(body["rows"])})}, sort_keys=True)
    carries = any(gate.cell(v) for r in rows for v in r if isinstance(v, str)) or bool(gate.cell(text))
    say(f"backup of the no-member rows carries a member of the gate or its history (values and file text): "
        f"{yn(carries)}")
    if carries:
        raise Refused("the backup would carry a member - nothing written, nothing deleted")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "x", encoding="utf-8") as f:
        f.write(text)


# ------------------------------------------------------------------------------------------------- licence state
def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def schema(con) -> list:
    return [(r[0], r[1] or "") for r in con.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name")]


def ref_columns(con, tables) -> list:
    """Every (table, column) outside `license` whose column is named license_id. A column whose name merely
    CONTAINS 'licen' is refused: this tool cannot tell whether it references a licence row."""
    out = []
    for t, _sql in tables:
        if t == "license":
            continue
        for c in con.execute(f"PRAGMA table_xinfo({q(t)})").fetchall():
            name = str(c[1])
            if name.lower() == "license_id":
                out.append((t, name))
            elif "licen" in name.lower():
                raise Refused("a column whose name contains 'licen' but is not license_id exists - this tool "
                              "cannot tell whether it references a licence row; review needed")
    return out


def licence_state(con) -> dict:
    """The licence rows and every reference to them. ONE DISTINCT scan per referencing column (L2)."""
    tables = schema(con)
    if "license" not in {t for t, _ in tables}:
        raise Refused("no license table")
    cols = [str(c[1]) for c in con.execute("PRAGMA table_xinfo(license)").fetchall()]
    if "license_id" not in cols:
        raise Refused("the license table has no license_id column")
    rows = con.execute("SELECT * FROM license").fetchall()
    li = cols.index("license_id")
    ids = []
    for r in rows:
        if not isinstance(r[li], str) or not r[li]:
            raise Refused("a licence row has a non-text or empty license_id - review needed")
        ids.append(r[li])
    refs = ref_columns(con, tables)
    exact, folded, by_table = set(), set(), {}
    for t, c in refs:
        seen = set()
        for v, typ in con.execute(f"SELECT DISTINCT {q(c)}, typeof({q(c)}) FROM {q(t)}"):
            if v is None:
                continue
            if typ != "text":
                raise Refused(f"{t}.{c} holds a non-text value - review needed")
            seen.add(v)
        by_table[(t, c)] = seen
        exact |= seen
        folded |= {v.strip().lower() for v in seen}
    unref = [i for i in ids if i not in exact and i.strip().lower() not in folded]
    return {"cols": cols, "rows": rows, "ids": ids, "refs": refs, "exact": exact, "folded": folded,
            "by_table": by_table, "unref": unref, "dangling": bool(exact - set(ids))}


# ------------------------------------------------------------------------------------ the selection (plan rev 6.7)
def selection(con, gate: Gate, wide: Gate, with_code: bool = True, st=None) -> dict:
    """The licence classes (in-memory sets; nothing here prints). `gate` = today's deny list; `wide` = today's plus
    every committed version's (history_members) - used wherever a row could reach a file.

      unref     licence rows that NO row of any other table references through a license_id column (allow-list)
      named     unref rows whose id a tracked file at HEAD names as a token (code_named)
      delete    unref - named: DELETED by --apply (with or without a member, review rev 6.6 M3)
      del_mem   delete rows that carry a member of `wide`: deleted, never written to a file
      del_clean delete rows that carry none: written to the sealed backup before the delete
      sub_only  del_mem rows whose every hit is a plain substring (SUBALL only) - YES/NO (code review L9)
      named_mem named rows that carry a member of `wide`: KEPT and reported for B34's code PR (K1N)
      held      v6: REFERENCED rows that carry a member of today's gate (licence rows hosted sources use)"""
    st = licence_state(con) if st is None else st
    li = st["cols"].index("license_id")
    unref = set(st["unref"])
    wide_carry, gate_carry, sub_only = set(), set(), set()
    for r in st["rows"]:
        texts = [v for v in r if isinstance(v, str)]
        hits = set().union(*(wide.cell(v) for v in texts)) if texts else set()
        if hits:
            wide_carry.add(r[li])
            if hits == {"SUBALL"}:
                sub_only.add(r[li])
        if any(gate.cell(v) for v in texts):
            gate_carry.add(r[li])
    named, head = (code_named(unref) if with_code else (set(), ""))
    delete = unref - named
    return {"st": st, "unref": unref, "named": named, "head": head, "delete": delete,
            "del_mem": delete & wide_carry, "del_clean": delete - wide_carry, "sub_only": delete & sub_only,
            "named_mem": named & wide_carry, "held": gate_carry - unref}


def gates(con) -> tuple:
    """(gate, wide gate, keep reasons, keep set). MEASURED 2026-11-28 at 6d496eb88: no member of today's gate has an
    entry tools/audit_license_coverage.py can read, so the CLEARED/REVIEW part of the keep set is empty by
    derivation (reported as the (info) line)."""
    gate = load_gate()
    src = con.execute("SELECT source_id FROM source").fetchall()
    _sel, why, keep = selected_members(gate, [r[0] for r in src if isinstance(r[0], str)])
    wide = Gate(set(gate.ms) | history_members())
    return gate, wide, why, keep


def _check_inputs() -> str:
    if not tree_clean():
        raise Refused("an input this tool reads from the checkout differs from HEAD or is untracked - commit or "
                      "restore it first (code review M3)")
    return head_sha()


def _older_copy_misses(rows_by_id: dict, older: str):
    """True when some deleted member row is ABSENT (by its whole row) from the older F: copy - no F: copy then holds
    it after the delete (INFO, review rev 6.6 L1); None when the copy cannot be read."""
    import pathlib                                                            # noqa: PLC0415
    import sqlite3                                                            # noqa: PLC0415
    if not rows_by_id:
        return False
    # names used nowhere else in this file: the repo's plain-open taint rule is file-wide by name
    older_real = os.path.normcase(os.path.realpath(older)) if older else ""
    if not older_real or older_real.startswith(("\\\\", "//")) or \
            _under(older_real, catalog_path.LIVE_STORE_ROOT) or any(_under(older_real, a) for a in LIVE_AREAS) \
            or not os.path.isfile(older_real):
        return None                                         # never E:'s live files (R1940); a missing copy: None
    try:
        older_uri = pathlib.Path(older_real).resolve().as_uri() + "?mode=ro&immutable=1"
        older_con = sqlite3.connect(older_uri, uri=True)  # plain-open: an F: copy, mode=ro&immutable=1 only
        try:
            older_con.execute("PRAGMA temp_store=MEMORY")
            have = {r[0]: tuple(r) for r in older_con.execute("SELECT * FROM license")}
        finally:
            older_con.close()
    except Exception:  # noqa: BLE001
        return None
    return any(have.get(lid) != tuple(row) for lid, row in rows_by_id.items())


# ------------------------------------------------------------------------------------- record images (signatures)
def _varint(v: int) -> bytes:
    """SQLite's big-endian varint (1-9 bytes)."""
    if v < 0:
        v &= (1 << 64) - 1
    if v > 0x00FFFFFFFFFFFFFF:
        out = [v & 0xFF]
        v >>= 8
        for _ in range(8):
            out.append((v & 0x7F) | 0x80)
            v >>= 7
        return bytes(reversed(out))
    out = [v & 0x7F]
    v >>= 7
    while v:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    return bytes(reversed(out))


def _serial(v, fmt4: bool) -> tuple:
    if v is None:
        return 0, b""
    if isinstance(v, bool):
        v = int(v)
    if isinstance(v, int):
        if fmt4 and v in (0, 1):
            return 8 + v, b""
        for t, n in ((1, 1), (2, 2), (3, 3), (4, 4), (5, 6), (6, 8)):
            if -(1 << (8 * n - 1)) <= v < (1 << (8 * n - 1)):
                return t, v.to_bytes(n, "big", signed=True)
    if isinstance(v, float):
        return 7, struct.pack(">d", v)
    if isinstance(v, str):
        b = v.encode("utf-8")
        return 13 + 2 * len(b), b
    if isinstance(v, (bytes, bytearray)):
        return 12 + 2 * len(v), bytes(v)
    raise Refused("an unexpected value type in a licence row")


def record_image(values, fmt4: bool) -> bytes:
    """The exact SQLite record of `values` (header-size varint, serial types, body)."""
    types, body = [], b""
    for v in values:
        t, b = _serial(v, fmt4)
        types.append(_varint(t))
        body += b
    hdr = b"".join(types)
    n = len(hdr) + 1
    while len(_varint(n)) + len(hdr) != n:
        n = len(_varint(n)) + len(hdr)
    return _varint(n) + hdr + body


def row_signatures(con, p: str, ids) -> list:
    """For each licence row in `ids`: its table record and its primary-key index record (license_id, rowid), as
    exact bytes (code review M5: whole records, never 'id + one byte'). In memory only."""
    fmt4 = struct.unpack(">I", header(p)[44:48])[0] >= 4
    want, out = set(ids), []
    for row in con.execute("SELECT rowid, * FROM license"):
        rid, vals = row[0], row[1:]
        cols = [str(c[1]) for c in con.execute("PRAGMA table_xinfo(license)").fetchall()]
        lid = vals[cols.index("license_id")]
        if lid not in want:
            continue
        out.append(record_image(vals, fmt4))
        out.append(record_image((lid, rid), fmt4))
    return out


# ----------------------------------------------------------------------------------------------------- raw pages
_BTREE = {2: 12, 5: 12, 10: 8, 13: 8}


def _freelist_pages(f, page_size: int, trunk: int, total: int, npages: int) -> set:
    free, seen = set(), set()
    while trunk:
        if trunk in seen or trunk > npages or len(free) > total + 1:
            raise Refused("the freelist is damaged")
        seen.add(trunk)
        f.seek((trunk - 1) * page_size)
        pg = f.read(page_size)
        nxt, n = struct.unpack(">II", pg[:8])
        if n > (page_size // 4) - 2:
            raise Refused("the freelist is damaged")
        free.add(trunk)
        free.update(struct.unpack(f">{n}I", pg[8:8 + 4 * n]))
        trunk = nxt
    if len(free) != total:
        raise Refused("the freelist walk does not match the header's free page count")
    return free


def _unalloc(pg: bytes, h: int):
    """The unallocated byte ranges of a b-tree page: the gap after the cell-pointer array and every freeblock."""
    hdr = _BTREE[pg[h]]
    ncell = struct.unpack(">H", pg[h + 3:h + 5])[0]
    start = struct.unpack(">H", pg[h + 5:h + 7])[0] or 65536
    out = []
    a = h + hdr + 2 * ncell
    if a < start <= len(pg):
        out.append((a, start))
    fb, hops = struct.unpack(">H", pg[h + 1:h + 3])[0], 0
    while fb and hops < 4096 and fb + 4 <= len(pg):
        nxt, size = struct.unpack(">HH", pg[fb:fb + 4])
        out.append((fb + 4, min(len(pg), fb + size)))
        fb, hops = nxt, hops + 1
    return out


def _raw_hit(gate: Gate, data: bytes) -> bool:
    """A deny-list member in raw bytes, by a bounded sense (EXACT/KEY/TOKEN/LOOSE/SEPN/CONCAT or SUB5) - a short
    member as a plain substring of raw page bytes is not evidence (code review M7)."""
    v = data.lower().decode("latin-1")
    return bool(gate.maybe(v) and (gate.senses_base(v) - {"SUBALL"}))


def _page_ranges(npages: int, lockpg: int) -> list:
    if lockpg > npages:
        return [(1, npages)]
    return [r for r in ((1, lockpg - 1), (lockpg + 1, npages)) if r[0] <= r[1]]


def residue_scan(p: str, gate: Gate, sigs=None) -> dict:
    """Raw page scan, outside SQLite, under a SHARED read transaction. Booleans only:
      FREE / UNALLOC / SIDECAR  a deny-list member in that region;
      SIG_SEEN                  every signature was found somewhere (the control before a delete);
      SIG_LIVE / SIG_DEAD       a signature found in a live cell / in a free page or unallocated space."""
    sigs = [s for s in (sigs or ()) if s]
    res = {"FREE": False, "UNALLOC": False, "SIDECAR": False, "SIG_LIVE": False, "SIG_DEAD": False,
           "SIG_SEEN": not sigs, "measured": True}
    seen = set()
    con = connect_ro(p)
    try:
        con.execute("BEGIN")
        con.execute("SELECT count(*) FROM sqlite_master").fetchone()        # SHARED held from here
        with open(p, "rb") as f:
            h = f.read(100)
            ps = struct.unpack(">H", h[16:18])[0]
            ps = 65536 if ps == 1 else ps
            npages = os.path.getsize(p) // ps
            hdr_pages = struct.unpack(">I", h[28:32])[0]
            if h[24:28] == h[92:96] and hdr_pages:
                npages = min(npages, hdr_pages)
            free = _freelist_pages(f, ps, struct.unpack(">I", h[32:36])[0], struct.unpack(">I", h[36:40])[0],
                                   npages)
            lockpg = (1 << 30) // ps + 1                    # the lock-byte page holds no data and is not read
            keep = max([gate.maxlen] + [len(s) for s in sigs]) - 1
            if keep >= ps:
                raise Refused("a signature is longer than a page")
            chunk = RAW_CHUNK_PAGES

            def region(abs_off: int, first: int, buf: bytes, prev_page) -> str:
                """DEAD (a free page, or a b-tree page's unallocated space) or LIVE, for the page holding
                abs_off; the page comes from this chunk (pages first..) or is the previous chunk's last page."""
                n = abs_off // ps + 1
                if n in free:
                    return "DEAD"
                if prev_page is not None and n == prev_page[0]:
                    pg = prev_page[1]
                elif first <= n < first + len(buf) // ps:
                    pg = buf[(n - first) * ps:(n - first + 1) * ps]
                else:
                    raise Refused("a signature's page is not in the scan window")   # cannot tell: never LIVE/NO
                o = 100 if n == 1 else 0
                if pg[o] in _BTREE:
                    within = abs_off - (n - 1) * ps
                    if any(a <= within < b for a, b in _unalloc(pg, o)):
                        return "DEAD"
                return "LIVE"

            for lo, hi in _page_ranges(npages, lockpg):
                free_tail, raw_tail, pg_no, prev_page = "", b"", lo, None
                while pg_no <= hi:
                    last = min(hi, pg_no + chunk - 1)
                    f.seek((pg_no - 1) * ps)
                    want = (last - pg_no + 1) * ps
                    buf = f.read(want)
                    if len(buf) != want:
                        raise OSError("short read")
                    if sigs:
                        # the tail of the previous chunk is re-read so that an image crossing the chunk edge is
                        # seen; an image that lies wholly inside that tail was already counted there and is
                        # skipped, and one that STARTS there is classified by its own page (code review r3 C2)
                        joined = raw_tail + buf
                        jbase = (pg_no - 1) * ps - len(raw_tail)
                        for k, s in enumerate(sigs):
                            i = joined.find(s)
                            while i >= 0:
                                if i + len(s) > len(raw_tail):
                                    seen.add(k)
                                    if region(jbase + i, pg_no, buf, prev_page) == "LIVE":
                                        res["SIG_LIVE"] = True
                                    else:
                                        res["SIG_DEAD"] = True
                                i = joined.find(s, i + 1)
                        raw_tail = buf[-keep:] if keep > 0 else b""
                        prev_page = (last, buf[-ps:])
                    mv = memoryview(buf)
                    for k in range(last - pg_no + 1):
                        n = pg_no + k
                        if n in free:
                            txt = free_tail + bytes(mv[k * ps:(k + 1) * ps]).lower().decode("latin-1")
                            if not res["FREE"] and gate.maybe(txt) and (gate.senses_base(txt) - {"SUBALL"}):
                                res["FREE"] = True
                            free_tail = txt[-keep:] if keep > 0 else ""
                            continue
                        free_tail = ""
                        if res["UNALLOC"]:
                            continue
                        off = 100 if n == 1 else 0
                        if mv[k * ps + off] in _BTREE:
                            pg = bytes(mv[k * ps:(k + 1) * ps])
                            for a, b in _unalloc(pg, off):
                                if b > a and _raw_hit(gate, pg[a:b]):
                                    res["UNALLOC"] = True
                                    break
                    pg_no = last + 1
        for s in SIDE:
            if os.path.lexists(p + s):
                with open(p + s, "rb") as sf:
                    tail = b""
                    while True:
                        b = sf.read(1 << 22)
                        if not b:
                            break
                        res["SIDECAR"] |= _raw_hit(gate, tail + b) or any(x in tail + b for x in sigs)
                        tail = b[-keep:] if keep > 0 else b""
        con.execute("COMMIT")
    except (OSError, Refused) as e:
        say(f"raw scan: UNMEASURED ({type(e).__name__})")
        res["measured"] = False
    finally:
        con.close()
    res["SIG_SEEN"] = len(seen) == len(sigs)
    return res


def residue_control(gate: Gate) -> bool:
    g = plant_member(gate)
    pos = b"\x00\x81\x07" + g.upper().encode() + b"\x00\x13"
    neg = b"\x00\x81\x07~~~~~~\x00\x13"
    a, b = _raw_hit(gate, pos), _raw_hit(gate, neg)
    say(f"raw-scan control: planted member fires: {'PASS' if a else 'FAIL'}; negative silent: "
        f"{'PASS' if not b else 'FAIL'}")
    return a and not b


def do_residue(p: str) -> int:
    t0 = time.time()
    _check_inputs()
    gate = load_gate()
    if not residue_control(gate):
        say("RESIDUE CONTROL FAILED - no answer below is trustworthy")
        return ERR
    res = residue_scan(p, gate)
    say(f"raw scan measured every region: {yn(res['measured'])}")
    say(f"deny-list member in FREE pages (freelist trunk + leaf): {yn(res['FREE'])}")
    say(f"deny-list member in UNALLOC b-tree space (gap + freeblocks; stale LIVE copies sit there too): "
        f"{yn(res['UNALLOC'])}")
    say(f"deny-list member in a sidecar file: {yn(res['SIDECAR'])}")
    say(f"residue wall time: {time.time() - t0:.1f} s")
    return OK if res["measured"] else ERR


# ------------------------------------------------------------------------------------------------------------ VACUUM
def _check_tmpdir(tmpdir: str) -> str:
    """The VACUUM temp folder: an empty folder under PROOF_ROOTS (an allow-list - code review M1; temp_store=MEMORY
    keeps VACUUM's temp copy in RAM, the folder catches anything SQLite still writes)."""
    if not tmpdir or tmpdir.startswith(("\\\\", "//")):
        raise Refused("--tmpdir must name a folder under PROOF_ROOTS (the work folder, F:)")
    td = os.path.realpath(tmpdir)
    if not any(_under(os.path.normcase(td), r) for r in PROOF_ROOTS):
        raise Refused("--tmpdir must lie under PROOF_ROOTS (the work folder on F:), never C:, D: or E:")
    os.makedirs(td, exist_ok=True)
    if os.listdir(td):
        raise Refused("--tmpdir must be an empty folder")
    return td


def other_handles(p: str):
    """True when another handle has the file open, False when none, None when it cannot be told. On Windows it
    opens the file with share mode 0: that fails with ERROR_SHARING_VIOLATION (32) while ANY other handle - an
    SQLite connection of any process, an editor, a backup reader - is open. Call it with no connection of our own.
    Elsewhere None (BEGIN EXCLUSIVE is then the only check)."""
    if os.name != "nt":
        return None
    import ctypes                                                             # noqa: PLC0415
    from ctypes import wintypes                                               # noqa: PLC0415
    k = ctypes.WinDLL("kernel32", use_last_error=True)
    k.CreateFileW.restype = wintypes.HANDLE
    k.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD,
                              wintypes.DWORD, wintypes.HANDLE]
    k.CloseHandle.argtypes = [wintypes.HANDLE]
    h = k.CreateFileW(p, 0x80000000, 0, None, 3, 0x80, None)                # GENERIC_READ, no sharing, OPEN_EXISTING
    if h is None or h == ctypes.c_void_p(-1).value:
        return True if ctypes.get_last_error() == 32 else None
    k.CloseHandle(h)
    return False


def vacuum_preconditions(p: str) -> None:
    import shutil                                                            # noqa: PLC0415
    room = shutil.disk_usage(os.path.dirname(p)).free >= 2 * os.path.getsize(p)
    say(f"VACUUM precondition: free space on the database's drive >= 2x the file size: {yn(room)}")
    if not room:
        raise Refused("less than 2x the file size is free on the database's drive (temp copy + rollback journal)")
    busy = other_handles(p)
    say(f"VACUUM precondition: another handle has the file open: "
        f"{'CANNOT TELL' if busy is None else yn(busy)}")
    if busy or (busy is None and os.name == "nt"):
        raise Refused("another handle has the database open, or the check could not tell (code review M2)")


def vacuum_core(p: str, td: str, check: str, cache_mib: int = 0) -> list:
    """VACUUM with its preconditions; returns the receipt lines [(text, ok)]. The caller holds the writer lock."""
    os.environ["TMP"] = os.environ["TEMP"] = os.environ["SQLITE_TMPDIR"] = td
    vacuum_preconditions(p)
    con = catalog_path.connect_path(p, write=True, timeout=5.0)
    con.isolation_level = None
    try:
        for pragma, want in (("journal_mode=DELETE", "delete"), ("locking_mode=NORMAL", "normal")):
            r = con.execute(f"PRAGMA {pragma}").fetchone()
            if r is None or r[0] != want:
                raise Refused(f"PRAGMA {pragma.split('=')[0]} did not read back as required")
        con.execute("PRAGMA temp_store=MEMORY")
        con.execute("PRAGMA secure_delete=ON")
        if cache_mib > 0:
            # a big page cache keeps VACUUM's dirty pages in RAM instead of spilling them through journal syncs
            # (the default-cache proof run grew its journal at about 4-5 MB/s); costs that much RAM
            con.execute(f"PRAGMA cache_size = {-int(cache_mib) * 1024}")
        say(f"VACUUM page cache: {'SQLite default' if cache_mib <= 0 else f'{cache_mib} MiB'}")
        con.execute("BEGIN EXCLUSIVE")                 # no other connection holds a lock right now
        con.execute("COMMIT")
        t_v = time.time()
        con.execute("VACUUM")
        t_v = time.time() - t_v
        say(f"VACUUM: done  ({t_v:.1f} s)")
        fl = con.execute("PRAGMA freelist_count").fetchone()[0]
    finally:
        con.close()
    side = [x for x in SIDE if os.path.lexists(p + x)]
    tmp_left = bool(os.listdir(td))
    con = connect_ro(p)
    try:
        con.execute("BEGIN")
        dangling = licence_state(con)["dangling"]
        con.execute("COMMIT")
        t_q = time.time()
        qc = con.execute("PRAGMA quick_check" if check == "quick" else "PRAGMA integrity_check").fetchall()
        t_q = time.time() - t_q
    finally:
        con.close()
    return [("freelist_count = 0 after VACUUM", fl == 0),
            ("no -journal/-wal/-shm beside the file after VACUUM", not side),
            ("--tmpdir left empty (the VACUUM temp copy was held in memory)", not tmp_left),
            ("no dangling licence reference (every series/source license_id has a row)", not dangling),
            (f"PRAGMA {'quick_check' if check == 'quick' else 'integrity_check'} = ok  ({t_q:.1f} s)",
             qc == [("ok",)])]


def _receipt(name: str, checks, info=(), not_covered=()) -> int:
    for text, good in checks:
        say(f"  {'PASS' if good else 'FAIL'}  {text}")
    for text in info:
        say(f"  INFO  {text}")
    for text in not_covered:
        say(f"  NOT COVERED  {text}")
    good = all(g for _, g in checks)
    say(f"{name} {'PASS' if good else 'FAIL'}")
    return OK if good else FAIL


NOT_COVERED = ("D1 (econ-catalog, econ-catalog-climate): this tool reads the local file only",
               "byte copies made before this run (F: copies, R2 .bak objects, the coherence copy, S7c/B7 backups, "
               "origin generations, shadow copies): owner item 37",
               "the unlinked rollback journal's and VACUUM journal's disk clusters (raw-disk residue, no file)",
               "state.db, index.db, the S4c queue copies and every other file: not this tool's surface")


def do_vacuum(p: str, live: bool, tmpdir: str, check: str, cache_mib: int = 0) -> int:
    t0 = time.time()
    _check_inputs()
    gate = load_gate()
    if not residue_control(gate):
        raise Refused("the raw-scan control failed")
    td = _check_tmpdir(tmpdir)
    lock = catalog_path.writer_lock() if live else contextlib.nullcontext()
    with lock:
        if live and catalog_path._held is None:                             # noqa: SLF001
            raise Refused("the writer lock is not held")
        checks = vacuum_core(p, td, check, cache_mib)
        t_r = time.time()
        res = residue_scan(p, gate)
        checks += [(f"raw scan after VACUUM measured every page ({time.time() - t_r:.1f} s)", res["measured"]),
                   ("a deny-list member in free pages: NO", not res["FREE"])]
    say(f"vacuum wall time: {time.time() - t0:.1f} s")
    return _receipt("VACUUM", checks,
                    info=[f"a deny-list member in unallocated b-tree space after VACUUM: {yn(res['UNALLOC'])} (stale "
                          "copies of LIVE cells, e.g. the held mentions, can sit there)",
                          "the deleted rows' record images are not re-checked here (never stored: member bytes); "
                          "--apply checked them absent from every live cell, and VACUUM rebuilds every page from "
                          "the live rows only"],
                    not_covered=NOT_COVERED)


# ----------------------------------------------------------------------------------------------------------- --plan
def do_plan(p: str, state: str) -> int:
    t0 = time.time()
    _work_path(state, must_exist=False, what="--state")
    head = _check_inputs()
    con = connect_ro(p)
    try:
        con.execute("BEGIN")
        gate, wide, why, keep = gates(con)
        s = selection(con, gate, wide)
        fl = con.execute("PRAGMA freelist_count").fetchone()[0]
        con.execute("COMMIT")
    finally:
        con.close()
    if not matcher_self_test(gate):
        say("PLAN FAIL: the matcher self-test failed")
        return ERR
    say(f"referencing columns: {', '.join(f'{t}.{c}' for t, c in s['st']['refs']) or '(none)'}")
    for k, b in why.items():
        say(f"keep set (R908 rule 2): {k if k.startswith('(') else 'a member is kept as ' + k}: {yn(b)}")
    say(f"the gate's git history was read (former members widen the backup test): YES")
    say(f"licence rows to delete exist (unreferenced, named by no tracked file): {yn(bool(s['delete']))}")
    say(f"   ... of them, carrying a member (deleted, never written to a file): {yn(bool(s['del_mem']))}")
    say(f"   ...... of those, matched only as a plain substring (no backup either): {yn(bool(s['sub_only']))}")
    say(f"   ... of them, carrying none (backed up, sealed, then deleted): {yn(bool(s['del_clean']))}")
    say(f"unreferenced rows a tracked file names, carrying a member (KEPT; B34's code PR first): "
        f"{yn(bool(s['named_mem']))}")
    say(f"unreferenced rows a tracked file names, carrying none (KEPT): {yn(bool(s['named'] - s['named_mem']))}")
    say(f"a reference names no licence row (dangling): {yn(s['st']['dangling'])}")
    say(f"free pages present: {yn(fl > 0)}")
    say(f"tracked files read at HEAD {head[:12]}")
    write_state(state, {"kind": "plan", "tool_sha": tool_sha(), "token": confirm_token(p, "apply"),
                        "db": os.path.normcase(p), "sel_hash": set_hash(s["delete"]), "keep_hash": set_hash(keep),
                        "held_hash": set_hash(s["held"]), "head": head,
                        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    say("state written (sealed; it holds hashes only and is never printed)")
    say(f"--confirm {confirm_token(p, 'apply')}")
    say(f"plan wall time: {time.time() - t0:.1f} s")
    ok = not s["st"]["dangling"]
    say(f"PLAN {'PASS' if ok else 'FAIL (a dangling licence reference)'}")
    return OK if ok else FAIL


# ---------------------------------------------------------------------------------------------------------- --apply
def do_apply(p: str, live: bool, state: str, confirm: str, tmpdir: str, check: str, backup: str, older: str,
             no_vacuum: bool, cache_mib: int = 0) -> int:
    if not state or not confirm or not backup:
        say("REFUSED: --apply needs --state (from --plan on this file), --confirm and --backup")
        return ERR
    plan = read_state(state)
    if plan.get("kind") != "plan" or plan.get("db") != os.path.normcase(p):
        raise Refused("the state file is not a plan of this database")
    if plan.get("tool_sha") != tool_sha():
        raise Refused("this tool changed since --plan - run --plan again")
    head = _check_inputs()
    if plan.get("head") != head:
        raise Refused("HEAD moved since --plan - run --plan again")
    _work_path(backup, must_exist=False, what="--backup")
    td = None if no_vacuum else _check_tmpdir(tmpdir)
    if confirm != plan.get("token") or confirm != confirm_token(p, "apply"):
        say("REFUSED: --confirm does not match the plan or this file's identity and state - run --plan again")
        return ERR
    gate0 = load_gate()
    if not residue_control(gate0):
        raise Refused("the raw-scan control failed")
    lock = catalog_path.writer_lock() if live else contextlib.nullcontext()
    t_all = time.time()
    with lock:
        if live and catalog_path._held is None:                             # noqa: SLF001
            raise Refused("the writer lock is not held")
        if not no_vacuum:
            vacuum_preconditions(p)                    # fail before the delete, not after it
        # BEFORE: the selection, the rows' record images, and the raw scan that must SEE every one of them (the
        # control of the after-scans: an instrument that cannot find the rows cannot prove them gone)
        con = connect_ro(p)
        try:
            con.execute("BEGIN")
            gate, wide, _why, keep = gates(con)
            s0 = selection(con, gate, wide)
            sigs = row_signatures(con, p, s0["delete"])
            con.execute("COMMIT")
        finally:
            con.close()
        if set_hash(s0["delete"]) != plan.get("sel_hash") or set_hash(keep) != plan.get("keep_hash"):
            say("REFUSED: the selection or the keep set differs from the plan - run --plan again")
            return ERR
        t_r = time.time()
        res0 = residue_scan(p, gate, sigs=sigs)
        say(f"raw scan before the delete ({time.time() - t_r:.1f} s): measured {yn(res0['measured'])}; every "
            f"deleted row's record images are seen: {yn(res0['SIG_SEEN'])}; a member in free pages: "
            f"{yn(res0['FREE'])}; in unallocated b-tree space: {yn(res0['UNALLOC'])}")
        if not res0["measured"] or not res0["SIG_SEEN"]:
            raise Refused("the raw scan before the delete is not measured, or cannot see every row it must see")
        con = catalog_path.connect_path(p, write=True, timeout=30.0)
        con.isolation_level = None
        try:
            for pragma, want in (("journal_mode=DELETE", "delete"), ("locking_mode=NORMAL", "normal"),
                                 ("secure_delete=ON", 1)):
                r = con.execute(f"PRAGMA {pragma}").fetchone()
                if r is None or r[0] != want:
                    raise Refused(f"PRAGMA {pragma.split('=')[0]} did not read back as required")
            con.execute("PRAGMA temp_store=MEMORY")
            if con.execute("PRAGMA secure_delete").fetchone()[0] != 1 or \
                    con.execute("PRAGMA temp_store").fetchone()[0] != 2:
                raise Refused("secure_delete / temp_store do not read back 1 / MEMORY")
            t_tx = time.time()
            con.execute("BEGIN IMMEDIATE")
            ok = False
            try:
                if confirm != confirm_token(p, "apply"):
                    raise Refused("the file changed between --plan and BEGIN IMMEDIATE - run --plan again")
                s = selection(con, gate, wide)                # re-measured inside the transaction (L2)
                if set_hash(s["delete"]) != plan.get("sel_hash"):
                    raise Refused("the selection re-measured inside the transaction differs from the plan")
                if s["st"]["dangling"]:
                    raise Refused("a licence reference is dangling before the delete")
                li = s["st"]["cols"].index("license_id")
                rows = {r[li]: r for r in s["st"]["rows"]}
                older_miss = _older_copy_misses({k: rows[k] for k in s["del_mem"]}, older)
                write_backup(backup, wide, s["st"]["cols"], [rows[k] for k in sorted(s["del_clean"])])
                refd = s["st"]["exact"] & set(s["st"]["ids"])
                for lid in s["delete"]:
                    if lid in s["st"]["exact"] or lid.strip().lower() in s["st"]["folded"]:
                        raise Refused("a candidate is referenced")
                    if con.execute("DELETE FROM license WHERE license_id = ?", (lid,)).rowcount != 1:
                        raise Refused("a licence delete did not remove exactly one row")
                left = {r[0] for r in con.execute("SELECT license_id FROM license")}
                if left & s["delete"] or not refd <= left:
                    raise Refused("the licence table is not in the expected state after the deletes")
                con.execute("COMMIT")
                ok = True
            finally:
                if not ok:
                    with contextlib.suppress(Exception):
                        con.execute("ROLLBACK")
            say(f"delete transaction committed: PASS  ({time.time() - t_tx:.1f} s, BEGIN IMMEDIATE to COMMIT)")
        finally:
            con.close()
        journal_gone = not os.path.lexists(p + "-journal")
        t_r = time.time()
        res1 = residue_scan(p, gate, sigs=sigs)
        t_r1 = time.time() - t_r
        con = connect_ro(p)
        try:
            con.execute("BEGIN")
            s2 = selection(con, gate, wide, with_code=False)
            con.execute("COMMIT")
        finally:
            con.close()
        checks = [
            ("no -journal left after the delete COMMIT", journal_gone),
            ("no licence row is left that the plan selected", not (set(s2["st"]["ids"]) & s0["delete"])),
            ("the unreferenced rows left are exactly the code-named ones", s2["unref"] == s0["named"]),
            ("no dangling licence reference (every series/source license_id has a row)", not s2["st"]["dangling"]),
            ("every referenced licence row is still present", s["st"]["exact"] & set(s["st"]["ids"])
             <= set(s2["st"]["ids"])),
            (f"raw scan after the delete measured every page ({t_r1:.1f} s)", res1["measured"]),
            ("the deleted rows' record images are in NO live cell after the delete", not res1["SIG_LIVE"]),
        ]
        info = [f"the deleted rows' record images in free pages / unallocated space after the delete (VACUUM "
                f"clears): {yn(res1['SIG_DEAD'])}",
                f"a deleted member-carrying row is absent from the older F: copy (no F: copy holds it now): "
                f"{'UNMEASURED' if older_miss is None else yn(older_miss)}",
                f"unreferenced rows a tracked file names that carry a member (left for B34's code PR): "
                f"{yn(bool(s0['named_mem']))}"]
        if not no_vacuum:
            checks += vacuum_core(p, td, check, cache_mib)
            t_r = time.time()
            res2 = residue_scan(p, gate, sigs=sigs)
            checks += [(f"raw scan after VACUUM measured every page ({time.time() - t_r:.1f} s)", res2["measured"]),
                       ("the deleted rows' record images are found NOWHERE in the file after VACUUM",
                        not (res2["SIG_LIVE"] or res2["SIG_DEAD"])),
                       ("a deny-list member in free pages: NO", not res2["FREE"])]
            info.append(f"a deny-list member in unallocated b-tree space after VACUUM: {yn(res2['UNALLOC'])} "
                        "(stale copies of LIVE cells can sit there)")
        else:
            info.append("--no-vacuum: free pages and page slack remain until --vacuum runs (S4d step 3)")
    say(f"apply wall time: {time.time() - t_all:.1f} s")
    return _receipt("APPLY", checks, info=info, not_covered=NOT_COVERED)


# ---------------------------------------------------------------------------------------------------------- --sweep
def fts_maps(tables) -> dict:
    """<fts>_content table -> {cN: declared column name} for every fts5 virtual table with its own content table."""
    out = {}
    names = {t for t, _ in tables}
    for t, sql in tables:
        m = re.match(r"\s*CREATE\s+VIRTUAL\s+TABLE\s+\S+\s+USING\s+fts5\s*\((.*)\)\s*$", sql, re.I | re.S)
        if not m or f"{t}_content" not in names:
            continue
        args = [a.strip() for a in m.group(1).split(",")]
        cols = [a.split()[0].strip('"\'`[]') for a in args if a and "=" not in a]
        out[f"{t}_content"] = {f"c{i}": c for i, c in enumerate(cols)}
    return out


def classify_row(gate: Gate, carve: dict, cols, roles, row) -> tuple:
    """(hits {column: senses}, k2 gated-source row, k2c carve-out row). roles: column -> 'source'/'key'/None."""
    hits, k2, k2c = {}, False, False
    for c, v in zip(cols, row):
        if not isinstance(v, str):
            continue
        h = gate.cell(v)
        if h:
            hits[c] = h
        role = roles.get(c)
        if role == "source":
            bv = base(v).strip()
            if bv in gate.set or norm(bv) in gate.normset:               # re-spelled too (code review L2)
                k2 = True
        elif role == "key":
            m = key_member(gate, v)
            if m is not None:
                code = base(v).split(":", 1)[1] if ":" in v else ""
                if code in carve.get(m, set()):
                    k2c = True
                else:
                    k2 = True
    return hits, k2, k2c


def _scan_task(args) -> tuple:
    """One shard, in a worker process. Returns only booleans, sense names and row/cell counts - never a value. Every
    exception is caught HERE and returned as its type name (a RemoteTraceback would carry the message)."""
    p, table, cols, roles, lo, hi, members, carve, hosted, rid = args
    try:
        _lower_priority()
        gate, ctl = Gate(members), Gate(hosted)
        con = connect_ro(p)
        try:
            con.execute("BEGIN")
            sel = ", ".join(q(c) for c in cols)
            if rid is None:
                sql, par, csql = f"SELECT {sel} FROM {q(table)}", (), f"SELECT count(*) FROM {q(table)}"
            else:
                sql = f"SELECT {sel} FROM {q(table)} WHERE {rid} > ? AND {rid} <= ?"
                csql = f"SELECT count(*) FROM {q(table)} WHERE {rid} > ? AND {rid} <= ?"
                par = (lo, hi)
            expect = con.execute(csql, par).fetchone()[0]
            flags = {c: set() for c in cols}
            nrows, k2, k2c, nat = 0, False, False, False
            ntext, nblob = {c: 0 for c in cols}, {c: 0 for c in cols}
            for row in con.execute(sql, par):
                nrows += 1
                texts = []
                for c, v in zip(cols, row):
                    if isinstance(v, str):
                        if v:
                            ntext[c] += 1
                            texts.append(v)
                    elif isinstance(v, bytes):
                        nblob[c] += 1
                if not texts:
                    continue
                joined = "\x1f".join(texts)
                bj = base(joined)
                # no member's prefilter string in the row -> no sense and no K2 can fire
                if gate.maybe(bj) or ("%" in joined and gate.maybe(base(urllib.parse.unquote(joined)))):
                    hits, a, b = classify_row(gate, carve, cols, roles, row)
                    for c, h in hits.items():
                        flags[c] |= h
                    k2 |= a
                    k2c |= b
                if not nat and nrows <= CONTROL_SAMPLE and ctl.maybe(bj):
                    nat = any(ctl.cell(v) for v in texts)
            con.execute("COMMIT")
        finally:
            con.close()
        return ("OK", table, lo, hi, nrows, expect, {c: sorted(s) for c, s in flags.items()}, ntext, k2, k2c, nat,
                nblob)
    except Exception as e:  # noqa: BLE001
        return ("ERR", table, lo, hi, type(e).__name__)


def _planted_control(gate: Gate, carve: dict, cols, roles, row):
    """PASS/FAIL (None when the row has no text) - a member planted into an in-memory copy of a real row."""
    g = plant_member(gate)
    forms = [g, "zz " + g.upper() + " zz"]
    if g in gate.multi:
        forms += [_SEPS_RX.sub("-", g), _SEPS_RX.sub(" ", g).title(), "x/" + _SEPS_RX.sub("\u2013", g)]
    tcols = [c for c, v in zip(cols, row) if isinstance(v, str)]
    if not tcols:
        return None
    good = True
    for c in tcols:
        i = cols.index(c)
        for form in forms:
            r = list(row)
            r[i] = (row[i] or "") + " " + form + " "
            hits, _a, _b = classify_row(gate, carve, cols, roles, r)
            good &= bool(hits.get(c))
    neg = ["~~~~" if isinstance(v, str) else v for v in row]
    hits, a, b = classify_row(gate, carve, cols, roles, neg)
    return good and not hits and not a and not b


def classify_licences(gate: Gate, st: dict, rows) -> tuple:
    """(referenced by a source row and carrying, referenced by series rows only and carrying, columns that carry)."""
    li = st["cols"].index("license_id")
    src = set()
    for (t, _c), vals in st["by_table"].items():
        if t == "source":
            src |= vals
    src_f = {x.strip().lower() for x in src}
    unref = set(st["unref"])
    k3s = k3ser = False
    cols_hit = set()
    for r in rows:
        hit_cols = {c for c, v in zip(st["cols"], r) if isinstance(v, str) and gate.cell(v)}
        if not hit_cols:
            continue
        cols_hit |= hit_cols
        lid = r[li]
        if lid in unref:
            continue
        if lid in src or lid.strip().lower() in src_f:
            k3s = True
        else:
            k3ser = True
    return k3s, k3ser, cols_hit


def licence_control(gate: Gate, wide: Gate, st: dict) -> bool:
    """An in-memory copy of a real licence row, its id changed to one nothing references and a member planted in its
    name, must classify as an unreferenced member row; the same row under a source-referenced id as v6; a row of
    '~' cells as nothing. Nothing is written."""
    if not st["rows"]:
        return False
    g = plant_member(gate)
    li, cols = st["cols"].index("license_id"), st["cols"]
    real = list(st["rows"][0])
    ni = cols.index("name") if "name" in cols else li
    plant = list(real)
    plant[li] = "~b32-planted-" + hashlib.sha256(repr(real).encode()).hexdigest()[:8]
    plant[ni] = (real[ni] if isinstance(real[ni], str) else "") + " " + _SEPS_RX.sub("-", g)
    if plant[li] in st["exact"]:
        return False
    unref_hit = any(wide.cell(v) for v in plant if isinstance(v, str))
    src = sorted(v for (t, _c2), vals in st["by_table"].items() if t == "source" for v in vals)
    if not src:
        return False
    shared = list(plant)
    shared[li] = src[0]
    k3s, _r, _c = classify_licences(gate, st, [shared])
    neg = ["~~~~" if isinstance(v, str) else v for v in real]
    return unref_hit and k3s and not any(wide.cell(v) for v in neg if isinstance(v, str))


def _has_table_list(con) -> bool:
    try:
        con.execute("SELECT 1 FROM pragma_table_list LIMIT 1").fetchone()
        return True
    except Exception:  # noqa: BLE001
        return False


def do_sweep(p: str, workers: int, compare_state: str = "", allow_free: bool = False,
             licence_only: bool = False) -> int:
    t0 = time.time()
    _lower_priority()
    _check_inputs()
    gate, carve = load_gate(), load_carveouts()
    if not matcher_self_test(gate):
        say("SWEEP FAIL: the matcher self-test failed - no answer is trustworthy")
        return ERR
    before = file_state(p)
    con = connect_ro(p)
    fail, held, unmeasured = [], [], []
    try:
        con.execute("BEGIN")
        tables = schema(con)
        gate, wide, _why, _keep = gates(con)
        st = licence_state(con)
        sl = selection(con, gate, wide, st=st)
        src_rows = con.execute("SELECT source_id FROM source").fetchall()
        hosted = sorted({base(r[0]) for r in src_rows if isinstance(r[0], str)} - gate.set)
        fl = con.execute("PRAGMA freelist_count").fetchone()[0]
        k3s, k3ser, lic_cols_hit = classify_licences(gate, st, st["rows"])
        k1a, k1n = bool(sl["delete"]), bool(sl["named_mem"])
        held_hash = set_hash(sl["held"])
        lic_ctl = licence_control(gate, wide, st)
        nat_lic = any(Gate(hosted).cell(v) for r in st["rows"] for v in r if isinstance(v, str)) if hosted else False
        fmap = fts_maps(tables)
        tasks, small, meta = [], [], {}
        scan_tables = [(t, s) for t, s in tables if t != "license" and (not licence_only or t == "source")]
        for t, sql in scan_tables:
            xinfo = con.execute(f"PRAGMA table_xinfo({q(t)})").fetchall()
            cols = [str(c[1]) for c in xinfo if not (len(c) > 6 and c[6] in (2, 3))]   # skip generated
            is_virtual = sql.upper().lstrip().startswith("CREATE VIRTUAL")
            if is_virtual and f"{t}_content" in fmap:
                meta[t] = {"virtual": True, "cols": cols}
                continue
            roles = {}
            names = fmap.get(t, {})
            for c in cols:
                logical = names.get(c, c).lower()
                if logical == "source_id":
                    roles[c] = "source"
                elif logical == "series_id":
                    roles[c] = "key"
            sample = con.execute(f"SELECT {', '.join(q(c) for c in cols)} FROM {q(t)} LIMIT 200").fetchall()
            first = max(sample, key=lambda r: sum(isinstance(v, str) for v in r), default=None)
            if first is not None and not any(isinstance(v, str) for v in first):
                first = None
            meta[t] = {"cols": cols, "roles": roles, "first": first}
            wr = con.execute("SELECT 1 FROM pragma_table_list WHERE name=? AND wr=1", (t,)).fetchone() \
                if _has_table_list(con) else None
            rid = None if (wr or is_virtual) else "rowid"
            n = con.execute(f"SELECT count(*) FROM {q(t)}").fetchone()[0] if rid else None
            if rid and n > SHARD_ROWS:
                lo, hi = con.execute(f"SELECT min(rowid), max(rowid) FROM {q(t)}").fetchone()
                k = max(1, min(workers * 2, (n // SHARD_ROWS) + 1))
                step = (hi - lo + k) // k
                edges = [lo - 1 + i * step for i in range(k)] + [hi]
                for a, b in zip(edges, edges[1:]):
                    tasks.append((p, t, cols, roles, a, b, gate.ms, carve, hosted, rid))
                meta[t]["n"] = n
                meta[t]["range"] = (lo - 1, hi)
            else:
                small.append((p, t, cols, roles, None, None, gate.ms, carve, hosted, None))
        sm_hit = any(gate.cell(v) for r in con.execute("SELECT name, tbl_name, sql FROM sqlite_master")
                     for v in r if isinstance(v, str))
        con.execute("COMMIT")
    finally:
        con.close()
    jobs = small + tasks
    if workers > 1 and len(jobs) > 1:
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(processes=min(workers, len(jobs))) as pool:
            results = list(pool.imap_unordered(_scan_task, jobs))
    else:
        results = [_scan_task(j) for j in jobs]
    after = file_state(p)
    per = {}
    for r in results:
        if r[0] != "OK":
            per.setdefault(r[1], {"err": []})["err"].append(r[4])
            continue
        _ok, t, lo, hi, nrows, expect, flags, ntext, k2, k2c, nat, nblob = r
        d = per.setdefault(t, {"err": []})
        d.setdefault("ranges", []).append((lo, hi))
        d["rows"] = d.get("rows", 0) + nrows
        d["short"] = d.get("short", False) or nrows != expect
        for key, src in (("ntext", ntext), ("nblob", nblob)):
            d.setdefault(key, {})
            for c, n in src.items():
                d[key][c] = d[key].get(c, 0) + n
        d.setdefault("flags", {})
        for c, s in flags.items():
            d["flags"].setdefault(c, set()).update(s)
        d["k2"] = d.get("k2", False) or k2
        d["k2c"] = d.get("k2c", False) or k2c
        d["nat"] = d.get("nat", False) or nat
    say(f"tables in sqlite_master: {len(tables)}; scanned here: {len(scan_tables) + 1}"
        + ("  (--licence-only: license, source, sqlite_master)" if licence_only else ""))
    say(f"sqlite_master text carries a member: {yn(sm_hit)}")
    say("-- license (row count withheld: a before/after difference would be a count of rows removed)")
    say(f"   K1A FAIL-class  an unreferenced licence row that no tracked file names: {yn(k1a)}"
        + ("  (--apply deletes them)" if k1a else ""))
    say(f"   K1N HELD        an unreferenced row a tracked file names that carries a member: {yn(k1n)}"
        + ("  <- B34's code PR first (R2263)" if k1n else ""))
    say(f"   K3  HELD (v6)   referenced by a source row and carrying a member:  {yn(k3s)}")
    say(f"   K3  HELD (v6)   referenced by series rows only, carrying a member: {yn(k3ser)}")
    say(f"   K6  FAIL-class  a license_id reference names no licence row (dangling): {yn(st['dangling'])}")
    say(f"   columns that carry a member: {', '.join(sorted(lic_cols_hit)) or '-'}")
    say(f"   planted control (in-memory licence rows through the same classifier): {'PASS' if lic_ctl else 'FAIL'}")
    say(f"   natural control (a hosted source id fires in the licence table): {yn(nat_lic)}")
    if compare_state:
        ref = read_state(compare_state)
        say(f"   v6 keys equal the reference state's (REPORTED, not a FAIL - owner item 40): "
            f"{yn(ref.get('held_hash') == held_hash)}")
    if k1a:
        fail.append("K1A")
    if st["dangling"]:
        fail.append("K6")
    if k3s or k3ser or k1n:
        held.append("K3/K1N")
    if not lic_ctl:
        fail.append("license control")
    k2_any = False
    for t, _sql in scan_tables:
        m = meta.get(t, {})
        if m.get("virtual"):
            say(f"-- {t}: fts5 virtual table - its text is scanned through {t}_content")
            continue
        d = per.get(t, {"err": []})
        if d["err"] or not d.get("ranges"):
            say(f"-- {t}: INCOMPLETE - a shard failed or none ran"
                + (f" ({','.join(sorted(set(d['err'])))})" if d["err"] else ""))
            fail.append(f"{t} incomplete")
            continue
        tc = [c for c in m.get("cols", []) if d.get("ntext", {}).get(c)]
        blobs = [c for c in m.get("cols", []) if d.get("nblob", {}).get(c)]
        if not tc:
            say(f"-- {t}: UNMEASURED (no text cells{'; BLOB columns' if blobs else ''})")
            unmeasured.append(t)
            continue
        complete = not d.get("short", True)
        if "range" in m:
            rs = sorted(d.get("ranges", []))
            lo, hi = m["range"]
            complete &= bool(rs) and rs[0][0] == lo and rs[-1][1] == hi and all(
                a[1] == b[0] for a, b in zip(rs, rs[1:])) and d.get("rows") == m["n"]
        ctl = _planted_control(gate, carve, m["cols"], m["roles"], m["first"]) if m.get("first") else None
        say(f"-- {t}: {d.get('rows', 0):,} rows scanned, complete: {yn(complete)}")
        if not complete:
            fail.append(f"{t} incomplete")
            say(f"   {t}: INCOMPLETE - no NO may be read from this block")
            continue
        anyhit = set().union(*d["flags"].values()) if d.get("flags") else set()
        say(f"   K4  HELD (v7)   mentions of a member: {yn(bool(anyhit))}"
            + ("   by sense: " + " ".join(s for s in SENSES if s in anyhit) if anyhit else ""))
        for c in tc:
            if d["flags"].get(c):
                say(f"       column {c}: {','.join(s for s in SENSES if s in d['flags'][c])}")
        empty = [c for c in m.get("cols", []) if c not in tc and c not in blobs]
        if empty:
            say(f"   columns with no text cell (not read for text): {', '.join(empty)}")
        if blobs:
            say(f"   BLOB columns not read (UNMEASURED): {', '.join(blobs)}")
        if any(r for r in m["roles"].values()):
            say(f"   K2  FAIL-class  rows of a deny-listed source:            {yn(d.get('k2'))}")
            say(f"   K2C HELD        keys under a member that are carve-outs: {yn(d.get('k2c'))}")
            k2_any |= bool(d.get("k2"))
        say(f"   planted control (in-memory copy of a real row): "
            f"{'PASS' if ctl else ('FAIL' if ctl is False else 'UNMEASURED')}")
        say(f"   natural control (a hosted source id fires on a sample): {yn(d.get('nat'))}"
            + ("" if d.get("nat") else "  <- the planted control alone backs this table's answers"))
        if anyhit:
            held.append(f"K4 {t}")
        if ctl is False:
            fail.append(f"{t} control")
    if k2_any:
        fail.append("K2")
    say(f"-- K5  {'INFO      ' if allow_free else 'FAIL-class'}  free pages present (freelist_count > 0): "
        f"{yn(fl > 0)}" + ("  (--allow-free-pages: S10b; run --residue for their content)" if allow_free else ""))
    if fl > 0 and not allow_free:
        fail.append("K5")
    same = before == after
    say(f"-- the file was unchanged during the sweep (identity + change counter): {yn(same)}")
    if not same:
        fail.append("file changed")
    say(f"tables UNMEASURED: {len(unmeasured)}" + (f" ({', '.join(unmeasured)})" if unmeasured else ""))
    say(f"HELD classes present (not a FAIL: v6, v7, K1N): {yn(bool(held))}")
    for text in NOT_COVERED:
        say(f"NOT COVERED  {text}")
    say(f"sweep wall time: {time.time() - t0:.1f} s")
    hard = [x for x in fail if x.endswith(("incomplete", "control")) or x == "file changed"]
    say(f"SWEEP {'PASS' if not fail else 'FAIL'}" + (f"  ({', '.join(fail)})" if fail else ""))
    if hard:
        return ERR
    return OK if not fail else FAIL


# ------------------------------------------------------------------------------------------------------------- main
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    m = ap.add_mutually_exclusive_group(required=True)
    for mode in ("--plan", "--apply", "--vacuum", "--sweep", "--residue"):
        m.add_argument(mode, action="store_true")
    ap.add_argument("--db", required=True)
    ap.add_argument("--live", action="store_true", help="the build itself, at the named steps S4d / S10b")
    ap.add_argument("--state", default="", help="--plan writes / --apply reads the sealed state file")
    ap.add_argument("--confirm", default="", help="--apply: the token --plan printed")
    ap.add_argument("--backup", default="", help="--apply: a NEW sealed file for the deleted no-member rows")
    ap.add_argument("--older-copy", default=OLDER_COPY, help="--apply: the older F: copy for the INFO line")
    ap.add_argument("--no-vacuum", action="store_true", help="--apply: leave the VACUUM to a later --vacuum run")
    ap.add_argument("--tmpdir", default="", help="--apply / --vacuum: an empty folder under PROOF_ROOTS")
    ap.add_argument("--check", choices=("quick", "integrity"), default="quick")
    ap.add_argument("--cache-mib", type=int, default=0, help="VACUUM: page cache in MiB (0 = SQLite default)")
    ap.add_argument("--compare-state", default="", help="--sweep: a plan state of the proof copy (v6 report)")
    ap.add_argument("--allow-free-pages", action="store_true", help="--sweep: free pages are INFO (S10b)")
    ap.add_argument("--licence-only", action="store_true", help="--sweep: license, source and sqlite_master only")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args(argv)
    mode = next(x for x in ("plan", "apply", "vacuum", "sweep", "residue") if getattr(a, x))
    try:
        p = resolve_target(a.db, a.live)
        say(f"B32 r888_licence_sweep  tool blob {tool_sha()[:12]}  mode {mode}{'  LIVE' if a.live else ''}  start "
            f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
        if mode == "plan":
            return do_plan(p, a.state)
        if mode == "apply":
            return do_apply(p, a.live, a.state, a.confirm, a.tmpdir, a.check, a.backup, a.older_copy, a.no_vacuum,
                            a.cache_mib)
        if mode == "vacuum":
            return do_vacuum(p, a.live, a.tmpdir, a.check, a.cache_mib)
        if mode == "sweep":
            return do_sweep(p, max(1, a.workers), a.compare_state, a.allow_free_pages, a.licence_only)
        return do_residue(p)
    except Refused as e:
        say(f"REFUSED: {e}")
        return ERR
    except Exception as e:  # noqa: BLE001 - L7: the type only, never a message that could carry a value
        say(f"ERROR {type(e).__name__} (message withheld, R888)")
        return ERR


if __name__ == "__main__":
    sys.exit(main())
