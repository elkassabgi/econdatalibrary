"""THE CATALOGUE RECONCILE TOOL of the econ self-hosting move (T0 plan row B24, rules "(b2) B24's rules",
rulings (k) and (l), the coordinator's rulings of 2026-11-28 and the design review R2249; PLAN.md step 6b;
design F:/work/econ_finish/b24_reconcile/DESIGN.md).

The D1 catalogue sync has been frozen since 2026-08-31/09-01 (ledger R542), so the local catalogue and the two
econ D1 databases (econ-catalog, econ-catalog-climate) have drifted. This tool measures the drift cell by cell,
decides each difference by the class rules below, writes the 6c summary for the owner (item 36), and at S11
applies a PINNED, approved plan to the live build under the writer lock, then rebuilds series_fts.

STEPS (each one a subcommand; every output goes under --out, on F:):
  copy          make the dry run's NEW plain copy of F:/work/owed_pay/20261116/catalog_copy.db (a full byte
                copy, the source hashed before and after, link count 1) - never the linked file itself
  probe         read D1's schema, source_counts and ONE series page per database; print the cost formula with
                the measured constants (the one-SELECT-first rule, R492)
  read-d1       the paged snapshot of both D1 databases (keyset pages by primary key, budgeted, resumable)
  classify      offline: local catalogue + snapshots + the reviewed lists -> cells, rows, summary, plan
  dry-run       read-d1 then classify
  rehearse      BEFORE T0 ONLY, on the plain copy: apply the plan, time the FTS rebuild (both forms), verify
  search        the 50-query search comparison: local index vs D1, and optionally vs an origin copy
  apply         S11 ONLY, after the CUTOVER flag, under the writer lock: apply EXACTLY the pinned plan
                (--plan + --plan-sha), rebuild series_fts, verify V1-V5 (V6 = search, V7 = the C6 count)
  c7-candidates list the booking lines (FINISH_PLAN.md, NUMBERS.md) that may name a catalogue writer run

THE ALLOW-LIST (coordinator ruling on H1/H2; AR-264 / R1492 rule 2): the tool may handle only the sources the
LOCAL catalogue hosts (its series and source rows) plus the named C3 writer sources, minus the worker gate
(core.gen_denylist.committed_gate). Every table is gated: a series row by its id prefix AND its source_id, a
source row by its id, a licence row only when an allowed series/source row references it and no gated id appears
in it. Anything else is dropped ON RECEIPT, before validation or storage, and reported only as a YES/NO line -
never a name, a key or a count (R888). Messages print a key's sha256 prefix, never the key.

CLASSES (b2), first match wins:
  cells: LIC-HELD (any licence / redistributability cell: never applied in either direction, listed with its
         direction and its DATABASE_LICENSES_VERBATIM.md line; R490 rule 3, R86/R95) > ROUNDTRIP (a number the
         D1 answer cannot carry exactly: listed, local kept) > C7 (keep local) > C5 (a column D1 never carried)
         > C2 (run_location: local) > C6 (sec_edgar: verify only) > C1 (a reviewed CI-only column: D1 wins; on a
         C7-gap source listed, not applied) > LOCAL-NULL (local NULL, D1 value: not taken) > DEFAULT (local wins)
  rows:  C3 (only in D1: not taken, unless the source is a named D1-direct writer -> C3-TAKEN), C4 (only local:
         kept, unless on item 17's approved delete list -> C4-DEL); an approved id on BOTH sides is a FAILED check
  V7 (the sec_edgar level proof) counts EVERY differing sec_edgar cell and every sec_edgar C3 row BEFORE classes,
  except Road B's own null_over_local (id, column) pairs.

COST (hfdatalibrary CLAUDE.md "COST", R492, R982): every D1 statement is one plain SELECT (checked with
core.d1_remote.read_statement_problem BEFORE it is sent, before T0 too), its meta.rows_read is charged to a
--budget-rows that has no default and a hard ceiling, is CUMULATIVE across resumes, and is checked before each
statement. Nothing is ever written to D1. series is paged by primary key; series_fts is never filtered by id.

THE CATALOGUE is opened only through core.catalog_path. Before T0 every input and --out must be on the F: volume
and nothing under the E: repository is accepted (R1940 in code; the dry run runs from an F: clone). The FTS
rebuild repeats the core catalogue module's rebuild_fts statements (pinned by tests/test_reconcile_catalogue.py);
that module is not imported, because its importers must refuse after T0 (tests/test_catalogue_writers_main.py
LIBRARY_WRITERS) and this tool runs at S11.
"""
from __future__ import annotations

import argparse
import datetime as dt
import decimal
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import sqlite3
import sys
import threading
import time
import traceback

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from core import catalog_path  # noqa: E402 - the one catalogue resolver
from core import cutover  # noqa: E402

LISTS_DIR = os.path.join(ROOT, "tools", "selfhost", "reconcile")
AUDIT_FILE = os.path.join(ROOT, "DATABASE_LICENSES_VERBATIM.md")
PRIMARY = "econ-catalog"
CLIMATE = "econ-catalog-climate"
DATABASES = (PRIMARY, CLIMATE)
TABLES = ("license", "source", "series")                # the reconciled tables, in classification order
PK = {"license": "license_id", "source": "source_id", "series": "series_id"}
JSON_COLUMNS = frozenset({("series", "metadata")})
LICENCE_FLAGS = ("reservable", "commercial_ok", "attribution_required", "no_modify")
PERMISSION_FLAGS = frozenset({"reservable", "commercial_ok"})
LICENCE_REFS = frozenset({("series", "license_id"), ("source", "license_id")})
C6_SOURCE = "sec_edgar"
# Ruling (k) + coordinator 2026-11-28 (Q3): the first FAILING sync (.github/workflows/updater-daily.yml:431,
# "Every catalogue sync since that commit has FATAL'd"; R542). The per-workflow freeze dates were 2026-08-31
# (updater-daily.yml:422) and 2026-09-01 (updater-heavy.yml:445); the earlier date only widens C7.
C7_CUTOFF = "2026-08-24T00:00:00Z"
MAX_BUDGET_ROWS = 30_000_000
DEFAULT_PAGE_ROWS = 2_000                               # safe default; the probe measures the answer size
MAX_PAGE_ROWS = 10_000
MIN_PAGE_ROWS = 50
ANSWER_BYTES_TARGET = 4_000_000
LOCAL_PAGE_ROWS = 50_000
WRITE_CHUNK = 400
SEARCH_LOCAL_MAX = 2_000                                # a query with more local matches is refused (cost)
SMALL_EXPECTED = 5_000                                  # the budget check's guess for one small-table statement
JS_SAFE_INT = 2 ** 53                                   # the D1 answer is JSON from a JS runtime (review M3)
# PRAGMA synchronous for the tool's OWN files (snapshots, budget ledger, result file). NORMAL survives a killed
# process (each page transaction is atomic); tests set OFF to spare the disk's fsyncs.
OWN_SYNC = "NORMAL"
# Outputs and inputs: the F: volume before T0 (tests point these at their tmp folder).
OUT_ROOTS = ("F:\\",)
F_ANCHOR = "F:\\work"
# The only file the tool may read under F:\econ_live (coordinator 2026-11-28, Q6), and only mode=ro&immutable=1.
ECON_LIVE = r"F:\econ_live"
SERVED_INDEX = r"F:\econ_live\blobs\index.db"
ENV_FILE = os.path.join(catalog_path.LIVE_STORE_ROOT, ".env")
# D1 tables the tool expects; any other name is FLAGGED (Q9). A flagged name is printed only when no gated id
# appears in it; otherwise only "YES".
KNOWN_D1_TABLES = frozenset({
    "series", "source", "license", "series_fts", "series_fts_config", "series_fts_content", "series_fts_data",
    "series_fts_docsize", "series_fts_idx", "source_counts", "unit_state", "source_state", "source_data_through",
    "sync_receipt", "econ_pageview", "econ_pageview_import", "econ_ops_status", "econ_download_log", "pageview",
    "_cf_KV", "d1_migrations", "sqlite_sequence", "sqlite_stat1", "series_fts_new"})

FTS_DELETE = "DELETE FROM series_fts;"
FTS_INSERT = "INSERT INTO series_fts(series_id,title,geography) SELECT series_id,title,geography FROM series;"
FTS_FORMS = ("rebuild_fts", "drop_create")
PAGE_SQL_FIRST = "SELECT * FROM series WHERE series_id >= ? ORDER BY series_id LIMIT ?"
PAGE_SQL = "SELECT * FROM series WHERE series_id > ? ORDER BY series_id LIMIT ?"
MASTER_SQL = "SELECT type, name, tbl_name, sql FROM sqlite_master"
COUNTS_SQL = "SELECT source_id, n FROM source_counts"
SEARCH_SQL = ("SELECT f.series_id AS series_id, f.title AS fts_title, f.geography AS fts_geo, s.title AS series_title,"
              " s.geography AS series_geo FROM series_fts f JOIN series s ON s.series_id = f.series_id"
              " WHERE series_fts MATCH ?")

WITHHELD = b"\x00withheld-by-the-allow-list\x00"           # stands in, in a snapshot, for a value naming a gated id
RAW, NORM, DIFF = "raw", "norm", "diff"
LOCAL, TAKE_D1, VERIFY, HELD = "local", "d1", "verify", "held"
CELL_CLASSES = ("LIC-HELD", "ROUNDTRIP", "C7", "C5", "C2", "C6", "C1", "C1-GAP", "LOCAL-NULL", "DEFAULT")
ROW_CLASSES = ("C3", "C3-TAKEN", "C4", "C4-DEL", "C4-DEL-BOTH")

# The D1 road. None = core.d1_remote.query (REST: before T0 CLOUDFLARE_API_TOKEN, after T0 D1_READ_TOKEN; never
# wrangler - coordinator 2026-11-28, Q12). Tests set a fake with the same signature and answer shape.
D1_QUERY = None


class Refused(Exception):
    """A precondition does not hold; nothing was done (exit 2)."""


class Stop(Exception):
    """A measurement or budget stop; what was read so far is kept and the run can resume (exit 3)."""


class Failed(Exception):
    """A check failed (exit 1)."""


# ---- small helpers -----------------------------------------------------------------------------------------

def utcnow() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def h(key) -> str:
    """What a message prints instead of a key: its sha256 prefix (a key may be a gated id, R888)."""
    return "key#" + hashlib.sha256(repr(key).encode("utf-8", "replace")).hexdigest()[:12]


def file_sha256(path: str, block: int = 64 << 20) -> str:
    hh = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            b = fh.read(block)
            if not b:
                return hh.hexdigest()
            hh.update(b)


def _jdefault(v):
    if isinstance(v, (bytes, bytearray)):
        return {"$blob": bytes(v).hex()}
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (set, frozenset)):
        return sorted(v)
    raise TypeError(type(v).__name__)


def _unj(v):
    if isinstance(v, dict) and set(v) == {"$blob"}:
        return bytes.fromhex(v["$blob"])
    return v


def write_json(path: str, obj) -> None:
    tmp = path + ".partial"
    with open(tmp, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(obj, fh, indent=1, ensure_ascii=False, default=_jdefault)
        fh.write("\n")
    os.replace(tmp, path)


def ro_uri(path: str, immutable: bool = False) -> str:
    """A read-only SQLite URI for one of the tool's own files (or an F: copy)."""
    return pathlib.Path(os.path.abspath(path)).as_uri() + ("?mode=ro&immutable=1" if immutable else "?mode=ro")


def say(msg: str) -> None:
    enc = sys.stdout.encoding or "utf-8"
    sys.stdout.write(msg.encode(enc, "replace").decode(enc, "replace") + "\n")
    sys.stdout.flush()


def _text_or_bytes(b: bytes):
    """The local catalogue's text: str when it is valid UTF-8, else the raw bytes (reported, never a crash)."""
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return bytes(b)


# ---- path guards (review M7: string tests for E:, a volume test for F:, never a stat of E:'s data/) --------

def _abs(p) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(p)))


def _real(p) -> str:
    return os.path.normcase(os.path.realpath(os.fspath(p)))


def _is_under(p, root) -> bool:
    rp, rr = _real(p), _abs(root).rstrip("\\/")
    return rp == rr or rp.startswith(rr + os.sep)


def _under_e_repo(p) -> bool:
    """String tests only: the E: repository is never stat-ed or resolved by this check."""
    root = _abs(catalog_path.LIVE_STORE_ROOT).rstrip("\\/")
    q = _abs(p)
    if q == root or q.startswith(root + os.sep):
        return True                                          # the spelled path: never resolved, never stat-ed
    if os.path.splitdrive(q)[0] == os.path.splitdrive(root)[0]:
        return False                                         # another E: path: not resolved either (no E: access);
                                                             # it then fails the F: volume test
    q = _real(p)                                             # an F: path: resolved, so a junction to E: is seen
    return q == root or q.startswith(root + os.sep)


def _volume(p) -> int:
    q = os.path.abspath(os.fspath(p))
    while not os.path.exists(q):
        parent = os.path.dirname(q)
        if parent == q:
            break
        q = parent
    return os.stat(q).st_dev


def _on_f(p) -> bool:
    if os.path.splitdrive(os.path.abspath(os.fspath(p)))[0].lower() != \
            os.path.splitdrive(os.path.abspath(F_ANCHOR))[0].lower():
        return False                                              # another drive: not stat-ed at all
    try:
        return _volume(p) == os.stat(F_ANCHOR).st_dev
    except OSError:
        return False                                              # cannot tell = not on F: (fail closed)


def guard_input(path, what: str, *, own_list: bool = False):
    """Every input except the catalogue. Before T0: on the F: volume and not under the E: repository. After T0
    the same, except the tool's own reviewed lists in its checkout (own_list). Never under F:\\econ_live except
    the served index, which only --served-index may name."""
    if path is None:
        return None
    if own_list and _is_under(path, LISTS_DIR) and (cutover.is_cut_over() or (not _under_e_repo(path) and _on_f(path))):
        return path
    if _under_e_repo(path):
        raise Refused(f"refused: {what} is under the E: repository (R1940: inputs are F: copies)")
    if not _on_f(path):
        raise Refused(f"refused: {what} is not on the F: volume")
    if _is_under(path, ECON_LIVE) and not (what == "--served-index" and _real(path) == _real(SERVED_INDEX)):
        raise Refused(f"refused: {what} is under {ECON_LIVE}; the tool reads only {SERVED_INDEX} there")
    return path


def guard_catalogue(path, *, for_apply: bool = False) -> str:
    """Before T0 the catalogue is an F: copy (never anything under the E: repository); after T0 it is the build,
    and only the build (core.catalog_path.connect_path refuses anything else)."""
    if cutover.is_cut_over():
        build = catalog_path.catalog_path()
        if path is None:
            return build
        if _abs(path) != _abs(build):
            raise Refused("refused: after T0 the catalogue is the build, not another file")
        return build
    if for_apply:
        raise Refused("refused: apply runs only at S11, after the CUTOVER flag (it writes the live build)")
    if path is None:
        raise Refused("refused: --catalogue is required before T0 (the dry run reads an F: copy; no default)")
    guard_input(path, "--catalogue")
    return path


def guard_out(out) -> str:
    if not out:
        raise Refused("refused: --out is required")
    if not any(_is_under(out, r) for r in OUT_ROOTS) or _under_e_repo(out) or _is_under(out, ECON_LIVE):
        raise Refused(f"refused: --out {out} must be on {OUT_ROOTS} and not under the live store")
    if not cutover.is_cut_over() and not _on_f(out):
        raise Refused(f"refused: --out {out} is not on the F: volume")
    os.makedirs(out, exist_ok=True)
    return out


def refuse_e_checkout() -> None:
    """Before T0 the tool runs from an F: clone: its own lists and code must not come from the E: repository."""
    if not cutover.is_cut_over() and _under_e_repo(ROOT):
        raise Refused("refused: before T0 run this tool from an F: clone, not from the E: repository (R1940)")


def plain_copy_problem(path) -> str | None:
    """Why `path` is not a writable PLAIN copy (rehearse / copy targets), or None."""
    if _under_e_repo(path):
        return "it is under the E: repository"
    if not _on_f(path):
        return "it is not on the F: volume"
    if _is_under(path, ECON_LIVE):
        return f"it is under {ECON_LIVE}"
    if os.path.exists(path):
        st = os.stat(path)
        if st.st_nlink != 1:
            return f"it has {st.st_nlink} hard links (a linked copy is never written)"
    return None


# ---- normalisation (ruling k; review M3) ---------------------------------------------------------------------

_NUM_RX = re.compile(r"[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?\Z")
_CAST_CON = None


def affinity(decl: str | None) -> str:
    """SQLite's column affinity rules for a declared type."""
    d = (decl or "").upper()
    if "INT" in d:
        return "INTEGER"
    if any(x in d for x in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    if not d or "BLOB" in d:
        return "BLOB"
    if any(x in d for x in ("REAL", "FLOA", "DOUB")):
        return "REAL"
    return "NUMERIC"


def column_kind(table: str, col: str, local_decl: str | None, d1_decl: str | None) -> str:
    if (table, col) in JSON_COLUMNS:
        return "json"
    affs = {affinity(x) for x in (local_decl, d1_decl) if x is not None}
    if affs & {"INTEGER", "REAL", "NUMERIC"}:
        return "numeric"
    return "text"


def sqlite_text(v):
    """A value as SQLite renders it as TEXT (CAST(v AS TEXT)) - used only for a TYPELESS D1 column."""
    global _CAST_CON
    if v is None or isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return ("blob", bytes(v))
    if _CAST_CON is None:
        _CAST_CON = sqlite3.connect(":memory:", check_same_thread=False)  # plain-open: in-memory CAST only
    return _CAST_CON.execute("SELECT CAST(? AS TEXT)", (v,)).fetchone()[0]


def num_canon(v):
    if isinstance(v, bool):
        return ("bool", v)
    if isinstance(v, int):
        return ("num", decimal.Decimal(v))
    if isinstance(v, decimal.Decimal):
        return ("num", v) if v.is_finite() else ("text", str(v))
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ("text", repr(v))
        return ("num", decimal.Decimal(repr(v)))
    if isinstance(v, str) and _NUM_RX.match(v):
        return ("num", decimal.Decimal(v))
    if isinstance(v, (bytes, bytearray)):
        return ("blob", bytes(v))
    return ("text", v if isinstance(v, str) else str(v))


def unsafe_number(v) -> bool:
    """A number the D1 JSON answer cannot carry exactly (an integer beyond 2^53)."""
    return isinstance(v, int) and not isinstance(v, bool) and abs(v) > JS_SAFE_INT


class _NotJson(Exception):
    pass


def _pairs(pairs):
    d = {}
    for k, v in pairs:
        if k in d:
            raise _NotJson("duplicate key")
        d[k] = v
    return d


def _no_const(c):
    raise _NotJson(c)


def json_parse(s: str):
    try:
        return True, json.loads(s, object_pairs_hook=_pairs, parse_constant=_no_const,
                                parse_float=decimal.Decimal, parse_int=decimal.Decimal)
    except (ValueError, _NotJson, RecursionError, decimal.InvalidOperation):
        return False, None


def json_eq(a, b) -> bool:
    """STRICT parsed equality: key order ignored, numbers by value, true/false are not numbers (Decimal(1) == True
    is True in Python, so booleans are tested first)."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float, decimal.Decimal)) and isinstance(b, (int, float, decimal.Decimal)):
        return num_canon(a) == num_canon(b)
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(json_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(json_eq(a[k], b[k]) for k in a)
    return False


def raw_equal(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return type(a) is type(b) and a == b


def compare(kind: str, a, b) -> str:
    """RAW (same type and value), NORM (equal after the kind's normalisation), or DIFF. NULL equals only NULL."""
    if a is None or b is None:
        return RAW if (a is None and b is None) else DIFF
    if raw_equal(a, b):
        return RAW
    if kind == "numeric":
        return NORM if num_canon(a) == num_canon(b) else DIFF
    if kind == "json":
        if isinstance(a, str) and isinstance(b, str):
            oka, pa = json_parse(a)
            okb, pb = json_parse(b)
            if oka and okb:
                return NORM if json_eq(pa, pb) else DIFF
            return DIFF                                  # a side that does not parse: raw text, already unequal
        return NORM if sqlite_text(a) == sqlite_text(b) else DIFF
    return NORM if sqlite_text(a) == sqlite_text(b) else DIFF


def _permissive(v) -> bool | None:
    """A permission (or obligation) flag is ON only when it is exactly the number 1; None = not a number."""
    if v is None:
        return False
    c = num_canon(v)
    if c[0] != "num":
        return None
    return c[1] == 1


def licence_flag_direction(col: str, lv, dv) -> str:
    """Which side is tighter for one licence flag (a LABEL for the owner; the cell is HELD either way)."""
    pl, pd = _permissive(lv), _permissive(dv)
    if pl is None or pd is None:
        return "undecidable"
    if pl == pd:
        return "same strength"
    if col in PERMISSION_FLAGS:                          # tighter = does not grant
        return "D1 tighter" if pl and not pd else "local tighter"
    return "D1 tighter" if pd and not pl else "local tighter"   # obligation: tighter = requires


def _tighter(fa: dict, fb: dict) -> bool:
    for col in LICENCE_FLAGS:
        a, b = _permissive(fa.get(col)), _permissive(fb.get(col))
        if col in PERMISSION_FLAGS and a and not b:
            return False
        if col not in PERMISSION_FLAGS and b and not a:
            return False
    return True


def licence_ref_direction(lv, dv, local_flags: dict, d1_flags: dict) -> str:
    fl, fd = local_flags.get(lv), d1_flags.get(dv) or local_flags.get(dv)
    if fl is None or fd is None or any(_permissive(v) is None for f in (fl, fd) for v in f.values()):
        return "undecidable"
    a, b = _tighter(fl, fd), _tighter(fd, fl)
    if a and b:
        return "same strength"
    if b:
        return "D1 tighter"
    if a:
        return "local tighter"
    return "incomparable"


# ---- inputs --------------------------------------------------------------------------------------------------

def load_gate() -> set[str]:
    """The committed worker gate, lower-cased. An absent or unreadable gate STOPS the tool (as
    core.sync_state_d1._gated_ids does): a gate that cannot be read is not an empty gate."""
    from core import gen_denylist                                       # noqa: PLC0415
    if not os.path.exists(gen_denylist.OUT):
        raise Refused("refused: the worker gate (api/worker/src/denylist.ts) is absent")
    try:
        return {s.lower() for s in gen_denylist.committed_gate()}
    except Exception as e:                                              # noqa: BLE001 - unreadable = refused
        raise Refused(f"refused: the worker gate cannot be read ({type(e).__name__})") from None


def gate_sha() -> str | None:
    from core import gen_denylist                                       # noqa: PLC0415
    return file_sha256(gen_denylist.OUT) if os.path.exists(gen_denylist.OUT) else None


def load_run_locations() -> tuple[dict, str | None]:
    """{source_id: run_location} through the registry's own loader (the health.py accessor, R276)."""
    from updater import config, registry                               # noqa: PLC0415
    reg = registry.load()
    out = {e["source_id"]: e.get("run_location") for e in reg.get("sources", []) if e.get("source_id")}
    return out, file_sha256(config.REGISTRY) if os.path.exists(config.REGISTRY) else None


def load_shard_map() -> dict:
    from core.sync_state_d1 import CATALOG_SHARD_FOR                    # noqa: PLC0415 - the one map
    return dict(CATALOG_SHARD_FOR)


def read_tsv(path: str, required: tuple[str, ...]) -> list[dict]:
    """A reviewed list: tab-separated, a header line, '#' lines are comments."""
    rows, header = [], None
    with open(path, encoding="utf-8-sig") as fh:
        for n, ln in enumerate(fh, 1):
            ln = ln.rstrip("\r\n")
            if not ln.strip() or ln.lstrip().startswith("#"):
                continue
            parts = [p.strip() for p in ln.split("\t")]
            if header is None:
                header = parts
                missing = [c for c in required if c not in header]
                if missing:
                    raise Refused(f"refused: {path} lacks the column(s) {missing}")
                continue
            parts += [""] * (len(header) - len(parts))
            row = dict(zip(header, parts))
            row["_line"] = n
            rows.append(row)
    if header is None:
        raise Refused(f"refused: {path} has no header line")
    return rows


def load_c1(path: str) -> set:
    return {(r["source"], r["table"], r["column"]) for r in read_tsv(path, ("source", "table", "column", "evidence"))}


def load_c3(path: str) -> set:
    return {r["source"] for r in read_tsv(path, ("source", "writer", "evidence"))}


def load_ids(path: str) -> list[str]:
    with open(path, encoding="utf-8-sig") as fh:
        return [ln.strip() for ln in fh if ln.strip() and not ln.lstrip().startswith("#")]


class C7:
    """The keep-local list: ids, (id, column) cells and sources, each with where it came from; the C7-gap
    sources; and Road B's null_over_local pairs, the only exemptions from V7."""

    def __init__(self):
        self.ids: dict[str, set] = {}
        self.cells: dict[tuple, set] = {}
        self.srcs: dict[str, set] = {}
        self.gaps: dict[str, list] = {}
        self.runs: list[dict] = []
        self.notes: list[str] = []
        self.v7_exempt: set = set()

    def add_ids(self, ids, why: str) -> None:
        for i in ids:
            self.ids.setdefault(i, set()).add(why)

    def add_cell(self, key: str, col: str, why: str) -> None:
        self.cells.setdefault((key, col), set()).add(why)

    def add_src(self, s: str, why: str) -> None:
        self.srcs.setdefault(s, set()).add(why)

    def has(self, key: str, col: str, source) -> bool:
        return key in self.ids or (key, col) in self.cells or (source is not None and source in self.srcs)


def parse_receipt(parser: str, path: str) -> tuple[set, set, set]:
    """(ids, sources, null_over_local cells) a receipt names. parsers: sync_rows (sync_source_rows_d1_to_local's
    JSON), ids_file (one id per line), json_list:<key>."""
    if parser == "ids_file":
        return set(load_ids(path)), set(), set()
    with open(path, encoding="utf-8-sig") as fh:
        j = json.load(fh)
    if parser == "sync_rows":
        nol = j.get("null_over_local") or {}
        cells = {(sid, col) for sid, cs in nol.items() for col in (cs or {})}
        ids = set(j.get("missing") or []) | set(j.get("differing") or []) | set(j.get("before_after") or {})
        return ids, ({j["source"]} if j.get("source") else set()), cells
    if parser.startswith("json_list:"):
        v = j.get(parser.split(":", 1)[1])
        if not isinstance(v, list):
            raise Refused(f"refused: a receipt has no list under {parser}")
        return set(v), set(), set()
    raise Refused(f"refused: unknown receipt parser {parser!r}")


def load_c7_runs(c7: C7, runs_path: str, receipts_dir: str | None, writers_path: str) -> None:
    """C7(b): the reviewed run list. A row with a receipt adds the receipt's ids (or its source, for a source-grain
    receipt; Road B's null_over_local pairs become C7 CELLS and the V7 exemptions); a row with none is a C7 GAP for
    its sources. A named receipt that is missing is an error, never a gap. Receipts are read ONLY from
    --receipts-dir (F: copies made by the coordinator's reviewed copy step, Q2)."""
    writers = {w["writer"]: w for w in read_tsv(writers_path, ("writer", "parser", "grain", "evidence"))}
    cutoff = dt.datetime.strptime(C7_CUTOFF, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    for r in read_tsv(runs_path, ("run_key", "writer", "utc", "sources", "receipt", "grain", "booking")):
        where = f"run list line {r['_line']}"
        try:
            when = dt.datetime.strptime(r["utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
        except ValueError:
            raise Refused(f"refused: {where}: utc is not YYYY-MM-DDTHH:MM:SSZ") from None
        if r["writer"] not in writers:
            raise Refused(f"refused: {where}: the writer is not in the writer map")
        srcs = [s.strip() for s in r["sources"].split(",") if s.strip()]
        if not srcs or "*" in srcs:
            raise Refused(f"refused: {where}: name the run's sources (no '*')")
        rec = {"run_key": r["run_key"], "writer": r["writer"], "utc": r["utc"], "sources": srcs,
               "receipt": r["receipt"], "grain": r["grain"], "booking": r["booking"]}
        if when < cutoff:
            rec["status"] = "before the cutoff"
            c7.runs.append(rec)
            continue
        if r["grain"] not in ("ids", "source", "none"):
            raise Refused(f"refused: {where}: grain must be ids, source or none")
        if r["grain"] == "none" or not r["receipt"]:
            for s in srcs:
                c7.gaps.setdefault(s, []).append(rec)
            rec["status"] = "C7 gap"
            c7.runs.append(rec)
            continue
        if not receipts_dir:
            raise Refused(f"refused: {where} names a receipt; --receipts-dir is required")
        p = os.path.realpath(os.path.join(receipts_dir, r["receipt"]))
        if not _is_under(p, receipts_dir):
            raise Refused(f"refused: {where}: the receipt is outside --receipts-dir")
        if not os.path.isfile(p):
            raise Refused(f"refused: {where}: the receipt is missing (the list is wrong)")
        parser = r.get("parser") or writers[r["writer"]]["parser"]
        if r["grain"] == "source":
            for s in srcs:
                c7.add_src(s, f"run {r['run_key']}")
            rec["status"] = "source-grain receipt"
        else:
            ids, _, cells = parse_receipt(parser, p)
            c7.add_ids(ids, f"run {r['run_key']}")
            for k, col in cells:
                c7.add_cell(k, col, f"run {r['run_key']} null_over_local")
                if r.get("road_b", "").lower() in ("yes", "1", "true"):
                    c7.v7_exempt.add((k, col))
            rec["status"], rec["ids"], rec["cells"] = "receipt parsed", len(ids), len(cells)
        rec["receipt_sha256"] = file_sha256(p)
        c7.runs.append(rec)


def load_c7_hand(c7: C7, path: str) -> None:
    """grain id (a row), cell (key = <id>|<column>), or source."""
    for r in read_tsv(path, ("key", "grain", "reason", "evidence")):
        if r["grain"] == "id":
            c7.add_ids([r["key"]], "hand list")
        elif r["grain"] == "cell":
            k, _, col = r["key"].rpartition("|")
            if not k or not col:
                raise Refused(f"refused: hand list line {r['_line']}: a cell key is <id>|<column>")
            c7.add_cell(k, col, "hand list")
        elif r["grain"] == "source":
            c7.add_src(r["key"], "hand list")
        else:
            raise Refused(f"refused: hand list line {r['_line']}: grain must be id, cell or source")


def load_queue(c7: C7, queue: str, s4c_receipt: str | None) -> dict:
    """C7(a): the S4c copy of pending_catalog_sync.txt, checked against its S4c receipt (sha256, size, lines)."""
    info = {"path": queue, "sha256": file_sha256(queue), "size": os.path.getsize(queue)}
    with open(queue, "rb") as fh:
        info["lines"] = sum(1 for _ in fh)
    if s4c_receipt:
        with open(s4c_receipt, encoding="utf-8") as fh:
            want = json.load(fh)
        for k in ("sha256", "size", "lines"):
            if want.get(k) != info[k]:
                raise Refused(f"refused: the queue copy's {k} differs from the S4c receipt's")
        info["s4c_receipt"] = s4c_receipt
    c7.add_ids(load_ids(queue), "queue")
    return info


# ---- the allow-list (coordinator ruling on H1/H2) --------------------------------------------------------------

class Allow:
    """What the tool may handle. Built from the LOCAL catalogue: its series and source sources plus the named C3
    sources, minus the gate; the licences those rows reference (on either side, added as D1 rows are admitted)
    and in which no gated id appears."""

    def __init__(self, sources: set, gate: set, licence_refs: set):
        self.gate = {g.lower() for g in gate if g}
        self.sources = {s for s in sources if s and s.lower() not in self.gate}
        self.licence_refs = set(licence_refs)
        # a gated id not glued to a letter or digit: 'ilo' does not match 'ilostat', 'imf_fsi' does not match
        # 'imf_fsire' (R112), but 'x_<gated>', '<gated>_v2' and '<gated>:key' all match (R888: when in doubt, the
        # text is withheld), case-insensitive
        self._rx = re.compile("|".join(r"(?<![a-z0-9])" + re.escape(g) + r"(?![a-z0-9])"
                                       for g in sorted(self.gate)), re.I) if self.gate else None

    def digest(self) -> str:
        return hashlib.sha256("\n".join(sorted(self.sources)).encode("utf-8")).hexdigest()

    def names_gated(self, *texts) -> bool:
        if self._rx is None:
            return False
        return any(isinstance(t, str) and self._rx.search(t) for t in texts)

    def row_names_gated(self, row: dict | None) -> bool:
        return bool(row) and (any(v == WITHHELD for v in row.values())
                              or self.names_gated(*[v for v in row.values() if isinstance(v, str)]))

    def redact(self, table: str, row: dict) -> tuple[dict, bool]:
        """A D1 row as it may be STORED: every non-key value in which a gated id appears is replaced by WITHHELD
        (a value D1's JSON answer can never carry), so no snapshot file holds the text (R888)."""
        out, hit = {}, False
        for k, v in row.items():
            if k != PK[table] and isinstance(v, str) and self.names_gated(v):
                out[k], hit = WITHHELD, True
            else:
                out[k] = v
        return out, hit

    def series(self, row: dict) -> bool:
        sid = row.get("series_id")
        if not isinstance(sid, str) or ":" not in sid or self.names_gated(sid):
            return False
        if sid.split(":", 1)[0] not in self.sources:
            return False
        s = row.get("source_id", sid.split(":", 1)[0])
        return s in self.sources

    def source(self, row: dict) -> bool:
        return row.get("source_id") in self.sources

    def licence(self, row: dict) -> bool:
        lid = row.get("license_id")
        return lid in self.licence_refs and not self.names_gated(lid, row.get("name"), row.get("url"))

    def admit(self, table: str, row: dict) -> bool:
        return {"series": self.series, "source": self.source, "license": self.licence}[table](row)


def build_allow(con, gate: set, c3n: set) -> Allow:
    """From the local catalogue: DISTINCT series.source_id (the source_id index), the source table, the named C3
    sources; and the licence ids that the allowed local rows reference (one read of series' two columns)."""
    srcs = {r[0] for r in con.execute("SELECT DISTINCT source_id FROM series") if isinstance(r[0], str)}
    srcs |= {r[0] for r in con.execute("SELECT source_id FROM source") if isinstance(r[0], str)}
    srcs |= set(c3n)
    allow = Allow(srcs, gate, set())
    refs = set()
    for s, lid in con.execute("SELECT DISTINCT source_id, license_id FROM series"):
        if s in allow.sources and isinstance(lid, str):
            refs.add(lid)
    for s, lid in con.execute("SELECT source_id, license_id FROM source"):
        if s in allow.sources and isinstance(lid, str):
            refs.add(lid)
    allow.licence_refs = refs
    return allow


# ---- the D1 road ---------------------------------------------------------------------------------------------

def ensure_token(env_file: str | None = None) -> str:
    """The D1 token, loaded IN-PROCESS by this reviewed tool (R1685; coordinator 2026-11-28, Q12): the process
    environment first, else the live checkout's .env. Before T0 CLOUDFLARE_API_TOKEN (today's road), after T0
    D1_READ_TOKEN only. The value is never printed; only where it came from."""
    name = "D1_READ_TOKEN" if cutover.is_cut_over() else "CLOUDFLARE_API_TOKEN"
    if (os.environ.get(name) or "").strip():
        return f"{name}: from the process environment"
    path = env_file or ENV_FILE
    val = None
    if os.path.isfile(path):
        with open(path, encoding="utf-8-sig") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln or ln.startswith("#") or "=" not in ln:
                    continue
                k, v = ln.split("=", 1)
                k = k.strip()
                if k.startswith("export "):
                    k = k[len("export "):].strip()
                if k == name:
                    v = v.strip().strip('"').strip("'")
                    if v and not set(v) <= {"."} and v.lower() not in ("changeme", "todo", "xxx"):
                        val = v
    if not val:
        raise Refused(f"refused: {name} is set neither in the environment nor in the .env file")
    os.environ[name] = val
    return f"{name}: loaded in-process from the .env file (the value is not printed)"


def _query_fn():
    if D1_QUERY is not None:
        return D1_QUERY
    from core import d1_remote                                          # noqa: PLC0415
    return d1_remote.query


def read_problem(sql: str) -> str | None:
    from core import d1_remote                                          # noqa: PLC0415
    return d1_remote.read_statement_problem(sql)


class Budget:
    """--budget-rows, CUMULATIVE across resumes (review M4): every statement's rows_read is recorded in
    <out>/budget.sqlite BEFORE its result is used, and a resumed run starts from that total."""

    def __init__(self, limit, ledger: str | None = None):
        if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
            raise Refused("refused: --budget-rows is required: a positive count of D1 rows read (R492)")
        if limit > MAX_BUDGET_ROWS:
            raise Refused(f"refused: --budget-rows {limit:,} is above the ceiling {MAX_BUDGET_ROWS:,}")
        self.limit, self.spent, self.statements, self.ledger = limit, 0, 0, None
        if ledger:
            self.ledger = sqlite3.connect(ledger)  # plain-open: the tool's own budget ledger on F:
            self.ledger.execute(f"PRAGMA synchronous={OWN_SYNC}")
            self.ledger.execute("CREATE TABLE IF NOT EXISTS spent(utc, db, what, rows_read)")
            self.ledger.commit()
            self.spent = self.ledger.execute("SELECT COALESCE(SUM(rows_read), 0) FROM spent").fetchone()[0]
            self.statements = self.ledger.execute("SELECT COUNT(*) FROM spent").fetchone()[0]

    def check(self, expected: int, what: str) -> None:
        if self.spent + expected > self.limit:
            raise Stop(f"STOP before {what}: {self.spent:,} rows read so far (all runs) + {expected:,} expected > "
                       f"the budget {self.limit:,}")

    def charge(self, n: int, db: str, what: str) -> None:
        self.spent += n
        self.statements += 1
        if self.ledger is not None:
            self.ledger.execute("INSERT INTO spent VALUES(?,?,?,?)", (utcnow(), db, what, n))
            self.ledger.commit()


def d1(budget: Budget, db: str, sql: str, params: list, expected: int, what: str):
    """ONE plain read on D1. Checked as a read BEFORE it is sent, charged by D1's own meta.rows_read. `what` must
    never hold a key."""
    if db not in DATABASES:
        raise Refused(f"refused: unknown D1 database {db!r}")
    why = read_problem(sql)
    if why:
        raise Refused(f"refused before sending: not one plain read ({why})")
    budget.check(expected, what)
    t = time.monotonic()
    res = _query_fn()(db, sql, list(params))
    secs = time.monotonic() - t
    meta = (res or {}).get("meta") if isinstance(res, dict) else None
    rr = (meta or {}).get("rows_read")
    if not isinstance(rr, int) or isinstance(rr, bool) or rr < 0:
        raise Stop(f"STOP: D1 gave no meta.rows_read for {what} - an unmeasured read is a failure (R982)")
    budget.charge(rr, db, what)
    rows = res.get("results")
    if not isinstance(rows, list):
        raise Stop(f"STOP: D1 gave no result list for {what}")
    return rows, rr, secs


def projection(ns: dict, page_rows: int, e: dict, r_small: int, r_search: int) -> int:
    """R_total = SUM_db [ N_db + pages_db * e_db ] + R_small + R_search, pages_db = N_db // P + 1."""
    return sum(int(ns[db]) + (int(ns[db]) // page_rows + 1) * max(int(e.get(db, 1)), 0) for db in ns) \
        + int(r_small) + int(r_search)


def replay_types(master_rows: list) -> dict:
    """{table: [(column, declared type)]} for the reconciled tables, by replaying D1's own CREATE TABLE into an
    in-memory SQLite (no hand-parsing of DDL)."""
    mem = sqlite3.connect(":memory:")  # plain-open: in-memory DDL replay of D1's schema, nothing on disk
    out = {}
    try:
        for r in master_rows:
            if r.get("type") == "table" and r.get("name") in TABLES and r.get("sql"):
                mem.execute(r["sql"])
                out[r["name"]] = [(c[1], c[2] or "") for c in mem.execute(f'PRAGMA table_info("{r["name"]}")')]
    finally:
        mem.close()
    return out


def _admit_master(master_rows: list, allow: Allow) -> tuple[list, bool]:
    """sqlite_master rows in which no gated id appears (a dropped one is only a YES)."""
    keep = [m for m in master_rows if not allow.row_names_gated(m)]
    return keep, len(keep) != len(master_rows)


def _other_tables(master_rows: list) -> list[dict]:
    out = []
    for r in master_rows:
        if r.get("type") != "table" or r.get("name") in TABLES:
            continue
        sql = (r.get("sql") or "").upper()
        out.append({"name": r["name"], "known": r["name"] in KNOWN_D1_TABLES,
                    "virtual": sql.startswith("CREATE VIRTUAL"), "without_rowid": "WITHOUT ROWID" in sql})
    return out


def allowed_count(budget: Budget, db: str, allow: Allow) -> int:
    """N for the projection: source_counts summed over ALLOWED sources only (review M1)."""
    rows, _, _ = d1(budget, db, COUNTS_SQL, [], SMALL_EXPECTED, f"{db} source_counts")
    return sum(int(r.get("n") or 0) for r in rows if r.get("source_id") in allow.sources)


def _allow_from_catalogue(catalogue: str, gate: set, c3n: set) -> Allow:
    con = catalog_path.connect_path(catalogue, write=False, timeout=300)
    con.text_factory = _text_or_bytes
    try:
        return build_allow(con, gate, c3n)
    finally:
        con.close()


def cmd_probe(a) -> int:
    refuse_e_checkout()
    out = guard_out(a.out)
    catalogue = guard_catalogue(a.catalogue)
    budget = Budget(a.budget_rows, os.path.join(out, "budget.sqlite"))
    allow = _allow_from_catalogue(catalogue, load_gate(), load_c3(guard_input(a.c3, "--c3", own_list=True)))
    if D1_QUERY is None:
        say(ensure_token(a.env_file))
    res = {"step": "probe", "utc_start": utcnow(), "page_rows": a.page_rows, "allow_list_sha256": allow.digest(),
           "databases": {}}
    for db in DATABASES:
        master_raw, rr_m, _ = d1(budget, db, MASTER_SQL, [], SMALL_EXPECTED, f"{db} sqlite_master")
        master, dropped = _admit_master(master_raw, allow)
        try:
            n = allowed_count(budget, db, allow)
        except RuntimeError:                                            # no such table: recorded, not guessed
            n = None
        page, rr_p, secs = d1(budget, db, PAGE_SQL_FIRST, ["", a.page_rows], a.page_rows + 10, f"{db} series page 1")
        nbytes = len(json.dumps(page, default=_jdefault).encode("utf-8"))
        per_row = nbytes / max(len(page), 1)
        others = _other_tables(master)
        res["databases"][db] = {
            "rows_read_master": rr_m, "allowed_rows_by_source_counts": n, "e": rr_p - len(page),
            "page_was_full": len(page) == a.page_rows, "bytes_per_row": round(per_row, 1),
            "page_seconds": round(secs, 3), "tables": others,
            "unknown_tables": sorted(t["name"] for t in others if not t["known"]),
            "a_table_name_matches_the_gate": "YES" if dropped else "NO",
            "page_size_recommended": max(MIN_PAGE_ROWS, min(MAX_PAGE_ROWS, int(ANSWER_BYTES_TARGET // max(per_row, 1))))}
    dbs = res["databases"]
    if all(dbs[db]["allowed_rows_by_source_counts"] is not None for db in DATABASES):
        p_rec = min(dbs[db]["page_size_recommended"] for db in DATABASES)
        ns = {db: dbs[db]["allowed_rows_by_source_counts"] for db in DATABASES}
        e = {db: dbs[db]["e"] for db in DATABASES}
        small = sum(dbs[db]["rows_read_master"] for db in DATABASES) + 8_000
        r = projection(ns, p_rec, e, small, 300_000)
        res["projection"] = {"formula": "SUM_db [N_db + (N_db // P + 1) * e_db] + R_small + R_search",
                             "P": p_rec, "N_allowed": ns, "e": e, "R_small_est": small, "R_search_est": 300_000,
                             "R_total": r, "scan_equivalents": round(r / max(ns[PRIMARY], 1), 2),
                             "usd_if_billable": round(r / 1e6 * 0.001, 4),
                             "note": "N counts allowed sources only; gated rows are read and charged but never "
                                     "counted here"}
    res.update(rows_read_total=budget.spent, statements=budget.statements, utc_end=utcnow())
    write_json(os.path.join(out, "probe.json"), res)
    say(json.dumps(res.get("projection") or {"projection": "not computed (no source_counts)"}, default=str))
    say(f"probe: {budget.statements} statement(s), {budget.spent:,} rows read")
    return 0


# ---- read-d1: the paged snapshot ---------------------------------------------------------------------------

def _snap_connect(path: str):
    con = sqlite3.connect(path, timeout=60)  # plain-open: the tool's own D1 snapshot on F:, never a catalogue
    con.execute(f"PRAGMA synchronous={OWN_SYNC}")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS _master(type, name, tbl_name, sql);
        CREATE TABLE IF NOT EXISTS _pages(db, tbl, page_no, rows_read, seconds, page_rows_asked, utc);
        CREATE TABLE IF NOT EXISTS _state(k PRIMARY KEY, v);
        CREATE TABLE IF NOT EXISTS _colstats(tbl, source_id, col, n_rows, n_nonnull);
        CREATE TABLE IF NOT EXISTS _other_tables(name, known, virtual, without_rowid, max_rowid, note);
    """)
    return con


def _state_get(con, k, default=None):
    r = con.execute("SELECT v FROM _state WHERE k=?", (k,)).fetchone()
    return json.loads(r[0]) if r else default


def _state_set(con, k, v) -> None:
    con.execute("INSERT OR REPLACE INTO _state(k, v) VALUES(?, ?)", (k, json.dumps(v)))


def _create_untyped(con, table: str, cols: list) -> None:
    # NO declared types: SQLite keeps each value exactly as the D1 answer gave it; the D1 declared types are kept
    # in _master and replayed for the normalisation
    pk = PK[table]
    con.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ('
                + ", ".join(f'"{c}" PRIMARY KEY' if c == pk else f'"{c}"' for c in cols) + ")")


def _snapval(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v)                       # D1 returns only scalars; anything else is kept as its JSON text
    return v


def _check_types(table: str, row: dict, decl: dict) -> None:
    """A non-text value in a column D1 declares TEXT cannot happen under TEXT affinity: an anomaly, a STOP."""
    for c, v in row.items():
        if v is not None and not isinstance(v, str) and affinity(decl.get(c)) == "TEXT":
            raise Stop(f"STOP: D1 returned a {type(v).__name__} in the TEXT column {table}.{c} of {h(row.get(PK[table]))}")


def read_db_main(budget: Budget, db: str, snap_path: str, page_rows: int, allow: Allow, probe_e: int | None) -> dict:
    """Schema, other tables, series pages and the source table of ONE database. Every row is ADMITTED on receipt
    (allow-list) before any check that could print it; only admitted rows are stored. One transaction per page
    stores the page's admitted rows AND the cursor (the last ADMITTED key) - a resume re-reads at most the dropped
    tail. No per-page row counts are stored (review M1)."""
    con = _snap_connect(snap_path)
    try:
        if _state_get(con, "allow_list_sha256") not in (None, allow.digest()):
            raise Refused("refused: this snapshot was made with a different allow-list; start a new --out")
        master = [dict(zip(("type", "name", "tbl_name", "sql"), r)) for r in con.execute("SELECT * FROM _master")]
        if not master:
            raw, _, _ = d1(budget, db, MASTER_SQL, [], SMALL_EXPECTED, f"{db} sqlite_master")
            master, dropped = _admit_master(raw, allow)
            with con:
                con.executemany("INSERT INTO _master VALUES(?,?,?,?)",
                                [(m.get("type"), m.get("name"), m.get("tbl_name"), m.get("sql")) for m in master])
                _state_set(con, "allow_list_sha256", allow.digest())
                _state_set(con, "dropped_master", bool(dropped))
        types = replay_types(master)
        for t, cols in types.items():
            _create_untyped(con, t, [c for c, _ in cols])
        if _state_get(con, "others_done") is None:
            with con:
                for o in _other_tables(master):
                    mr, note = None, ""
                    if not o["virtual"] and not o["without_rowid"]:
                        try:
                            rows, _, _ = d1(budget, db, f'SELECT MAX(rowid) AS m FROM "{o["name"]}"', [], 2,
                                            f"{db} other table max(rowid)")
                            mr = rows[0].get("m") if rows else None
                        except RuntimeError as e:
                            note = f"unreadable ({type(e).__name__})"
                    else:
                        note = "no cheap row count (virtual or WITHOUT ROWID)"
                    con.execute("INSERT INTO _other_tables VALUES(?,?,?,?,?,?)",
                                (o["name"], int(o["known"]), int(o["virtual"]), int(o["without_rowid"]), mr, note))
                _state_set(con, "others_done", True)
        if "series" in types and not _state_get(con, "series_done", False):
            decl = dict(types["series"])
            cols = [c for c, _ in types["series"]]
            cursor = _state_get(con, "series_cursor")                   # the last ADMITTED key stored
            page_no = _state_get(con, "series_pages", 0)
            stored = _state_get(con, "series_stored", 0)
            first_in_run, p = True, page_rows
            raw_last = cursor
            while True:
                sql, params = (PAGE_SQL_FIRST, ["", p]) if raw_last is None else (PAGE_SQL, [raw_last, p])
                try:
                    rows, rr, secs = d1(budget, db, sql, params, p + max(probe_e or 1, 0) + 1,
                                        f"{db} series page {page_no + 1}")
                except RuntimeError:                                    # an answer too large: halve P (review L5)
                    if p // 2 < MIN_PAGE_ROWS:
                        raise
                    p //= 2
                    _state_set(con, "page_rows_halved", True)
                    con.commit()
                    continue
                prev = raw_last
                for i, r in enumerate(rows):                            # order over ALL rows; messages hold no key
                    sid = r.get("series_id")
                    if not isinstance(sid, str) or (prev is not None and not sid > prev):
                        raise Stop(f"STOP: {db} page {page_no + 1} is not strictly increasing at position {i}")
                    prev = sid
                keep, redacted = [], False
                for r in rows:
                    if not allow.series(r):
                        continue
                    if set(r) - set(cols):
                        raise Stop(f"STOP: {db} returned series columns its schema lacks")
                    _check_types("series", r, decl)
                    r2, hit = allow.redact("series", r)
                    keep.append(r2)
                    redacted = redacted or hit
                with con:
                    if redacted:
                        _state_set(con, "redacted_values", True)
                    con.executemany(f'INSERT INTO "series" VALUES ({",".join("?" * len(cols))})',
                                    [[_snapval(r.get(c)) for c in cols] for r in keep])
                    page_no += 1
                    stored += len(keep)
                    con.execute("INSERT INTO _pages VALUES(?,?,?,?,?,?,?)",
                                (db, "series", page_no, rr, round(secs, 3), p, utcnow()))
                    _state_set(con, "series_pages", page_no)
                    _state_set(con, "series_stored", stored)
                    if keep:
                        _state_set(con, "series_cursor", keep[-1]["series_id"])
                    if len(keep) != len(rows):
                        _state_set(con, "dropped_series", True)
                    if len(rows) < p:
                        _state_set(con, "series_done", True)
                if first_in_run and probe_e is not None and len(rows) == p and abs((rr - len(rows)) - probe_e) > 1:
                    raise Stop(f"STOP after {db} page {page_no}: rows_read per page differs from the probe's e by "
                               f"more than 1 - re-project before reading on (--resume)")
                first_in_run = False
                if len(rows) < p:
                    break
                raw_last = rows[-1]["series_id"]                        # in memory only; never stored or printed
        if "source" in types and _state_get(con, "source_done") is None:
            rows, rr, secs = d1(budget, db, "SELECT * FROM source", [], SMALL_EXPECTED, f"{db} source")
            keep, redacted = [], False
            for r in rows:
                if allow.source(r):
                    _check_types("source", r, dict(types["source"]))
                    r2, hit = allow.redact("source", r)
                    keep.append(r2)
                    redacted = redacted or hit
            cols = [c for c, _ in types["source"]]
            with con:
                if redacted:
                    _state_set(con, "redacted_values", True)
                con.executemany(f'INSERT OR REPLACE INTO "source" VALUES ({",".join("?" * len(cols))})',
                                [[_snapval(r.get(c)) for c in cols] for r in keep])
                con.execute("INSERT INTO _pages VALUES(?,?,?,?,?,?,?)", (db, "source", 0, rr, round(secs, 3), None,
                                                                         utcnow()))
                _state_set(con, "source_done", True)
                if len(keep) != len(rows):
                    _state_set(con, "dropped_source", True)
        if "series" in types and _state_get(con, "series_count_checked") is None:
            if con.execute('SELECT COUNT(*) FROM "series"').fetchone()[0] != _state_get(con, "series_stored", 0):
                raise Failed(f"{db}: the stored series rows differ from the pages' running total (a lost page)")
            _state_set(con, "series_count_checked", True)
            con.commit()
        return {t: True for t in types}
    finally:
        con.close()


def d1_licence_refs(snap_path: str) -> set:
    con = sqlite3.connect(ro_uri(snap_path), uri=True)  # plain-open: the tool's own snapshot, read-only
    try:
        refs = set()
        for t in ("series", "source"):
            if con.execute("SELECT 1 FROM sqlite_master WHERE name=?", (t,)).fetchone():
                names = [r[1] for r in con.execute(f'PRAGMA table_info("{t}")')]
                if "license_id" in names:
                    refs |= {r[0] for r in con.execute(f'SELECT DISTINCT license_id FROM "{t}"') if isinstance(r[0], str)}
        return refs
    finally:
        con.close()


def read_db_licence(budget: Budget, db: str, snap_path: str, allow: Allow) -> None:
    """The licence table, after BOTH databases' series and source rows are stored: a licence row is admitted only
    when an allowed row (local or D1) references it and no gated id appears in it (review H1)."""
    con = _snap_connect(snap_path)
    try:
        master = [dict(zip(("type", "name", "tbl_name", "sql"), r)) for r in con.execute("SELECT * FROM _master")]
        types = replay_types(master)
        if "license" in types and _state_get(con, "license_done") is None:
            rows, rr, secs = d1(budget, db, "SELECT * FROM license", [], SMALL_EXPECTED, f"{db} license")
            keep = [r for r in rows if allow.licence(r)]
            cols = [c for c, _ in types["license"]]
            with con:
                con.executemany(f'INSERT OR REPLACE INTO "license" VALUES ({",".join("?" * len(cols))})',
                                [[_snapval(r.get(c)) for c in cols] for r in keep])
                con.execute("INSERT INTO _pages VALUES(?,?,?,?,?,?,?)", (db, "license", 0, rr, round(secs, 3), None,
                                                                         utcnow()))
                _state_set(con, "license_done", True)
                if len(keep) != len(rows):
                    _state_set(con, "dropped_license", True)
        if _state_get(con, "colstats_done") is None:
            with con:
                con.execute("DELETE FROM _colstats")
                for t, cols in types.items():
                    names = [c for c, _ in cols]
                    src = '"source_id"' if "source_id" in names else "NULL"
                    for c in names:
                        con.execute(f'INSERT INTO _colstats SELECT ?, {src}, ?, COUNT(*), COUNT("{c}") '
                                    f'FROM "{t}" GROUP BY {src}', (t, c))
                _state_set(con, "colstats_done", True)
    finally:
        con.close()


def snapshot_d1(budget: Budget, out: str, page_rows: int, allow: Allow, probe: dict | None) -> dict:
    paths = {db: os.path.join(out, f"d1_{db}.sqlite") for db in DATABASES}
    for db in DATABASES:
        read_db_main(budget, db, paths[db], page_rows, allow, (probe["databases"][db]["e"] if probe else None))
    refs = set(allow.licence_refs)
    for db in DATABASES:
        refs |= d1_licence_refs(paths[db])
    allow.licence_refs = refs
    for db in DATABASES:
        read_db_licence(budget, db, paths[db], allow)
    return paths


def dropped_flags(paths: dict) -> dict:
    out = {}
    for db, p in paths.items():
        con = sqlite3.connect(ro_uri(p), uri=True)  # plain-open: the tool's own snapshot, read-only
        try:
            out[db] = "YES" if any(_state_get(con, k, False) for k in
                                   ("dropped_master", "dropped_series", "dropped_source", "dropped_license",
                                    "redacted_values")) else "NO"
        finally:
            con.close()
    return out


def cmd_read_d1(a) -> int:
    refuse_e_checkout()
    out = guard_out(a.out)
    catalogue = guard_catalogue(a.catalogue)
    budget = Budget(a.budget_rows, os.path.join(out, "budget.sqlite"))
    gate = load_gate()
    allow = _allow_from_catalogue(catalogue, gate, load_c3(guard_input(a.c3, "--c3", own_list=True)))
    if D1_QUERY is None:
        say(ensure_token(a.env_file))
    probe = None
    if a.probe:
        with open(guard_input(a.probe, "--probe"), encoding="utf-8") as fh:
            probe = json.load(fh)
    res = {"step": "read-d1", "utc_start": utcnow(), "page_rows": a.page_rows, "allow_list_sha256": allow.digest()}
    resuming = budget.statements > 0
    if not resuming:
        ns = {}
        for db in DATABASES:
            try:
                ns[db] = allowed_count(budget, db, allow)
            except RuntimeError:
                n = probe["databases"][db].get("allowed_rows_by_source_counts") if probe else None
                if n is None:
                    raise Refused(f"refused: {db} has no readable source_counts and no probe N - cannot project") \
                        from None
                ns[db] = int(n)
        e = {db: (probe["databases"][db]["e"] if probe else 1) for db in DATABASES}
        r_proj = projection(ns, a.page_rows, e, 8_000, 0)
        res["projection"] = {"N_allowed": ns, "e": e, "P": a.page_rows, "R_total": r_proj,
                             "scan_equivalents": round(r_proj / max(ns[PRIMARY], 1), 2)}
        say(f"read-d1: projected {r_proj:,} rows read (allowed N {ns}, P {a.page_rows}, e {e}); budget {budget.limit:,}")
        if r_proj + budget.spent > budget.limit:
            raise Refused(f"refused: the projection {r_proj:,} is above --budget-rows {budget.limit:,}")
    else:
        say(f"read-d1: resuming; {budget.spent:,} rows already read by earlier runs count against the budget")
    try:
        paths = snapshot_d1(budget, out, a.page_rows, allow, probe)
        res["outside_the_allow_list_dropped"] = dropped_flags(paths)
    finally:
        res.update(rows_read_total=budget.spent, statements=budget.statements, utc_end=utcnow())
        write_json(os.path.join(out, f"read_d1_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}.json"), res)
    say(f"read-d1: {budget.statements} statement(s) in all runs, {budget.spent:,} rows read; D1 held rows outside "
        f"the allow-list: {res['outside_the_allow_list_dropped']}")
    return 0


# ---- classify ------------------------------------------------------------------------------------------------

def _local_iter(con, table: str, cols: list):
    """Every local row of `table` in primary-key order, strictly increasing (keyset pages for series)."""
    pk = PK[table]
    idx = cols.index(pk)
    if table != "series":
        rows = con.execute(f'SELECT * FROM "{table}"').fetchall()
        if any(not isinstance(r[idx], str) for r in rows):
            raise Failed(f"local {table}: a key is NULL or not valid text")
        rows.sort(key=lambda r: r[idx])
        for i in range(1, len(rows)):
            if rows[i][idx] == rows[i - 1][idx]:
                raise Failed(f"local {table}: key {h(rows[i][idx])} is not unique")
        for r in rows:
            yield dict(zip(cols, r))
        return
    last = None
    while True:
        if last is None:
            rows = con.execute(f"SELECT * FROM series WHERE series_id >= ? ORDER BY series_id LIMIT {LOCAL_PAGE_ROWS}",
                               ("",)).fetchall()
        else:
            rows = con.execute(f"SELECT * FROM series WHERE series_id > ? ORDER BY series_id LIMIT {LOCAL_PAGE_ROWS}",
                               (last,)).fetchall()
        for i, r in enumerate(rows):
            if not isinstance(r[idx], str) or (last is not None and not r[idx] > last):
                raise Failed(f"local series: the key at position {i} of a page breaks the primary-key order "
                             "(NULL, not valid text, or out of order)")
            last = r[idx]
            yield dict(zip(cols, r))
        if len(rows) < LOCAL_PAGE_ROWS:
            return


def _snap_iter(con, table: str):
    if con is None or not con.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone():
        return
    cur = con.execute(f'SELECT * FROM "{table}" ORDER BY "{PK[table]}"')
    cols = [d[0] for d in cur.description]
    prev = None
    for r in cur:
        row = dict(zip(cols, r))
        k = row[PK[table]]
        if prev is not None and not k > prev:
            raise Failed(f"snapshot {table}: {h(k)} breaks the order")
        prev = k
        yield row


def merge3(its: list, key: str):
    """Group three key-ordered iterators by key: yields (k, [row or None for each iterator])."""
    cur = [next(it, None) for it in its]
    while any(c is not None for c in cur):
        k = min(c[key] for c in cur if c is not None)
        group = []
        for i, c in enumerate(cur):
            if c is not None and c[key] == k:
                group.append(c)
                cur[i] = next(its[i], None)
            else:
                group.append(None)
        yield k, group


class Out:
    """classify.sqlite: cells, rows, agg - every DIFFERING cell, every row-class row, every count. It holds only
    allowed rows."""

    def __init__(self, path: str):
        if os.path.exists(path):
            raise Refused("refused: classify.sqlite exists in --out (one classify per --out)")
        self.con = sqlite3.connect(path)  # plain-open: the tool's own result file on F:, never a catalogue
        self.con.execute(f"PRAGMA synchronous={OWN_SYNC}")
        self.con.executescript("""
            CREATE TABLE cells(tbl, key, source, col, cls, decision, local_value, d1_value, sublabels);
            CREATE TABLE rows(tbl, key, source, cls, decision, flags, row_json, d1_db);
            CREATE TABLE agg(tbl, source, col, metric, n);
            CREATE TABLE meta(k PRIMARY KEY, v);
        """)
        self.cells, self.rows, self.counts = [], [], {}

    def cell(self, *v):
        self.cells.append(v)
        if len(self.cells) >= 20_000:
            self.flush()

    def row(self, *v):
        self.rows.append(v)
        if len(self.rows) >= 20_000:
            self.flush()

    def count(self, tbl, source, col, metric, n=1):
        k = (tbl, source, col, metric)
        self.counts[k] = self.counts.get(k, 0) + n

    def flush(self):
        with self.con:
            self.con.executemany("INSERT INTO cells VALUES(?,?,?,?,?,?,?,?,?)", self.cells)
            self.con.executemany("INSERT INTO rows VALUES(?,?,?,?,?,?,?,?)", self.rows)
        self.cells, self.rows = [], []

    def close(self, meta: dict):
        self.flush()
        with self.con:
            self.con.executemany("INSERT INTO agg VALUES(?,?,?,?,?)", [(*k, n) for k, n in self.counts.items()])
            self.con.executemany("INSERT INTO meta VALUES(?,?)", [(k, json.dumps(v, default=_jdefault))
                                                                 for k, v in meta.items()])
        self.con.close()


def row_digest_hash(cols: list, row: dict) -> str:
    from core.catalog_sync_manifest import row_hash                    # noqa: PLC0415 - the one row hash
    return row_hash(cols, row)


class Digests:
    """Per bucket (an allowed source, '_withheld', '_table:<t>') sha256 of row hashes in primary-key order."""

    def __init__(self):
        self.h: dict[str, object] = {}

    def add(self, bucket: str, cols: list, row: dict) -> None:
        self.h.setdefault(bucket, hashlib.sha256()).update(row_digest_hash(cols, row).encode() + b"\n")

    def result(self) -> dict:
        return {k: v.hexdigest() for k, v in sorted(self.h.items())}


def _bucket(table: str, row: dict, allow: Allow) -> str:
    if not allow.admit(table, row):
        return "_withheld"
    if table != "series":
        return f"_table:{table}"
    return str(row.get("source_id"))


class Manifest:
    """C7(d), optional: the S4c copy of the sync manifest (catalog_sync_sent.db), read-only and immutable."""

    def __init__(self, path: str):
        for side in ("-journal", "-wal"):
            if os.path.exists(path + side):
                raise Refused(f"refused: a {side} file sits beside the manifest copy - not a static copy (R1306)")
        self.con = sqlite3.connect(ro_uri(path, True), uri=True)  # plain-open: the manifest COPY on F:, immutable
        self.path = path

    def changed(self, cols: list, rows: list[dict]) -> set:
        from core.catalog_sync_manifest import row_hash                # noqa: PLC0415
        from core.titles import clean_title                            # noqa: PLC0415
        if not rows:
            return set()
        lo, hi = rows[0]["series_id"], rows[-1]["series_id"]
        known = dict(self.con.execute("SELECT series_id, row_hash FROM sent WHERE series_id >= ? AND series_id <= ?",
                                      (lo, hi)))
        out = set()
        for r in rows:
            t = r.get("title")
            hsh = row_hash(cols, {**r, "title": clean_title(t) if isinstance(t, str) else t})
            if known.get(r["series_id"]) != hsh:
                out.add(r["series_id"])
        return out


class ServedIndex:
    def __init__(self, path: str):
        self.con = sqlite3.connect(ro_uri(path, True), uri=True)  # plain-open: the served index, ro+immutable (Q6)

    def has(self, series_id: str) -> bool:
        import urllib.parse                                            # noqa: PLC0415
        key = "series/" + urllib.parse.quote(series_id, safe="") + ".csv"
        return self.con.execute("SELECT 1 FROM blobs WHERE key=?", (key,)).fetchone() is not None


def _manifest_iter(it, manifest, cols):
    """Annotate local series rows with '_mchanged' (manifest witness), page by page."""
    buf = []
    for r in it:
        buf.append(r)
        if len(buf) >= LOCAL_PAGE_ROWS:
            ch = manifest.changed(cols, buf)
            for x in buf:
                x["_mchanged"] = x["series_id"] in ch
                yield x
            buf = []
    ch = manifest.changed(cols, buf)
    for x in buf:
        x["_mchanged"] = x["series_id"] in ch
        yield x


_AUDIT_CACHE: dict = {}


def audit_line(source) -> str:
    """The first DATABASE_LICENSES_VERBATIM.md line naming `source` (word boundary), as file:line."""
    if source is None:
        return "no source"
    if "lines" not in _AUDIT_CACHE:
        try:
            with open(AUDIT_FILE, encoding="utf-8", errors="replace") as fh:
                _AUDIT_CACHE["lines"] = fh.read().splitlines()
        except OSError:
            _AUDIT_CACHE["lines"] = None
    lines = _AUDIT_CACHE["lines"]
    if lines is None:
        return "the audit file is not readable here"
    rx = re.compile(r"(?<![a-z0-9_])" + re.escape(source.lower()) + r"(?![a-z0-9_])")
    for n, ln in enumerate(lines, 1):
        if rx.search(ln.lower()):
            return f"DATABASE_LICENSES_VERBATIM.md:{n}"
    return "no audit line found"


def classify(*, catalogue: str, snapshots: dict, out_dir: str, allow: Allow, runloc: dict, shard: dict, c1: set,
             c3n: set, c7: C7, deletes: set, manifest: Manifest | None = None,
             served: ServedIndex | None = None) -> dict:
    """The class rules of (b2) over local x D1, in primary-key order, over ALLOWED rows only. Writes
    classify.sqlite and returns the plan and the counts. Pure reads of the catalogue."""
    from core.titles import clean_title                                # noqa: PLC0415
    con = catalog_path.connect_path(catalogue, write=False, timeout=300)
    con.text_factory = _text_or_bytes
    con.execute("PRAGMA busy_timeout=300000")
    snaps = {}
    for db, p in snapshots.items():
        snaps[db] = sqlite3.connect(ro_uri(p, True), uri=True) if p and os.path.exists(p) else None  # plain-open: own snapshot
    for db, sc in snaps.items():
        if sc is not None and _state_get(sc, "allow_list_sha256") != allow.digest():
            raise Refused(f"refused: the {db} snapshot was made with a different allow-list (re-read D1)")
    o = Out(os.path.join(out_dir, "classify.sqlite"))
    dig_before, dig_after = Digests(), Digests()
    plan = {"cells": [], "inserts": [], "deletes": []}
    stats = {"misplaced": 0, "duplicate": 0, "shard_copy_differs": 0, "series_rows_local_allowed": 0,
             "c6_cells": 0, "c6_rows": 0, "c3_served": 0, "c3_local_removal": 0, "c3_parent_missing": 0,
             "del_both": 0, "del_withheld": False, "licence_held": 0, "roundtrip": 0, "local_invalid_utf8": 0,
             "withheld_values": False}
    streamed = 0
    d1_types, colstats = {}, {}
    for db, sc in snaps.items():
        if sc is not None:
            master = [dict(zip(("type", "name", "tbl_name", "sql"), r)) for r in sc.execute("SELECT * FROM _master")]
            for t, cols in replay_types(master).items():
                d1_types.setdefault(t, dict(cols))
            for tbl, src, col, n_rows, n_nonnull in sc.execute("SELECT * FROM _colstats"):
                colstats[(db, tbl, src, col)] = (n_rows, n_nonnull)
    local_src = {r[0] for r in con.execute("SELECT source_id FROM source")}
    local_lic = {r[0] for r in con.execute("SELECT license_id FROM license")}
    flags_local = {r[0]: dict(zip(LICENCE_FLAGS, r[1:])) for r in con.execute(
        "SELECT license_id, reservable, commercial_ok, attribution_required, no_modify FROM license")}
    flags_d1: dict = {}
    for sc in snaps.values():
        if sc is not None and sc.execute("SELECT 1 FROM sqlite_master WHERE name='license'").fetchone():
            names = [r[1] for r in sc.execute('PRAGMA table_info("license")')]
            if all(c in names for c in LICENCE_FLAGS):
                for r in sc.execute("SELECT license_id, reservable, commercial_ok, attribution_required, no_modify "
                                    "FROM license"):
                    flags_d1.setdefault(r[0], dict(zip(LICENCE_FLAGS, r[1:])))
    try:
        for t in TABLES:
            lcols_decl = [(r[1], r[2] or "") for r in con.execute(f'PRAGMA table_info("{t}")')]
            lcols = [c for c, _ in lcols_decl]
            ldecl = dict(lcols_decl)
            dcols = d1_types.get(t, {})
            pk = PK[t]
            kinds = {x: column_kind(t, x, ldecl.get(x), dcols.get(x)) for x in set(lcols) | set(dcols)}
            local_it = _local_iter(con, t, lcols)
            if t == "series" and manifest is not None:
                local_it = _manifest_iter(local_it, manifest, lcols)
            group_it = merge3([local_it, _snap_iter(snaps.get(PRIMARY), t), _snap_iter(snaps.get(CLIMATE), t)], pk)
            for key, (l, p, c) in group_it:
                if l is not None:
                    dig_before.add(_bucket(t, l, allow), lcols, l)
                    if t == "series":
                        streamed += 1
                # THE GATE FIRST (review M1): a local row outside the allow-list is not compared, and the snapshot
                # holds only admitted rows; MISPLACED is decided after this line
                if l is not None and not allow.admit(t, l):
                    dig_after.add("_withheld", lcols, l)
                    continue
                if t == "series":
                    s = (l or p or c).get("source_id")
                    home = shard.get(s, PRIMARY)
                    d, other = (p, c) if home == PRIMARY else (c, p)
                    d_db = home
                    if other is not None:
                        flag = "DUPLICATE" if d is not None else "MISPLACED"
                        stats["misplaced" if d is None else "duplicate"] += 1
                        o.row(t, key, s, flag, "none", "{}", None, CLIMATE if home == PRIMARY else PRIMARY)
                else:
                    s = key if t == "source" else None
                    d, d_db = (p, PRIMARY) if p is not None else (c, CLIMATE)
                    if p is not None and c is not None and any(compare("text", p.get(x), c.get(x)) == DIFF for x in p):
                        stats["shard_copy_differs"] += 1
                if l is None and d is None:
                    continue                                        # only in the wrong database: listed above
                if t == "series" and l is not None:
                    stats["series_rows_local_allowed"] += 1
                if t == "series" and key in deletes and l is not None and d is not None:
                    stats["del_both"] += 1                           # review M9: B31 incomplete - a FAILED check
                    o.row(t, key, s, "C4-DEL-BOTH", "failed", "{}", None, d_db)
                    o.count(t, s, None, "rows_C4-DEL-BOTH")
                    dig_after.add(_bucket(t, l, allow), lcols, l)
                    continue
                if l is not None and d is None:
                    if t == "series" and key in deletes and allow.row_names_gated(l):
                        stats["del_withheld"] = True                 # cannot be planned without its values: FAILED
                        stats["withheld_values"] = True
                        o.row(t, key, s, "C4-DEL-WITHHELD", "failed", "{}", None, None)
                        dig_after.add(_bucket(t, l, allow), lcols, l)
                        continue
                    if t == "series" and key in deletes:
                        lrow = {x: l[x] for x in lcols}
                        o.row(t, key, s, "C4-DEL", "delete_local", "{}", json.dumps(lrow, default=_jdefault), None)
                        plan["deletes"].append([t, key, lrow])
                        o.count(t, s, None, "rows_C4-DEL")
                        continue                                    # not in the after digest
                    o.row(t, key, s, "C4", "keep", "{}", None, None)
                    o.count(t, s, None, "rows_C4")
                    dig_after.add(_bucket(t, l, allow), lcols, l)
                    continue
                if d is not None and l is None:
                    fl = {}
                    if t == "series":
                        if key in c7.ids:
                            fl["local_removal_evidence"] = sorted(c7.ids[key])
                            stats["c3_local_removal"] += 1
                        if served is not None:
                            fl["served_object"] = served.has(key)
                            stats["c3_served"] += int(fl["served_object"])
                        if s == C6_SOURCE:
                            stats["c6_rows"] += 1                    # V7 counts it whatever its class (M6/M8)
                    if allow.row_names_gated(d):                     # a value names a gated id: not taken, never
                        stats["withheld_values"] = True              # stored or counted - only the YES line
                        continue
                    if t == "series" and s in c3n:
                        if d.get("source_id") not in local_src or d.get("license_id") not in local_lic:
                            fl["parent_missing"] = True              # R346: refused before any write
                            stats["c3_parent_missing"] += 1
                        o.row(t, key, s, "C3-TAKEN", "insert_local", json.dumps(fl), json.dumps(d, default=_jdefault),
                              d_db)
                        new = {x: d.get(x) for x in lcols}
                        plan["inserts"].append([t, key, {x: d[x] for x in lcols if x in d}])
                        o.count(t, s, None, "rows_C3-TAKEN")
                        dig_after.add(_bucket(t, new, allow), lcols, new)
                    else:
                        o.row(t, key, s, "C3", "not_taken", json.dumps(fl), json.dumps(d, default=_jdefault), d_db)
                        o.count(t, s, None, "rows_C3")
                    continue
                after = dict(l)
                for col in lcols + [x for x in dcols if x not in ldecl]:
                    if col == pk:
                        continue
                    if col not in dcols:
                        if l.get(col) is not None:
                            o.count(t, s, col, "c5_absent_local_nonnull")
                        continue
                    if col not in ldecl:
                        if d.get(col) is not None:
                            o.count(t, s, col, "d1_only_column_nonnull")
                        continue
                    kind = kinds[col]
                    lv, dv = l.get(col), d.get(col)
                    sub = []
                    if isinstance(lv, bytes) and not isinstance(dv, bytes):
                        sub.append("LOCAL-INVALID-UTF8")
                        stats["local_invalid_utf8"] += 1
                    eq = compare(kind, lv, dv)
                    if eq == RAW:
                        o.count(t, s, col, "equal_raw")
                        continue
                    if eq == NORM and not (unsafe_number(lv) or unsafe_number(dv)):
                        o.count(t, s, col, "equal_norm")
                        continue
                    if s == C6_SOURCE and (key, col) not in c7.v7_exempt:
                        stats["c6_cells"] += 1                       # V7: BEFORE the classes (review M6)
                    if dv == WITHHELD or allow.names_gated(lv, dv):  # a value names a gated id: local kept, and
                        stats["withheld_values"] = True              # only the YES line says it exists (R888)
                        continue
                    if t == "license" or (t, col) in LICENCE_REFS:
                        cls, dec = "LIC-HELD", HELD
                        stats["licence_held"] += 1
                        if t == "license" and col in LICENCE_FLAGS:
                            sub.append(licence_flag_direction(col, lv, dv))
                        elif t == "license":
                            sub.append("licence text")
                        else:
                            sub.append(licence_ref_direction(lv, dv, flags_local, flags_d1))
                    elif kind == "numeric" and (unsafe_number(lv) or unsafe_number(dv)):
                        cls, dec = "ROUNDTRIP", LOCAL
                        stats["roundtrip"] += 1
                    elif t == "series" and (c7.has(key, col, s) or l.get("_mchanged")):
                        cls, dec = "C7", LOCAL
                    elif s is not None and colstats.get((d_db, t, s, col), (1, 1))[1] == 0:
                        cls, dec = "C5", LOCAL
                    elif s is not None and runloc.get(s) == "local":
                        cls, dec = "C2", LOCAL
                    elif s == C6_SOURCE:
                        cls, dec = "C6", VERIFY
                    elif (s, t, col) in c1:
                        cls, dec = ("C1-GAP", LOCAL) if s in c7.gaps else ("C1", TAKE_D1)
                    elif lv is None:
                        cls, dec = "LOCAL-NULL", LOCAL
                    else:
                        cls, dec = "DEFAULT", LOCAL
                    if t == "series" and col == "title" and isinstance(lv, str) and clean_title(lv) == dv:
                        sub.append("TITLE-CLEAN-ONLY")
                    if t == "series" and manifest is not None:
                        sub.append("MANIFEST-LOCAL-CHANGED" if l.get("_mchanged") else "MANIFEST-LOCAL-UNCHANGED")
                    o.cell(t, key, s, col, cls, dec, _snapval(lv), _snapval(dv), ",".join(sub))
                    o.count(t, s, col, f"cells_{cls}")
                    o.count(t, s, col, f"decision_{dec}")
                    if dec == TAKE_D1:
                        plan["cells"].append([t, key, col, kind, lv, dv, cls])
                        after[col] = dv
                after.pop("_mchanged", None)
                dig_after.add(_bucket(t, after, allow), lcols, after)
        local_count = con.execute("SELECT COUNT(*) FROM series").fetchone()[0]
        if local_count != streamed:
            raise Failed("local series: COUNT(*) differs from the rows the ordered read saw - a row with a NULL or "
                         "non-text key was missed")
    finally:
        o.close({"stats": stats, "utc": utcnow()})
        con.close()
        for sc in snaps.values():
            if sc is not None:
                sc.close()
    return {"plan": plan, "stats": stats, "digests": {"before": dig_before.result(), "after": dig_after.result()}}


# ---- summary ---------------------------------------------------------------------------------------------------

SUMMARY_METRICS = ("cells_kept_local", "cells_taken_d1", "cells_verify_C6", "cells_held_licence",
                   "rows_C3_not_taken", "rows_C3_taken", "rows_C4_kept", "rows_C4_deleted", "rows_C4_del_both",
                   "equal_raw", "equal_after_normalisation", "cells_C7", "cells_C5", "cells_C2", "cells_C1",
                   "cells_C1-GAP", "cells_LOCAL-NULL", "cells_DEFAULT", "cells_LIC-HELD", "cells_ROUNDTRIP")
_METRIC_MAP = {"decision_local": "cells_kept_local", "decision_d1": "cells_taken_d1", "decision_verify":
               "cells_verify_C6", "decision_held": "cells_held_licence", "rows_C3": "rows_C3_not_taken",
               "rows_C3-TAKEN": "rows_C3_taken", "rows_C4": "rows_C4_kept", "rows_C4-DEL": "rows_C4_deleted",
               "rows_C4-DEL-BOTH": "rows_C4_del_both", "equal_norm": "equal_after_normalisation"}


def summarise(out_dir: str, result: dict, c7: C7, inputs: dict, compare_to: str | None, allow: Allow,
              dropped: dict | None = None) -> dict:
    db = sqlite3.connect(os.path.join(out_dir, "classify.sqlite"))  # plain-open: the tool's own result file
    try:
        per: dict[str, dict] = {}

        def add(src, m, n):
            d = per.setdefault(src if src is not None else "(licence table)", {k: 0 for k in SUMMARY_METRICS})
            d[m] = d.get(m, 0) + n
        for _tbl, src, _col, metric, n in db.execute("SELECT tbl, source, col, metric, n FROM agg"):
            add(src, _METRIC_MAP.get(metric, metric), n)
        totals = {k: sum(d.get(k, 0) for d in per.values()) for k in SUMMARY_METRICS}
        held = []
        for tbl, src, col, sub, n in db.execute(
                "SELECT tbl, source, col, sublabels, COUNT(*) FROM cells WHERE cls='LIC-HELD' GROUP BY 1,2,3,4 "
                "ORDER BY 1,2,3,4"):
            held.append({"table": tbl, "source": src, "column": col, "direction": sub, "cells": n,
                         "audit": audit_line(src) if src else "see the licence row's sources"})
        held_rows = [dict(zip(("table", "key", "column", "local", "d1", "direction"), r)) for r in db.execute(
            "SELECT tbl, key, col, local_value, d1_value, sublabels FROM cells WHERE tbl='license' AND cls='LIC-HELD' "
            "ORDER BY 2, 3")]
        for x in held_rows:
            x["audit"] = audit_line(x["key"])
        suspects = [dict(zip(("table", "source", "column", "cells"), r)) for r in db.execute(
            "SELECT tbl, source, col, COUNT(*) FROM cells WHERE sublabels LIKE '%MANIFEST-LOCAL-UNCHANGED%' "
            "GROUP BY 1,2,3 ORDER BY 1,2,3")]
        c5 = [dict(zip(("table", "source", "column", "cells"), r)) for r in db.execute(
            "SELECT tbl, source, col, COUNT(*) FROM cells WHERE cls='C5' GROUP BY 1,2,3 ORDER BY 1,2,3")]
        c5_absent = [dict(zip(("table", "source", "column", "local_nonnull"), r)) for r in db.execute(
            "SELECT tbl, source, col, n FROM agg WHERE metric='c5_absent_local_nonnull' ORDER BY 1,2,3")]
        local_null = [dict(zip(("table", "source", "column", "cells"), r)) for r in db.execute(
            "SELECT tbl, source, col, COUNT(*) FROM cells WHERE cls='LOCAL-NULL' GROUP BY 1,2,3 ORDER BY 1,2,3")]
        c1_gap = [dict(zip(("table", "key", "column"), r)) for r in db.execute(
            "SELECT tbl, key, col FROM cells WHERE cls='C1-GAP' ORDER BY 1,2,3")]
        failed_rows = [dict(zip(("table", "key"), r)) for r in db.execute(
            "SELECT tbl, key FROM rows WHERE cls='C4-DEL-BOTH' ORDER BY 1,2")]
        s = {"step": "classify", "utc": utcnow(), "per_source": dict(sorted(per.items())), "totals": totals,
             "stats": result["stats"], "licence_held": held, "licence_held_rows": held_rows,
             "d1_side_change_suspects": suspects, "c5_columns": c5, "c5_absent_columns": c5_absent,
             "local_null_columns": local_null,
             "c7_gaps": {k: v for k, v in sorted(c7.gaps.items()) if k in allow.sources},
             "c1_gap_cells": c1_gap, "c4_del_both_failed": failed_rows,
             "c7_runs": [r for r in c7.runs if all(x in allow.sources for x in r["sources"])],
             "c7_runs_outside_the_allow_list": "YES" if any(not all(x in allow.sources for x in r["sources"])
                                                            for r in c7.runs) else "NO",
             "c7_sizes": {"ids": len(c7.ids), "cells": len(c7.cells),
                          "sources": sorted(x for x in c7.srcs if x in allow.sources)},
             "outside_the_allow_list_dropped": dropped or {}, "inputs": inputs,
             "plan_counts": {k: len(v) for k, v in result["plan"].items()},
             "classify_db": os.path.join(out_dir, "classify.sqlite"),
             "caveats": [
                 "LICENCE cells are HELD: never applied by this tool in either direction; each is decided per row "
                 "under DATABASE_LICENSES_VERBATIM.md at owner item 36, outside this tool, and S12b waits for item "
                 "36 (coordinator 2026-11-28 on H3; R490 rule 3; R86/R95).",
                 "R1304: E:'s pending_catalog_sync.txt was emptied on 2026-09-30T03:47Z by a dry run (54,619 lines, "
                 "36,765 unique ids, no copy); the S4c copy holds only ids appended after that moment.",
                 "The manifest (catalog_sync_sent.db) was seeded on 2026-08-31 by COUNTS only: an unchanged row "
                 "proves 'local unchanged since the seed or the last send', never 'D1 holds the recorded content'.",
                 "The dry run reads the 11-16 local copy; the statcan lane merges, the 2026-11-27 six-cube run, the "
                 "11-17 sec_edgar sync, S5's Road B and the S4c queue are only in S11 (R3-M5).",
                 "The allow-list was applied (the local catalogue's sources minus the worker gate, denylist.ts "
                 f"sha256 {inputs.get('gate_sha256')}); nothing outside it is compared, written, counted or named."]}
        if compare_to:
            s["side_by_side"] = _side_by_side(compare_to, s, db, allow)
    finally:
        db.close()
    write_json(os.path.join(out_dir, "summary.json"), s)
    with open(os.path.join(out_dir, "summary.md"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(render_md(s))
    return s


def _side_by_side(compare_to: str, now: dict, db, allow: Allow) -> dict:
    """The dry run's numbers RE-FILTERED by today's allow-list (review M1: a source gated since the dry run is not
    named here), then dry run | S11 | delta, and the two S12 lists."""
    with open(compare_to, encoding="utf-8") as fh:
        dry = json.load(fh)
    keep = {k: v for k, v in (dry.get("per_source") or {}).items() if k in allow.sources or k == "(licence table)"}
    dry_tot = {k: sum(v.get(k, 0) for v in keep.values()) for k in SUMMARY_METRICS}
    out = {"dry_run_summary": compare_to,
           "totals": {k: {"dry_run": dry_tot[k], "s11": now["totals"].get(k, 0),
                          "delta": now["totals"].get(k, 0) - dry_tot[k]} for k in SUMMARY_METRICS}}
    oc1, oc3 = set(), set()
    old_db = dry.get("classify_db")
    if old_db and os.path.exists(old_db):
        old = sqlite3.connect(ro_uri(old_db), uri=True)  # plain-open: the dry run's result file, read-only
        try:
            oc1 = {(r[0], r[1], r[2]) for r in old.execute("SELECT tbl, key, col FROM cells WHERE cls='C1'")}
            oc3 = {(r[0], r[1]) for r in old.execute("SELECT tbl, key FROM rows WHERE cls='C3-TAKEN'")}
        finally:
            old.close()
    else:
        out["note"] = "the dry run's classify.sqlite is not readable here: every C1 cell and C3-TAKEN row is NEW"
    out["c1_cells_new_since_dry_run"] = [list(k) for k in db.execute("SELECT tbl, key, col FROM cells WHERE cls='C1'")
                                         if tuple(k) not in oc1]
    out["c3_taken_rows_new_since_dry_run"] = [list(k) for k in db.execute(
        "SELECT tbl, key FROM rows WHERE cls='C3-TAKEN'") if tuple(k) not in oc3]
    return out


def render_md(s: dict) -> str:
    L = ["# Catalogue reconcile - the 6c summary", "", f"UTC {s['utc']}. Plan writes: {s['plan_counts']}.", ""]
    L += [f"- {c}" for c in s["caveats"]]
    L += [f"- D1 held rows outside the allow-list (dropped on receipt, not named or counted): {v} ({k})"
          for k, v in (s.get("outside_the_allow_list_dropped") or {}).items()]
    L += [f"- a differing value or a D1-only row names a gated id (kept local, not listed): "
          f"{'YES' if s['stats'].get('withheld_values') else 'NO'}"]
    L += ["", "## Licence cells - HELD (decided at owner item 36 under the verbatim audit)", ""]
    L += [f"- {x['table']}.{x['column']} on {x['source'] or '(licence table)'}: {x['cells']:,} cell(s), {x['direction']}"
          f" - {x['audit']}" for x in s["licence_held"]] or ["- none"]
    L += [f"  - licence row {x['key']} {x['column']}: local {x['local']!r}, D1 {x['d1']!r} ({x['direction']}) - "
          f"{x['audit']}" for x in s["licence_held_rows"]]
    L += ["", "## D1-side change suspects (manifest: local unchanged, yet D1 differs)", ""]
    L += [f"- {x['table']}.{x['column']} on {x['source']}: {x['cells']:,}" for x in s["d1_side_change_suspects"]] \
        or ["- none (or no manifest copy in this run)"]
    L += ["", "## Totals", "", "| metric | n |", "|---|---:|"]
    L += [f"| {k} | {v:,} |" for k, v in s["totals"].items()]
    if s.get("side_by_side"):
        L += ["", "## Dry run vs S11", "", "| metric | dry run | S11 | delta |", "|---|---:|---:|---:|"]
        L += [f"| {k} | {v['dry_run']:,} | {v['s11']:,} | {v['delta']:+,} |" for k, v in s["side_by_side"]["totals"].items()]
        L += ["", f"C1 cells new since the dry run: {len(s['side_by_side']['c1_cells_new_since_dry_run'])}; "
              f"C3-TAKEN rows new since the dry run: {len(s['side_by_side']['c3_taken_rows_new_since_dry_run'])}"]
    cols = ("cells_kept_local", "cells_taken_d1", "cells_verify_C6", "cells_held_licence", "rows_C3_not_taken",
            "rows_C3_taken", "rows_C4_kept", "rows_C4_deleted", "equal_after_normalisation", "cells_C7", "cells_C5",
            "cells_LOCAL-NULL")
    L += ["", "## Per source (sources with any difference)", "", "| source | " + " | ".join(cols) + " |",
          "|---|" + "---:|" * len(cols)]
    for src, d in s["per_source"].items():
        if any(d.get(c, 0) for c in cols if c != "equal_after_normalisation"):
            L.append(f"| {src} | " + " | ".join(f"{d.get(c, 0):,}" for c in cols) + " |")
    L += ["", "## FAILED: item-17 ids still on BOTH sides (B31 incomplete)", ""]
    L += [f"- {x['table']} {x['key']}" for x in s["c4_del_both_failed"]] or ["- none"]
    L += ["", "## C7 gaps (runs with no receipt)", ""]
    L += [f"- {src}: " + "; ".join(f"{r['writer']} {r['utc']} ({r['booking']})" for r in runs)
          for src, runs in s["c7_gaps"].items()] or ["- none"]
    L += ["", "## C5 columns (D1 never carried them)", ""]
    L += [f"- {x['table']}.{x['column']} on {x['source']}: {x['cells']:,} cell(s)" for x in s["c5_columns"]]
    L += [f"- {x['table']}.{x['column']} absent from D1's table on {x['source']}: {x['local_nonnull']:,} local value(s)"
          for x in s["c5_absent_columns"]] or ["- none"]
    L += ["", "## Local NULL, D1 value (not taken)", ""]
    L += [f"- {x['table']}.{x['column']} on {x['source']}: {x['cells']:,}" for x in s["local_null_columns"]] or ["- none"]
    st = s["stats"]
    L += ["", "## Row flags and checks", "", f"- C3 rows with a served object: {st['c3_served']:,}",
          f"- C3 rows with local-removal evidence (a C7 id): {st['c3_local_removal']:,}",
          f"- C3-TAKEN rows whose parent source/licence row is missing locally: {st['c3_parent_missing']:,}",
          f"- misplaced shard rows: {st['misplaced']:,}; duplicates across the two databases: {st['duplicate']:,}",
          f"- V7 (sec_edgar level, before classes): {st['c6_cells']:,} differing cell(s), {st['c6_rows']:,} row(s)",
          f"- numbers the D1 answer cannot carry exactly (ROUNDTRIP): {st['roundtrip']:,}",
          f"- local cells that are not valid UTF-8: {st['local_invalid_utf8']:,}", ""]
    return "\n".join(L) + "\n"


def _inputs_record(paths: dict) -> dict:
    return {k: ({"path": v, "sha256": file_sha256(v)} if v and os.path.isfile(v) else None) for k, v in paths.items()}


def cmd_classify(a) -> int:
    refuse_e_checkout()
    out = guard_out(a.out)
    catalogue = guard_catalogue(a.catalogue)
    for k in ("snapshot_dir", "c7_queue", "s4c_receipt", "c7_runs", "receipts_dir", "approved_deletes", "manifest",
              "compare_to"):
        guard_input(getattr(a, k), f"--{k.replace('_', '-')}")
    for k in ("c1", "c3", "c7_hand", "writers"):
        guard_input(getattr(a, k), f"--{k.replace('_', '-')}", own_list=True)
    guard_input(a.served_index, "--served-index")
    if cutover.is_cut_over() and not (a.c7_queue and a.s4c_receipt and a.c7_runs):
        raise Refused("refused: after T0 the S4c queue copy (--c7-queue), its S4c receipt (--s4c-receipt) and the "
                      "reviewed run list (--c7-runs) are required")
    snapshots = {db: os.path.join(a.snapshot_dir, f"d1_{db}.sqlite") for db in DATABASES}
    for db, p in snapshots.items():
        if not os.path.isfile(p):
            raise Refused(f"refused: no D1 snapshot for {db} in --snapshot-dir (run read-d1 first)")
    gate = load_gate()
    runloc, reg_sha = load_run_locations()
    shard = load_shard_map()
    c1 = load_c1(a.c1)
    c3n = load_c3(a.c3)
    allow = _allow_from_catalogue(catalogue, gate, c3n)
    for p in snapshots.values():                                        # licences referenced on EITHER side (H1)
        allow.licence_refs |= d1_licence_refs(p)
    allow.licence_refs = {x for x in allow.licence_refs if not allow.names_gated(x)}
    c7 = C7()
    queue_info = load_queue(c7, a.c7_queue, a.s4c_receipt) if a.c7_queue else None
    if not a.c7_queue:
        c7.notes.append("C7(a): no queue copy in this run (the dry run does not have it - ruling k)")
    if a.c7_runs:
        load_c7_runs(c7, a.c7_runs, a.receipts_dir, a.writers)
    if a.c7_hand:
        load_c7_hand(c7, a.c7_hand)
    deletes = set(load_ids(a.approved_deletes)) if a.approved_deletes else set()
    manifest = Manifest(a.manifest) if a.manifest else None
    served = ServedIndex(a.served_index) if a.served_index else None
    utc0 = utcnow()
    say("classify: hashing the catalogue ...")
    cat_sha = file_sha256(catalogue)
    result = classify(catalogue=catalogue, snapshots=snapshots, out_dir=out, allow=allow, runloc=runloc, shard=shard,
                      c1=c1, c3n=c3n, c7=c7, deletes=deletes, manifest=manifest, served=served)
    if file_sha256(catalogue) != cat_sha:
        raise Failed("the catalogue changed while it was classified - nothing can be planned from this read")
    inputs = _inputs_record({"c1": a.c1, "c3": a.c3, "c7_runs": a.c7_runs, "c7_hand": a.c7_hand, "writers": a.writers,
                             "approved_deletes": a.approved_deletes, "manifest": a.manifest,
                             "s4c_receipt": a.s4c_receipt})
    inputs.update(gate_sha256=gate_sha(), registry_sha256=reg_sha, queue=queue_info, c7_notes=c7.notes,
                  allow_list_sha256=allow.digest(), licence_refs=sorted(allow.licence_refs))
    st = result["stats"]
    plan = {"version": 2, "created_utc": utcnow(), "classify_started_utc": utc0,
            "tool_sha256": file_sha256(os.path.abspath(__file__)), "cut_over_at_classify": cutover.is_cut_over(),
            "catalogue": {"path": os.path.abspath(catalogue), "sha256": cat_sha, "size": os.path.getsize(catalogue)},
            "snapshots": {db: {"path": os.path.abspath(p), "sha256": file_sha256(p)} for db, p in snapshots.items()},
            "inputs": inputs, "writes": result["plan"], "digests": result["digests"],
            "counts": {k: st[k] for k in ("c6_cells", "c6_rows", "del_both", "del_withheld", "c3_parent_missing",
                                          "licence_held")}
            | {k: len(v) for k, v in result["plan"].items()}}
    plan_path = os.path.join(out, "plan.json")
    write_json(plan_path, plan)
    sha = file_sha256(plan_path)
    with open(plan_path + ".sha256", "w", encoding="utf-8", newline="\n") as fh:
        fh.write(sha + "\n")
    s = summarise(out, result, c7, inputs, a.compare_to, allow, dropped_flags(snapshots))
    say(f"classify: plan sha256 {sha}; writes {plan['counts']}; summary in --out")
    say("totals: " + json.dumps(s["totals"]))
    rc = 0
    if st["del_withheld"]:
        say("FAILED: an item-17 id cannot be planned (a value in it names a gated id): YES - for the R888 process")
        rc = 1
    if st["del_both"]:
        say(f"FAILED: {st['del_both']} item-17 id(s) are still on BOTH sides (B31 incomplete; listed in the summary)")
        rc = 1
    if cutover.is_cut_over() and (st["c6_cells"] or st["c6_rows"]):
        say(f"FAILED (V7): after T0 sec_edgar must be level (Road B): {st['c6_cells']} differing cell(s), "
            f"{st['c6_rows']} sec_edgar row(s) only in D1")
        rc = 1
    return rc


# ---- the writes (rehearse / apply), the FTS rebuild, verify -------------------------------------------------

def load_plan(path: str, sha: str) -> dict:
    if not sha or file_sha256(path) != sha.strip().lower():
        raise Refused("refused: the plan's sha256 is not --plan-sha (the plan is pinned)")
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _same_before(a, b) -> bool:
    return raw_equal(_unj(a), _unj(b))


def preflight_writes(con, plan: dict, apply: bool = True) -> None:
    """Every planned write checked BEFORE the first one (reviews M8, M9; R346): no item-17 id on both sides, no held
    licence cell among the writes, every row to insert has its parent rows locally, and - at S11 (apply) - no V7
    difference (before Road B the dry run's rehearsal expects sec_edgar differences and rehearses them)."""
    c = plan["counts"]
    checks = [("del_both", "item-17 ids are on both sides"), ("del_withheld", "an item-17 id cannot be planned"),
              ("c3_parent_missing", "rows to insert lack their parent source/licence rows (R346)")]
    if apply:
        checks += [("c6_cells", "sec_edgar (V7) differs"),
                   ("c6_rows", "sec_edgar rows are only in D1 (Road B did not finish)")]
    for k, why in checks:
        if c.get(k):
            raise Refused(f"refused before any write: {why}")
    for t, _key, _col, _kind, _b, _a, cls in plan["writes"]["cells"]:
        if cls == "LIC-HELD" or t == "license":
            raise Refused("refused before any write: the plan writes a licence cell (licence cells are HELD)")
    src = {r[0] for r in con.execute("SELECT source_id FROM source")}
    lic = {r[0] for r in con.execute("SELECT license_id FROM license")}
    for _t, key, row in plan["writes"]["inserts"]:
        if row.get("source_id") not in src or row.get("license_id") not in lic:
            raise Refused(f"refused before any write: {h(key)} lacks its parent source/licence row (R346)")


def apply_writes(con, plan: dict, out_dir: str, label: str) -> dict:
    """The planned writes in chunks of WRITE_CHUNK, each chunk one IMMEDIATE transaction, each row re-read and
    compared with the plan's before value first. The plan (with every before value) is on disk before this runs."""
    preflight_writes(con, plan, apply=(label == "apply"))
    w = plan["writes"]
    done = {"cells": 0, "inserts": 0, "deletes": 0}
    rec = {"label": label, "utc_start": utcnow(), "done": done}
    write_json(os.path.join(out_dir, f"{label}_before.json"), {"utc": utcnow(), "writes": w})

    def run(items, body, what):
        for k in range(0, len(items), WRITE_CHUNK):
            part = items[k:k + WRITE_CHUNK]
            con.execute("BEGIN IMMEDIATE")
            try:
                for it in part:
                    body(it)
            except BaseException:
                con.execute("ROLLBACK")
                raise
            con.execute("COMMIT")
            done[what] += len(part)

    def cell(it):
        t, key, col, _kind, before, after, _cls = it
        r = con.execute(f'SELECT "{col}" FROM "{t}" WHERE "{PK[t]}" = ?', (key,)).fetchone()
        if r is None or not _same_before(r[0], before):
            raise Refused(f"refused: {t} {h(key)} {col} changed since the plan was made")
        if con.execute(f'UPDATE "{t}" SET "{col}" = ? WHERE "{PK[t]}" = ?', (_unj(after), key)).rowcount != 1:
            raise Refused(f"refused: the UPDATE of {t} {h(key)} matched no row")

    def ins(it):
        t, key, row = it
        if con.execute(f'SELECT 1 FROM "{t}" WHERE "{PK[t]}" = ?', (key,)).fetchone():
            raise Refused(f"refused: {t} {h(key)} appeared locally since the plan was made")
        cols = list(row)
        con.execute(f'INSERT INTO "{t}" ({", ".join(chr(34) + c + chr(34) for c in cols)}) VALUES '
                    f'({", ".join("?" * len(cols))})', [_unj(row[c]) for c in cols])

    def dele(it):
        t, key, row = it
        cur = con.execute(f'SELECT * FROM "{t}" WHERE "{PK[t]}" = ?', (key,))
        names = [d[0] for d in cur.description]
        now = cur.fetchone()
        if now is None or any(not _same_before(now[i], row.get(n)) for i, n in enumerate(names)):
            raise Refused(f"refused: {t} {h(key)} changed since the plan was made (delete)")
        if con.execute(f'DELETE FROM "{t}" WHERE "{PK[t]}" = ?', (key,)).rowcount != 1:
            raise Refused(f"refused: the DELETE of {t} {h(key)} matched no row")
    try:
        run(w["cells"], cell, "cells")
        run(w["inserts"], ins, "inserts")
        run(w["deletes"], dele, "deletes")
    finally:
        rec["utc_end"] = utcnow()
        write_json(os.path.join(out_dir, f"{label}_writes.json"), rec)
    return rec


class Sampler(threading.Thread):
    """Every `every` seconds: the size of <db>-journal (its peak) and the free space of the db's drive (its min)."""

    def __init__(self, db_path: str, every: float = 1.0):
        super().__init__(daemon=True)
        self.journal, self.dir, self.every = db_path + "-journal", os.path.dirname(os.path.abspath(db_path)), every
        self.peak, self.free_min, self.samples = 0, None, 0
        self._stop_ev = threading.Event()

    def run(self):
        while not self._stop_ev.is_set():
            self.sample()
            self._stop_ev.wait(self.every)

    def sample(self):
        try:
            self.peak = max(self.peak, os.path.getsize(self.journal))
        except OSError:
            pass
        free = shutil.disk_usage(self.dir).free
        self.free_min = free if self.free_min is None else min(self.free_min, free)
        self.samples += 1

    def stop(self):
        self._stop_ev.set()
        self.join()
        self.sample()


def _peak_rss() -> int | None:
    try:
        if os.name == "nt":
            import ctypes                                               # noqa: PLC0415
            from ctypes import wintypes                                 # noqa: PLC0415

            class PMC(ctypes.Structure):
                _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            hproc = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(hproc, ctypes.byref(pmc), pmc.cb):
                return int(pmc.PeakWorkingSetSize)
            return None
        import resource                                                 # noqa: PLC0415
        return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024
    except Exception:                                                   # noqa: BLE001 - best effort, reported None
        return None


def fts_rebuild(con, form: str) -> None:
    """series_fts from `series`, in ONE explicit transaction on a connection with isolation_level=None (review M2).
    "rebuild_fts" = the core catalogue module's rebuild_fts (the same two statements, pinned by the tests);
    "drop_create" = DROP, re-CREATE from the DDL captured from sqlite_master, INSERT ... SELECT, and the new DDL
    text must equal the captured text byte for byte before COMMIT."""
    if form not in FTS_FORMS:
        raise Refused(f"refused: unknown FTS form {form!r}")
    if con.isolation_level is not None:
        raise Refused("refused: the FTS rebuild needs a connection with isolation_level=None (explicit transaction)")
    ddl = con.execute("SELECT sql FROM sqlite_master WHERE name='series_fts'").fetchone()
    if not ddl or not ddl[0]:
        raise Refused("refused: no series_fts DDL in this catalogue")
    con.execute("BEGIN IMMEDIATE")
    try:
        if form == "rebuild_fts":
            con.execute(FTS_DELETE)
            con.execute(FTS_INSERT)
        else:
            con.execute("DROP TABLE series_fts")
            con.execute(ddl[0])
            if con.execute("SELECT sql FROM sqlite_master WHERE name='series_fts'").fetchone()[0] != ddl[0]:
                raise Failed("the re-created series_fts DDL differs from the captured text")
            con.execute(FTS_INSERT)
    except BaseException:
        con.execute("ROLLBACK")
        raise
    con.execute("COMMIT")


def timed_fts(con, db_path: str, form: str, every: float) -> dict:
    s = Sampler(db_path, every)
    st0 = os.stat(db_path)
    pragmas = {p: con.execute(f"PRAGMA {p}").fetchone()[0] for p in ("journal_mode", "page_size", "page_count",
                                                                     "freelist_count")}
    s.start()
    t0, c0 = time.monotonic(), time.process_time()
    utc0 = utcnow()
    try:
        fts_rebuild(con, form)
    finally:
        s.stop()
    after = {p: con.execute(f"PRAGMA {p}").fetchone()[0] for p in ("page_count", "freelist_count")}
    rec = {"form": form, "utc_start": utc0, "utc_end": utcnow(), "elapsed_s": round(time.monotonic() - t0, 3),
           "cpu_s": round(time.process_time() - c0, 3), "journal_peak_bytes": s.peak, "free_min_bytes": s.free_min,
           "samples": s.samples, "peak_rss_bytes": _peak_rss(), "before": pragmas, "after": after,
           "db_bytes_before": st0.st_size, "db_bytes_after": os.stat(db_path).st_size,
           "growth_bytes": max(0, os.stat(db_path).st_size - st0.st_size),
           "drive": os.path.splitdrive(os.path.abspath(db_path))[0]}
    rec["series_rows"] = con.execute("SELECT COUNT(*) FROM series").fetchone()[0]
    rec["series_fts_rows"] = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
    return rec


def probe_queries(con, queries: list[str] | None, n: int = 20) -> list[str]:
    if queries:
        return list(queries)
    words, seen = [], set()
    for (title,) in con.execute("SELECT title FROM series ORDER BY series_id LIMIT 1000"):
        for w in re.findall(r"[A-Za-z]{5,}", title if isinstance(title, str) else ""):
            if w.lower() not in seen:
                seen.add(w.lower())
                words.append(f'"{w}"')
            if len(words) >= n:
                return words
    return words


def match_sets(con, queries: list[str]) -> dict:
    return {q: sorted({r[0] for r in con.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH ?", (q,))})
            for q in queries}


def verify(con, plan: dict, allow: Allow) -> dict:
    """V1 every written cell equals its D1 value; V2 every inserted row is present and equal; V3 every deleted id is
    absent; V4 every bucket's row digest equals the plan's predicted after-digest (nothing else changed);
    V5 count(series_fts) == count(series)."""
    w = plan["writes"]
    bad = {"V1": [], "V2": [], "V3": [], "V4": [], "V5": []}
    for t, key, col, kind, _before, after, _cls in w["cells"]:
        r = con.execute(f'SELECT "{col}" FROM "{t}" WHERE "{PK[t]}" = ?', (key,)).fetchone()
        if r is None or compare(kind, r[0], _unj(after)) == DIFF:
            bad["V1"].append([t, key, col])
    for t, key, row in w["inserts"]:
        cur = con.execute(f'SELECT * FROM "{t}" WHERE "{PK[t]}" = ?', (key,))
        names = [d[0] for d in cur.description]
        now = cur.fetchone()
        if now is None or any(compare(column_kind(t, c, None, None), now[names.index(c)], _unj(v)) == DIFF
                              for c, v in row.items() if c in names):
            bad["V2"].append([t, key])
    for t, key, _row in w["deletes"]:
        if con.execute(f'SELECT 1 FROM "{t}" WHERE "{PK[t]}" = ?', (key,)).fetchone():
            bad["V3"].append([t, key])
    dig = Digests()
    con.text_factory = _text_or_bytes
    for t in TABLES:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{t}")')]
        for row in _local_iter(con, t, cols):
            dig.add(_bucket(t, row, allow), cols, row)
    got, want = dig.result(), plan["digests"]["after"]
    bad["V4"] = sorted(k for k in set(got) | set(want) if got.get(k) != want.get(k))
    n_s = con.execute("SELECT COUNT(*) FROM series").fetchone()[0]
    n_f = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
    if n_s != n_f:
        bad["V5"] = [n_s, n_f]
    return {"ok": not any(bad.values()), "failed": {k: len(v) for k, v in bad.items() if v},
            "failed_buckets": [b for b in bad["V4"] if b != "_withheld"],
            "withheld_bucket_changed": "_withheld" in bad["V4"], "series": n_s, "series_fts": n_f, "utc": utcnow()}


def _allow_for_plan(con, plan: dict, gate: set, c3n: set) -> Allow:
    allow = build_allow(con, gate, c3n)
    if allow.digest() != plan["inputs"].get("allow_list_sha256"):
        raise Refused("refused: the allow-list differs from the plan's (the catalogue or the gate changed)")
    allow.licence_refs = set(plan["inputs"].get("licence_refs") or [])   # the licences classify admitted
    return allow


def cmd_rehearse(a) -> int:
    cutover.refuse_if_cut_over("reconcile_catalogue rehearse - it writes a plain COPY, a before-T0 step")
    refuse_e_checkout()
    out = guard_out(a.out)
    path = os.path.abspath(a.catalogue)
    why = plain_copy_problem(path)
    if why:
        raise Refused(f"refused: the rehearse target: {why}")
    plan = load_plan(guard_input(a.plan, "--plan"), a.plan_sha)
    if _abs(plan["catalogue"]["path"]) != _abs(path) or file_sha256(path) != plan["catalogue"]["sha256"]:
        raise Refused("refused: the copy is not the one the plan was made from (path or sha256 differs)")
    gate = load_gate()
    if plan["inputs"].get("gate_sha256") != gate_sha():
        raise Refused("refused: the gate changed since the plan was made")
    c3n = load_c3(guard_input(a.c3, "--c3", own_list=True))
    queries = load_ids(guard_input(a.queries, "--queries")) if a.queries else None
    con = catalog_path.connect_path(path, write=True, timeout=300, isolation_level=None)
    con.text_factory = _text_or_bytes
    con.execute("PRAGMA busy_timeout=300000")
    rec = {"step": "rehearse", "utc_start": utcnow(), "plan_sha256": a.plan_sha}
    try:
        allow = _allow_for_plan(con, plan, gate, c3n)
        rec["writes"] = apply_writes(con, plan, out, "rehearse")
        rec["fts_A"] = timed_fts(con, path, "rebuild_fts", a.sample_every)
        qs = probe_queries(con, queries)
        res_a = match_sets(con, qs)
        rec["verify"] = verify(con, plan, allow)
        if a.fts_both:
            ddl_a = con.execute("SELECT sql FROM sqlite_master WHERE name='series_fts'").fetchone()[0]
            rec["fts_B"] = timed_fts(con, path, "drop_create", a.sample_every)
            res_b = match_sets(con, qs)
            ddl_b = con.execute("SELECT sql FROM sqlite_master WHERE name='series_fts'").fetchone()[0]
            diff_q = [q for q in qs if res_a[q] != res_b[q]]
            rec["fts_equivalence"] = {
                "equivalent": (not diff_q and ddl_a == ddl_b
                               and rec["fts_A"]["series_fts_rows"] == rec["fts_B"]["series_fts_rows"]
                               == rec["fts_B"]["series_rows"]),
                "queries": len(qs), "queries_differing": len(diff_q), "ddl_equal": ddl_a == ddl_b,
                "rows_A": rec["fts_A"]["series_fts_rows"], "rows_B": rec["fts_B"]["series_fts_rows"]}
    finally:
        con.close()
        rec["utc_end"] = utcnow()
        write_json(os.path.join(out, "rehearse.json"), rec)
    ok = rec.get("verify", {}).get("ok")
    say(f"rehearse: verify {'OK' if ok else 'FAILED'}; FTS {rec['fts_A']['elapsed_s']} s, journal peak "
        f"{rec['fts_A']['journal_peak_bytes']:,} B, growth {rec['fts_A']['growth_bytes']:,} B")
    return 0 if ok else 1


def cmd_apply(a) -> int:
    path = guard_catalogue(None, for_apply=True)
    out = guard_out(a.out)
    if catalog_path._held is None:                                      # noqa: SLF001 - the lock this process holds
        raise Refused("refused: apply needs the catalogue writer lock held by this process (its __main__ takes it)")
    plan = load_plan(guard_input(a.plan, "--plan"), a.plan_sha)
    if _abs(plan["catalogue"]["path"]) != _abs(path):
        raise Refused("refused: the plan was made from another file, not the build")
    if not plan.get("cut_over_at_classify"):
        raise Refused("refused: the plan was classified before the flag; S11 needs a classify after S10")
    for db, snap in plan["snapshots"].items():
        if not os.path.isfile(snap["path"]) or file_sha256(snap["path"]) != snap["sha256"]:
            raise Refused(f"refused: the {db} D1 snapshot differs from the plan's")
    q = (plan["inputs"] or {}).get("queue")
    if not q or not os.path.isfile(q["path"]) or file_sha256(q["path"]) != q["sha256"]:
        raise Refused("refused: the queue copy differs from the plan's (or the plan has none)")
    gate = load_gate()
    if plan["inputs"].get("gate_sha256") != gate_sha():
        raise Refused("refused: the gate changed since the plan was made")
    c3n = load_c3(guard_input(a.c3, "--c3", own_list=True))
    with open(guard_input(a.fts_measure, "--fts-measure"), encoding="utf-8") as fh:
        reh = json.load(fh)
    need = int(reh["fts_A"]["journal_peak_bytes"]) + int(reh["fts_A"].get("growth_bytes", 0))
    if a.fts_form == "drop_create":
        if not (reh.get("fts_equivalence") or {}).get("equivalent"):
            raise Refused("refused: drop_create needs a rehearsal that proved it equivalent (Q7, M2)")
        need = max(need, int(reh["fts_B"]["journal_peak_bytes"]) + int(reh["fts_B"].get("growth_bytes", 0)))
    free = shutil.disk_usage(os.path.dirname(path)).free
    if free < 1.5 * need:
        raise Refused(f"refused: {free:,} B free on the build's drive, under 1.5 x (journal peak + file growth) "
                      f"= 1.5 x {need:,} B")
    say("apply: hashing the build ...")
    if file_sha256(path) != plan["catalogue"]["sha256"]:
        raise Refused("refused: the build changed since the plan was made (sha256 differs)")
    con = catalog_path.connect_path(path, write=True, timeout=300, isolation_level=None)
    con.text_factory = _text_or_bytes
    con.execute("PRAGMA busy_timeout=300000")
    rec = {"step": "apply", "utc_start": utcnow(), "plan_sha256": a.plan_sha, "fts_form": a.fts_form}
    try:
        allow = _allow_for_plan(con, plan, gate, c3n)
        rec["writes"] = apply_writes(con, plan, out, "apply")
        rec["fts"] = timed_fts(con, path, a.fts_form, a.sample_every)
        rec["verify"] = verify(con, plan, allow)
    finally:
        con.close()
        rec["utc_end"] = utcnow()
        write_json(os.path.join(out, "apply.json"), rec)
    ok = rec.get("verify", {}).get("ok")
    say(f"apply: verify {'OK' if ok else 'FAILED ' + json.dumps(rec.get('verify', {}).get('failed'))}")
    return 0 if ok else 1


# ---- search: the 50-query comparison -------------------------------------------------------------------------

_QUERY_RX = re.compile(r'\s*("[A-Za-z0-9 ._-]+"\s*)+\Z')          # ASCII word tokens only (review L8)


class TextMatcher:
    """Does query q match a given (title, geography)? Decided by the same fts5 DDL on an in-memory table."""

    def __init__(self, ddl: str):
        self.mem = sqlite3.connect(":memory:")  # plain-open: in-memory one-row fts5 table for a MATCH check
        self.mem.execute(ddl)

    def matches(self, q: str, title, geography) -> bool:
        self.mem.execute("DELETE FROM series_fts")
        self.mem.execute("INSERT INTO series_fts(series_id, title, geography) VALUES ('x', ?, ?)",
                         (title if isinstance(title, str) else None, geography if isinstance(geography, str) else None))
        return self.mem.execute("SELECT COUNT(*) FROM series_fts WHERE series_fts MATCH ?", (q,)).fetchone()[0] > 0


def _snap_row(snaps: dict, key: str):
    for sc in snaps.values():
        if sc is not None:
            cur = sc.execute('SELECT * FROM "series" WHERE "series_id" = ?', (key,))
            r = cur.fetchone()
            if r is not None:
                return dict(zip([d[0] for d in cur.description], r))
    return None


def search_compare(con, queries: list[str], classify_db: str, budget: Budget, allow: Allow,
                   snapshots: dict | None = None, origin: list | None = None) -> dict:
    """Local index vs D1 for each query; every difference must be explained by the reconcile's own row diff, by a
    STALE D1 index row (the D1 answer's index text != its series text) or by a MISSING D1 index row (the D1 series
    text, read from the snapshot, matches but D1's MATCH did not return it) - else FAIL. With `origin` (the S12b
    generation's copies), the origin's answers must equal the build's."""
    ddl = con.execute("SELECT sql FROM sqlite_master WHERE name='series_fts'").fetchone()[0]
    tm = TextMatcher(ddl)
    cdb = sqlite3.connect(ro_uri(classify_db), uri=True)  # plain-open: the tool's own result file, read-only
    c3 = {k for (k,) in cdb.execute("SELECT key FROM rows WHERE tbl='series' AND cls='C3'")}
    c4 = {k for (k,) in cdb.execute("SELECT key FROM rows WHERE tbl='series' AND cls='C4'")}
    c4del = {k for (k,) in cdb.execute("SELECT key FROM rows WHERE tbl='series' AND cls='C4-DEL'")}
    textdiff = {}
    for k, col, lv, dv, dec in cdb.execute("SELECT key, col, local_value, d1_value, decision FROM cells "
                                           "WHERE tbl='series' AND col IN ('title','geography')"):
        textdiff.setdefault(k, {})[col] = (lv, dv, dec)
    cdb.close()
    snaps = {db: (sqlite3.connect(ro_uri(p, True), uri=True) if p and os.path.exists(p) else None)  # plain-open: own snapshot
             for db, p in (snapshots or {}).items()}
    origins = [sqlite3.connect(ro_uri(p), uri=True) for p in (origin or [])]  # plain-open: an origin COPY, read-only
    res = {"queries": [], "fail": 0, "d1_rows_read": 0}
    try:
        for q in queries:
            if not _QUERY_RX.match(q):
                raise Refused("refused: a query is not double-quoted ASCII word tokens")
            local = [r for r in con.execute(SEARCH_SQL, (q,)) if allow.series({"series_id": r[0]})]
            if len(local) > SEARCH_LOCAL_MAX:
                raise Refused(f"refused: a query matches {len(local):,} local rows (over {SEARCH_LOCAL_MAX:,})")
            d1rows = []
            for db in DATABASES:
                rows, rr, _ = d1(budget, db, SEARCH_SQL, [q], 3 * len(local) + 100, f"{db} search query")
                res["d1_rows_read"] += rr
                d1rows += [r for r in rows if allow.series({"series_id": r.get("series_id")})]
            lset = {r[0] for r in local}
            dset = {r["series_id"] for r in d1rows}
            dup = len(d1rows) - len(dset)
            d1_by = {r["series_id"]: r for r in d1rows}
            local_by = {r[0]: r for r in local}
            lines = []
            for sid in sorted(lset - dset):
                lt, lg = local_by[sid][3], local_by[sid][4]
                td = textdiff.get(sid, {})
                why = None
                if sid in c4:
                    why = "C4 (local only)"
                elif td and any(v[2] != TAKE_D1 for v in td.values()) and tm.matches(q, lt, lg) and not tm.matches(
                        q, td.get("title", (lt, lt))[1], td.get("geography", (lg, lg))[1]):
                    why = "row diff kept local (title/geography)"
                else:
                    dr = _snap_row(snaps, sid)
                    if dr is not None and tm.matches(q, dr.get("title"), dr.get("geography")):
                        why = "D1's own index is MISSING this row (its series text matches)"
                lines.append({"id": sid, "side": "local only", "expected": why is not None, "why": why or "UNEXPLAINED"})
            for sid in sorted(dset - lset):
                r = d1_by[sid]
                td = textdiff.get(sid, {})
                if sid in c3:
                    why = "C3 (D1 only, not taken)"
                elif sid in c4del:
                    why = "C4-DEL (deleted locally)"
                elif (r.get("fts_title"), r.get("fts_geo")) != (r.get("series_title"), r.get("series_geo")):
                    why = "D1's own index is STALE for this row (expected, listed; coordinator 2026-11-28, Q11)"
                elif td and tm.matches(q, r.get("series_title"), r.get("series_geo")) and not tm.matches(
                        q, td.get("title", (r.get("series_title"),))[0], td.get("geography", (r.get("series_geo"),))[0]):
                    why = "row diff kept local (title/geography)"
                else:
                    why = None
                lines.append({"id": sid, "side": "D1 only", "expected": why is not None, "why": why or "UNEXPLAINED"})
            origin_diff = None
            if origins:
                oset = set()
                for oc in origins:
                    oset |= {r[0] for r in oc.execute("SELECT series_id FROM series_fts WHERE series_fts MATCH ?", (q,))
                             if allow.series({"series_id": r[0]})}
                origin_diff = sorted(oset ^ lset)
            fails = [x for x in lines if not x["expected"]]
            res["fail"] += len(fails) + (len(origin_diff) if origin_diff else 0)
            res["queries"].append({"q": q, "local": len(lset), "d1": len(dset), "d1_duplicates": dup, "diffs": lines,
                                   "origin_differs": origin_diff, "pass": not fails and not origin_diff})
    finally:
        for sc in list(snaps.values()) + origins:
            if sc is not None:
                sc.close()
    res["pass"] = res["fail"] == 0
    return res


def cmd_search(a) -> int:
    refuse_e_checkout()
    out = guard_out(a.out)
    catalogue = guard_catalogue(a.catalogue)
    for k in ("queries", "classify_db", "snapshot_dir"):
        guard_input(getattr(a, k), f"--{k.replace('_', '-')}")
    for p in a.origin_copy or []:
        guard_input(p, "--origin-copy")
    budget = Budget(a.budget_rows, os.path.join(out, "budget.sqlite"))
    gate = load_gate()
    c3n = load_c3(guard_input(a.c3, "--c3", own_list=True))
    if D1_QUERY is None:
        say(ensure_token(a.env_file))
    con = catalog_path.connect_path(catalogue, write=False, timeout=300)
    con.text_factory = _text_or_bytes
    try:
        allow = build_allow(con, gate, c3n)
        snaps = {db: os.path.join(a.snapshot_dir, f"d1_{db}.sqlite") for db in DATABASES} if a.snapshot_dir else None
        res = search_compare(con, load_ids(a.queries), a.classify_db, budget, allow, snaps, a.origin_copy)
    finally:
        con.close()
    res.update(utc=utcnow(), rows_read_total=budget.spent, statements=budget.statements)
    write_json(os.path.join(out, "search_compare.json"), res)
    say(f"search: {len(res['queries'])} queries, {res['fail']} unexplained difference(s); {budget.spent:,} rows read")
    return 0 if res["pass"] else 1


# ---- copy, c7-candidates --------------------------------------------------------------------------------------

def cmd_copy(a) -> int:
    cutover.refuse_if_cut_over("reconcile_catalogue copy - the dry run's plain copy is a before-T0 step")
    out = guard_out(a.out)
    src, dst = os.path.abspath(a.src), os.path.abspath(a.dst)
    guard_input(src, "--from")
    if plain_copy_problem(dst) or os.path.exists(dst) or os.path.exists(dst + ".partial"):
        raise Refused("refused: --to exists, or is not a plain F: path")
    utc0, free0 = utcnow(), shutil.disk_usage(os.path.dirname(dst)).free
    sha_before = file_sha256(src)                                       # review L6: hashed before AND after
    hh = hashlib.sha256()
    with open(src, "rb") as fi, open(dst + ".partial", "wb") as fo:
        while True:
            b = fi.read(64 << 20)
            if not b:
                break
            hh.update(b)
            fo.write(b)
        fo.flush()
        os.fsync(fo.fileno())
    os.replace(dst + ".partial", dst)
    rec = {"step": "copy", "from": src, "to": dst, "utc_start": utc0, "utc_end": utcnow(),
           "size_from": os.path.getsize(src), "size_to": os.path.getsize(dst), "sha256_from_before": sha_before,
           "sha256_read": hh.hexdigest(), "sha256_from_after": file_sha256(src), "sha256_to": file_sha256(dst),
           "nlink_to": os.stat(dst).st_nlink, "free_before": free0,
           "free_after": shutil.disk_usage(os.path.dirname(dst)).free}
    write_json(os.path.join(out, "copy.json"), rec)
    if len({rec["sha256_from_before"], rec["sha256_read"], rec["sha256_from_after"], rec["sha256_to"]}) != 1 \
            or rec["size_from"] != rec["size_to"] or rec["nlink_to"] != 1:
        say("copy FAILED: the hashes, sizes or link count do not agree (see copy.json)")
        return 1
    say(f"copy: {rec['size_to']:,} B, sha256 {rec['sha256_to']} (source unchanged before/after), link count 1")
    return 0


_DATE_RX = re.compile(r"\b(20\d\d-\d\d-\d\d)")


def cmd_c7_candidates(a) -> int:
    out = guard_out(a.out)
    writers = [w["writer"] for w in read_tsv(guard_input(a.writers, "--writers", own_list=True),
                                             ("writer", "parser", "grain", "evidence"))]
    names = {os.path.basename(w): w for w in writers}
    cut = C7_CUTOFF[:10]
    rows = []
    for path in a.booking:
        guard_input(path, "--booking")
        with open(path, encoding="utf-8", errors="replace") as fh:
            for n, ln in enumerate(fh, 1):
                dates = [d for d in _DATE_RX.findall(ln) if d >= cut]
                hit = sorted(w for b, w in names.items() if b in ln)
                if dates and (hit or re.search(r"catalog", ln, re.I)):
                    rows.append([path, n, ",".join(sorted(set(dates))), ",".join(hit), ln.strip()[:300].replace("\t", " ")])
    p = os.path.join(out, "c7_candidates.tsv")
    with open(p, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("file\tline\tdates\twriters\ttext\n")
        for r in rows:
            fh.write("\t".join(str(x) for x in r) + "\n")
    say(f"c7-candidates: {len(rows)} line(s) (map each by hand into the reviewed run list)")
    return 0


# ---- the command line ------------------------------------------------------------------------------------------

def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    sub = ap.add_subparsers(dest="step", required=True)

    def common(p, d1_reads=False):
        p.add_argument("--out", required=True, help="the run folder (on F:)")
        p.add_argument("--c3", default=os.path.join(LISTS_DIR, "c3_writers.tsv"))
        if d1_reads:
            p.add_argument("--budget-rows", type=int, required=True, help="D1 rows read allowed, all runs (no default)")
            p.add_argument("--page-rows", type=int, default=DEFAULT_PAGE_ROWS)
            p.add_argument("--env-file", help="where the token is read in-process if the environment lacks it")

    def lists(p):
        p.add_argument("--c1", default=os.path.join(LISTS_DIR, "c1_columns.tsv"))
        p.add_argument("--c7-hand", default=os.path.join(LISTS_DIR, "c7_hand.tsv"))
        p.add_argument("--writers", default=os.path.join(LISTS_DIR, "writers.tsv"))
        p.add_argument("--c7-runs")
        p.add_argument("--receipts-dir")
        p.add_argument("--c7-queue")
        p.add_argument("--s4c-receipt")
        p.add_argument("--manifest")
        p.add_argument("--approved-deletes")
        p.add_argument("--served-index")
        p.add_argument("--compare-to")
    p = sub.add_parser("copy", allow_abbrev=False)
    common(p)
    p.add_argument("--from", dest="src", required=True)
    p.add_argument("--to", dest="dst", required=True)
    p = sub.add_parser("probe", allow_abbrev=False)
    common(p, True)
    p.add_argument("--catalogue")
    p = sub.add_parser("read-d1", allow_abbrev=False)
    common(p, True)
    p.add_argument("--catalogue")
    p.add_argument("--probe")
    p = sub.add_parser("classify", allow_abbrev=False)
    common(p)
    p.add_argument("--catalogue")
    p.add_argument("--snapshot-dir", required=True)
    lists(p)
    p = sub.add_parser("dry-run", allow_abbrev=False)
    common(p, True)
    p.add_argument("--catalogue")
    p.add_argument("--probe")
    lists(p)
    for name in ("rehearse", "apply"):
        p = sub.add_parser(name, allow_abbrev=False)
        common(p)
        p.add_argument("--plan", required=True)
        p.add_argument("--plan-sha", required=True)
        p.add_argument("--sample-every", type=float, default=1.0)
        if name == "rehearse":
            p.add_argument("--catalogue", required=True)
            p.add_argument("--fts-both", action="store_true")
            p.add_argument("--queries")
        else:
            p.add_argument("--fts-form", choices=FTS_FORMS, default="rebuild_fts")
            p.add_argument("--fts-measure", required=True, help="the dry run's rehearse.json")
    p = sub.add_parser("search", allow_abbrev=False)
    common(p, True)
    p.add_argument("--catalogue")
    p.add_argument("--queries", required=True)
    p.add_argument("--classify-db", required=True)
    p.add_argument("--snapshot-dir")
    p.add_argument("--origin-copy", nargs="*", help="the S12b origin generation's catalogue copies (local reads)")
    p = sub.add_parser("c7-candidates", allow_abbrev=False)
    common(p)
    p.add_argument("--writers", default=os.path.join(LISTS_DIR, "writers.tsv"))
    p.add_argument("--booking", nargs="+", required=True)
    return ap


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    a = _parser().parse_args(argv)
    if not argv or argv[0] != a.step:                                   # the lock rule reads argv[0] exactly
        say("refused: the step must be the first argument, spelled exactly")
        return 2
    steps = {"copy": cmd_copy, "probe": cmd_probe, "read-d1": cmd_read_d1, "classify": cmd_classify,
             "rehearse": cmd_rehearse, "apply": cmd_apply, "search": cmd_search, "c7-candidates": cmd_c7_candidates}
    try:
        if a.step == "dry-run":
            a.snapshot_dir = a.out
            rc = cmd_read_d1(a)
            return rc or cmd_classify(a)
        return steps[a.step](a)
    except Refused as e:
        say(str(e))
        return 2
    except Stop as e:
        say(str(e))
        return 3
    except Failed as e:
        say(f"FAILED: {e}")
        return 1
    except Exception as e:                                              # noqa: BLE001 - R888: no row content out
        frames = " < ".join(f"{os.path.basename(f.filename)}:{f.lineno}" for f in traceback.extract_tb(e.__traceback__))
        say(f"FAILED: unexpected {type(e).__name__} in step {a.step} (its message is withheld: it may hold row "
            f"content); at {frames}")
        return 1


if __name__ == "__main__":
    if sys.argv[1:2] == ["apply"]:
        with catalog_path.write_session():   # S11: the single-writer lock for the whole apply (main() refuses before T0)
            sys.exit(main())
    else:
        sys.exit(main())                     # every other step reads, or writes only an F: copy before T0
