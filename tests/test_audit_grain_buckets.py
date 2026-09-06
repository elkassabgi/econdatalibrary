"""The store audit must not read a DESIGNED grain difference as a coverage gap - and must not
use that as a licence to excuse a real one.

WHY. `tools/audit_store_vs_catalog.py` prints `distinct store keys - catalogue rows` per source.
That subtraction only measures coverage when one catalogue row means one store key. For a source
served at flow / dot-table / file grain, one row deliberately stands for thousands of keys, so
the same subtraction measures the GRAIN. Unqualified, the fleet total is dominated by it: abs
alone reported 376,333,067 "hosted but not catalogued" against 18 catalogue rows, and bls
154,190,118 against 9 - neither of which is a defect.

The dangerous fix is the one that excuses too much. A source with a BESPOKE resolver is not
thereby table-grain; all that establishes is that the audit has not established its grain. So
there are three buckets, not two, and this file pins all three from both sides:

    series grain            -> the gap IS coverage, counted in the headline total
    declared flow/table/file-> the gap is design, reported apart, never in the total
    bespoke resolver        -> grain UNESTABLISHED, reported apart under its own heading,
                               never counted as clean and never folded into either of the above

Two-sided, because a one-sided test passes on a tool that excuses everything (assert the grain
sources are excluded) and equally on one that excuses nothing (assert the real gap is counted).
Both directions are asserted here, plus the fail-closed branch: if the grain index cannot be
built, nothing may be excused on its authority.
"""
from __future__ import annotations

import importlib.util
import io
import os
import sqlite3
import sys

import pyarrow as pa
import pyarrow.parquet as pq

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "audit_store_vs_catalog.py")


def _load():
    spec = importlib.util.spec_from_file_location("_audit_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _build(root, spec):
    """spec: {source: (n_store_keys, n_catalogue_rows)} -> a tiny store + catalogue at `root`."""
    store = os.path.join(root, "data", "clean_full")
    os.makedirs(os.path.join(root, "logs"), exist_ok=True)
    for src, (n_keys, _n_cat) in spec.items():
        d = os.path.join(store, src)
        os.makedirs(d, exist_ok=True)
        keys = [f"{src}_k{i}" for i in range(n_keys)]
        pq.write_table(pa.table({"series_key": keys, "obs_value": [1.0] * n_keys}),
                       os.path.join(d, "part.parquet"))
    con = sqlite3.connect(os.path.join(root, "data", "catalog.db"))
    con.execute("CREATE TABLE series (series_id TEXT, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?)",
                    [(f"{s}:{i}", s) for s, (_k, c) in spec.items() for i in range(c)])
    con.commit()
    con.close()
    return store


def _run(tmp_path, spec, grain, fail_index=False):
    root = str(tmp_path)
    store = _build(root, spec)
    m = _load()
    m.ROOT, m.STORE = root, store

    if fail_index:
        def _boom():
            raise ImportError("simulated: econdl not importable")
        m.grain_index = _boom
    else:
        m.grain_index = lambda: dict(grain)

    out = os.path.join(root, "logs", "audit.tsv")
    argv = sys.argv
    sys.argv = ["audit", "--out", out, "--memory-limit", "512MB"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
    assert rc == 0
    return buf.getvalue(), io.open(out, encoding="utf-8").read()


# alpha: plain series grain, a REAL 30-key coverage gap (40 keys, 10 catalogue rows)
# bravo: DECLARED flow grain, a 95-key "gap" that is the design (100 keys, 5 rows)
# charlie: bespoke resolver, 50-key gap whose meaning nobody has established (60 keys, 10 rows)
SPEC = {"alpha": (40, 10), "bravo": (100, 5), "charlie": (60, 10)}
GRAIN = {"bravo": "flow", "charlie": "custom"}


def test_real_series_grain_gap_is_still_counted(tmp_path):
    """DIRECTION 1 - the fix must not hide a genuine coverage gap."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert "hosted but not catalogued          : 30 series" in stdout, stdout
    assert "alpha\t40\t10\t30\tpartial" in tsv, tsv


def test_declared_grain_gap_is_excluded_and_named(tmp_path):
    """DIRECTION 2 - a designed difference is reported, never totalled as a defect."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert "NOT COMPARABLE" in stdout and "1 source(s) served at a NON-SERIES grain" in stdout
    assert "95 store keys" in stdout, stdout
    assert "grain:flow" in tsv
    # and it is NOT in the coverage total
    assert "hosted but not catalogued          : 125 series" not in stdout
    assert "hosted but not catalogued          : 30 series" in stdout


def test_bespoke_resolver_is_its_own_bucket_not_an_excuse(tmp_path):
    """DIRECTION 3 - the dangerous one. `custom` must not buy the same pass as a declared grain,
    and must not read as clean either."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert "GRAIN UNESTABLISHED" in stdout, stdout
    assert "50 store keys unaccounted" in stdout, stdout
    assert "grain UNESTABLISHED (custom resolver)" in tsv, tsv
    # charlie is in NEITHER of the other two buckets
    assert "1 source(s) served at a NON-SERIES grain" in stdout   # bravo only
    assert "hosted but not catalogued          : 30 series" in stdout  # alpha only


def test_fail_closed_when_the_grain_index_cannot_be_built(tmp_path):
    """R503 - an except branch that fails OPEN is decoration. With no index, nothing may be
    excused on its authority: every gap falls back into the headline total AND the run says
    loudly that the total is unqualified."""
    stdout, _tsv = _run(tmp_path, SPEC, GRAIN, fail_index=True)
    assert "GRAIN INDEX UNAVAILABLE" in stdout, stdout
    assert "the grain index failed to build" in stdout, stdout
    # 30 + 95 + 50 = 175: nothing was excused
    assert "hosted but not catalogued          : 175 series" in stdout, stdout
    assert "NOT COMPARABLE" not in stdout
    assert "GRAIN UNESTABLISHED" not in stdout

# --------------------------------------------------------------------------------------------
# --summarise: re-read a finished run's numbers through the current grain index.
#
# A full audit takes hours. When the CLASSIFICATION changes but the MEASUREMENT does not,
# re-running it is waste - and a run interrupted to pick up new code leaves a TSV carrying two
# generations of note strings. Measured on the 39 sources finished at the time this was written:
# the old headline summed every positive gap to 1,246,546,145 "hosted but not catalogued", of
# which 1,244,726,514 (99.85%) was grain. The honest figure was 1,819,631.


def _tsv(tmp_path, body):
    p = tmp_path / "audit.tsv"
    io.open(str(p), "w", encoding="utf-8").write(
        "source\tin_store\tcatalogued\tgap\tnote\n" + body)
    return str(p)


def _summarise(tmp_path, body, grain):
    m = _load()
    m.grain_index = lambda: dict(grain)
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        rc = m.summarise(_tsv(tmp_path, body))
    finally:
        sys.stdout = real
    assert rc == 0
    return buf.getvalue()


BODY = (
    "alpha\t40\t10\t30\tpartial\n"           # series grain, a real gap
    "bravo\t100\t5\t95\tpartial\n"           # declared flow grain
    "charlie\t60\t10\t50\tpartial\n"         # bespoke resolver
    "delta\t10\t25\t-15\tORPHAN\n"           # catalogue rows with no store key
    "echo\t\t7\t\tno key column\n"           # NOT MEASURED - must not read as clean
)


def test_summarise_reclassifies_without_measuring(tmp_path):
    out = _summarise(tmp_path, BODY, GRAIN)
    assert "MEASURED NOTHING, only re-classified" in out, out
    assert "hosted but not catalogued          : 30 series" in out, out
    assert "catalogued with no LOCAL STORE KEY : 15 series" in out, out
    assert "1 source(s) at a DECLARED non-series grain, 95 store keys" in out, out
    assert "50 store keys unaccounted" in out, out


def test_summarise_never_lets_an_unmeasured_source_read_as_clean(tmp_path):
    """A row the run could not count is a THIRD answer. Dropping it silently is how a bounded
    pass reads as full coverage."""
    out = _summarise(tmp_path, BODY, GRAIN)
    assert "NOT MEASURED — 1 source(s) the run could not count" in out, out
    assert "echo" in out and "no key column" in out, out


def test_summarise_fails_closed_without_a_grain_index(tmp_path):
    m = _load()

    def _boom():
        raise ImportError("simulated")
    m.grain_index = _boom
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.summarise(_tsv(tmp_path, BODY))
    finally:
        sys.stdout = real
    out = buf.getvalue()
    assert "GRAIN INDEX UNAVAILABLE" in out and "UNQUALIFIED" in out, out
    # nothing excused: 30 + 95 + 50 all land in the headline
    assert "hosted but not catalogued          : 175 series" in out, out
