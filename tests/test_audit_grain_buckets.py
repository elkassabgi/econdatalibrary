"""The store audit must not read a DESIGNED grain difference as a coverage gap - and must not
turn "I don't know the grain" into a licence to excuse one.

WHY. `tools/audit_store_vs_catalog.py` prints `distinct store keys - catalogue rows` per source
and totals the positives as "hosted but not catalogued". That subtraction only measures coverage
when one catalogue row means one store key. For a source served at flow / dot-table / file grain
that is false by design: one row deliberately stands for thousands of keys, so the subtraction
measures the GRAIN. `cbs_nl` alone contributed 688,924,158 that way.

WHY THE OBVIOUS FIX IS WORSE THAN THE BUG. The first version of this excluded every source with a
NON-DEFAULT resolver. Adversarial review killed it: `abs`, `bls` and `bis` have bespoke resolvers
and are SERIES grain - `_resolve_abs` / `_resolve_bls` / `_resolve_bis` all match an exact key,
and their catalogue ids name one series each (`abs:CPI:1.10001.10.50.Q`, `bls:CUUR0000SA0`,
`bis:WS_CBPOL:CA`). Excusing them hid **532,044,393** genuinely unreachable series. Worse, this
exact inference is already a caught error in the codebase: `api/worker/src/catalog.ts:30` names
abs (18) and bls (9) as "small hand-curated PER-SERIES catalogues" and says in as many words
"Do NOT infer grain from the catalogue row count" - which is R525.

So there are three buckets, and only ONE of them is an exemption:

    series grain            -> the gap IS coverage. In the headline total.
    DECLARED flow/table/file-> design. Reported apart, excluded from the total.
    bespoke resolver        -> grain UNESTABLISHED. **COUNTED in the total** and also named,
                               because a default of "unknown" must fail LOUD where a default of
                               "grain" fails silent.

Two-sided throughout: a one-sided test passes equally on a tool that excuses everything and on
one that excuses nothing.
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
# charlie: bespoke resolver, 50-key gap - the abs/bls/bis shape: unknown, therefore COUNTED
# zulu: a bespoke resolver with ZERO catalogue rows. The suite had no such case, which is
# exactly why the cat == 0 branch kept the pre-review behaviour and dropped 5,353,886 real
# store keys (imf 4,304,918 + owid 1,048,968) out of the headline on the last complete run.
SPEC = {"alpha": (40, 10), "bravo": (100, 5), "charlie": (60, 10), "zulu": (500, 0)}
GRAIN = {"bravo": "flow", "charlie": "custom", "zulu": "custom"}
# alpha 30 + charlie 50 + zulu 500
HEADLINE = "held locally but not catalogued    : 580 series"


def test_real_series_grain_gap_is_still_counted(tmp_path):
    """DIRECTION 1 - the fix must not hide a genuine coverage gap."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert HEADLINE in stdout, stdout
    assert "alpha\t40\t10\t30\tpartial" in tsv, tsv


def test_declared_grain_gap_is_excluded_and_named(tmp_path):
    """DIRECTION 2 - a designed difference is reported, never totalled as a defect. This is the
    ONLY exemption the tool grants."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert "NOT COMPARABLE" in stdout and "1 source(s) served at a NON-SERIES grain" in stdout
    assert "95 store keys" in stdout, stdout
    assert "grain:flow" in tsv
    assert "held locally but not catalogued    : 675 series" not in stdout   # bravo not counted
    assert HEADLINE in stdout


def test_bespoke_resolver_gap_is_COUNTED_not_excused(tmp_path):
    """DIRECTION 3 - the one that mattered, and the one the first version got wrong.

    A bespoke resolver is not a grain claim. abs/bls/bis have one and are series grain; excusing
    them hid 532,044,393 unreachable series. So an unestablished grain is counted in the headline
    AND named, never quietly set aside."""
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert HEADLINE in stdout, stdout                       # charlie's 50 IS in the total
    assert "GRAIN UNESTABLISHED" in stdout, stdout          # ...and it is named
    assert "INCLUDED in the total above" in stdout, stdout  # ...and the heading says so
    assert "50 store keys" in stdout, stdout
    assert "grain UNESTABLISHED (custom resolver) — COUNTED as a gap" in tsv, tsv
    # it must NOT have been granted the declared-grain exemption
    assert "1 source(s) served at a NON-SERIES grain" in stdout   # bravo only


def test_fail_closed_when_the_grain_index_cannot_be_built(tmp_path):
    """R503 - an except branch that fails OPEN is decoration. With no index nothing may be
    excused on its authority, and the run must say its total is unqualified."""
    stdout, _tsv = _run(tmp_path, SPEC, GRAIN, fail_index=True)
    assert "GRAIN INDEX UNAVAILABLE" in stdout, stdout
    assert "the grain index failed to build" in stdout, stdout
    # 30 + 95 + 50 + 500 = 675: nothing was excused
    assert "held locally but not catalogued    : 675 series" in stdout, stdout
    assert "NOT COMPARABLE" not in stdout
    assert "GRAIN UNESTABLISHED" not in stdout


# --------------------------------------------------------------------------------------------
# --summarise: re-read a finished run's numbers through the current grain index.
#
# A full audit takes hours. When the CLASSIFICATION changes but the MEASUREMENT does not,
# re-running it is waste - and a run interrupted to pick up new code leaves a TSV carrying two
# generations of note strings. Measured on the 39 sources finished at the time of writing: the
# old headline summed every positive gap to 1,246,546,145, of which 704,613,282 was DECLARED
# grain. The honest figure is 541,932,863 - which still includes 540,113,232 whose grain nobody
# has established, exactly so it cannot be forgotten.


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
    "zulu\t500\t0\t500\tUNCATALOGUED\n"      # bespoke resolver, ZERO catalogue rows
    "delta\t10\t25\t-15\tORPHAN\n"           # catalogue rows with no store key
    "echo\t\t7\t\tno key column\n"           # NOT MEASURED - must not read as clean
)


def test_summarise_reclassifies_without_measuring(tmp_path):
    out = _summarise(tmp_path, BODY, GRAIN)
    assert "MEASURED NOTHING, only re-classified" in out, out
    assert HEADLINE in out, out
    assert "catalogued with no LOCAL STORE KEY : 15 series" in out, out
    assert "1 source(s) at a DECLARED non-series grain, 95 store keys" in out, out
    assert "50 store keys — INCLUDED in the total above" in out, out
    assert "NO CATALOGUE ROWS AT ALL - 1 source(s), 500 store keys" in out, out


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
    # nothing excused: 30 + 95 + 50 + 500 all land in the headline
    assert "held locally but not catalogued    : 675 series" in out, out


def test_grain_index_reads_every_machine_readable_holder():
    """Grain is declared in SIX places here, and they do not overlap. Measured 2026-09-06:

        _resolve._FLOW_GRAIN        11
        _resolve._DOT_TABLE_GRAIN   18   (api/worker/src/catalog.ts said 13; corrected)
        _resolve_file_grain routing  5
        orchestrate._TABLE_GRAIN    14   (every imf_*_direct)

    The last one is DISJOINT from both _resolve sets - zero overlap either way - and the audit
    did not consult it, so 14 sources whose catalogue is deliberately at TABLE grain while their
    stores are at SERIES grain would have been counted as coverage gaps. This test is the pin:
    a new holder that is not read here is a silent regression.

    Reads the real repo on purpose. A fixture cannot catch a registry moving.
    """
    import importlib

    m = _load()
    g = m.grain_index()
    assert "table" in m.DECLARED_GRAINS

    tg = getattr(importlib.import_module("updater.orchestrate"), "_TABLE_GRAIN")
    assert tg, "orchestrate._TABLE_GRAIN vanished - the audit would silently misclassify it"
    missing = sorted(s for s in tg if g.get(s) != "table")
    assert not missing, f"declared table-grain sources not classified as such: {missing}"

    from econdl import _resolve
    for s in _resolve._FLOW_GRAIN:
        assert g.get(s) == "flow", s
    for s in _resolve._DOT_TABLE_GRAIN:
        assert g.get(s) == "dot-table", s

def test_grain_index_works_when_run_AS_A_SCRIPT(tmp_path):
    """The test suite is not the tool. pytest puts the repo root on sys.path; running
    `python tools/audit_store_vs_catalog.py` does not - sys.path[0] is tools/. So every test
    above passed while the real command refused every run with

        GRAIN INDEX UNAVAILABLE (RuntimeError: ... ModuleNotFoundError: No module named 'updater')

    and reported an unqualified 1,246,546,145. Caught by the tool's own fail-closed guard on the
    first live invocation, which is the only reason it was caught at all.

    So this one runs the actual command in a subprocess, the way a person does.
    """
    import subprocess

    repo = os.path.dirname(_HERE)
    tsv = tmp_path / "audit.tsv"
    io.open(str(tsv), "w", encoding="utf-8").write(
        "source\tin_store\tcatalogued\tgap\tnote\n"
        "bfs\t483667\t582\t483085\tpartial\n"
        "abs\t376333085\t18\t376333067\tpartial\n")

    r = subprocess.run(
        [sys.executable, os.path.join("tools", "audit_store_vs_catalog.py"),
         "--summarise", str(tsv)],
        cwd=repo, capture_output=True, text=True)

    assert r.returncode == 0, r.stderr[-800:]
    assert "GRAIN INDEX UNAVAILABLE" not in r.stdout, r.stdout
    assert "UNQUALIFIED" not in r.stdout, r.stdout
    # bfs is declared flow grain -> excluded; abs is unestablished -> counted
    assert "held locally but not catalogued    : 376,333,067 series" in r.stdout, r.stdout
    assert "grain:flow" in r.stdout, r.stdout

def test_zero_catalogue_rows_is_its_own_bucket_and_is_COUNTED(tmp_path):
    """THE BLOCKER an adversarial review found, and the suite could not see.

    A source with ZERO catalogue rows reaches nobody whatever its grain - `series.ts` 404s an id
    absent from the catalogue. The pre-review code added such a source's keys to the DECLARED
    grain tally ("NOT a coverage gap") and to NEITHER the headline nor the unestablished tally, so
    on the last complete run it printed "0 store keys" under a heading reading "INCLUDED in the
    total above" while listing two sources, and dropped imf (4,304,918) and owid (1,048,968) out
    of the headline entirely. It also booked ilo - file grain, 29,447,518 keys, no catalogue at
    all - as design.
    """
    stdout, tsv = _run(tmp_path, SPEC, GRAIN)
    assert HEADLINE in stdout, stdout                       # zulu's 500 IS in the total
    assert "NO CATALOGUE ROWS AT ALL - 1 source(s), 500 store keys" in stdout, stdout
    assert "INCLUDED in the total above" in stdout, stdout
    assert "UNCATALOGUED — 0 catalogue rows" in tsv, tsv
    # and it is in NEITHER gap bucket: those two now partition cleanly
    assert "1 source(s) served at a NON-SERIES grain" in stdout, stdout   # bravo only
    assert "GRAIN UNESTABLISHED — 1 source(s)" in stdout, stdout          # charlie only


def test_a_gated_source_is_not_reported_as_forgotten(tmp_path):
    """R838 - the rule I broke by hand, twice in one hour, now in the instrument.

    A denylisted source holds data, has no catalogue row and no R2 object BY DECISION (the
    2026-07-22/23 licence purge). Reporting it beside genuinely forgotten data is how I came to
    write that a gated store was "a dead directory nothing owns" when it has a working fetcher
    and sits on the denylist.

    SIMPLIFIED after a second review: the gate is now checked BEFORE grain, at ANY catalogue row
    count, so gated sources never reach the zero-catalogue bucket at all. One gated bucket, and
    everything left in the zero-catalogue bucket is unexplained by construction - which is why
    that bucket no longer prints a "GATED BY DECISION - 0" heading a reader could mistake for a
    measurement.
    """
    m = _load()
    m.denylisted = lambda: {"zulu"} | {f"pad{i}" for i in range(12)}
    m.grain_index = lambda: dict(GRAIN)
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.summarise(_tsv(tmp_path, BODY))
    finally:
        sys.stdout = real
    out = buf.getvalue()
    assert "GATED WITH A GAP - 1 denylisted source(s), 500 keys" in out, out
    assert "EXCLUDED from the total above" in out, out
    assert "not a coverage gap at any row count" in out, out
    # and it is NOT in the zero-catalogue bucket, which is gate-free by construction
    assert "NO CATALOGUE ROWS AT ALL" not in out, out


def test_the_denylist_read_fails_CLOSED(tmp_path):
    """If the denylist cannot be parsed, gated sources would be reported as forgotten ones - so
    the run must say so rather than print a confident bucket."""
    m = _load()

    def _boom():
        raise RuntimeError("simulated: denylist.ts unreadable")
    m.denylisted = _boom
    m.grain_index = lambda: dict(GRAIN)
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.summarise(_tsv(tmp_path, BODY))
    finally:
        sys.stdout = real
    out = buf.getvalue()
    assert "DENYLIST UNAVAILABLE" in out, out
    assert "reported as forgotten ones" in out, out


def test_summarise_refuses_to_assert_ORPHAN_from_an_estimate(tmp_path):
    """main() deliberately has no ORPHAN branch on the HLL path - measured error +19/-14% - and
    summarise must agree, or a giant that OOM'd into that path invents an orphan count from
    noise."""
    m = _load()
    m.grain_index = lambda: {}
    m.denylisted = lambda: {f"pad{i}" for i in range(12)}
    body = "giant\t900000\t1000000\t-100000\tinconclusive  [approx +19/-14%]\n"
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.summarise(_tsv(tmp_path, body))
    finally:
        sys.stdout = real
    out = buf.getvalue()
    assert "catalogued with no LOCAL STORE KEY : 0 series" in out, out
    assert "100,000" not in out, out

# --------------------------------------------------------------------------------------------
# THE GAPS A SECOND ADVERSARIAL REVIEW FOUND. It ran mutants I had not thought of and 5 of 8
# survived - because I had chosen mutants my own tests already covered. Choosing your own mutants
# is subject to the same blind spot as choosing your own assertions.

SPEC2 = {
    "alpha": (40, 10),      # series grain, real gap
    "yankee": (700, 0),     # DECLARED flow grain with ZERO catalogue rows - the file-grain case
    "xray": (300, 20),      # DENYLISTED with catalogue rows - the mis-assignment case
    "whisky": (90, 0),      # zero catalogue rows, NOT gated - the unexplained side
}
GRAIN2 = {"yankee": "flow"}
DENY2 = {"xray"} | {f"pad{i}" for i in range(12)}


def _run2(tmp_path, fail_deny=False):
    root = str(tmp_path)
    store = _build(root, SPEC2)
    m = _load()
    m.ROOT, m.STORE = root, store
    m.grain_index = lambda: dict(GRAIN2)
    if fail_deny:
        def _boom():
            raise RuntimeError("simulated")
        m.denylisted = _boom
    else:
        m.denylisted = lambda: set(DENY2)
    out = os.path.join(root, "logs", "audit.tsv")
    argv = sys.argv
    sys.argv = ["audit", "--out", out, "--memory-limit", "512MB"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        assert m.main() == 0
    finally:
        sys.stdout = real
        sys.argv = argv
    return buf.getvalue(), io.open(out, encoding="utf-8").read()


def test_zero_catalogue_at_a_DECLARED_grain_is_still_counted(tmp_path):
    """THE MUTANT THAT SURVIVED. No fixture had a cat==0 source at a DECLARED grain, so reverting
    the fix for flow/file/table/dot-table passed all 13 tests. On the real data that mutant moves
    29,447,518 keys - a file-grain store with no catalogue at all - back out of the headline,
    which is the very case the fix's own comment names."""
    stdout, _tsv = _run2(tmp_path)
    assert "NO CATALOGUE ROWS AT ALL" in stdout, stdout
    # alpha 30 + yankee 700 + whisky 90 = 820 ; xray is gated and excluded
    assert "held locally but not catalogued    : 820 series" in stdout, stdout
    assert "grain:flow" in stdout, stdout


def test_the_gated_split_cannot_be_inverted(tmp_path):
    """Inverting GATED and UNEXPLAINED passed, because the old assertions were "the gated line is
    present" and "UNEXPLAINED - 0 source(s)" - both of which an inversion also satisfies. Assert
    NON-ZERO on BOTH sides so a swap cannot pass."""
    stdout, _tsv = _run2(tmp_path)
    # BOTH sides non-zero, so a swap cannot pass: 2 unexplained (700 + 90) against
    # 1 gated (280). The old assertions were satisfied by an inversion.
    assert "NO CATALOGUE ROWS AT ALL - 2 source(s), 790 store keys" in stdout, stdout
    assert "GATED WITH A GAP - 1 denylisted source(s), 280 keys" in stdout, stdout
    gated_block = stdout.split("GATED WITH A GAP")[1].split("NO CATALOGUE ROWS")[0]
    assert "xray" in gated_block, gated_block
    assert "whisky" not in gated_block and "yankee" not in gated_block, gated_block


def test_a_gated_source_WITH_catalogue_rows_is_not_a_coverage_gap(tmp_path):
    """The real mis-assignment the review measured: a denylisted source holding 26 catalogue rows
    missed the zero-catalogue split entirely, so its 233-key gap printed as an unexplained
    coverage gap. The gate outranks the grain at ANY row count."""
    stdout, tsv = _run2(tmp_path)
    assert "GATED WITH A GAP - 1 denylisted source(s), 280 keys" in stdout, stdout
    assert "EXCLUDED from the total above" in stdout, stdout
    assert "GATED - 451, not a coverage gap" in tsv, tsv
    assert "held locally but not catalogued    : 820 series" in stdout, stdout


def test_the_REAL_denylist_parses_against_the_REAL_file():
    """FOUR mutants survived because NO test ever called the unpatched `denylisted()` - both that
    touch it monkeypatch it, and the fixture runs set ROOT to a tmp dir where denylist.ts does not
    exist, so the entire suite ran with DENYLIST UNAVAILABLE. Same class as this file's own
    script-invocation test: green tests over a function nothing exercises."""
    import re

    m = _load()
    ids = m.denylisted()          # unpatched, real ROOT, real api/worker/src/denylist.ts
    assert len(ids) >= 10, len(ids)
    assert all(isinstance(i, str) and i.islower() for i in ids), sorted(ids)[:5]
    raw = io.open(os.path.join(os.path.dirname(_HERE), "api", "worker", "src", "denylist.ts"),
                  encoding="utf-8").read()
    assert re.search(r"NON_REDISTRIBUTABLE[^=]*=\s*new Set", raw), "declaration shape changed"

def test_a_suspiciously_short_denylist_is_REFUSED(tmp_path):
    """The <10-ids guard survived a mutant because the REAL file has 49 ids, so the guard never
    fires and deleting it is invisible. A guard only counts as tested when something trips it.

    Why it exists: a regex that half-matches (a renamed export, a reformatted file, a set written
    with single quotes) yields a SHORT list rather than an empty one, and a short denylist reports
    gated sources as forgotten data - the exact failure the whole gated bucket exists to prevent.
    Failing closed on an implausibly small parse is cheaper than that.
    """
    root = tmp_path / "repo"
    d = root / "api" / "worker" / "src"
    d.mkdir(parents=True)
    io.open(str(d / "denylist.ts"), "w", encoding="utf-8").write(
        'export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([\n'
        '  "one",\n  "two",\n  "three",\n]);\n')

    m = _load()
    m.ROOT = str(root)
    try:
        ids = m.denylisted()
    except RuntimeError as e:
        assert "implausibly few" in str(e), str(e)
        assert "3" in str(e), str(e)
    else:
        raise AssertionError(f"a 3-id denylist was accepted: {ids}")


def test_a_full_length_denylist_is_ACCEPTED(tmp_path):
    """The other side: the guard must not refuse a legitimate list, or it is just an outage."""
    root = tmp_path / "repo"
    d = root / "api" / "worker" / "src"
    d.mkdir(parents=True)
    ids = "".join(f'  "src{i}",\n' for i in range(30))
    io.open(str(d / "denylist.ts"), "w", encoding="utf-8").write(
        "export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([\n" + ids + "]);\n")

    m = _load()
    m.ROOT = str(root)
    got = m.denylisted()
    assert len(got) == 30, len(got)
    assert "src0" in got and "src29" in got
