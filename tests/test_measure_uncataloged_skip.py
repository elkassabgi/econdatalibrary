"""A source that holds catalogue rows is skipped for COST, and must be named for it.

WHY (R834 / R525). `core/measure_uncataloged.py` decides what to catalogue next. It used to skip
any source that already held catalogue rows - `d in cataloged` - which is how `abs`, with **18**
catalogue rows over **376,333,085** distinct store keys, and `bls`, with **9** over
**154,190,127**, stayed invisible to it. Both are SERIES grain with exact-key resolvers, so those
keys are reachable by nobody: an id absent from the catalogue 404s
(`api/worker/src/series.ts:39`). Between abs, bls and bis that is **532,044,393** series the
measure was structurally unable to see.

Skipping is still the right default - a full key-column scan of abs takes ~662 s - but it is a
COST choice, not a verdict. The distinction only exists if the run says so out loud.

Two-sided: the skip must still HAPPEN (or every run scans the giants), and it must be NAMED (or
"not measured" reads as "clean"). Plus `--include-cataloged` must actually lift it.
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
_TOOL = os.path.join(os.path.dirname(_HERE), "core", "measure_uncataloged.py")


def _load():
    spec = importlib.util.spec_from_file_location("_measure_under_test", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _fixture(tmp_path):
    """`covered` holds 2 catalogue rows over 5 store keys - the abs shape in miniature.
    `bare` holds none."""
    root = str(tmp_path)
    store = os.path.join(root, "data", "clean_full")
    for src, n in (("covered", 5), ("bare", 3)):
        d = os.path.join(store, src)
        os.makedirs(d, exist_ok=True)
        pq.write_table(
            pa.table({"series_key": [f"{src}_k{i}" for i in range(n)],
                      "obs_date": ["2020-01-01"] * n,
                      "value": [1.0] * n}),
            os.path.join(d, "part.parquet"))
    con = sqlite3.connect(os.path.join(root, "data", "catalog.db"))
    con.execute("CREATE TABLE series (series_id TEXT, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?, ?)",
                    [("covered:0", "covered"), ("covered:1", "covered")])
    con.commit()
    con.close()
    return root, store


def _run(tmp_path, argv_extra):
    root, store = _fixture(tmp_path)
    m = _load()
    m.ROOT, m.STORE = root, store
    m.OUTDIR = os.path.join(root, "dist", "broaden")
    cwd, argv = os.getcwd(), sys.argv
    os.chdir(root)                      # the tool probes a relative data/catalog.db first
    sys.argv = ["measure_uncataloged"] + argv_extra
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
        os.chdir(cwd)
    return buf.getvalue()


def test_a_catalogued_source_is_skipped_by_default(tmp_path):
    """DIRECTION 1 - the cheap default still holds; the giants are not scanned every run."""
    out = _run(tmp_path, [])
    assert "bare" in out, out
    # `covered` appears only in the NOT MEASURED list, never as a measured row
    measured = out.split("NOT MEASURED")[0]
    assert "covered" not in measured, measured


def test_the_skip_is_named_and_never_reads_as_clean(tmp_path):
    """DIRECTION 2 - the whole point. A silent skip is how 532,044,393 series stayed unseen."""
    out = _run(tmp_path, [])
    assert "NOT MEASURED - 1 source(s) skipped" in out, out
    assert "covered" in out and "catalogue rows          2" in out, out
    assert "COST choice, not a clean bill" in out, out
    assert "--include-cataloged" in out, out


def test_include_cataloged_actually_lifts_the_skip(tmp_path):
    """DIRECTION 3 - an escape hatch that does not work is worse than none, because the message
    promises a way to look and there is none."""
    out = _run(tmp_path, ["--include-cataloged"])
    assert "NOT MEASURED" not in out, out
    measured = out
    assert "covered" in measured and "distinct=" in measured, measured
    # both sources measured, 5 + 3 distinct keys
    assert "total distinct series = 8" in measured, measured


# --------------------------------------------------------- the other three members of the class

def test_a_PROTECTED_source_is_skipped_AND_NAMED(tmp_path):
    """The review of PR #14 (2026-09-07) found the PR had fixed ONE instance of a class with four
    members: `skipped_cataloged` was recorded, while the PROTECTED skip, the no-parquet skip and
    the key-column candidate list were each silent. The PROTECTED branch hid `cbs_nl` - 688,929,413
    keys - and `dbnomics` from every number this tool prints. Protection is a decision about
    WRITING; it is not evidence that the store is covered."""
    root, store = _fixture(tmp_path)
    m = _load()
    m.ROOT, m.STORE = root, store
    m.OUTDIR = os.path.join(root, "dist", "broaden")
    m.PROTECTED = {"bare"}                       # the only otherwise-measured source in the fixture
    cwd, argv = os.getcwd(), sys.argv
    os.chdir(root)
    sys.argv = ["measure_uncataloged"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
        os.chdir(cwd)
    out = buf.getvalue()
    assert "PROTECTED source(s)" in out, out
    assert "bare" in out.split("PROTECTED source(s)")[1], out
    # THE MESSAGE MUST DESCRIBE THIS RUN, NOT A REMEMBERED ONE (R875 #6). The first version of
    # this test demanded the literal "688,929,413" while setting PROTECTED = {"bare"} - so it
    # required the tool to print a fact about cbs_nl on a run where cbs_nl was not protected, and
    # the message obliged. The size line is now conditional on cbs_nl actually being in the list.
    assert "688,929,413" not in out, (
        "a run that did not skip cbs_nl must not quote cbs_nl's key count"
    )
    # and it is still SKIPPED - naming it must not start scanning a running backfill
    assert "bare" not in out.split("NOT MEASURED")[0], out


def test_the_cbs_nl_figure_appears_only_when_cbs_nl_is_the_thing_skipped(tmp_path, monkeypatch):
    """The other side of the same rule: when cbs_nl IS protected and present, the number that
    says why the silence mattered must still be printed."""
    root, store = _fixture(tmp_path)
    os.makedirs(os.path.join(store, "cbs_nl"), exist_ok=True)
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": ["2020-01-01"], "value": [1.0]}),
                   os.path.join(store, "cbs_nl", "part.parquet"))
    m = _load()
    m.ROOT, m.STORE = root, store
    m.OUTDIR = os.path.join(root, "dist", "broaden")
    m.PROTECTED = {"cbs_nl", "not_on_disk_at_all"}
    cwd, argv = os.getcwd(), sys.argv
    os.chdir(root)
    sys.argv = ["measure_uncataloged"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
        os.chdir(cwd)
    out = buf.getvalue()
    assert "688,929,413" in out, out
    # ...and a PROTECTED entry with no directory is reported as never reached, not as skipped -
    # `dbnomics` is exactly that, and the first message named it as one of the hidden pair
    assert "no directory under the store" in out, out
    assert "not_on_disk_at_all" in out, out


def test_a_source_directory_with_no_parquet_is_named_not_dropped(tmp_path):
    """Third member. An empty directory is equally the shape of a store that moved, a pull that
    never landed, and a source that genuinely holds nothing - so it is UNCHECKED, never clean."""
    root, store = _fixture(tmp_path)
    os.makedirs(os.path.join(store, "empty_one"), exist_ok=True)
    m = _load()
    m.ROOT, m.STORE = root, store
    m.OUTDIR = os.path.join(root, "dist", "broaden")
    cwd, argv = os.getcwd(), sys.argv
    os.chdir(root)
    sys.argv = ["measure_uncataloged"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
        os.chdir(cwd)
    out = buf.getvalue()
    assert "no parquet under them" in out, out
    assert "empty_one" in out, out


def test_idbank_is_a_key_column_candidate(tmp_path):
    """Fourth member. `broaden_catalog._key_col` and `audit_store_vs_catalog` both accept
    `idbank`; this tool accepted only `series_key` and `series_id`, so a store keyed that way read
    as "not uniform-long" - the same silent exclusion R825/R821 recorded for bls (154,190,127
    series) and eia (3,862,801) when the candidate list was shorter still."""
    root, store = _fixture(tmp_path)
    d = os.path.join(store, "idb")
    os.makedirs(d, exist_ok=True)
    pq.write_table(pa.table({"idbank": ["a", "b", "c"], "obs_date": ["2020-01-01"] * 3,
                             "value": [1.0] * 3}), os.path.join(d, "part.parquet"))
    m = _load()
    m.ROOT, m.STORE = root, store
    m.OUTDIR = os.path.join(root, "dist", "broaden")
    cwd, argv = os.getcwd(), sys.argv
    os.chdir(root)
    sys.argv = ["measure_uncataloged"]
    buf, real = io.StringIO(), sys.stdout
    sys.stdout = buf
    try:
        m.main()
    finally:
        sys.stdout = real
        sys.argv = argv
        os.chdir(cwd)
    out = buf.getvalue()
    assert "idb" in out, out
    assert "not uniform-long" not in out.split("idb")[1][:120], out
