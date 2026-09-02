"""EVERY COUNT THE SUMMARY PRINTS, ON A RUN WHERE NO TWO COUNTS ARE EQUAL.

R656. Round 15 fixed the ORDER of mirror_sync's disclosures and wrote tests that bind the
order. It did not bind the SUBJECT of any count, because every fixture in
`test_mirror_sync_output.py` syncs exactly ONE file - so every list has length 0 or 1, every
aggregate is degenerate, and ten mutants survived all 59 tests. Five of them were count
substitutions inside the very lines R652 was written about. Measured by the reviewer: with
`len(weak_identity)` replaced by `len(one_axis)`, a 200-file run printed "of those, 150 were
REPLACED on a WEAK COMPARISON" where 50 were replaced - R652's own defect one round later and
numerically worse, with the suite green.

A single-file fixture cannot test a count. This one syncs 34 files whose fates are chosen so
that NO TWO of the run's numbers are equal:

    one_axis      26      stale_files    5
    weak_identity  4      fail           6
    withdrawals    3      check_failed  10
    rows lost     11      unreadable     2
    whole_row      7      pulled        13

so substituting any one of those lists or aggregates for any other changes the printed line.

It also covers the three terms of the pulled arithmetic (R628: `len(names) - len(fail) -
len(check_failed) - len(stale_files)`) rather than the one term round 15 tested, and the
`unreadable_local` term of the ledger-retention rule, which had no test at all - dropping it
deletes the ledger holding the only record that a corrupt file was replaced, which is R550's
exact shape.
"""
import contextlib
import datetime as dt
import io
import os
import shutil
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools import mirror_sync as ms  # noqa: E402

SRC = "_testcounts"
ROOTDIR = "_testdata"

# The intended shape of the run. Every value here is DISTINCT, which is the whole point.
N_WEAK = 5            # keyed, dateless -> one_axis AND key-only: a weak comparison
                      # (the fifth is the dateless-local/dated-incoming case below)
N_LOSSY = 3           # of those, the ones that also lose rows -> withdrawals
LOSSES = (3, 1, 7)    # 11 rows total; also gives the worst-loss line two candidates
N_WHOLE = 7           # keyless, dateless -> one_axis AND whole-row: NOT weak (one caveat)
N_STALE = 8           # classify() says 'same' -> refused, local kept
N_FAIL = 6            # the download itself raises
N_CHECKFAIL = 9       # the incoming file lacks a column the identity needs
N_UNREADABLE = 2      # local bytes are not parquet -> replaced, and the ledger must say so
# plus ONE file whose LOCAL copy has a date axis and whose INCOMING copy does not: it is
# one-axis by the `rm[1] is None` half of the test, which no other fixture exercises (R656 F4).
EXPECT = {
    # one_axis is appended in the CLASSIFY block, before any refusal, so it counts the stale
    # and check-failed files too - they were classified on the row count alone whether or not
    # they were then replaced. That is the line's own claim, and it is why this count is much
    # larger than the weak population nested under it.
    "one_axis": N_WEAK + N_WHOLE + 1 + N_STALE + N_CHECKFAIL,   # 26
    "weak": N_WEAK,                                      # 4
    "withdrawals": N_LOSSY,                              # 3
    "rows": sum(LOSSES),                                 # 11
    "whole_row": N_WHOLE,                                # 7
    "stale": N_STALE,                                    # 5
    "fail": N_FAIL,                                      # 6
    "check_failed": N_CHECKFAIL + 1,                     # 10 (the dated-local file joins them)
    "unreadable": N_UNREADABLE,                          # 2
}
N_NAMES = (N_WEAK + N_WHOLE + 1 + N_STALE + N_FAIL + N_CHECKFAIL + N_UNREADABLE)   # 34
EXPECT["pulled"] = N_NAMES - EXPECT["fail"] - EXPECT["check_failed"] - EXPECT["stale"]   # 13


def _keyed(vals, value=1.0):
    return pa.table({"series_key": vals, "value": [value] * len(vals)})


def _keyless(vals):
    return pa.table({"LEI": vals, "LegalName": ["N" + v for v in vals]})


def _dated(vals, year):
    return pa.table({"series_key": vals,
                     "obs_date": pa.array([dt.date(year, 1, 1)] * len(vals), pa.date32()),
                     "value": [1.0] * len(vals)})


class ManyS3:
    """One staged file per name; a name in `explode` raises instead of downloading."""

    def __init__(self, staged, explode):
        self.staged, self.explode = staged, explode

    def download_file(self, bucket, key, dest):
        n = os.path.basename(key)[:-len(".parquet")]
        if n in self.explode:
            raise OSError(f"network died fetching {n}")
        shutil.copyfile(self.staged[n], dest)


def _build(tmp_path, dest_dir):
    """(behind, staged, explode) — every file's fate decided here, in one readable place."""
    behind, staged, explode = [], {}, set()

    def put(n, local, incoming, local_bytes=None):
        p = os.path.join(dest_dir, f"{n}.parquet")
        if local_bytes is not None:
            with open(p, "wb") as fh:
                fh.write(local_bytes)
            old = time.time() - 7200          # not a write in progress
            os.utime(p, (old, old))
            lrows = 0
        else:
            pq.write_table(local, p)
            lrows = local.num_rows
        s = str(tmp_path / f"{n}.parquet")
        pq.write_table(incoming, s)
        staged[n] = s
        behind.append([n, lrows, incoming.num_rows])

    # WEAK: keyed and dateless, so one_axis AND key-only. Three of them also lose rows, by
    # different amounts, so the worst-loss line has to choose and a min/max swap is visible.
    for i, lost in enumerate(LOSSES):
        local = _keyed([f"a{j}" for j in range(lost + 2)])
        incoming = _keyed(["a0", "a1"] + [f"n{i}_{j}" for j in range(lost + 1)], value=9.0)
        put(f"weak_lossy{i}", local, incoming)
    for i in range(N_WEAK - N_LOSSY - 1):
        put(f"weak_clean{i}", _keyed(["a", "b"]), _keyed(["a", "b", "c"], value=9.0))

    # THE FIRST HALF OF THE ONE-AXIS TEST, which `dated_local` below does not cover:
    # a DATELESS local against a DATED incoming. With `lm[1] is None or rm[1] is None`
    # reduced to its SECOND clause this file stops being one-axis and loses its weak
    # disclosure entirely, while classify() really did decide on the row count alone.
    put("dateless_local", _keyed(["a", "b"]), _dated(["a", "b", "c"], 2021))

    # WHOLE ROW: keyless and dateless. one_axis, but a whole-row identity and no loss is a
    # single caveat, so these are deliberately NOT in the weak population.
    for i in range(N_WHOLE):
        put(f"whole{i}", _keyless(["L1", "L2"]), _keyless(["L1", "L2", "L3"]))

    # THE OTHER HALF OF THE ONE-AXIS TEST: local HAS a date axis, incoming does not. No other
    # fixture has this shape, so `lm[1] is None or rm[1] is None` reduced to its first clause
    # passed everything (R656 F4). Its identity then fails for the missing column, which is
    # correct and lands it in check_failed.
    put("dated_local", _dated(["a", "b"], 2020), _keyed(["a", "b", "c"], value=9.0))

    for i in range(N_STALE):                       # identical -> classify says 'same'
        same = _keyless(["L1", "L2", "L3"])
        put(f"stale{i}", same, same)
    for i in range(N_FAIL):
        put(f"fail{i}", _keyed(["a"]), _keyed(["a", "b"]))
        explode.add(f"fail{i}")
    for i in range(N_CHECKFAIL):                   # incoming lacks the key the identity needs
        put(f"checkfail{i}", _keyed(["a", "b"]),
            pa.table({"other_key": ["a", "b", "c"], "value": [1.0] * 3}))
    for i in range(N_UNREADABLE):
        put(f"junk{i}", None, _keyless(["L1", "L2"]),
            local_bytes=b"this is not a parquet file at all")
    return behind, staged, explode


def _run(tmp_path):
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    behind, staged, explode = _build(tmp_path, dest_dir)
    assert len(behind) == N_NAMES, f"fixture built {len(behind)} files, expected {N_NAMES}"
    rec = {"source": SRC, "root": ROOTDIR, "behind": behind, "r2_only": [], "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            pulled, _ahead = ms.sync_source(ManyS3(staged, explode), rec, apply=True)
        out = buf.getvalue()
        on_disk = {f[:-len(".parquet")]: pq.read_metadata(os.path.join(dest_dir, f)).num_rows
                   for f in os.listdir(dest_dir) if f.endswith(".parquet")}
    finally:
        ledgers = [os.path.join(ms.ROOT, "logs", f)
                   for f in os.listdir(os.path.join(ms.ROOT, "logs"))
                   if f.startswith("_mirror_sync_withdrawals_") and f.endswith(f"_{SRC}.tsv")]
        body = "".join(io.open(p, encoding="utf-8").read() for p in ledgers)
        for p in ledgers:
            os.remove(p)
        shutil.rmtree(dest_dir, ignore_errors=True)
    return out, pulled, body, on_disk


def test_every_printed_count_names_its_own_population(tmp_path):
    """The ten surviving mutants of R656 Finding 1, all in one run."""
    out, pulled, body, _ = _run(tmp_path)
    e = EXPECT
    assert f"{e['one_axis']} file(s) were classified on the ROW COUNT ALONE" in out, out
    assert f"of those, {e['weak']} were REPLACED on a WEAK COMPARISON" in out, out
    assert f"{e['withdrawals']} file(s) lost {e['rows']:,} ROWS" in out, out
    assert f"{e['whole_row']} file(s) have no key column" in out, out
    assert f"{e['stale']} file(s) no longer classify as 'behind'" in out, out
    assert f"{e['fail']} download(s) FAILED" in out, out
    assert f"{e['check_failed']} file(s) CHECK FAILED" in out, out
    assert f"{e['unreadable']} local file(s) were UNREADABLE" in out, out


def test_the_worst_loss_is_the_LARGEST_share_not_the_smallest(tmp_path):
    """Three lossy files with three different shares, so max/min are distinguishable. The
    biggest share here is 7 of 9 rows; the smallest is 1 of 3."""
    out, _pulled, _body, _ = _run(tmp_path)
    assert "Worst single loss 7 rows, 77.7778% of it (weak_lossy2)" in out, out


def test_the_pulled_count_subtracts_all_THREE_of_its_terms(tmp_path):
    """R656 Finding 2. Round 15 tested one term of `len(names) - len(fail) - len(check_failed)
    - len(stale_files)`. Dropping either of the other two passed 59/59 and reported files as
    pulled that were correctly refused - verbatim what R628's own comment forbids."""
    out, pulled, _body, on_disk = _run(tmp_path)
    assert pulled == EXPECT["pulled"], f"pulled={pulled}, expected {EXPECT['pulled']}\n{out}"
    # AND IT EQUALS WHAT REACHED DISK, which is the claim the number actually makes.
    replaced = EXPECT["weak"] + EXPECT["whole_row"] + EXPECT["unreadable"]
    assert pulled == replaced, f"pulled={pulled} but {replaced} files were replaced"


def test_the_refused_files_are_all_still_on_disk_unchanged(tmp_path):
    """The counts above are only worth something if they describe the filesystem."""
    _out, _pulled, _body, on_disk = _run(tmp_path)
    for i in range(N_STALE):
        assert on_disk[f"stale{i}"] == 3, on_disk
    for i in range(N_CHECKFAIL):
        assert on_disk[f"checkfail{i}"] == 2, on_disk
    for i in range(N_FAIL):
        assert on_disk[f"fail{i}"] == 1, on_disk
    for i in range(N_WHOLE):
        assert on_disk[f"whole{i}"] == 3, on_disk


def test_the_ledger_survives_a_run_whose_only_notable_event_is_a_corrupt_replacement(tmp_path):
    """R656 Finding 3: the `unreadable_local` term of the retention rule had no test. Dropping
    it deletes the ledger holding the only record that a corrupt local file was replaced -
    R550's exact shape, 'the local copy was gone and the ledger was deleted for having nothing
    to report'."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "only.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"this is not a parquet file at all")
    old = time.time() - 7200
    os.utime(dest, (old, old))
    staged = {"only": str(tmp_path / "only.parquet")}
    pq.write_table(_keyless(["L1", "L2"]), staged["only"])
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["only", 0, 2]], "r2_only": [],
           "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            ms.sync_source(ManyS3(staged, set()), rec, apply=True)
        out = buf.getvalue()
        after = pq.read_metadata(dest).num_rows
    finally:
        ledgers = [os.path.join(ms.ROOT, "logs", f)
                   for f in os.listdir(os.path.join(ms.ROOT, "logs"))
                   if f.startswith("_mirror_sync_withdrawals_") and f.endswith(f"_{SRC}.tsv")]
        body = "".join(io.open(p, encoding="utf-8").read() for p in ledgers)
        for p in ledgers:
            os.remove(p)
        shutil.rmtree(dest_dir, ignore_errors=True)
    assert after == 2, "the corrupt file was not replaced"
    assert "have been replaced" in out, out
    assert "LOCAL COPY CORRUPT" in body, \
        "the run replaced a corrupt file and kept no durable record of it:\n" + repr(body)


def test_a_CORRUPT_local_file_whose_replace_is_refused_is_NAMED(tmp_path):
    """R656 Finding 7. The only line printed for this case was the generic `1 file(s) CHECK
    FAILED - local copies kept`, which is true and reassuring and omits the one thing that
    matters: the copy being kept is not a parquet file, so that series is DOWN until a later
    sync succeeds. `local kept` reads like a safe outcome; here it is the unsafe one."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "only.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"this is not a parquet file at all")
    old = time.time() - 7200
    os.utime(dest, (old, old))
    staged = {"only": str(tmp_path / "only.parquet")}
    pq.write_table(_keyless(["L1", "L2"]), staged["only"])
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["only", 0, 2]], "r2_only": [],
           "ahead": []}
    real_replace = os.replace

    def refuse(a, b, *args, **kw):
        if str(b).endswith("only.parquet"):
            raise PermissionError(13, "another process holds it")
        return real_replace(a, b, *args, **kw)

    ms.os.replace = refuse
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            pulled, _ahead = ms.sync_source(ManyS3(staged, set()), rec, apply=True)
        out = buf.getvalue()
        still = open(dest, "rb").read()
    finally:
        ms.os.replace = real_replace
        for f in os.listdir(os.path.join(ms.ROOT, "logs")):
            if f.startswith("_mirror_sync_withdrawals_") and f.endswith(f"_{SRC}.tsv"):
                os.remove(os.path.join(ms.ROOT, "logs", f))
        shutil.rmtree(dest_dir, ignore_errors=True)
    assert still == b"this is not a parquet file at all"
    assert pulled == 0, pulled
    assert "CORRUPT and could NOT be replaced" in out, \
        "the run kept an unreadable file and did not say so:\n" + out
    assert "have been replaced" not in out, out
