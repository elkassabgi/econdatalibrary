"""What the OPERATOR sees, captured from a real sync_source() run.

An earlier version of this asserted that `inspect.getsource(sync_source)` contained two
substrings - which passes even if the print it is looking for is unreachable. These run the
function with only the network boundary stubbed and read its stdout, so an unreachable line
fails the test.

Four things a run must never do silently, all of them measured defects in this file's history:
serve a whole-row replacement without saying it had no key column (R617), destroy rows without
a withdrawal ledger (R550), consume a classification that no longer describes the object
(R624), and overwrite a file that is DIVERGED rather than behind (R631, and R388 before it).
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

SRC = "_testprobe"
ROOTDIR = "_testdata"


class StubS3:
    """Only the network boundary is replaced; every line of mirror_sync's logic runs."""

    def __init__(self, staged):
        self.staged = staged

    def download_file(self, bucket, key, dest):
        shutil.copyfile(self.staged, dest)


def _run(tmp_path, local_table, incoming_table, behind_entry):
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    pq.write_table(local_table, os.path.join(dest_dir, "f.parquet"))
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(incoming_table, staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [behind_entry], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    _run.pulled = None
    try:
        with contextlib.redirect_stdout(buf):
            # THE NUMBER THE RUN IS JUDGED BY. `sync_source` returns the pulled count that
            # main() prints, and R628 exists because that arithmetic once counted files it had
            # refused. Four call sites in this suite called it and not one bound the result, so
            # dropping the `check_failed` term - R628's own fix - survived all 45 tests (R652).
            # sync_source returns (pulled, ahead); the pulled count is what main() prints.
            _run.pulled = ms.sync_source(StubS3(staged), rec, apply=True)[0]
    finally:
        out = buf.getvalue()
        # THE FILESYSTEM, NOT JUST THE NARRATION. With os.replace no-op'd, the two positive
        # tests still passed: one asserted a ledger line that is written whenever the replace
        # does not raise, the other asserted only an absence, which "did nothing at all" also
        # satisfies (R637). The row count of the file that is actually on disk afterwards is
        # what those tests are about.
        after = None
        f = os.path.join(dest_dir, "f.parquet")
        if os.path.exists(f):
            after = pq.read_metadata(f).num_rows
        shutil.rmtree(dest_dir, ignore_errors=True)
    return out, after


def _keyless(values, year=2020):
    """No key column, and a date axis so classify() has both of its axes."""
    return pa.table({
        "exporter": values,
        "obs_date": pa.array([dt.date(year, 1, 1)] * len(values), pa.date32()),
        "value": [1.0] * len(values)})


def _ledgers():
    return [f for f in os.listdir(os.path.join(ms.ROOT, "logs"))
            if f.startswith("_mirror_sync_withdrawals_") and f.endswith(f"_{SRC}.tsv")]


def _read_and_clear():
    bodies = []
    for f in _ledgers():
        p = os.path.join(ms.ROOT, "logs", f)
        bodies.append(io.open(p, encoding="utf-8").read())
        os.remove(p)
    return bodies


def test_a_whole_row_replacement_says_so_and_leaves_a_ledger(tmp_path):
    """R2 is genuinely behind-worthy here: more rows AND a later date, so classify says
    'behind' - and every one of the local rows is still replaced."""
    out, after = _run(tmp_path,
                      _keyless(["FRA", "DEU", "ITA"], year=2020),
                      _keyless(["BRA", "IND", "JPN", "KOR"], year=2021),
                      ["f", 3, 4])
    assert "no key column" in out, out
    assert "ROWS" in out, out
    assert after == 4, f"the incoming 4-row file did not reach disk (found {after})"
    bodies = _read_and_clear()
    assert bodies, "a run that destroyed rows kept no ledger"
    assert any("intent" in b and "replaced" in b for b in bodies), bodies


def test_a_file_that_is_DIVERGED_is_refused_not_overwritten(tmp_path):
    """R631: the object has MORE rows but an EARLIER date, which classify files as 'ahead' -
    the merge queue. A rule that admitted anything with more rows destroyed the local file's
    later observations and logged it as 'replaced (publisher ahead)'."""
    out, after = _run(tmp_path,
                      _keyless(["FRA", "DEU", "ITA"], year=2021),
                      _keyless(["BRA", "IND", "JPN", "KOR", "MEX", "CAN"], year=2017),
                      ["f", 3, 4])
    assert "STALE CLASSIFICATION" in out or "no longer classify" in out, out
    assert after == 3, f"the diverged local file was replaced (now {after} rows)"
    bodies = _read_and_clear()
    assert any("STALE CLASSIFICATION" in b and "ahead" in b for b in bodies), bodies


def test_a_file_that_did_not_change_at_all_is_refused(tmp_path):
    """'same' is not 'behind' either: the sweep said this file was behind and it is not."""
    same = _keyless(["FRA", "DEU", "ITA"], year=2020)
    out, after = _run(tmp_path, same, same, ["f", 3, 3])
    assert "STALE CLASSIFICATION" in out or "no longer classify" in out, out
    assert after == 3, after
    assert any("'same'" in b for b in _read_and_clear())


def _keyed(values, year=2020, value=1.0):
    """A KEYED file with no date column: classify() has one axis, the identity has one column."""
    return pa.table({"series_key": values, "value": [value] * len(values)})


def test_a_RENAME_in_a_keyless_registry_still_syncs(tmp_path):
    """R649: real gleif has columns LEI, LegalName, ... - no `series_key` - so it takes the
    WHOLE-ROW path and has no date axis. One renamed entity in a 3,416,994-entity registry made
    `lost` non-zero, and the one-axis-with-a-loss refusal then meant it could never sync again
    after any refresh. The fixture that was supposed to guard R551 used a `series_key` column
    and so exercised the key-only path instead - green while the defect it names was live."""
    local = pa.table({"LEI": ["L1", "L2"], "LegalName": ["ACME", "STABLE"]})
    incoming = pa.table({"LEI": ["L1", "L2", "L3"],
                         "LegalName": ["ACME CORP", "STABLE", "THIRD"]})   # renamed + added
    out, after = _run(tmp_path, local, incoming, ["f", 2, 3])
    assert after == 3, f"a registry refresh was refused (rows now {after})"
    assert "WEAK COMPARISON" in out.upper(), out
    bodies = _read_and_clear()
    assert bodies, "a run that replaced rows on a one-axis verdict kept no ledger"
    assert any("may be a withdrawal OR an attribute change" in b for b in bodies), bodies


def test_a_weak_identity_sync_with_NO_loss_still_leaves_a_ledger(tmp_path):
    """R649: `weak_identity` was collected and printed but never recorded, and the retention
    rule deleted the ledger unless there were withdrawals, check failures, failures or stale
    files. So the one case where a silent restatement is possible BY CONSTRUCTION - a keyed,
    dateless file that loses nothing - left no durable record at all.

    Note this fixture loses NOTHING: with a loss the withdrawal path keeps the ledger anyway,
    which is how my first version of this passed with the retention rule reverted."""
    out, after = _run(tmp_path,
                      _keyed(["a", "b"], value=1.0),
                      _keyed(["a", "b", "c"], value=1.0),      # both local rows survive verbatim
                      ["f", 2, 3])
    assert after == 3, f"a clean behind file was not synced (rows now {after})"
    assert "WEAK COMPARISON" in out.upper(), out
    bodies = _read_and_clear()
    assert bodies, "the weak-identity run deleted its own ledger"
    assert any("key-only identity" in b for b in bodies), bodies


def test_a_keyed_dateless_file_is_replaced_but_the_weakness_is_REPORTED(tmp_path):
    """R645/R647: a key-only identity reports 0 lost for a revised VALUE under an unchanged key,
    and these files have no date axis either, so neither signal can tell 'behind' from
    'restated'.

    Neither offered remedy is safe. Refusing is a permanent outage - the condition is a property
    of the file, so a footer_diff re-run re-derives it for ever. Whole-row re-opens R551, where
    comparing gleif (a key, no date) on more than the key reported 6,817 losses with 0 LEIs
    gone. So it proceeds, and the run SAYS the identity was weak."""
    out, after = _run(tmp_path,
                      _keyed(["a", "b"], value=1.0),
                      _keyed(["a", "b", "c"], value=99.0),      # values restated AND a row added
                      ["f", 2, 3])
    assert after == 3, f"a genuine behind file was not synced (rows now {after})"
    assert "WEAK COMPARISON" in out.upper(), out
    assert any("key-only identity" in b for b in _read_and_clear()), "no durable record"


def test_the_weak_disclosure_is_written_as_INTENT_before_the_replace(tmp_path):
    """R612, third recurrence in this file (R650). Both disclosure lines said "replaced" at
    DECISION time, so a replace that then failed left a ledger claiming the file had been
    overwritten when it had not. A held handle raises PermissionError here - which is the whole
    reason the open-handle probe exists - so this is the ordinary case, not a contrived one.

    The withdrawal path already had it right: intent before, outcome after. This asserts the
    weak-comparison path does the same."""
    real_replace = os.replace
    calls = []

    def refuse(a, b, *args, **kw):
        if str(b).endswith("f.parquet"):
            calls.append(b)
            raise PermissionError(13, "another process holds it")
        return real_replace(a, b, *args, **kw)

    ms.os.replace = refuse
    try:
        out, after = _run(tmp_path,
                          _keyed(["a", "b"], value=1.0),
                          _keyed(["a", "c", "d"], value=99.0),  # 'b' lost, values restated
                          ["f", 2, 3])
    finally:
        ms.os.replace = real_replace
    assert calls, "the test never reached os.replace, so it proves nothing"
    assert after == 2, f"the local file changed despite a failed replace (rows now {after})"
    body = "".join(_read_and_clear())
    assert "WEAK COMPARISON - about to replace" in body, body
    assert "WEAK COMPARISON - replaced" not in body, \
        "the ledger claims a replacement that os.replace refused:\n" + body
    assert "replace FAILED" in body, body


def test_one_weak_file_produces_ONE_record_not_three(tmp_path):
    """R650. `weak_identity`, `one_axis_loss` and `one_axis` overlapped BY CONSTRUCTION: a keyed,
    dateless file that loses a row is all three at once, and it produced five printed lines,
    five counts and four ledger lines - three saying replaced and one saying kept. An operator
    summing the summary counted five files where there was one."""
    out, after = _run(tmp_path,
                      _keyed(["a", "b"], value=1.0),
                      _keyed(["a", "c", "d"], value=99.0),     # one_axis AND key-only AND a loss
                      ["f", 2, 3])
    assert after == 3, after
    body = "".join(_read_and_clear())
    assert body.count("WEAK COMPARISON - about to replace") == 1, body
    assert body.count("WEAK COMPARISON - replaced") == 1, body
    assert out.upper().count("WEAK COMPARISON") == 1, \
        "one file was summarised as more than one population:\n" + out
    # and the loss is a FRACTION: "1 row" and "3,400,000 rows" printed the same shape, where
    # 0.0000% reads as attribute churn and 100% reads as wholesale replacement.
    assert "1 of 2 local rows lost (50.0000%)" in body, body
    # THE SENTENCE MUST CLAIM WHAT HAPPENED. Round 15 makes the tense safe by ORDERING - the
    # list is filled only after os.replace - but the reviewer's point stands on its own: with
    # nothing pinning the verb, "were REPLACED" -> "were CONSIDERED" passed all 45 tests, and a
    # summary that asserts nothing tells an operator nothing (R652 Finding 4a).
    assert "of those, 1 were REPLACED on a WEAK COMPARISON" in out, out


def test_the_SUMMARY_never_claims_a_replacement_os_replace_refused(tmp_path):
    """R612's FOURTH recurrence in this file (R652). Round 14 ordered the two ledger lines
    against `os.replace` and left the three PRINTED summary lines counting lists that were
    appended at DECISION time. With the replace refused - which a held handle does, and is why
    the open-handle probe exists - stdout said "were REPLACED", "lost N ROWS ... followed the
    publisher" and "have been replaced", over a file still holding every original row.

    This asserts the ORDERING, not the wording: with the lists filled only after the act, no
    phrasing of those sentences can outrun it."""
    real_replace = os.replace

    def refuse(a, b, *args, **kw):
        if str(b).endswith("f.parquet"):
            raise PermissionError(13, "another process holds it")
        return real_replace(a, b, *args, **kw)

    ms.os.replace = refuse
    try:
        out, after = _run(tmp_path,
                          _keyed(["a", "b"], value=1.0),
                          _keyed(["a", "c", "d"], value=99.0),
                          ["f", 2, 3])
    finally:
        ms.os.replace = real_replace
    assert after == 2, f"the local file changed despite a refused replace (rows now {after})"
    assert "WEAK COMPARISON" not in out.upper(), \
        "the summary counts a weak replacement that was refused:\n" + out
    assert "followed the publisher" not in out, \
        "the summary reports a withdrawal that never reached disk:\n" + out
    assert "have been replaced" not in out, out
    assert _run.pulled == 0, f"a refused replace was counted as pulled ({_run.pulled})"


def test_an_UNREADABLE_local_file_is_only_summarised_once_it_is_gone(tmp_path):
    """R641's headline defect returning on the failure path (R652): "N local file(s) were
    UNREADABLE and have been replaced" printed while `this is not a parquet file at all` was
    still the content on disk. `unreadable_local` is appended after the replace now."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "f.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"this is not a parquet file at all")
    os.utime(dest, (time.time() - 3600, time.time() - 3600))
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(_keyless(["FRA", "DEU"], year=2021), staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["f", 0, 2]], "r2_only": [], "ahead": []}
    real_replace = os.replace

    def refuse(a, b, *args, **kw):
        if str(b).endswith("f.parquet"):
            raise PermissionError(13, "another process holds it")
        return real_replace(a, b, *args, **kw)

    ms.os.replace = refuse
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            pulled = ms.sync_source(StubS3(staged), rec, apply=True)[0]
        out = buf.getvalue()
        still = open(dest, "rb").read()
    finally:
        ms.os.replace = real_replace
        shutil.rmtree(dest_dir, ignore_errors=True)
        _read_and_clear()
    assert still == b"this is not a parquet file at all", "the corrupt bytes are gone"
    assert "have been replaced" not in out, \
        "the summary says a corrupt file was replaced, over the corrupt bytes:\n" + out
    assert pulled == 0, f"a refused replace was counted as pulled ({pulled})"


def test_a_keyed_file_WITH_a_date_axis_carries_no_weakness_caveat(tmp_path):
    """R652: `if n in one_axis:` mutated to `if True:` survived all 45 tests, because no fixture
    put a keyed, DATED file through the weak block. Every compared file in the fleet would then
    carry a false "no date axis" caveat in the durable record."""
    def keyed_dated(values, year):
        return pa.table({"series_key": values,
                         "obs_date": pa.array([dt.date(year, 1, 1)] * len(values), pa.date32()),
                         "value": [1.0] * len(values)})

    out, after = _run(tmp_path, keyed_dated(["a", "b"], 2020),
                      keyed_dated(["a", "b", "c"], 2021), ["f", 2, 3])
    assert after == 3, f"a plainly behind file was not synced (rows now {after})"
    body = "".join(_read_and_clear())
    assert "WEAK COMPARISON" not in out.upper(), out
    assert "no date axis" not in body, \
        "a file with an obs_date column was recorded as having no date axis:\n" + body
    assert _run.pulled == 1, f"a clean pull was not counted ({_run.pulled})"


def test_a_file_whose_STATISTICS_are_absent_is_not_called_key_only(tmp_path):
    """R652: `one_axis` fires on TWO causes - no recognised date column, OR the column present
    with no row-group statistics (mirror_sync's docstring measures 1,980 and 4). On the second
    the identity IS (key, date), so gating the caveat on "not WHOLE ROW" made the ledger's own
    `mode` column read `(series_key, obs_date), copy-aware` while its `outcome` column on the
    SAME row said "key-only identity". A false statement in what the code calls the durable
    record."""
    def no_stats(path, values, year):
        t = pa.table({"series_key": values,
                      "obs_date": pa.array([dt.date(year, 1, 1)] * len(values), pa.date32()),
                      "value": [1.0] * len(values)})
        pq.write_table(t, path, write_statistics=False)

    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    no_stats(os.path.join(dest_dir, "f.parquet"), ["a", "b"], 2020)
    staged = str(tmp_path / "incoming.parquet")
    no_stats(staged, ["a", "c", "d"], 2021)               # 'b' lost, so the file is weak
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["f", 2, 3]], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            ms.sync_source(StubS3(staged), rec, apply=True)
        out = buf.getvalue()
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
        body = "".join(_read_and_clear())
    assert "no date axis" in body, "the missing statistics were not disclosed at all:\n" + body
    assert "obs_date" in body, "the identity did not use the date column:\n" + body
    assert "key-only identity" not in body, \
        "a (key, date) identity was recorded as key-only:\n" + body
    assert "WEAK COMPARISON" in out.upper(), out


def test_the_summary_reports_the_worst_loss_as_a_SHARE(tmp_path):
    """R650 rule (4), applied to the ledger and not to the always-firing warning it was written
    for (R652). "Worst single loss 2 rows" was 100% of that file, and this line fires on every
    run that touches one of the 690 keyed dateless files."""
    out, after = _run(tmp_path,
                      _keyed(["a", "b"], value=1.0),
                      _keyed(["a", "c", "d"], value=99.0),
                      ["f", 2, 3])
    assert after == 3, after
    _read_and_clear()
    assert "50.0000% of it" in out, \
        "the worst loss is printed as a bare count, with no magnitude:\n" + out


def test_a_dateless_file_with_a_WHOLE_ROW_identity_and_no_loss_is_not_a_second_population(tmp_path):
    """The de-duplication must not go the other way and hide a real disclosure. This file has no
    date axis, so it IS reported on the one_axis line - but its identity is whole-row, which
    cannot miss a replaced row, and it loses nothing. There is no second thing to say about it,
    and saying one anyway is what produced the five-lines-for-one-file summary."""
    out, after = _run(tmp_path,
                      pa.table({"LEI": ["L1", "L2"], "LegalName": ["ACME", "STABLE"]}),
                      pa.table({"LEI": ["L1", "L2", "L3"],
                                "LegalName": ["ACME", "STABLE", "THIRD"]}),
                      ["f", 2, 3])
    assert after == 3, f"a clean registry refresh was refused (rows now {after})"
    assert "ROW COUNT ALONE" in out.upper(), out
    assert "WEAK COMPARISON" not in out.upper(), \
        "a whole-row, lossless file was reported as a weak comparison:\n" + out


def test_a_corrupt_local_file_is_actually_REPLACED_not_just_announced(tmp_path):
    """R641: the fix announced "have been replaced" and replaced nothing - the identity check
    re-entered the same corrupt bytes, raised, and returned before os.replace."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "f.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"this is not a parquet file at all")
    os.utime(dest, (time.time() - 3600, time.time() - 3600))     # not a write in progress
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(_keyless(["FRA", "DEU", "ITA"], year=2021), staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["f", 0, 3]], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            ms.sync_source(StubS3(staged), rec, apply=True)
        out = buf.getvalue()
        after = pq.read_metadata(dest).num_rows
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
        _read_and_clear()
    assert "CORRUPT" in out.upper(), out
    assert after == 3, f"the corrupt file was announced as replaced and was not (rows={after})"
    # and the summary SAYS so, in the completed tense, because here it IS completed. Softening
    # it to "might be" passed all 45 tests (R652 Finding 4b). Paired with the refused-replace
    # test above, this pins both halves: the claim is made when it is true, and not when it
    # is not.
    assert "have been replaced" in out, out


def test_a_local_file_being_written_RIGHT_NOW_is_left_alone(tmp_path):
    """R641: a truncated file raises the identical ArrowInvalid as a corrupt one. The only
    thing that tells them apart is how recently it was touched."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "f.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"PAR1 partial write in progress")             # mtime is NOW
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(_keyless(["FRA"], year=2021), staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["f", 0, 1]], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            ms.sync_source(StubS3(staged), rec, apply=True)
        out = buf.getvalue()
        body = open(dest, "rb").read()
    finally:
        shutil.rmtree(dest_dir, ignore_errors=True)
        _read_and_clear()
    assert b"partial write in progress" in body, "a file being written was overwritten"
    assert "CHECK FAILED" in out or "local kept" in out.lower(), out


def test_a_file_with_an_OPEN_HANDLE_is_left_alone_even_if_it_looks_old(tmp_path):
    """R647: the open-handle probe replaced mtime as the PRIMARY signal and had no test of its
    own - every fixture closed its file first, so only the mtime fallback was ever exercised.
    This one holds the handle open across the whole run AND back-dates the file, so mtime says
    'old' and only the probe can save it."""
    dest_dir = os.path.join(ms.ROOT, "data", ROOTDIR, SRC)
    shutil.rmtree(dest_dir, ignore_errors=True)
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, "f.parquet")
    with open(dest, "wb") as fh:
        fh.write(b"PAR1 a writer still holds this")
    old = time.time() - 7200
    os.utime(dest, (old, old))                     # mtime says: not a write in progress
    staged = str(tmp_path / "incoming.parquet")
    pq.write_table(_keyless(["FRA"], year=2021), staged)
    rec = {"source": SRC, "root": ROOTDIR, "behind": [["f", 0, 1]], "r2_only": [], "ahead": []}
    buf = io.StringIO()
    holder = open(dest, "rb")                      # the handle a real writer would hold
    try:
        with contextlib.redirect_stdout(buf):
            ms.sync_source(StubS3(staged), rec, apply=True)
        out = buf.getvalue()
        body = open(dest, "rb").read()
    finally:
        holder.close()
        shutil.rmtree(dest_dir, ignore_errors=True)
        _read_and_clear()
    assert b"a writer still holds this" in body, "a file with an open handle was replaced"
    assert "being written right now" in out.lower(), out


def test_a_genuine_behind_file_with_no_loss_prints_nothing_alarming(tmp_path):
    """The incoming copy CONTAINS both local rows verbatim and adds a later one, so it is
    behind-worthy on both axes and nothing is withdrawn."""
    local = pa.table({
        "exporter": ["FRA", "DEU"],
        "obs_date": pa.array([dt.date(2020, 1, 1)] * 2, pa.date32()),
        "value": [1.0, 1.0]})
    incoming = pa.table({
        "exporter": ["FRA", "DEU", "ITA"],
        "obs_date": pa.array([dt.date(2020, 1, 1), dt.date(2020, 1, 1), dt.date(2021, 1, 1)],
                             pa.date32()),
        "value": [1.0, 1.0, 1.0]})
    out, after = _run(tmp_path, local, incoming, ["f", 2, 3])
    assert "ROWS" not in out and "STALE" not in out, out
    assert after == 3, f"the incoming 3-row file did not reach disk (found {after})"
    assert not _ledgers(), f"a clean run kept a ledger: {_ledgers()}"
