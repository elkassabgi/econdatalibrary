"""derive_one may now refresh a served series, and may not shrink one silently.

Two defects, both found while sizing the gus_dbw repair.

CREATE-ONLY. `head_object` succeeding short-circuited to `SKIP ... (already present)`
unconditionally, so the tool that exists to derive one series to R2 could not REFRESH one -
and the 16 gus_dbw areas serving a nine-day-old vintage could not be repaired with it at all.

NO MIRROR PREFLIGHT. It then PUT whatever it had just built over whatever was there. A
reviewer measured `gus_dbw:area_8` as 20,015 bytes SMALLER locally than its served copy, so a
refresh would have published a regression and printed OK - R383's shape, and precisely what
mirror_sync's never-shrink rule exists to stop in the other direction.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.derive_one import SHRINK_TOLERANCE, _existing_rows, shrink_verdict  # noqa: E402


def head(size=None, rows=None, pre_fix=False):
    """An R2 HEAD response.

    A POST-FIX object carries BOTH `rows` and `bytes`. A PRE-FIX one (the 105 published on
    2026-09-02) carries only `rows`, and that value is really a BYTE count - which is why the
    guard must not read it as rows. `pre_fix=True` builds that shape."""
    h = {}
    if size is not None:
        h["ContentLength"] = size
    if rows is not None:
        h["Metadata"] = ({"rows": str(rows)} if pre_fix
                         else {"rows": str(rows), "bytes": str(size if size is not None else 0)})
    return h


def test_a_first_upload_cannot_shrink_anything():
    ok, why = shrink_verdict(100, 1000, None)
    assert ok and "no existing object" in why


def test_fewer_rows_than_the_served_copy_is_REFUSED():
    ok, why = shrink_verdict(90, 900, head(size=1000, rows=100))
    assert not ok
    assert "10 fewer" in why, why


def test_equal_or_more_rows_is_allowed():
    assert shrink_verdict(100, 900, head(size=1000, rows=100))[0]
    assert shrink_verdict(101, 900, head(size=1000, rows=100))[0]


def test_rows_beat_bytes_when_both_are_known():
    """A gzip that compresses better is not a loss. With row counts on both sides the byte
    comparison must not be consulted at all, or a better-compressed refresh gets refused."""
    ok, why = shrink_verdict(200, 10, head(size=1_000_000, rows=100))
    assert ok, why
    assert "rows" in why and "%" not in why


def test_a_served_copy_with_no_row_count_falls_back_to_bytes():
    """Every object uploaded before today. gus_dbw:area_8's real numbers."""
    ok, why = shrink_verdict(None, 1_000_000 - 20_015, head(size=1_000_000))
    assert not ok
    assert "2.00% smaller" in why, why


def test_a_byte_drop_inside_the_tolerance_passes():
    """gzip over nearly-identical CSV is stable to well under a percent, so a hair's
    difference must not block an otherwise good refresh."""
    ok, _why = shrink_verdict(None, int(1_000_000 * (1 - SHRINK_TOLERANCE / 2)),
                              head(size=1_000_000))
    assert ok


def test_nothing_comparable_means_no_opinion():
    assert shrink_verdict(None, None, head(size=1000))[0]
    assert shrink_verdict(None, 500, head())[0]


def test_the_row_metadata_is_read_from_either_header_form():
    """Both spellings, and only on an object that also carries `bytes` - without it the value
    is a pre-fix byte count and reading it as rows is the bug this guard was corrected for."""
    assert _existing_rows({"Metadata": {"rows": "7", "bytes": "99"}}) == 7
    assert _existing_rows({"Metadata": {"x-amz-meta-rows": "7", "bytes": "99"}}) == 7
    assert _existing_rows({"Metadata": {"rows": "not a number", "bytes": "99"}}) is None
    assert _existing_rows({"Metadata": {"rows": "7"}}) is None      # pre-fix: bytes, not rows
    assert _existing_rows({}) is None
    assert _existing_rows(None) is None


def test_the_refresh_flag_exists_and_the_default_still_skips():
    """A --force that main() does not read is not a flag, and changing the DEFAULT would
    silently make every existing caller re-derive its whole queue."""
    import inspect

    import tools.derive_one as m
    src = inspect.getsource(m.main)
    assert '"--force" in sys.argv' in src, "main() does not read --force"
    assert "if head and not force:" in src, "the skip is no longer conditional on --force"


def test_the_put_helper_carries_metadata_to_boto3():
    """The row count is only exact for FUTURE comparisons if it actually reaches the object."""
    import inspect

    from core import derive_csv as d
    sig = inspect.signature(d._put_gzip_file_with_backoff)
    assert "metadata" in sig.parameters, sig
    src = inspect.getsource(d._put_gzip_file_with_backoff)
    assert "Metadata=(metadata or {})" in src, "metadata never reaches put_object"


def test_a_PRE_FIX_object_is_not_read_as_a_row_count():
    """The 105 objects published on 2026-09-02 carry a BYTE count under the `rows` key,
    because `_series_csv_to_file_sorted` returns os.path.getsize and the first version of the
    caller bound it to a variable it then passed as `new_rows` and stored as `rows`.

    Comparing a new ROW count against a stored BYTE count would refuse almost every refresh,
    since a CSV is many bytes per row. A pre-fix object is identified by carrying `rows` and no
    `bytes`, and is treated as having no row count at all - so the guard falls back to the byte
    comparison, which is what those stored numbers actually are."""
    pre = {"ContentLength": 1_000_000, "Metadata": {"rows": "1000000"}}
    assert _existing_rows(pre) is None, "a pre-fix byte count was read as a row count"
    ok, why = shrink_verdict(50_000, 995_000, pre)
    assert ok, why
    assert "bytes" in why, why


def test_a_POST_FIX_object_carries_both_and_rows_win():
    post = {"ContentLength": 1_000_000, "Metadata": {"rows": "50000", "bytes": "1000000"}}
    assert _existing_rows(post) == 50_000
    ok, why = shrink_verdict(49_999, 999_999, post)
    assert not ok and "1 fewer" in why, why
    ok2, why2 = shrink_verdict(50_001, 10, post)
    assert ok2, "a better-compressed refresh with MORE rows was refused"


def test_the_byte_metadata_reader_exists_and_parses():
    from tools.derive_one import _existing_bytes_meta
    assert _existing_bytes_meta({"Metadata": {"bytes": "42"}}) == 42
    assert _existing_bytes_meta({"Metadata": {"bytes": "x"}}) is None
    assert _existing_bytes_meta({}) is None
def test_main_passes_ROWS_as_rows_and_BYTES_as_bytes(tmp_path, monkeypatch, capsys):
    """The call site, not the helper. Two mutations survived every other test in this file -
    swapping the two arguments, and dropping `bytes` from the recorded metadata - because
    nothing here called main(). The numbers below are far apart on purpose: a swap cannot pass.
    """
    import tools.derive_one as m
    from core import derive_csv as d
    from updater import blob as b

    ROWS, BYTES = 4_242, 9_000_001
    captured = {}

    class FakeClient:
        def head_object(self, **kw):
            raise RuntimeError("no such key")        # a first upload

    class FakeR2:
        client = FakeClient()

    def fake_put(client, bucket, key, path, metadata=None):
        captured["metadata"] = dict(metadata or {})
        captured["key"] = key

    monkeypatch.setattr(b, "R2Blob", FakeR2)
    monkeypatch.setattr(m, "_row_count", lambda _sid: ROWS)
    monkeypatch.setattr(d, "_series_csv_to_file_sorted", lambda _sid, out: BYTES)
    monkeypatch.setattr(d, "_put_gzip_file_with_backoff", fake_put)
    monkeypatch.setattr(m.sys, "argv", ["derive_one.py", "probe:one", "--force"])

    rc = m.main()
    assert rc == 0, rc
    meta = captured.get("metadata")
    assert meta, "nothing was uploaded"
    assert meta.get("rows") == str(ROWS), f"rows landed as {meta.get('rows')!r}"
    assert meta.get("bytes") == str(BYTES), f"bytes landed as {meta.get('bytes')!r}"

    out = capsys.readouterr().out
    assert f"{BYTES} bytes" in out, out
    assert f"{ROWS:,} rows" in out, out


def test_main_REFUSES_when_the_new_object_would_lose_rows(tmp_path, monkeypatch, capsys):
    """And the refusal reaches the call site too: nothing is uploaded."""
    import tools.derive_one as m
    from core import derive_csv as d
    from updater import blob as b

    uploaded = []

    class FakeClient:
        def head_object(self, **kw):
            return {"ContentLength": 9_000_000,
                    "Metadata": {"rows": "5000", "bytes": "9000000"}}

    class FakeR2:
        client = FakeClient()

    monkeypatch.setattr(b, "R2Blob", FakeR2)
    monkeypatch.setattr(m, "_row_count", lambda _sid: 4_000)          # 1,000 fewer
    monkeypatch.setattr(d, "_series_csv_to_file_sorted", lambda _sid, out: 8_900_000)
    monkeypatch.setattr(d, "_put_gzip_file_with_backoff",
                        lambda *a, **k: uploaded.append(k))
    monkeypatch.setattr(m.sys, "argv", ["derive_one.py", "probe:one", "--force"])

    assert m.main() == 0
    out = capsys.readouterr().out
    assert "REFUSE" in out and "1,000 fewer" in out, out
    assert not uploaded, "a regression was uploaded despite the refusal"
