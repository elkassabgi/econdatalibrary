"""The shipped Python client must parse what the API actually serves (R601).

From 2026-07-09 (citation header on every .csv) to 2026-09-02 every fetch_series_csv() raised
pandas ParserError: the body starts with '#' comment lines and the client read it bare. These
tests pin the served shapes: the citation form, the bare form, the large-object form with the
completeness line, and a truncated large-object body.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "clients", "python"))

from econdl._http import EmptyBody, HttpResolveError, TruncatedTransfer, UnverifiableTransfer, _decode_body, _read_body, parse_series_csv  # noqa: E402

CITATION = (b"# ============================================================================\n"
            b"#  DATA CITATION - please credit the original source in any use or publication.\n"
            b"#  Source:    Test source\n"
            b"#  (Pipelines: pandas pd.read_csv(url, comment='#'), or append ?raw=1 for bare CSV.)\n"
            b"# ============================================================================\n")
ROWS = b"series_id,obs_date,value\nt:a,2020-01-01,1.5\nt:a,2020-02-01,2.5\nt:b,2020-01-01,3.0\n"


def test_citation_form_with_content_length_parses():
    df = parse_series_csv(CITATION + ROWS, content_length=str(len(CITATION + ROWS)))
    assert list(df.columns) == ["series_id", "obs_date", "value"] and len(df) == 3


def test_bare_form_parses():
    df = parse_series_csv(ROWS, content_length=len(ROWS))
    assert len(df) == 3


def test_large_object_form_requires_and_checks_the_completeness_line():
    body = CITATION + ROWS + b"# econdl-complete rows=3\n"
    df = parse_series_csv(body, content_length=None)
    assert len(df) == 3
    with pytest.raises(TruncatedTransfer):
        parse_series_csv(CITATION + ROWS, content_length=None)            # cut off before the line
    two_rows = b"series_id,obs_date,value\nt:a,2020-01-01,1.5\nt:a,2020-02-01,2.5\n"
    with pytest.raises(TruncatedTransfer):
        parse_series_csv(CITATION + two_rows + b"# econdl-complete rows=3\n", content_length=None)  # rows disagree


def test_bare_large_object_form_with_the_line_parses():
    body = ROWS + b"# econdl-complete rows=3\n"
    assert len(parse_series_csv(body, content_length=None)) == 3


def test_gzip_passthrough_shape_is_decoded_and_needs_no_marker():
    """R607: an unfiltered large object arrives as the stored gzip bytes with content-length and
    x-econdl-citation-omitted; the client must decode it and must not demand a marker."""
    import gzip
    gz = gzip.compress(ROWS)
    body = _decode_body(gz, {"content-encoding": "gzip"})
    assert body == ROWS
    assert _decode_body(gz, {}) == ROWS                       # by magic, header or not
    assert _decode_body(ROWS, {}) == ROWS                     # plain stays plain
    df = parse_series_csv(body, content_length=str(len(gz)), citation_omitted=True)
    assert len(df) == 3
    # inflated by an intermediary: no content-length, no marker -> UNVERIFIABLE, refused (R614)
    with pytest.raises(UnverifiableTransfer):
        parse_series_csv(ROWS, content_length=None, citation_omitted=True)
    # but a non-passthrough body without length still needs the marker
    with pytest.raises(TruncatedTransfer):
        parse_series_csv(ROWS, content_length=None, citation_omitted=False)


def test_hash_inside_a_series_id_survives_only_line_start_comments_are_stripped():
    body = CITATION + b"series_id,obs_date,value\nstatcan:1#Nova Scotia,2020-01-01,1.5\n" + b"# econdl-complete rows=1\n"
    df = parse_series_csv(body, content_length=None)
    assert list(df["series_id"]) == ["statcan:1#Nova Scotia"] and len(df) == 1


def test_empty_or_marker_only_body_is_an_empty_body_not_a_pandas_error():
    with pytest.raises(EmptyBody):
        parse_series_csv(b"", content_length="0")
    with pytest.raises(EmptyBody):
        parse_series_csv(CITATION + b"# econdl-complete rows=0\n", content_length=None)


def test_short_read_and_cut_gzip_are_truncated_transfers_not_stray_exceptions():
    """R613: http.client.IncompleteRead and a gzip EOFError must surface as HttpResolveError
    'truncated', so bundle()/pull() skip the series loudly instead of aborting."""
    import gzip
    import http.client

    class Cut:
        def read(self):
            raise http.client.IncompleteRead(b"partial-bytes")
    with pytest.raises(HttpResolveError) as ei:
        _read_body(Cut(), "http://x/y.csv")
    assert ei.value.error == "truncated"
    gz = gzip.compress(ROWS * 50)
    with pytest.raises(HttpResolveError) as ei2:
        _decode_body(gz[: len(gz) // 2], {"content-encoding": "gzip"})
    assert ei2.value.error == "truncated"
    # a valid gzip header followed by garbage deflate data: undecodable (not a cut stream)
    garbage = gzip.compress(b"x")[:10] + b"\xff" * 32
    with pytest.raises(HttpResolveError) as ei3:
        _decode_body(garbage, {})
    assert ei3.value.error == "undecodable"
    # a header-level defect (flags byte 0x00, then text where the deflate stream should be): undecodable
    with pytest.raises(HttpResolveError) as ei4:
        _decode_body(b"\x1f\x8b\x08\x00" + b"\x00" * 6 + b"not really gzip at all", {})
    assert ei4.value.error == "undecodable"


def test_a_stalled_read_and_a_reset_are_truncated_transfers():
    """R614: TimeoutError (a stalled socket) and ConnectionResetError must map to 'truncated'."""
    class Stalled:
        def read(self):
            raise TimeoutError("timed out")

    class Reset:
        def read(self):
            raise ConnectionResetError(10054, "reset")
    for stub in (Stalled(), Reset()):
        with pytest.raises(HttpResolveError) as ei:
            _read_body(stub, "http://x/y.csv")
        assert ei.value.error == "truncated"
