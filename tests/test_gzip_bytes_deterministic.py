"""This repo's two gzip writers must agree with each other.

SCOPE, stated first because the earlier version of this file overstated it. These tests pin an
INTRA-REPO invariant: `core/derive_csv.py` writes some objects with `GzipFile` and others with
`gzip.compress`, and on Python 3.11 those disagreed. `GzipFile._write_gzip_header` writes
b"\\377" unconditionally; 3.11's `gzip.compress` returns `zlib.compress(data, level, wbits=31)`
untouched when mtime == 0, leaving zlib's build platform in the header's OS byte (3 on the
Linux runners). Python 3.14 forces 255. `core.r2_util.gzip_bytes` normalises it so both paths
match on every interpreter.

WHAT THESE TESTS CANNOT SEE, and the earlier docstring claimed they did. They do NOT show that
the same CSV becomes the same object on every machine. Every comparison below runs in ONE
interpreter linking ONE zlib, so cross-implementation divergence is structurally invisible to
them - and that divergence is real: the desktop's 3.14 links zlib-ng ("1.3.1.zlib-ng") and the
3.11 runners link stock zlib 1.3.1, producing 787,922 bytes against 788,191 for the same input
at level 9. Measured on 90 real bucket objects, each population was reproducible only by the
compressor that wrote it.

So byte-identity of a compressed stream is NOT a portable invariant, and the skip guard does
not rely on it any more: `updater/blob.py` records the MD5 of the CSV BEFORE compression in
object metadata and compares that, which is the same number on every machine, Python and zlib.
`tests/test_blob_plain_digest.py` is where that is tested, with a genuinely different
compressor standing in for the other machine.

Nothing that DECOMPRESSES ever noticed any of this, which is why it survived.
`tools/verify_source_served.py` gunzips before comparing (line 204), so its byte-compare passed
throughout.
"""
from __future__ import annotations

import gzip
import io
import os
import sys
import zlib

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.r2_util import gzip_bytes  # noqa: E402

SAMPLE = (b"series_id,obs_date,value\n"
          b"IDB:social-indicators:tasa_ffaa:PRY,2002-12-31,0.00121916734\n"
          b"IDB:social-indicators:tasa_ffaa:PRY,2003-12-31,0.00234517901\n") * 40


def test_the_os_byte_is_always_255():
    out = gzip_bytes(SAMPLE)
    assert out[:2] == b"\x1f\x8b", "not a gzip stream"
    assert out[9] == 0xFF, (
        "OS byte is %d; a Linux runner writing 3 here is the whole defect" % out[9])


def test_it_round_trips():
    assert gzip.decompress(gzip_bytes(SAMPLE)) == SAMPLE


def test_it_is_deterministic():
    assert gzip_bytes(SAMPLE) == gzip_bytes(SAMPLE)


def test_it_matches_the_streaming_writer():
    """core/derive_csv.py writes some objects with GzipFile and others in memory.

    GzipFile writes b"\\377" unconditionally on every Python, so the streaming path was always
    portable. If the in-memory path does not agree with it byte for byte, the two writers in
    this one repo produce different objects for the same series - which is the same defect,
    just inside the codebase instead of across machines.
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0, filename="") as gz:
        gz.write(SAMPLE)
    assert gzip_bytes(SAMPLE) == buf.getvalue(), (
        "the in-memory and streaming gzip writers disagree; headers %s vs %s"
        % (gzip_bytes(SAMPLE)[:10].hex(), buf.getvalue()[:10].hex()))


def test_the_normalisation_touches_only_the_os_byte_WITHIN_ONE_INTERPRETER():
    """The OPERATION is surgical. That is all this shows.

    It compares `zlib.compress(wbits=31)` with `gzip_bytes` in the SAME process linking the
    SAME zlib, so of course the deflate payloads match. It says nothing whatever about another
    machine's compressor, and the previous version of this test asserted otherwise in its own
    failure message - claiming to rule out a difference it could not observe.

    The portable comparison lives in tests/test_blob_plain_digest.py.
    """
    raw = zlib.compress(SAMPLE, 9, 31)
    out = gzip_bytes(SAMPLE)
    assert len(raw) == len(out)
    assert raw[:9] == out[:9], "header before the OS byte differs"
    assert raw[10:] == out[10:], (
        "same interpreter, same zlib, yet the payloads differ - that would mean gzip_bytes is "
        "doing more than rewriting one header byte")
    assert out[9] == 0xFF


def test_an_already_255_stream_is_returned_unchanged():
    """The fast path must not corrupt the common case."""
    out = gzip_bytes(SAMPLE)
    assert gzip_bytes(gzip.decompress(out)) == out


def test_empty_input_does_not_index_past_the_end():
    out = gzip_bytes(b"")
    assert gzip.decompress(out) == b""
    assert out[9] == 0xFF


def test_the_gzipping_write_sites_use_the_helper():
    """A new bare `gzip.compress(...)` on one of these paths reintroduces the split.

    NAMED FOR WHAT IT CHECKS. This was called "every R2 write site" and inspected five files.
    At least FIFTEEN other tools PUT `series/<id>.csv` into the same bucket with no gzip and no
    ContentEncoding: derive_pxweb_flowgrain.py:133, derive_unsdg_flows.py:136,
    derive_noaa_missing.py:154, flowgrain_insee_melodi.py:146, flowgrain_ons_uk.py:122,
    derive_dip_tables.py:69, derive_imts_tables.py:77, derive_mfs_tables.py:71,
    derive_pip_tables.py:68, derive_census_tables.py:253, derive_ilostat_indicators.py:201,
    derive_istat_flows.py:267, derive_usda_tables.py:154, _derive_bea_bulk.py:182 and
    refresh_sec_edgar.py:538. None of that is addressed here.

    TWO THINGS THIS DOCSTRING USED TO CLAIM, BOTH MEASURED FALSE. It said those tools "produce
    the still-plain majority of the bucket": the nine named first own about 16,789 plain
    objects, while a HEAD probe of eight other sources projects 1,333,274 - bea alone is
    913,230, fifty-four times the nine combined. And it said any shared id "alternates plain
    and gzip on every pass": alternation needs both writers to run recurrently, and only the
    updater does. These tools are manual, last wrote on 2026-08-05/08-07, and leave a STOCK of
    un-converted objects rather than a running loop.

    A test that checks five of twenty must not claim twenty - and its docstring must not
    describe a population it never measured.

    Parsed from the AST, not grepped. A text search cannot tell a CALL from the word appearing
    in a docstring, and the first version of this test failed on the very comment written to
    explain the defect - which is R142's lesson: count on a token you control, never on a
    formatted sentence.
    """
    import ast

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    offenders = []
    for rel in ("core/derive_csv.py", "core/r2_util.py", "updater/blob.py",
                "tools/derive_csv_bulk.py", "tools/derive_statcan_tables.py"):
        path = os.path.join(root, rel)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            tree = ast.parse(fh.read(), filename=rel)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not isinstance(f, ast.Attribute) or f.attr != "compress":
                continue
            base = f.value
            if isinstance(base, ast.Name) and base.id in ("gzip", "_gzip"):
                offenders.append(f"{rel}:{node.lineno}")

    # core/r2_util.py is the ONE place allowed to call it - that is what the helper wraps.
    allowed = [o for o in offenders if o.startswith("core/r2_util.py:")]
    assert len(allowed) == 1, (
        "expected exactly one gzip.compress call, inside r2_util.gzip_bytes; found %r"
        % (allowed,))
    rest = [o for o in offenders if not o.startswith("core/r2_util.py:")]
    assert not rest, (
        "bare gzip.compress CALL on an R2 write path - use core.r2_util.gzip_bytes: "
        + ", ".join(rest))
