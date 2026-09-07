"""Every derive that PUTs a series CSV must store it gzipped, through the shared helper.

WHY. Ahmed paid for a gzip-at-rest migration on 2026-08-18. `derive_statcan_tables.py` carries the
scar in its own comment - *"This tool has its OWN uploader predating the fleet gzip writers - the
first statcan campaign uploaded 1.37 TB uncompressed because of exactly this gap."*

MEASURED 2026-09-07, by an adversarial review of a proposed ilostat re-derive:
`derive_ilostat_indicators.py:274` and `derive_istat_flows.py:340` were still calling

    s3.put_object(Bucket=..., Key=key, Body=body, ContentType="text/csv")

with no compression and no `ContentEncoding`, while **284 of the 502 objects already in the two
ilostat prefixes carry `ContentEncoding: gzip`**. An istat re-derive of 100 flows would have added
2,535 uncompressed objects to a fleet that had been migrated.

AND WHY THE SHARED HELPER, not a fourth private copy: `r2_util.series_csv_put_args` carries the
magic-byte check that R560 was written about - 188 objects shipped double-gzipped and served as
`text/csv` because one uploader compressed a body its producer had already compressed. statcan
kept that check privately; the two fixed here now get it by construction.
"""
from __future__ import annotations

import io
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_TOOLS = os.path.join(_ROOT, "tools")

# Every tool that PUTs an object under the served `series/` prefix.
PUTTERS = ["derive_istat_flows.py", "derive_ilostat_indicators.py", "derive_statcan_tables.py"]

_BARE_PUT = re.compile(
    r"put_object\([^)]*ContentType\s*=\s*[\"']text/csv[\"'][^)]*\)", re.S)


def _src(fn):
    return io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()


def test_no_derive_puts_an_uncompressed_series_csv():
    """A `put_object(... ContentType="text/csv")` with no ContentEncoding stores plain bytes."""
    for fn in PUTTERS:
        src = _src(fn)
        for m in _BARE_PUT.finditer(src):
            call = m.group(0)
            assert ("ContentEncoding" in call or "**_kw" in call), (
                f"{fn}: this put stores an UNCOMPRESSED CSV -\n    {call.strip()[:200]}")


def test_the_two_fixed_tools_use_the_shared_helper():
    """R560's magic-byte check lives in `series_csv_put_args`. Re-implementing the gzip locally
    is how a tool ends up double-gzipping a body its producer already compressed."""
    for fn in ("derive_istat_flows.py", "derive_ilostat_indicators.py"):
        src = _src(fn)
        assert "r2_util.series_csv_put_args(body)" in src, (
            f"{fn}: must compress through the shared helper, not a private gzip call")
        assert "s3.put_object(Bucket=a.bucket, Key=key, Body=_body, **_kw)" in src, fn


def test_statcan_keeps_its_own_guarded_uploader():
    """statcan compresses at ENQUEUE so its queue buffers small bodies; its private magic-byte
    check is therefore load-bearing and must not be 'simplified' away."""
    src = _src("derive_statcan_tables.py")
    assert 'body[:2] != b"\\x1f\\x8b"' in src, "statcan lost its double-gzip guard"
    assert 'ContentEncoding="gzip"' in src


def test_the_shared_helper_still_refuses_to_double_gzip():
    """The property the helper exists for. If this ever regresses, 188 objects' worth of R560
    comes back."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_r2u", os.path.join(_ROOT, "core", "r2_util.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    plain = b"series_id,obs_date,value\nx,2020-01-01,1\n"
    body, kw, digest = m.series_csv_put_args(plain)
    assert body[:2] == b"\x1f\x8b", "a plain CSV must come back gzipped"
    assert kw["ContentEncoding"] == "gzip"
    assert digest is not None, "a plain CSV's digest is of the CSV and must be returned"

    again, kw2, digest2 = m.series_csv_put_args(body)
    assert again is body, "an already-gzipped body must be returned untouched"
    assert kw2["ContentEncoding"] == "gzip"
    assert digest2 is None, (
        "the digest of compressed bytes is not comparable with the digest of a CSV, so the "
        "helper must return None rather than a number that looks comparable")
