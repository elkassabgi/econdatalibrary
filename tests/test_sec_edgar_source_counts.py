"""A tool that writes `series` directly must refresh `source_counts` in the same operation.

MEASURED DEFECT (2026-09-07). D1 held 17,467 `sec_edgar` rows in `series` while `source_counts`
said 17,437. `source_counts.n` is what the worker serves as the source's browse total
(`sql.ts::BROWSE_SOURCE_COUNT_CACHED`) and what `/v1/stats` sums, so 30 series were advertised
away - a 200 with a plausible number, which is why nothing caught it.

CAUSE, dated by two receipts under `data/`: `_sec_edgar_catalog_receipt_20260905T130238Z.json`
records `new_on_d1: 26` and `...130450Z.json` records `4`. 26 + 4 = 30. `d1_catalog_statements`
emits `INSERT OR IGNORE INTO series` for a new id; `core/sync_catalog_d1.py` refreshes
`source_counts` only for sources its own push touched. Structural, not a one-off - the CI job
`.github/workflows/sec-edgar-daily.yml` runs this path at 08:00 UTC every day.

WHAT THIS FILE PINS, and why each choice was contested:

  * the recount is emitted in `update_catalog`, NOT inside `d1_catalog_statements` - that
    function's exact output is asserted by `tests/test_sec_edgar_d1_statements.py`
    (`assert stmts == [...]`, `assert len(stmts) == 2`), and the recount is not part of the
    per-span statement contract;
  * it runs through `--command`, never `--file`: the file path is the IMPORT endpoint, which
    blocked reads for 112 minutes in R709;
  * it is UNCONDITIONAL, not gated on `n_new_d1` - gating cannot repair drift a previous failed
    run left, so the tool would never self-heal, and the recount is an index seek (measured
    rows_read 17,468 for n = 17,467) so gating buys nothing;
  * a failure REDDENS the run rather than passing quietly (R503).
"""
from __future__ import annotations

import io
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL = os.path.join(os.path.dirname(_HERE), "tools", "refresh_sec_edgar.py")


def _src():
    return io.open(_TOOL, encoding="utf-8").read()


def test_the_refresher_refreshes_the_cached_total():
    src = _src()
    assert "INSERT OR REPLACE INTO source_counts(source_id, n)" in src, (
        "the tool writes `series` directly and must refresh the total it invalidates")
    assert "SELECT 'sec_edgar', COUNT(*) FROM series WHERE source_id = 'sec_edgar'" in src, (
        "use the catalogue sync's own canonical statement, not a hand-rolled variant")


def test_it_uses_the_command_endpoint_not_the_import_endpoint():
    """R709: `wrangler d1 execute --file` puts the database into an import mode that blocked
    every origin read for 112 minutes. One statement must not take that path."""
    src = _src()
    i = src.index("INSERT OR REPLACE INTO source_counts")
    window = src[i:i + 1200]
    assert '"--command", sc_sql' in window, "the recount must go through --command"
    assert '"--file", sc_sql' not in window


def test_it_is_not_gated_on_new_ids():
    """Gating on `n_new_d1` cannot repair drift a previous FAILED run left, so the tool would
    never self-heal and the manual repair would be needed again after every failure."""
    src = _src()
    i = src.index("INSERT OR REPLACE INTO source_counts")
    # STRIP COMMENTS FIRST. The first version of this test matched `if n_new_d1` inside the
    # comment that explains why the recount is NOT gated on it - a test failing on its own
    # rationale. An assertion must read code, not prose about code.
    head = "\n".join(l for l in src[max(0, i - 2500):i].split("\n")
                     if not l.lstrip().startswith("#"))
    assert "if n_new_d1" not in head, (
        "the recount must not be conditional on this run having added ids")
    assert "if not n_new_d1" not in head


def test_a_failed_recount_reddens_the_run():
    """A stale total returns a 200 with a plausible number. Silence here is how the original
    drift survived from 2026-09-05 to 2026-09-07 unnoticed (R503)."""
    src = _src()
    i = src.index("source_counts refresh FAILED")
    window = src[i:i + 400]
    assert "d1_failed = True" in window, "a failed recount must not leave the run green"


def test_the_result_lands_in_the_receipt():
    """On CI the receipt is the only durable record - stdout is not uploaded (R737)."""
    src = _src()
    assert 'receipt["source_counts_after"]' in src
    assert 'receipt["source_counts_meta"]' in src
    assert 'receipt["source_counts_error"]' in src


def test_it_reads_the_value_back():
    """Write-then-verify, and with the cheap statement: a one-row indexed lookup, NOT
    `audit_d1_source_counts.py --remote-truth`, which is two full scans (~13.4M rows) to check a
    single-row write."""
    src = _src()
    assert "SELECT n FROM source_counts " in src
    assert "--remote-truth" not in src


def test_the_recount_comes_after_the_inserts():
    """It counts what the batches wrote; running it first would publish the pre-insert total."""
    src = _src()
    assert src.index("for j in range(0, len(stmts), 400)") < src.index(
        "INSERT OR REPLACE INTO source_counts")


def test_the_statement_builder_is_unchanged():
    """`d1_catalog_statements` output is pinned elsewhere by exact equality; the recount must
    not have leaked into it."""
    src = _src()
    start = src.index("def d1_catalog_statements(")
    end = src.index("\ndef ", start + 10)
    assert "source_counts" not in src[start:end], (
        "the recount belongs in the caller, not in the per-span statement contract")
