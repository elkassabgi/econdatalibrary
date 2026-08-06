"""§5.7 boundary: coverage (no catalog row to go stale) vs coherence (served CSV frozen).

Pinned here (2026-08-05, the defillama cycle):
  * mapped ids derived + residual keys provably uncatalogued -> a 'csv coverage note:'
    that must NOT demote (before this, partial catalogue coverage was punished HARDER
    than zero coverage, and statfin/snb/unesco_*/who_sdg sat permanently partial —
    the R244 always-red gate disease);
  * ZERO mapped keys while the catalogue has rows (the key-form-mismatch class,
    defillama pre-fix) stays a hard 'csv coherence unmet' demotion — that is a real
    §5.7 violation, the served CSVs are frozen;
  * the defillama fetcher qualifies cursor keys to the catalog suffix, so its served
    ids map under the exact rule.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _catalog(tmp_path, ids):
    db = tmp_path / "catalog.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [(i, i.split(":", 1)[0]) for i in ids])
    con.commit()
    con.close()
    return str(db)


class _Unit:
    def __init__(self, source_id):
        self.source_id = source_id
        self.key = f"{source_id}/_all"


class _Res:
    def __init__(self, cursors, obs=10):
        self.series_cursors = cursors
        self.obs = obs


def _run(monkeypatch, tmp_path, catalog_ids, cursors, derive_out=None, source="src"):
    from updater import orchestrate as O
    from updater import config as C
    # The boundary under test is the r2 branch: locally, derive-all repairs any
    # unmapped residue for small sources and neither note can ever be produced.
    monkeypatch.setattr(C, "BACKEND", "r2", raising=False)
    monkeypatch.setenv("ECONDL_CATALOG",
                       _catalog(tmp_path, catalog_ids))
    fake = types.ModuleType("updater.derive")
    fake.derive_and_put = lambda ids, blob: derive_out or {"failed": []}
    monkeypatch.setitem(sys.modules, "updater.derive", fake)
    monkeypatch.setattr(O, "_record_for_catalog_sync", lambda ids: None, raising=False)
    monkeypatch.setattr(O, "_resolve_blob", lambda: None, raising=False)
    return O._derive_changed_csvs(_Unit(source), _Res(cursors), blob=object())


def test_mapped_plus_uncatalogued_residue_is_coverage_note(monkeypatch, tmp_path):
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["src:a", "src:b"],
                        cursors={"a": "2026-08-01", "dark1": "2026-08-01",
                                 "dark2": "2026-08-01"})
    assert not failed
    assert note and note.startswith("csv coverage note:"), note
    assert "2 changed keys" in note


def test_zero_mapped_with_rows_stays_coherence_unmet(monkeypatch, tmp_path):
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["src:chain_tvl:BTC"],
                        cursors={"BTC": "2026-08-01"})
    assert note and "csv coherence unmet" in note, note
    assert "none matched" in note


def test_caller_prefix_contract(monkeypatch, tmp_path):
    # The run-loop treats exactly the 'csv coverage note:' prefix as non-demoting;
    # any drift between producer and consumer silently re-reddens the fleet, so the
    # prefix is pinned on both sides here.
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["src:a"],
                        cursors={"a": "2026-08-01", "dark": "2026-08-01"})
    assert note.startswith("csv coverage note:")
    import inspect
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    assert 'csv_err.startswith("csv coverage note:")' in src


def test_bfs_store_prefixed_cursor_maps_exactly(monkeypatch, tmp_path):
    # bfs catalog ids are 'bfs:BFS:{dbid}' while the fetcher used to report bare
    # dbids — 582/582 unmapped, the hard unmet demotion, every run. The fix reports
    # 'BFS:{dbid}', which the exact rule maps with no fallback needed.
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["bfs:BFS:px-x-0102010000_100"],
                        cursors={"BFS:px-x-0102010000_100": "2026-08-05"},
                        source="bfs")
    assert not failed and note is None, (failed, note)


def test_split_part_expansion_maps_table_cursor_to_all_parts(monkeypatch, tmp_path):
    # census tables too large for one CSV are catalogued ONLY as '<table>#<part>' rows;
    # a table-grain cursor must conservatively re-derive every part, not fall through
    # to the coverage note (the parts' CSVs genuinely go stale).
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["census:eits__m3#no", "census:eits__m3#yes"],
                        cursors={"eits__m3": "2026-08-05"},
                        source="census")
    assert not failed and note is None, (failed, note)


def test_split_part_expansion_escapes_like_wildcards(monkeypatch, tmp_path):
    # A cursor key containing % must not over-match: 'a%b' may expand only to its own
    # parts, never to 'axb#...'. With one genuinely-mapped key alongside, the wildcard
    # key must land in the coverage residue rather than derive a stranger's parts.
    failed, note = _run(monkeypatch, tmp_path,
                        catalog_ids=["src:ok", "src:axb#1"],
                        cursors={"ok": "2026-08-05", "a%b": "2026-08-05"})
    assert not failed
    assert note and note.startswith("csv coverage note:") and "1 changed keys" in note, note


def test_defillama_cursor_keys_are_catalog_qualified(monkeypatch):
    from updater.strategies.fetchers import defillama as D
    from updater import blob
    import pyarrow as pa
    monkeypatch.setattr(blob, "row_count", lambda p: 0)
    monkeypatch.setattr(D.merge, "merge_and_write",
                        lambda path, tbl, mode, dedup_keys: (tbl.num_rows, "2026-08-05"))
    import datetime as dt
    keys = ["__ALL__", "Bitcoin"]
    dates = [dt.date(2026, 8, 5), dt.date(2026, 8, 5)]
    tbl = pa.table({"series_key": keys,
                    "obs_date": pa.array(dates, pa.date32()),
                    "value": [1.0, 2.0]})
    cursors = {}
    from updater.strategies.fetchers._common import Tally
    D._merge_file("/x/chains_tvl.parquet", tbl, ["series_key", "obs_date"], Tally(),
                  cursors, keys, dates,
                  qualify=lambda k: "tvl:total" if k == "__ALL__" else f"chain_tvl:{k}")
    assert set(cursors) == {"tvl:total", "chain_tvl:Bitcoin"}, cursors
