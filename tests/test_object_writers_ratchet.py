"""Object-store WRITES go through the chokepoints (plan step 1): updater.blob (R2 before T0, the self-hosted
blob store after it), core.r2_util's put_series_csv, and core.licence_targets (licence removals, with its own
self-hosted backend). After T0 r2_util.client() refuses every operation, so a tool that PUTs, COPYs or DELETEs
with its own client stops working at T0 - the files below are the ones still to move. The list may only
SHRINK: a new direct writer fails this test, and a moved one must leave the list.

The scan reads CODE through the parser (tests/_repo_walk), so a comment or docstring naming put_object does
not count, and an aliased or wrapped call still does (the attribute name is what is matched)."""
import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import _repo_walk  # noqa: E402

WRITE_CALLS = {"put_object", "upload_file", "upload_fileobj", "put_series_csv", "copy_object", "delete_object",
               "delete_objects"}
CHOKEPOINTS = {"updater/blob.py", "core/r2_util.py", "core/licence_targets.py"}

LEGACY_OBJECT_WRITERS = {
    "core/derive_csv.py", "core/upload_r2.py",
    "tools/_delete_statcan_r2.py", "tools/_derive_bea_bulk.py", "tools/_upload_biotrademerch_store.py",
    "tools/_upload_clean_full_parquet.py", "tools/cso_repull_matrix.py", "tools/cso_repull_subject.py",
    "tools/delist_timeless_tables.py", "tools/derive_noaa_missing.py", "tools/derive_unsdg_flows.py",
    "tools/flowgrain_insee_melodi.py", "tools/flowgrain_ons_uk.py", "tools/guard_heartbeat.py",
    "tools/probe_csv_freshness.py", "tools/purge_unpermitted_r2.py", "tools/rebuild_cso_retired_from_csv.py",
    "tools/refresh_r2_catalog.py", "tools/refresh_sec_edgar.py", "tools/repull_file.py",
    "tools/sec_edgar_union_repair.py", "tools/series_census.py", "tools/trim_bfs_corrupt_tail.py",
    "tools/upload_statcan_store.py",
}


def _writes(tree) -> set[str]:
    out = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Call):
            name = n.func.attr if isinstance(n.func, ast.Attribute) else getattr(n.func, "id", None)
            if name in WRITE_CALLS:
                out.add(name)
    return out


def _direct_writers() -> set[str]:
    found = set()
    for rel, p in _repo_walk.code_files((".py",), ROOT):
        try:
            tree = ast.parse(open(p, encoding="utf-8").read())
        except SyntaxError:
            continue
        if _writes(tree):
            found.add(rel)
    return found - CHOKEPOINTS


def test_no_new_direct_object_writer():
    new = _direct_writers() - LEGACY_OBJECT_WRITERS
    assert not new, ("these write the object store with their own client - write through updater.blob "
                     f"(from_env().put_atomic / list_keys / list_modified) instead: {sorted(new)}")


def test_the_legacy_list_only_shrinks():
    gone = LEGACY_OBJECT_WRITERS - _direct_writers()
    assert not gone, f"these no longer write directly - remove them from LEGACY_OBJECT_WRITERS: {sorted(gone)}"


def test_the_ratchet_can_fail():
    for code in ("s3.put_object(Bucket=b, Key=k, Body=x)", "c = client(); c.upload_file(p, b, k)",
                 "put_series_csv(s3, b, k, csv)", "s3.delete_objects(Bucket=b, Delete=d)",
                 "f = lambda: s3.copy_object(Bucket=b, Key=k, CopySource=s)"):
        assert _writes(ast.parse(code)), code
    assert not _writes(ast.parse('"""uses put_object"""\n# s3.put_object(...)\nstore.put_atomic(k, b)'))
