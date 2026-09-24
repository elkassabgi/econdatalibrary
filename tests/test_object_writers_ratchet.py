"""Object-store WRITES go through the chokepoints (plan step 1): updater.blob (R2 before T0, the self-hosted
blob store after it), core.r2_util's put_series_csv, and core.licence_targets (licence removals, with its own
self-hosted backend). After T0 r2_util.client() refuses every operation, so a tool that PUTs, COPYs or DELETEs
with its own client stops working at T0 - the files below are the ones still to move. The list may only
SHRINK: a new direct writer fails this test, and a moved one must leave the list.

The scan reads CODE through the parser (tests/_repo_walk), so a comment or docstring naming put_object does
not count, and an aliased or wrapped call still does (the attribute name is what is matched)."""
import ast
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import _repo_walk  # noqa: E402

WRITE_CALLS = {"put_object", "upload_file", "upload_fileobj", "put_series_csv", "copy_object", "delete_object",
               "delete_objects",
               # R1204/R1206: multipart, and the two core.derive_csv helpers that PUT with the client they are
               # handed (derive_one, derive_eia_tables and derive_usda_bulk wrote through them unseen)
               "create_multipart_upload", "upload_part", "upload_part_copy", "complete_multipart_upload",
               "_put_with_backoff", "_put_gzip_file_with_backoff"}
# a string that makes another program write the bucket: wrangler's `r2 object put/delete`, DuckDB `COPY ... TO 's3://'`
_WRITE_TEXT = re.compile(r"r2\s+object\s+(put|delete)|COPY\b.*\bTO\s+'s3://", re.I | re.S)
CHOKEPOINTS = {"updater/blob.py", "core/r2_util.py", "core/licence_targets.py"}

LEGACY_OBJECT_WRITERS = {
    "core/derive_csv.py", "core/upload_r2.py",
    # found by the transitive names above (R1206); they move with core/derive_csv.py
    "tools/derive_eia_tables.py", "tools/derive_one.py", "tools/derive_usda_bulk.py",
    "tools/_delete_statcan_r2.py", "tools/_upload_biotrademerch_store.py",
    "tools/_upload_clean_full_parquet.py", "tools/cso_repull_matrix.py", "tools/cso_repull_subject.py",
    "tools/delist_timeless_tables.py", "tools/guard_heartbeat.py",
    "tools/probe_csv_freshness.py", "tools/purge_unpermitted_r2.py", "tools/rebuild_cso_retired_from_csv.py",
    "tools/refresh_r2_catalog.py", "tools/refresh_sec_edgar.py", "tools/repull_file.py",
    "tools/sec_edgar_union_repair.py", "tools/series_census.py", "tools/trim_bfs_corrupt_tail.py",
    "tools/upload_statcan_store.py",
}


def _writes(tree) -> set[str]:
    out = set()
    # docstrings describe; they do not run (tools/selfhost/cutover_hook.py documents the wrangler commands it blocks)
    docs = {id(b[0].value) for n in ast.walk(tree)
            if isinstance(n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            for b in [getattr(n, "body", [])] if b and isinstance(b[0], ast.Expr) and isinstance(b[0].value, ast.Constant)}
    for n in ast.walk(tree):
        if id(n) in docs:
            continue
        if isinstance(n, ast.Call):
            name = n.func.attr if isinstance(n.func, ast.Attribute) else getattr(n.func, "id", None)
            if name in WRITE_CALLS:
                out.add(name)
            # getattr(s3, "put_object")(...) names the operation as a string
            if name == "getattr" and len(n.args) >= 2 and isinstance(n.args[1], ast.Constant) \
                    and n.args[1].value in WRITE_CALLS:
                out.add(n.args[1].value)
        elif isinstance(n, ast.Constant) and isinstance(n.value, str) and _WRITE_TEXT.search(n.value):
            out.add("external write: " + _WRITE_TEXT.search(n.value).group(0)[:30])
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
    assert not new, ("these write the object store with their own client - write through the CSV store instead: "
                     "updater.blob.csv_store(bucket) with updater.derive._put_with_retry / put_atomic / list_keys "
                     "(NOT from_env(): its default is LocalBlob, R1200): " f"{sorted(new)}")


def test_the_legacy_list_only_shrinks():
    gone = LEGACY_OBJECT_WRITERS - _direct_writers()
    assert not gone, f"these no longer write directly - remove them from LEGACY_OBJECT_WRITERS: {sorted(gone)}"


def test_the_ratchet_can_fail():
    for code in ("s3.put_object(Bucket=b, Key=k, Body=x)", "c = client(); c.upload_file(p, b, k)",
                 "put_series_csv(s3, b, k, csv)", "s3.delete_objects(Bucket=b, Delete=d)",
                 "f = lambda: s3.copy_object(Bucket=b, Key=k, CopySource=s)",
                 # R1204 / R1206 shapes that passed before
                 'getattr(s3, "put_object")(Bucket=b, Key=k, Body=x)',
                 "s3.create_multipart_upload(Bucket=b, Key=k)",
                 "d._put_gzip_file_with_backoff(r2.client, b, k, tmp)",
                 "_put_with_backoff(s3, b, k, body)",
                 'subprocess.run("npx wrangler r2 object put econ-data/k --file p", shell=True)',
                 "con.execute(\"COPY t TO 's3://econ-data/series/x.csv'\")"):
        assert _writes(ast.parse(code)), code
    assert not _writes(ast.parse('"""uses put_object"""\n# s3.put_object(...)\nstore.put_atomic(k, b)'))
    assert not _writes(ast.parse('"""blocks wrangler r2 object\n put"""\ndef f():\n    """COPY x TO \'s3://b\'"""\n'))
