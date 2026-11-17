"""core/catalog.py, the catalogue LIBRARY, opens through core.catalog_path (t0_ready legacy-catalogue, 2026-11-17).

Its premise (refused after T0 without the lock, read-write with it, never creates a missing file, 600 s) is RUN
in tests/test_catalogue_writers_main.py::test_the_library_writer_premise, and its importers' refusals at entry
after T0 are run there too. This file checks what that test cannot: the two ways the module is imported both resolve
core.catalog_path - as core.catalog (jobs/), and as plain `catalog` with core/ first on the path
(core/apply_stage0b.py and core/build_registry.py, run as `python core/<file>.py`)."""
import os
import sqlite3
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# the child is "before T0" whatever this machine's flag says (a child does not see the conftest's moved flag)
_BEFORE_T0 = "import core.cutover as _cu; _cu.FLAG_PATH = sys.argv[2] + '.no_flag'; "


def test_imported_as_plain_catalog_from_core_it_resolves_core_catalog_path(tmp_path):
    db = tmp_path / "data" / "catalog.db"
    db.parent.mkdir()
    sqlite3.connect(db).close()
    code = ("import sys; sys.path.insert(0, sys.argv[1]); import catalog; " + _BEFORE_T0 +
            "assert catalog.catalog_path.__name__ == 'core.catalog_path'; "
            "c = catalog.connect(sys.argv[2]); catalog.init(c); "
            "print(c.execute(\"SELECT count(*) FROM sqlite_master WHERE name='series'\").fetchone()[0])")
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    r = subprocess.run([sys.executable, "-c", code, os.path.join(ROOT, "core"), str(db)], cwd=str(tmp_path),
                       capture_output=True, text=True, env=env, timeout=120)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "1"


def test_imported_as_plain_catalog_a_missing_file_is_not_created(tmp_path):
    db = tmp_path / "data" / "catalog.db"
    db.parent.mkdir()
    code = "import sys; sys.path.insert(0, sys.argv[1]); import catalog; " + _BEFORE_T0 + "catalog.connect(sys.argv[2])"
    env = {k: v for k, v in os.environ.items() if not k.startswith("PYTHON")}
    r = subprocess.run([sys.executable, "-c", code, os.path.join(ROOT, "core"), str(db)], cwd=str(tmp_path),
                       capture_output=True, text=True, env=env, timeout=120)
    assert r.returncode != 0 and "FileNotFoundError" in r.stderr
    assert not db.exists()


def test_the_default_is_the_checkout_s_data_catalogue():
    from core import catalog, catalog_path
    assert os.path.normcase(catalog.catalog_path.under(catalog._ROOT)) == \
        os.path.normcase(os.path.join(ROOT, "data", "catalog" + ".db"))
    assert catalog.catalog_path is catalog_path
    assert not hasattr(catalog, "DB"), "the old module constant named the file outside the resolver"
    with pytest.raises(TypeError):
        catalog.connect(db=None, write=False)          # one mode only: the library is a writer
