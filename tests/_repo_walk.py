"""The ONE folder walk the repo ratchets share (test_r2_cutover_guard, test_d1_remote, test_catalog_path).

R1178: each ratchet had its own walk, and each pruned folders by NAME at any depth - so a file under, say,
tools/x/data/ or updater/state/ was never scanned. The skip list names TOP-LEVEL folders only (the repo's
data, docs, tests, ...); below the top, only folders that never hold our code are pruned. One walk, one
pin (test_repo_walk.py), so the three cannot drift apart again."""
from __future__ import annotations

import os
from typing import Iterator

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOP_LEVEL_SKIP = frozenset({".git", "node_modules", "data", "dist", "tests", "docs", "scratchpad", ".wrangler",
                            "__pycache__", ".claude", "logs", "state"})
ALWAYS_SKIP = frozenset({".git", "node_modules", "__pycache__", ".wrangler"})


def code_files(extensions: tuple[str, ...], root: str = ROOT) -> Iterator[tuple[str, str]]:
    """(repo-relative path with forward slashes, absolute path) for every file ending in `extensions`."""
    for dirpath, dirs, files in os.walk(root):
        skip = TOP_LEVEL_SKIP if os.path.normcase(dirpath) == os.path.normcase(root) else ALWAYS_SKIP
        dirs[:] = [d for d in dirs if d not in skip]
        for f in files:
            if f.endswith(extensions):
                p = os.path.join(dirpath, f)
                yield os.path.relpath(p, root).replace(os.sep, "/"), p
