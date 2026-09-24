"""The shared ratchet walk (tests/_repo_walk.py): the top-level skip list must not prune nested folders (R1178)."""
import _repo_walk as W


def test_nested_folders_with_skipped_names_are_walked(tmp_path):
    for rel in ("tools/x/data/nested.py", "updater/state/nested.py", "core/docs/nested.py", "a/tests/b.py",
                "top.py", "data/skipped.py", "docs/skipped.py", "tests/skipped.py",
                "tools/node_modules/skipped.py", "tools/__pycache__/skipped.py", "tools/.git/skipped.py"):
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("x = 1\n")
    found = {rel for rel, _ in W.code_files((".py",), str(tmp_path))}
    assert found == {"tools/x/data/nested.py", "updater/state/nested.py", "core/docs/nested.py", "a/tests/b.py",
                     "top.py"}


def test_the_real_repo_is_walked():
    """Positive control on the real tree: a known nested file is seen, the repo's own tests are not."""
    found = {rel for rel, _ in W.code_files((".py",))}
    assert "tools/selfhost/cutover_hook.py" in found and "core/r2_util.py" in found
    assert not any(r.startswith("tests/") for r in found)
