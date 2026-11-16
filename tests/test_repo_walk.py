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


# ---- in a git checkout the ratchets judge the TRACKED files (t0_ready 2026-11-16: an untracked June scratch
# file in the production checkout failed the catalogue ratchet there while CI was green) -------------------
import os  # noqa: E402
import subprocess  # noqa: E402

import pytest  # noqa: E402


def _git(repo, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    r = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr
    return r.stdout


@pytest.fixture
def checkout(tmp_path):
    """A git repository in a temporary folder: some files tracked (added to the index), some not."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")

    def put(rel, text="x = 1\n", track=True):
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        if track:
            _git(repo, "add", "--", rel)
        return p
    put.repo = repo
    return put


def test_a_checkout_walks_its_tracked_files_only(checkout):
    for rel in ("tools/tracked.py", "core/x/data/nested.py", "top.py", "data/skipped.py", "tests/skipped.py",
                "tools/__pycache__/skipped.py", "tools/node_modules/skipped.py"):
        checkout(rel)
    for rel in ("_verify_eia_resolver.py", "tools/scratch.py", "core/untracked/new.py"):
        checkout(rel, track=False)
    gone = checkout("tools/deleted_from_worktree.py")
    gone.unlink()                                          # tracked, but not there to read
    repo = str(checkout.repo)
    assert W.tracked_files(repo) is not None, "precondition: a checkout is recognised"
    found = {rel for rel, _ in W.code_files((".py",), repo)}
    assert found == {"tools/tracked.py", "core/x/data/nested.py", "top.py"}, found


def test_a_folder_that_is_not_a_checkout_is_walked(tmp_path):
    (tmp_path / "a.py").write_text("x = 1\n")
    assert W.tracked_files(str(tmp_path)) is None
    assert {rel for rel, _ in W.code_files((".py",), str(tmp_path))} == {"a.py"}


def test_a_git_failure_in_a_checkout_raises(tmp_path):
    """A .git that git cannot read is not 'no files': the ratchet must not pass by not looking."""
    (tmp_path / ".git").write_text("gitdir: nowhere-at-all\n")
    (tmp_path / "a.py").write_text("x = 1\n")
    with pytest.raises(RuntimeError, match="git ls-files failed"):
        list(W.code_files((".py",), str(tmp_path)))


def test_an_empty_git_folder_inside_another_repository_raises(checkout):
    """git searches upward from an empty .git and answers for the PARENT (exit 0, no files under it): that is
    not this folder's file list (review of #268, finding 2)."""
    inner = checkout.repo / "nested"
    (inner / ".git").mkdir(parents=True)
    (inner / "a.py").write_text("x = 1\n")
    with pytest.raises(RuntimeError, match="git ls-files failed"):
        list(W.code_files((".py",), str(inner)))


def test_a_git_environment_from_a_caller_does_not_redirect_the_list(checkout, tmp_path, monkeypatch):
    """A hook's GIT_DIR / GIT_INDEX_FILE naming another repository must not replace this checkout's index
    (review of #268, finding 1: removing the strip survived every other test)."""
    checkout("tools/tracked.py")
    other = tmp_path / "other"
    other.mkdir()
    _git(other, "init", "-q")
    monkeypatch.setenv("GIT_DIR", str(other / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(other / ".git" / "index"))
    assert {rel for rel, _ in W.code_files((".py",), str(checkout.repo))} == {"tools/tracked.py"}


def test_the_catalogue_ratchet_sees_a_new_tracked_opener_and_not_an_untracked_one(checkout):
    """The proof at the ratchet itself (tests/test_catalog_path._naming_files): an untracked opener - the
    production-checkout case - is not found; the same file once tracked is, and so fails the ratchet."""
    import test_catalog_path as T
    checkout("core/ok.py", "from core import catalog_path\ncon = catalog_path.connect()\n")
    opener = 'import sqlite3\ncon = sqlite3.connect("data/catalog.db")\n'
    checkout("_verify_eia_resolver.py", opener, track=False)
    repo = str(checkout.repo)
    assert T._naming_files(repo) == set()
    _git(checkout.repo, "add", "--", "_verify_eia_resolver.py")
    assert T._naming_files(repo) == {"_verify_eia_resolver.py"}


def test_the_d1_ratchet_sees_a_new_tracked_caller_and_not_an_untracked_one(checkout):
    import test_d1_remote as T
    caller = 'import subprocess\nsubprocess.run(["npx", "wrangler", "d1", "execute", "econ-catalog", "--remote"])\n'
    checkout("tools/scratch_d1.py", caller, track=False)
    repo = str(checkout.repo)
    assert T._remote_d1_files(repo) == set()
    _git(checkout.repo, "add", "--", "tools/scratch_d1.py")
    assert T._remote_d1_files(repo) == {"tools/scratch_d1.py"}


def test_the_real_checkout_is_judged_by_git_when_it_is_one():
    """CI's checkout and the production checkout both carry .git: there the tracked list is what is walked."""
    if not os.path.exists(os.path.join(W.ROOT, ".git")):
        pytest.skip("not a git checkout (an exported tree): the folder walk applies")
    tracked = set(W.tracked_files(W.ROOT))
    found = {rel for rel, _ in W.code_files((".py",))}
    assert found and found <= tracked
    assert "core/catalog_path.py" in found
