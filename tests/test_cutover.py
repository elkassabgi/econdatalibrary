"""core/cutover.py - the machine-wide CUTOVER flag (plan 3.5, 4b). Tests never create the real folder:
FLAG_PATH is monkeypatched to temporary paths, and os.stat is replaced for the error outcomes."""
import errno
import os
import re

import pytest

from core import cutover


def test_absent_means_not_cut_over(tmp_path, monkeypatch):
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "econ" / "CUTOVER"))
    assert cutover.is_cut_over() is False
    cutover.refuse_if_cut_over("a test write")            # does not raise


def test_present_means_cut_over(tmp_path, monkeypatch):
    flag = tmp_path / "CUTOVER"
    flag.write_text("")
    monkeypatch.setattr(cutover, "FLAG_PATH", str(flag))
    assert cutover.is_cut_over() is True
    with pytest.raises(cutover.CutoverRefused, match="a test write"):
        cutover.refuse_if_cut_over("a test write")


def test_a_file_where_the_folder_should_be_means_not_cut_over(tmp_path, monkeypatch):
    blocker = tmp_path / "econ"
    blocker.write_text("")                                  # a FILE named like the folder
    monkeypatch.setattr(cutover, "FLAG_PATH", str(blocker / "CUTOVER"))
    # POSIX raises NotADirectoryError; Windows raises FileNotFoundError: both are "not cut over"
    assert cutover.is_cut_over() is False


@pytest.mark.parametrize("exc", [PermissionError(errno.EACCES, "denied"), OSError(errno.EIO, "io"),
                                 OSError(errno.ELOOP, "loop"), ValueError("embedded null"), RuntimeError("x")])
def test_any_other_failure_is_cut_over(monkeypatch, exc):
    """Fail closed: a check that cannot tell must refuse the write (os.path.exists would say 'absent')."""
    def stat(_p, *a, **k):
        raise exc
    monkeypatch.setattr(cutover.os, "stat", stat)
    assert cutover.is_cut_over() is True


def test_the_real_path_is_the_constant_and_absent_on_this_machine_or_ci():
    assert cutover.FLAG_PATH == r"C:\ProgramData\econ\CUTOVER"
    # Planted control for the whole suite: if the real flag exists, every other test that writes to the
    # cloud copy is running against a retired system - stop here, loudly.
    assert cutover.is_cut_over() is False, "the real CUTOVER flag exists: this machine is past T0"


def test_the_flag_module_has_no_override():
    """R1167 B: no environment read, no argument, no config file - only the module constant."""
    src = open(cutover.__file__, encoding="utf-8").read()
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("#"))
    code = re.sub(r'"""[\s\S]*?"""', "", code)
    for banned in ("environ", "getenv", "argv", "open(", "json", "toml", "configparser", "dotenv"):
        assert banned not in code, f"core/cutover.py reads {banned!r}: the flag path must not be overridable"
    import inspect
    assert list(inspect.signature(cutover.is_cut_over).parameters) == [], "is_cut_over takes no path"
    assert os.path.basename(cutover.__file__) == "cutover.py"
