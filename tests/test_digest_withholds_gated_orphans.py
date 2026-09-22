"""The daily digest must not email the id of a withheld source.

`send_digest` lists "unmanaged leftover state row(s)" — state rows whose source_id is not in the
registry — and it listed them BY ID in both the text and the HTML body. A released or gated source
is by construction absent from the registry, so its id is exactly what ends up in that list, and
the digest is emailed. The count is what the reader needs; the ids do not have to be in an email.

The split fails CLOSED: if the gate cannot be read, nothing is named. An unreadable gate is not an
empty gate (R900), and the cost of guessing wrong here is a withheld id in an email.
"""
from __future__ import annotations

import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import send_digest as sd  # noqa: E402


def test_an_ordinary_id_is_named_and_a_gated_one_is_not(monkeypatch):
    monkeypatch.setattr(sd, "_gated_ids", lambda: {"secret_one", "secret_two"})
    named, withheld = sd._split_withheld(["worldbank", "secret_one", "imf_ifs"])
    assert named == ["worldbank", "imf_ifs"], named
    assert withheld == 1


def test_matching_is_case_insensitive(monkeypatch):
    monkeypatch.setattr(sd, "_gated_ids", lambda: {"secret_one"})
    named, withheld = sd._split_withheld(["SECRET_One"])
    assert named == [] and withheld == 1


def test_an_unreadable_gate_names_nothing(monkeypatch):
    """Fail closed. The alternative - naming everything when the gate is missing - is the exact
    failure this change exists to stop."""
    monkeypatch.setattr(sd, "_gated_ids", lambda: None)
    named, withheld = sd._split_withheld(["worldbank", "anything"])
    assert named == []
    assert withheld == 2


def test_the_real_gate_is_readable_and_withholds_a_real_member():
    """A planted positive against the COMMITTED gate, so this cannot pass by testing only fakes.
    No id is printed: the assertions are on counts."""
    gate = sd._gated_ids()
    assert gate is not None, "the committed gate could not be read at all"
    assert len(gate) > 0, "the gate is empty, so withholding could never fire"
    member = sorted(gate)[0]
    named, withheld = sd._split_withheld([member, "worldbank"])
    assert named == ["worldbank"]
    assert withheld == 1


def test_the_rendered_text_covers_all_three_shapes():
    assert sd._orphan_text(["a", "b"], 0) == "a, b"
    assert sd._orphan_text([], 3) == "3 withheld"
    assert sd._orphan_text(["a"], 2) == "a (+2 withheld)"
    assert sd._orphan_text([], 0) == "none"


def test_neither_body_joins_orphan_ids_directly():
    """The regression guard with teeth: both render sites must go through the splitter. A future
    edit that re-adds a direct join to either body fails here."""
    src = inspect.getsource(sd)
    code = "\n".join(ln.split("#")[0] for ln in src.splitlines())
    assert "r[0] for r in orphans" not in code.replace(
        "orphan_named, orphan_withheld = _split_withheld(sorted(r[0] for r in orphans))", ""), (
        "an orphan id list is being built outside the splitter")
    assert code.count("_orphan_text(orphan_named, orphan_withheld)") == 2, (
        "both the text body and the HTML body must render through _orphan_text")
