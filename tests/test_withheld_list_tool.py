"""tools/withheld_list.py on a fake store: create once, conditional edits, refusals, read-back."""
from __future__ import annotations

import hashlib
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))
import withheld_list as T  # noqa: E402


class Store:
    def __init__(self):
        self.objs, self.puts = {}, 0

    def get(self, key):
        return self.objs.get(key)

    def etag(self, key):
        return hashlib.md5(self.objs[key]).hexdigest() if key in self.objs else None

    def put_if(self, key, data, *, if_match=None, if_none_match=False):
        if if_none_match and key in self.objs:
            return None
        if if_match is not None and self.etag(key) != if_match:
            return None
        self.objs[key] = data
        self.puts += 1
        return hashlib.md5(data).hexdigest()


@pytest.fixture
def st(monkeypatch):
    s = Store()
    monkeypatch.setattr(T, "_store", lambda: s)
    monkeypatch.setattr(T, "catalogued", lambda ids: set(ids))
    return s


def _ids(s):
    return json.loads(s.objs[T.KEY])["ids"]


def test_create_is_a_dry_run_unless_applied_and_never_overwrites(st):
    assert T.main(["create"]) == 0 and st.puts == 0
    assert T.main(["create", "--apply"]) == 0 and st.puts == 1
    committed = json.load(open(T.G.JSON_PATH, encoding="utf-8"))
    assert _ids(st) == committed["ids"]
    assert T.main(["create", "--apply"]) == 2 and st.puts == 1, "an existing list was overwritten"


def test_withhold_and_release_are_conditional_and_read_back(st):
    T.main(["create", "--apply"])
    n = len(_ids(st))
    assert T.main(["withhold", "insee_melodi:DS_NEW", "--why", "truncated", "--apply"]) == 0
    assert _ids(st)["insee_melodi:DS_NEW"]["until"] == "complete" and len(_ids(st)) == n + 1
    assert T.main(["release", "insee_melodi:DS_NEW", "--apply"]) == 0
    assert "insee_melodi:DS_NEW" not in _ids(st) and len(_ids(st)) == n
    assert T.main(["release", "insee_melodi:DS_NEVER", "--apply"]) == 2


def test_release_of_everything_leaves_an_empty_list_not_no_object(st):
    st.objs[T.KEY] = T.encode({"version": 1, "ids": {"insee_melodi:A": {"until": "complete"}}})
    assert T.main(["release", "insee_melodi:A", "--apply"]) == 0
    assert _ids(st) == {} and T.KEY in st.objs


def test_an_invalid_id_is_refused_and_nothing_written(st):
    T.main(["create", "--apply"])
    before = st.puts
    assert T.main(["withhold", "insee_melodi:bad id'", "--why", "x", "--apply"]) == 2
    assert st.puts == before


def test_a_change_in_between_refuses(st, monkeypatch):
    T.main(["create", "--apply"])
    real = st.put_if

    def racing(key, data, *, if_match=None, if_none_match=False):
        st.objs[key] = st.objs[key] + b" "                       # another writer
        return real(key, data, if_match=if_match, if_none_match=if_none_match)
    monkeypatch.setattr(st, "put_if", racing)
    assert T.main(["withhold", "insee_melodi:DS_NEW", "--why", "x", "--apply"]) == 4


def test_withhold_needs_a_reason(st):
    T.main(["create", "--apply"])
    with pytest.raises(SystemExit):
        T.main(["withhold", "insee_melodi:DS_NEW", "--apply"])
