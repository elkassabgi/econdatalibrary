"""hagstofa: a stored table that answers HTTP 400/404 is WITHDRAWN, MOVED or unknown - decided by a search
of the WHOLE table tree, never by its own folder alone (review R1108).

Measured live 2026-09-23 (one tree search: 395 listings, 351 s, 1,858 table ids):
  SJA04901, TEK01003          absent from the whole tree -> withdrawn, stored history kept frozen
  FYR02103, FYR02104, FYR03002 moved to fyrirtaeki/skradfyrirtaeki/9_eldraefni/ -> structural (a re-key)
The real _get_meta, _listing, _table_tree and _fetch_table run; only HTTP is faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import hagstofa as H  # noqa: E402

B = H.BASE


def _t(i):
    return {"id": i, "type": "t", "text": i, "updated": "2026-09-15T09:00:22"}


def _l(i):
    return {"id": i, "type": "l", "text": i}


# The live shape: SJA04901 gone from sjavarutvegur/utf; FYR02103 moved to 9_eldraefni.
TREE = {
    f"{B}/": [{"dbid": "Atvinnuvegir", "text": "Atvinnuvegir"}],
    f"{B}/Atvinnuvegir/": [_l("sjavarutvegur"), _l("fyrirtaeki")],
    f"{B}/Atvinnuvegir/sjavarutvegur/": [_l("utf")],
    f"{B}/Atvinnuvegir/sjavarutvegur/utf/": [_t("SJA04902.px"), _t("SJA04903.px")],
    f"{B}/Atvinnuvegir/fyrirtaeki/": [_l("2_skraningar"), _l("9_eldraefni")],
    f"{B}/Atvinnuvegir/fyrirtaeki/2_skraningar/": [_t("FYR02101.px")],
    f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/": [_t("FYR02103.px")],
}
GONE = {f"{B}/Atvinnuvegir/sjavarutvegur/utf/SJA04901.px/",
        f"{B}/Atvinnuvegir/fyrirtaeki/2_skraningar/FYR02103.px/"}


class _R:
    def __init__(self, status, body=None, bad_json=False):
        self.status_code, self._body, self._bad = status, body, bad_json

    def json(self):
        if self._bad:
            raise ValueError("not json")
        return self._body


class _Sess:
    def __init__(self, tree=None, override=None):
        self.tree, self.override, self.asked = dict(tree or TREE), dict(override or {}), []

    def get(self, url, timeout=None):
        self.asked.append(url)
        if url in self.override:
            o = self.override[url]
            if isinstance(o, list):                          # a sequence: one response per request
                o = o.pop(0) if len(o) > 1 else o[0]
            if isinstance(o, Exception):
                raise o
            return o
        if url in GONE:
            return _R(400, {"error": "Bad request"})
        if url in self.tree:
            return _R(200, self.tree[url])
        raise AssertionError(f"unexpected GET {url}")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(H.time, "sleep", lambda s: None)


def _fetch(sess, path="sjavarutvegur/utf/SJA04901.px", since=dt.date(2024, 12, 31)):
    return H._fetch_table(sess, "Atvinnuvegir", path, "ICE:Atvinnuvegir:" + path.replace("/", ":"), since)


def test_a_table_absent_from_the_whole_tree_is_withdrawn_and_kept_frozen():
    sess = _Sess()
    sess._hagstofa_withdrawn = {}
    assert _fetch(sess) == ([], "quiet")
    v = sess._hagstofa_withdrawn["Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"]
    assert v == {"verdict": "withdrawn", "date": dt.date.today().isoformat()}


def test_a_table_found_elsewhere_in_the_tree_is_moved_not_withdrawn():
    """Review R1108: 3 of the 4 tables a folder-only rule called withdrawn had MOVED."""
    sess = _Sess()
    sess._hagstofa_withdrawn = {}
    assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
    v = sess._hagstofa_withdrawn["Atvinnuvegir/fyrirtaeki/2_skraningar/FYR02103.px"]
    assert v["verdict"] == "moved" and v["to"] == ["Atvinnuvegir/fyrirtaeki/9_eldraefni/FYR02103.px"]


def test_a_400_on_a_table_still_listed_where_it_was_is_a_break_not_a_withdrawal():
    tree = dict(TREE)
    tree[f"{B}/Atvinnuvegir/sjavarutvegur/utf/"] = TREE[f"{B}/Atvinnuvegir/sjavarutvegur/utf/"] + [_t("SJA04901.px")]
    assert _fetch(_Sess(tree)) == ([], "structural")


@pytest.mark.parametrize("url,bad", [
    (f"{B}/", _R(500, [{"dbid": "Atvinnuvegir"}])),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(500, [_t("FYR02103.px")])),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(429, [_t("FYR02103.px")])),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, [])),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, {"a": 1})),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", _R(200, bad_json=True)),
    (f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/", H.requests.ConnectionError("dropped")),
])
def test_a_tree_not_read_in_full_is_never_evidence_of_withdrawal(url, bad):
    """One unreadable listing voids the search: a partial tree would call a moved table withdrawn."""
    assert _fetch(_Sess(override={url: bad})) == ([], "structural")


def test_a_listing_refused_once_with_429_is_retried_not_given_up():
    url = f"{B}/Atvinnuvegir/fyrirtaeki/9_eldraefni/"
    sess = _Sess(override={url: [_R(429), _R(200, TREE[url])]})
    assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
    assert sess.asked.count(url) == 2
    sess = _Sess(override={url: [_R(429), _R(200, TREE[url])]})
    assert _fetch(sess) == ([], "quiet"), "the tree was read in full after the retry"


def test_the_tree_is_searched_once_per_run():
    sess = _Sess()
    _fetch(sess)
    n = len(sess.asked)
    _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px")
    assert len(sess.asked) == n + 1, "only the second table's own metadata GET, no second tree search"


def test_a_recent_verdict_is_reused_without_searching_and_an_old_one_is_rechecked():
    today = dt.date.today()
    me = "Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"
    sess = _Sess()
    sess._hagstofa_withdrawn = {me: {"verdict": "withdrawn", "date": today.isoformat()}}
    assert _fetch(sess) == ([], "quiet") and len(sess.asked) == 1, sess.asked
    old = (today - dt.timedelta(days=H.WITHDRAWN_RECHECK_DAYS)).isoformat()
    sess = _Sess()
    sess._hagstofa_withdrawn = {me: {"verdict": "withdrawn", "date": old}}
    assert _fetch(sess) == ([], "quiet") and len(sess.asked) > 1, "a month-old verdict is re-verified"
    moved = "Atvinnuvegir/fyrirtaeki/2_skraningar/FYR02103.px"
    sess = _Sess()
    sess._hagstofa_withdrawn = {moved: {"verdict": "moved", "to": ["x"], "date": today.isoformat()}}
    assert _fetch(sess, "fyrirtaeki/2_skraningar/FYR02103.px") == ([], "structural")
    assert len(sess.asked) == 1


def test_a_never_stored_table_that_400s_is_absent_without_searching():
    sess = _Sess()
    assert _fetch(sess, since=None) == ([], "empty") and len(sess.asked) == 1


def test_update_persists_the_verdict_and_the_next_run_does_not_search(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(H.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(H, "_load_catalog", lambda: [{"db": "Atvinnuvegir", "path": "sjavarutvegur/utf/SJA04901.px",
                                                     "id": "SJA04901.px", "text": "x"}])
    pq.write_table(pa.table({"series_key": ["ICE:Atvinnuvegir:sjavarutvegur:utf:SJA04901.px:Fisktegund=0"],
                             "obs_date": pa.array([dt.date(2024, 12, 31)]), "value": [1.0]}),
                   str(tmp_path / "Atvinnuvegir.parquet"))
    sessions = []

    def _new():
        s = _Sess()
        sessions.append(s)
        return s
    monkeypatch.setattr(H, "_session", _new)
    unit = types.SimpleNamespace(config={}, key="hagstofa/_all")
    H.update(unit, None)
    saved = json.loads((tmp_path / H.WITHDRAWN_FILE).read_text())
    assert saved["Atvinnuvegir/sjavarutvegur/utf/SJA04901.px"]["verdict"] == "withdrawn"
    H.update(unit, None)
    assert len(sessions[1].asked) == 1, sessions[1].asked
